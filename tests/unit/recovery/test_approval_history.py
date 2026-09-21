import copy
import io
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError
import recovery_lambda as recovery


class ApprovalHistoryTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime.now(timezone.utc)
        self.pid = 'a' * 64
        original = {'event_id': 'evt-batch-003', 'household_id': recovery.HOUSEHOLD,
                    'device_id': 'camera-002', 'event_time': '2026-09-18T12:00:00Z',
                    'battery_percent': 150}
        preview = {'preview_version': 1, 'household_id': recovery.HOUSEHOLD,
                   'rejection_id': 6, 'reason': 'Synthetic test',
                   'original_record': original, 'proposed_record': {**original, 'battery_percent': 75},
                   'changes': [{'field': 'battery_percent', 'before': 150, 'after': 75}]}
        self.proposal = {'proposal_version': 1, 'proposal_id': self.pid,
                         'household_id': recovery.HOUSEHOLD, 'approval_status': 'pending',
                         'preview': preview, 'preview_sha256': recovery.digest(preview),
                         'request_sha256': 'b' * 64, 'creation_request_id': 'creation-request',
                         'created_at': (self.now - timedelta(minutes=10)).isoformat(),
                         'expires_at': (self.now + timedelta(minutes=50)).isoformat()}
        reason = 'Reviewed synthetic correction'
        request = {'operation': 'approve_proposal', 'proposal_id': self.pid,
                   'expected_proposal_sha256': recovery.digest(self.proposal), 'reason': reason}
        self.approval = {'approval_version': 1,
                         'approval_id': recovery.digest({'request': request, 'operator': recovery.APPROVER}),
                         'proposal_id': self.pid, 'proposal_sha256': recovery.digest(self.proposal),
                         'preview_sha256': self.proposal['preview_sha256'],
                         'household_id': recovery.HOUSEHOLD, 'decision': 'approved',
                         'operator': dict(recovery.APPROVER), 'reason': reason,
                         'approved_at': (self.now - timedelta(minutes=5)).isoformat(),
                         'expires_at': self.proposal['expires_at'],
                         'url_request_id': 'approval-request', 'lambda_request_id': 'approval-request'}
        self.pkey = f'recovery/proposals/{recovery.HOUSEHOLD}/{self.pid}.json'
        self.akey = f'recovery/approvals/{recovery.HOUSEHOLD}/{self.pid}.json'
        self.objects = {self.pkey: recovery.canonical_bytes(self.proposal),
                        self.akey: recovery.canonical_bytes(self.approval)}
        self.s3 = MagicMock()
        self.s3.list_objects_v2.side_effect = lambda **kw: {
            'Contents': [{'Key': key} for key in sorted(self.objects)
                         if key.startswith(kw['Prefix'])][:kw['MaxKeys']]}
        def get(**kw):
            if kw['Key'] not in self.objects:
                raise ClientError({'Error': {'Code': 'NoSuchKey'}}, 'GetObject')
            return {'Body': io.BytesIO(self.objects[kw['Key']])}
        self.s3.get_object.side_effect = get
        self.s3.put_object.side_effect = AssertionError('Read operations cannot write')
        self.s3.delete_object.side_effect = AssertionError('Read operations cannot delete')

    def test_status_recognizes_approval_without_enabling_execution(self):
        result = recovery.get_operation_status(self.s3, self.pid)
        self.assertEqual(result['proposal_status'], 'approved')
        self.assertEqual(result['stored_approval_status'], 'pending')
        self.assertEqual(result['approval']['operator'], recovery.APPROVER)
        self.assertEqual(result['approval']['approval_sha256'], recovery.digest(self.approval))
        self.assertFalse(result['can_execute'])
        self.s3.list_objects_v2.assert_any_call(Bucket=recovery.BUCKET, Prefix=self.akey, MaxKeys=1)

    def test_audit_preserves_creator_unknown_and_identifies_approver(self):
        before = dict(self.objects)
        first = recovery.get_audit_history(self.s3, self.pid)
        second = recovery.get_audit_history(self.s3, self.pid)
        self.assertEqual(first, second)
        self.assertEqual(first['returned_events'], 2)
        self.assertIsNone(first['events'][0]['actor_identity'])
        self.assertEqual(first['events'][1]['actor_identity'], recovery.APPROVER)
        self.assertEqual(first['events'][1]['occurred_at'], self.approval['approved_at'])
        self.assertFalse(first['history_complete'])
        self.assertIn('approval_retries', first['coverage']['not_included'])
        self.assertEqual(before, self.objects)

    def test_exact_expiry_changes_status_but_not_historical_events(self):
        history = recovery.get_audit_history(self.s3, self.pid)
        with patch.object(recovery, 'datetime', wraps=datetime) as clock:
            clock.now.return_value = datetime.fromisoformat(self.proposal['expires_at'])
            status = recovery.get_operation_status(self.s3, self.pid)
            later_history = recovery.get_audit_history(self.s3, self.pid)
        self.assertEqual(status['proposal_status'], 'expired')
        self.assertEqual(status['approval']['status'], 'expired')
        self.assertEqual(status['approval']['recorded_decision'], 'approved')
        self.assertEqual(status['seconds_until_expiry'], 0)
        self.assertEqual(history, later_history)

    def test_no_approval_is_pending_with_one_creation_event(self):
        del self.objects[self.akey]
        result = recovery.get_operation_status(self.s3, self.pid)
        self.assertEqual(result['proposal_status'], 'pending_approval')
        self.assertEqual(result['approval'], {'found': False, 'status': 'not_recorded'})
        self.assertEqual(recovery.get_audit_history(self.s3, self.pid)['returned_events'], 1)

    def test_expired_unapproved_proposal_has_no_approval(self):
        del self.objects[self.akey]
        with patch.object(recovery, 'datetime', wraps=datetime) as clock:
            clock.now.return_value = datetime.fromisoformat(self.proposal['expires_at'])
            status = recovery.get_operation_status(self.s3, self.pid)
        self.assertEqual(status['proposal_status'], 'expired')
        self.assertFalse(status['approval']['found'])

    def test_missing_proposal_does_not_read_approval(self):
        del self.objects[self.pkey]
        self.assertFalse(recovery.get_operation_status(self.s3, self.pid)['found'])
        self.assertFalse(recovery.get_audit_history(self.s3, self.pid)['found'])
        self.s3.list_objects_v2.assert_not_called()

    def test_similar_key_is_not_an_approval(self):
        self.objects[self.akey + '.backup'] = self.objects.pop(self.akey)
        self.assertIsNone(recovery.load_verified_approval(self.s3, self.proposal))
        self.s3.get_object.assert_not_called()

    def test_read_errors_are_not_reported_as_absent(self):
        for method, code in [('list_objects_v2', 'AccessDenied'), ('get_object', 'AccessDenied'),
                             ('get_object', 'NoSuchKey')]:
            with self.subTest(method=method, code=code):
                client = MagicMock()
                client.list_objects_v2.return_value = {'Contents': [{'Key': self.akey}]}
                getattr(client, method).side_effect = ClientError({'Error': {'Code': code}}, method)
                with self.assertRaises(recovery.RequestError) as caught:
                    recovery.load_verified_approval(client, self.proposal)
                self.assertEqual(caught.exception.code, 'APPROVAL_UNAVAILABLE')

    def test_mismatched_evidence_is_rejected(self):
        cases = {'household_id': 'household-002', 'proposal_id': 'c' * 64,
                 'proposal_sha256': '0' * 64, 'preview_sha256': '0' * 64,
                 'operator': {**recovery.APPROVER, 'user_id': 'OTHER'},
                 'decision': 'pending', 'reason': 'Changed reason', 'approval_id': '0' * 64,
                 'approval_version': True, 'lambda_request_id': '',
                 'approved_at': (self.now - timedelta(hours=1)).isoformat(),
                 'expires_at': (self.now + timedelta(hours=2)).isoformat()}
        for field, value in cases.items():
            with self.subTest(field=field):
                changed = copy.deepcopy(self.approval)
                changed[field] = value
                self.objects[self.akey] = recovery.canonical_bytes(changed)
                with self.assertRaises(recovery.RequestError) as caught:
                    recovery.get_operation_status(self.s3, self.pid)
                self.assertEqual(caught.exception.code, 'INVALID_APPROVAL')

    def test_invalid_json_is_rejected(self):
        for raw in (b'{', b'{"decision":"approved","decision":"pending"}', b'{"x":NaN}', b'[]', b' ' * 524289):
            with self.subTest(raw_length=len(raw)):
                self.objects[self.akey] = raw
                with self.assertRaises(recovery.RequestError) as caught:
                    recovery.load_verified_approval(self.s3, self.proposal)
                self.assertEqual(caught.exception.code, 'INVALID_APPROVAL')

    def test_handler_only_uses_s3_and_closes_it(self):
        for operation in ('get_operation_status', 'get_audit_history'):
            self.s3.reset_mock()
            with patch.object(recovery.boto3, 'client', return_value=self.s3) as factory:
                result = recovery.lambda_handler({'operation': operation, 'proposal_id': self.pid},
                                                  SimpleNamespace(aws_request_id='read-test'))
            self.assertEqual(result['status'], 'ok')
            self.assertEqual(factory.call_args.args, ('s3',))
            self.s3.close.assert_called_once()
            self.s3.put_object.assert_not_called()
            self.s3.delete_object.assert_not_called()

    def test_preview_reports_approval_but_does_not_execute(self):
        with patch.object(recovery.boto3, 'client') as factory, patch.object(
                recovery, 'obtain_replay_prediction', return_value={'predicted_outcome': 'accepted'}):
            result = recovery.preview_saved_replay(self.s3, self.pid)
        self.assertEqual(result['proposal_status'], 'approved')
        self.assertEqual(result['approval']['status'], 'approved')
        self.assertFalse(result['can_execute'])
        self.assertFalse(result['replay_performed'])
        factory.return_value.close.assert_called_once()

    def test_expired_approved_preview_does_not_query(self):
        with patch.object(recovery, 'datetime', wraps=datetime) as clock, patch.object(recovery.boto3, 'client') as factory:
            clock.now.return_value = datetime.fromisoformat(self.proposal['expires_at'])
            result = recovery.preview_saved_replay(self.s3, self.pid)
        self.assertEqual(result['block_reason'], 'PROPOSAL_EXPIRED')
        self.assertEqual(result['approval']['status'], 'expired')
        self.assertIsNone(result['prediction'])
        factory.assert_not_called()


if __name__ == '__main__':
    unittest.main(verbosity=2)
