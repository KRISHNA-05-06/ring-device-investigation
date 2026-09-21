import copy
import hashlib
import io
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError, ReadTimeoutError
import recovery_lambda as app

# Reuse the approved/unapproved evidence fixture against this replacement backend.
import test_approval_history as fixtures


class ApplicationTests(unittest.TestCase):
    def setUp(self):
        f = fixtures.ApprovalHistoryTests()
        f.setUp()
        self.f = f
        original = f.proposal['preview']['original_record']
        self.raw_line = app.canonical_bytes(original)
        self.source_key = 'raw/kinesis/test.jsonl'
        f.proposal['preview']['source'] = {
            'bucket': app.BUCKET, 'key': self.source_key, 'line_number': 1,
            'raw_record_sha256': hashlib.sha256(self.raw_line).hexdigest(), 'run_id': 'source-run',
        }
        f.proposal['preview_sha256'] = app.digest(f.proposal['preview'])
        f.approval['preview_sha256'] = f.proposal['preview_sha256']
        f.approval['proposal_sha256'] = app.digest(f.proposal)
        request = {'operation': 'approve_proposal', 'proposal_id': f.pid,
                   'expected_proposal_sha256': app.digest(f.proposal), 'reason': f.approval['reason']}
        f.approval['approval_id'] = app.digest({'request': request, 'operator': app.APPROVER})
        f.objects[f.pkey] = app.canonical_bytes(f.proposal)
        f.objects[f.akey] = app.canonical_bytes(f.approval)
        f.objects[self.source_key] = self.raw_line + b'\n'
        self.key = f'recovery/corrections/{app.HOUSEHOLD}/{f.pid}.json'
        def put(**kw):
            self.assertEqual(kw['IfNoneMatch'], '*')
            self.assertEqual(kw['Bucket'], app.BUCKET)
            self.assertEqual(kw['Key'], self.key)
            if kw['Key'] in f.objects:
                raise ClientError({'Error': {'Code': 'PreconditionFailed'}}, 'PutObject')
            f.objects[kw['Key']] = kw['Body']
            return {}
        f.s3.put_object.side_effect = put
        self.query = MagicMock()
        self.query.invoke.side_effect = lambda **kw: {'Payload': io.BytesIO(app.canonical_bytes({
            'status': 'ok', 'operation': 'preview_record_correction', 'household_id': app.HOUSEHOLD,
            'database_user': 'ring_reader', 'result': {'preview_status': 'ready',
                'preview': f.proposal['preview'], 'preview_sha256': f.proposal['preview_sha256']}}))}
        self.factory = patch.object(app.boto3, 'client', return_value=self.query)
        self.factory.start()
        self.addCleanup(self.factory.stop)

    def apply(self):
        return app.apply_record_correction(self.f.s3, self.f.pid, 'application-request')

    def test_save_then_retry_preserves_id_hash_and_originals(self):
        before = dict(self.f.objects)
        first, second = self.apply(), self.apply()
        self.assertTrue(first['created'])
        self.assertFalse(second['created'])
        self.assertEqual(first['correction'], second['correction'])
        self.assertEqual(first['correction_sha256'], second['correction_sha256'])
        self.assertEqual(first['correction']['corrected_record']['battery_percent'], 75)
        self.assertFalse(first['database_modified'])
        self.assertFalse(first['replayed'])
        self.assertFalse(first['can_replay'])
        for key, value in before.items():
            self.assertEqual(self.f.objects[key], value)
        self.assertEqual(set(self.f.objects) - set(before), {self.key})
        self.f.s3.put_object.assert_called_once()
        self.query.invoke.assert_called_once()
        self.query.close.assert_called_once()

    def test_missing_approval_and_expiry_block_new_writes(self):
        approval = self.f.objects.pop(self.f.akey)
        with self.assertRaises(app.RequestError) as caught:
            self.apply()
        self.assertEqual(caught.exception.code, 'APPROVAL_REQUIRED')
        self.f.objects[self.f.akey] = approval
        with patch.object(app, 'datetime', wraps=datetime) as clock:
            clock.now.return_value = datetime.fromisoformat(self.f.proposal['expires_at'])
            with self.assertRaises(app.RequestError) as caught:
                self.apply()
        self.assertEqual(caught.exception.code, 'PROPOSAL_EXPIRED')
        self.f.s3.put_object.assert_not_called()
        self.query.invoke.assert_not_called()

    def test_expired_retry_reads_existing_without_reauthorizing(self):
        first = self.apply()
        with patch.object(app, 'datetime', wraps=datetime) as clock:
            clock.now.return_value = datetime.fromisoformat(self.f.proposal['expires_at'])
            second = self.apply()
        self.assertFalse(second['created'])
        self.assertEqual(second['correction'], first['correction'])
        self.assertEqual(second['approval_current_status'], 'expired')
        self.f.s3.put_object.assert_called_once()

    def test_source_changes_and_size_limit_block_write(self):
        for raw in (b'{"battery_percent":151}\n', b'x' * 1_048_577, b'\xff', b''):
            with self.subTest(length=len(raw)):
                self.f.objects[self.source_key] = raw
                with self.assertRaises(app.RequestError) as caught:
                    self.apply()
                self.assertEqual(caught.exception.code, 'SOURCE_CHANGED')
        self.f.s3.put_object.assert_not_called()

    def test_fresh_preview_mismatch_blocks_write(self):
        self.query.invoke.side_effect = lambda **kw: {'Payload': io.BytesIO(app.canonical_bytes({
            'status': 'ok', 'operation': 'preview_record_correction', 'household_id': app.HOUSEHOLD,
            'database_user': 'ring_reader', 'result': {'preview_status': 'unavailable'}}))}
        with self.assertRaises(app.RequestError) as caught:
            self.apply()
        self.assertEqual(caught.exception.code, 'PREVIEW_UNAVAILABLE')
        self.f.s3.put_object.assert_not_called()

    def test_expiry_during_source_read_stops_write(self):
        real = self.f.s3.get_object.side_effect
        with patch.object(app, 'datetime', wraps=datetime) as clock:
            clock.now.return_value = self.f.now
            def get(**kw):
                if kw['Key'] == self.source_key:
                    clock.now.return_value = datetime.fromisoformat(self.f.proposal['expires_at'])
                return real(**kw)
            self.f.s3.get_object.side_effect = get
            with self.assertRaises(app.RequestError) as caught:
                self.apply()
        self.assertEqual(caught.exception.code, 'PROPOSAL_EXPIRED')
        self.f.s3.put_object.assert_not_called()

    def test_expiry_during_write_is_reported_and_never_enables_replay(self):
        real = self.f.s3.put_object.side_effect
        with patch.object(app, 'datetime', wraps=datetime) as clock:
            clock.now.return_value = self.f.now
            def put(**kw):
                result = real(**kw)
                clock.now.return_value = datetime.fromisoformat(self.f.proposal['expires_at'])
                return result
            self.f.s3.put_object.side_effect = put
            result = self.apply()
        self.assertTrue(result['persisted'])
        self.assertEqual(result['approval_current_status'], 'expired')
        self.assertFalse(result['can_replay'])

    def test_committed_write_with_lost_response_is_safe_to_retry(self):
        real = self.f.s3.put_object.side_effect
        def put(**kw):
            real(**kw)
            raise ReadTimeoutError(endpoint_url='test')
        self.f.s3.put_object.side_effect = put
        with self.assertRaises(app.RequestError) as caught:
            self.apply()
        self.assertEqual(caught.exception.code, 'RETRY_SAME_PROPOSAL')
        receipt = self.apply()
        self.assertFalse(receipt['created'])
        self.f.s3.put_object.assert_called_once()

    def test_conditional_race_returns_existing_winner(self):
        real = self.f.s3.put_object.side_effect
        def put(**kw):
            real(**kw)
            raise ClientError({'Error': {'Code': 'PreconditionFailed'}}, 'PutObject')
        self.f.s3.put_object.side_effect = put
        self.assertFalse(self.apply()['created'])

    def test_conflicting_saved_correction_cannot_be_overwritten(self):
        result = self.apply()
        saved = copy.deepcopy(result['correction'])
        saved['corrected_record']['battery_percent'] = 76
        self.f.objects[self.key] = app.canonical_bytes(saved)
        with self.assertRaises(app.RequestError) as caught:
            self.apply()
        self.assertEqual(caught.exception.code, 'INVALID_CORRECTION')
        self.f.s3.put_object.assert_called_once()

    def test_changed_approval_cannot_apply(self):
        changed = copy.deepcopy(self.f.approval)
        changed['operator']['arn'] = 'other'
        self.f.objects[self.f.akey] = app.canonical_bytes(changed)
        with self.assertRaises(app.RequestError) as caught:
            self.apply()
        self.assertEqual(caught.exception.code, 'INVALID_APPROVAL')
        self.f.s3.put_object.assert_not_called()

    def test_storage_denial_or_conflict_returns_explicit_error(self):
        for code, expected in [('AccessDenied', 'CORRECTION_WRITE_DENIED'), ('ConditionalRequestConflict', 'RETRY_SAME_PROPOSAL')]:
            with self.subTest(code=code):
                self.f.s3.put_object.side_effect = ClientError({'Error': {'Code': code}}, 'PutObject')
                with self.assertRaises(app.RequestError) as caught:
                    self.apply()
                self.assertEqual(caught.exception.code, expected)
        self.assertNotIn(self.key, self.f.objects)

    def test_request_cannot_supply_approval_or_replacement(self):
        for extra in ({'approved': True}, {'battery_percent': 76}, {'household_id': 'household-002'}):
            with self.subTest(extra=extra):
                result = app.lambda_handler({'operation': 'apply_record_correction', 'proposal_id': self.f.pid, **extra}, None)
                self.assertEqual(result['error_code'], 'INVALID_REQUEST')
        self.f.s3.put_object.assert_not_called()

    def test_handler_dispatch_and_single_attempt_storage_client(self):
        def factory(name, **kw):
            if name == 's3':
                self.assertEqual(kw['config'].retries['total_max_attempts'], 1)
                return self.f.s3
            return self.query
        with patch.object(app.boto3, 'client', side_effect=factory):
            body = app.lambda_handler({'operation': 'apply_record_correction', 'proposal_id': self.f.pid},
                                      SimpleNamespace(aws_request_id='test-handler'))
        self.assertEqual(body['status'], 'ok')
        self.assertEqual(body['operation'], 'apply_record_correction')
        self.f.s3.close.assert_called_once()


if __name__ == '__main__':
    unittest.main(verbosity=2)
