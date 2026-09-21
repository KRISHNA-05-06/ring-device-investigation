import copy
import io
import json
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

from botocore.exceptions import ClientError, ReadTimeoutError
import recovery_lambda as app
import test_apply_correction as fixtures


class ReplaySubmissionTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.ApplicationTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.f = self.fixture.f
        self.fixture.apply()
        self.f.s3.reset_mock()
        self.query = self.fixture.query
        self.query.reset_mock()
        self.key = f"raw/replays/{app.HOUSEHOLD}/{self.f.pid}.jsonl"
        self.metadata = {}
        self.attributes = {}
        self.get_original = self.f.s3.get_object.side_effect
        self.f.s3.get_object.side_effect = self.get
        self.f.s3.put_object.side_effect = self.put
        self.query.invoke.side_effect = self.invoke

    def get(self, **kw):
        response = self.get_original(**kw)
        if kw['Key'] in self.metadata:
            response.update(Metadata=copy.deepcopy(self.metadata[kw['Key']]),
                            **self.attributes[kw['Key']])
        return response

    def put(self, **kw):
        self.assertEqual(kw['Key'], self.key)
        self.assertEqual(kw['Bucket'], app.BUCKET)
        self.assertEqual(kw['IfNoneMatch'], '*')
        if self.key in self.f.objects:
            raise ClientError({'Error': {'Code': 'PreconditionFailed'}}, 'PutObject')
        self.f.objects[self.key] = kw['Body']
        self.metadata[self.key] = copy.deepcopy(kw['Metadata'])
        self.attributes[self.key] = {name: kw[name] for name in ('ContentType', 'ServerSideEncryption')}
        return {}

    def invoke(self, **kw):
        request = json.loads(kw['Payload'])
        operation = request['operation']
        if operation == 'get_replay_ingestion_status':
            result = {'scope': 'ingestion_attempts_for_one_replay_source', 'replay_id': self.f.pid,
                      'source_bucket': app.BUCKET, 'source_key': self.key, 'observation': 'no_runs_observed',
                      'matching_run_count': 0, 'runs': [], 'returned_runs': 0, 'limit': 50, 'truncated': False,
                      'submission_checked': False, 'data_modified': False}
        elif operation == 'preview_record_correction':
            result = {'preview_status': 'ready', 'preview': self.f.proposal['preview'],
                      'preview_sha256': self.f.proposal['preview_sha256']}
        else:
            self.assertEqual(operation, 'preview_replay')
            event = dict(request['candidate'])
            event['event_time'] = '2026-09-18T12:00:00+00:00'
            result = {'scope': 'one_candidate_against_current_database_snapshot',
                      'predicted_outcome': 'accepted', 'error_code': None, 'error_message': None,
                      'candidate_digest': app.digest(event), 'normalized_event_time': event['event_time'],
                      'validator_rules_sha256': 'e' * 64, 'data_modified': False,
                      'replay_performed': False, 'authorization_checked': False, 'requires_revalidation': True}
        return {'Payload': io.BytesIO(app.canonical_bytes({
            'status': 'ok', 'operation': operation, 'household_id': app.HOUSEHOLD,
            'database_user': 'ring_reader', 'generated_at': datetime.now(timezone.utc).isoformat(), 'result': result}))}

    def replay(self, remaining=None):
        return app.replay_records(self.f.s3, self.f.pid, 'replay-request', remaining)

    def assert_error(self, code, function=None):
        with self.assertRaises(app.RequestError) as caught:
            (function or self.replay)()
        self.assertEqual(caught.exception.code, code)

    def test_submit_and_repeat_preserve_bytes_metadata_and_originals(self):
        before = dict(self.f.objects)
        first, second = self.replay(), self.replay()
        self.assertTrue(first['created'])
        self.assertFalse(second['created'])
        self.assertEqual(first['submission'], second['submission'])
        self.assertEqual(first['submission_status'], 'submitted')
        self.assertEqual(first['ingestion_status'], 'awaiting_ingestion')
        self.assertTrue(first['ingestion']['available'])
        self.assertTrue(first['pipeline_may_modify_database'])
        self.assertFalse(first['database_written_by_this_function'])
        payload = self.f.objects[self.key]
        self.assertEqual(json.loads(payload), self.f.proposal['preview']['proposed_record'])
        self.assertEqual(len(payload.splitlines()), 1)
        self.assertLessEqual(sum(len(k.encode('ascii')) + len(v.encode('ascii')) for k, v in self.metadata[self.key].items()), 2048)
        self.assertEqual(set(self.f.objects) - set(before), {self.key})
        for key, raw in before.items():
            self.assertEqual(self.f.objects[key], raw)
        self.f.s3.put_object.assert_called_once()
        self.assertEqual(self.query.invoke.call_count, 4)
        self.assertEqual(self.query.close.call_count, 3)

    def test_missing_proposal_approval_or_correction_cannot_submit(self):
        for key, code in [(self.f.pkey, 'PROPOSAL_NOT_FOUND'), (self.f.akey, 'APPROVAL_REQUIRED'), (self.fixture.key, 'CORRECTION_REQUIRED')]:
            with self.subTest(code=code):
                raw = self.f.objects.pop(key)
                self.assert_error(code)
                self.f.objects[key] = raw
        self.f.s3.put_object.assert_not_called()
        self.query.invoke.assert_not_called()

    def test_expired_new_submission_is_blocked_but_existing_is_readable(self):
        expiry = datetime.fromisoformat(self.f.approval['expires_at'])
        with patch.object(app, 'datetime', wraps=datetime) as clock:
            clock.now.return_value = expiry
            self.assert_error('PROPOSAL_EXPIRED')
        self.f.s3.put_object.assert_not_called()
        first = self.replay()
        with patch.object(app, 'datetime', wraps=datetime) as clock:
            clock.now.return_value = expiry
            second = self.replay()
        self.assertEqual(second['approval_current_status'], 'expired')
        self.assertEqual(first['submission'], second['submission'])
        self.assertFalse(second['created'])
        self.f.s3.put_object.assert_called_once()

    def test_expiry_during_checks_prevents_submission(self):
        with patch.object(app, 'datetime', wraps=datetime) as clock:
            clock.now.return_value = datetime.now(timezone.utc)
            def get(**kw):
                response = self.get(**kw)
                if kw['Key'] == self.fixture.source_key:
                    clock.now.return_value = datetime.fromisoformat(self.f.approval['expires_at'])
                return response
            self.f.s3.get_object.side_effect = get
            self.assert_error('PROPOSAL_EXPIRED')
        self.f.s3.put_object.assert_not_called()

    def test_write_can_finish_after_expiry_receipt_reports_that(self):
        with patch.object(app, 'datetime', wraps=datetime) as clock:
            clock.now.return_value = datetime.now(timezone.utc)
            def put(**kw):
                self.put(**kw)
                clock.now.return_value = datetime.fromisoformat(self.f.approval['expires_at'])
            self.f.s3.put_object.side_effect = put
            result = self.replay()
        self.assertEqual(result['submission_status'], 'submitted')
        self.assertEqual(result['approval_current_status'], 'expired')
        self.assertEqual(result['ingestion_status'], 'awaiting_ingestion')

    def test_timeout_after_commit_returns_same_submission_on_retry(self):
        def put(**kw):
            self.put(**kw)
            raise ReadTimeoutError(endpoint_url='test')
        self.f.s3.put_object.side_effect = put
        self.assert_error('RETRY_SAME_PROPOSAL')
        result = self.replay()
        self.assertFalse(result['created'])
        self.f.s3.put_object.assert_called_once()

    def test_timeout_before_commit_retries_same_key(self):
        self.f.s3.put_object.side_effect = ReadTimeoutError(endpoint_url='test')
        self.assert_error('RETRY_SAME_PROPOSAL')
        self.assertNotIn(self.key, self.f.objects)
        self.f.s3.put_object.side_effect = self.put
        self.assertTrue(self.replay()['created'])
        self.assertEqual({c.kwargs['Key'] for c in self.f.s3.put_object.call_args_list}, {self.key})

    def test_conditional_race_returns_winner(self):
        def put(**kw):
            self.put(**kw)
            raise ClientError({'Error': {'Code': 'PreconditionFailed'}}, 'PutObject')
        self.f.s3.put_object.side_effect = put
        self.assertFalse(self.replay()['created'])

    def test_foreign_object_and_tampered_metadata_are_not_overwritten(self):
        self.replay()
        original = self.f.objects[self.key]
        self.f.objects[self.key] = b'{"battery_percent":76}\n'
        self.assert_error('REPLAY_CONFLICT')
        self.f.objects[self.key] = original
        metadata = copy.deepcopy(self.metadata[self.key])
        for field, value in [('approval-sha256', '0' * 64), ('candidate-digest', '0' * 64), ('executor-role', 'other'), ('write-authorized-at', self.f.approval['expires_at'])]:
            self.metadata[self.key] = {**metadata, field: value}
            self.assert_error('REPLAY_CONFLICT')
        self.f.s3.put_object.assert_called_once()

    def test_unreadable_or_oversized_existing_object_is_not_missing(self):
        self.replay()
        original = self.f.objects[self.key]
        self.f.objects[self.key] = b'x' * 16001
        self.assert_error('REPLAY_CONFLICT')
        self.f.objects[self.key] = original
        def get(**kw):
            if kw['Key'] == self.key:
                raise ClientError({'Error': {'Code': 'AccessDenied'}}, 'GetObject')
            return self.get(**kw)
        self.f.s3.get_object.side_effect = get
        self.assert_error('REPLAY_UNAVAILABLE')
        self.f.s3.put_object.assert_called_once()

    def test_source_change_blocks_submission(self):
        self.f.objects[self.fixture.source_key] = b'changed'
        self.assert_error('SOURCE_CHANGED')
        self.f.s3.put_object.assert_not_called()

    def test_rejected_prediction_blocks_submission(self):
        def invoke(**kw):
            response = self.invoke(**kw)
            body = app.read_json_body(response['Payload'])
            if body['operation'] == 'preview_replay':
                body['result'].update(predicted_outcome='rejected', error_code='EVENT_ID_CONFLICT', error_message='conflict')
            return {'Payload': io.BytesIO(app.canonical_bytes(body))}
        self.query.invoke.side_effect = invoke
        self.assert_error('REPLAY_VALIDATION_FAILED')
        self.f.s3.put_object.assert_not_called()

    def test_duplicate_prediction_can_use_normal_deduplication(self):
        def invoke(**kw):
            body = app.read_json_body(self.invoke(**kw)['Payload'])
            if body['operation'] == 'preview_replay':
                body['result']['predicted_outcome'] = 'duplicate'
            return {'Payload': io.BytesIO(app.canonical_bytes(body))}
        self.query.invoke.side_effect = invoke
        self.assertEqual(self.replay()['submission']['metadata']['predicted-outcome'], 'duplicate')

    def test_correction_changed_during_checks_blocks_submission(self):
        def invoke(**kw):
            response = self.invoke(**kw)
            if json.loads(kw['Payload'])['operation'] == 'preview_replay':
                correction = json.loads(self.f.objects[self.fixture.key])
                correction['lambda_request_id'] = 'other-valid-request'
                self.f.objects[self.fixture.key] = app.canonical_bytes(correction)
            return response
        self.query.invoke.side_effect = invoke
        self.assert_error('CORRECTION_CHANGED')
        self.f.s3.put_object.assert_not_called()

    def test_insufficient_time_stops_before_write(self):
        self.assert_error('RETRY_SAME_PROPOSAL', lambda: self.replay(lambda: 34999))
        self.f.s3.put_object.assert_not_called()

    def test_denied_and_conflicting_writes_report_explicit_errors(self):
        for code, expected in [('AccessDenied', 'REPLAY_WRITE_DENIED'), ('ConditionalRequestConflict', 'RETRY_SAME_PROPOSAL')]:
            self.f.s3.put_object.side_effect = ClientError({'Error': {'Code': code}}, 'PutObject')
            self.assert_error(expected)
        self.assertNotIn(self.key, self.f.objects)

    def test_caller_cannot_supply_source_replacement_or_approval(self):
        request = {'operation': 'replay_records', 'proposal_id': self.f.pid}
        for extra in [{'approved': True}, {'battery_percent': 76}, {'household_id': 'other'}, {'source_key': 'raw/other'}]:
            result = app.lambda_handler({**request, **extra}, None)
            self.assertEqual(result['error_code'], 'INVALID_REQUEST')
        self.f.s3.put_object.assert_not_called()

    def test_handler_dispatches_with_no_automatic_write_retry(self):
        def factory(name, **kw):
            self.assertEqual(kw['config'].retries['total_max_attempts'], 1)
            return self.f.s3 if name == 's3' else self.query
        with patch.object(app.boto3, 'client', side_effect=factory):
            result = app.lambda_handler({'operation': 'replay_records', 'proposal_id': self.f.pid},
                SimpleNamespace(aws_request_id='handler-request', get_remaining_time_in_millis=lambda: 60000))
        self.assertEqual(result['status'], 'ok')
        self.assertEqual(result['operation'], 'replay_records')
        self.f.s3.close.assert_called_once()


if __name__ == '__main__':
    unittest.main()
