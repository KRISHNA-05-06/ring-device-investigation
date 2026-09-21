import copy
import io
import json
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import aws_mcp_server as mcp
import recovery_lambda as backend
import test_replay_submit as fixtures


class ReplayIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.ReplaySubmissionTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.f = self.fixture.f
        self.rows = []
        self.total = None
        self.query_failure = False
        self.fixture.query.invoke.side_effect = self.invoke

    def row(self, outcome, run_id='run-1'):
        started = datetime.now(timezone.utc) - timedelta(seconds=30)
        counts = {'accepted': (1, 1, 0, 0), 'duplicate': (1, 0, 1, 0), 'rejected': (1, 0, 0, 1),
                  'processing': (0, 0, 0, 0), 'failed': (0, 0, 0, 0), 'unexpected_run_shape': (2, 2, 0, 0)}[outcome]
        return {'run_id': run_id, 'status': {'processing': 'running', 'failed': 'failed'}.get(outcome, 'completed'),
                'started_at': started.isoformat(),
                'completed_at': None if outcome == 'processing' else (started + timedelta(seconds=1)).isoformat(),
                **dict(zip(('input_count', 'accepted_count', 'duplicate_count', 'rejected_count'), counts)),
                'record_outcome': outcome}

    def observation(self):
        total = len(self.rows) if self.total is None else self.total
        return {'scope': 'ingestion_attempts_for_one_replay_source', 'replay_id': self.f.pid,
                'source_bucket': backend.BUCKET, 'source_key': self.fixture.key,
                'observation': 'runs_observed' if total else 'no_runs_observed',
                'runs': copy.deepcopy(self.rows), 'matching_run_count': total,
                'returned_runs': len(self.rows), 'limit': 50, 'truncated': total > len(self.rows),
                'submission_checked': False, 'data_modified': False}

    def invoke(self, **kw):
        request = json.loads(kw['Payload'])
        if request['operation'] != 'get_replay_ingestion_status':
            return self.fixture.invoke(**kw)
        if self.query_failure:
            raise RuntimeError('database unavailable')
        return {'Payload': io.BytesIO(backend.canonical_bytes({
            'status': 'ok', 'operation': request['operation'], 'household_id': backend.HOUSEHOLD,
            'database_user': 'ring_reader', 'generated_at': datetime.now(timezone.utc).isoformat(),
            'result': self.observation()}))}

    def wrapper(self, operation, result):
        client = MagicMock()
        client.invoke.return_value = {'Payload': io.BytesIO(backend.canonical_bytes({
            'status': 'ok', 'operation': operation, 'household_id': backend.HOUSEHOLD, 'result': result}))}
        with patch.object(mcp.boto3, 'Session') as session:
            session.return_value.client.return_value = client
            body = getattr(mcp, operation)(self.f.pid)
        self.assertEqual(json.loads(client.invoke.call_args.kwargs['Payload']), {'operation': operation, 'proposal_id': self.f.pid})
        client.close.assert_called_once()
        return body['result']

    def test_real_backend_receipt_status_audit_pass_mcp_contract(self):
        self.rows = [self.row('accepted')]
        receipt = self.fixture.replay()
        self.assertEqual(self.wrapper('replay_records', receipt)['ingestion_status'], 'accepted')
        status = backend.get_operation_status(self.f.s3, self.f.pid)
        self.assertEqual(self.wrapper('get_operation_status', status)['replay']['ingestion']['observed_outcome'], 'accepted')
        audit = backend.get_audit_history(self.f.s3, self.f.pid)
        self.wrapper('get_audit_history', audit)
        self.assertEqual([e['event_type'] for e in audit['events']], ['proposal_created', 'proposal_approved', 'correction_artifact_created', 'replay_submitted'])
        self.assertIsNone(audit['events'][3]['actor_identity'])
        self.assertIsNone(audit['events'][3]['occurred_at'])
        self.assertFalse(audit['history_complete'])
        self.assertFalse(status['replay']['ingestion']['event_content_verified'])

    def test_retry_and_new_ingestion_attempt_do_not_add_submission_event(self):
        self.fixture.replay()
        initial = backend.get_audit_history(self.f.s3, self.f.pid)
        self.rows = [self.row('duplicate', 'retry'), self.row('accepted', 'original')]
        receipt = self.fixture.replay()
        self.assertFalse(receipt['created'])
        self.assertEqual(receipt['ingestion_status'], 'accepted')
        later = backend.get_audit_history(self.f.s3, self.f.pid)
        self.assertEqual(initial['events'], later['events'])
        self.assertEqual(len(later['replay']['ingestion']['observation']['runs']), 2)
        self.f.s3.put_object.assert_called_once()

    def test_no_submission_requires_no_ingestion_query(self):
        self.fixture.query.reset_mock()
        status = backend.get_operation_status(self.f.s3, self.f.pid)
        self.assertEqual(status['replay'], {'found': False, 'submission_status': 'not_observed'})
        self.wrapper('get_operation_status', status)
        self.fixture.query.invoke.assert_not_called()

    def test_pending_processing_failed_and_rejected_are_distinct(self):
        for state in ('awaiting_ingestion', 'processing', 'failed', 'rejected', 'duplicate'):
            self.rows = [] if state == 'awaiting_ingestion' else [self.row(state)]
            receipt = self.fixture.replay()
            self.assertEqual(receipt['ingestion_status'], state)
            self.wrapper('replay_records', receipt)

    def test_incomplete_and_contradictory_evidence_is_not_success(self):
        cases = [([self.row('accepted', 'a'), self.row('accepted', 'b')], 'inconsistent'),
                 ([self.row('rejected', 'a'), self.row('duplicate', 'b')], 'inconsistent'),
                 ([self.row('unexpected_run_shape')], 'inconsistent'),
                 ([self.row('accepted', 'a'), self.row('processing', 'b')], 'processing')]
        for rows, expected in cases:
            self.rows = rows
            self.assertEqual(self.fixture.replay()['ingestion_status'], expected)
        self.rows = [self.row('accepted', str(i)) for i in range(50)]
        self.total = 51
        result = self.fixture.replay()
        self.assertEqual(result['ingestion_status'], 'indeterminate')
        self.wrapper('replay_records', result)

    def test_failed_attempt_then_accepted_preserves_failed_run_evidence(self):
        self.rows = [self.row('accepted', 'later'), self.row('failed', 'earlier')]
        result = self.fixture.replay()
        self.assertEqual(result['ingestion_status'], 'accepted')
        self.assertEqual(result['ingestion']['observation']['matching_run_count'], 2)

    def test_unavailable_database_does_not_erase_submission(self):
        self.query_failure = True
        receipt = self.fixture.replay()
        self.assertEqual(receipt['submission_status'], 'submitted')
        self.assertEqual(receipt['ingestion_status'], 'unknown')
        self.assertFalse(receipt['ingestion']['available'])
        self.wrapper('replay_records', receipt)
        self.wrapper('get_operation_status', backend.get_operation_status(self.f.s3, self.f.pid))
        self.wrapper('get_audit_history', backend.get_audit_history(self.f.s3, self.f.pid))

    def test_bad_query_scope_or_counts_is_reported_as_unavailable(self):
        self.rows = [self.row('accepted')]
        original = self.observation
        for extra in ({'replay_id': 'b' * 64}, {'source_key': 'raw/other'}, {'matching_run_count': True}, {'truncated': True}):
            with patch.object(self, 'observation', return_value={**original(), **extra}):
                self.assertFalse(self.fixture.replay()['ingestion']['available'])

    def test_expired_approval_does_not_hide_historical_replay(self):
        self.fixture.replay()
        with patch.object(backend, 'datetime', wraps=datetime) as clock:
            clock.now.return_value = datetime.fromisoformat(self.f.approval['expires_at'])
            status = backend.get_operation_status(self.f.s3, self.f.pid)
        self.assertEqual(status['proposal_status'], 'expired')
        self.assertTrue(status['replay']['found'])
        self.wrapper('get_operation_status', status)

    def test_existing_foreign_object_is_not_a_successful_replay(self):
        self.f.objects[self.fixture.key] = b'foreign'
        with self.assertRaises(backend.RequestError) as caught:
            backend.get_operation_status(self.f.s3, self.f.pid)
        self.assertEqual(caught.exception.code, 'REPLAY_CONFLICT')

    def test_wrapper_rejects_fabricated_outcomes_bindings_and_actor(self):
        receipt = self.fixture.replay()
        changed = copy.deepcopy(receipt)
        changed['ingestion']['observed_outcome'] = 'accepted'
        with self.assertRaises(RuntimeError):
            self.wrapper('replay_records', changed)
        status = backend.get_operation_status(self.f.s3, self.f.pid)
        changed = copy.deepcopy(status)
        changed['replay']['submission']['metadata']['correction-sha256'] = '0' * 64
        with self.assertRaises(RuntimeError):
            self.wrapper('get_operation_status', changed)
        audit = backend.get_audit_history(self.f.s3, self.f.pid)
        changed = copy.deepcopy(audit)
        changed['events'][3]['actor_identity'] = backend.APPROVER
        with self.assertRaises(RuntimeError):
            self.wrapper('get_audit_history', changed)

    def test_backend_and_wrapper_contract_functions_match(self):
        import inspect
        self.assertEqual(inspect.getsource(backend.checked_ingestion_outcome), inspect.getsource(mcp.checked_ingestion_outcome))

    def test_near_timeout_returns_known_submission_without_query(self):
        self.fixture.replay()
        self.fixture.query.reset_mock()
        result = self.fixture.replay(lambda: 1000)
        self.assertFalse(result['created'])
        self.assertEqual(result['submission_status'], 'submitted')
        self.assertEqual(result['ingestion_status'], 'unknown')
        self.fixture.query.invoke.assert_not_called()


if __name__ == '__main__':
    unittest.main()
