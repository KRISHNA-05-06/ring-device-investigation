import contextlib
import http.client
import io
import json
import re
import threading
import unittest
from unittest.mock import Mock
from http.server import ThreadingHTTPServer
import dashboard


class DashboardTests(unittest.TestCase):
    def test_parameter_boundaries(self):
        options = dashboard.request_options('report_day=2026-09-20&silence_minutes=1&as_of=2026-09-20T03:29:55Z')
        self.assertEqual(options['as_of'],'2026-09-20T03:29:55+00:00')
        for query in ['operation=replay_records','silence_minutes=0','silence_minutes=1&silence_minutes=10',
                      'report_day=2026-02-30','report_day=9999-12-31','as_of=2026-09-20T12:00:00']:
            with self.subTest(query=query), self.assertRaises((ValueError, OverflowError)):
                dashboard.request_options(query)

    def test_only_four_fixed_read_operations_and_partial_failure(self):
        adapter = Mock()
        def call(op,args):
            if op=='get_pipeline_status': raise RuntimeError('test failure')
            return {'status':'ok','household_id':'household-001','database_user':'ring_reader',
                    'operation':op,'result':{}}
        adapter.invoke_query.side_effect=call
        with contextlib.redirect_stderr(io.StringIO()):
            result=dashboard.collect_snapshot(adapter,dashboard.request_options(''))
        self.assertFalse(result['panels']['pipeline']['ok'])
        self.assertTrue(result['panels']['health']['ok'])
        self.assertEqual({c.args[0] for c in adapter.invoke_query.call_args_list},
                         {'get_device_health','get_pipeline_status','get_rejection_summary','get_daily_device_metrics'})

    def test_wrong_household_is_unavailable(self):
        adapter=Mock()
        adapter.invoke_query.return_value={'status':'ok','household_id':'household-002','result':{}}
        with contextlib.redirect_stderr(io.StringIO()):
            result=dashboard.collect_snapshot(adapter,dashboard.request_options(''))
        self.assertTrue(all(not p['ok'] for p in result['panels'].values()))

    def test_loopback_http_token_host_origin_and_no_write_route(self):
        server=ThreadingHTTPServer(('127.0.0.1',0),dashboard.handler_factory(demo=True))
        thread=threading.Thread(target=server.serve_forever,daemon=True); thread.start()
        def request(path,headers=None,method='GET'):
            c=http.client.HTTPConnection('127.0.0.1',server.server_port,timeout=3)
            try:
                c.request(method,path,headers=headers or {})
                response=c.getresponse(); return response.status,response.read().decode(),dict(response.getheaders())
            finally: c.close()
        try:
            status,page,headers=request('/')
            self.assertEqual(status,200); self.assertIn('Offline sample data',page)
            self.assertEqual(headers['Cache-Control'],'no-store')
            token=re.search('name="dashboard-token" content="([^"]+)"',page).group(1)
            self.assertEqual(request('/api/snapshot')[0],403)
            self.assertEqual(request('/',{'Host':'evil.example'})[0],403)
            h={'X-Dashboard-Token':token}
            self.assertEqual(request('/api/snapshot',dict(h,Origin='https://evil.example'))[0],403)
            self.assertEqual(request('/api/snapshot?operation=replay_records',h)[0],400)
            self.assertEqual(request('/api/snapshot',h,'POST')[0],405)
            self.assertEqual(request('/../dashboard.py')[0],404)
            status,body,_=request('/api/snapshot?silence_minutes=1',h)
            self.assertEqual(status,200); self.assertEqual(json.loads(body)['mode'],'demo')
            states=[d['health_status'] for d in json.loads(body)['panels']['health']['result']['devices']]
            self.assertEqual(states,['reporting','suspected_offline','reporting'])
        finally:
            server.shutdown(); server.server_close(); thread.join(timeout=2)


if __name__=='__main__': unittest.main(verbosity=2)
