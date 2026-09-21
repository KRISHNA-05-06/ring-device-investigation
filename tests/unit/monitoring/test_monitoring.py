"""Offline guardrail tests. No AWS access."""
import unittest
from unittest.mock import patch
import monitoring as m


class MonitoringTests(unittest.TestCase):
    def test_only_ten_scoped_alarms_without_actions(self):
        resources = m.template()["Resources"]
        self.assertEqual(len(resources), 10)
        self.assertEqual(len({r["Properties"]["AlarmName"] for r in resources.values()}), 10)
        for resource in resources.values():
            self.assertEqual(resource["Type"], "AWS::CloudWatch::Alarm")
            p = resource["Properties"]
            self.assertFalse(p["ActionsEnabled"])
            self.assertEqual(p["AlarmActions"], [])
            self.assertEqual(len(p["Dimensions"]), 1)
            self.assertEqual(p["TreatMissingData"], "notBreaching")

    def test_lag_uses_milliseconds_and_two_of_three(self):
        p = m.template()["Resources"]["ArchiveLag"]["Properties"]
        self.assertEqual((p["Threshold"], p["Statistic"], p["Period"], p["EvaluationPeriods"], p["DatapointsToAlarm"]),
                         (120000, "Maximum", 60, 3, 2))

    def test_wrong_account_stops_before_other_calls(self):
        with patch.object(m, "aws", return_value={"Account": "000000000000"}) as aws:
            with self.assertRaisesRegex(RuntimeError, "Wrong account"):
                m.identity(None)
            self.assertEqual(aws.call_count, 1)

    def test_verify_detects_wrong_metric_not_just_alarm_existence(self):
        alarms = [dict(r["Properties"], StateValue="OK") for r in m.template()["Resources"].values()]
        alarms[0]["MetricName"] = "Invocations"
        with patch.object(m, "aws", side_effect=[{"status": "CREATE_COMPLETE"}, {"MetricAlarms": alarms}]):
            with self.assertRaisesRegex(RuntimeError, "Configuration mismatch"):
                m.verify(None)

    def test_preview_never_calls_aws(self):
        with patch("sys.argv", ["monitoring.py"]), patch.object(m, "aws", side_effect=AssertionError("No AWS")), patch("builtins.print"):
            m.main()


if __name__ == "__main__":
    unittest.main(verbosity=2)
