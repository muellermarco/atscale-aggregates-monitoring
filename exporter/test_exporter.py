"""Unit tests for the AtScale aggregate lifecycle exporter. Run: python3 -m unittest exporter/test_exporter.py"""
import json
import unittest
import urllib.error

import exporter as ex

CAT, MODEL = "11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222"
CFG = {"api_prefix": "/v1", "page_size": 2, "max_pages": 10, "timeout_s": 5, "max_response_bytes": 10 ** 6,
       "catalog_cache_s": 600, "details": True, "schedules": True}


def agg(agg_id, instance="i1", subtype="demand_defined", message="Normal aggregate activated",
        start="2026-10-30T10:00:00Z", end="2026-10-30T10:00:05Z", batch=None, incremental=False):
    active = {"id": instance, "status": "active", "message": message, "tableName": "as_agg_" + agg_id,
              "stats": {"materializationStartTime": start, "materializationEndTime": end,
                        "buildDuration": 5000, "numberOfRows": 42}, "buildQueryId": "q-" + instance}
    if batch:
        active["batchId"] = batch
    return {"id": agg_id, "name": agg_id, "type": "system_defined", "subtype": subtype, "incremental": incremental,
            "catalogId": CAT, "catalogCaption": "Sales (main)", "modelId": MODEL, "activeInstance": active,
            "latestInstance": active, "stats": {"createdAt": "2026-10-30T09:59:00Z", "queryUtilization": 0}}


class FakeClient:
    """Serves canned API answers by path; records the requests."""

    def __init__(self, routes):
        self.env, self.routes, self.calls = "test", routes, []

    def get_json(self, path, params=None):
        self.calls.append((path, params))
        answer = self.routes[path]
        if isinstance(answer, Exception):
            raise answer
        return answer(params) if callable(answer) else answer

    get_pages = ex.AtScaleClient.get_pages


def make_poller(state, instances=None, schedules=None):
    """state: list holding the current active aggregates (mutate it between polls)."""
    def aggregates(params):
        page, size = params["page"], params["size"]
        return {"data": state[(page - 1) * size: page * size], "total": len(state)}
    routes = {"/catalogs": [{"id": CAT, "name": "sales_catalog", "models": [{"id": MODEL, "name": "Sales"}]}],
              "/aggregates": aggregates,
              "/aggregates/instances": lambda p: {"data": (instances or {}).get(p["definitionId"], []) if p["page"] == 1 else []},
              "/catalogs/%s/models/%s/aggregates-creation/scheduler" % (CAT, MODEL): schedules if schedules is not None else []}
    client = FakeClient(routes)
    client.cfg = CFG
    return ex.InstancePoller(client, CFG), client


class LifecycleTest(unittest.TestCase):
    def setUp(self):
        ex.METRICS.counters.clear()
        ex.METRICS.gauges.clear()
        ex.METRICS.pending.clear()
        self.events = []
        self._emit = ex.EVENT_LOG.info
        ex.EVENT_LOG.info = lambda line: self.events.append(json.loads(line))

    def tearDown(self):
        ex.EVENT_LOG.info = self._emit

    def test_warm_up_emits_nothing(self):
        poller, _ = make_poller([agg("a"), agg("b"), agg("c")])
        self.assertEqual(poller.poll(), 0)
        self.assertEqual(self.events, [])
        self.assertEqual(len(poller.active), 3)    # paged over two pages of size 2

    def test_created_with_reason_from_subtype(self):
        state = [agg("a")]
        poller, _ = make_poller(state)
        poller.poll()
        state.append(agg("b", subtype="prediction_defined"))
        self.assertEqual(poller.poll(), 1)
        ev = self.events[0]
        self.assertEqual((ev["action"], ev["reason"], ev["catalog"], ev["model"]), ("created", "prediction", "sales_catalog", "Sales"))
        self.assertEqual(ev["time"], "2026-10-30T09:59:00Z")

    def test_deactivated_reason_from_latest_instance(self):
        state = [agg("a"), agg("b")]
        instances = {"b": [{"id": "old", "status": "deleted", "message": "Aggregate dropped and marked Deleted",
                            "stats": {"materializationStartTime": "2026-10-01T00:00:00Z"}},
                           {"id": "i1", "status": "invalid", "message": "Aggregate has not been used during the past 45 days",
                            "stats": {"materializationStartTime": "2026-10-30T10:00:00Z"}}]}
        poller, _ = make_poller(state, instances)
        poller.poll()
        state.pop()
        poller.poll()
        ev = self.events[0]
        self.assertEqual((ev["action"], ev["reason"], ev["status"]), ("deactivated", "unused", "invalid"))

    def test_deactivated_while_still_listed(self):
        state = [agg("a"), agg("b")]
        poller, client = make_poller(state)
        poller.poll()
        gone = agg("b")
        gone["activeInstance"] = dict(gone["activeInstance"], status="invalid",
                                      message="Deactivated in AggregateManagerRest by user [u]")
        gone["latestInstance"] = gone["activeInstance"]
        state[1] = gone
        poller.poll()
        ev = self.events[0]
        self.assertEqual((ev["action"], ev["reason"], ev["status"]), ("deactivated", "removed_by_user", "invalid"))
        self.assertNotIn("/aggregates/instances", [c[0] for c in client.calls])
        self.assertEqual(len(poller.active), 1)

    def test_deactivated_unknown_when_instances_unreadable(self):
        state = [agg("a")]
        poller, client = make_poller(state)
        client.routes["/aggregates/instances"] = OSError("timeout")
        poller.poll()
        state.clear()
        poller.poll()
        self.assertEqual(self.events[0]["reason"], "unknown")

    def test_rebuilt_on_demand_and_scheduled(self):
        state = [agg("a"), agg("b")]
        sched = [{"jobId": "j", "latestExecution": {"executedAt": "2026-10-30T11:00:00Z"}}]
        poller, _ = make_poller(state, schedules=sched)
        poller.poll()
        state[0] = agg("a", instance="i2", message="Activated by batch [33333333-3333-3333-3333-333333333333]",
                       start="2026-10-30T11:05:00Z", end="2026-10-30T11:06:00Z")
        state[1] = agg("b", instance="i3", message="Activated by batch [33333333-3333-3333-3333-333333333333]",
                       start="2026-10-30T13:00:00Z", end="2026-10-30T13:01:00Z", incremental=True)
        poller.poll()
        reasons = {e["aggregate_id"]: e["reason"] for e in self.events}
        self.assertEqual(reasons, {"a": "scheduled_batch_full", "b": "on_demand_batch_incremental"})
        self.assertEqual(self.events[0]["time"], "2026-10-30T11:06:00Z")

    def test_schedules_forbidden_falls_back_once(self):
        state = [agg("a")]
        poller, client = make_poller(state)
        client.routes["/catalogs/%s/models/%s/aggregates-creation/scheduler" % (CAT, MODEL)] = \
            urllib.error.HTTPError("u", 403, "Forbidden", {}, None)
        poller.poll()
        state[0] = agg("a", instance="i2", batch="33333333-3333-3333-3333-333333333333")
        poller.poll()
        self.assertEqual(self.events[0]["reason"], "on_demand_batch_full")
        self.assertFalse(poller.schedules_readable)

    def test_counter_and_gauges(self):
        state = [agg("a"), agg("b", subtype="hinted")]
        poller, _ = make_poller(state)
        poller.poll()
        state.append(agg("c"))
        poller.poll()
        self.assertIn('event="created",reason="query_demand",subtype="demand_defined"} 0.0', "\n".join(ex.METRICS.render()))
        ex.METRICS.flush_pending()
        text = "\n".join(ex.METRICS.render())
        self.assertIn('atscale_aggregate_events_total{env="test",catalog="sales_catalog",model="Sales",event="created",'
                      'reason="query_demand",subtype="demand_defined"} 1.0', text)
        self.assertIn('atscale_aggregates{env="test",catalog="sales_catalog",model="Sales",subtype="demand_defined",'
                      'type="system_defined"} 2.0', text)
        self.assertIn('atscale_aggregate_rows{env="test",catalog="sales_catalog",model="Sales",aggregate_id="c"', text)


class ReasonTest(unittest.TestCase):
    def test_messages_from_the_engine(self):
        cases = {
            "Aggregate has not been used during the past 45 days": "unused",
            "Aggregate limit exceeded in cube 'Sales' (x)": "aggregate_limit_exceeded",
            "Deactivated by batch [b]": "replaced_by_rebuild",
            "Deactivated in AggregateManagerRest by user [admin]": "removed_by_user",
            "Failed / Rejected / Cancelled aggregate marked invalid": "build_failed",
            "Invalidated because invalid plan for too long": "model_changed",
            "Invalidated because invalid project for too long": "catalog_invalid",
            "Expired by orphan checker": "orphaned",
            "Unreliable aggregate marked invalid": "unreliable",
            "Invalidated because aggregate table [t] no longer exists": "table_missing",
            "User Defined Aggregate for [u] no longer exists.": "user_defined_removed",
            "Aggregate dropped and marked Deleted": "deleted_no_reason_logged",
            "": "unknown",
            "something new": "other",
        }
        for message, reason in cases.items():
            self.assertEqual(ex.deactivate_reason(message), reason, message)


class LabelTest(unittest.TestCase):
    def test_escape(self):
        self.assertEqual(ex.fmt_labels(("a",), ('x"y\\z\n',)), 'a="x\\"y\\\\z "')

    def test_token_var(self):
        self.assertEqual(ex.token_var("prod-eu"), "ATSCALE_TOKEN_PROD_EU")


if __name__ == "__main__":
    unittest.main()
