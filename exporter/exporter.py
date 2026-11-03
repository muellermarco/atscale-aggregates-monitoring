"""AtScale aggregate lifecycle exporter for Prometheus.

Sample code, provided as is under the MIT License. Not an AtScale product and not
supported by AtScale. See DISCLAIMER.md in the repository root.

Purpose: AtScale creates, rebuilds and deactivates aggregate tables on its own. This exporter
  polls the aggregates API of one or more AtScale instances, compares each poll with the one
  before, and turns the differences into lifecycle events with a reason:
    created      a new aggregate became active; reason = why AtScale made it (subtype)
    rebuilt      an existing aggregate got a new active instance; reason = batch or on demand
    deactivated  an aggregate left the active set; reason = classified from AtScale's status message
  It also publishes how many aggregates each catalog and model has.
Inputs (environment variables):
  ATSCALE_INSTANCES      comma list of name=base_url, e.g. prod=https://atscale.example.com
  ATSCALE_TOKEN_<NAME>   AtScale API token per instance; NAME is the instance name in upper
                         case with non-alphanumeric characters replaced by "_"
  API_PREFIX             path prefix of the public API (default /v1; /v1/public when you call
                         the sml-api service directly inside Kubernetes)
  CA_BUNDLE              optional PEM file with extra CA certificates (self-signed or
                         private CA); certificate verification is always on
  AGGREGATE_DETAILS      true (default) adds per-aggregate gauges (created time, last build,
                         rows, queries); false keeps only the per-model counts
  SCHEDULES              true (default) reads the build schedules to tell scheduled rebuilds
                         from on-demand ones; needs a token that may read schedules
  POLL_SECONDS (120), CATALOG_CACHE_MINUTES (10), PAGE_SIZE (100, the API maximum), MAX_PAGES (200),
  HTTP_TIMEOUT_SECONDS (30), MAX_RESPONSE_MB (20), PORT (9102), BIND_ADDRESS (0.0.0.0)
  ALLOW_INSECURE_HTTP    true allows http:// instance URLs (the token then travels unencrypted);
                         only for a service inside the cluster. Default: https:// only
  EVENT_LOG              true (default) writes one JSON line per lifecycle event to stdout
Outputs: Prometheus text format on http://$BIND_ADDRESS:$PORT/metrics; /healthz answers 503 when a
  poll loop has stopped. With EVENT_LOG, one JSON line per event on stdout (event "atscale_aggregate")
  for a log store such as Loki or Elasticsearch. User names and query text are never written.
Run: python3 exporter.py

The first poll of each instance only records the current state (warm-up): aggregates that exist when
the exporter starts produce no events, so a restart does not count them twice. Run exactly one replica.
Requests never follow redirects: a redirect would send the API token to another address.
"""
import datetime
import http.client
import json
import logging
import os
import re
import ssl
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

LOG = logging.getLogger("atscale-aggregates-exporter")
EVENT_LOG = logging.getLogger("atscale-aggregates-exporter.events")

MAX_API_PAGE_SIZE = 100       # the AtScale API returns at most 101 rows and no next page above this
HEALTH_GRACE_POLLS = 3        # /healthz fails when a poll loop has not made progress for this many polls
SCHEDULE_WINDOW_S = 1800      # a rebuild that starts up to 30 min after a schedule ran counts as scheduled
UUID = re.compile(r"[0-9a-fA-F-]{36}")
BATCH_MESSAGE = re.compile(r"Activated by batch \[([0-9a-fA-F-]{36})\]")

# Why AtScale created an aggregate: the definition subtype.
CREATE_REASONS = {
    "demand_defined": "query_demand",          # a user query had no matching aggregate
    "prediction_defined": "prediction",        # predicted from the query history
    "hinted": "user_defined_in_model",         # aggregate defined in the model (SML)
    "manual": "user_defined_manual",           # aggregate created by a user
}

# Why AtScale deactivated an aggregate: its status message. AtScale stores a reason code
# internally, but the public API returns only the message. First match wins.
DEACTIVATE_REASONS = (
    (re.compile(r"has not been used during the past", re.I), "unused"),
    (re.compile(r"Aggregate limit exceeded", re.I), "aggregate_limit_exceeded"),
    (re.compile(r"SLA budget exceeded", re.I), "sla_budget_exceeded"),
    (re.compile(r"Deactivated by batch", re.I), "replaced_by_rebuild"),
    (re.compile(r"Deactivated (in AggregateManagerRest|aggs in cube).* by user", re.I), "removed_by_user"),
    (re.compile(r"Unreliable aggregate", re.I), "unreliable"),
    (re.compile(r"Failed / Rejected / Cancelled", re.I), "build_failed"),
    (re.compile(r"invalid (physical )?plan", re.I), "model_changed"),
    (re.compile(r"locality constraint", re.I), "locality_violation"),
    (re.compile(r"orphan", re.I), "orphaned"),
    (re.compile(r"unknown cube", re.I), "model_removed"),
    (re.compile(r"invalid project", re.I), "catalog_invalid"),
    (re.compile(r"Fusing aggregates", re.I), "fused"),
    (re.compile(r"Demand-defined aggregates have been disabled", re.I), "demand_defined_disabled"),
    (re.compile(r"row count limit", re.I), "row_count_limit"),
    (re.compile(r"aggregate table .* no longer exists", re.I), "table_missing"),
    (re.compile(r"User Defined Aggregate .* no longer exists", re.I), "user_defined_removed"),
    (re.compile(r"no longer matches instance", re.I), "user_defined_changed"),
    (re.compile(r"connection .* was removed", re.I), "connection_removed"),
    (re.compile(r"marked Deleted", re.I), "deleted_no_reason_logged"),
)

Aggregate = dict[str, Any]    # one definition from GET /v1/aggregates


def fmt_labels(names: tuple, values: tuple) -> str:
    return ",".join('%s="%s"' % (n, escape(v)) for n, v in zip(names, values))


def escape(value: Any) -> str:
    return str(value).replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ")


class Metrics:
    """Thread-safe store: counters keep growing, gauges of an instance are replaced on every poll."""

    def __init__(self) -> None:
        self.counters: dict[tuple, float] = {}                 # (metric, labels) -> value
        self.gauges: dict[str, dict[tuple, float]] = {}        # env -> {(metric, labels): value}
        self.help: dict[str, tuple[str, str, tuple]] = {}      # metric -> (type, help, label names)
        self.pending: dict[tuple, float] = {}                  # increments held back for new series
        self._lock = threading.Lock()

    def define(self, metric: str, kind: str, help_text: str, label_names: tuple) -> None:
        self.help[metric] = (kind, help_text, label_names)

    def inc(self, metric: str, labels: tuple, amount: float = 1) -> None:
        with self._lock:
            self.counters[(metric, labels)] = self.counters.get((metric, labels), 0) + amount

    def inc_new_series_later(self, metric: str, labels: tuple) -> None:
        """Count one event. A series seen for the first time starts at 0 and gets the event on the next
        flush: PromQL increase() ignores the first sample of a series, so a series born at 1 loses the event."""
        with self._lock:
            key = (metric, labels)
            if key in self.counters:
                self.counters[key] += 1
            else:
                self.counters[key] = 0
                self.pending[key] = self.pending.get(key, 0) + 1

    def flush_pending(self) -> None:
        with self._lock:
            for key, amount in self.pending.items():
                self.counters[key] += amount
            self.pending.clear()

    def set_counter(self, metric: str, labels: tuple, value: float) -> None:
        with self._lock:
            self.counters[(metric, labels)] = value

    def replace_gauges(self, env: str, values: dict[tuple, float]) -> None:
        with self._lock:
            self.gauges[env] = values

    def render(self) -> list[str]:
        with self._lock:
            series = list(self.counters.items())
            for values in self.gauges.values():
                series.extend(values.items())
        by_metric: dict[str, list] = {}
        for (metric, labels), value in series:
            by_metric.setdefault(metric, []).append((labels, value))
        lines = []
        for metric in sorted(by_metric):
            kind, help_text, names = self.help[metric]
            lines += ["# HELP %s %s" % (metric, help_text), "# TYPE %s %s" % (metric, kind)]
            lines += ["%s{%s} %s" % (metric, fmt_labels(names, labels), repr(float(value)))
                      for labels, value in sorted(by_metric[metric])]
        return lines


METRICS = Metrics()
MODEL_LABELS = ("env", "catalog", "model")
AGG_LABELS = MODEL_LABELS + ("aggregate_id", "aggregate", "subtype", "table")
for _name, _help in (("atscale_aggregates", "Active aggregates per catalog, model, subtype and type"),
                     ("atscale_aggregate_rows_total", "Rows in all active aggregate tables, per catalog, model and subtype"),
                     ("atscale_aggregates_unused", "Active aggregates that no query has used yet")):
    METRICS.define(_name, "gauge", _help, MODEL_LABELS + ("subtype", "type"))
METRICS.define("atscale_aggregate_events_total", "counter",
               "Aggregate lifecycle events seen by the exporter, by event and reason", MODEL_LABELS + ("event", "reason", "subtype"))
METRICS.define("atscale_model_info", "gauge", "1 per model the token can read; maps catalog and model IDs to names",
               ("env", "catalog_id", "catalog", "model_id", "model"))
for _name, _help in (("atscale_aggregate_created_timestamp_seconds", "When AtScale created the aggregate definition"),
                     ("atscale_aggregate_built_timestamp_seconds", "When the build of the active instance finished"),
                     ("atscale_aggregate_build_duration_seconds", "Build time of the active instance"),
                     ("atscale_aggregate_rows", "Rows in the active aggregate table"),
                     ("atscale_aggregate_queries", "Queries that used the aggregate (AtScale queryUtilization)"),
                     ("atscale_aggregate_last_query_timestamp_seconds", "When a query last used the aggregate")):
    METRICS.define(_name, "gauge", _help, AGG_LABELS)
for _name, _kind, _help in (("atscale_aggregates_exporter_up", "gauge", "1 if the last poll of the instance succeeded"),
                            ("atscale_aggregates_exporter_last_success_timestamp_seconds", "gauge", "Time of the last successful poll"),
                            ("atscale_aggregates_exporter_poll_errors_total", "counter", "Failed polls")):
    METRICS.define(_name, _kind, _help, ("env",))


def parse_ts(text: Any) -> float:
    """ISO 8601 timestamp (as the AtScale API returns it) to epoch seconds; 0 if missing or invalid."""
    if not isinstance(text, str) or not text:
        return 0.0
    try:
        return datetime.datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return 0.0


def iso(epoch: float) -> str:
    return datetime.datetime.fromtimestamp(epoch, datetime.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def as_dict(value: Any) -> dict:
    return value if isinstance(value, dict) else {}


def num(value: Any) -> float:
    return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else 0.0


def deactivate_reason(message: str | None) -> str:
    if not message:
        return "unknown"
    for pattern, reason in DEACTIVATE_REASONS:
        if pattern.search(message):
            return reason
    return "other"


def rebuild_reason(agg: Aggregate, schedule_runs: list[float]) -> str:
    """Why an aggregate got a new active instance. AtScale stores no trigger, so this is derived:
    a batch build that started shortly after a schedule ran is scheduled, any other batch is on demand
    (API, UI or trigger file), and a build outside a batch rebuilt an aggregate a query needed again."""
    active = as_dict(agg.get("activeInstance"))
    incremental = "incremental" if agg.get("incremental") else "full"
    if not BATCH_MESSAGE.search(str(active.get("message") or "")) and not active.get("batchId"):
        return "query_demand_" + incremental
    started = parse_ts(as_dict(active.get("stats")).get("materializationStartTime"))
    if any(0 <= started - run <= SCHEDULE_WINDOW_S for run in schedule_runs):
        return "scheduled_batch_" + incremental
    return "on_demand_batch_" + incremental


class NoRedirect(urllib.request.HTTPRedirectHandler):
    """Refuse every redirect: following it would send the bearer token to the new address."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        target = urllib.parse.urlsplit(newurl).netloc
        raise urllib.error.HTTPError(req.full_url, code, "redirect to %s refused" % target, headers, fp)


class Heartbeats:
    """When each poll loop last made progress. /healthz fails when a loop has stopped."""

    def __init__(self) -> None:
        self.last: dict[str, float] = {}
        self.max_age_s: float | None = None
        self._lock = threading.Lock()

    def beat(self, env: str) -> None:
        with self._lock:
            self.last[env] = time.time()

    def stale(self) -> list[str]:
        if self.max_age_s is None:
            return []
        now = time.time()
        with self._lock:
            return sorted(env for env, t in self.last.items() if now - t > self.max_age_s)


HEARTBEATS = Heartbeats()


class AtScaleClient:
    """GET requests to one AtScale instance with a bearer token, bounded in time and size."""

    def __init__(self, env: str, base_url: str, token: str, cfg: dict[str, Any]) -> None:
        self.env, self.base_url, self.token, self.cfg = env, base_url.rstrip("/"), token, cfg
        self.opener = urllib.request.build_opener(NoRedirect, urllib.request.HTTPSHandler(context=cfg.get("ssl")))

    def get_json(self, path: str, params: dict[str, Any] | None = None) -> Any:
        HEARTBEATS.beat(self.env)
        url = self.base_url + self.cfg["api_prefix"] + path + ("?" + urllib.parse.urlencode(params) if params else "")
        req = urllib.request.Request(url, headers={"Authorization": "Bearer " + self.token, "Accept": "application/json"})
        with self.opener.open(req, timeout=self.cfg["timeout_s"]) as resp:
            body = resp.read(self.cfg["max_response_bytes"] + 1)
        if len(body) > self.cfg["max_response_bytes"]:
            raise ValueError("%s: response larger than MAX_RESPONSE_MB" % path)
        return json.loads(body)

    def get_pages(self, path: str, params: dict[str, Any]) -> list[dict]:
        """All rows of a paged list ({data: [...], total?}). Pages until a short page when there is no total."""
        rows: list[dict] = []
        size = self.cfg["page_size"]
        for page in range(1, self.cfg["max_pages"] + 1):
            doc = as_dict(self.get_json(path, dict(params, page=page, size=size)))
            batch = doc.get("data")
            if not isinstance(batch, list):
                raise ValueError("%s: response has no 'data' list; check API_PREFIX and the AtScale version" % path)
            rows.extend(r for r in batch if isinstance(r, dict))
            total = doc.get("total")
            if len(batch) < size or (isinstance(total, int) and len(rows) >= total):
                return rows
        LOG.warning("%s: %s stopped at %d pages; raise MAX_PAGES", self.env, path, self.cfg["max_pages"])
        return rows


class InstancePoller:
    """Polls one AtScale instance and turns differences between polls into lifecycle events."""

    def __init__(self, client: AtScaleClient, cfg: dict[str, Any]) -> None:
        self.client, self.cfg, self.env = client, cfg, client.env
        self.active: dict[str, Aggregate] | None = None     # definition id -> aggregate of the last poll
        self.models: dict[str, tuple[str, str]] = {}        # model id -> (catalog name, model name)
        self.models_expiry = 0.0
        self.model_info: dict[tuple, float] = {}           # atscale_model_info series of the last refresh
        self.schedules_readable = cfg["schedules"]

    def poll(self) -> int:
        """One poll; returns the number of events."""
        self.refresh_models()
        listed = {a["id"]: a for a in self.client.get_pages("/aggregates", {}) if isinstance(a.get("id"), str)}
        # A deactivated aggregate stays in the list for a while, with its instance status "invalid".
        current = {i: a for i, a in listed.items() if as_dict(a.get("activeInstance")).get("status") == "active"}
        events = [] if self.active is None else self.diff(self.active, current, listed)
        if self.active is None:
            LOG.info("%s: warm-up, %d active aggregates recorded without events", self.env, len(current))
        self.active = current
        for event in events:
            self.emit(event)
        self.publish_gauges(current)
        return len(events)

    def diff(self, before: dict[str, Aggregate], after: dict[str, Aggregate],
             listed: dict[str, Aggregate] | None = None) -> list[dict[str, Any]]:
        events = []
        rebuilt = {i for i, a in after.items() if i in before and active_id(a) != active_id(before[i])}
        runs = self.schedule_runs([after[i] for i in rebuilt]) if rebuilt else {}
        for agg_id, agg in after.items():
            if agg_id not in before:
                events.append(self.event("created", agg, CREATE_REASONS.get(agg.get("subtype"), "other")))
            elif agg_id in rebuilt:
                events.append(self.event("rebuilt", agg, rebuild_reason(agg, runs.get(agg.get("modelId"), []))))
        gone = [before[i] for i in before if i not in after]
        if gone:
            last = self.last_instances([a for a in gone if a["id"] not in (listed or {})])
            last.update({a["id"]: as_dict(listed[a["id"]].get("latestInstance")) for a in gone if a["id"] in (listed or {})})
            for agg in gone:
                inst = last.get(agg["id"], {})
                event = self.event("deactivated", agg, deactivate_reason(inst.get("message")))
                event.update(status=inst.get("status"), message=inst.get("message"))
                events.append(event)
        return events

    def last_instances(self, gone: list[Aggregate]) -> dict[str, dict]:
        """{definition id: latest instance} for aggregates that left the active set. The definition
        endpoint lists active aggregates only, so the reason comes from the instances endpoint."""
        out = {}
        for agg in gone:
            try:
                rows = self.client.get_pages("/aggregates/instances", {"definitionId": agg["id"]})
            except (OSError, ValueError, http.client.HTTPException) as exc:
                LOG.warning("%s: instances of %s not readable, reason unknown: %s", self.env, agg["id"], exc)
                continue
            if rows:
                out[agg["id"]] = max(rows, key=lambda r: parse_ts(as_dict(r.get("stats")).get("materializationStartTime")))
        return out

    def schedule_runs(self, aggs: list[Aggregate]) -> dict[str, list[float]]:
        """{model id: [epoch of the latest run of each build schedule]}; empty if schedules are not readable."""
        out: dict[str, list[float]] = {}
        if not self.schedules_readable:
            return out
        pairs = {(a.get("catalogId"), a.get("modelId")) for a in aggs}
        for catalog_id, model_id in pairs:
            if not (isinstance(catalog_id, str) and UUID.fullmatch(catalog_id) and isinstance(model_id, str) and UUID.fullmatch(model_id)):
                continue
            try:
                jobs = self.client.get_json("/catalogs/%s/models/%s/aggregates-creation/scheduler" % (catalog_id, model_id))
            except urllib.error.HTTPError as exc:
                if exc.code in (401, 403):
                    LOG.warning("%s: the token may not read build schedules; rebuilds are reported as "
                                "batch builds without scheduled/on-demand split", self.env)
                    self.schedules_readable = False
                    return {}
                LOG.warning("%s: schedules of model %s not readable: %s", self.env, model_id, exc)
                continue
            except (OSError, ValueError, http.client.HTTPException) as exc:
                LOG.warning("%s: schedules of model %s not readable: %s", self.env, model_id, exc)
                continue
            out[model_id] = [parse_ts(as_dict(j.get("latestExecution")).get("executedAt"))
                             for j in (jobs if isinstance(jobs, list) else []) if isinstance(j, dict)]
        return out

    def refresh_models(self) -> None:
        if time.time() < self.models_expiry:
            return
        models, info = {}, {}
        for cat in self.client.get_json("/catalogs") or []:
            if not isinstance(cat, dict):
                continue
            for model in cat.get("models") or []:
                if isinstance(model, dict) and model.get("id"):
                    models[model["id"]] = (str(cat.get("name") or cat.get("id")), str(model.get("name") or model["id"]))
                    info[("atscale_model_info", (self.env, cat.get("id"), models[model["id"]][0], model["id"], models[model["id"]][1]))] = 1
        self.models, self.models_expiry = models, time.time() + self.cfg["catalog_cache_s"]
        self.model_info = info

    def names(self, agg: Aggregate) -> tuple[str, str]:
        """(catalog, model) names. A model the token cannot see in /catalogs keeps its ID."""
        catalog, model = self.models.get(agg.get("modelId"), (None, None))
        return (catalog or str(agg.get("catalogCaption") or agg.get("catalogId")), model or str(agg.get("modelId")))

    def event(self, kind: str, agg: Aggregate, reason: str) -> dict[str, Any]:
        catalog, model = self.names(agg)
        active = as_dict(agg.get("activeInstance"))
        stats = as_dict(active.get("stats"))
        when = {"created": parse_ts(as_dict(agg.get("stats")).get("createdAt")),
                "rebuilt": parse_ts(stats.get("materializationEndTime"))}.get(kind) or time.time()
        return {"event": "atscale_aggregate", "action": kind, "reason": reason, "time": iso(when),
                "env": self.env, "catalog": catalog, "model": model, "catalog_id": agg.get("catalogId"),
                "model_id": agg.get("modelId"), "aggregate_id": agg.get("id"), "aggregate": agg.get("name"),
                "subtype": agg.get("subtype"), "type": agg.get("type"), "incremental": bool(agg.get("incremental")),
                "instance_id": active.get("id"), "batch_id": active.get("batchId"), "table": active.get("tableName"),
                "rows": stats.get("numberOfRows"), "build_ms": stats.get("buildDuration"),
                "build_query_id": active.get("buildQueryId"), "message": active.get("message")}

    def emit(self, event: dict[str, Any]) -> None:
        labels = (self.env, event["catalog"], event["model"], event["action"], event["reason"], event.get("subtype") or "")
        METRICS.inc_new_series_later("atscale_aggregate_events_total", labels)
        EVENT_LOG.info(json.dumps({k: v for k, v in event.items() if v is not None}, sort_keys=True))

    def publish_gauges(self, current: dict[str, Aggregate]) -> None:
        values: dict[tuple, float] = dict(self.model_info)
        for agg in current.values():
            catalog, model = self.names(agg)
            key = (self.env, catalog, model, str(agg.get("subtype")), str(agg.get("type")))
            stats, active = as_dict(agg.get("stats")), as_dict(agg.get("activeInstance"))
            inst = as_dict(active.get("stats"))
            for metric, value in (("atscale_aggregates", 1), ("atscale_aggregate_rows_total", num(inst.get("numberOfRows"))),
                                  ("atscale_aggregates_unused", 1 if num(stats.get("queryUtilization")) == 0 else 0)):
                values[(metric, key)] = values.get((metric, key), 0) + value
            if self.cfg["details"]:
                labels = (self.env, catalog, model, agg["id"], str(agg.get("name") or agg["id"]),
                          str(agg.get("subtype")), str(active.get("tableName") or ""))
                for metric, value in (("atscale_aggregate_created_timestamp_seconds", parse_ts(stats.get("createdAt"))),
                                      ("atscale_aggregate_built_timestamp_seconds", parse_ts(inst.get("materializationEndTime"))),
                                      ("atscale_aggregate_build_duration_seconds", num(inst.get("buildDuration")) / 1000),
                                      ("atscale_aggregate_rows", num(inst.get("numberOfRows"))),
                                      ("atscale_aggregate_queries", num(stats.get("queryUtilization"))),
                                      ("atscale_aggregate_last_query_timestamp_seconds", parse_ts(stats.get("mostRecentQuery")))):
                    values[(metric, labels)] = value
        METRICS.replace_gauges(self.env, values)


def active_id(agg: Aggregate) -> Any:
    return as_dict(agg.get("activeInstance")).get("id")


def poll_forever(poller: InstancePoller, poll_s: int) -> None:
    """Poll one instance until the process ends. Each instance has its own thread, so a slow or unreachable
    instance does not delay the others."""
    env = poller.env
    while True:
        HEARTBEATS.beat(env)
        METRICS.flush_pending()     # one poll later: Prometheus has scraped the new series at 0 by now
        try:
            events = poller.poll()
            METRICS.set_counter("atscale_aggregates_exporter_up", (env,), 1)
            METRICS.set_counter("atscale_aggregates_exporter_last_success_timestamp_seconds", (env,), int(time.time()))
            LOG.info("%s: %d active aggregates, %d events", env, len(poller.active or {}), events)
        except (OSError, ValueError, http.client.HTTPException) as exc:   # network, HTTP status, bad JSON
            poll_failed(env)
            LOG.warning("%s: poll failed: %s", env, exc)
        except Exception:   # thread boundary: an unexpected API value must not stop this loop for good
            poll_failed(env)
            LOG.exception("%s: poll failed with an unexpected error", env)
        time.sleep(poll_s)


def poll_failed(env: str) -> None:
    METRICS.set_counter("atscale_aggregates_exporter_up", (env,), 0)
    METRICS.inc("atscale_aggregates_exporter_poll_errors_total", (env,))


class MetricsHandler(BaseHTTPRequestHandler):
    timeout = 10    # seconds; an idle client must not hold a handler thread

    def do_GET(self) -> None:
        if self.path == "/healthz":     # for liveness/readiness probes
            stale = HEARTBEATS.stale()
            if stale:
                self.send_body(("poll loop stopped: %s\n" % ", ".join(stale)).encode(), status=503)
            else:
                self.send_body(b"ok\n")
        elif self.path in ("/metrics", "/"):
            self.send_body(("\n".join(METRICS.render()) + "\n").encode(), "text/plain; version=0.0.4")
        else:
            self.send_error(404)

    def send_body(self, body: bytes, content_type: str = "text/plain", status: int = 200) -> None:
        try:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            LOG.debug("metrics client closed the connection early")

    def log_message(self, format: str, *args: Any) -> None:   # noqa: A002 (name from BaseHTTPRequestHandler)
        LOG.debug("http: " + format, *args)


def token_var(name: str) -> str:
    return "ATSCALE_TOKEN_" + re.sub(r"[^A-Z0-9]", "_", name.upper())


def env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    """A whole-number setting from the environment, checked against its range."""
    raw = os.environ.get(name, str(default))
    try:
        value = int(raw)
    except ValueError:
        raise SystemExit("%s must be a whole number, not %r" % (name, raw)) from None
    if not minimum <= value <= maximum:
        raise SystemExit("%s must be between %d and %d, not %d" % (name, minimum, maximum, value))
    return value


def env_bool(name: str, default: str) -> bool:
    return os.environ.get(name, default).lower() in ("1", "true", "yes")


def load_config() -> dict[str, Any]:
    ca_bundle = os.environ.get("CA_BUNDLE")
    page_size = env_int("PAGE_SIZE", MAX_API_PAGE_SIZE, 1, 10000)
    if page_size > MAX_API_PAGE_SIZE:
        LOG.warning("PAGE_SIZE %d is above the API maximum; using %d", page_size, MAX_API_PAGE_SIZE)
        page_size = MAX_API_PAGE_SIZE
    return {"api_prefix": "/" + os.environ.get("API_PREFIX", "/v1").strip("/"),
            "catalog_cache_s": env_int("CATALOG_CACHE_MINUTES", 10, 1, 1440) * 60,
            "page_size": page_size,
            "max_pages": env_int("MAX_PAGES", 200, 1, 10000),
            "timeout_s": env_int("HTTP_TIMEOUT_SECONDS", 30, 1, 300),
            "max_response_bytes": env_int("MAX_RESPONSE_MB", 20, 1, 1024) * 1024 * 1024,
            "poll_s": env_int("POLL_SECONDS", 120, 30, 3600),
            "port": env_int("PORT", 9102, 1, 65535),
            "bind": os.environ.get("BIND_ADDRESS", "0.0.0.0"),
            "details": env_bool("AGGREGATE_DETAILS", "true"),
            "schedules": env_bool("SCHEDULES", "true"),
            "allow_insecure_http": env_bool("ALLOW_INSECURE_HTTP", "false"),
            "ssl": ssl.create_default_context(cafile=ca_bundle or None)}


def load_pollers(cfg: dict[str, Any]) -> list[InstancePoller]:
    schemes = ("https://", "http://") if cfg.get("allow_insecure_http") else ("https://",)
    pollers = []
    for item in filter(None, (i.strip() for i in os.environ.get("ATSCALE_INSTANCES", "").split(","))):
        env, _, url = item.partition("=")
        token = os.environ.get(token_var(env))
        if not env or not url.startswith(schemes):
            raise SystemExit("ATSCALE_INSTANCES entry %r must be name=https://host "
                             "(http:// only with ALLOW_INSECURE_HTTP=true)" % item)
        if not token:
            raise SystemExit("missing %s for instance %r" % (token_var(env), env))
        if url.startswith("http://"):
            LOG.warning("%s: http:// sends the API token unencrypted; use it only inside the cluster", env)
        pollers.append(InstancePoller(AtScaleClient(env, url, token, cfg), cfg))
    if not pollers:
        raise SystemExit("ATSCALE_INSTANCES is empty")
    return pollers


def setup_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")   # stderr
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(message)s"))   # bare JSON, parseable by the log store
    EVENT_LOG.addHandler(handler)
    EVENT_LOG.propagate = False
    EVENT_LOG.setLevel(logging.INFO if env_bool("EVENT_LOG", "true") else logging.WARNING)


def main() -> None:
    setup_logging()
    cfg = load_config()
    pollers = load_pollers(cfg)
    # a loop beats at least once per poll and per HTTP request, and every request is bounded by the timeout
    HEARTBEATS.max_age_s = HEALTH_GRACE_POLLS * cfg["poll_s"] + 2 * cfg["timeout_s"]
    for poller in pollers:
        threading.Thread(target=poll_forever, args=(poller, cfg["poll_s"]), daemon=True,
                         name="poll-" + poller.env).start()
    LOG.info("serving /metrics on %s:%d for %s", cfg["bind"], cfg["port"], ", ".join(p.env for p in pollers))
    ThreadingHTTPServer((cfg["bind"], cfg["port"]), MetricsHandler).serve_forever()


if __name__ == "__main__":
    main()
