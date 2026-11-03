"""Generate the Grafana dashboard "AtScale aggregates: lifecycle".

Sample code, provided as is under the MIT License. Not an AtScale product. See DISCLAIMER.md.

Purpose: writes the dashboard JSON from one definition, so panels, PromQL and LogQL stay consistent.
  Four sources: the exporter's metrics (Prometheus), the exporter's event log (Loki), the AtScale
  engine log (Loki), which also covers the time before the exporter ran, and the prediction reports in
  the AtScale engine database (PostgreSQL, optional): why AtScale did not propose or rejected an aggregate.
Inputs: none. Outputs: dashboard/atscale-aggregates-lifecycle.json and the copy the Helm chart ships.
Run: python3 dashboard/generate.py
"""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
NAME = "atscale-aggregates-lifecycle.json"
OUTPUTS = [ROOT / "dashboard" / NAME, ROOT / "deploy" / "helm" / "atscale-aggregates-monitoring" / "files" / NAME]

PROM = {"type": "prometheus", "uid": "${datasource}"}
LOKI = {"type": "loki", "uid": "${loki}"}
F = 'env=~"$env",catalog=~"$catalog",model=~"$model"'
EVENTS = '$event_selector |= "atscale_aggregate" | json | env=~"$env" | catalog=~"$catalog" | model=~"$model"'
UUID = "[0-9a-f-]{36}"
# Engine log: one line per instance status change, with the catalog (projectId) and model (cubeId) in the MDC.
STATUS = ('$engine_selector |= "status changed to" '
          '| regexp `Aggregate instance (?P<instance_id>%s) status changed to "(?P<status>\\w+)"\\.\\s*(?P<message>.*?)\\s*\\(aggDefId=(?P<aggregate_id>%s)\\)` '
          '| regexp `projectId=(?P<catalog_id>%s)` | regexp `cubeId=(?P<model_id>%s)`' % (UUID, UUID, UUID, UUID))
NORMALIZE = '| label_format message=`{{ if .message }}{{ regexReplaceAll "%s" .message "<id>" }}{{ else }}-{{ end }}`' % UUID
KEY = '| label_format key=`{{ if .model_id }}{{ .model_id }}{{ else }}{{ .catalog_id }}{{ end }}`'
# Engine log: the aggregate candidate AtScale approved for a user query (the query ID is in the MDC).
CANDIDATE = ('$engine_selector |= "is approved" |= "Candidate" '
             '| regexp `queryId=(?P<query_id>%s)` '
             '| regexp `Candidate (?P<aggregate_id>%s) is approved \\(incidental=(?P<incidental>\\w+).*?selection=(?P<selection>.*)\\)$` '
             '| label_format selection=`{{ regexReplaceAll "AggregateDefinitionAttribute\\\\(([^,]*),(\\\\w+),(?:Some\\\\(\\\\w+\\\\)|None),\\\\w+\\\\)" .selection "$1 ($2)" }}`'
             % (UUID, UUID))
# Prometheus lookup: key = model ID (catalog and model name) or catalog ID (catalog name only), for the engine log tables.
NAMES = ('label_replace(max by (model_id, catalog, model) (atscale_model_info{env=~"$env"}), "key", "$1", "model_id", "(.*)") '
         'or label_replace(label_replace(max by (catalog_id, catalog) (atscale_model_info{env=~"$env"}), "key", "$1", "catalog_id", "(.*)"), "model", "(not in the log line)", "", "")')

# Engine database: AtScale stores one prediction report per rule decision and query part (setting
# aggregates.prediction.reports.storePredictorData, default on). G codes: no candidate proposed. R codes: a
# candidate rejected. Rows named after a compound rule repeat a G or R row, so only G### and R### are read.
PG = {"type": "grafana-postgresql-datasource", "uid": "${enginedb}"}
RULES = {
    "G010": "Dataset does not allow system aggregates", "G020": "Measures come from more than one dataset",
    "G030": "Only dimensional measures, with unrelated dimensions", "G040": "Dimensional aggregates are off",
    "G050": "Query construction not supported for aggregates", "G060": "A dataset in the query does not allow aggregates",
    "G070": "Dimensional aggregate would need narrowing, which is off",
    "G080": "Dimension-only query: keys from different datasets", "G090": "Dimension-only query: hierarchies differ",
    "G100": "Dimension-only query: attribute not reachable from all keys",
    "G110": "Dimension-only query: several hierarchies, or attribute excluded from aggregates",
    "G120": "Key excluded from dimensional aggregates", "G130": "Table with the attribute values already exists",
    "G140": "Table with the key values already exists", "G150": "Table with the attribute values already exists",
    "G160": "Table with the key values already exists", "G170": "Measures come from different fact tables",
    "G190": "Existing aggregate has the same dimensionality", "G200": "Existing aggregate has the same dimensionality and all measures",
    "G210": "Query construction not supported for aggregates", "G220": "Aggregate basis could not be built for this query part",
    "G230": "Exact distinct count or distinct sum, not allowed in aggregates", "G240": "No compatible HLL or quantile sketches on the warehouse",
    "G250": "Measures from several datasets, or no preferred store copy", "G260": "Not a multi-source query (deprecated path)",
    "G270": "No promotion to the preferred store", "G280": "Query has a dimensional modification without its own aggregate",
    "G290": "Aggregates for dimensional modifications are off", "G300": "No calculated members in the query",
    "G310": "Dataset is an aggregate in the preferred store",
    "R010": "Filter on a non-additive measure not in the GROUP BY", "R020": "Identical aggregate already exists",
    "R030": "Narrower aggregate does not compress enough", "R040": "Row security (dynamic constraint) in the selection",
    "R045": "Source table has a warehouse row or masking policy", "R050": "Compression score below threshold, or no statistics",
    "R051": "Compression score with slowly changing dimension below threshold", "R060": "Attribute excluded from dimensional aggregates",
    "R070": "Preferred store copy above the per-table row limit", "R080": "No target warehouse connection for the aggregate",
    "R090": "Attribute excluded from fact aggregates", "R110": "Dataset does not allow the preferred store",
    "R120": "Preferred store copy: row count unknown or above the limit", "R130": "Quantile measures without sketch support",
    "R140": "Distinct count estimate measures without sketch support", "R150": "Preferred store total row limit reached",
    "R160": "Calculation type not allowed for a dimensional modification aggregate", "R170": "Dimensional modification aggregate above the complexity limit",
    "R180": "ParallelPeriod inside a range", "R190": "Decimal without precision for the preferred store",
}
RULE_VALUES = ", ".join("('%s', '%s')" % (code, text.replace("'", "''")) for code, text in RULES.items())
# Filters shared by every engine database query: time range, and the catalog and model IDs of the selected
# catalogs and models (from the exporter's atscale_model_info), applied only when not all are selected.
PG_FILTER = ("$__timeFilter(q.received) "
             "and ('${catalog:regex}' = '.*' or qp.project_id::text ~ '^(${catalog_id:regex})$') "
             "and ('${model:regex}' = '.*' or qp.cube_id::text ~ '^(${model_id:regex})$')")
PG_FROM = ("from engine.queries q join engine.aggregate_prediction_reports r on r.query_id = q.query_id "
           "left join engine.queries_planned qp on qp.query_id = q.query_id")
# One row per G or R decision. routine = a code that only says a path does not apply (no preferred store,
# not multi-source, no calculated members); the dashboard hides these unless "Routine codes" is "show".
DECISIONS = ("with d as (select q.received as time, q.query_id::text as query_id, q.query_language as language, "
             "coalesce(qp.cube_name, '-') as model, r.query_part as part, r.content->>'code' as code, "
             "r.content->>'reason' as reason, r.content->>'candidateId' as candidate_id, "
             "(select string_agg((s->>'settingName') || ' = ' || (s->>'value'), ', ') "
             "from json_array_elements(r.content->'applicableSettings') s) as settings "
             "%s where r.report_type = 'rule' and r.content->>'code' ~ '^[GR][0-9]{3}$' and %s), "
             "m(code, meaning) as (values %s), "
             "dm as (select d.*, coalesce(m.meaning, '') as meaning, "
             "regexp_replace(regexp_replace(regexp_replace(d.reason, '^Not nominating [a-z ]+: ', ''), "
             "'[0-9a-f]{8}-[0-9a-f-]{27}', '…', 'g'), '[0-9]+\\.[0-9]+(E-?[0-9]+)?', 'n', 'g') as reason_group, "
             "(d.code in ('G260', 'G270', 'G300') or (d.code = 'G250' and d.reason like 'Not nominating a promoted%%')) as routine "
             "from d left join m using (code)) " % (PG_FROM, PG_FILTER, RULE_VALUES))
SHOWN = "('$routine' = 'show' or not routine)"
# Query parts that went through prediction: one "approved" report each, with the candidates that survived.
APPROVED = ("select count(*) %s where r.report_type = 'approved' and %s%%s" % (PG_FROM, PG_FILTER))

ACTION_COLORS = {"created": "green", "rebuilt": "blue", "deactivated": "red"}
STATUS_COLORS = {"active": "green", "inprogress": "blue", "done": "light-blue", "invalid": "orange",
                 "deleted": "red", "failed": "dark-red", "pending": "yellow"}

pid = [0]


def nid():
    pid[0] += 1
    return pid[0]


def prom(expr, legend="", ref="A", instant=False, fmt="time_series"):
    t = {"datasource": PROM, "expr": expr, "refId": ref, "legendFormat": legend, "format": fmt}
    if instant:
        t.update(instant=True, range=False)
    return t


def loki(expr, ref="A", legend="", instant=False, qtype=None):
    t = {"datasource": LOKI, "expr": expr, "refId": ref, "legendFormat": legend, "queryType": qtype or ("instant" if instant else "range")}
    return t


def sql(raw, ref="A", fmt="table"):
    return {"datasource": PG, "refId": ref, "rawSql": raw, "format": fmt, "rawQuery": True, "editorMode": "code"}


def color_overrides(colors, by="byName"):
    return [{"matcher": {"id": by, "options": k}, "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": c}}]}
            for k, c in colors.items()]


def row(title, y):
    return {"type": "row", "title": title, "id": nid(), "collapsed": False, "gridPos": {"h": 1, "w": 24, "x": 0, "y": y}, "panels": []}


def text(content, x, y, w, h):
    return {"type": "text", "title": "", "id": nid(), "gridPos": {"h": h, "w": w, "x": x, "y": y},
            "options": {"mode": "markdown", "content": content}}


def stat(title, target, x, y, desc, w=4, unit="locale", ds=PROM, color="text", time_from=None, thresholds=None):
    p = {"type": "stat", "title": title, "id": nid(), "datasource": ds, "description": desc,
         "gridPos": {"h": 4, "w": w, "x": x, "y": y}, "targets": [target],
         "options": {"reduceOptions": {"calcs": ["sum" if ds is LOKI else "lastNotNull"], "values": False, "fields": ""},
                     "colorMode": "value", "graphMode": "none", "textMode": "value", "justifyMode": "center"},
         "fieldConfig": {"defaults": {"unit": unit, "decimals": 0, "noValue": "0", "color": {"mode": "thresholds"},
                                      "thresholds": thresholds or {"mode": "absolute", "steps": [{"color": color, "value": None}]}},
                         "overrides": []}}
    if time_from:
        p["timeFrom"] = time_from
    return p


def bars(title, targets, x, y, w, h, desc, ds, colors=None, unit="short"):
    """Stacked bars over time."""
    return {"type": "timeseries", "title": title, "id": nid(), "datasource": ds, "description": desc,
            "gridPos": {"h": h, "w": w, "x": x, "y": y}, "targets": targets, "interval": "10m",
            "options": {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi", "sort": "desc"}},
            "fieldConfig": {"defaults": {"unit": unit, "decimals": 0, "min": 0, "custom": {"drawStyle": "bars", "fillOpacity": 80, "lineWidth": 1,
                                                                                  "stacking": {"mode": "normal", "group": "A"}}},
                            "overrides": color_overrides(colors or {})}}


def lines(title, targets, x, y, w, h, desc, unit="short"):
    return {"type": "timeseries", "title": title, "id": nid(), "datasource": PROM, "description": desc,
            "gridPos": {"h": h, "w": w, "x": x, "y": y}, "targets": targets,
            "options": {"legend": {"displayMode": "table", "placement": "right", "calcs": ["lastNotNull"], "sortBy": "Last *", "sortDesc": True},
                        "tooltip": {"mode": "multi", "sort": "desc"}},
            "fieldConfig": {"defaults": {"unit": unit, "decimals": 0, "custom": {"lineWidth": 2, "fillOpacity": 0}}, "overrides": []}}


def reason_table(title, target, x, y, w, h, desc, labels, rename):
    """Count per label value from a Loki instant metric query, as a sorted table."""
    return table(title, [target], x, y, w, h, desc, LOKI,
                 [{"id": "labelsToFields", "options": {"mode": "columns"}}, {"id": "merge", "options": {}},
                  {"id": "organize", "options": {"excludeByName": {"Time": True}, "indexByName": {k: i for i, k in enumerate(labels)},
                                                 "renameByName": dict(rename, **{"Value #A": "Count"})}}],
                 overrides=[width("Count", 70)], sort="Count")


def table(title, targets, x, y, w, h, desc, ds, transformations, overrides=None, sort=None):
    p = {"type": "table", "title": title, "id": nid(), "datasource": ds, "description": desc,
         "gridPos": {"h": h, "w": w, "x": x, "y": y}, "targets": targets, "transformations": transformations,
         "options": {"showHeader": True, "cellHeight": "sm", "footer": {"show": False}},
         "fieldConfig": {"defaults": {"custom": {"filterable": True, "align": "auto"}}, "overrides": overrides or []}}
    if sort:
        p["options"]["sortBy"] = [{"displayName": sort, "desc": True}]
    return p


def width(name, px):
    return {"matcher": {"id": "byName", "options": name}, "properties": [{"id": "custom.width", "value": px}]}


def cell_colors(name, colors):
    return {"matcher": {"id": "byName", "options": name},
            "properties": [{"id": "custom.cellOptions", "value": {"type": "color-text"}},
                           {"id": "mappings", "value": [{"type": "value", "options": {k: {"color": c, "index": i} for i, (k, c) in enumerate(colors.items())}}]}]}


def unit(name, u):
    return {"matcher": {"id": "byName", "options": name}, "properties": [{"id": "unit", "value": u}]}


def link(name, title, url):
    return {"matcher": {"id": "byName", "options": name}, "properties": [{"id": "links", "value": [{"title": title, "url": url}]}]}


def explore_link(expr):
    """Data link that opens Grafana Explore on the Loki data source with this LogQL (${__value.raw} = the cell)."""
    pane = {"datasource": "${loki}", "queries": [{"refId": "A", "expr": expr, "datasource": {"type": "loki", "uid": "${loki}"}}],
            "range": {"from": "now-30d", "to": "now"}}
    return '/explore?schemaVersion=1&panes={"a":%s}' % json.dumps(pane, separators=(",", ":")).replace('"', '%22')


def logs_table_transforms(keep, rename, join_names=False):
    """Loki log lines -> one column per extracted label; optional join with the Prometheus name lookup."""
    t = [{"id": "extractFields", "options": {"source": "labels", "replace": False, "keepTime": False}}]
    if join_names:     # the join numbers the time fields: Time 1 = log line, Time 2 = Prometheus sample
        keep = ["Time 1" if k == "Time" else k for k in keep]
        rename = dict(rename, **{"Time 1": "Time"})
        t += [{"id": "joinByField", "options": {"byField": "key", "mode": "outerTabular"}},
              {"id": "filterByValue", "options": {"type": "exclude", "match": "any",
                                                  "filters": [{"fieldName": "Line", "config": {"id": "isNull", "options": {}}}]}}]
    t.append({"id": "organize", "options": {"includeByName": {k: True for k in keep}, "indexByName": {k: i for i, k in enumerate(keep)},
                                            "renameByName": rename}})
    return t


def variable_query(name, label, query, ds=PROM, include_all=True, hide=0):
    return {"name": name, "label": label, "type": "query", "datasource": ds, "query": {"query": query, "refId": name},
            "definition": query, "refresh": 2, "multi": include_all, "includeAll": include_all,
            "allValue": ".*" if include_all else None, "current": {"text": "All", "value": "$__all"} if include_all else {},
            "sort": 1, "hide": hide}


def textbox(name, label, default, hide=0):
    return {"name": name, "label": label, "type": "textbox", "query": default,
            "current": {"text": default, "value": default}, "hide": hide}


panels, y = [], 0

# ---- Now -------------------------------------------------------------------------------------------
panels.append(row("Aggregates now", y)); y += 1
panels += [
    stat("Active aggregates", prom('sum(atscale_aggregates{%s})' % F, instant=True), 0, y,
         "Aggregates that AtScale can use for queries now (latest poll of the aggregates API)."),
    stat("Rows in aggregate tables", prom('sum(atscale_aggregate_rows_total{%s})' % F, instant=True), 4, y,
         "Rows in all active aggregate tables.", unit="short"),
    stat("Not used by any query yet", prom('sum(atscale_aggregates_unused{%s})' % F, instant=True), 8, y,
         "Active aggregates whose query count (AtScale queryUtilization) is 0. AtScale deactivates aggregates that "
         "stay unused for the configured number of days (setting aggregates.maintenance.zeroUtilizationTTL, default 45).",
         color="orange"),
    stat("Models with aggregates", prom('count(count by (env, catalog, model) (atscale_aggregates{%s}))' % F, instant=True), 12, y,
         "Models that have at least one active aggregate."),
    stat("User-defined aggregates", prom('sum(atscale_aggregates{%s,type="user_defined"})' % F, instant=True), 16, y,
         "Aggregates defined in the model (SML) or created by a user. AtScale creates all other aggregates itself."),
    stat("Exporter up", prom('min(atscale_aggregates_exporter_up{env=~"$env"})', instant=True), 20, y,
         "1 = the last poll of every selected AtScale instance succeeded. Always shows the last 5 minutes.",
         time_from="5m", thresholds={"mode": "absolute", "steps": [{"color": "red", "value": None}, {"color": "green", "value": 1}]}),
]
y += 4

# ---- Per catalog and model ---------------------------------------------------------------------------
panels.append(row("Aggregates per catalog and model", y)); y += 1
by_model = "sum by (env, catalog, model) (%s)"
model_targets = [
    prom(by_model % 'atscale_aggregates{%s}' % F, ref="A", instant=True, fmt="table"),
    prom(by_model % 'atscale_aggregates{%s,subtype="demand_defined"}' % F, ref="B", instant=True, fmt="table"),
    prom(by_model % 'atscale_aggregates{%s,subtype="prediction_defined"}' % F, ref="C", instant=True, fmt="table"),
    prom(by_model % 'atscale_aggregates{%s,type="user_defined"}' % F, ref="D", instant=True, fmt="table"),
    prom(by_model % 'atscale_aggregates_unused{%s}' % F, ref="E", instant=True, fmt="table"),
    prom(by_model % 'atscale_aggregate_rows_total{%s}' % F, ref="F", instant=True, fmt="table"),
]
panels.append(table(
    "Active aggregates per catalog and model", model_targets, 0, y, 14, 11,
    "Demand: created because a user query had no matching aggregate. Predicted: created from the query history. "
    "User-defined: defined in the model (SML) or created by a user. Not used yet: no query has used the aggregate.",
    PROM,
    [{"id": "merge", "options": {}},
     {"id": "organize", "options": {"excludeByName": {"Time": True},
                                    "indexByName": {"env": 0, "catalog": 1, "model": 2, "Value #A": 3, "Value #B": 4, "Value #C": 5,
                                                    "Value #D": 6, "Value #E": 7, "Value #F": 8},
                                    "renameByName": {"env": "Instance", "catalog": "Catalog", "model": "Model", "Value #A": "Total",
                                                     "Value #B": "Demand", "Value #C": "Predicted", "Value #D": "User-defined",
                                                     "Value #E": "Not used yet", "Value #F": "Rows"}}}],
    overrides=[{"matcher": {"id": "byName", "options": "Total"},
                "properties": [{"id": "custom.cellOptions", "value": {"type": "gauge", "mode": "basic", "valueDisplayMode": "text"}},
                               {"id": "min", "value": 0}, {"id": "color", "value": {"mode": "fixed", "fixedColor": "#5794F2"}},
                               {"id": "custom.width", "value": 200}]},
               unit("Rows", "short"), width("Instance", 70), width("Catalog", 230), width("Model", 200)],
    sort="Total"))
panels.append({
    "type": "barchart", "title": "Aggregates per catalog and why they exist", "id": nid(), "datasource": PROM,
    "description": "Active aggregates per instance and catalog, split by subtype (why AtScale created them). The table on the left splits them by model.",
    "gridPos": {"h": 11, "w": 10, "x": 14, "y": y},
    "targets": [prom('label_join(sum by (env, catalog, subtype) (atscale_aggregates{%s}), "catalog_in_env", " / ", "env", "catalog")' % F,
                     instant=True, fmt="table")],
    "transformations": [{"id": "groupingToMatrix", "options": {"columnField": "subtype", "rowField": "catalog_in_env", "valueField": "Value"}},
                        {"id": "renameByRegex", "options": {"regex": "catalog_in_env\\\\subtype", "renamePattern": "Catalog"}},
                        {"id": "sortBy", "options": {"sort": [{"field": "demand_defined", "desc": True}]}}],
    "options": {"orientation": "horizontal", "stacking": "normal", "showValue": "never", "barWidth": 0.8,
                "legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}},
    "fieldConfig": {"defaults": {"unit": "short", "decimals": 0, "noValue": "0", "custom": {"fillOpacity": 85}},
                    "overrides": color_overrides({"demand_defined": "blue", "prediction_defined": "purple", "hinted": "green", "manual": "yellow"})}})
y += 11
panels.append(lines("Active aggregates over time", [prom('sum by (env, catalog, model) (atscale_aggregates{%s})' % F, "{{env}} {{catalog}} / {{model}}")],
                    0, y, 12, 9, "Number of active aggregates per model, as the exporter saw it on each poll."))
panels.append(lines("Rows in aggregate tables over time", [prom('sum by (env, catalog, model) (atscale_aggregate_rows_total{%s})' % F, "{{env}} {{catalog}} / {{model}}")],
                    12, y, 12, 9, "Rows in all active aggregate tables per model."))
y += 9

# ---- Lifecycle events (exporter) ----------------------------------------------------------------------
panels.append(row("Lifecycle events and reasons (exporter)", y)); y += 1
count = 'sum(count_over_time(%s | action="%s" [$__range]))'
panels += [
    stat("Created", loki(count % (EVENTS, "created"), instant=True), 0, y,
         "Aggregates that became active for the first time in the selected range.", w=4, ds=LOKI, color="green"),
    stat("Rebuilt", loki(count % (EVENTS, "rebuilt"), instant=True), 4, y,
         "Aggregates that got a new active instance (a new table) in the selected range.", w=4, ds=LOKI, color="blue"),
    stat("Deactivated", loki(count % (EVENTS, "deactivated"), instant=True), 8, y,
         "Aggregates that AtScale stopped using in the selected range. AtScale drops the table later "
         "(setting aggregates.invalid.removalAge, default 2 days).", w=4, ds=LOKI, color="red"),
]
panels.append(bars("Events over time", [loki('sum by (action) (count_over_time(%s [$__interval]))' % EVENTS, legend="{{action}}")],
                   12, y, 12, 8, "Lifecycle events per 10 minutes (or wider on long ranges), from the exporter's event log. A bar appears when its interval has ended.", LOKI, ACTION_COLORS))
reason = 'sum by (reason) (count_over_time(%s | action="%s" [$__range]))'
panels.append(reason_table("Why created", loki(reason % (EVENTS, "created"), legend="{{reason}}", instant=True), 0, y + 4, 4, 8,
                       "query_demand: a user query had no matching aggregate. prediction: predicted from the query history. "
                       "user_defined_in_model: defined in the model (SML). user_defined_manual: created by a user.", ["reason"], {"reason": "Reason"}))
panels.append(reason_table("Why rebuilt", loki(reason % (EVENTS, "rebuilt"), legend="{{reason}}", instant=True), 4, y + 4, 4, 8,
                       "scheduled_batch: a build schedule ran shortly before. on_demand_batch: a batch build started by the API, "
                       "the UI or a trigger file. query_demand: built again outside a batch because a query needed it. "
                       "full or incremental: from the aggregate definition. AtScale stores no trigger, so the exporter derives it.", ["reason"], {"reason": "Reason"}))
panels.append(reason_table("Why deactivated", loki(reason % (EVENTS, "deactivated"), legend="{{reason}}", instant=True), 8, y + 4, 4, 8,
                       "Classified from AtScale's status message of the last instance. unused: not used for the configured "
                       "number of days. replaced_by_rebuild: a batch build replaced it. model_changed: the model no longer "
                       "matches the aggregate. removed_by_user: deleted in the UI or API. deleted_no_reason_logged: AtScale "
                       "deleted it without an earlier status that names a reason.", ["reason"], {"reason": "Reason"}))
y += 12
events_keep = ["time", "action", "reason", "env", "catalog", "model", "aggregate", "subtype", "rows", "build_ms", "message", "aggregate_id", "build_query_id"]
panels.append(table(
    "Event log", [loki(EVENTS)], 0, y, 24, 10,
    "One row per lifecycle event. Time is when AtScale created or built the aggregate; for deactivations it is when the "
    "exporter saw the change. Click an aggregate ID to see its full history in the engine log.", LOKI,
    [{"id": "extractFields", "options": {"source": "Line", "format": "json", "replace": False, "keepTime": False}},
     {"id": "convertFieldType", "options": {"conversions": [{"targetField": "time", "destinationType": "time"}]}},
     {"id": "organize", "options": {"includeByName": {k: True for k in events_keep}, "indexByName": {k: i for i, k in enumerate(events_keep)},
                                    "renameByName": {"time": "Time", "action": "Event", "reason": "Reason", "env": "Instance", "catalog": "Catalog",
                                                     "model": "Model", "aggregate": "Aggregate", "subtype": "Subtype", "rows": "Rows",
                                                     "build_ms": "Build time", "message": "AtScale message", "aggregate_id": "Aggregate ID",
                                                     "build_query_id": "Build query ID"}}}],
    overrides=[cell_colors("Event", ACTION_COLORS), unit("Build time", "ms"), width("Time", 170), width("Event", 100),
               link("Aggregate ID", "History of this aggregate in the engine log",
                    explore_link('$engine_selector |= "${__value.raw}"'))],
    sort="Time"))
y += 10

# ---- Engine log ----------------------------------------------------------------------------------------
panels.append(row("Status history from the engine log (also before the exporter ran)", y)); y += 1
panels.append(text(
    "AtScale writes one engine log line for each status change of an aggregate instance: "
    "`new` → `inprogress` (build) → `done` → `active`, and later `invalid` (deactivated, with the reason) → `deleted` (table dropped). "
    "These panels read those lines, so they show the history as far back as the log store keeps it. "
    "The catalog and model names come from the exporter; IDs that the exporter does not know stay as IDs.", 0, y, 24, 2))
y += 2
panels.append(bars("Status changes over time", [loki('sum by (status) (count_over_time(%s [$__interval]))' % STATUS, legend="{{status}}")],
                   0, y, 14, 9, "Instance status changes per 10 minutes (or wider on long ranges), from the engine log. A bar appears when its interval has ended. active = an aggregate became usable "
                   "(new or rebuilt). invalid = deactivated. deleted = table dropped.", LOKI, STATUS_COLORS))
panels.append(reason_table(
    "Why deactivated or failed (engine log)",
    loki('sum by (status, message) (count_over_time(%s | status=~"invalid|failed" %s [$__range]))' % (STATUS, NORMALIZE), instant=True),
    14, y, 10, 9, "The status message of every change to invalid (deactivated) or failed (build failed), as the engine "
    "logged it. This is the reason. A table drop (deleted) without an earlier invalid line has no logged reason.",
    ["status", "message"], {"status": "Status", "message": "Reason (AtScale message)"}))
panels[-1]["fieldConfig"]["overrides"] += [width("Status", 80), cell_colors("Status", STATUS_COLORS)]
y += 9
status_keep = ["Time", "status", "message", "catalog", "model", "aggregate_id", "instance_id", "key"]
panels.append(table(
    "Status changes", [loki(STATUS + " " + NORMALIZE.replace("<id>", "…") + " " + KEY, ref="A"),
                       prom(NAMES, ref="B", instant=True, fmt="table")],
    0, y, 24, 10, "Every status change of an aggregate instance. Message is the reason AtScale gives (empty for build steps). "
    "Filter a column with the funnel icon, or click an aggregate ID for its full history.", LOKI,
    logs_table_transforms(status_keep, {"status": "Status", "message": "Message", "catalog": "Catalog", "model": "Model",
                                        "aggregate_id": "Aggregate ID", "instance_id": "Instance ID"}, join_names=True),
    overrides=[cell_colors("Status", STATUS_COLORS), width("Time", 170), width("Status", 90),
               {"matcher": {"id": "byName", "options": "key"}, "properties": [{"id": "custom.hidden", "value": True}]},
               link("Aggregate ID", "History of this aggregate", explore_link('$engine_selector |= "${__value.raw}"'))],
    sort="Time"))
panels[-1]["targets"][0]["datasource"] = LOKI
panels[-1]["datasource"] = {"type": "datasource", "uid": "-- Mixed --"}
y += 10
cand_keep = ["Time", "aggregate_id", "query_id", "incidental", "selection"]
panels.append(table(
    "Why created: the query that asked for each new aggregate", [loki(CANDIDATE)], 0, y, 24, 9,
    "AtScale approves an aggregate candidate while it plans a user query that had no matching aggregate. Query ID is that "
    "query. Selection lists the attributes (dimension, key, measure) the aggregate holds. incidental = true: AtScale "
    "added the candidate while it planned another candidate.", LOKI,
    logs_table_transforms(cand_keep, {"aggregate_id": "Aggregate ID", "query_id": "Query ID", "incidental": "Incidental",
                                      "selection": "Attributes in the aggregate"}),
    overrides=[width("Time", 170), width("Aggregate ID", 290), width("Query ID", 290), width("Incidental", 90),
               link("Query ID", "Engine log of this query", explore_link('$engine_selector |= "${__value.raw}"')),
               link("Aggregate ID", "History of this aggregate", explore_link('$engine_selector |= "${__value.raw}"'))],
    sort="Time"))
y += 9

# ---- Prediction reports (engine database) --------------------------------------------------------------
panels.append(row("Why AtScale did not propose or rejected an aggregate (engine database)", y)); y += 1
panels.append(text(
    "AtScale runs prediction rules for each part of a user query that no aggregate answers. A **G** code means the "
    "rule proposed no aggregate. An **R** code means AtScale proposed a candidate and then rejected it. AtScale stores "
    "these decisions in its database; select the instance's database in **Engine database** at the top. "
    "Routine codes (G260, G270, G300, and G250 for preferred store copies) only say that a path does not apply. "
    "They appear on almost every query, so they are hidden unless **Routine codes** is set to show.", 0, y, 24, 3))
y += 3
panels += [
    stat("Query parts predicted", sql(APPROVED % ""), 0, y,
         "Query parts that went through aggregate prediction in the selected range.", w=6, ds=PG),
    stat("With an aggregate proposed", sql(APPROVED % " and json_array_length(r.content->'approvedCandidates') > 0"), 6, y,
         "Query parts for which at least one candidate passed all rules. AtScale then builds it.", w=6, ds=PG, color="green"),
    stat("Not proposed (G)", sql(DECISIONS + "select count(distinct (query_id, part)) from dm where code like 'G%' and " + SHOWN), 12, y,
         "Query parts with at least one G code, without routine codes unless they are shown.", w=6, ds=PG, color="orange"),
    stat("Candidates rejected (R)", sql(DECISIONS + "select count(distinct candidate_id) from dm where code like 'R%'"), 18, y,
         "Candidates that a rejecting rule removed.", w=6, ds=PG, color="red"),
]
y += 4
panels.append(bars("Decisions over time", [sql(DECISIONS + "select $__timeGroupAlias(time, $__interval), code as metric, count(*) as value "
                                                "from dm where %s group by 1, 2 order by 1" % SHOWN, fmt="time_series")],
                   0, y, 24, 8, "G and R decisions per 10 minutes (or wider on long ranges), by code.", PG))
y += 8
reason_cols = {"Code": 60, "Meaning": 330, "Settings": 240, "Count": 95, "Last seen": 170}
for title, prefix, desc in (
        ("Why no aggregate was proposed (G)", "G",
         "Each G reason with how often it occurred. Reason: AtScale's text, with IDs replaced by … and decimals by n. "
         "Settings: the AtScale settings that decided it, with their value."),
        ("Why a candidate was rejected (R)", "R",
         "Each R reason with how often it occurred. R050 and R030 compare the compression score (fact rows / aggregate rows) "
         "with aggregates.create.compression.threshold.")):
    panels.append(table(
        title, [sql(DECISIONS + "select code as \"Code\", meaning as \"Meaning\", reason_group as \"Reason\", settings as \"Settings\", "
                    "count(*) as \"Count\", max(time) as \"Last seen\" from dm where code like '%s%%' and %s "
                    "group by 1, 2, 3, 4 order by 5 desc limit 200" % (prefix, SHOWN))],
        0, y, 24, 8, desc, PG, [], overrides=[width(k, v) for k, v in reason_cols.items()], sort="Count"))
    y += 8
panels.append(table(
    "Prediction decisions per query", [sql(DECISIONS + "select time as \"Time\", model as \"Model\", query_id as \"Query ID\", "
                                           "language as \"Language\", part as \"Query part\", code as \"Code\", meaning as \"Meaning\", "
                                           "reason as \"Reason\", settings as \"Settings\", candidate_id as \"Candidate ID\" "
                                           "from dm where %s order by time desc limit 1000" % SHOWN)],
    0, y, 24, 10, "The latest 1,000 decisions. Click a query ID for the engine log of that query. A candidate that "
    "passed all rules becomes an aggregate with the same ID.", PG, [],
    overrides=[width("Time", 170), width("Model", 160), width("Query ID", 290), width("Language", 80), width("Query part", 150),
               width("Code", 60), width("Meaning", 260), width("Settings", 220), width("Candidate ID", 290),
               link("Query ID", "Engine log of this query", explore_link('$engine_selector |= "${__value.raw}"')),
               link("Candidate ID", "Engine log of this candidate", explore_link('$engine_selector |= "${__value.raw}"'))],
    sort="Time"))
y += 10

# ---- Inventory -----------------------------------------------------------------------------------------
panels.append(row("Aggregate inventory (exporter, AGGREGATE_DETAILS=true)", y)); y += 1
AGG_BY = "max by (env, catalog, model, aggregate_id, aggregate, subtype, table) "
inv = [(AGG_BY + "(atscale_aggregate_created_timestamp_seconds{%s}) * 1000", "A"), (AGG_BY + "(atscale_aggregate_built_timestamp_seconds{%s}) * 1000", "B"),
       (AGG_BY + "(atscale_aggregate_build_duration_seconds{%s})", "C"), (AGG_BY + "(atscale_aggregate_rows{%s})", "D"),
       (AGG_BY + "(atscale_aggregate_queries{%s})", "E"), (AGG_BY + "(atscale_aggregate_last_query_timestamp_seconds{%s}) * 1000 > 0", "F")]
inv_keep = ["env", "catalog", "model", "aggregate", "subtype", "Value #A", "Value #B", "Value #C", "Value #D", "Value #E", "Value #F", "table", "aggregate_id"]
panels.append(table(
    "Active aggregates", [prom(e % F, ref=r, instant=True, fmt="table") for e, r in inv], 0, y, 24, 12,
    "One row per active aggregate. Created: when AtScale created the definition. Last build: when the build of the "
    "active table finished. Queries: how many queries used it (AtScale queryUtilization).", PROM,
    [{"id": "merge", "options": {}},
     {"id": "organize", "options": {"includeByName": {k: True for k in inv_keep}, "indexByName": {k: i for i, k in enumerate(inv_keep)},
                                    "renameByName": {"env": "Instance", "catalog": "Catalog", "model": "Model", "aggregate": "Aggregate",
                                                     "subtype": "Subtype", "Value #A": "Created", "Value #B": "Last build",
                                                     "Value #C": "Build time", "Value #D": "Rows", "Value #E": "Queries",
                                                     "Value #F": "Last used", "table": "Table", "aggregate_id": "Aggregate ID"}}}],
    overrides=[unit("Created", "dateTimeAsIso"), unit("Last build", "dateTimeAsIso"), unit("Last used", "dateTimeAsIso"),
               unit("Build time", "s"), unit("Rows", "locale"), unit("Queries", "locale"),
               width("Instance", 70), width("Subtype", 140), width("Created", 170), width("Last build", 170), width("Last used", 170),
               link("Aggregate ID", "History of this aggregate", explore_link('$engine_selector |= "${__value.raw}"'))],
    sort="Created"))
y += 12

dashboard = {
    "title": "AtScale aggregates: lifecycle", "uid": "atscale-aggregates-lifecycle",
    "description": "When AtScale created, rebuilt and deactivated aggregates, why, and how many each catalog and model has.",
    "tags": ["atscale", "aggregates"], "timezone": "browser", "schemaVersion": 39, "version": 1,
    "time": {"from": "now-7d", "to": "now"}, "refresh": "5m", "editable": True, "graphTooltip": 1,
    "templating": {"list": [
        {"name": "datasource", "label": "Prometheus", "type": "datasource", "query": "prometheus", "current": {}, "hide": 0},
        {"name": "loki", "label": "Loki", "type": "datasource", "query": "loki", "current": {}, "hide": 0},
        variable_query("env", "Instance", "label_values(atscale_aggregates, env)"),
        variable_query("catalog", "Catalog", 'label_values(atscale_aggregates{env=~"$env"}, catalog)'),
        variable_query("model", "Model", 'label_values(atscale_aggregates{env=~"$env",catalog=~"$catalog"}, model)'),
        {"name": "enginedb", "label": "Engine database", "type": "datasource", "query": "grafana-postgresql-datasource",
         "current": {}, "hide": 0,
         "description": "PostgreSQL data source on the AtScale database of one instance, for the prediction reports. Pick the instance you look at."},
        {"name": "routine", "label": "Routine codes", "type": "custom", "query": "hide,show",
         "current": {"text": "hide", "value": "hide"}, "options": [], "hide": 0},
        # IDs of the selected catalogs and models, to filter the engine database (All expands to the full list, no regex).
        dict(variable_query("catalog_id", "Catalog ID", 'label_values(atscale_model_info{env=~"$env",catalog=~"$catalog"}, catalog_id)',
                            hide=2), allValue=None),
        dict(variable_query("model_id", "Model ID",
                            'label_values(atscale_model_info{env=~"$env",catalog=~"$catalog",model=~"$model"}, model_id)',
                            hide=2), allValue=None),
        textbox("event_selector", "Exporter log", '{container="aggregates-exporter"}', hide=2),
        textbox("engine_selector", "Engine log", '{container="engine"}', hide=2),
    ]},
    "annotations": {"list": [
        {"builtIn": 1, "datasource": {"type": "grafana", "uid": "-- Grafana --"}, "enable": True, "hide": True,
         "iconColor": "rgba(0, 211, 255, 1)", "name": "Annotations & Alerts", "type": "dashboard"},
        {"name": "Batch builds", "enable": True, "iconColor": "purple", "datasource": LOKI,
         "expr": '$engine_selector |= "Aggregate Batch - Creating Batch"', "titleFormat": "Aggregate batch build", "textFormat": "{{__line__}}"},
    ]},
    "panels": panels,
}

for out in OUTPUTS:
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(dashboard, indent=2) + "\n")
    print("wrote", out.relative_to(ROOT))
