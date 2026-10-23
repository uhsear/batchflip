#!/usr/bin/env python3
"""batchflip - swap a hosted table's contents without ever emptying it, with preconditions.

The usual nightly job empties a hosted table and appends the new rows. For minutes
every reader sees an empty or half-filled table, and when the append fails the table
stays empty until someone notices.

batchflip keeps old and new rows side by side. Every row carries a batch id and a
batch state. The tool does three things, in this order:

    1. stage    add the new rows as a new batch, state 'staged' (readers ignore them)
    2. promote  set the new rows to 'live'
    3. retire   set the previous live rows to 'retired'

Readers filter on the state column (batch_state = 'live'), so they see one complete
batch. When the new and old rows together fit in one chunk (--chunk, default 500),
promote and retire go out in one applyEdits call, which is the one visible step. Above
one chunk the tool promotes first and retires second, so a reader can briefly see both
batches (duplicates), never an empty table.

Before it writes anything the tool runs the overwrite preconditions (the ovwcheck
checks). It refuses when the layer is a view, has sync enabled, lacks an edit
capability, lacks the two batch columns, or when the source schema, value types,
string lengths or geometry type do not fit the layer. It also reads every ArcGIS REST
reply for an error object and for per-row success flags, so an error dictionary can
never be logged as success.

    python batchflip.py --self-test
    python batchflip.py --layer-url https://host/arcgis/rest/services/X/FeatureServer/0 --source rows.json
    python batchflip.py --layer-url ... --source rows.csv --apply
    python batchflip.py --layer-url ... --recover 20260101T020000Z --expect-rows 480 --apply

Dry run by default. Only --apply writes. The token comes from the environment
(ARCGIS_TOKEN by default), never from the command line.

Exit codes: 0 done or check passed, 1 refused before any write, 3 the apply failed part way,
64 usage error. (argparse itself exits 2 on an unknown or abbreviated flag.)
"""

from __future__ import print_function

import argparse
import collections
import csv
import datetime
import http.client
import io
import json
import math
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

__version__ = "0.1.0"

EXIT_OK = 0
EXIT_REFUSED = 1
EXIT_FAILED = 3
EXIT_USAGE = 64

STAGED = "staged"
LIVE = "live"
RETIRED = "retired"
STATES = (STAGED, LIVE, RETIRED)

DEFAULT_BATCH_FIELD = "batch_id"
DEFAULT_STATE_FIELD = "batch_state"
DEFAULT_CHUNK = 500
DEFAULT_PAGE = 1000
BATCH_ID_MAX = 40
STATE_MIN_LENGTH = max(len(s) for s in STATES)

REQUIRED_CAPS = ("Query", "Create", "Update", "Delete")
# Esri documents the property as supportsRollbackOnFailureParameter; its own page
# also shows supportsRollbackOnFailures. Either one set to false is noted.
ROLLBACK_KEYS = ("supportsRollbackOnFailureParameter", "supportsRollbackOnFailures")

T_STRING = "esriFieldTypeString"
T_DATE = "esriFieldTypeDate"
T_INT = frozenset(["esriFieldTypeSmallInteger", "esriFieldTypeInteger",
                   "esriFieldTypeBigInteger"])
T_FLOAT = frozenset(["esriFieldTypeSingle", "esriFieldTypeDouble"])

GEOMETRY_KEYS = (("x", "esriGeometryPoint"), ("points", "esriGeometryMultipoint"),
                 ("paths", "esriGeometryPolyline"), ("rings", "esriGeometryPolygon"))

# \Z, not $: $ also matches before a trailing newline.
IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}\Z")
BATCH_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,%d}\Z" % (BATCH_ID_MAX - 1))
LAYER_PATH_RE = re.compile(r"/FeatureServer/[0-9]+/?$")
SERVICE_RE = re.compile(r"/[0-9]+$")
LOOPBACK = ("127.0.0.1", "localhost", "::1")


class BatchflipError(Exception):
    """Base class, so one except clause covers every expected failure."""


class SourceError(BatchflipError):
    pass


class RestError(BatchflipError):
    pass


class FlipError(BatchflipError):
    """A write failed part way. `stage` names where, for the exit message."""

    def __init__(self, stage, message):
        BatchflipError.__init__(self, message)
        self.stage = stage


Layer = collections.namedtuple("Layer", "url token oid batch state chunk page")


# ----------------------------------------------------------------- the one socket

def http_json(url, params, retries):
    """POST form-encoded params and return the parsed JSON. The only socket in this file."""
    body = urllib.parse.urlencode(params, doseq=True).encode("ascii")
    last = None
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, data=body, method="POST")
            req.add_header("User-Agent", "batchflip/" + __version__)
            with urllib.request.urlopen(req, timeout=60) as resp:
                text = resp.read().decode("utf-8", "replace")
            return json.loads(text)
        except (OSError, ValueError, http.client.HTTPException) as exc:
            # HTTPException covers IncompleteRead (a reply cut off mid-body) and InvalidURL.
            last = exc
            if attempt < retries:
                _SLEEP(2 * attempt)
    raise RestError("HTTP call failed after %d attempt(s): %s (%s)" % (retries, url, last))


_HTTP = http_json
_SLEEP = time.sleep
_NOW = lambda: datetime.datetime.now(datetime.timezone.utc)  # noqa: E731


def call(url, params, token, write=False):
    """Every REST call goes through here. Never logs params, which hold the token.

    A write is sent once. Retrying an applyEdits that timed out can add every row twice.
    """
    payload = dict(params)
    payload["f"] = "json"
    if token:
        payload["token"] = token
    resp = _HTTP(url, payload, 1 if write else 3)
    if not isinstance(resp, dict):
        raise RestError("reply from %s was not a JSON object" % url)
    if "error" in resp:
        err = resp["error"] if isinstance(resp["error"], dict) else {}
        raise RestError("ArcGIS REST error %s: %s"
                        % (err.get("code"), err.get("message") or err.get("description")))
    return resp


# ------------------------------------------------------------------- pure core

def chunks(values, size):
    if size < 1:
        raise ValueError("chunk size must be >= 1")
    return [values[i:i + size] for i in range(0, len(values), size)]


def url_problem(url):
    """Why this layer URL is unsafe or wrong, or None."""
    parts = urllib.parse.urlsplit(url)
    if parts.scheme not in ("http", "https") or not parts.hostname:
        return "layer URL must be http(s)://host/.../FeatureServer/<id>"
    if parts.scheme == "http" and parts.hostname not in LOOPBACK:
        return "layer URL uses plain http, which would send the token in the clear"
    if "@" in parts.netloc:
        return "layer URL must not carry a user name or password (it would be logged)"
    if parts.query or parts.fragment:
        return "layer URL must not carry a query string or fragment (a token there would be logged)"
    if not LAYER_PATH_RE.search(parts.path):
        return "layer URL must end in /FeatureServer/<layer id>"
    return None


def sql_str(value):
    return "'" + value.replace("'", "''") + "'"


def where_state(layer, state):
    return "%s = %s" % (layer.state, sql_str(state))


def where_batch(layer, batch_id, state):
    return "%s = %s AND %s = %s" % (layer.batch, sql_str(batch_id),
                                    layer.state, sql_str(state))


def where_batch_any(layer, batch_id):
    return "%s = %s" % (layer.batch, sql_str(batch_id))


def where_invalid(layer):
    return "%s IS NULL OR %s NOT IN (%s)" % (
        layer.state, layer.state, ", ".join(sql_str(s) for s in STATES))


def default_batch_id(now):
    return now.strftime("%Y%m%dT%H%M%SZ")


def geometry_kind(geom):
    """The esriGeometry* name of an Esri JSON geometry, or None when unrecognised."""
    if isinstance(geom, dict):
        for key, kind in GEOMETRY_KEYS:
            if key in geom:
                return kind
    return None


def _wkids(sr):
    """The well-known ids (wkid and latestWkid) that a spatialReference object names."""
    if not isinstance(sr, dict):
        return set()
    return set(sr[k] for k in ("wkid", "latestWkid")
               if isinstance(sr.get(k), int) and not isinstance(sr.get(k), bool))


def layer_srs(props):
    """The layer's spatial reference object, or None when the layer JSON names none.

    Esri shows it inside `extent` on the Feature layer page.
    """
    for holder in (props.get("extent"), props):
        sr = holder.get("spatialReference") if isinstance(holder, dict) else None
        if isinstance(sr, dict) and sr:
            return sr
    return None


def sr_same(a, b):
    """True when two spatialReference objects are the same system. Compares wkid and
    latestWkid (102100 and 3857 share latestWkid 3857), then wkt text when neither has an id."""
    ids_a, ids_b = _wkids(a), _wkids(b)
    if ids_a and ids_b:
        return bool(ids_a & ids_b)
    wkt = a.get("wkt") if isinstance(a, dict) else None
    return isinstance(wkt, str) and bool(wkt) and wkt == b.get("wkt")


def sr_text(sr):
    ids = sorted(_wkids(sr))
    if ids:
        return "wkid " + "/".join(str(i) for i in ids)
    return "wkt" if isinstance(sr, dict) and sr.get("wkt") else "an unreadable spatial reference"


def _reject_constant(name):
    raise ValueError("%s is not valid JSON for a feature service" % name)


def _parse_json(text):
    try:
        doc = json.loads(text, parse_constant=_reject_constant)
    except ValueError as exc:
        raise SourceError("source is not valid JSON: %s" % exc)
    items = doc.get("features") if isinstance(doc, dict) else doc
    if not isinstance(items, list):
        raise SourceError("JSON source must be a list or an object with a 'features' list")
    if isinstance(doc, dict) and doc.get("exceededTransferLimit") is True:
        raise SourceError("source says exceededTransferLimit is true: it is a cut-off query "
                          "result, and swapping it in would drop the rows that were not returned")
    # An envelope spatialReference covers every geometry that names none of its own.
    env_sr = doc.get("spatialReference") if isinstance(doc, dict) else None
    rows, geoms = [], []
    for number, item in enumerate(items, 1):
        if not isinstance(item, dict):
            raise SourceError("row %d is not an object" % number)
        if "attributes" in item:
            attrs, geom = item["attributes"], item.get("geometry")
        else:
            attrs, geom = item, None
        if not isinstance(attrs, dict):
            raise SourceError("row %d: attributes is not an object" % number)
        if env_sr is not None and isinstance(geom, dict) and "spatialReference" not in geom:
            geom = dict(geom, spatialReference=env_sr)
        rows.append(attrs)
        geoms.append(geom)
    return rows, geoms


def _parse_csv(text):
    reader = csv.DictReader(io.StringIO(text, newline=""))
    names = reader.fieldnames
    if not names:
        raise SourceError("CSV source has no header row")
    if len(set(n.lower() for n in names)) != len(names):
        raise SourceError("CSV header repeats a column name")
    rows = []
    try:
        for number, row in enumerate(reader, 1):
            if None in row or None in row.values():
                raise SourceError("CSV row %d has a different number of cells than the header"
                                  % number)
            rows.append(dict(row))
    except csv.Error as exc:
        raise SourceError("CSV source cannot be read: %s" % exc)
    return rows, [None] * len(rows)


def parse_source(text, kind):
    """(attribute dicts, geometries) from source text. kind is 'json' or 'csv'."""
    return _parse_csv(text) if kind == "csv" else _parse_json(text)


def read_source(path):
    kind = {".csv": "csv", ".json": "json"}.get(os.path.splitext(path)[1].lower())
    if kind is None:
        raise SourceError("source must be a .json (Esri features) or .csv file")
    try:
        with io.open(path, encoding="utf-8-sig", newline="") as handle:
            text = handle.read()
    except (OSError, ValueError) as exc:
        raise SourceError("cannot read source %s: %s" % (path, exc))
    rows, geoms = parse_source(text, kind)
    return rows, geoms, kind


def _by_name(props):
    out = {}
    for field in props.get("fields") or []:
        if isinstance(field, dict) and isinstance(field.get("name"), str):
            out[field["name"].lower()] = field
    return out


def layer_checks(props, batch_field, state_field, allow_sync, service=None):
    """The overwrite preconditions that need only the layer JSON and the service JSON.

    Esri documents syncEnabled on the feature service, not on the layer, so `service`
    is the service root reply. A layer reply that carries it is honoured too.

    Returns (refusals, warnings). Each refusal stops the run before any write.
    """
    refusals, warnings = [], []
    by_name = _by_name(props)
    if not by_name:
        return ["layer metadata has no fields list"], warnings
    if props.get("type") not in ("Feature Layer", "Table"):
        refusals.append("layer type is %r, not Feature Layer or Table" % (props.get("type"),))
    caps = set(c.strip() for c in str(props.get("capabilities") or "").split(","))
    missing = [c for c in REQUIRED_CAPS if c not in caps]
    if missing:
        refusals.append("layer capabilities lack: %s" % ", ".join(missing))
    if props.get("isView") is True:
        refusals.append("layer is a view; Esri says a view cannot be overwritten, so this tool "
                        "refuses it as a precaution")
    synced = props.get("syncEnabled") is True or (service or {}).get("syncEnabled") is True
    if synced and not allow_sync:
        refusals.append("sync is enabled on the layer; every flip would reach every replica "
                        "(pass --allow-sync to accept that)")
    for key in ("supportsPagination", "supportsAdvancedQueries"):
        if props.get(key) is False:
            refusals.append("layer reports %s=false, so the paged read cannot be trusted" % key)
    if any(props.get(key) is False for key in ROLLBACK_KEYS):
        warnings.append("layer does not honour rollbackOnFailure; Esri says the service then "
                        "treats it as true; the post-flip count check still runs")
    oid = props.get("objectIdField")
    if not (isinstance(oid, str) and oid.lower() in by_name):
        refusals.append("layer has no usable objectIdField")
    for role, name, need in (("batch", batch_field, BATCH_ID_MAX),
                             ("state", state_field, STATE_MIN_LENGTH)):
        field = by_name.get(name.lower())
        if field is None:
            refusals.append("layer has no %s column named %s; add a text column of length %d "
                            "or more" % (role, name, need))
            continue
        if field.get("type") != T_STRING:
            refusals.append("%s column %s is not a text field" % (role, name))
        length = field.get("length")
        if isinstance(length, int) and length < need:
            refusals.append("%s column %s has length %d; it needs %d or more"
                            % (role, name, length, need))
        if field.get("editable") is False:
            refusals.append("%s column %s is not editable" % (role, name))
    return refusals, warnings


def _finite(value):
    """True when every number inside value is finite. A huge int counts as not finite."""
    if isinstance(value, float):
        return math.isfinite(value)
    if isinstance(value, int):
        try:
            return math.isfinite(value)
        except OverflowError:
            return False
    if isinstance(value, (list, tuple)):
        return all(_finite(v) for v in value)
    if isinstance(value, dict):
        return all(_finite(v) for v in value.values())
    return True


def coerce_cell(value, field, csv_mode):
    """(value, None) when the cell fits the field, (None, reason) when it does not."""
    ftype = field.get("type")
    if csv_mode:
        if value == "":
            value = None
        elif ftype in T_INT or ftype == T_DATE:
            try:
                value = int(value)
            except ValueError:
                return None, "not a whole number"
        elif ftype in T_FLOAT:
            try:
                value = float(value)
            except ValueError:
                return None, "not a number"
    if value is None:
        if field.get("nullable") is False:
            return None, "null in a field that does not allow null"
        return None, None
    if ftype in T_INT or ftype == T_DATE:
        if isinstance(value, bool) or not isinstance(value, int):
            return None, "not a whole number"
    elif ftype in T_FLOAT:
        if isinstance(value, bool) or not isinstance(value, (int, float)) \
                or not _finite(value):
            return None, "not a finite number"
    elif ftype == T_STRING:
        if not isinstance(value, str):
            return None, "not text"
        length = field.get("length")
        if isinstance(length, int) and length > 0 and len(value) > length:
            return None, "longer than the field length"
    return value, None


def prepare_rows(props, rows, geoms, batch_field, state_field, csv_mode, allow_empty,
                 allow_no_geometry=False, allow_unstated_srs=False):
    """Compare the source with the layer. Returns (problems, clean) where clean is a
    list of (attributes keyed by the layer's own field names, geometry or None)."""
    if not rows:
        if allow_empty:
            return [], []
        return ["source has no rows; swapping in nothing would empty the table "
                "(pass --allow-empty if that is the intent)"], []
    by_name = _by_name(props)
    reserved = set([batch_field.lower(), state_field.lower()])
    columns = []
    for row in rows:
        for col in row:
            if col not in columns:
                columns.append(col)
    problems = []
    if len(set(c.lower() for c in columns)) != len(columns):
        problems.append("source has two columns that differ only by case")
    plan = {}
    buckets = {"reserved": [], "unknown": [], "locked": []}
    for col in columns:
        field = by_name.get(col.lower())
        if col.lower() in reserved:
            buckets["reserved"].append(col)
        elif field is None:
            buckets["unknown"].append(col)
        elif field.get("editable") is False:
            buckets["locked"].append(col)
        else:
            plan[col] = field
    if buckets["reserved"]:
        problems.append("source carries the batch column(s) %s; the tool sets them"
                        % ", ".join(buckets["reserved"]))
    if buckets["unknown"]:
        problems.append("source column(s) not in the layer: %s" % ", ".join(buckets["unknown"]))
    if buckets["locked"]:
        problems.append("source column(s) the layer marks not editable: %s"
                        % ", ".join(buckets["locked"]))
    present = set(c.lower() for c in columns)
    required = [f["name"] for f in by_name.values()
                if f.get("editable") is not False and f.get("nullable") is False
                and f.get("defaultValue") is None and f["name"].lower() not in reserved
                and f["name"].lower() not in present]
    if required:
        problems.append("layer needs a value for field(s) the source lacks: %s"
                        % ", ".join(sorted(required)))
    bad = {}
    clean = []
    for number, row in enumerate(rows, 1):
        attrs = {}
        for col, field in plan.items():
            value, reason = coerce_cell(row.get(col), field, csv_mode)
            if reason is None:
                attrs[field["name"]] = value
            else:
                entry = bad.setdefault((col, reason), [0, number])
                entry[0] += 1
        clean.append((attrs, geoms[number - 1]))
    for (col, reason), (count, first) in sorted(bad.items()):
        problems.append("column %s: %d value(s) %s (first at row %d)" % (col, count, reason, first))
    layer_kind = props.get("geometryType")
    layer_sr = layer_srs(props)
    unknown_geo = mismatch = missing = nonfinite = unstated = 0
    wrong_sr = set()
    wrong_sr_count = 0
    for _attrs, geom in clean:
        if geom is None:
            missing += 1
            continue
        kind = geometry_kind(geom)
        if kind is None:
            unknown_geo += 1
        elif kind != layer_kind:
            mismatch += 1
        elif not _finite(geom):
            nonfinite += 1
        elif layer_sr is not None:
            sr = geom.get("spatialReference")
            if sr is None:
                unstated += 1
            elif not sr_same(sr, layer_sr):
                wrong_sr_count += 1
                wrong_sr.add(sr_text(sr))
    if missing and layer_kind and not allow_no_geometry:
        problems.append("%d row(s) have no geometry, and the layer holds %s; a CSV has none "
                        "(pass --allow-no-geometry to load them anyway)" % (missing, layer_kind))
    if nonfinite:
        problems.append("%d geometry value(s) hold a number that is not finite" % nonfinite)
    if unknown_geo:
        problems.append("%d geometry value(s) are not Esri JSON point, multipoint, "
                        "polyline or polygon" % unknown_geo)
    if mismatch:
        problems.append("%d geometry value(s) do not match the layer's %s"
                        % (mismatch, layer_kind or "table (no geometry)"))
    if wrong_sr_count:
        problems.append("%d geometry value(s) use %s, but the layer uses %s; the tool does not "
                        "reproject" % (wrong_sr_count, ", ".join(sorted(wrong_sr)),
                                       sr_text(layer_sr)))
    if unstated and not allow_unstated_srs:
        problems.append("%d geometry value(s) name no spatial reference, and the layer uses %s; "
                        "coordinates in another system would load unchanged (pass "
                        "--allow-unstated-srs if they already use it)"
                        % (unstated, sr_text(layer_sr)))
    return problems, clean


def stage_features(clean, layer, batch_id):
    """applyEdits 'adds' payload: the clean rows plus the two batch columns."""
    out = []
    for attrs, geom in clean:
        item = {"attributes": dict(attrs)}
        item["attributes"][layer.batch] = batch_id
        item["attributes"][layer.state] = STAGED
        if geom is not None:
            item["geometry"] = geom
        out.append(item)
    return out


def state_refusals(live_rows, layer, staged_count, invalid_count, collision_count,
                   discard_staged):
    """Refusals that come from what the table holds right now."""
    refusals = []
    ids = sorted(set(str(r.get(layer.batch) or "(blank)") for r in live_rows))
    if len(ids) > 1:
        refusals.append("%d batches are live at once (%s); readers already see duplicates. "
                        "Run --recover with the batch to keep" % (len(ids), ", ".join(ids[:3])))
    if invalid_count:
        refusals.append("%d row(s) have no valid %s; set them to 'live' with a batch id "
                        "before the first run" % (invalid_count, layer.state))
    if staged_count and not discard_staged:
        refusals.append("%d staged row(s) are left from an earlier run; pass --discard-staged "
                        "to remove them. Use --recover BATCH --expect-rows N only when you know "
                        "that batch is complete; a batch whose staging failed is not"
                        % staged_count)
    if collision_count:
        refusals.append("batch id already exists in the table (%d row(s))" % collision_count)
    return refusals


def plan_flip(new_oids, old_oids, layer):
    """The ordered applyEdits update calls: [(label, updates)]."""
    promote = [{"attributes": {layer.oid: o, layer.state: LIVE}} for o in new_oids]
    retire = [{"attributes": {layer.oid: o, layer.state: RETIRED}} for o in old_oids]
    if not promote and not retire:
        return []
    if len(promote) + len(retire) <= layer.chunk:
        return [("promote and retire in one call", promote + retire)]
    steps = []
    for index, part in enumerate(chunks(promote, layer.chunk), 1):
        steps.append(("promote chunk %d" % index, part))
    for index, part in enumerate(chunks(retire, layer.chunk), 1):
        steps.append(("retire chunk %d" % index, part))
    return steps


def _result_error(result):
    if not isinstance(result, dict):
        return "not an object"
    err = result.get("error")
    if isinstance(err, dict):
        return "%s %s" % (err.get("code"), err.get("description") or err.get("message"))
    return "success flag is %r" % (result.get("success"),)


def edit_failures(resp, key, expected, need_oid=False):
    """Why an applyEdits or deleteFeatures reply is not a clean success. [] means clean.

    HTTP 200 carries both a top-level error object and per-row failures, so the reply
    is trusted only after every result is checked.
    """
    results = resp.get(key)
    if not isinstance(results, list):
        return ["reply has no %s list, so no row is confirmed" % key]
    problems = []
    if len(results) != expected:
        problems.append("expected %d %s, got %d" % (expected, key, len(results)))
    for index, result in enumerate(results):
        if not isinstance(result, dict) or result.get("success") is not True:
            problems.append("%s[%d] failed: %s" % (key, index, _result_error(result)))
        elif need_oid and (isinstance(result.get("objectId"), bool)
                           or not isinstance(result.get("objectId"), int)):
            problems.append("%s[%d] has no objectId" % (key, index))
    return problems[:5]


def oid_list(rows, oid_field):
    oids = [r.get(oid_field) for r in rows]
    if any(isinstance(o, bool) or not isinstance(o, int) for o in oids):
        raise RestError("a row came back without an integer %s" % oid_field)
    return oids


def page_size(props):
    value = props.get("maxRecordCount")
    if isinstance(value, int) and not isinstance(value, bool) and value > 0:
        return value
    return DEFAULT_PAGE


def arg_problems(args):
    """Usage errors, as a list of messages. Pure."""
    out = []
    problem = url_problem(args.layer_url or "")
    if problem:
        out.append(problem)
    for flag, name in (("--batch-field", args.batch_field), ("--state-field", args.state_field)):
        if not IDENT_RE.match(name):
            out.append("%s must be a plain field name" % flag)
    if args.batch_field.lower() == args.state_field.lower():
        out.append("--batch-field and --state-field must differ")
    if args.chunk < 1:
        out.append("--chunk must be 1 or more")
    if bool(args.source) == bool(args.recover):
        out.append("give exactly one of --source and --recover")
    if args.expect_rows is not None and not args.recover:
        out.append("--expect-rows goes with --recover only")
    if args.recover:
        if args.expect_rows is None or args.expect_rows < 1:
            out.append("--recover needs --expect-rows N, the row count of the whole batch "
                       "(N is 1 or more)")
        if not BATCH_RE.match(args.recover):
            out.append("--recover needs a batch id of letters, digits, '_', '.', '-'")
        if args.batch_id or args.discard_staged or args.allow_empty:
            out.append("--recover cannot be combined with --batch-id, --discard-staged "
                       "or --allow-empty")
    if args.batch_id and not BATCH_RE.match(args.batch_id):
        out.append("--batch-id must be 1 to %d letters, digits, '_', '.', '-'" % BATCH_ID_MAX)
    return out


# --------------------------------------------------------------------- REST I/O

def fetch_count(layer, where):
    resp = call(layer.url + "/query", {"where": where, "returnCountOnly": "true"}, layer.token)
    count = resp.get("count")
    if isinstance(count, bool) or not isinstance(count, int):
        raise RestError("count query returned no integer count")
    return count


def fetch_rows(layer, where, out_fields):
    """Attribute dicts for every row matching where, paged, then reconciled to the count.

    A layer can cut a page short without saying so, so the total is compared with a
    separate count query. A mismatch stops the run.
    """
    expected = fetch_count(layer, where)
    rows = []
    while len(rows) < expected:
        resp = call(layer.url + "/query",
                    {"where": where, "outFields": ",".join(out_fields),
                     "returnGeometry": "false", "orderByFields": layer.oid,
                     "resultOffset": len(rows), "resultRecordCount": layer.page},
                    layer.token)
        feats = resp.get("features")
        if not isinstance(feats, list) or not feats:
            break
        rows.extend(dict(f.get("attributes") or {}) for f in feats if isinstance(f, dict))
    if len(rows) != expected:
        raise RestError("read %d row(s) but the count query says %d; the layer changed under "
                        "the read or a page was cut short" % (len(rows), expected))
    return rows


def apply_edits(layer, key, items, need_oid=False):
    resp = call(layer.url + "/applyEdits",
                {key: json.dumps(items, allow_nan=False), "rollbackOnFailure": "true"},
                layer.token, write=True)
    result_key = key[:-1] + "Results"
    problems = edit_failures(resp, result_key, len(items), need_oid)
    if problems:
        raise RestError("; ".join(problems))
    return resp[result_key]


def delete_rows(layer, where):
    """Delete the rows matching where, by object id so each one is confirmed."""
    oids = oid_list(fetch_rows(layer, where, [layer.oid]), layer.oid)
    for part in chunks(oids, layer.chunk):
        resp = call(layer.url + "/deleteFeatures",
                    {"objectIds": ",".join(str(o) for o in part), "rollbackOnFailure": "true"},
                    layer.token, write=True)
        problems = edit_failures(resp, "deleteResults", len(part))
        if problems:
            raise RestError("; ".join(problems))
    return len(oids)


def stage_rows(layer, features):
    oids = []
    for part in chunks(features, layer.chunk):
        oids.extend(r["objectId"] for r in apply_edits(layer, "adds", part, need_oid=True))
    return oids


def _abandon(layer, batch_id, stage, exc):
    """Remove a half-staged batch (readers never saw it) and build the FlipError."""
    if not isinstance(exc, BatchflipError):
        exc = "%s: %s" % (type(exc).__name__, exc)
    try:
        gone = delete_rows(layer, where_batch(layer, batch_id, STAGED))
        tail = "removed %d staged row(s) of the failed batch %s" % (gone, batch_id)
    except BatchflipError as exc2:
        tail = ("cleanup failed too (%s); the staged rows of batch %s are incomplete, so run "
                "with --discard-staged and do not use --recover on them" % (exc2, batch_id))
    return FlipError(stage, "%s; %s" % (exc, tail))


def finish_flip(layer, batch_id, new_oids, old_oids, batch_rows, log):
    """Promote new_oids, retire old_oids, then check that batch_rows rows are live."""
    for label, updates in plan_flip(new_oids, old_oids, layer):
        try:
            apply_edits(layer, "updates", updates)
        except BatchflipError as exc:
            raise FlipError("flip", "%s failed: %s. Readers may see both batches. Finish with "
                            "--recover %s --expect-rows %d" % (label, exc, batch_id, batch_rows))
        log("  %s: %d row(s)" % (label, len(updates)))
    try:
        live = fetch_count(layer, where_state(layer, LIVE))
    except BatchflipError as exc:
        raise FlipError("verify", "could not count live rows after the flip: %s" % exc)
    if live != batch_rows:
        raise FlipError("verify", "expected %d live row(s) after the flip, found %d"
                        % (batch_rows, live))
    log("  verified: %d live row(s)" % live)


def purge_retired(layer, log):
    try:
        gone = delete_rows(layer, where_state(layer, RETIRED))
    except BatchflipError as exc:
        raise FlipError("purge", "the flip is done but removing retired rows failed: %s" % exc)
    log("  purged %d retired row(s)" % gone)
    return gone


def swap(layer, features, batch_id, old_oids, discard_staged, purge, log):
    if discard_staged:
        try:
            gone = delete_rows(layer, where_state(layer, STAGED))
        except BatchflipError as exc:
            raise FlipError("discard", "removing the staged leftovers failed: %s. Some of them "
                            "may already be gone. Nothing new was staged" % exc)
        log("  discarded %d staged row(s) from earlier runs" % gone)
    try:
        new_oids = stage_rows(layer, features)
        staged = fetch_count(layer, where_batch(layer, batch_id, STAGED))
        if staged != len(features):
            raise RestError("staged %d row(s) but the layer holds %d" % (len(features), staged))
    except Exception as exc:  # any failure here leaves a half-staged batch to remove
        raise _abandon(layer, batch_id, "stage", exc)
    log("  staged %d row(s) as batch %s" % (len(new_oids), batch_id))
    finish_flip(layer, batch_id, new_oids, old_oids, len(new_oids), log)
    if purge:
        purge_retired(layer, log)


# ------------------------------------------------------------------------ runner

def _report(lines, stream=None):
    for line in lines:
        print(line, file=stream or sys.stdout)


def _guarded(fn, *args):
    """Run an apply step. Once writes may have started, no failure may end as exit 1
    ('refused, nothing changed'), so an unexpected exception becomes a FlipError."""
    try:
        return fn(*args)
    except BatchflipError:
        raise
    except Exception as exc:
        raise FlipError("unexpected", "%s: %s. The table may be partly changed; look at it "
                        "before you run again" % (type(exc).__name__, exc))


def run(args, log=print):
    problems = arg_problems(args)
    token = (os.environ.get(args.token_env) or "").strip()
    if args.apply and not token:
        # Never echo args.token_env: a caller may have passed the token itself.
        problems.append("--apply needs a token in the environment variable that --token-env "
                        "names (default ARCGIS_TOKEN)")
    if problems:
        _report(["error: " + p for p in problems], sys.stderr)
        return EXIT_USAGE
    try:
        return _run(args, token, log)
    except FlipError as exc:
        _report(["FAILED at %s: %s" % (exc.stage, exc)], sys.stderr)
        return EXIT_FAILED
    except BatchflipError as exc:
        _report(["refused: %s" % exc, "Nothing was changed."], sys.stderr)
        return EXIT_REFUSED


def _open_layer(args, token, log):
    """Read the layer JSON, run the layer preconditions, return (layer, props)."""
    props = call(args.layer_url, {}, token)
    service = call(SERVICE_RE.sub("", args.layer_url.rstrip("/")), {}, token)
    refusals, warnings = layer_checks(props, args.batch_field, args.state_field,
                                      args.allow_sync, service)
    if refusals:
        raise BatchflipError("; ".join(refusals))
    by_name = _by_name(props)
    layer = Layer(args.layer_url.rstrip("/"), token, props["objectIdField"],
                  by_name[args.batch_field.lower()]["name"],
                  by_name[args.state_field.lower()]["name"], args.chunk, page_size(props))
    if props.get("geometryType") and layer_srs(props) is None:
        warnings.append("layer JSON names no spatial reference, so the coordinates in the "
                        "source cannot be compared with the layer")
    for line in warnings:
        log("warning: " + line)
    return layer, props


def _run(args, token, log):
    layer, props = _open_layer(args, token, log)
    mode = "APPLY" if args.apply else "check only"
    log("batchflip %s (%s)" % (__version__, mode))
    log("layer: %s" % layer.url)
    live_rows = fetch_rows(layer, where_state(layer, LIVE), [layer.oid, layer.batch])
    staged_count = fetch_count(layer, where_state(layer, STAGED))
    log("live now: %d row(s); staged leftovers: %d" % (len(live_rows), staged_count))
    if args.recover:
        return _recover(args, layer, live_rows, log)
    rows, geoms, kind = read_source(args.source)
    problems, clean = prepare_rows(props, rows, geoms, layer.batch, layer.state,
                                   kind == "csv", args.allow_empty, args.allow_no_geometry,
                                   args.allow_unstated_srs)
    batch_id = args.batch_id or default_batch_id(_NOW())
    problems += state_refusals(
        live_rows, layer, staged_count, fetch_count(layer, where_invalid(layer)),
        fetch_count(layer, where_batch_any(layer, batch_id)), args.discard_staged)
    if problems:
        _report(["REFUSED: " + p for p in problems])
        log("Nothing was changed.")
        return EXIT_REFUSED
    log("source: %d row(s), batch id %s" % (len(clean), batch_id))
    if not args.apply:
        log("Check passed. The layer was not changed.")
        log("Re-run with --apply to stage %d row(s) and retire %d live row(s)."
            % (len(clean), len(live_rows)))
        return EXIT_OK
    log("=== APPLY ===")
    _guarded(swap, layer, stage_features(clean, layer, batch_id), batch_id,
             oid_list(live_rows, layer.oid), args.discard_staged, args.purge, log)
    log("Done.")
    return EXIT_OK


def _recover_apply(args, layer, staged_rows, old, log):
    finish_flip(layer, args.recover, oid_list(staged_rows, layer.oid), oid_list(old, layer.oid),
                args.expect_rows, log)
    if args.purge:
        purge_retired(layer, log)


def _recover(args, layer, live_rows, log):
    keep = args.recover
    staged_rows = fetch_rows(layer, where_batch(layer, keep, STAGED), [layer.oid])
    live_keep = [r for r in live_rows if r.get(layer.batch) == keep]
    old = [r for r in live_rows if r.get(layer.batch) != keep]
    if not staged_rows and not live_keep:
        _report(["REFUSED: batch %s has no staged or live rows in this layer" % keep])
        return EXIT_REFUSED
    held = len(staged_rows) + len(live_keep)
    if held != args.expect_rows:
        # A batch whose staging failed is incomplete; promoting it would publish a partial table.
        _report(["REFUSED: batch %s holds %d row(s) (%d staged, %d live) but --expect-rows says "
                 "%d; a batch that is not complete must not be recovered (use --discard-staged "
                 "on the next run)" % (keep, held, len(staged_rows), len(live_keep),
                                       args.expect_rows)])
        return EXIT_REFUSED
    log("recover batch %s: %d staged to promote, %d live batch row(s) to retire"
        % (keep, len(staged_rows), len(old)))
    if not args.apply:
        log("Check passed. The layer was not changed.")
        log("Re-run with --apply to finish the flip.")
        return EXIT_OK
    log("=== APPLY ===")
    _guarded(_recover_apply, args, layer, staged_rows, old, log)
    log("Done.")
    return EXIT_OK


def build_parser():
    ap = argparse.ArgumentParser(
        prog="batchflip.py", allow_abbrev=False,
        description="Swap a hosted table's contents without ever emptying it, with "
                    "overwrite preconditions.",
        epilog="Dry run by default. Only --apply writes. The token comes from the "
               "environment, never the command line.")
    ap.add_argument("--layer-url", help="https://host/arcgis/rest/services/X/FeatureServer/0")
    ap.add_argument("--source", help="new rows: Esri JSON features (.json) or a .csv")
    ap.add_argument("--recover", metavar="BATCH",
                    help="finish an interrupted flip: promote BATCH, retire every other live batch")
    ap.add_argument("--expect-rows", type=int, metavar="N",
                    help="with --recover: row count of the whole batch; the run refuses when "
                         "the table holds a different number")
    ap.add_argument("--batch-id", help="id for the new batch (default: UTC timestamp)")
    ap.add_argument("--batch-field", default=DEFAULT_BATCH_FIELD,
                    help="text column holding the batch id (default %s)" % DEFAULT_BATCH_FIELD)
    ap.add_argument("--state-field", default=DEFAULT_STATE_FIELD,
                    help="text column holding staged/live/retired (default %s)"
                         % DEFAULT_STATE_FIELD)
    ap.add_argument("--chunk", type=int, default=DEFAULT_CHUNK,
                    help="rows per applyEdits call (default %d; a tool choice, not a "
                         "service limit)" % DEFAULT_CHUNK)
    ap.add_argument("--discard-staged", action="store_true",
                    help="delete staged rows left by an earlier failed run, then continue")
    ap.add_argument("--purge", action="store_true",
                    help="after the flip, delete every retired row")
    ap.add_argument("--allow-empty", action="store_true",
                    help="allow a source with no rows (this empties the table)")
    ap.add_argument("--allow-no-geometry", action="store_true",
                    help="allow rows with no geometry in a layer that has a geometry type")
    ap.add_argument("--allow-unstated-srs", action="store_true",
                    help="allow geometries that name no spatial reference in a layer that "
                         "reports one (they must already use the layer's)")
    ap.add_argument("--allow-sync", action="store_true",
                    help="allow a layer that has sync enabled")
    ap.add_argument("--token-env", default="ARCGIS_TOKEN",
                    help="environment variable holding the token (default ARCGIS_TOKEN)")
    ap.add_argument("--apply", action="store_true", help="write (default: check only)")
    ap.add_argument("--self-test", action="store_true",
                    help="run the offline assertions and a local stub server, then exit")
    ap.add_argument("--version", action="version", version="batchflip " + __version__)
    return ap


def main(argv=None):
    args = build_parser().parse_args(argv)
    if args.self_test:
        return self_test()
    return run(args)


# ------------------------------------------------------------------- self-test

_BASE = "/arcgis/rest/services/T/FeatureServer/0"
_TOKEN = "tok-S3CRET-91"
_ROOT = _BASE[:-2]
_HOST = "https://h.example/arcgis/rest/services/T/FeatureServer/0"


def _stub_props(table=False):
    """A synthetic hosted layer. Every name and number here is made up."""
    fields = [
        {"name": "OBJECTID", "type": "esriFieldTypeOID", "editable": False, "nullable": False},
        {"name": "NAME", "type": T_STRING, "length": 8, "editable": True, "nullable": True},
        {"name": "CODE", "type": "esriFieldTypeInteger", "editable": True, "nullable": False},
        {"name": "SCORE", "type": "esriFieldTypeDouble", "editable": True, "nullable": True},
        {"name": "SEEN", "type": T_DATE, "editable": True, "nullable": True},
        {"name": "batch_id", "type": T_STRING, "length": 40, "editable": True, "nullable": True},
        {"name": "batch_state", "type": T_STRING, "length": 10, "editable": True,
         "nullable": True},
    ]
    props = {"type": "Table" if table else "Feature Layer", "objectIdField": "OBJECTID",
             "capabilities": "Create,Delete,Query,Update,Editing", "maxRecordCount": 4,
             "supportsPagination": True, "supportsAdvancedQueries": True, "fields": fields}
    if not table:
        props["geometryType"] = "esriGeometryPoint"
    return props


class _Cut(object):
    """A canned reply that promises 500 bytes, sends five and hangs up."""


_CUT = _Cut()


class _Stub(object):
    """An in-memory stand-in for one hosted layer, driven over real HTTP.

    It understands only the where clauses this tool sends. Faults are injected by
    call number: canned[(op, n)] replaces the reply to the n-th call of that op.
    """

    def __init__(self):
        self.props = _stub_props()
        self.service = {"syncEnabled": False}
        self.rows = []
        self.geoms = {}
        self.next_oid = 1
        self.token = None
        self.truncate_at = None
        self.http_fail = 0
        self.canned = {}
        self.counts = {}
        self.requests = []

    def add(self, attrs):
        row = dict(attrs)
        row["OBJECTID"] = self.next_oid
        self.next_oid += 1
        self.rows.append(row)
        return row["OBJECTID"]

    def seed(self, n, batch, state):
        for i in range(n):
            self.add({"NAME": "s%d" % i, "CODE": i, "batch_id": batch, "batch_state": state})

    def reset(self, n=0, batch="B3", state=LIVE):
        self.rows = []
        self.canned = {}
        self.truncate_at = None
        self.seed(n, batch, state)

    def number(self, state, batch=None):
        return len([r for r in self.rows if r.get("batch_state") == state
                    and (batch is None or r.get("batch_id") == batch)])

    def calls(self, op):
        return self.counts.get(op, 0)

    @staticmethod
    def match(where, attrs):
        invalid = re.match(r"^(\w+) IS NULL OR \1 NOT IN \('staged', 'live', 'retired'\)$", where)
        if invalid:
            return attrs.get(invalid.group(1)) not in STATES
        for clause in where.split(" AND "):
            parts = re.match(r"^(\w+) = '(.*)'$", clause)
            if attrs.get(parts.group(1)) != parts.group(2):
                return False
        return True

    def find(self, oid):
        return next((r for r in self.rows if r["OBJECTID"] == oid), None)

    def handle(self, path, params):
        op = "service" if path.rstrip("/") == _ROOT else path[len(_BASE):].strip("/") or "layer"
        self.counts[op] = self.counts.get(op, 0) + 1
        self.requests.append((op, dict(params)))
        if self.http_fail > 0:
            self.http_fail -= 1
            return 500, b"boom"
        canned = self.canned.pop((op, self.counts[op]), None)
        if canned is not None:
            return 200, canned
        if self.token and params.get("token") != self.token:
            return 200, {"error": {"code": 498, "message": "Invalid token."}}
        ops = {"layer": self.op_layer, "service": self.op_service, "query": self.op_query,
               "applyEdits": self.op_apply, "deleteFeatures": self.op_delete}
        return 200, ops[op](params)

    def op_layer(self, params):
        return self.props

    def op_service(self, params):
        return self.service

    def op_query(self, params):
        hits = [r for r in self.rows if self.match(params["where"], r)]
        if params.get("returnCountOnly") == "true":
            return {"count": len(hits)}
        hits = sorted(hits, key=lambda r: r["OBJECTID"])[:self.truncate_at]
        start = int(params["resultOffset"])
        limit = min(int(params["resultRecordCount"]), self.props["maxRecordCount"])
        names = params["outFields"].split(",")
        return {"features": [{"attributes": dict((k, r.get(k)) for k in names)}
                             for r in hits[start:start + limit]]}

    def op_apply(self, params):
        out = {}
        if "adds" in params:
            out["addResults"] = []
            for item in json.loads(params["adds"]):
                oid = self.add(item["attributes"])
                self.geoms[oid] = item.get("geometry")
                out["addResults"].append({"objectId": oid, "success": True})
        if "updates" in params:
            out["updateResults"] = []
            for item in json.loads(params["updates"]):
                row = self.find(item["attributes"]["OBJECTID"])
                if row is None:
                    out["updateResults"].append(
                        {"success": False, "error": {"code": 1019, "description": "not found"}})
                else:
                    row.update(item["attributes"])
                    out["updateResults"].append({"objectId": row["OBJECTID"], "success": True})
        return out

    def op_delete(self, params):
        results = []
        for text in params["objectIds"].split(","):
            row = self.find(int(text))
            if row is None:
                results.append({"success": False, "error": {"code": 1019,
                                                             "description": "not found"}})
            else:
                self.rows.remove(row)
                results.append({"objectId": row["OBJECTID"], "success": True})
        return {"deleteResults": results}


def _serve(stub):
    from http.server import BaseHTTPRequestHandler, HTTPServer
    import threading

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt, *rest):
            pass

        def do_POST(self):
            raw = self.rfile.read(int(self.headers.get("Content-Length") or 0)).decode("utf-8")
            params = dict((k, v[0]) for k, v in
                          urllib.parse.parse_qs(raw, keep_blank_values=True).items())
            status, body = stub.handle(self.path, params)
            if body is _CUT:
                self.send_response(200)
                self.send_header("Content-Length", "500")
                self.end_headers()
                self.wfile.write(b'{"err')
                self.close_connection = True
                return
            data = body if isinstance(body, bytes) else json.dumps(body).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever)
    thread.daemon = True
    thread.start()
    return server, thread


def _cli(argv):
    """Run main(argv) with stdout and stderr captured. Returns (exit code, out, err)."""
    out, err = io.StringIO(), io.StringIO()
    saved = sys.stdout, sys.stderr
    sys.stdout, sys.stderr = out, err
    try:
        try:
            code = main(argv)
        except SystemExit as exc:
            code = exc.code
    finally:
        sys.stdout, sys.stderr = saved
    return code, out.getvalue(), err.getvalue()


def _feat(i, geom=True):
    item = {"attributes": {"NAME": "n%d" % i, "CODE": i, "SCORE": i + 0.5,
                           "SEEN": 1700000000000 + i}}
    if geom:
        item["geometry"] = {"x": i, "y": i}
    return item


def _args(*argv):
    return build_parser().parse_args(list(argv))


def _t_core(check, raises):
    # ---- small helpers
    check(chunks([1, 2, 3, 4, 5], 2) == [[1, 2], [3, 4], [5]], "chunks splits a list with a short tail")
    check(chunks([], 3) == [], "chunks of an empty list is empty")
    raises(lambda: chunks([1], 0), "a chunk size of 0 raises", ValueError)
    check(sql_str("a'b") == "'a''b'", "sql_str doubles a single quote")
    layer = Layer("u", "t", "OBJECTID", "batch_id", "batch_state", 3, 4)
    check(where_state(layer, LIVE) == "batch_state = 'live'", "where_state builds the state filter")
    check(where_batch(layer, "B1", STAGED) == "batch_id = 'B1' AND batch_state = 'staged'",
          "where_batch builds the batch and state filter")
    check(where_batch_any(layer, "B1") == "batch_id = 'B1'", "where_batch_any builds the batch filter")
    check(where_invalid(layer) ==
          "batch_state IS NULL OR batch_state NOT IN ('staged', 'live', 'retired')",
          "where_invalid catches a NULL state and an unknown state")
    check(default_batch_id(datetime.datetime(2026, 1, 2, 3, 4, 5)) == "20260102T030405Z",
          "the default batch id is a UTC timestamp")
    check(page_size({"maxRecordCount": 7}) == 7, "page size follows maxRecordCount")
    check(page_size({}) == DEFAULT_PAGE, "page size defaults when maxRecordCount is missing")
    check(page_size({"maxRecordCount": True}) == DEFAULT_PAGE, "a boolean maxRecordCount is ignored")
    check(page_size({"maxRecordCount": 0}) == DEFAULT_PAGE, "a zero maxRecordCount is ignored")

    # ---- geometry kinds
    check(geometry_kind({"x": 1, "y": 2}) == "esriGeometryPoint", "a point is recognised")
    check(geometry_kind({"points": []}) == "esriGeometryMultipoint", "a multipoint is recognised")
    check(geometry_kind({"paths": []}) == "esriGeometryPolyline", "a polyline is recognised")
    check(geometry_kind({"rings": []}) == "esriGeometryPolygon", "a polygon is recognised")
    check(geometry_kind({"xmin": 1}) is None, "an envelope is not a feature geometry")
    check(geometry_kind("POINT(1 2)") is None, "a text geometry is not recognised")
    check(geometry_kind("xyz") is None, "a text that contains a geometry key letter is not a geometry")
    s3857 = {"wkid": 102100, "latestWkid": 3857}
    check(sr_same({"wkid": 3857}, s3857) and sr_same({"wkid": 102100}, s3857),
          "wkid and latestWkid are compared, so 102100 and 3857 are the same system")
    check(not sr_same({"wkid": 4326}, s3857), "two different well-known ids are different systems")
    check(sr_same({"wkt": "W"}, {"wkt": "W"}) and not sr_same({"wkt": "W"}, {"wkt": "V"}),
          "two spatial references with no id are compared by their wkt text")
    check(not sr_same({"wkt": "W"}, {"wkid": 4326}) and not sr_same({}, {}) and not sr_same("x", {"wkt": "W"}),
          "a wkt against a wkid, an empty object and a non-object are never the same")
    check(not sr_same({"wkid": True}, {"wkid": 1}), "a boolean wkid is not an id")
    check(sr_text(s3857) == "wkid 3857/102100" and sr_text({"wkt": "W"}) == "wkt"
          and sr_text("x") == "an unreadable spatial reference", "sr_text names the system")
    check(layer_srs({"extent": {"spatialReference": {"wkid": 4326}}}) == {"wkid": 4326},
          "the layer's spatial reference is read from its extent")
    check(layer_srs({"spatialReference": {"wkid": 4326}}) == {"wkid": 4326},
          "a top level layer spatialReference is read when there is no extent")
    check(layer_srs({}) is None and layer_srs({"extent": 5, "spatialReference": {}}) is None,
          "a layer that names no spatial reference gives None")

    # ---- layer url
    check(url_problem(_HOST) is None, "an https layer url is accepted")
    check(url_problem("http://127.0.0.1:8/a/FeatureServer/3/") is None,
          "plain http is accepted on loopback only")
    check("plain http" in url_problem("http://h.example/a/FeatureServer/0"),
          "plain http to another host is refused")
    check("query string" in url_problem(_HOST + "?token=abc"),
          "a token in the url is refused  <-- pinned defect")
    check("query string" in url_problem(_HOST + "#frag"), "a url fragment is refused")
    check("FeatureServer" in url_problem("https://h.example/a/MapServer/0"),
          "a url that is not a feature layer is refused")
    check("http(s)" in url_problem("ftp://h.example/a/FeatureServer/0"), "a non-http scheme is refused")
    check("http(s)" in url_problem(""), "an empty url is refused")
    check("user name or password" in url_problem("https://bob:pw@h.example/a/FeatureServer/0"),
          "a url with a user name and password is refused  <-- pinned defect")
    check("user name or password" in url_problem("https://@h.example/a/FeatureServer/0"),
          "a url with an empty user name is refused")
    check(DEFAULT_CHUNK == 500 and DEFAULT_PAGE == 1000, "the default chunk is 500 and the default page is 1000")


def _t_source(check, raises, tmp):
    feats = json.dumps({"features": [_feat(1), _feat(2, geom=False)]})
    rows, geoms = parse_source(feats, "json")
    check(len(rows) == 2 and geoms[0] == {"x": 1, "y": 1} and geoms[1] is None,
          "an Esri feature set parses into rows and geometries")
    rows, geoms = parse_source(json.dumps([_feat(1)]), "json")
    check(len(rows) == 1 and geoms[0] is not None, "a bare list of features parses")
    rows, geoms = parse_source(json.dumps([{"NAME": "a"}, {"NAME": "b"}]), "json")
    check(rows == [{"NAME": "a"}, {"NAME": "b"}] and geoms == [None, None],
          "a list of flat attribute objects parses")
    raises(lambda: parse_source("{not json", "json"), "invalid JSON raises", SourceError)
    raises(lambda: parse_source('[{"CODE": NaN}]', "json"),
          "NaN in JSON raises  <-- pinned defect", SourceError)
    raises(lambda: parse_source('[{"CODE": -Infinity}]', "json"),
           "Infinity in JSON raises", SourceError)
    raises(lambda: parse_source('{"rows": []}', "json"), "an object without features raises", SourceError)
    raises(lambda: parse_source("[1]", "json"), "a row that is not an object raises", SourceError)
    raises(lambda: parse_source('[{"attributes": 3}]', "json"),
           "attributes that are not an object raise", SourceError)
    rows, geoms = parse_source(json.dumps({
        "spatialReference": {"wkid": 3857},
        "features": [_feat(1), _feat(2, geom=False),
                     {"attributes": {"CODE": 3},
                      "geometry": {"x": 1, "y": 1, "spatialReference": {"wkid": 4326}}}]}), "json")
    check(geoms[0]["spatialReference"] == {"wkid": 3857} and geoms[1] is None
          and geoms[2]["spatialReference"] == {"wkid": 4326},
          "an envelope spatialReference reaches every geometry that names none  <-- pinned defect")
    raises(lambda: parse_source('{"exceededTransferLimit": true, "features": []}', "json"),
           "a cut-off query result (exceededTransferLimit) is refused  <-- pinned defect", SourceError)
    rows, geoms = parse_source('{"exceededTransferLimit": false, "features": [{"CODE": 1}]}', "json")
    check(len(rows) == 1, "exceededTransferLimit false is an ordinary source")
    rows, geoms = parse_source("NAME,CODE\na,1\n\nb,2\n", "csv")
    check(rows == [{"NAME": "a", "CODE": "1"}, {"NAME": "b", "CODE": "2"}],
          "a csv parses and skips blank lines")
    raises(lambda: parse_source("", "csv"), "a csv with no header raises", SourceError)
    raises(lambda: parse_source("A,a\n1,2\n", "csv"), "a csv header that repeats a name raises", SourceError)
    raises(lambda: parse_source("A,B\n1,2,3\n", "csv"), "a csv row with extra cells raises", SourceError)
    raises(lambda: parse_source("A,B\n1\n", "csv"), "a csv row with missing cells raises", SourceError)
    raises(lambda: parse_source("A\n" + "x" * 140000 + "\n", "csv"),
           "a csv cell over the reader limit raises SourceError, not csv.Error  <-- pinned defect", SourceError)

    def path(name, data):
        full = os.path.join(tmp, name)
        with io.open(full, "wb") as handle:
            handle.write(data)
        return full

    rows, geoms, kind = read_source(path("ok.csv", b"\xef\xbb\xbfNAME\nx\n"))
    check(kind == "csv" and rows == [{"NAME": "x"}], "read_source strips a utf-8 byte order mark")
    rows, geoms, kind = read_source(path("ok.json", b'[{"NAME": "x"}]'))
    check(kind == "json" and rows == [{"NAME": "x"}], "read_source reads a .json file")
    raises(lambda: read_source(path("bad.txt", b"x")), "an unknown extension raises", SourceError)
    raises(lambda: read_source(path("valid.txt", b'[{"NAME": "x"}]')),
           "a .txt file is refused even when it holds valid JSON", SourceError)
    raises(lambda: read_source(os.path.join(tmp, "missing.csv")), "a missing file raises", SourceError)
    raises(lambda: read_source(path("bin.csv", b"\xff\xfe\x00\x80")),
           "a file that is not utf-8 raises", SourceError)


def _t_layer(check):
    def props(**change):
        out = _stub_props()
        out.update(change)
        return out

    def checks(p, **kw):
        return layer_checks(p, kw.get("batch", "batch_id"), kw.get("state", "batch_state"),
                            kw.get("sync", False), kw.get("service"))

    check(checks(props()) == ([], []), "a well formed layer passes every precondition")
    check(checks(props(), batch="BATCH_ID")[0] == [], "column names match without regard to case")
    check(checks({"type": "Feature Layer"})[0] == ["layer metadata has no fields list"],
          "a layer without fields is refused")
    check("not Feature Layer" in checks(props(type="Raster Layer"))[0][0], "a raster layer is refused")
    check(checks(props(capabilities="Query"))[0] ==
          ["layer capabilities lack: Create, Update, Delete"], "missing edit capabilities are named")
    check(checks(props(capabilities=None))[0][0].endswith("Query, Create, Update, Delete"),
          "a layer with no capabilities is refused")
    check("view" in checks(props(isView=True))[0][0], "a view is refused")
    check("sync is enabled" in checks(props(syncEnabled=True))[0][0], "a sync enabled layer is refused")
    check(checks(props(syncEnabled=True), sync=True)[0] == [], "--allow-sync accepts a sync enabled layer")
    check("sync is enabled" in checks(props(), service={"syncEnabled": True})[0][0],
          "a sync enabled service is refused, which is where Esri documents the property")
    check(checks(props(), service={"syncEnabled": True}, sync=True)[0] == [],
          "--allow-sync accepts a sync enabled service")
    check(checks(props(), service={"syncEnabled": "yes"})[0] == [], "only a true syncEnabled counts")
    check("supportsPagination" in checks(props(supportsPagination=False))[0][0],
          "a layer without paging is refused")
    check("supportsAdvancedQueries" in checks(props(supportsAdvancedQueries=False))[0][0],
          "a layer without advanced queries is refused")
    for key in ROLLBACK_KEYS:
        p = props()
        p[key] = False
        check(len(checks(p)[1]) == 1 and checks(p)[0] == [],
              "%s=false is a warning, not a refusal" % key)
    check("objectIdField" in checks(props(objectIdField="NOPE"))[0][0], "an unknown objectIdField is refused")
    p = props()
    p["fields"] = [f for f in p["fields"] if f["name"] != "batch_id"]
    check("no batch column named batch_id" in checks(p)[0][0], "a missing batch column is refused")
    p = props()
    p["fields"][5]["type"] = "esriFieldTypeInteger"
    check("not a text field" in checks(p)[0][0], "a numeric batch column is refused")
    p = props()
    p["fields"][5]["length"] = 10
    check("length 10" in checks(p)[0][0], "a short batch column is refused")
    p = props()
    p["fields"][6]["length"] = 5
    check("state column batch_state has length 5" in checks(p)[0][0], "a short state column is refused")
    p = props()
    p["fields"][6]["editable"] = False
    check("not editable" in checks(p)[0][0], "a read only state column is refused")
    p = props()
    del p["fields"][6]["length"]
    check(checks(p)[0] == [], "a state column without a length property is accepted")
    p = props()
    p["fields"] = [5, {"name": 7}] + p["fields"]
    check(checks(p) == ([], []), "junk entries in the fields list are ignored")

    # ---- cells
    num = {"type": "esriFieldTypeInteger", "nullable": True}
    check(coerce_cell(5, num, False) == (5, None), "an integer fits an integer field")
    check(coerce_cell(True, num, False)[1] == "not a whole number", "a boolean is not an integer")
    check(coerce_cell(5.5, num, False)[1] == "not a whole number", "a float is not an integer")
    check(coerce_cell("5", num, False)[1] == "not a whole number", "text is not an integer in json mode")
    check(coerce_cell("5", num, True) == (5, None), "csv text becomes an integer")
    check(coerce_cell("x", num, True)[1] == "not a whole number", "csv text that is not an integer is named")
    dbl = {"type": "esriFieldTypeDouble", "nullable": True}
    check(coerce_cell(1, dbl, False) == (1, None), "an integer fits a double field")
    check(coerce_cell(float("inf"), dbl, False)[1] == "not a finite number", "infinity is refused")
    check(coerce_cell(True, dbl, False)[1] == "not a finite number", "a boolean is not a double")
    check(coerce_cell(10 ** 400, dbl, False)[1] == "not a finite number",
          "an integer too big for a double is refused, not a traceback  <-- pinned defect")
    check(coerce_cell("a", dbl, False)[1] == "not a finite number", "text is not a double in json mode")
    check(coerce_cell("1.5", dbl, True) == (1.5, None), "csv text becomes a double")
    check(coerce_cell("x", dbl, True)[1] == "not a number", "csv text that is not a number is named")
    txt = {"type": T_STRING, "length": 3, "nullable": True}
    check(coerce_cell("abc", txt, False) == ("abc", None), "text at the field length fits")
    one = {"type": T_STRING, "length": 1}
    check(coerce_cell("a", one, False) == ("a", None)
          and coerce_cell("ab", one, False)[1] == "longer than the field length",
          "text over a field length of 1 is refused")
    check(coerce_cell("abcd", txt, False)[1] == "longer than the field length",
          "text over the field length is refused")
    check(coerce_cell(5, txt, False)[1] == "not text", "a number is not text")
    check(coerce_cell("abcd", {"type": T_STRING}, False) == ("abcd", None),
          "text in a field with no length property fits")
    check(coerce_cell("", txt, True) == (None, None), "an empty csv cell is NULL")
    check(coerce_cell("", {"type": T_STRING, "nullable": False}, True)[1] ==
          "null in a field that does not allow null", "an empty csv cell in a required field is refused")
    check(coerce_cell(None, {"type": T_STRING, "nullable": False}, False)[1] ==
          "null in a field that does not allow null", "null in a required field is refused")
    check(coerce_cell(None, txt, False) == (None, None), "null in a nullable field fits")
    check(coerce_cell("17", {"type": T_DATE}, True) == (17, None), "csv dates are epoch milliseconds")
    check(coerce_cell("2026-01-01", {"type": T_DATE}, True)[1] == "not a whole number",
          "a csv date that is not epoch milliseconds is refused")
    check(coerce_cell({"a": 1}, {"type": "esriFieldTypeGUID"}, False) == ({"a": 1}, None),
          "a field type the tool does not model passes through")


def _t_rows(check):
    props = _stub_props()
    good = [_feat(1)["attributes"], _feat(2)["attributes"]]
    geo = [{"x": 1, "y": 1}, {"x": 2, "y": 2}]

    def prep(rows, geoms=None, p=None, csv_mode=False, empty=False, no_geo=False):
        p = p or props
        fill = {"x": 1, "y": 1} if p.get("geometryType") else None
        return prepare_rows(p, rows, geoms or [fill] * len(rows),
                            "batch_id", "batch_state", csv_mode, empty, no_geo)

    problems, clean = prep(good, geo)
    check(problems == [] and len(clean) == 2, "a matching source passes")
    check(clean[0][0]["CODE"] == 1 and clean[1][1] == {"x": 2, "y": 2},
          "clean rows keep their values and geometry")
    problems, clean = prep([{"name": "a", "code": 1}])
    check(problems == [] and clean[0][0] == {"NAME": "a", "CODE": 1},
          "source columns are renamed to the layer's own field names")
    check(prep([])[0][0].startswith("source has no rows"),
          "an empty source is refused  <-- pinned defect")
    check(prep([], empty=True) == ([], []), "--allow-empty accepts an empty source")
    check("not in the layer: ZED" in prep([{"CODE": 1, "ZED": 1}])[0][0], "an unknown source column is named")
    check("carries the batch column" in prep([{"CODE": 1, "batch_id": "x"}])[0][0],
          "a source that carries the batch column is refused")
    check("not editable: OBJECTID" in prep([{"CODE": 1, "OBJECTID": 5}])[0][0],
          "a source that targets the object id is refused")
    check("lacks: CODE" in prep([{"NAME": "a"}])[0][0], "a required layer field missing from the source is named")
    p = _stub_props()
    p["fields"][2]["defaultValue"] = 0
    check(prep([{"NAME": "a"}], p=p)[0] == [], "a required field with a default may be left out")
    check("differ only by case" in prep([{"CODE": 1, "code": 2}])[0][0],
          "two source columns that differ by case are refused")
    problems, _ = prep([{"CODE": 1}, {"CODE": "x"}, {"CODE": "y"}])
    check(problems == ["column CODE: 2 value(s) not a whole number (first at row 2)"],
          "bad values are counted by column with the first row  <-- pinned defect")
    check(prep([{"CODE": 1, "NAME": "far too long"}])[0] == ["column NAME: 1 value(s) longer than the field length (first at row 1)"],
          "a string over the field length is named")
    problems, _ = prep(good, [{"x": 1, "y": 1}, {"foo": 1}])
    check("1 geometry value(s) are not Esri JSON" in problems[0], "an unrecognised geometry is counted")
    problems, _ = prep(good, [{"x": 1, "y": 1}, {"rings": []}])
    check("1 geometry value(s) do not match the layer's esriGeometryPoint" in problems[0],
          "a polygon into a point layer is refused  <-- pinned defect")
    problems, _ = prep(good, [None, None])
    check(problems == ["2 row(s) have no geometry, and the layer holds esriGeometryPoint; a CSV has "
                       "none (pass --allow-no-geometry to load them anyway)"],
          "rows with no geometry are refused for a point layer  <-- pinned defect")
    problems, _ = prep([{"CODE": 1}], csv_mode=True, geoms=[None])
    check("1 row(s) have no geometry" in problems[0], "a csv into a point layer is refused  <-- pinned defect")
    problems, clean = prep(good, [None, {"x": 2, "y": 2}], no_geo=True)
    check(problems == [] and clean[0][1] is None, "--allow-no-geometry accepts rows with no geometry")
    problems, _ = prep(good, [{"x": 1, "y": float("inf")}, {"x": 2, "y": 2}])
    check(problems == ["1 geometry value(s) hold a number that is not finite"],
          "a geometry coordinate that is not finite is refused  <-- pinned defect")
    problems, _ = prep(good, [{"rings": [[[0, 0], [1e999, 1]]]}, geo[1]])
    check("do not match" in problems[0] and len(problems) == 1,
          "a polygon into a point layer is a mismatch, not a second finite-number problem")
    poly = _stub_props()
    poly["geometryType"] = "esriGeometryPolygon"
    ring = {"spatialReference": {"wkid": 4326, "wkt": "w"}, "rings": [[[0, 0], [float("inf"), 1]]]}
    problems, _ = prep(good, [ring, {"rings": [[[0, 0], [1, 1]]]}], p=poly)
    check(problems == ["1 geometry value(s) hold a number that is not finite"],
          "an infinite coordinate nested in a polygon is refused")
    table = _stub_props(table=True)
    problems, _ = prep(good, geo, p=table)
    check("table (no geometry)" in problems[0], "geometry into a table is refused")

    # ---- spatial reference
    sp = _stub_props()
    sp["extent"] = {"spatialReference": {"wkid": 102100, "latestWkid": 3857}}

    def sprep(srs, **kw):
        geoms3 = [dict({"x": 1, "y": 1}, **({} if s_ is None else {"spatialReference": s_}))
                  for s_ in srs]
        return prepare_rows(sp, [good[0]] * len(srs), geoms3, "batch_id", "batch_state",
                            False, False, **kw)[0]

    check(sprep([{"wkid": 3857}, {"latestWkid": 3857}]) == [], "geometry in the layer's own system passes")
    check(sprep([{"wkid": 4326}, {"wkid": 4326}, {"wkt": "W"}]) == [
        "3 geometry value(s) use wkid 4326, wkt, but the layer uses wkid 3857/102100; the tool does "
        "not reproject"], "geometry in another system is refused and the systems are named  <-- pinned defect")
    check(sprep(["junk"])[0].startswith("1 geometry value(s) use an unreadable spatial reference"),
          "a spatial reference that is not an object is refused")
    check("name no spatial reference" in sprep([None, {"wkid": 3857}])[0] and len(sprep([None])) == 1,
          "geometry that names no spatial reference is refused when the layer names one  <-- pinned defect")
    check(sprep([None], allow_unstated_srs=True) == [], "--allow-unstated-srs accepts it")
    check(sprep([None, {"wkid": 4326}], allow_unstated_srs=True)[0].startswith("1 geometry value(s) use wkid 4326"),
          "--allow-unstated-srs does not excuse a geometry in another system")
    problems, clean = prep([{"CODE": "7", "NAME": "", "SCORE": "2.5"}], p=table, csv_mode=True)
    check(problems == [] and clean[0][0] == {"CODE": 7, "NAME": None, "SCORE": 2.5},
          "csv rows are coerced to the layer's field types")

    layer = Layer("u", "t", "OBJECTID", "batch_id", "batch_state", 3, 4)
    staged = stage_features([({"CODE": 1}, {"x": 1, "y": 2}), ({"CODE": 2}, None)], layer, "B9")
    check(staged[0] == {"attributes": {"CODE": 1, "batch_id": "B9", "batch_state": "staged"},
                        "geometry": {"x": 1, "y": 2}}, "staged rows carry the batch id, state and geometry")
    check("geometry" not in staged[1], "a row without geometry is staged without a geometry key")

    # ---- state refusals
    def refuse(live=(), staged=0, invalid=0, collide=0, discard=False):
        return state_refusals(list(live), layer, staged, invalid, collide, discard)

    check(refuse() == [], "an empty table has no state refusals")
    check(refuse(live=[{"batch_id": "A"}, {"batch_id": "A"}]) == [], "one live batch is fine")
    check("2 batches are live" in refuse(live=[{"batch_id": "A"}, {"batch_id": "B"}])[0],
          "two live batches are refused")
    check("(blank)" in refuse(live=[{"batch_id": "A"}, {"batch_id": None}])[0],
          "a live row with no batch id counts as its own batch")
    check("no valid batch_state" in refuse(invalid=2)[0], "rows with no valid state are refused")
    check("staged row(s) are left" in refuse(staged=1)[0], "leftover staged rows are refused")
    check(refuse(staged=1, discard=True) == [], "--discard-staged clears the leftover refusal")
    check("already exists" in refuse(collide=1)[0], "a reused batch id is refused")

    # ---- flip plan
    check(plan_flip([], [], layer) == [], "nothing to flip gives an empty plan")
    one = plan_flip([1, 2], [3], layer)
    check(len(one) == 1 and len(one[0][1]) == 3 and one[0][0] == "promote and retire in one call",
          "rows that fit one chunk flip in one call")
    check([u["attributes"]["batch_state"] for u in one[0][1]] == [LIVE, LIVE, RETIRED],
          "the one call promotes before it retires")
    many = plan_flip([1, 2, 3, 4, 5, 6, 7], [8, 9, 10], layer)
    check([(label, len(u)) for label, u in many] ==
          [("promote chunk 1", 3), ("promote chunk 2", 3), ("promote chunk 3", 1),
           ("retire chunk 1", 3)], "rows above one chunk promote every chunk before any retire")
    check(len(plan_flip([], [1], layer)) == 1, "a flip with nothing new retires in one call")
    check(len(plan_flip([1], [], layer)) == 1, "a flip with nothing old promotes in one call")

    # ---- edit replies
    ok = {"addResults": [{"success": True, "objectId": 4}]}
    check(edit_failures(ok, "addResults", 1, True) == [], "a clean reply has no failures")
    check("no addResults list" in edit_failures({}, "addResults", 1)[0],
          "a reply with no result list is not success  <-- pinned defect")
    check("no addResults list" in edit_failures({"addResults": None}, "addResults", 1)[0],
          "a null result list is not success")
    check("expected 2 addResults, got 1" in edit_failures(ok, "addResults", 2)[0],
          "a short result list is not success  <-- pinned defect")
    bad = {"addResults": [{"success": False, "error": {"code": 1019, "description": "x"}}]}
    check(edit_failures(bad, "addResults", 1) == ["addResults[0] failed: 1019 x"],
          "a failed row is reported with its error")
    check("success flag is None" in edit_failures({"addResults": [{}]}, "addResults", 1)[0],
          "a result with no success flag is a failure")
    check("not an object" in edit_failures({"addResults": [7]}, "addResults", 1)[0],
          "a result that is not an object is a failure")
    check("has no objectId" in edit_failures({"addResults": [{"success": True}]}, "addResults", 1, True)[0],
          "a staged row without an objectId is a failure")
    check("has no objectId" in edit_failures({"addResults": [{"success": True, "objectId": True}]},
                                              "addResults", 1, True)[0],
          "a boolean objectId is a failure")
    check(len(edit_failures({"addResults": [{}] * 9}, "addResults", 9)) == 5, "failures are capped at five")
    check(oid_list([{"OBJECTID": 1}, {"OBJECTID": 2}], "OBJECTID") == [1, 2], "oid_list reads the ids")
    raises_oid = False
    try:
        oid_list([{"OBJECTID": None}], "OBJECTID")
    except RestError:
        raises_oid = True
    check(raises_oid, "a row with no object id raises")
    raises_oid = False
    try:
        oid_list([{"OBJECTID": True}], "OBJECTID")
    except RestError:
        raises_oid = True
    check(raises_oid, "a boolean object id raises")
    check(len(edit_failures({"addResults": [{"success": "false"}, {"success": 1}]}, "addResults", 2)) == 2,
          "a success that is not exactly true is a failure")

    # ---- arguments
    base = ["--layer-url", _HOST]
    check(arg_problems(_args(*base + ["--source", "a.csv"])) == [], "a plain check run has no usage problems")
    check(len(arg_problems(_args("--source", "a.csv"))) == 1, "a missing layer url is a usage problem")
    check("exactly one" in arg_problems(_args(*base))[0], "neither --source nor --recover is a usage problem")
    check("exactly one" in arg_problems(_args(*base + ["--source", "a", "--recover", "B"]))[0],
          "both --source and --recover is a usage problem")
    check(arg_problems(_args(*base + ["--recover", "B1", "--expect-rows", "1"])) == [], "a recover run has no usage problems")
    check("letters, digits" in arg_problems(_args(*base + ["--recover", "B 1", "--expect-rows", "1"]))[0],
          "a recover id with a space is refused")
    check("cannot be combined" in arg_problems(_args(*base + ["--recover", "B1", "--expect-rows", "1", "--batch-id", "B2"]))[0],
          "--recover with --batch-id is refused")
    check("cannot be combined" in arg_problems(_args(*base + ["--recover", "B1", "--expect-rows", "1", "--allow-empty"]))[0],
          "--recover with --allow-empty is refused")
    check("--expect-rows N" in arg_problems(_args(*base + ["--recover", "B1"]))[0]
          and "--expect-rows N" in arg_problems(_args(*base + ["--recover", "B1", "--expect-rows", "0"]))[0],
          "--recover without a positive --expect-rows is refused  <-- pinned defect")
    check("goes with --recover only" in arg_problems(_args(*base + ["--source", "a", "--expect-rows", "3"]))[0],
          "--expect-rows without --recover is refused")
    check("--batch-id must be" in arg_problems(_args(*base + ["--source", "a", "--batch-id", "x'y"]))[0],
          "a batch id with a quote is refused  <-- pinned defect")
    check("--batch-id must be" in arg_problems(_args(*base + ["--source", "a", "--batch-id", "x" * 41]))[0],
          "a batch id over 40 characters is refused")
    check("--chunk must be" in arg_problems(_args(*base + ["--source", "a", "--chunk", "0"]))[0],
          "a chunk of 0 is refused")
    check(arg_problems(_args(*base + ["--source", "a", "--chunk", "1"])) == [], "a chunk of 1 is accepted")
    check(arg_problems(_args(*base + ["--source", "a", "--batch-id", "x" * 40])) == [],
          "a batch id of 40 characters is accepted")
    check("--batch-id must be" in arg_problems(_args(*base + ["--source", "a", "--batch-id", "B1\n"]))[0],
          "a batch id that ends in a newline is refused  <-- pinned defect")
    check("--batch-id must be" in arg_problems(_args(*base + ["--source", "a", "--batch-id", "_lead"]))[0],
          "a batch id that starts with an underscore is refused")
    check("--state-field must be" in arg_problems(_args(*base + ["--source", "a", "--state-field", "s\n"]))[0],
          "a field name that ends in a newline is refused")
    check("--state-field must be a plain" in arg_problems(_args(*base + ["--source", "a", "--state-field", "a b"]))[0],
          "a field name with a space is refused")
    check("must differ" in arg_problems(_args(*base + ["--source", "a", "--state-field", "BATCH_ID"]))[0],
          "the same column for batch and state is refused")
    check(build_parser().parse_args(["--apply"]).apply is True, "--apply sets the write flag")
    check(build_parser().parse_args([]).apply is False, "the write flag is off by default")
    for prefix in ("--ap", "--app", "--appl", "--sourc", "--recov", "--dis", "--expect", "--allow-unst"):
        code, _out, err = _cli(["--layer-url", _HOST, prefix, "x"])
        check(code == 2 and "ambiguous" not in err and "unrecognized arguments" in err,
              "a prefix %s of a flag is REFUSED by the parser  <-- pinned defect" % prefix)
    code, _out, _err = _cli(["--version"])
    check(code == 0, "--version exits 0")


def _t_net(check, tmp):
    """Everything below talks real HTTP to the stub on 127.0.0.1."""
    global _SLEEP, _NOW, plan_flip, _HTTP
    real_plan = plan_flip
    stub = _Stub()
    server, thread = _serve(stub)
    url = "http://127.0.0.1:%d%s" % (server.server_address[1], _BASE)
    sleeps = []
    saved = (_SLEEP, _NOW, os.environ.get("NO_PROXY"), os.environ.get("ARCGIS_TOKEN"))
    _SLEEP = sleeps.append
    os.environ["NO_PROXY"] = "127.0.0.1,localhost"
    os.environ["ARCGIS_TOKEN"] = _TOKEN
    stub.token = _TOKEN
    outputs = []
    layer = Layer(url, _TOKEN, "OBJECTID", "batch_id", "batch_state", 3, 4)

    def src(name, items):
        full = os.path.join(tmp, name)
        with io.open(full, "w", encoding="utf-8", newline="") as handle:
            handle.write(items if isinstance(items, str) else json.dumps({"features": items}))
        return full

    src3 = src("src3.json", [_feat(i) for i in range(3)])
    src7 = src("src7.json", [_feat(i) for i in range(7)])

    leaked = []

    def cli(*argv):
        before = sorted(os.listdir(tmp))
        code, out, err = _cli(list(argv))
        outputs.append(out + err)
        leaked.extend(a for a in [argv] if sorted(os.listdir(tmp)) != before)
        return code, out, err

    def writes():
        return stub.calls("applyEdits") + stub.calls("deleteFeatures")

    try:
        # ---- the seam: retries, errors, token
        stub.http_fail = 2
        resp = call(url, {}, _TOKEN)
        check(resp["type"] == "Feature Layer" and stub.calls("layer") == 3 and sleeps == [2, 4],
              "a read is retried after two HTTP 500 replies, with backoff")
        sleeps[:] = []
        stub.http_fail = 5
        failed = None
        try:
            call(url, {}, _TOKEN)
        except RestError as exc:
            failed = str(exc)
        check(failed is not None and "after 3 attempt(s)" in failed and _TOKEN not in failed,
              "a read gives up after three attempts and never prints the token")
        stub.http_fail = 0
        before = stub.calls("applyEdits")
        stub.http_fail = 1
        failed = None
        try:
            apply_edits(layer, "adds", [{"attributes": {"CODE": 1}}])
        except RestError as exc:
            failed = str(exc)
        check(failed is not None and stub.calls("applyEdits") == before + 1 and "after 1 attempt(s)" in failed,
              "a write is sent once and never retried  <-- pinned defect")
        stub.http_fail = 0
        for k in (1, 2, 3):
            stub.canned[("layer", stub.calls("layer") + k)] = b"<html>not json</html>"
        failed = None
        try:
            call(url, {}, _TOKEN)
        except RestError as exc:
            failed = str(exc)
        check(failed is not None and "after 3 attempt(s)" in failed, "a reply that is not JSON is a failure")
        stub.canned[("layer", stub.calls("layer") + 1)] = [1, 2]
        failed = None
        try:
            call(url, {}, _TOKEN)
        except RestError as exc:
            failed = str(exc)
        check(failed is not None and "not a JSON object" in failed, "a JSON list reply is a failure")
        stub.canned[("layer", stub.calls("layer") + 1)] = {"error": {"code": 400, "message": "nope"}}
        failed = None
        try:
            call(url, {}, _TOKEN)
        except RestError as exc:
            failed = str(exc)
        check(failed == "ArcGIS REST error 400: nope",
              "an error object in an HTTP 200 reply is a failure  <-- pinned defect")
        stub.canned[("layer", stub.calls("layer") + 1)] = {"error": "oops"}
        failed = None
        try:
            call(url, {}, _TOKEN)
        except RestError as exc:
            failed = str(exc)
        check(failed == "ArcGIS REST error None: None", "an error that is not an object is still a failure")
        stub.token = None
        check(call(url, {}, "")["type"] == "Feature Layer" and "token" not in stub.requests[-1][1],
              "no token parameter is sent when there is no token")
        stub.token = _TOKEN
        for k in (1, 2, 3):
            stub.canned[("layer", stub.calls("layer") + k)] = _CUT
        failed = None
        try:
            call(url, {}, _TOKEN)
        except RestError as exc:
            failed = str(exc)
        check(failed is not None and "IncompleteRead" in failed and "after 3 attempt(s)" in failed,
              "a reply cut off mid-body is a failure, not a traceback  <-- pinned defect")
        failed = None
        try:
            call(url + " x", {}, _TOKEN)
        except RestError as exc:
            failed = str(exc)
        check(failed is not None and "after 3 attempt(s)" in failed,
              "a url that http.client rejects is a failure, not a traceback")

        # ---- direct reads and edits
        stub.reset(5, "R", LIVE)
        base_q = stub.calls("query")
        stub.canned[("query", base_q + 2)] = {}
        failed = None
        try:
            fetch_rows(layer, where_state(layer, LIVE), ["OBJECTID"])
        except RestError as exc:
            failed = str(exc)
        check(failed is not None and "read 0 row(s) but the count query says 5" in failed,
              "a page with no features stops the read")
        rows = fetch_rows(layer, where_state(layer, LIVE), ["OBJECTID"])
        check(len(rows) == 5 and stub.calls("query") == base_q + 2 + 3,
              "a read pages through maxRecordCount rows at a time")
        stub.canned[("query", stub.calls("query") + 1)] = {"count": 3}
        failed = None
        try:
            fetch_rows(layer, where_state(layer, LIVE), ["OBJECTID"])
        except RestError as exc:
            failed = str(exc)
        check(failed is not None and "read 4 row(s) but the count query says 3" in failed,
              "a read that returns more rows than the count says stops the run")
        stub.canned[("query", stub.calls("query") + 1)] = {"count": "five"}
        failed = None
        try:
            fetch_count(layer, "batch_id = 'R'")
        except RestError as exc:
            failed = str(exc)
        check(failed is not None and "no integer count" in failed, "a count that is not an integer is a failure")
        failed = None
        try:
            apply_edits(layer, "updates", [{"attributes": {"OBJECTID": 999, "batch_state": LIVE}}])
        except RestError as exc:
            failed = str(exc)
        check(failed is not None and "updateResults[0] failed: 1019 not found" in failed,
              "an update of a missing row is reported, not swallowed")
        gone = call(url + "/deleteFeatures", {"objectIds": "99999"}, _TOKEN, write=True)
        check("1019 not found" in edit_failures(gone, "deleteResults", 1)[0],
              "a delete of a row that is already gone is reported, not swallowed")
        before = len(stub.rows)
        failed = None
        stub.rows.append({"OBJECTID": 777, "batch_id": "Q", "batch_state": RETIRED})
        stub.canned[("deleteFeatures", stub.calls("deleteFeatures") + 1)] = {
            "deleteResults": [{"success": False, "error": {"code": 1, "description": "locked"}}]}
        try:
            delete_rows(layer, "batch_id = 'Q'")
        except RestError as exc:
            failed = str(exc)
        check(failed is not None and "locked" in failed, "a failed delete result is reported")
        check(len(stub.rows) == before + 1, "a failed delete leaves the row in place")
        stub.rows.pop()
        real_http, attempts = _HTTP, []

        def flaky(target, params, retries):
            if target.endswith("/deleteFeatures"):
                attempts.append(retries)
                raise RestError("planned")
            return real_http(target, params, retries)

        _HTTP = flaky
        try:
            stub.rows.append({"OBJECTID": 778, "batch_id": "Q", "batch_state": RETIRED})
            try:
                delete_rows(layer, "batch_id = 'Q'")
            except RestError:
                pass
        finally:
            _HTTP = real_http
        check(attempts == [1], "a deleteFeatures call is sent once and never retried  <-- pinned defect")
        stub.rows.pop()

        # ---- first load: a dry run, then the write
        stub.reset(0)
        mark = writes()
        code, out, err = cli("--layer-url", url, "--source", src3, "--batch-id", "B1", "--chunk", "4")
        check(code == 0 and "Check passed" in out, "a check run on an empty table passes")
        check(stub.rows == [] and writes() == mark, "a check run writes nothing")
        start = len(stub.requests)
        code, out, err = cli("--layer-url", url, "--source", src3, "--batch-id", "B1", "--chunk", "4", "--apply")
        check(code == 0 and "Done." in out and stub.number(LIVE, "B1") == 3 and len(stub.rows) == 3,
              "the first load stages and promotes three rows")
        check(writes() - mark == 2, "three rows and a chunk of four take one add call and one update call")
        sent = stub.requests[start:]
        check(all(p.get("rollbackOnFailure") == "true" for op, p in sent
                  if op in ("applyEdits", "deleteFeatures")),
              "every write asks for rollbackOnFailure=true")
        check(all(p.get("token") == _TOKEN for _op, p in sent), "every request carried the token")
        check(len([g for g in stub.geoms.values() if g is not None]) == 3, "geometry reaches the service")

        # ---- the next night: a swap above one chunk
        mark = writes()
        code, out, err = cli("--layer-url", url, "--source", src7, "--batch-id", "B2", "--chunk", "3")
        check(code == 0 and "retire 3 live row(s)" in out and writes() == mark,
              "a check run reports how many live rows it would retire")
        code, out, err = cli("--layer-url", url, "--source", src7, "--batch-id", "B2", "--chunk", "3", "--apply")
        check(code == 0 and stub.number(LIVE, "B2") == 7 and stub.number(RETIRED, "B1") == 3
              and stub.number(LIVE) == 7, "the swap leaves the new batch live and the old batch retired")
        check(writes() - mark == 7, "seven rows take three add calls, three promote calls and one retire call")
        check("promote chunk 3: 1 row(s)" in out and "retire chunk 1: 3 row(s)" in out and "verified: 7" in out,
              "the run reports each chunk and the verified live count")

        # ---- purge
        mark = stub.calls("deleteFeatures")
        start = len(stub.requests)
        code, out, err = cli("--layer-url", url, "--source", src3, "--batch-id", "B3", "--chunk", "4",
                             "--purge", "--apply")
        deletes = [p for op, p in stub.requests[start:] if op == "deleteFeatures"]
        check(len(deletes) == 3 and all(p.get("rollbackOnFailure") == "true" for p in deletes),
              "every delete asks for rollbackOnFailure=true")
        check(code == 0 and stub.number(RETIRED) == 0 and stub.number(LIVE, "B3") == 3 and len(stub.rows) == 3,
              "--purge removes every retired row after the flip")
        check(stub.calls("deleteFeatures") - mark == 3 and "purged 10 retired" in out,
              "ten retired rows are deleted in chunks of four")

        # ---- refusals change nothing
        def refused(extra, needle, label):
            mark = writes()
            code, out, err = cli("--layer-url", url, *extra)
            check(code == 1 and needle in out + err and writes() == mark, label)

        refused(["--source", src("unk.json", [{"attributes": {"NOPE": 1, "CODE": 1}}]), "--apply"],
                "not in the layer: NOPE", "an unknown source column refuses and writes nothing")
        refused(["--source", src("empty.json", []), "--apply"], "no rows",
                "an empty source refuses and writes nothing  <-- pinned defect")
        refused(["--source", src3, "--batch-id", "B3", "--apply"], "already exists",
                "a reused batch id refuses and writes nothing")
        refused(["--source", src("bad.csv", "CODE\nx\n"), "--apply"], "not a whole number",
                "a bad csv value refuses and writes nothing")
        code, out, err = cli("--layer-url", url + "/", "--source", src3)
        check(code == 0, "a layer url with a trailing slash is accepted")
        stub.props["isView"] = True
        refused(["--source", src3], "view", "a view is refused before any source is read")
        del stub.props["isView"]
        stub.props["capabilities"] = "Query"
        refused(["--source", src3], "capabilities lack", "a layer that cannot edit is refused")
        stub.props["capabilities"] = "Create,Delete,Query,Update,Editing"
        stub.props["syncEnabled"] = True
        refused(["--source", src3], "sync is enabled", "a sync enabled layer is refused")
        mark = writes()
        code, out, err = cli("--layer-url", url, "--source", src3, "--allow-sync")
        check(code == 0 and writes() == mark, "--allow-sync lets the check run pass")
        del stub.props["syncEnabled"]
        stub.service["syncEnabled"] = True
        refused(["--source", src3], "sync is enabled", "a sync enabled service is refused")
        mark = writes()
        code, out, err = cli("--layer-url", url, "--source", src3, "--allow-sync")
        check(code == 0 and writes() == mark, "--allow-sync lets a sync enabled service pass the check")
        stub.service["syncEnabled"] = False
        stub.canned[("service", stub.calls("service") + 1)] = {"error": {"code": 403, "message": "no access"}}
        refused(["--source", src3], "no access", "a service root that cannot be read refuses before any write")
        stub.props["supportsRollbackOnFailureParameter"] = False
        code, out, err = cli("--layer-url", url, "--source", src3)
        check(code == 0 and "warning: layer does not honour rollbackOnFailure" in out
              and "treats it as true" in out, "a layer that does not honour rollback is noted")
        del stub.props["supportsRollbackOnFailureParameter"]
        stub.truncate_at = 2
        refused(["--source", src3], "read 2 row(s) but the count query says 3",
                "a layer that cuts a page short refuses  <-- pinned defect")
        stub.truncate_at = None

        # ---- state refusals through the whole tool
        stub.reset(3, "B3")
        stub.seed(2, "LEFT", STAGED)
        refused(["--source", src3, "--batch-id", "N1", "--apply"], "staged row(s) are left",
                "leftover staged rows refuse the next run")
        code, out, err = cli("--layer-url", url, "--source", src3, "--batch-id", "N1", "--discard-staged", "--apply")
        check(code == 0 and stub.number(STAGED) == 0 and stub.number(LIVE, "N1") == 3 and "discarded 2 staged" in out,
              "--discard-staged removes the leftovers and the run continues")
        stub.reset(3, "B3")
        stub.seed(2, "Z", LIVE)
        refused(["--source", src3, "--batch-id", "N2", "--apply"], "2 batches are live",
                "two live batches refuse the next run")
        refused(["--recover", "Q9", "--expect-rows", "1"], "has no staged or live rows", "recovering an unknown batch refuses")
        mark = writes()
        code, out, err = cli("--layer-url", url, "--recover", "B3", "--expect-rows", "3")
        check(code == 0 and "0 staged to promote, 2 live batch row(s) to retire" in out and writes() == mark
              and "Check passed" in out, "a recover check run reports the plan and writes nothing")
        code, out, err = cli("--layer-url", url, "--recover", "B3", "--expect-rows", "3", "--apply")
        check(code == 0 and stub.number(LIVE, "B3") == 3 and stub.number(LIVE) == 3 and stub.number(RETIRED, "Z") == 2,
              "--recover retires every other live batch")
        code, out, err = cli("--layer-url", url, "--recover", "B3", "--expect-rows", "3", "--apply")
        check(code == 0 and "0 staged to promote, 0 live batch row(s) to retire" in out and stub.number(LIVE) == 3,
              "recovering a batch that is already the only live batch changes nothing")
        stub.add({"CODE": 1, "NAME": "x"})
        stub.add({"CODE": 2, "NAME": "y", "batch_state": "weird"})
        refused(["--source", src3, "--apply"], "no valid batch_state", "rows with no valid state refuse the run")

        # ---- an empty source is allowed only on request
        stub.reset(3, "B3")
        code, out, err = cli("--layer-url", url, "--source", src("empty2.json", []), "--batch-id", "E0",
                             "--allow-empty", "--apply")
        check(code == 0 and stub.number(LIVE) == 0 and stub.number(RETIRED, "B3") == 3,
              "--allow-empty swaps in nothing and retires the old rows")

        # ---- usage and access
        mark = writes()
        code, out, err = cli("--layer-url", "http://h.example/a/FeatureServer/0", "--source", src3)
        check(code == 64 and "plain http" in err and writes() == mark, "plain http to a remote host is a usage error")
        code, out, err = cli("--layer-url", "https://bob:hunter2pw@h.example/a/FeatureServer/0", "--source", src3)
        check(code == 64 and "user name or password" in err and "hunter2pw" not in out + err and writes() == mark,
              "a password in the layer url is a usage error and is never printed  <-- pinned defect")
        code, out, err = cli("--source", src3)
        check(code == 64 and "http(s)" in err, "a missing layer url is a usage error")
        os.environ["ARCGIS_TOKEN"] = ""
        code, out, err = cli("--layer-url", url, "--source", src3, "--apply")
        check(code == 64 and "needs a token" in err and writes() == mark,
              "--apply without a token is a usage error  <-- pinned defect")
        os.environ["ARCGIS_TOKEN"] = _TOKEN
        code, out, err = cli("--layer-url", url, "--source", src3, "--apply", "--token-env", _TOKEN)
        check(code == 64 and "needs a token" in err and _TOKEN not in out + err and writes() == mark,
              "a token passed as the --token-env value is never echoed  <-- pinned defect")
        os.environ["ARCGIS_TOKEN"] = ""
        os.environ["BF_TOK"] = _TOKEN
        code, out, err = cli("--layer-url", url, "--source", src3, "--token-env", "BF_TOK")
        check(code == 0 and "Check passed" in out, "--token-env reads the token from the variable it names")
        os.environ.pop("BF_TOK")
        os.environ["ARCGIS_TOKEN"] = ""
        os.environ["ARCGIS_TOKEN"] = "wrong-token-777"
        code, out, err = cli("--layer-url", url, "--source", src3)
        check(code == 1 and "Invalid token" in err and "wrong-token-777" not in out + err,
              "a rejected token refuses and is not printed")
        os.environ["ARCGIS_TOKEN"] = _TOKEN

        # ---- csv into a table, and the default batch id
        stub.reset(0)
        stub.props = _stub_props(table=True)
        _NOW = lambda: datetime.datetime(2026, 3, 4, 5, 6, 7, tzinfo=datetime.timezone.utc)  # noqa: E731
        csv_path = src("t.csv", "NAME,CODE,SCORE,SEEN\na,1,1.5,1700000000000\n,2,,\n")
        code, out, err = cli("--layer-url", url, "--source", csv_path, "--apply")
        row_a = [r for r in stub.rows if r["CODE"] == 1][0]
        row_b = [r for r in stub.rows if r["CODE"] == 2][0]
        check(code == 0 and row_a["NAME"] == "a" and row_a["SCORE"] == 1.5 and row_a["SEEN"] == 1700000000000
              and row_b["NAME"] is None and row_b["SCORE"] is None and row_b["SEEN"] is None,
              "csv rows reach a hosted table as typed values and NULLs")
        check(row_a["batch_id"] == "20260304T050607Z" and row_a["batch_state"] == LIVE,
              "the default batch id comes from the clock")
        stub.props = _stub_props()

        # ---- rows the layer would store without a shape
        stub.reset(3, "B3")
        csv_pt = src("pt.csv", "NAME,CODE\na,1\n")
        refused(["--source", csv_pt, "--apply"], "have no geometry",
                "a csv into a point layer refuses and keeps the live rows  <-- pinned defect")
        check(stub.number(LIVE, "B3") == 3, "the live rows survive that refusal")
        code, out, err = cli("--layer-url", url, "--source", csv_pt, "--batch-id", "G1", "--allow-no-geometry", "--apply")
        check(code == 0 and stub.number(LIVE, "G1") == 1 and stub.geoms[stub.rows[-1]["OBJECTID"]] is None,
              "--allow-no-geometry loads the rows with no shape on request")
        refused(["--source", src("inf.json", '[{"attributes": {"CODE": 1}, "geometry": {"x": 1e999, "y": 1}}]'),
                 "--apply"], "not finite",
                "a coordinate that overflows to infinity refuses before any write  <-- pinned defect")

        # ---- spatial reference, through the whole tool
        stub.props["extent"] = {"spatialReference": {"wkid": 4326}}

        def sr_src(name, srs, extra=None, geom=True):
            doc = {"features": [_feat(i, geom) for i in range(3)]}
            if srs is not None:
                doc["spatialReference"] = srs
            doc.update(extra or {})
            return src(name, json.dumps(doc))

        stub.reset(5, "B3")
        refused(["--source", sr_src("w3857.json", {"wkid": 3857}), "--apply"], "use wkid 3857",
                "a source in another coordinate system refuses and writes nothing  <-- pinned defect")
        refused(["--source", sr_src("nosr.json", None), "--apply"], "name no spatial reference",
                "a source that names no spatial reference refuses when the layer names one  <-- pinned defect")
        refused(["--source", sr_src("cut.json", {"wkid": 4326}, {"exceededTransferLimit": True}), "--apply"],
                "exceededTransferLimit",
                "a cut-off query result refuses and keeps the live rows  <-- pinned defect")
        check(stub.number(LIVE, "B3") == 5 and len(stub.rows) == 5, "the live rows survive those refusals")
        code, out, err = cli("--layer-url", url, "--source", sr_src("w4326.json", {"wkid": 4326}),
                             "--batch-id", "S1", "--apply")
        check(code == 0 and stub.number(LIVE, "S1") == 3
              and all(stub.geoms[r["OBJECTID"]].get("spatialReference") == {"wkid": 4326}
                      for r in stub.rows if r.get("batch_id") == "S1"),
              "a source in the layer's own system loads and sends its spatialReference")
        code, out, err = cli("--layer-url", url, "--source", sr_src("nosr2.json", None),
                             "--batch-id", "S2", "--allow-unstated-srs", "--apply")
        check(code == 0 and stub.number(LIVE, "S2") == 3, "--allow-unstated-srs loads geometry that names none")
        del stub.props["extent"]
        code, out, err = cli("--layer-url", url, "--source", sr_src("w3857b.json", {"wkid": 3857}))
        check(code == 0 and "warning: layer JSON names no spatial reference" in out,
              "a layer that names no spatial reference is noted, and nothing is compared")

        # ---- faults while writing
        def fault(call_no, reply, args, label, needle, rows_after, code_want=3):
            stub.reset(0)
            stub.canned[("applyEdits", stub.calls("applyEdits") + call_no)] = reply
            code, out, err = cli("--layer-url", url, *args)
            check(code == code_want and needle in err and "Done." not in out and len(stub.rows) == rows_after,
                  label)

        a3 = ["--source", src3, "--batch-id", "F1", "--chunk", "4", "--apply"]
        fault(1, {"error": {"code": 400, "message": "Unable to complete operation."}}, a3,
              "an error object on the first add stops the run  <-- pinned defect",
              "FAILED at stage: ArcGIS REST error 400", 0)
        fault(1, {}, a3, "a reply with no addResults is not success  <-- pinned defect", "no addResults list", 0)
        fault(1, {"addResults": []}, a3, "a short addResults list is not success", "expected 3 addResults, got 0", 0)
        fault(1, {"addResults": [{"success": True}] * 3}, a3, "adds with no objectId are not success",
              "has no objectId", 0)
        fault(2, {"addResults": [{"success": True, "objectId": 90}, {"success": False,
                                  "error": {"code": 1019, "description": "bad geometry"}},
                                 {"success": True, "objectId": 91}]},
              ["--source", src7, "--batch-id", "F2", "--chunk", "3", "--apply"],
              "a failed row in the second add call is reported and the first call is removed",
              "1019 bad geometry; removed 3 staged row(s) of the failed batch", 0)

        fault(2, _CUT, ["--source", src7, "--batch-id", "F8", "--chunk", "3", "--apply"],
              "a reply cut off mid-body while staging exits 3 and removes the staged rows  <-- pinned defect",
              "IncompleteRead", 0)
        stub.reset(0)
        stub.props = _stub_props(table=True)
        stub.props["fields"].append({"name": "GID", "type": "esriFieldTypeGUID", "editable": True,
                                     "nullable": True})
        nan_src = src("nan.json", '[{"CODE": 1}, {"CODE": 2}, {"CODE": 3}, {"CODE": 4, "GID": 1e999}]')
        code, out, err = cli("--layer-url", url, "--source", nan_src, "--batch-id", "F9", "--chunk", "3", "--apply")
        check(code == 3 and "FAILED at stage: ValueError" in err and "removed 3 staged" in err and not stub.rows,
              "an unexpected error while staging removes the staged rows and exits 3  <-- pinned defect")
        stub.props = _stub_props()
        stub.reset(0)

        def boom(*_args):
            raise ZeroDivisionError("planned")

        plan_flip = boom
        code, out, err = cli("--layer-url", url, "--source", src3, "--batch-id", "F10", "--apply")
        plan_flip = real_plan
        check(code == 3 and "FAILED at unexpected: ZeroDivisionError" in err and "partly changed" in err,
              "an unexpected error after the first write exits 3, not 1  <-- pinned defect")

        # a --discard-staged that fails part way
        stub.reset(3, "B3")
        stub.seed(4, "LEFT", STAGED)
        stub.canned[("deleteFeatures", stub.calls("deleteFeatures") + 2)] = {"error": {"code": 500, "message": "boom"}}
        code, out, err = cli("--layer-url", url, "--source", src3, "--batch-id", "D1", "--chunk", "2",
                             "--discard-staged", "--apply")
        check(code == 3 and "FAILED at discard" in err and "may already be gone" in err
              and "Nothing was changed" not in err and stub.number(STAGED) == 2 and stub.number(LIVE, "B3") == 3,
              "a --discard-staged that fails part way exits 3 and says rows may be gone  <-- pinned defect")

        # cleanup that also fails leaves staged rows, and the next run says so
        stub.reset(0)
        n = stub.calls("applyEdits")
        stub.canned[("applyEdits", n + 2)] = {"error": {"code": 500, "message": "boom"}}
        stub.canned[("deleteFeatures", stub.calls("deleteFeatures") + 1)] = {"error": {"code": 500, "message": "boom2"}}
        code, out, err = cli("--layer-url", url, "--source", src7, "--batch-id", "F3", "--chunk", "3", "--apply")
        err0 = err
        check(code == 3 and "cleanup failed too" in err and stub.number(STAGED, "F3") == 3,
              "a failed cleanup is reported and the staged rows stay")
        refused(["--source", src3, "--batch-id", "F4", "--apply"], "staged row(s) are left",
                "the next run refuses until the leftovers are dealt with")
        check("batch F3 are incomplete" in err0 and "do not use --recover" in err0,
              "the cleanup failure says the staged rows are incomplete and not to recover them")
        mark = writes()
        code, out, err = cli("--layer-url", url, "--recover", "F3", "--expect-rows", "7", "--apply")
        check(code == 1 and "holds 3 row(s) (3 staged, 0 live) but --expect-rows says 7" in out
              and writes() == mark and stub.number(STAGED, "F3") == 3 and stub.number(LIVE) == 0,
              "--recover refuses a half-staged batch and changes nothing  <-- pinned defect")
        code, out, err = cli("--layer-url", url, "--recover", "F3", "--expect-rows", "3")
        check(code == 0 and "Check passed" in out, "--expect-rows that matches the staged rows passes the check")
        code, out, err = cli("--layer-url", url, "--source", src3, "--batch-id", "F4", "--discard-staged", "--apply")
        check(code == 0 and stub.number(LIVE, "F4") == 3 and len(stub.rows) == 3, "--discard-staged repairs it")

        # the staged count does not match what was sent
        stub.reset(0)
        q = stub.calls("query")
        stub.canned[("query", q + 5)] = {"count": 99}
        code, out, err = cli("--layer-url", url, "--source", src3, "--batch-id", "F5", "--chunk", "4", "--apply")
        check(code == 3 and "staged 3 row(s) but the layer holds 99" in err and len(stub.rows) == 0,
              "a staged count that does not match is a failure and the batch is removed")

        # the live count after the flip does not match
        stub.reset(0)
        q = stub.calls("query")
        stub.canned[("query", q + 6)] = {"count": 1}
        code, out, err = cli("--layer-url", url, "--source", src3, "--batch-id", "F6", "--chunk", "4", "--apply")
        check(code == 3 and "FAILED at verify: expected 3 live row(s) after the flip, found 1" in err,
              "a live count that does not match after the flip is a failure  <-- pinned defect")
        stub.reset(0)
        q = stub.calls("query")
        stub.canned[("query", q + 6)] = {"count": 5}
        code, out, err = cli("--layer-url", url, "--source", src3, "--batch-id", "F11", "--chunk", "4", "--apply")
        check(code == 3 and "expected 3 live row(s) after the flip, found 5" in err,
              "a live count that is too high after the flip is a failure  <-- pinned defect")
        stub.reset(0)
        q = stub.calls("query")
        stub.canned[("query", q + 6)] = {"error": {"code": 500, "message": "gone"}}
        code, out, err = cli("--layer-url", url, "--source", src3, "--batch-id", "F7", "--chunk", "4", "--apply")
        check(code == 3 and "could not count live rows" in err, "a count that fails after the flip is a failure")

        # a promote call fails part way, then --recover finishes the flip
        stub.reset(3, "OLD")
        n = stub.calls("applyEdits")
        stub.canned[("applyEdits", n + 5)] = {"error": {"code": 500, "message": "boom"}}
        code, out, err = cli("--layer-url", url, "--source", src7, "--batch-id", "N7", "--chunk", "3", "--apply")
        check(code == 3 and "promote chunk 2 failed" in err and "--recover" in err
              and stub.number(LIVE, "N7") == 3 and stub.number(STAGED, "N7") == 4 and stub.number(LIVE, "OLD") == 3,
              "a promote call that fails leaves both batches live and says how to finish")
        code, out, err = cli("--layer-url", url, "--recover", "N7", "--expect-rows", "7", "--chunk", "3", "--apply")
        check(code == 0 and stub.number(LIVE) == 7 and stub.number(LIVE, "N7") == 7 and stub.number(RETIRED, "OLD") == 3,
              "--recover promotes what is left and retires the old batch")

        # the flip failure names the real batch id and the row count that --recover needs
        stub.reset(3, "OLD")
        n = stub.calls("applyEdits")
        stub.canned[("applyEdits", n + 5)] = {"error": {"code": 500, "message": "boom"}}
        code, out, err = cli("--layer-url", url, "--source", src7, "--batch-id", "N6", "--chunk", "3", "--apply")
        check(code == 3 and "--recover N6 --expect-rows 7" in err,
              "the flip failure message names the batch id and the row count, not the word BATCH")
        code, out, err = cli("--layer-url", url, "--recover", "N6", "--expect-rows", "7", "--chunk", "3", "--apply")
        check(code == 0 and stub.number(LIVE, "N6") == 7 and stub.number(LIVE) == 7,
              "the command that the flip failure message prints finishes the flip")

        # a retire call fails, then --recover --purge finishes and cleans up
        stub.reset(3, "OLD")
        n = stub.calls("applyEdits")
        stub.canned[("applyEdits", n + 3)] = {"error": {"code": 500, "message": "boom"}}
        code, out, err = cli("--layer-url", url, "--source", src3, "--batch-id", "N8", "--chunk", "4", "--apply")
        check(code == 3 and "retire chunk 1 failed" in err and stub.number(LIVE) == 6,
              "a retire call that fails leaves the new batch live beside the old one")
        code, out, err = cli("--layer-url", url, "--recover", "N8", "--expect-rows", "3", "--chunk", "4", "--purge", "--apply")
        check(code == 0 and stub.number(LIVE) == 3 and stub.number(RETIRED) == 0 and len(stub.rows) == 3,
              "--recover with --purge retires and removes the old batch")

        # a purge that fails comes after a flip that worked
        stub.reset(3, "OLD")
        stub.canned[("deleteFeatures", stub.calls("deleteFeatures") + 1)] = {"error": {"code": 500, "message": "boom"}}
        code, out, err = cli("--layer-url", url, "--source", src3, "--batch-id", "N9", "--purge", "--apply")
        check(code == 3 and "FAILED at purge: the flip is done" in err and stub.number(LIVE, "N9") == 3,
              "a purge that fails is reported after the flip is done")

        # ---- nothing leaked, nothing written outside the service
        blob = "\n".join(outputs)
        check(_TOKEN not in blob, "the token is in no stdout or stderr from any run  <-- pinned defect")
        check(leaked == [], "no run wrote a file")
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
        _SLEEP, _NOW, plan_flip = saved[0], saved[1], real_plan
        for key, value in (("NO_PROXY", saved[2]), ("ARCGIS_TOKEN", saved[3])):
            os.environ.pop(key, None)
            os.environ.update(dict([(key, value)] if value is not None else []))
    # the server is gone now: a connection error is a failure, not a hang
    _SLEEP = lambda seconds: None  # noqa: E731
    try:
        failed = None
        try:
            call(url, {}, "")
        except RestError as exc:
            failed = str(exc)
        check(failed is not None and "after 3 attempt(s)" in failed, "a refused connection is a failure after three attempts")
    finally:
        _SLEEP = saved[0]


class _Harness(object):
    """The check()/raises() pair and the final count."""

    def __init__(self):
        self.passed = 0
        self.failed = []

    def check(self, cond, label):
        if cond:
            self.passed += 1
            print("PASS  %s" % label)
        else:
            self.failed.append(label)
            print("FAIL  %s" % label)

    def raises(self, fn, label, exc=BatchflipError):
        try:
            fn()
        except exc:
            self.check(True, label)
        except Exception as other:
            self.check(False, "%s (wrong exception %r)" % (label, other))
        else:
            self.check(False, "%s (no error raised)" % label)

    def finish(self):
        print("-" * 68)
        total = self.passed + len(self.failed)
        if self.failed:
            print("%d assertions, %d failed" % (total, len(self.failed)))
            for label in self.failed:
                print("  FAILED: %s" % label)
            return 1
        print("%d assertions, 0 failed" % total)
        return 0


def _t_harness(check):
    """The harness must fail loudly, so it is tested with assertions that are meant to fail."""
    inner = _Harness()
    saved = sys.stdout
    sys.stdout = io.StringIO()
    try:
        inner.check(True, "fine")
        inner.check(False, "bad condition")
        inner.raises(lambda: 1 // 0, "wrong exception", BatchflipError)
        inner.raises(lambda: None, "no exception")
        code = inner.finish()
        text = sys.stdout.getvalue()
    finally:
        sys.stdout = saved
    check(code == 1 and inner.passed == 1 and len(inner.failed) == 3,
          "the harness counts a failed condition, a wrong exception and a missing exception")
    check("3 failed" in text and "FAILED: no exception (no error raised)" in text
          and "wrong exception (wrong exception" in text, "the harness names each failure in its report")


def self_test():
    """Offline assertions, then the whole tool against a stub server on 127.0.0.1."""
    import shutil
    import tempfile
    harness = _Harness()
    check, raises = harness.check, harness.raises
    print("batchflip self-test: no ArcGIS, no credentials; the network tests use 127.0.0.1")
    print("-" * 68)
    tmp = tempfile.mkdtemp(prefix="batchflip-")
    try:
        _t_core(check, raises)
        _t_source(check, raises, tmp)
        _t_layer(check)
        _t_rows(check)
        _t_net(check, tmp)
        _t_harness(check)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    return harness.finish()


if __name__ == "__main__":  # pragma: no branch
    sys.exit(main())
