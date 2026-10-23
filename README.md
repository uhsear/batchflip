# batchflip

Swap a hosted table's contents without ever emptying it, instead of truncating it and appending.
When the old and new rows fit in one `--chunk` (default 500), readers see the swap as one
`applyEdits` call. Above that, the tool promotes first and retires second, and readers can see
both batches for a few seconds.
Refuses to start when the layer or the new rows would make the swap fail, and refuses to call
an edit a success until every row in the reply says so.

> **Status: work in progress.** The code runs and the self-test passes. It has been run against a
> local stub server only, never against a live ArcGIS service. See [Limits](#limits).

## The disaster

A nightly job empties a hosted table and appends the new rows. For minutes, every reader sees an
empty or half-filled table.

Then one night the append call comes back with HTTP 200 and an `error` object in the body. The
job script checks that the call returned. It does not look inside. The log says `append ok`. The
table stays empty until a person opens the map the next morning.

The same night, a second job calls the same service with a source that has one column too few.
The service rejects half the rows. The reply lists `success: false` for each of them, and the
script counts the reply, not the flags. A third of the table is gone and nothing failed.

This is a pattern, not one incident. Both failures have one cause. The script trusted the call, not the reply. This tool keeps the old
rows until the new rows are staged and checked, and it reads every reply row by row.

## What this does, and what already exists

1. **Stage.** Add the new rows as a new batch, state `staged`. Readers ignore them.
2. **Promote.** Set the new rows to `live`.
3. **Retire.** Set the previous live rows to `retired`.

Readers filter on `batch_state = 'live'`, so they see one complete batch. When the new and old rows
together fit in one chunk, promote and retire go out in one `applyEdits` call. Above one chunk the
tool promotes first and retires second. A reader can then see both batches for a few seconds. A
reader never sees an empty table.

Before any write, the tool runs the overwrite preconditions (the `ovwcheck` checks, below). After
every write, it checks each row in the reply, then counts the live rows and compares the count.

What already exists, and what it does well:

- **Esri's own overwrite** ([Overwrite a hosted feature layer](https://doc.arcgis.com/en/arcgis-online/manage-data/overwrite-hosted-feature-layer.htm))
  keeps the layer URL, symbology and pop-ups, and it is one click or one API call. It replaces the
  data, so Esri says the layer "will be unavailable to other users for a brief time". It also
  needs the same fields, the same file type and the same file name as the original upload.
  Prefer it when a short outage is fine and you publish from a file.
- **Upsert append** (`FeatureLayer.append(upsert=True, upsert_matching_field=KEY)`) is the better
  answer when your rows have a stable key. It updates matching rows and adds the rest, so the table
  is never emptied, and it needs no extra column. The matching field must have a unique index
  ([ArcGIS API for Python, `FeatureLayer.append`](https://developers.arcgis.com/python/latest/api-reference/arcgis.features.toc.html)).
  Pair it with [hostedreap](https://github.com/uhsear/hostedreap), which deletes the rows that left the source.
- **A filtered hosted view.** Esri lets you "filter a view's contents by defining filter
  expressions" ([Create hosted feature layer views](https://doc.arcgis.com/en/arcgis-online/manage-data/create-hosted-views.htm)).
  A view filtered on `batch_state = 'live'` gives every reader the filter without asking each one
  to add it. It works well beside this tool.
- **Other scripts.** `gh search repos` on 2026-10-22 found update scripts such as
  [py3-portal-hosted-layer-updater](https://github.com/stratosigma/py3-portal-hosted-layer-updater).
  I did not review them, and I did not find one that stages a batch beside the live one. A search
  for the name `batchflip` on 2026-10-23 found only this repository.

## Self-test

The self-test needs no ArcGIS and no credentials. It starts a stub of a hosted layer on
`127.0.0.1` and runs the whole tool against it over real HTTP.

```
$ python batchflip.py --self-test
batchflip self-test: no ArcGIS, no credentials; the network tests use 127.0.0.1
--------------------------------------------------------------------
PASS  chunks splits a list with a short tail
...
PASS  a prefix --ap of a flag is REFUSED by the parser  <-- pinned defect
PASS  a prefix --appl of a flag is REFUSED by the parser  <-- pinned defect
...
PASS  a write is sent once and never retried  <-- pinned defect
PASS  an error object in an HTTP 200 reply is a failure  <-- pinned defect
...
PASS  a reply cut off mid-body is a failure, not a traceback  <-- pinned defect
...
PASS  a layer that cuts a page short refuses  <-- pinned defect
...
PASS  --recover refuses a half-staged batch and changes nothing  <-- pinned defect
PASS  a source in another coordinate system refuses and writes nothing  <-- pinned defect
PASS  a cut-off query result refuses and keeps the live rows  <-- pinned defect
...
PASS  a --discard-staged that fails part way exits 3 and says rows may be gone  <-- pinned defect
...
PASS  a token passed as the --token-env value is never echoed  <-- pinned defect
...
PASS  the token is in no stdout or stderr from any run  <-- pinned defect
...
--------------------------------------------------------------------
310 assertions, 0 failed
```

The count is 310 on Windows (Python 3.13.2) and on Linux (Python 3.12.3). The two full outputs are
the same, line for line (the Windows file ends its lines with CRLF). Branch coverage is 100 percent (1585 statements, 368 branches):

```
coverage run --branch batchflip.py --self-test
coverage report -m
```

Every line is reached offline. The only pragma is `# pragma: no branch` on the
`if __name__ == "__main__":` line, because a measured run is always a script.

## Quick start

```
set ARCGIS_TOKEN=<token>
python batchflip.py --layer-url https://HOST/arcgis/rest/services/NAME/FeatureServer/0 --source rows.json
python batchflip.py --layer-url https://HOST/arcgis/rest/services/NAME/FeatureServer/0 --source rows.json --apply
```

The first command is a check. It reads the layer and the source and writes nothing. Only `--apply`
writes. Here is a real run against the stub (the port changes on each run):

```
$ batchflip.py --layer-url http://127.0.0.1:62164/arcgis/rest/services/T/FeatureServer/0 --source night2.json --batch-id n2 --chunk 4 --apply
batchflip 0.1.0 (APPLY)
layer: http://127.0.0.1:62164/arcgis/rest/services/T/FeatureServer/0
live now: 3 row(s); staged leftovers: 0
source: 5 row(s), batch id n2
=== APPLY ===
  staged 5 row(s) as batch n2
  promote chunk 1: 4 row(s)
  promote chunk 2: 1 row(s)
  retire chunk 1: 3 row(s)
  verified: 5 live row(s)
Done.
[exit 0]
```

## Before the first run

The table needs two text columns: a batch id (length 40 or more) and a state (length 7 or more).
Their default names are `batch_id` and `batch_state`. Set them with `--batch-field` and
`--state-field`.

Rows that already exist need a state. The tool refuses to run while any row has no state, or a
state other than `staged`, `live` or `retired`. Set the existing rows to `live` and give them one
batch id (for example `legacy`) once, by hand. Then every reader adds `batch_state = 'live'`, or
reads a view that has that filter.

## Source files

- `.json`: Esri JSON. Either `{"features": [{"attributes": {...}, "geometry": {...}}]}`, a list
  of those features, or a list of flat attribute objects. `NaN` and `Infinity` are refused ("NaN in JSON raises", "Infinity in JSON raises").
- `.csv`: one header row, text cells. The tool converts each cell to the layer's field type. An
  empty cell is NULL. Dates must be whole numbers of epoch milliseconds. A CSV has no geometry.

A layer with a geometry type refuses a row that has no geometry, so a CSV fits a table but not a
point, line or polygon layer. Without that check the swap would retire every shape and leave only
rows with no shape. Pass `--allow-no-geometry` if you want that. A source in Esri JSON that has no
`geometry` key is treated the same way. An Esri JSON export that includes `OBJECTID` is refused
as "not editable". Remove that column first. An export taken from a hosted layer usually carries
other columns the layer marks not editable too, such as `GlobalID`, `Shape__Area` and
`Shape__Length`, and the tool refuses those the same way.

Geometry must be in the layer's own spatial reference, and the tool does not reproject. A
`spatialReference` at the top of the feature set applies to every geometry that has none of its own.
A source with `"exceededTransferLimit": true` is a query result that the service cut off at its
record limit, so the tool refuses it rather than swap in a partial table. See the
[ovwcheck](#the-overwrite-preconditions-ovwcheck) section for the spatial reference rules.

Column names match the layer without regard to case, and the tool writes them with the layer's
own spelling.

## Flags

| Flag | Default | What it does |
|---|---|---|
| `--layer-url` | none | The feature layer, ending in `/FeatureServer/<id>`. https only (plain http only for `127.0.0.1`, `localhost` and `::1`). No query string. |
| `--source` | none | New rows, `.json` or `.csv`. Give this or `--recover`. |
| `--recover BATCH` | off | Finish an interrupted flip. Promotes the staged rows of BATCH and retires every other live row. Needs `--expect-rows`. |
| `--expect-rows N` | none | With `--recover`: the row count of the whole batch. The run refuses when the staged plus live rows of BATCH are not N, because a batch whose staging failed is incomplete. |
| `--batch-id` | UTC timestamp | Id of the new batch. 1 to 40 of letters, digits, `_`, `.`, `-`. The first character is a letter or digit. |
| `--batch-field`, `--state-field` | `batch_id`, `batch_state` | The two columns. |
| `--chunk` | `500` | Rows per `applyEdits` call. This is a choice of this tool, not a service limit. |
| `--discard-staged` | off | Delete every staged row in the table (any batch), then continue. |
| `--purge` | off | After the flip, delete every retired row. |
| `--allow-empty` | off | Accept a source with no rows. This empties the table. |
| `--allow-no-geometry` | off | Accept rows with no geometry in a layer that has a geometry type. |
| `--allow-unstated-srs` | off | Accept geometry that names no spatial reference when the layer names one. You assert that it already uses the layer's. |
| `--allow-sync` | off | Accept a layer that has sync enabled. |
| `--token-env` | `ARCGIS_TOKEN` | The NAME of the environment variable that holds the token, never the token itself. The tool never prints this value. |
| `--apply` | off | Write. Without it the tool only reads. |
| `--self-test` | off | Run the offline assertions and the stub server, then exit. |

The parser sets `allow_abbrev=False`. A prefix of the write flag (`--ap`, `--app`, `--appl`) is
refused, and so are prefixes of `--source`, `--recover` and `--discard-staged`. The token comes from
the environment only, so it never sits in a process listing or a shell history.

Exit codes: `0` done or check passed, `1` refused (nothing was changed), `3` a write failed part
way (the message says where and how to finish), `64` usage error. The parser itself exits `2` for
an unknown or abbreviated flag. Exit `1` also covers a layer that could not be reached or read.
`REFUSED` lines go to stdout and read errors go to stderr, so read stderr to tell a failed
precondition from a failed read. An error that the tool did not
expect, once the first write may have gone out, exits `3`, not `1`.

## The overwrite preconditions (ovwcheck)

A failed overwrite that logs success has the same root as the disaster above: a precondition did
not hold, and nothing looked. Esri's overwrite page lists what must hold. This tool applies the
same kind of check before it writes. A failed check stops the run before the first write.

| Esri says (overwrite page) | What this tool does |
|---|---|
| "The same fields and feature classes ... must be present in the updated file" | Refuses a source column that the layer does not have, and a required layer field that the source lacks. |
| "the schema in the source file must be the same" | Checks every cell against the field type (whole number, number, text, date as epoch milliseconds), the string length and `nullable`. It reports counts by column, with the first bad row. |
| "The file name must be the same as the original file name." | Not applicable. No file is published. |
| "You must be the owner of the hosted feature layer or a member of the default administrator role." | Not checked. The owner is on the item, not on the layer URL. The capability check reads the `capabilities` that the layer reports. I did not test a token whose account lacks edit rights. |
| The option is unavailable for "a hosted feature layer view" | Refuses a layer with `isView: true`. That is a precaution drawn from the overwrite page, which says a view cannot be overwritten. Some views accept edits. |
| The option is unavailable when "Synchronization is enabled" | Reads the feature service root and refuses `syncEnabled: true`, unless you pass `--allow-sync`. A layer reply that carries `syncEnabled: true` is refused too. |
| "the schema in the source file must be the same" (geometry) | Compares the spatial reference of each geometry with the layer's `extent.spatialReference` (wkid and latestWkid, so 102100 and 3857 match). Refuses a different system, and refuses geometry that names none unless you pass `--allow-unstated-srs`. The tool does not reproject. A layer JSON with no spatial reference gets a warning and no comparison. |
| The option is unavailable when "You enabled data change tracking on the layer." | Not checked. I could not confirm the layer JSON property from the REST pages. |

Checks that come from the REST pages, not from the overwrite page:

- The layer lists `Query`, `Create`, `Update` and `Delete` in `capabilities`
  ([Feature layer](https://developers.arcgis.com/rest/services-reference/enterprise/feature-layer/)).
- Paging: `resultOffset` "only applies if `supportsPagination` is `true`", and `orderByFields`
  "is supported on only those layers/tables that indicate `supportsAdvancedQueries` is `true`"
  ([Query](https://developers.arcgis.com/rest/services-reference/enterprise/query-feature-service-layer/)).
  A layer that says `false` for either is refused.
- The geometry in the source must match `geometryType` (point, multipoint, polyline, polygon),
  every coordinate must be finite, and a table takes none. A layer that has a geometry type
  refuses rows with no geometry unless you pass `--allow-no-geometry`.
- No two batches may be live, no leftover staged rows may exist, and the new batch id must be new.
- The source must have rows, unless you pass `--allow-empty`.
- A feature set with `exceededTransferLimit: true` is refused. The
  [Query](https://developers.arcgis.com/rest/services-reference/enterprise/query-feature-service-layer/)
  page uses that flag for a result the service cut off.
- The feature layer reports its spatial reference inside `extent`
  ([Feature layer](https://developers.arcgis.com/rest/services-reference/enterprise/feature-layer/)).
  A geometry may carry an optional `spatialReference` with `wkid`, `latestWkid` or `wkt`
  ([Geometry objects](https://developers.arcgis.com/rest/services-reference/enterprise/geometry-objects/)).

`syncEnabled` is documented on the
[Feature service](https://developers.arcgis.com/rest/services-reference/enterprise/feature-service/)
page, so the tool reads the service root, which is one more read-only call. `isView` and
`defaultValue` are read from the layer JSON when present. The Feature layer page does not define
them, and I could not confirm the name `isView` against a live service. A missing property counts as
"not set".

## Reading the reply

`applyEdits` returns `addResults`, `updateResults` and `deleteResults`. Each result holds a boolean
`success`, and a failed result holds an `error` with `code` and `description`. A failed call can
also return a top-level `error` object
([Apply edits](https://developers.arcgis.com/rest/services-reference/enterprise/apply-edits-feature-service-layer/)).
That page does not state the HTTP status of the top-level case. The tool does not need to know. It
treats all of these as failure:

- a reply that is not a JSON object, or has a top-level `error`;
- a reply with no result list, or a list of the wrong length;
- any result whose `success` is not exactly `true`;
- a staged row that comes back without an integer `objectId`.

`rollbackOnFailure` is `true` on every write, on `applyEdits` and on `deleteFeatures`. The page says
it defaults to `true`. When the layer property `supportsRollbackOnFailureParameter` is `false`,
Esri says the service treats `rollbackOnFailure` "as if it were set to true, regardless of the
input". The Feature layer page shows the same property as `supportsRollbackOnFailures`, so the tool
reads both names. The tool notes the `false` case and still counts the live rows after the flip.
Deletes go by `objectIds`, so each deleted row has its own result
([Delete features](https://developers.arcgis.com/rest/services-reference/enterprise/delete-features/)).

A write is sent once, on `applyEdits` and on `deleteFeatures`. A timed out `applyEdits` may have
been applied, and a second send could add every row twice. Reads are tried up to three times.

## When a write fails

| Where | What the tool does | What you do |
|---|---|---|
| Before staging, with `--discard-staged` | Exits `3` and says some leftovers may already be gone. Nothing new was staged. | Run again with a new batch id. |
| While staging | Removes the staged rows of that batch (readers never saw them) and exits `3`. This includes a reply that is cut off mid-body and an error the tool did not expect. | Fix the cause and run again. |
| The cleanup also fails | Says so, names the batch id, and leaves the staged rows. The batch is incomplete. The next run refuses and gives the number of staged rows. | Run with `--discard-staged`. Do not use `--recover` on it: `--recover` refuses when `--expect-rows` does not match. |
| While promoting or retiring | Exits `3`. Readers may see both batches. The message prints the command to finish. | Run `--recover BATCH --expect-rows N --apply`, with the batch id and N from the message. It promotes what is left and retires the old batch. |
| The live count differs after the flip | Exits `3` with both numbers. | Look at the layer before you run again. |
| While purging | Exits `3`. The flip is already done. | Run `--recover BATCH --expect-rows N --purge --apply`. |

## What is exercised

Each claim in this file was run, against the stub over real HTTP unless it says otherwise. The
labels are the PASS lines of `--self-test`.

| Claim | Where it ran |
|---|---|
| Only `--apply` writes | "a check run writes nothing", and every refusal asserts the write count did not move |
| No run writes a file | "no run wrote a file" |
| A prefix of `--apply` is refused | "a prefix --ap/--app/--appl of a flag is REFUSED by the parser" |
| An error object in a 200 reply is a failure | "an error object in an HTTP 200 reply is a failure", "an error object on the first add stops the run" |
| A reply with no results, short results or a failed row is a failure | "a reply with no addResults is not success", "a short addResults list is not success", "a failed row in the second add call is reported and the first call is removed" |
| A write is never retried, a read is | "a write is sent once and never retried", "a deleteFeatures call is sent once and never retried", "a read is retried after two HTTP 500 replies, with backoff" |
| `--recover` cannot publish a partial batch | "--recover refuses a half-staged batch and changes nothing", "the flip failure message names the batch id and the row count, not the word BATCH" |
| A source in another coordinate system, or a cut-off export, is refused | "a source in another coordinate system refuses and writes nothing", "a source that names no spatial reference refuses when the layer names one", "a cut-off query result refuses and keeps the live rows" |
| `--token-env` never prints its value | "a token passed as the --token-env value is never echoed", "--token-env reads the token from the variable it names" |
| The token never reaches output | "the token is in no stdout or stderr from any run", "a rejected token refuses and is not printed" |
| A short page is caught | "a layer that cuts a page short refuses" |
| Chunking and order | "rows above one chunk promote every chunk before any retire", "the swap leaves the new batch live and the old batch retired" |
| Recovery | "--recover promotes what is left and retires the old batch", "--recover with --purge retires and removes the old batch" |
| Refusals change nothing | the `refused` checks, one per refusal, each asserting that the write count did not move |
| CSV into a table | "csv rows reach a hosted table as typed values and NULLs" |
| A cut-off reply, a password in the URL and a CSV into a point layer are handled | "a reply cut off mid-body while staging exits 3 and removes the staged rows", "a password in the layer url is a usage error and is never printed", "a csv into a point layer refuses and keeps the live rows" |
| Every delete asks for `rollbackOnFailure=true` | "every delete asks for rollbackOnFailure=true" |
| A live count that is too high or too low after the flip fails the run | "a live count that is too high after the flip is a failure", "a live count that does not match after the flip is a failure" |
| The checks fail when the code is wrong | I broke controls by hand, one at a time, and the self-test went red each time: error object, write retry, abbreviations, objectId check, `NaN`, the rollback flag on `applyEdits` and on `deleteFeatures`, the live count test (both directions), the extra-rows test in the read, the cut-off reply handler, the URL password check, the no-geometry check, the infinity check, the `--discard-staged` handler, the unexpected-error handler, the CSV reader limit and the newline in a field name. This was a manual sweep, not a script in this repo. I did not re-run it for the count reconcile, promote order, retire loop, empty source or token print controls after the last change. |

Not exercised against a real service: everything listed under Limits.

## Limits

- **Never run against a live ArcGIS service.** The stub is my reading of the REST pages cited above.
  It cannot tell me how a real hosted layer behaves.
- **The chunk ceiling.** Above one chunk, a reader can see both batches for the seconds between
  calls. The tool narrows the window from minutes to seconds. It does not remove it.
- **One call is not proven isolated.** `rollbackOnFailure=true` makes a failed call all or nothing.
  The pages do not say whether a reader can see a call half done, and I did not test it.
- **Adoption cost.** The table needs two extra columns, and every reader needs a `batch_state`
  filter (or a filtered view).
- **Retired rows stay** until you pass `--purge`, so the table grows by one batch per run.
- **Concurrent writers.** The tool reads the live object ids, then writes. Another editor's changes
  in between are not detected, except through the live count after the flip.
- **Not carried over:** attachments, related records, global ids and editor tracking values.
  New rows get new object ids.
- **Owner and change tracking** are not checked (see the table above).
- **Every call is a POST,** including the read of the layer JSON, so the token stays out of the URL.
  I did not confirm against Esri's pages that every resource accepts POST.
- **Sign-in:** a token in the environment only. There is no user name and password flow.
- **CSV:** an empty cell is NULL, so the tool cannot write an empty string from a CSV.
- **Python 3.9** is checked by hand, by parsing the file with `ast.parse(..., feature_version=(3, 9))`.
  That check is not part of the self-test. I ran the self-test on 3.12 and 3.13 only.
- **Branch coverage is 100 percent,** so no line is unreached offline.

Edge cases that I know about and have not fixed. Each is one sentence.

- Check mode does not check integer ranges (a value too big for a SmallInteger passes), coded-value
  domains or geometry validity, so "Check passed" does not guarantee that the service accepts every
  row. A rejected row fails at staging, the batch is removed, and the run exits `3`.
- The spatial reference check compares well-known ids and wkt text only. It does not reproject, and it
  cannot compare a layer whose JSON names no spatial reference (the tool warns). I did not test a
  custom spatial reference that carries only `wkt` against a live service.
- The tool does not model the field types GUID, GlobalID, Blob, DateOnly, TimeOnly and
  TimestampOffset, so any source value for them goes to the service as given.
- A required field that has a `defaultValue` is accepted when the column is absent everywhere, but
  refused ("null in a field that does not allow null") when the column exists and one row omits it.
- Python's `int()` and `float()` accept `1_000` and non-ASCII digits, so a CSV cell `1_000` is
  written as `1000` in a whole-number field.
- A JSON number such as `5.0` in a whole-number field is refused as "not a whole number", which may
  surprise you with a pandas export.
- A flat JSON attribute object that has a key named `attributes` is read as an Esri feature wrapper.
- A server that ignores `resultOffset` while it reports `supportsPagination` as true can return
  duplicate object ids. The tool does not remove them, and the live count after the flip then fails
  the run with exit `3`.
- A live service may behave differently from the stub. I did not confirm that a live service accepts
  adds with no geometry, that the layer JSON answers a POST, or the exact shape of live replies.
- The live count after the flip counts rows. It does not check which rows are live.
- `--discard-staged` deletes the staged rows of every batch, so do not run two flips against one
  table at the same time. It also fails ("batch id already exists") when you re-run it with the
  same explicit `--batch-id` as a run that failed part way. Use a new batch id.
- `urllib` follows redirects. A 301, 302 or 303 turns the POST into a GET with no body, so the
  reply is an error and the run refuses. A 307 or 308 raises an error, so the token does not follow
  a redirect to another host. I checked this by hand against a throwaway local server, not in the
  self-test.
- The tool prints text unescaped. On a Windows console or pipe that uses `cp1252`, a `REFUSED` line
  (printed to stdout) that holds a column name outside that code page raises `UnicodeEncodeError`,
  and the run ends in a traceback with exit `1`. Stderr is safe, and the apply-time `FAILED` path
  still exits `3`. Set `PYTHONUTF8=1` to avoid it.
- A bug in the tool before the first write ends in a Python traceback with exit `1`, the same code
  as a refusal.
- The sample run above cannot be replayed from the shipped files, because the stub exists only
  inside `--self-test`. Its lines come from the tool's own log formats.
- An automated sweep of the decision core (485 sites) left 8 survivors. Three were test gaps (a
  `deleteFeatures` retry, a string field of length 1, a geometry that is not an object), and the
  self-test now pins them. I did not re-run the sweep after that. Three equivalent mutants remain:
  the `urlencode` `doseq` flag, the 60 second timeout and the id count in a message. The sweep
  script is not in this repository. In that sweep five controls (no geometry, short `addResults`,
  batch column length, `objectId` and URL password) went red through a crash, not a named assertion.
- The hand sweep did not cover the row numbers in parse errors, `json.dumps(allow_nan=False)` or
  the guard on the retry sleep. Messages and boundaries there are not pinned.
- A JSON source nested about 1000 levels deep raises `RecursionError`, and a CSV header row longer
  than 131072 characters raises `csv.Error`. Both end in a Python traceback with exit `1` before any
  write, not in a refusal message.
- A duplicate key inside one JSON attribute object (two `CODE` keys, for example) is accepted, and
  the last value wins with no warning.
- A CSV header with a trailing comma creates a column with an empty name, so the refusal reads
  "source column(s) not in the layer:" with nothing after the colon.
- `--recover` does not run the checks for a NULL or unknown state or for two live batches that a
  normal run does, so such rows stay as they are and the run does not report them.
- Server error text is printed unchanged and is not scrubbed. A service that echoes your token in an
  error message puts it in the output.
- `urllib` uses the proxy settings in your environment (`HTTP_PROXY`, `HTTPS_PROXY`), including for a
  plain `http` loopback target. Set `NO_PROXY` for a loopback target, or the token goes to the proxy.
- Do not pipe `--apply` output to a command that can close early. A closed pipe raises inside the log
  call, the run exits `3` as "FAILED at unexpected: BrokenPipeError", and any rows already staged
  stay staged.
- Ctrl+C is not an `Exception`, so an interrupt prints a Python traceback with no "FAILED at"
  message. An interrupt during staging leaves staged rows behind, and the next run refuses until you
  pass `--discard-staged`. It does not print a `--recover` command.
- Check mode trusts the `capabilities` that the layer reports, not your token's own edit rights. A
  read-only token passes the check, fails at the first add with exit `3`, and may print "cleanup
  failed too", which is misleading because nothing was staged.
- Tokens that are bound to a referer are not supported, because the tool sends no `Referer` header.
  I did not test this against a live service.
- `--discard-staged` trusts the server's count query. A service that wrongly answers `0` leaves the
  stale staged rows, and the run still exits `0`.
- `--batch-field` and `--state-field` with non-default names are not run end to end in the
  self-test. The checks and the staged rows use the names, but only the defaults go through the stub.
- Pass `--batch-id` yourself in unattended jobs. The default is a timestamp, and a failure message
  prints it, but a log that was lost leaves you to find the id in the table.

## Contributing

Open an issue or pull request on GitHub.

## Author

Built by [Asir Khan](https://www.linkedin.com/in/asir-khan-310317264/).

## License

MIT.

## Related

- [hostedreap](https://github.com/uhsear/hostedreap) deletes the rows that no longer exist in your source.
- [safe-republish](https://github.com/uhsear/safe-republish) refuses to truncate when the staged replacement fails a check.
- [restfake](https://github.com/uhsear/restfake) is a mock ArcGIS REST server for testing against real ArcGIS failure modes.
