# batchflip

Swap a hosted table's contents in one visible step, instead of truncating it and appending.

> **Status: started, not built.** This repo holds the plan only. Nothing here runs yet.

## The disaster

A nightly job empties a hosted table and appends the new rows. For minutes, every reader sees an empty or half-filled table. If the append fails, the table stays empty until someone notices.

## The plan

1. Stage the new rows under a new batch id, marked `staged`, beside the live batch.
2. Promote the new batch by flipping its `batch_state`.
3. Retire the old batch.

Readers filter on `batch_state`, so they always see one complete batch.

The tool uses only the Python standard library and talks to the ArcGIS REST `applyEdits` and `query` endpoints. It is a dry run by default, and every write is behind `--apply`.

## Limits that will be on the first screen

- **The chunk ceiling.** Above one chunk, a reader can see a partial flip for the seconds between chunk calls. The tool narrows the window from minutes to seconds. It does not remove the window.
- **The usual better answer.** When rows have a stable key, `FeatureLayer.append(upsert=True, upsert_matching_field=KEY)` together with [hostedreap](https://github.com/uhsear/hostedreap) beats this tool.
- **The adoption cost.** The table needs two extra columns, and every reader needs a `batch_state` filter.

## Related

- [hostedreap](https://github.com/uhsear/hostedreap) deletes the rows that no longer exist in your source.
- [safe-republish](https://github.com/uhsear/safe-republish) refuses to truncate when the staged replacement fails a check.
- [restfake](https://github.com/uhsear/restfake) is a mock ArcGIS REST server for testing against real ArcGIS failure modes.
