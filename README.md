# Analytics Package Implementation

A local rebuild of the Event Reconciliation tool. Compares game-side
telemetry against package-side event specs — on the event itself, its
fields, and field data types — from
`TDS_DB.BI_DEV.ANALYTICS_EVENT_SPEC_RECONCILIATION`.

**UI fidelity source: `../design_handoff_event_reconciliation/`** (its
`README.md` is the authoritative visual/interaction spec — colors, spacing,
type, copy, dropdown/chip behavior). `index.html`/`styles.css`/`app.js` here
implement that spec against this project's own data pipeline; the handoff's
`.dc.html` + `support.js` are a design reference only, never ported directly.
Notable interaction details carried over: an emptied filter (unticking `All`
with nothing re-checked) reverts to "all" once its dropdown closes rather
than silently filtering to nothing; every active filter/bucket/checkbox
value appears as a removable "In scope" chip, in the filter bar for the four
main filters, and in the Events panel for Event + status. (RC Version is on
by default and deliberately never shown as a chip.)

This replaced an earlier, differently-structured local tool in this same
folder (single-page event list + deepdive). The current version has: a
scorecard row, an "App to Package Usage" rollup panel, events grouped by app
version (each with its own capped/scrollable list), and a **Game-only**
bucket for telemetry that has no package spec behind it at all.

## Run

```bash
python3 -m http.server 4174 --directory event-reconciliation-tool
```

Open <http://localhost:4174>. Must be HTTP — `file://` blocks the JSON fetch.

## Data model

Two status columns drive everything, at two different grains:

| Column | Grain | Meaning |
|---|---|---|
| `MATCH_STATUS` | per field row | is *this field* declared by the package, observed by the game, or both |
| `EVENT_MATCH_STATUS` | per (app, app_version, event) | is *this event as a whole* declared, observed, or both — constant across every field row sharing that context, regardless of individual fields' `MATCH_STATUS` |

| Status | Meaning | `PKG` column (field rows with this `MATCH_STATUS`) |
|---|---|---|
| `BOTH` | package declares it, telemetry confirmed it | set |
| `PACKAGE_ONLY` | package declares it, never observed | set |
| `GAME_ONLY` | telemetry fired it, no package declares this exact field | **null** |

Every fact row in `data/reconciliation.json` is one of these three at the
field grain, unified into a single 16-column array (see the header comment
in `app.js` for the exact column indices) — a game-only field is just a fact
with `PKG = null` and `MATCH_STATUS = GAME_ONLY`, flowing through the exact
same grouping/filtering code as everything else.

**The Layer-1/deepdive event status badge (Used/Package-only/Game-only)
comes from `EVENT_MATCH_STATUS`, not from aggregating field-level
`MATCH_STATUS`.** The two can disagree for one event: e.g. `Game_End` might
be `EVENT_MATCH_STATUS = BOTH` (the package declares it, telemetry
confirms it) while individual extra fields on that same event — ones the
game fires but no package ever declared — are `MATCH_STATUS = GAME_ONLY`.
Those fields still show up in the deepdive Fields table tagged
`Game-only`, but they don't count toward the event's badge or its Field
Usage ratio (see "Events" below).

This also fixed a real grouping bug: earlier builds grouped events by
`(package, event name)`, which rendered one logical event as two rows
whenever it had both package-matched fields and extra undeclared fields
(different `PKG` values, same event) — e.g. sushiflow 1.02.02's `Game_End`
showed a phantom second "Game-only" row. Events are now grouped by
`(app, app_version, event name)` alone, with every contributing package
listed on the row/deepdive header. This also correctly merges the rare case
of one event name declared by more than one package in the same
app-version (confirmed for 11 event names globally, e.g. jelly 1.07.01's
`Screen_Interaction` declared by both `com.tripledot.analytics-events` and
`com.tripledot.creatives`) into a single row rather than splitting it.

**Game-only fields have no package type to compare against.** For an event
whose `EVENT_MATCH_STATUS` is itself `GAME_ONLY` (no package spec exists for
it at all), every observed field counts as "covered" by convention — usage
always reads N/N, mirroring the design's own mock data (`3/3`). For an event
that *does* have a package spec (`Used`/`Package-only`), field usage is
`fields confirmed BOTH ÷ fields the package declares` (`MATCH_STATUS` ∈
{BOTH, PACKAGE_ONLY}) — any extra `MATCH_STATUS = GAME_ONLY` fields on that
event are real, undeclared telemetry and still listed in the Fields table,
but don't inflate or deflate the ratio. Both conventions are implemented
identically in three places: the Events panel row, the Layer-2 `FIELD USAGE`
stat, and the "Contexts in scope" table.

### Why `GAME_ONLY` is scoped to existing app-versions

As of the 2026-09-15 table rebuild, `MATCH_STATUS='GAME_ONLY'` alone is
**1,763,212 rows** — deduplicating by (app, version, event, field, type)
barely helps (1,763,158 distinct tuples; almost no duplication to collapse).
The real driver is high, sometimes messy cardinality: 14,414 distinct field
names across game-only rows, some clearly templated (`Game_End{N}_Blocktava`,
one single event with 11,957 distinct "field names"), and 3,002 distinct
(app, version) pairs — far more than the ~110 that actually have package
reconciliation data.

Since the App/App Version filters are built entirely from the `BOTH`/
`PACKAGE_ONLY` data, an app-version with **no** package context can never be
selected anyway — so game-only rows for it are unreachable regardless. The
pull restricts to `MATCH_STATUS='GAME_ONLY' AND EXISTS (... app-version also
has BOTH/PACKAGE_ONLY rows ...)`, cutting 1.76M rows to **97,367** with no
loss of anything the UI could actually show.

## Filters

App / App Version / Package / Package Version — multi-select, no checkboxes
(design decision): rows highlight blue when selected, hover reveals an
**Only** shortcut, a type-to-filter search box narrows the list live.
Unticking `All` clears the selection to empty (shows nothing) rather than
leaving everything silently checked.

Dependency chain: App → App Version options and Package options both narrow;
(App + App Version + Package) → Package Version narrows tightest, since it's
a live scan of the fact table rather than a static index (three filters at
once). **RC Version** (`IS_PRERELEASE`) sits in the usage panel header, is
**checked by default**, and cascades everywhere — scorecards, usage table,
events, and the deepdive; unchecking it excludes prerelease rows. Layer 1
**always includes decorated fields** (there is no Layer-1 Decorated Field
filter); the only Decorated Field toggle is local to the Layer-2 Fields
panel (see below). Game-only rows always carry `IS_PRERELEASE=0` and a null
`DECORATED_BY`, so neither setting can hide them.

**Package / Package Version filters and game-only fields.** A game-only
field has no package of its own, so these filters would otherwise drop every
undeclared field as soon as a package is picked. Instead an extra field
follows its event: it passes when the same (app, version, event) has a
declared row from a selected package/version. Events with no package
context at all (`EVENT_MATCH_STATUS=GAME_ONLY`) still can't match a package
filter.

## Scorecards

All five are computed over the four main filters + RC Version (not the
bucket tabs — those are events-list-only):

- **Package Usage** — distinct `(app, package)` pairs in scope, version-independent.
- **Event Coverage** — distinct declared events confirmed `BOTH` ÷ distinct declared events (`BOTH`+`PACKAGE_ONLY`). Best-case across contexts: an event counts as confirmed if it's `BOTH` *anywhere* in scope.
- **Field Coverage** — same idea at field grain.
- **Data Type Mismatch** — count of fields that are confirmed matched but have an incompatible type in *every* context where they're matched (best-case the other way: a field that's compatible anywhere doesn't count as a mismatch).
- **Game-Only Event** — distinct event *names* with `MATCH_STATUS=GAME_ONLY` in scope (not occurrences — consistent with how Event Coverage counts distinct names too).

## Page 1 — App to Package Usage + Events

**App to Package Usage**: one row per distinct `(app, app_version, package,
package_version, package_version_base)`. `Event Usage` = distinct events
confirmed `BOTH` ÷ distinct events declared, for that exact combo (the
finest grain, no aggregation needed).

**Events**: grouped by `(app, app_version)`, collapsed by default (auto-opens
if filters narrow to exactly one app-version), each group capped at a
10-row-visible scroller with its own sticky header. Within a group, events
are further grouped by event **name alone** — one row per event name per
app-version, regardless of how many packages contribute fields to it (see
"Data model" above) — with status (`Used`/`Package-only`/`Game-only`) read
directly from `EVENT_MATCH_STATUS` and field coverage resolved for *that
specific context*. The row's package line lists every contributing package.
Sort: Used → Package-only → Game-only, then alphabetical. Tabs
(All/Used/Package-only/Game-only) plus Event and Field dropdown filters
(options scoped to whatever the main filters leave visible) narrow the list
further. Columns: Event Name, Last Seen, First/Last Seen Ver, Field
Coverage, **Game-Only Field** (distinct fields the game fires that no
package declares for that event; purple when > 0), Type Issues.

## Page 2 — event deepdive

Click any event row. Breadcrumb + scope pill name the exact context you
drilled in from. `FIELD USAGE` / `TYPE MISMATCHES` / `GAME-ONLY FIELD` stat
cards are recomputed for that context specifically. **Fields** table lists every field observed
for this event in this context — fields the package(s) declare *and* any
extra undeclared fields the game fires — with per-field status resolved
against the active scope from `MATCH_STATUS`, so a field can show
`Package-only` or `Game-only` here even though the event overall is `Used`.
`Issues only` filters to anything not `Used`. A **Decorated Field** checkbox
(checked by default) sits beside it: unchecking drops decorated fields from
the table, the stat cards and the contexts table. The last column,
`DECORATED FIELD`, reads `yes`/`no` per field (`DECORATED_BY` populated or
not; game-only fields are always `no`). **Contexts in scope** lists
every other `(app, app_version)` carrying this same event, restricted to
the current main-filter scope (not global) — its Package Version column
lists every package version contributing to that context; click a row to
jump the deepdive there instead of going back.

Type compatibility (`app.js: pkgTypeBucket` / `gameTypeBucket` /
`typesCompatible`): `int/long/float/double/decimal` → number, `bool` →
boolean, `T[]`/`Dictionary<...>` accept a `string` observation (common
serialize-as-string behavior, not treated as a real mismatch).

## Data pipeline

Two raw pulls, merged by `build_dataset.py` into one star-schema
`data/reconciliation.json` (string-interned dictionary + integer-indexed
fact rows, ~8.8 MB for 152,992 facts, 16 columns including `EVENT_MATCH_STATUS`):

- `raw_main.json` — `SELECT ... MATCH_STATUS, EVENT_MATCH_STATUS, ... WHERE MATCH_STATUS IN ('BOTH','PACKAGE_ONLY') AND APP_NAME IS NOT NULL` (55,625 rows)
- `raw_gameonly.json` — `MATCH_STATUS='GAME_ONLY'`, plus `EVENT_MATCH_STATUS`, scoped to app-versions already in the main pull (97,367 rows, see "Why GAME_ONLY is scoped" above)

To refresh: re-run both pulls, overwrite the two raw files, `python3
build_dataset.py`, then **bump the `?v=` query on `app.js`/`styles.css` in
`index.html` and `DATA_VERSION` in `app.js`** — the preview proxy has
repeatedly been observed serving a stale cached copy after a rebuild, even
across hard-reloads and new tabs, so a version bump is the only reliable way
to force a refetch.

## Known gaps (mockup, not production)

- **No live Snowflake connection.** Point-in-time snapshot; no refresh
  button. Rebuilding needs re-running the two extractions above.
- **Game-only events outside any app-version that has package data are
  invisible by design** (see "Why GAME_ONLY is scoped") — this is a
  deliberate, discussed-with-the-user scope limit, not an oversight, but
  it means an app with *zero* package integration shows nothing at all,
  even if it fires plenty of telemetry.
- **`app_version`/`package_version` filter values are exact strings**, not
  semver-aware beyond the natural-sort used for display order.
