"""
Event Reconciliation tool -- daily data refresh task.

Reference implementation to adapt into the existing Airflow DAG that refreshes
TDS_DB.BI_DEV.ANALYTICS_EVENT_SPEC_RECONCILIATION. Add this as the LAST task in that
DAG (downstream of the refresh task) so it always runs against fresh data:

    refresh_reconciliation_table >> refresh_event_audit_json

It re-pulls the two scoped queries this project's build_dataset.py has always
used, rebuilds the same star-schema JSON the frontend (app.js) expects, and
overwrites the single stable blob at https://<store>.public.blob.vercel-storage.com/reconciliation.json.
No Vercel-side rebuild/redeploy is needed for a data refresh -- the static app
fetches this URL directly (see DATA_URL in app.js).

This is a standalone port of build_dataset.py's transform logic (not an
import of it) since this script is meant to live in the Airflow DAGs repo,
not the app repo. If the fact column layout ever changes, update BOTH:
  - app.js's `C` column-index map and `cols` comment
  - the FACT COLUMNS section below

Requires:
    pip install snowflake-connector-python vercel

Configure via Airflow Variables (or swap for your existing secrets backend):
    SNOWFLAKE_CONN_ID              -- an existing Airflow Snowflake connection
                                       (reuse whatever the table-refresh task uses)
    VERCEL_BLOB_READ_WRITE_TOKEN   -- from the Vercel dashboard: the
                                       tds-event-audit project -> Storage ->
                                       tds-event-audit-data -> copy the
                                       BLOB_READ_WRITE_TOKEN. Store this as an
                                       Airflow Variable/Secret, never in code.
"""

from __future__ import annotations

import datetime as dt
import re

from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable

SNOWFLAKE_CONN_ID = "snowflake_ds"  # matches conn_id used by the "base" task in dsi_analytics_event_spec_reconciliation.py

MAIN_QUERY = """
    SELECT APP_NAME, APP_VERSION, PACKAGE, PACKAGE_VERSION, PACKAGE_VERSION_BASE,
           EVENT_NAME_PACKAGE, FIELD_NAME_PACKAGE, FIELD_TYPE_PACKAGE, FIELD_TYPE_GAME,
           DECORATED_BY, IS_PRERELEASE, MATCH_STATUS, EVENT_MATCH_STATUS,
           FIRST_SEEN_VERSION_GAME, LAST_SEEN_VERSION_GAME, LAST_SEEN_DATE_GAME
    FROM TDS_DB.BI_DEV.ANALYTICS_EVENT_SPEC_RECONCILIATION
    WHERE MATCH_STATUS IN ('BOTH','PACKAGE_ONLY') AND APP_NAME IS NOT NULL
"""

# Scoped to app-versions that already appear in MAIN_QUERY -- see the app's
# README ("Why GAME_ONLY is scoped") for why the unscoped ~1.8M-row table
# isn't pulled whole: those app-versions are unreachable via the UI's App /
# App Version filters anyway, since those filters are built from MAIN_QUERY.
GAMEONLY_QUERY = """
    SELECT g.APP_NAME, g.APP_VERSION, g.EVENT_NAME_GAME, g.FIELD_NAME_GAME,
           g.FIELD_TYPE_GAME, g.EVENT_MATCH_STATUS, g.FIRST_SEEN_VERSION_GAME,
           g.LAST_SEEN_VERSION_GAME, g.LAST_SEEN_DATE_GAME
    FROM TDS_DB.BI_DEV.ANALYTICS_EVENT_SPEC_RECONCILIATION g
    WHERE g.MATCH_STATUS = 'GAME_ONLY' AND g.APP_NAME IS NOT NULL
      AND EXISTS (
        SELECT 1 FROM TDS_DB.BI_DEV.ANALYTICS_EVENT_SPEC_RECONCILIATION r
        WHERE r.MATCH_STATUS IN ('BOTH','PACKAGE_ONLY') AND r.APP_NAME IS NOT NULL
          AND r.APP_NAME = g.APP_NAME AND r.APP_VERSION = g.APP_VERSION
      )
"""

STATUS_CODE = {"PACKAGE_ONLY": 0, "BOTH": 1, "GAME_ONLY": 2}

# fact column order -- must match app.js's `C` map exactly
COLS = [
    "APP_NAME", "APP_VERSION", "PACKAGE", "PACKAGE_VERSION", "PACKAGE_VERSION_BASE",
    "EVENT_NAME", "FIELD_NAME", "FIELD_TYPE_PACKAGE", "FIELD_TYPE_GAME", "DECORATED_BY",
    "MATCH_STATUS", "IS_PRERELEASE", "FIRST_SEEN_VERSION_GAME", "LAST_SEEN_VERSION_GAME",
    "LAST_SEEN_DATE_GAME", "EVENT_MATCH_STATUS",
]


def _build_dataset(main_rows: list[dict], gameonly_rows: list[dict]) -> dict:
    """Star-schema transform -- identical logic to build_dataset.py."""
    strings: list[str] = []
    str_idx: dict[str, int] = {}

    def sidx(v):
        if v is None or v == "":
            return None
        v = str(v)
        i = str_idx.get(v)
        if i is None:
            i = len(strings)
            strings.append(v)
            str_idx[v] = i
        return i

    facts = []
    for r in main_rows:
        if r.get("APP_NAME") is None:
            continue
        facts.append([
            sidx(r.get("APP_NAME")), sidx(r.get("APP_VERSION")),
            sidx(r.get("PACKAGE")), sidx(r.get("PACKAGE_VERSION")), sidx(r.get("PACKAGE_VERSION_BASE")),
            sidx(r.get("EVENT_NAME_PACKAGE")), sidx(r.get("FIELD_NAME_PACKAGE")),
            sidx(r.get("FIELD_TYPE_PACKAGE")), sidx(r.get("FIELD_TYPE_GAME")),
            sidx(r.get("DECORATED_BY")),
            STATUS_CODE[r["MATCH_STATUS"]],
            1 if r.get("IS_PRERELEASE") else 0,
            sidx(r.get("FIRST_SEEN_VERSION_GAME")), sidx(r.get("LAST_SEEN_VERSION_GAME")),
            sidx(r.get("LAST_SEEN_DATE_GAME")),
            STATUS_CODE[r["EVENT_MATCH_STATUS"]],
        ])

    for r in gameonly_rows:
        if r.get("APP_NAME") is None:
            continue
        facts.append([
            sidx(r.get("APP_NAME")), sidx(r.get("APP_VERSION")),
            None, None, None,
            sidx(r.get("EVENT_NAME_GAME")), sidx(r.get("FIELD_NAME_GAME")),
            None, sidx(r.get("FIELD_TYPE_GAME")),
            None,
            STATUS_CODE["GAME_ONLY"],
            0,
            sidx(r.get("FIRST_SEEN_VERSION_GAME")), sidx(r.get("LAST_SEEN_VERSION_GAME")),
            sidx(r.get("LAST_SEEN_DATE_GAME")),
            STATUS_CODE[r["EVENT_MATCH_STATUS"]],
        ])

    app_versions: dict[int, set[int]] = {}
    packages: set[int] = set()
    package_versions: dict[int, set[int]] = {}
    app_packages: dict[int, set[int]] = {}
    for row in facts:
        app_i, ver_i, pkg_i, pkgver_i = row[0], row[1], row[2], row[3]
        if app_i is not None and ver_i is not None:
            app_versions.setdefault(app_i, set()).add(ver_i)
        if pkg_i is not None:
            packages.add(pkg_i)
            if app_i is not None:
                app_packages.setdefault(app_i, set()).add(pkg_i)
            if pkgver_i is not None:
                package_versions.setdefault(pkg_i, set()).add(pkgver_i)

    def sortedver(idx_set):
        def key(i):
            parts = re.split(r"[._-]", strings[i])
            return [int(p) if p.isdigit() else p for p in parts]
        return sorted(idx_set, key=key)

    apps = sorted(app_versions.keys(), key=lambda i: strings[i])
    packages_list = sorted(packages, key=lambda i: strings[i])

    return {
        "generatedAt": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M"),
        "strings": strings,
        "cols": COLS,
        "statusCodes": STATUS_CODE,
        "facts": facts,
        "apps": apps,
        "packages": packages_list,
        "appVersions": {str(i): sortedver(v) for i, v in app_versions.items()},
        "packageVersions": {str(i): sortedver(v) for i, v in package_versions.items()},
        "appPackages": {str(i): sorted(v, key=lambda x: strings[x]) for i, v in app_packages.items()},
    }


@task(task_id="refresh_event_audit_json")
def refresh_event_audit_json() -> None:
    import json
    from vercel.blob import BlobClient

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    main_rows = hook.get_pandas_df(MAIN_QUERY).to_dict("records")
    gameonly_rows = hook.get_pandas_df(GAMEONLY_QUERY).to_dict("records")

    dataset = _build_dataset(main_rows, gameonly_rows)

    # sanity check before publishing -- refuse to overwrite good data with an
    # empty/broken pull (e.g. a transient Snowflake or connection issue)
    if len(dataset["facts"]) < 1000:
        raise ValueError(
            f"refresh_event_audit_json: only {len(dataset['facts'])} facts pulled, "
            "expected 100k+. Refusing to overwrite the published dataset."
        )

    body = json.dumps(dataset, separators=(",", ":")).encode("utf-8")

    client = BlobClient(token=Variable.get("VERCEL_BLOB_READ_WRITE_TOKEN"))
    client.put(
        "reconciliation.json",
        body,
        access="public",
        add_random_suffix=False,
        overwrite=True,
        cache_control_max_age=3600,
        content_type="application/json",
    )
