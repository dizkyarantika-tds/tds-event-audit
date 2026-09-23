import json
import re
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from airflow.sdk import DAG, Variable, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from utils.dag_builder import default_args, task_default_args

DAG_DIR = Path(__file__).parent

# --- Event Audit tool refresh -------------------------------------------
# Pulls the two scoped queries the tool has always used, rebuilds the same
# star-schema JSON its frontend expects, and publishes it to Vercel Blob so
# https://tds-event-audit.vercel.app always shows today's data. Runs after
# the "base" task below so it never reads a half-refreshed table.
# Full context: https://github.com/dizkyarantika-tds/tds-event-audit

MAIN_QUERY = """
    SELECT APP_NAME, APP_VERSION, PACKAGE, PACKAGE_VERSION, PACKAGE_VERSION_BASE,
           EVENT_NAME_PACKAGE, FIELD_NAME_PACKAGE, FIELD_TYPE_PACKAGE, FIELD_TYPE_GAME,
           DECORATED_BY, IS_PRERELEASE, MATCH_STATUS, EVENT_MATCH_STATUS,
           FIRST_SEEN_VERSION_GAME, LAST_SEEN_VERSION_GAME, LAST_SEEN_DATE_GAME
    FROM TDS_DB.BI_DEV.ANALYTICS_EVENT_SPEC_RECONCILIATION
    WHERE MATCH_STATUS IN ('BOTH','PACKAGE_ONLY') AND APP_NAME IS NOT NULL
"""

# Scoped to app-versions that already appear in MAIN_QUERY -- an app-version
# with no package data can never be selected in the tool's filters anyway,
# so this avoids pulling the full ~1.8M-row GAME_ONLY population for nothing.
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
    """Star-schema transform -- identical logic to build_dataset.py in the app repo."""
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
        "generatedAt": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"),
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


def _upload_to_vercel_blob(pathname: str, body: bytes, token: str, content_type: str) -> str:
    """
    Plain-stdlib PUT to Vercel Blob's REST API -- no @vercel/blob dependency
    needed. Reverse-engineered from the official JS SDK (unpkg.com/@vercel/blob)
    and verified against the live API before this file was written.
    """
    store_id = token.split("_")[3]
    url = f"https://vercel.com/api/blob/?{urllib.parse.urlencode({'pathname': pathname})}"
    req = urllib.request.Request(url, data=body, method="PUT", headers={
        "authorization": f"Bearer {token}",
        "x-api-version": "12",
        "x-vercel-blob-store-id": store_id,
        "x-vercel-blob-access": "public",
        "x-content-type": content_type,
        "x-add-random-suffix": "0",
        "x-allow-overwrite": "1",
        "x-cache-control-max-age": "3600",
    })
    with urllib.request.urlopen(req, timeout=120) as resp:
        return json.loads(resp.read())["url"]


with DAG(
    dag_id="dsi__analytics_event_spec_reconciliation",
    tags=["DSI"],
    default_args=default_args | task_default_args | {
        "email": ["dizky.darmawan@tripledotstudios.com"],
        "retries": 0,
    },
    start_date=datetime(2024, 8, 1),
    schedule="0 5 * * *",
    catchup=False,
    max_active_runs=1,
    doc_md="""
        ## analytics_event_spec_reconciliation
        Dataset reconciling the analytics events declared in the Tripledot package specs against the events actually observed firing in production, broken down by app version, event, field and field source.
        See [README](https://github.com/tripledotstudios/tds-dsi-pipelines/blob/main/dags/dsi_analytics_event_spec_reconciliation/README.md).

        Also refreshes the Event Audit tool (https://tds-event-audit.vercel.app)
        as a downstream task -- see refresh_event_audit_tool below.
    """,
) as dag:

    temp = SQLExecuteQueryOperator(
        task_id="base",
        conn_id="snowflake_ds",
        sql=(DAG_DIR / "sql" / "analytics_event_spec_reconciliation.sql").read_text()
    )

    @task(task_id="refresh_event_audit_tool")
    def refresh_event_audit_tool():
        hook = SnowflakeHook(snowflake_conn_id="snowflake_ds")
        main_rows = hook.get_pandas_df(MAIN_QUERY).to_dict("records")
        gameonly_rows = hook.get_pandas_df(GAMEONLY_QUERY).to_dict("records")

        dataset = _build_dataset(main_rows, gameonly_rows)

        # refuse to publish a suspiciously small pull (e.g. a transient
        # Snowflake issue) rather than blanking out the live tool
        if len(dataset["facts"]) < 1000:
            raise ValueError(
                f"refresh_event_audit_tool: only {len(dataset['facts'])} facts pulled, "
                "expected 100k+. Refusing to overwrite the published dataset."
            )

        body = json.dumps(dataset, separators=(",", ":")).encode("utf-8")
        token = Variable.get("VERCEL_BLOB_READ_WRITE_TOKEN")
        url = _upload_to_vercel_blob("reconciliation.json", body, token, "application/json")
        print(f"published {len(dataset['facts'])} facts -> {url}")

    temp >> refresh_event_audit_tool()
