import json, os, re
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
MAIN_SRC = os.path.join(HERE, "raw_main.json")       # MATCH_STATUS IN ('BOTH','PACKAGE_ONLY')
GAMEONLY_SRC = os.path.join(HERE, "raw_gameonly.json")  # MATCH_STATUS='GAME_ONLY', scoped to app-versions in MAIN_SRC
OUT_DIR = os.path.join(HERE, "data")
os.makedirs(OUT_DIR, exist_ok=True)

with open(MAIN_SRC) as f:
    main_rows = json.load(f)
with open(GAMEONLY_SRC) as f:
    gameonly_rows = json.load(f)

print(f"loaded {len(main_rows)} main rows, {len(gameonly_rows)} game-only rows")

# ---- string dictionary ----
strings = []
str_idx = {}

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

# unified fact columns (see README "Data model" section)
COLS = [
    "APP_NAME", "APP_VERSION", "PACKAGE", "PACKAGE_VERSION", "PACKAGE_VERSION_BASE",
    "EVENT_NAME", "FIELD_NAME", "FIELD_TYPE_PACKAGE", "FIELD_TYPE_GAME", "DECORATED_BY",
    "MATCH_STATUS", "IS_PRERELEASE", "FIRST_SEEN_VERSION_GAME", "LAST_SEEN_VERSION_GAME",
    "LAST_SEEN_DATE_GAME", "EVENT_MATCH_STATUS",
]
STATUS_CODE = {"PACKAGE_ONLY": 0, "BOTH": 1, "GAME_ONLY": 2}

facts = []
skipped_no_app = 0
for r in main_rows:
    if r.get("APP_NAME") is None:
        skipped_no_app += 1
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

n_main_kept = len(facts)

skipped_go_no_app = 0
for r in gameonly_rows:
    if r.get("APP_NAME") is None:
        skipped_go_no_app += 1
        continue
    facts.append([
        sidx(r.get("APP_NAME")), sidx(r.get("APP_VERSION")),
        None, None, None,                                   # no package context
        sidx(r.get("EVENT_NAME_GAME")), sidx(r.get("FIELD_NAME_GAME")),
        None, sidx(r.get("FIELD_TYPE_GAME")),
        None,                                                # decorated_by always null for game-only
        STATUS_CODE["GAME_ONLY"],
        0,                                                   # is_prerelease always null/false for game-only
        sidx(r.get("FIRST_SEEN_VERSION_GAME")), sidx(r.get("LAST_SEEN_VERSION_GAME")),
        sidx(r.get("LAST_SEEN_DATE_GAME")),
        STATUS_CODE[r["EVENT_MATCH_STATUS"]],
    ])

print(f"skipped {skipped_no_app} main / {skipped_go_no_app} game-only no-app rows")
print(f"kept {n_main_kept} main facts + {len(facts) - n_main_kept} game-only facts = {len(facts)} total")
print(f"dictionary size: {len(strings)} strings")

# ---- filter/scope indexes ----
app_versions = {}    # app -> set(app_version)   [union across all three statuses]
packages = set()     # set(package)              [only from declared/main facts]
package_versions = {}  # package -> set(package_version, exact)
app_packages = {}    # app -> set(package)

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
        parts = re.split(r'[._-]', strings[i])
        return [int(p) if p.isdigit() else p for p in parts]
    return sorted(idx_set, key=key)

apps = sorted(app_versions.keys(), key=lambda i: strings[i])
packages_list = sorted(packages, key=lambda i: strings[i])

out = {
    "generatedAt": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"),
    "strings": strings,
    "cols": COLS,
    "statusCodes": {"PACKAGE_ONLY": 0, "BOTH": 1, "GAME_ONLY": 2},
    "facts": facts,
    "apps": apps,
    "packages": packages_list,
    "appVersions": {str(i): sortedver(v) for i, v in app_versions.items()},
    "packageVersions": {str(i): sortedver(v) for i, v in package_versions.items()},
    "appPackages": {str(i): sorted(v, key=lambda x: strings[x]) for i, v in app_packages.items()},
}

out_path = os.path.join(OUT_DIR, "reconciliation.json")
with open(out_path, "w") as f:
    json.dump(out, f, separators=(",", ":"))

size_mb = os.path.getsize(out_path) / 1024 / 1024
print(f"wrote {out_path} ({size_mb:.2f} MB)")
print(f"apps: {len(apps)}, packages: {len(packages_list)}")
