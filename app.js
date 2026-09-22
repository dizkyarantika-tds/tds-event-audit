// ---------------------------------------------------------------------------
// Analytics Package Implementation -- Event Reconciliation
// Data: TDS_DB.BI_DEV.ANALYTICS_EVENT_RECONCILIATION (local snapshot)
// UI: design_handoff_event_reconciliation/ (high-fidelity spec -- colors,
// layout, copy and interaction rules below mirror its README 1:1).
//
// Unified fact row (see build_dataset.py):
//   0 APP  1 APP_VER  2 PKG(null=game-only)  3 PKG_VER  4 PKG_VER_BASE
//   5 EVENT  6 FIELD  7 TYPE_PKG  8 TYPE_GAME  9 DECORATED_BY
//   10 MATCH_STATUS (0=PACKAGE_ONLY 1=BOTH 2=GAME_ONLY) -- field-level
//   11 IS_PRERELEASE  12 FIRST_SEEN_VER_GAME  13 LAST_SEEN_VER_GAME  14 LAST_SEEN_DATE_GAME
//   15 EVENT_MATCH_STATUS (same 3-value enum) -- event-level rollup, constant
//      across every field row sharing the same (app, app_version, event)
//
// The Layer-1/deepdive event STATUS BADGE is driven by EVENT_MATCH_STATUS
// (col 15). Per-field status within the deepdive Fields table is still
// driven by field-level MATCH_STATUS (col 10) -- an event can be "Used"
// overall while one specific field is Package-only or an extra Game-only
// field never declared by any package.
//
// GAME_ONLY facts are scoped to app-versions that already exist in the
// BOTH/PACKAGE_ONLY data (the only ones reachable via the App/App Version
// filters) -- see README for why the unscoped table can't be pulled whole.
// ---------------------------------------------------------------------------

const C = {
  APP: 0, APP_VER: 1, PKG: 2, PKG_VER: 3, PKG_VER_BASE: 4,
  EVENT: 5, FIELD: 6, TYPE_PKG: 7, TYPE_GAME: 8, DECORATED_BY: 9,
  STATUS: 10, PRERELEASE: 11, FIRST_SEEN_VER: 12, LAST_SEEN_VER: 13, LAST_SEEN_DATE: 14,
  EVENT_STATUS: 15,
};
const ST = { PACKAGE_ONLY: 0, BOTH: 1, GAME_ONLY: 2 };
const ST_LABEL = { 0: 'Package-only', 1: 'Used', 2: 'Game-only' };
const ST_CLASS = { 0: 'package-only', 1: 'used', 2: 'game-only' };
const ST_DOT = { 0: '#4d5766', 1: '#3fb950', 2: '#a882ff' };

// Data lives in Vercel Blob storage, not this deployment -- an Airflow task
// overwrites this same path daily, right after it refreshes the source
// Snowflake table, so freshness is tied to that pipeline instead of a guessed
// cron time. `cache: 'no-store'` bypasses the browser's HTTP cache so every
// page load re-checks the CDN, which itself refreshes within the blob's
// cache-control max-age (1 hour) after each overwrite.
const DATA_URL = 'https://fk7hnthujtfc7ylv.public.blob.vercel-storage.com/reconciliation.json';

// design tokens (design_handoff_event_reconciliation/README.md)
const TOK = { ok: '#3fb950', warn: '#d29922', bad: '#f0616d', accent: '#5b93ff', gameOnly: '#a882ff' };
function pctColor(p) { return p >= 85 ? TOK.ok : p >= 60 ? TOK.warn : TOK.bad; }

let DATA = null, S = null;

function str(i) { return (i === null || i === undefined) ? null : S[i]; }
function esc(s) { return String(s).replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c])); }

function naturalVerSort(a, b) {
  const pa = a.split(/[._-]/).map(x => (/^\d+$/.test(x) ? parseInt(x, 10) : x));
  const pb = b.split(/[._-]/).map(x => (/^\d+$/.test(x) ? parseInt(x, 10) : x));
  for (let i = 0; i < Math.max(pa.length, pb.length); i++) {
    const x = pa[i], y = pb[i];
    if (x === undefined) return -1;
    if (y === undefined) return 1;
    if (x === y) continue;
    if (typeof x === 'number' && typeof y === 'number') return x - y;
    return String(x).localeCompare(String(y));
  }
  return 0;
}

// ---------------------------------------------------------------------------
// type compatibility (declared C#-ish package type vs observed live type)
// ---------------------------------------------------------------------------

function pkgTypeBucket(t) {
  if (!t) return null;
  const s = t.toLowerCase();
  if (s === 'string') return 'string';
  if (['int', 'long', 'float', 'double', 'decimal'].includes(s)) return 'number';
  if (s === 'bool') return 'boolean';
  if (s.endsWith('[]')) return 'array';
  if (s.startsWith('dictionary')) return 'object';
  return s;
}
function gameTypeBucket(t) {
  if (!t) return null;
  const s = t.toLowerCase();
  if (s === 'string' || s === 'timestamp') return 'string';
  if (s === 'integer' || s === 'float' || s === 'number') return 'number';
  if (s === 'boolean') return 'boolean';
  if (s === 'array') return 'array';
  if (s === 'object') return 'object';
  return s;
}
function typesCompatible(pkgType, gameType) {
  if (!pkgType || !gameType) return true;
  const p = pkgTypeBucket(pkgType), g = gameTypeBucket(gameType);
  if (p === g) return true;
  if ((p === 'array' || p === 'object') && g === 'string') return true;
  return false;
}

// ---------------------------------------------------------------------------
// state
// ---------------------------------------------------------------------------

const state = {
  selectedApps: null, selectedAppVersions: null, selectedPackages: null, selectedPackageVersions: null,
  rcVersion: false, decoratedField: false, bucket: 'all',
  selectedEvents: null, selectedFields: null,
  expandedGroups: new Set(),
  page2: null,   // {event, app, ver} -- active scope
};

// ---------------------------------------------------------------------------
// bootstrap
// ---------------------------------------------------------------------------

fetch(DATA_URL, { cache: 'no-store' })
  .then(r => { if (!r.ok) throw new Error('HTTP ' + r.status); return r.json(); })
  .then(d => {
    DATA = d; S = d.strings;
    // index.html already provides the sibling <span class="dot"> next to
    // each of these -- setting innerHTML here previously nested a *second*
    // dot inside the text span, showing two dots in an awkward stack.
    const cached = DATA.generatedAt ? `last cached ${DATA.generatedAt} UTC` : 'ready';
    document.getElementById('cacheStatus').textContent = cached;
    document.getElementById('cacheStatus2').textContent = cached;
    initUI();
    render();
  })
  .catch(err => {
    document.getElementById('fatal').hidden = false;
    document.getElementById('fatal').textContent = 'Failed to load data/reconciliation.json: ' + err.message;
    document.getElementById('layer1').hidden = true;
  });

// ---------------------------------------------------------------------------
// dropdown component -- no checkboxes, blue highlight for selected rows,
// hover "Only", type-to-filter search, "All" toggle. Matches the design's
// exact behavior: unticking "All" empties the selection; closing the
// dropdown (mousedown outside its [data-filter-cell]) reverts an emptied
// selection back to "all" rather than leaving a dead "nothing selected" view.
// ---------------------------------------------------------------------------

let openDropdown = null; // the currently-open control, for the shared away-listener

function setupDropdown({ cellId, ddId, btnId, panelId, labelId, allItems, blankAll, getSelected, setSelected, onChange }) {
  const cell = document.getElementById(cellId || ddId);
  const dd = document.getElementById(ddId), btn = document.getElementById(btnId);
  const panel = document.getElementById(panelId), labelEl = document.getElementById(labelId);

  function summaryText() {
    const sel = getSelected(), all = allItems();
    if (sel === null || sel.size === 0) return blankAll ? '' : `All (${all.length})`;
    if (sel.size === 1) return S[[...sel][0]];
    return `${sel.size} selected`;
  }
  function refreshLabel() { labelEl.textContent = summaryText(); }

  // Builds the search input + scroll container ONCE per open(); typing only
  // ever touches the scroll container's contents afterward. Previously the
  // 'input' handler called this whole function again, which recreated the
  // <input> element on every keystroke -- destroying and replacing the
  // focused element, so only one character could ever be typed before focus
  // silently dropped out of the box.
  function renderPanel(items) {
    panel.innerHTML = '';
    const search = document.createElement('input');
    search.className = 'dd-search'; search.placeholder = 'Type to filter…';
    panel.appendChild(search);

    const scroll = document.createElement('div');
    scroll.className = 'dd-scroll';
    panel.appendChild(scroll);

    function renderList(filterText) {
      const sel = getSelected(), ft = (filterText || '').toLowerCase();
      scroll.innerHTML = '';

      const allOn = sel === null;
      const allRow = document.createElement('button');
      allRow.type = 'button';
      allRow.className = 'dd-row all-row' + (allOn ? ' selected' : '');
      allRow.textContent = 'All';
      allRow.addEventListener('click', () => {
        setSelected(allOn ? new Set() : null);
        refreshLabel(); renderList(search.value); onChange();
      });
      scroll.appendChild(allRow);

      const filtered = items.filter(i => S[i].toLowerCase().includes(ft));
      filtered.forEach(i => {
        const row = document.createElement('div');
        const isSel = sel === null || sel.has(i);
        row.className = 'dd-row' + (isSel ? ' selected' : '');
        const lbl = document.createElement('span'); lbl.className = 'lbl'; lbl.textContent = S[i];
        const only = document.createElement('button'); only.type = 'button'; only.className = 'dd-only'; only.textContent = 'Only';
        only.addEventListener('click', (e) => {
          e.stopPropagation();
          setSelected(new Set([i])); refreshLabel(); renderList(search.value); onChange();
        });
        row.appendChild(lbl); row.appendChild(only);
        row.addEventListener('click', (e) => {
          if (e.target === only) return;
          let cur = getSelected();
          cur = cur === null ? new Set(items) : new Set(cur);
          if (cur.has(i)) cur.delete(i); else cur.add(i);
          setSelected(cur.size === items.length ? null : cur);
          refreshLabel(); renderList(search.value); onChange();
        });
        scroll.appendChild(row);
      });
      if (filtered.length === 0) {
        const none = document.createElement('div');
        none.className = 'dd-no-match'; none.textContent = 'No match';
        scroll.appendChild(none);
      }
    }

    search.addEventListener('input', () => renderList(search.value));
    renderList('');
  }

  function close() {
    panel.hidden = true;
    if (openDropdown === control) openDropdown = null;
    // An emptied-but-not-null selection reverts to "all" once the dropdown closes.
    const sel = getSelected();
    if (sel !== null && sel.size === 0) {
      setSelected(null); refreshLabel(); onChange();
    }
  }
  function open() {
    if (openDropdown && openDropdown !== control) openDropdown.close();
    renderPanel(allItems());
    panel.hidden = false;
    openDropdown = control;
  }

  btn.addEventListener('click', () => { panel.hidden ? open() : close(); });

  const control = { refreshLabel, rerenderItems: () => { if (!panel.hidden) renderPanel(allItems()); }, close, cell };
  refreshLabel();
  return control;
}

document.addEventListener('mousedown', (e) => {
  if (!openDropdown) return;
  if (openDropdown.cell && openDropdown.cell.contains(e.target)) return;
  openDropdown.close();
}, true);

let appDD, appVerDD, pkgDD, pkgVerDD, eventDD, fieldDD;

function initUI() {
  appDD = setupDropdown({
    cellId: 'appDD', ddId: 'appDD', btnId: 'appBtn', panelId: 'appPanel', labelId: 'appBtnLabel',
    allItems: () => DATA.apps, blankAll: false,
    getSelected: () => state.selectedApps, setSelected: v => { state.selectedApps = v; },
    onChange: onScopeFilterChange,
  });
  appVerDD = setupDropdown({
    cellId: 'appVerDD', ddId: 'appVerDD', btnId: 'appVerBtn', panelId: 'appVerPanel', labelId: 'appVerBtnLabel',
    allItems: appVersionsInScope, blankAll: false,
    getSelected: () => state.selectedAppVersions, setSelected: v => { state.selectedAppVersions = v; },
    onChange: onScopeFilterChange,
  });
  pkgDD = setupDropdown({
    cellId: 'pkgDD', ddId: 'pkgDD', btnId: 'pkgBtn', panelId: 'pkgPanel', labelId: 'pkgBtnLabel',
    allItems: packagesInScope, blankAll: false,
    getSelected: () => state.selectedPackages, setSelected: v => { state.selectedPackages = v; },
    onChange: onScopeFilterChange,
  });
  pkgVerDD = setupDropdown({
    cellId: 'pkgVerDD', ddId: 'pkgVerDD', btnId: 'pkgVerBtn', panelId: 'pkgVerPanel', labelId: 'pkgVerBtnLabel',
    allItems: packageVersionsInScope, blankAll: false,
    getSelected: () => state.selectedPackageVersions, setSelected: v => { state.selectedPackageVersions = v; },
    onChange: onScopeFilterChange,
  });
  eventDD = setupDropdown({
    cellId: 'eventDD', ddId: 'eventDD', btnId: 'eventBtn', panelId: 'eventPanel', labelId: 'eventBtnLabel',
    allItems: eventsInScope, blankAll: true,
    getSelected: () => state.selectedEvents, setSelected: v => { state.selectedEvents = v; },
    onChange: () => render(),
  });
  fieldDD = setupDropdown({
    cellId: 'fieldDD', ddId: 'fieldDD', btnId: 'fieldBtn', panelId: 'fieldPanel', labelId: 'fieldBtnLabel',
    allItems: fieldsInScope, blankAll: true,
    getSelected: () => state.selectedFields, setSelected: v => { state.selectedFields = v; },
    onChange: () => render(),
  });

  document.getElementById('resetBtn').addEventListener('click', () => {
    state.selectedApps = null; state.selectedAppVersions = null;
    state.selectedPackages = null; state.selectedPackageVersions = null;
    state.rcVersion = false; state.decoratedField = false; state.bucket = 'all';
    state.selectedEvents = null; state.selectedFields = null; state.expandedGroups.clear();
    document.getElementById('rcVersionChk').checked = false;
    document.getElementById('decoratedChk').checked = false;
    document.querySelectorAll('.bucket-tabs .tab').forEach(t => t.classList.toggle('active', t.dataset.bucket === 'all'));
    refreshAllDropdowns(); render();
  });

  document.getElementById('rcVersionChk').addEventListener('change', e => { state.rcVersion = e.target.checked; onScopeFilterChange(); });
  document.getElementById('decoratedChk').addEventListener('change', e => { state.decoratedField = e.target.checked; onScopeFilterChange(); });

  document.querySelectorAll('.bucket-tabs .tab').forEach(tab => {
    tab.addEventListener('click', () => {
      state.bucket = tab.dataset.bucket;
      document.querySelectorAll('.bucket-tabs .tab').forEach(t => t.classList.toggle('active', t === tab));
      render();
    });
  });

  document.getElementById('backBtn').addEventListener('click', () => { state.page2 = null; render(); });
  document.getElementById('issuesOnlyChk').addEventListener('change', () => renderPage2());

  refreshAllDropdowns();
}

function pruneSelection(key, validItems) {
  const cur = state[key];
  if (cur === null) return;
  const valid = new Set(validItems);
  state[key] = new Set([...cur].filter(x => valid.has(x)));
}

function onScopeFilterChange() {
  pruneSelection('selectedPackages', packagesInScope());
  pruneSelection('selectedAppVersions', appVersionsInScope());
  pruneSelection('selectedPackageVersions', packageVersionsInScope());
  pruneSelection('selectedEvents', eventsInScope());
  pruneSelection('selectedFields', fieldsInScope());
  refreshAllDropdowns();
  render();
}

function refreshAllDropdowns() {
  appDD.refreshLabel(); appVerDD.refreshLabel(); pkgDD.refreshLabel(); pkgVerDD.refreshLabel();
  appDD.rerenderItems(); appVerDD.rerenderItems(); pkgDD.rerenderItems(); pkgVerDD.rerenderItems();
  eventDD.refreshLabel(); fieldDD.refreshLabel(); eventDD.rerenderItems(); fieldDD.rerenderItems();
}

// ---------------------------------------------------------------------------
// scope helpers
// ---------------------------------------------------------------------------

function appVersionsInScope() {
  const apps = state.selectedApps === null ? DATA.apps : [...state.selectedApps];
  const set = new Set();
  apps.forEach(a => (DATA.appVersions[a] || []).forEach(v => set.add(v)));
  return [...set].sort((x, y) => naturalVerSort(S[x], S[y])).reverse();
}
function packagesInScope() {
  if (state.selectedApps === null) return DATA.packages;
  const set = new Set();
  state.selectedApps.forEach(a => (DATA.appPackages[a] || []).forEach(p => set.add(p)));
  return DATA.packages.filter(p => set.has(p));
}
function packageVersionsInScope() {
  const appSet = state.selectedApps, verSet = state.selectedAppVersions, pkgSet = state.selectedPackages;
  const set = new Set();
  for (const f of DATA.facts) {
    if (f[C.PKG_VER] === null) continue;
    if (appSet !== null && !appSet.has(f[C.APP])) continue;
    if (verSet !== null && !verSet.has(f[C.APP_VER])) continue;
    if (pkgSet !== null && !pkgSet.has(f[C.PKG])) continue;
    set.add(f[C.PKG_VER]);
  }
  return [...set].sort((x, y) => naturalVerSort(S[x], S[y])).reverse();
}
function eventsInScope() {
  const set = new Set();
  for (const f of mainFilteredFacts()) set.add(f[C.EVENT]);
  return [...set].sort((x, y) => S[x].localeCompare(S[y]));
}
function fieldsInScope() {
  const set = new Set();
  for (const f of mainFilteredFacts()) set.add(f[C.FIELD]);
  return [...set].sort((x, y) => S[x].localeCompare(S[y]));
}

// facts passing App/App-version/Package/Package-version/RC-version/Decorated-field.
// RC Version and Decorated Field are both OFF by default, and default-off means
// EXCLUDE that data from every calculation (scorecards, usage table, events
// table, and the deepdive they feed into) -- not just "don't filter to it".
// Checking either box adds that data back in. Neither ever hides a game-only
// row: game-only facts always carry PRERELEASE=0 and DECORATED_BY=null (see
// build_dataset.py), so they pass through regardless of these two checkboxes.
function passesMainFilters(f) {
  const appSet = state.selectedApps, verSet = state.selectedAppVersions;
  const pkgSet = state.selectedPackages, pkgVerSet = state.selectedPackageVersions;
  if (appSet !== null && !appSet.has(f[C.APP])) return false;
  if (verSet !== null && !verSet.has(f[C.APP_VER])) return false;
  if (pkgSet !== null) { if (f[C.PKG] === null || !pkgSet.has(f[C.PKG])) return false; }
  if (pkgVerSet !== null) { if (f[C.PKG_VER] === null || !pkgVerSet.has(f[C.PKG_VER])) return false; }
  if (!state.rcVersion && f[C.PRERELEASE] === 1) return false;
  if (!state.decoratedField && f[C.DECORATED_BY] !== null) return false;
  return true;
}
function mainFilteredFacts() { return DATA.facts.filter(passesMainFilters); }

// ---------------------------------------------------------------------------
// render dispatch
// ---------------------------------------------------------------------------

function render() {
  if (!DATA) return;
  if (state.page2) {
    document.getElementById('layer1').hidden = true;
    document.getElementById('layer2').hidden = false;
    renderPage2();
  } else {
    document.getElementById('layer1').hidden = false;
    document.getElementById('layer2').hidden = true;
    const facts = mainFilteredFacts();
    renderFilterChips();
    renderScorecards(facts);
    renderUsageTable(facts);
    renderEvents(facts);
  }
}

// ---------------------------------------------------------------------------
// "In scope" chips
// ---------------------------------------------------------------------------

function renderChipsInto(containerId, groups) {
  const el = document.getElementById(containerId);
  const chips = [];
  groups.forEach(g => chips.push(...g));
  if (chips.length === 0) { el.hidden = true; el.innerHTML = ''; return; }
  el.hidden = false;
  el.innerHTML = `<span class="chips-label">In scope</span>` + chips.map((c, idx) =>
    `<span class="chip"><span class="chip-kind">${esc(c.kind)}</span><span>${esc(c.label)}</span><button type="button" class="chip-x" data-idx="${idx}">&times;</button></span>`
  ).join('');
  el.querySelectorAll('.chip-x').forEach(btn => {
    btn.addEventListener('click', () => { chips[parseInt(btn.dataset.idx, 10)].onRemove(); render(); refreshAllDropdowns(); });
  });
}

function selectionChips(key, kind, allItems) {
  const sel = state[key];
  if (sel === null || sel.size === 0) return [];
  return [...sel].map(v => ({
    kind, label: S[v],
    onRemove: () => {
      const next = new Set(sel); next.delete(v);
      state[key] = next.size === allItems().length ? null : next;
    },
  }));
}

function renderFilterChips() {
  const groups = [
    selectionChips('selectedApps', 'App', () => DATA.apps),
    selectionChips('selectedAppVersions', 'App ver', appVersionsInScope),
    selectionChips('selectedPackages', 'Package', packagesInScope),
    selectionChips('selectedPackageVersions', 'Pkg ver', packageVersionsInScope),
  ];
  // RC Version and Decorated Field both exclude data by default now (see
  // passesMainFilters) and affect scorecards/usage table/events table alike,
  // so both chips live in the main "in scope" row, not the events-only one.
  if (state.rcVersion) groups.push([{ kind: '', label: 'RC version', onRemove: () => { state.rcVersion = false; document.getElementById('rcVersionChk').checked = false; } }]);
  if (state.decoratedField) groups.push([{ kind: '', label: 'Decorated field', onRemove: () => { state.decoratedField = false; document.getElementById('decoratedChk').checked = false; } }]);
  renderChipsInto('filterChips', groups);
}

const BUCKET_LABEL = { used: 'Used', 'package-only': 'Package-only', 'game-only': 'Game-only' };

function renderEventChips() {
  const groups = [selectionChips('selectedEvents', 'Event', eventsInScope)];
  if (state.bucket !== 'all') groups.push([{ kind: 'Status', label: BUCKET_LABEL[state.bucket], onRemove: () => {
    state.bucket = 'all';
    document.querySelectorAll('.bucket-tabs .tab').forEach(t => t.classList.toggle('active', t.dataset.bucket === 'all'));
  } }]);
  renderChipsInto('eventChips', groups);
}

// ---------------------------------------------------------------------------
// scorecards
// ---------------------------------------------------------------------------

function renderScorecards(facts) {
  const appPkgPairs = new Set();
  const declaredEvents = new Map();  // key "pkg:event" -> anyBoth
  const declaredFields = new Map();  // key "pkg:event:field" -> {anyBoth, anyCompat, anyIncompat}
  const gameOnlyEvents = new Set();

  facts.forEach(f => {
    if (f[C.PKG] !== null) appPkgPairs.add(f[C.APP] + ':' + f[C.PKG]);
    // Game-Only Event scorecard: driven by the event-level rollup, not the
    // field's own status -- a field can be individually GAME_ONLY (an extra,
    // undeclared field) on an event that's otherwise Used/Package-only, and
    // that must NOT count as a game-only event.
    if (f[C.EVENT_STATUS] === ST.GAME_ONLY) gameOnlyEvents.add(f[C.EVENT]);
    if (f[C.STATUS] === ST.GAME_ONLY) return;
    const evKey = f[C.PKG] + ':' + f[C.EVENT];
    if (!declaredEvents.has(evKey)) declaredEvents.set(evKey, false);
    const fdKey = evKey + ':' + f[C.FIELD];
    if (!declaredFields.has(fdKey)) declaredFields.set(fdKey, { anyBoth: false, anyCompat: false, anyIncompat: false });
    if (f[C.STATUS] === ST.BOTH) {
      declaredEvents.set(evKey, true);
      const fo = declaredFields.get(fdKey);
      fo.anyBoth = true;
      if (f[C.TYPE_GAME] !== null) {
        if (typesCompatible(str(f[C.TYPE_PKG]), str(f[C.TYPE_GAME]))) fo.anyCompat = true;
        else fo.anyIncompat = true;
      }
    }
  });

  const totalDeclaredEvents = declaredEvents.size;
  const usedEvents = [...declaredEvents.values()].filter(Boolean).length;
  const totalDeclaredFields = declaredFields.size;
  const usedFields = [...declaredFields.values()].filter(v => v.anyBoth).length;
  const mismatches = [...declaredFields.values()].filter(v => v.anyBoth && v.anyIncompat && !v.anyCompat).length;
  const eventPct = totalDeclaredEvents ? Math.round(100 * usedEvents / totalDeclaredEvents) : 0;
  const fieldPct = totalDeclaredFields ? Math.round(100 * usedFields / totalDeclaredFields) : 0;

  const cards = [
    { label: 'Package Usage', value: appPkgPairs.size.toLocaleString(), desc: '# package use by App', color: TOK.accent },
    { label: 'Event Coverage', value: `${usedEvents}/${totalDeclaredEvents}`, desc: '# event use by App', color: pctColor(eventPct) },
    { label: 'Field Coverage', value: `${usedFields}/${totalDeclaredFields}`, desc: '# field use by App', color: pctColor(fieldPct) },
    { label: 'Data Type Mismatch', value: mismatches.toLocaleString(), desc: '# data type mismatch', color: mismatches > 0 ? TOK.bad : TOK.ok },
    { label: 'Game-Only Event', value: gameOnlyEvents.size.toLocaleString(), desc: '# event not found in package', color: gameOnlyEvents.size > 0 ? TOK.gameOnly : TOK.ok },
  ];
  const row = document.getElementById('scorecards');
  row.innerHTML = cards.map(c => `
    <div class="stat-card">
      <div class="label">${c.label}</div>
      <div class="value" style="color:${c.color};">${c.value}</div>
      <div class="desc">${c.desc}</div>
      <div class="accent-strip" style="background:${c.color};"></div>
    </div>`).join('');
}

// ---------------------------------------------------------------------------
// App to Package Usage table
// ---------------------------------------------------------------------------

function renderUsageTable(facts) {
  const groups = new Map(); // "app:ver:pkg:pkgver:pkgverbase" -> {events: Map(event->anyBoth)}
  facts.forEach(f => {
    if (f[C.PKG] === null) return; // declared-only table
    const key = [f[C.APP], f[C.APP_VER], f[C.PKG], f[C.PKG_VER], f[C.PKG_VER_BASE]].join(':');
    if (!groups.has(key)) {
      groups.set(key, {
        app: f[C.APP], ver: f[C.APP_VER], pkg: f[C.PKG], pkgVer: f[C.PKG_VER], pkgVerBase: f[C.PKG_VER_BASE],
        events: new Map(),
      });
    }
    const g = groups.get(key);
    if (!g.events.has(f[C.EVENT])) g.events.set(f[C.EVENT], false);
    if (f[C.STATUS] === ST.BOTH) g.events.set(f[C.EVENT], true);
  });

  const rows = [...groups.values()].sort((a, b) => {
    const an = S[a.app], bn = S[b.app];
    if (an !== bn) return an.localeCompare(bn);
    const av = naturalVerSort(S[a.ver], S[b.ver]); if (av) return av;
    const ap = S[a.pkg], bp = S[b.pkg];
    if (ap !== bp) return ap.localeCompare(bp);
    return naturalVerSort(S[a.pkgVer], S[b.pkgVer]);
  });

  const tbody = document.getElementById('usageBody');
  if (rows.length === 0) {
    tbody.innerHTML = `<tr><td colspan="6" class="empty-state">No app/package combinations match the current filters.</td></tr>`;
    return;
  }
  tbody.innerHTML = rows.map(g => {
    const total = g.events.size, used = [...g.events.values()].filter(Boolean).length;
    const pct = total ? Math.round(100 * used / total) : 0;
    return `<tr>
      <td>${S[g.app]}</td>
      <td>${S[g.ver]}</td>
      <td class="dim">${S[g.pkg]}</td>
      <td>${S[g.pkgVer]}</td>
      <td>${S[g.pkgVerBase]}</td>
      <td><div class="bar-wrap"><div class="bar-track"><div class="bar-fill" style="width:${pct}%;background:${pctColor(pct)};"></div></div><span class="bar-label">${used}/${total}</span></div></td>
    </tr>`;
  }).join('');
}

// ---------------------------------------------------------------------------
// Events panel (grouped by app+version)
// ---------------------------------------------------------------------------

function bucketOfStatus(status) {
  if (status === ST.BOTH) return 'used';
  if (status === ST.PACKAGE_ONLY) return 'package-only';
  return 'game-only';
}

function renderEvents(facts) {
  renderEventChips();

  // apply event/field dropdown filters on top of main filters (Decorated
  // Field is now a data-level exclusion handled in passesMainFilters, not a
  // row-display filter here -- see that function's comment)
  let evFacts = facts;
  if (state.selectedEvents !== null) evFacts = evFacts.filter(f => state.selectedEvents.has(f[C.EVENT]));
  if (state.selectedFields !== null) evFacts = evFacts.filter(f => state.selectedFields.has(f[C.FIELD]));

  // group by app+version -> event -> field rows. Grouped by event NAME alone
  // (not by package) so a single logical event with both package-matched
  // fields and extra undeclared fields (PKG=null) renders as one row -- see
  // README "Data model" for why keying on (pkg,event) used to split these.
  // The rare case of one event name declared by multiple distinct packages
  // in the same app-version (e.g. Screen_Interaction) also merges into one
  // row; its package line lists every contributing package.
  const groups = new Map(); // "app:ver" -> Map(event -> {fields:[]})
  const allPackagesByGroup = new Map(); // "app:ver" -> Set(pkg) across ALL buckets (for group meta, unfiltered by bucket)

  facts.forEach(f => {
    const gk = f[C.APP] + ':' + f[C.APP_VER];
    if (f[C.PKG] !== null) {
      if (!allPackagesByGroup.has(gk)) allPackagesByGroup.set(gk, new Set());
      allPackagesByGroup.get(gk).add(f[C.PKG]);
    }
  });

  evFacts.forEach(f => {
    const gk = f[C.APP] + ':' + f[C.APP_VER];
    if (!groups.has(gk)) groups.set(gk, { app: f[C.APP], ver: f[C.APP_VER], events: new Map() });
    const g = groups.get(gk);
    const ek = f[C.EVENT];
    if (!g.events.has(ek)) g.events.set(ek, { event: ek, fields: [] });
    g.events.get(ek).fields.push(f);
  });

  // resolve each event's status/coverage within its group, apply bucket filter
  const groupList = [];
  for (const g of groups.values()) {
    const evRows = [];
    for (const eo of g.events.values()) {
      // EVENT_MATCH_STATUS is a consistent per-(app,ver,event) rollup --
      // the Layer-1 tag comes from it directly, not from aggregating
      // field-level MATCH_STATUS.
      const status = eo.fields[0][C.EVENT_STATUS];
      const bucket = bucketOfStatus(status);
      if (state.bucket !== 'all' && state.bucket !== bucket) continue;
      // Field coverage denominator is the package-DECLARED field set
      // (MATCH_STATUS BOTH/PACKAGE_ONLY); fields with MATCH_STATUS=GAME_ONLY
      // are extras with no spec to compare against and don't count toward
      // it -- unless the event itself has no package spec at all, in which
      // case every observed field counts as covered by convention (N/N).
      let total, used;
      if (status === ST.GAME_ONLY) {
        const all = new Set(eo.fields.map(f => f[C.FIELD]));
        total = all.size; used = all.size;
      } else {
        const declared = new Set(eo.fields.filter(f => f[C.STATUS] !== ST.GAME_ONLY).map(f => f[C.FIELD]));
        const matched = new Set(eo.fields.filter(f => f[C.STATUS] === ST.BOTH).map(f => f[C.FIELD]));
        total = declared.size; used = matched.size;
      }
      let typeIssues = 0;
      eo.fields.forEach(f => {
        if (f[C.STATUS] === ST.BOTH && f[C.TYPE_GAME] !== null && !typesCompatible(str(f[C.TYPE_PKG]), str(f[C.TYPE_GAME]))) typeIssues++;
      });
      const lastSeen = eo.fields.map(f => f[C.LAST_SEEN_DATE]).find(x => x !== null) || null;
      const firstSeenVer = eo.fields.map(f => f[C.FIRST_SEEN_VER]).find(x => x !== null) || null;
      const lastSeenVer = eo.fields.map(f => f[C.LAST_SEEN_VER]).find(x => x !== null) || null;
      const pkgs = [...new Set(eo.fields.map(f => f[C.PKG]).filter(p => p !== null))].sort((a, b) => S[a].localeCompare(S[b]));
      evRows.push({
        pkgs, event: eo.event, status, bucket,
        used, total, typeIssues, lastSeen, firstSeenVer, lastSeenVer,
      });
    }
    if (evRows.length === 0) continue;
    evRows.sort((a, b) => {
      const order = { used: 0, 'package-only': 1, 'game-only': 2 };
      if (order[a.bucket] !== order[b.bucket]) return order[a.bucket] - order[b.bucket];
      return S[a.event].localeCompare(S[b.event]);
    });
    const pkgCount = (allPackagesByGroup.get(g.app + ':' + g.ver) || new Set()).size;
    groupList.push({ app: g.app, ver: g.ver, events: evRows, pkgCount });
  }

  groupList.sort((a, b) => {
    const an = S[a.app], bn = S[b.app];
    if (an !== bn) return an.localeCompare(bn);
    // latest version first -- naturalVerSort(x,y) is ascending, so swap args
    return naturalVerSort(S[b.ver], S[a.ver]);
  });

  // auto-expand the sole group when filters narrow to exactly one app-version
  if (groupList.length === 1) state.expandedGroups.add(groupList[0].app + ':' + groupList[0].ver);

  const tbody = document.getElementById('eventsBody');
  if (groupList.length === 0) {
    tbody.innerHTML = `<tr><td class="empty-state">No events match the current filters and search.</td></tr>`;
    return;
  }

  let html = '';
  groupList.forEach(g => {
    const gk = g.app + ':' + g.ver;
    const open = state.expandedGroups.has(gk);
    html += `<tr class="group-header-row" data-gk="${gk}">
      <td colspan="7">
        <div class="group-header">
          <span class="group-caret">${open ? '▾' : '▸'}</span>
          <span class="group-app">${S[g.app]}</span>
          <span class="group-ver">${S[g.ver]}</span>
          <span class="group-meta">${g.events.length} event${g.events.length === 1 ? '' : 's'} &middot; ${g.pkgCount} packages</span>
        </div>
      </td>
    </tr>`;
    if (open) {
      const maxH = g.events.length > 10 ? 'max-height:604px;' : '';
      html += `<tr class="group-scroll-row"><td colspan="7"><div class="group-scroll" style="${maxH}"><table><colgroup>
        <col style="width:400px"><col style="width:120px"><col style="width:135px"><col style="width:135px"><col style="width:175px"><col style="width:115px"><col style="width:30px">
        </colgroup>
        <thead><tr><th>EVENT NAME</th><th>LAST SEEN</th><th>FIRST SEEN VER</th><th>LAST SEEN VER</th><th>FIELD COVERAGE</th><th style="text-align:right;">TYPE ISSUES</th><th></th></tr></thead>
        <tbody>`;
      g.events.forEach(e => {
        const pct = e.total ? Math.round(100 * e.used / e.total) : 0;
        const nameCls = e.bucket === 'package-only' ? ' dim-name' : '';
        const pkgLine = e.pkgs.length ? e.pkgs.map(p => S[p]).join(', ') : '';
        html += `<tr class="event-row clickable" data-event="${e.event}" data-app="${g.app}" data-ver="${g.ver}">
          <td><div class="event-name-cell">
            <div class="event-name-line">
              <span class="event-dot" style="background:${ST_DOT[e.status]};"></span>
              <span class="event-name${nameCls}">${S[e.event]}</span>
              <span class="badge ${ST_CLASS[e.status]}">${ST_LABEL[e.status]}</span>
            </div>
            ${pkgLine ? `<div class="event-pkg-line">${pkgLine}</div>` : ''}
          </div></td>
          <td class="last-seen-cell">${e.lastSeen !== null ? S[e.lastSeen] : '&mdash;'}</td>
          <td class="ver-cell">${e.firstSeenVer !== null ? S[e.firstSeenVer] : '&mdash;'}</td>
          <td class="ver-cell">${e.lastSeenVer !== null ? S[e.lastSeenVer] : '&mdash;'}</td>
          <td class="field-cov-cell"><div class="bar-wrap"><div class="bar-track"><div class="bar-fill" style="width:${pct}%;background:${pctColor(pct)};"></div></div><span class="bar-label">${e.used} / ${e.total}</span></div></td>
          <td class="type-issues-cell ${e.typeIssues ? '' : 'zero'}">${e.typeIssues ? e.typeIssues : '&mdash;'}</td>
          <td class="chevron-cell">&rsaquo;</td>
        </tr>`;
      });
      html += `</tbody></table></div></td></tr>`;
    }
  });
  tbody.innerHTML = html;

  tbody.querySelectorAll('.group-header-row').forEach(row => {
    row.addEventListener('click', () => {
      const gk = row.dataset.gk;
      if (state.expandedGroups.has(gk)) state.expandedGroups.delete(gk); else state.expandedGroups.add(gk);
      render();
    });
  });
  tbody.querySelectorAll('.event-row').forEach(row => {
    row.addEventListener('click', () => {
      state.page2 = { event: parseInt(row.dataset.event, 10), app: parseInt(row.dataset.app, 10), ver: parseInt(row.dataset.ver, 10) };
      render();
    });
  });
}

// ---------------------------------------------------------------------------
// Layer 2: event deepdive
// ---------------------------------------------------------------------------

function renderPage2() {
  const { event, app, ver } = state.page2;

  // all facts for this event, within current main-filter scope (for Contexts in scope)
  const scopeFacts = DATA.facts.filter(f => f[C.EVENT] === event && passesMainFilters(f));
  // facts for the exact clicked context
  const ctxFacts = scopeFacts.filter(f => f[C.APP] === app && f[C.APP_VER] === ver);

  const status = ctxFacts.length ? ctxFacts[0][C.EVENT_STATUS] : ST.GAME_ONLY;

  document.getElementById('scopePill').innerHTML = `<span class="scope-app">${S[app]}</span><span class="scope-ver">${S[ver]}</span>`;
  document.getElementById('deepEventName').textContent = S[event];
  document.getElementById('deepStatusBadge').className = `badge ${ST_CLASS[status]}`;
  document.getElementById('deepStatusBadge').textContent = ST_LABEL[status];
  const pkgs = [...new Set(ctxFacts.map(f => f[C.PKG]).filter(p => p !== null))].sort((a, b) => S[a].localeCompare(S[b]));
  if (pkgs.length) {
    document.getElementById('deepPackageLine').textContent = pkgs.map(p => {
      const pkgVerFact = ctxFacts.find(f => f[C.PKG] === p && f[C.PKG_VER] !== null);
      const pkgVerLabel = pkgVerFact ? S[pkgVerFact[C.PKG_VER]] : '';
      return `${S[p]} (${pkgVerLabel})`;
    }).join(', ');
  } else {
    document.getElementById('deepPackageLine').textContent = '';
  }

  // same declared-field-only convention as the Events panel (see renderEvents)
  let fieldSet, matchedSet;
  if (status === ST.GAME_ONLY) {
    fieldSet = new Set(ctxFacts.map(f => f[C.FIELD]));
    matchedSet = new Set(fieldSet);
  } else {
    fieldSet = new Set(ctxFacts.filter(f => f[C.STATUS] !== ST.GAME_ONLY).map(f => f[C.FIELD]));
    matchedSet = new Set(ctxFacts.filter(f => f[C.STATUS] === ST.BOTH).map(f => f[C.FIELD]));
  }
  let typeMismatchCount = 0;
  ctxFacts.forEach(f => {
    if (f[C.STATUS] === ST.BOTH && f[C.TYPE_GAME] !== null && !typesCompatible(str(f[C.TYPE_PKG]), str(f[C.TYPE_GAME]))) typeMismatchCount++;
  });
  const fieldPct = fieldSet.size ? Math.round(100 * matchedSet.size / fieldSet.size) : 0;
  document.getElementById('deepFieldUsage').textContent = `${matchedSet.size}/${fieldSet.size}`;
  document.getElementById('deepFieldUsage').style.color = pctColor(fieldPct);
  document.getElementById('deepTypeMismatches').textContent = typeMismatchCount;
  document.getElementById('deepTypeMismatches').style.color = typeMismatchCount > 0 ? TOK.bad : TOK.ok;

  // ---- Fields table ----
  // A merged event can now mix a package-declared field row with an extra,
  // undeclared field row of the same name (or the same field declared by
  // more than one package) -- resolve status by precedence (Used/mismatch >
  // Package-only > Game-only) instead of letting whichever row is iterated
  // last silently win.
  const fields = new Map();
  ctxFacts.forEach(f => {
    const fi = f[C.FIELD];
    if (!fields.has(fi)) fields.set(fi, { typePkg: null, typeGame: null, status: null, firstSeen: null, lastSeen: null });
    const fo = fields.get(fi);
    if (f[C.STATUS] === ST.BOTH) {
      fo.typePkg = f[C.TYPE_PKG];
      fo.typeGame = f[C.TYPE_GAME];
      fo.firstSeen = f[C.FIRST_SEEN_VER]; fo.lastSeen = f[C.LAST_SEEN_VER];
      const mismatch = f[C.TYPE_GAME] !== null && !typesCompatible(str(f[C.TYPE_PKG]), str(f[C.TYPE_GAME]));
      fo.status = mismatch ? 'mismatch' : 'used';
    } else if (f[C.STATUS] === ST.PACKAGE_ONLY) {
      fo.typePkg = f[C.TYPE_PKG];
      if (fo.status === null) fo.status = 'package-only';
    } else if (fo.status === null) {
      fo.status = 'game-only';
      fo.typeGame = f[C.TYPE_GAME];
      fo.firstSeen = f[C.FIRST_SEEN_VER]; fo.lastSeen = f[C.LAST_SEEN_VER];
    }
  });

  const issuesOnly = document.getElementById('issuesOnlyChk').checked;
  let fieldRows = [...fields.entries()].sort((a, b) => S[a[0]].localeCompare(S[b[0]]));
  if (issuesOnly) fieldRows = fieldRows.filter(([, fo]) => fo.status !== 'used');

  const fbody = document.getElementById('fieldsBody');
  if (fieldRows.length === 0) {
    fbody.innerHTML = `<tr><td colspan="6" class="empty-state fields-empty">No fields with issues on this event.</td></tr>`;
  } else {
    fbody.innerHTML = fieldRows.map(([fi, fo]) => {
      const statusLabel = fo.status === 'mismatch' ? 'Type mismatched' : fo.status === 'used' ? 'Used' : fo.status === 'package-only' ? 'Package-only' : 'Game-only';
      const statusCls = fo.status === 'mismatch' ? 'type-mismatched' : fo.status === 'used' ? 'used' : fo.status === 'package-only' ? 'package-only' : 'game-only';
      const rowBg = fo.status === 'mismatch' ? 'background:rgba(240,97,109,0.05);' : '';
      const typeGameHtml = fo.typeGame !== null ? `<span class="type-pill${fo.status === 'mismatch' ? ' mismatch' : ''}">${S[fo.typeGame]}</span>` : '<span class="dim">&mdash;</span>';
      const typePkgHtml = fo.typePkg !== null ? `<span class="type-pill${fo.status === 'mismatch' ? ' mismatch' : ''}">${S[fo.typePkg]}</span>` : '<span class="dim">&mdash;</span>';
      return `<tr style="${rowBg}">
        <td class="field-name-cell">${S[fi]}</td>
        <td>${typeGameHtml}</td>
        <td>${typePkgHtml}</td>
        <td><span class="badge ${statusCls}">${statusLabel}</span></td>
        <td style="color:var(--dim);">${fo.firstSeen !== null ? S[fo.firstSeen] : '&mdash;'}</td>
        <td style="color:var(--mid);">${fo.lastSeen !== null ? S[fo.lastSeen] : '&mdash;'}</td>
      </tr>`;
    }).join('');
  }

  // ---- Contexts in scope ----
  // Keyed by (app, app_version) -- not by package version -- since a merged
  // event can span more than one package (and package version) within a
  // single app-version; the Package Version column lists all of them.
  const ctxMap = new Map();
  scopeFacts.forEach(f => {
    const ck = f[C.APP] + ':' + f[C.APP_VER];
    if (!ctxMap.has(ck)) {
      ctxMap.set(ck, { app: f[C.APP], ver: f[C.APP_VER], pkgVers: new Set(), declaredFields: new Set(), matched: new Set(), gameOnlyFields: new Set(), typeIssues: 0 });
    }
    const c = ctxMap.get(ck);
    if (f[C.PKG_VER] !== null) c.pkgVers.add(f[C.PKG_VER]);
    if (f[C.STATUS] === ST.GAME_ONLY) {
      c.gameOnlyFields.add(f[C.FIELD]);
    } else {
      c.declaredFields.add(f[C.FIELD]);
      if (f[C.STATUS] === ST.BOTH) {
        c.matched.add(f[C.FIELD]);
        if (f[C.TYPE_GAME] !== null && !typesCompatible(str(f[C.TYPE_PKG]), str(f[C.TYPE_GAME]))) c.typeIssues++;
      }
    }
  });
  const ctxRows = [...ctxMap.values()].map(c => {
    // same declared-field-only convention as above: game-only extras don't
    // count toward coverage unless the event has no package spec at all
    const total = c.declaredFields.size > 0 ? c.declaredFields.size : c.gameOnlyFields.size;
    const matched = c.declaredFields.size > 0 ? c.matched.size : c.gameOnlyFields.size;
    const pkgVerLabel = [...c.pkgVers].map(x => S[x]).sort().join(', ');
    return { ...c, total, matched, pkgVerLabel };
  }).sort((a, b) => {
    const an = S[a.app], bn = S[b.app];
    if (an !== bn) return an.localeCompare(bn);
    return naturalVerSort(S[a.ver], S[b.ver]);
  });
  document.getElementById('ctxCount').textContent = `${ctxRows.length} app + version combination${ctxRows.length === 1 ? '' : 's'}`;
  const cbody = document.getElementById('ctxBody');
  cbody.innerHTML = ctxRows.map(c => {
    const active = c.app === app && c.ver === ver;
    return `<tr style="${active ? 'background:rgba(61,111,214,0.13);' : ''}" class="${active ? '' : 'clickable'}" data-app="${c.app}" data-ver="${c.ver}">
      <td>${active ? '▸ ' : ''}${S[c.app]}</td>
      <td style="color:var(--mid);">${S[c.ver]}</td>
      <td style="color:var(--mid);">${c.pkgVerLabel || '&mdash;'}</td>
      <td style="text-align:right;color:var(--mid);">${c.matched}/${c.total}</td>
      <td class="type-issues-cell ${c.typeIssues ? '' : 'zero'}" style="text-align:right;">${c.typeIssues ? c.typeIssues : '&mdash;'}</td>
    </tr>`;
  }).join('');
  cbody.querySelectorAll('tr.clickable').forEach(row => {
    row.addEventListener('click', () => {
      state.page2 = { event, app: parseInt(row.dataset.app, 10), ver: parseInt(row.dataset.ver, 10) };
      render();
    });
  });
}
