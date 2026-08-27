// Trench Radar — shared calls server v2.9
// Both bots (dubski + Tony) POST their calls here; everyone GETs the merged
// leaderboard with 24h / 7d / all-time best-call windows.
// Persistence: Postgres if DATABASE_URL is set, else in-memory (dev/testing).
// Auth: shared secret in the X-Radar-Key header (set RADAR_KEY env var).

const http = require('http');
const { URL } = require('url');

const PORT = process.env.PORT || 3000;
const RADAR_KEY = process.env.RADAR_KEY || 'trench-dev-key';
const DATABASE_URL = process.env.DATABASE_URL || null;
const MAX_BODY = 64 * 1024;
const MAX_INGEST = 3 * 1024 * 1024; // full session payloads (samples/devBook/feed) can be ~0.5MB

// v1.7 MERGE RULES — non-destructive, identical to the client's ledgerWrite.
// A win is permanent, a peak only ever rises, a worst only ever falls, and the
// earliest call time wins. Two PCs writing the same coin therefore converge on
// the same row no matter who writes last — order can never change the result.
// v1.8 — every shared book, and how two PCs reconcile it.
//   ledger    verdicts        merge rules below (a W is permanent)
//   runner/closed archives    immutable, first write wins
//   devbook   dev reputation  counters take the MAX (both PCs counted real events)
//   rug/devban/hidden/hotwallet/wmeta  newest reading wins
//   early     early-window book   union of windows, widest outcome envelope
//   paper     paper trades        union of policies, first close per policy wins
//
// ⚠ ADDING A KIND HERE IS HALF THE JOB. The other half is uploading this file
// to GitHub so Railway redeploys. v0.87 shipped the `good` kind to a server
// that did not have it, every POST came back 400, and the client's
// `booksServerOk` latch turned that single 400 into a total sync blackout for
// EVERY book. Client fixed in v0.88 (per-kind skip), and tests/ship-gate.js
// now fails the build if the script's SYNC_BOOKS and this list disagree.
const BOOK_KINDS = ['ledger', 'runner', 'closed', 'rug', 'good', 'devban', 'hidden',
                    'devbook', 'hotwallet', 'wmeta', 'early', 'paper'];
function mergeBook(kind, a, b) {
  if (kind === 'early') {
    // Two PCs watch the same coin from different moments. Neither reading is
    // wrong: keep the EARLIEST sighting, fill any window the other side
    // served and we missed, and widen the outcome envelope to cover both.
    // A null window is a recorded MISS and must never overwrite a real one.
    const out = { ...a };
    let changed = false;
    if (b.t0 && (!a.t0 || b.t0 < a.t0)) {
      out.t0 = b.t0;
      for (const f of ['age0', 'mc0', 'vol0', 'hold0', 'top0']) if (b[f] !== undefined) out[f] = b[f];
      changed = true;
    }
    const aw = (a.w && typeof a.w === 'object') ? a.w : {};
    const bw = (b.w && typeof b.w === 'object') ? b.w : {};
    const w = { ...aw };
    for (const k of Object.keys(bw)) {
      if (w[k] === undefined || (w[k] === null && bw[k] !== null)) { w[k] = bw[k]; changed = true; }
    }
    if (changed) out.w = w;
    if (b.peakMc !== undefined && (out.peakMc === undefined || b.peakMc > out.peakMc)) { out.peakMc = b.peakMc; changed = true; }
    if (b.lowMc !== undefined && (out.lowMc === undefined || b.lowMc < out.lowMc)) { out.lowMc = b.lowMc; changed = true; }
    if ((Number(b.lastT) || 0) > (Number(a.lastT) || 0)) { out.lastT = b.lastT; out.lastMc = b.lastMc; changed = true; }
    if ((Number(b.seenN) || 0) > (Number(a.seenN) || 0)) { out.seenN = b.seenN; changed = true; }
    if (!a.name && b.name) { out.name = b.name; changed = true; }
    if (a.called === undefined && b.called !== undefined) { out.called = b.called; changed = true; }
    if (a.veto === undefined && b.veto !== undefined) { out.veto = b.veto; changed = true; }
    return changed ? out : null;
  }
  if (kind === 'paper') {
    // One row per coin holding EVERY policy's simulated trade, so the 48-char
    // mint stays the key. A closed trade is a historical fact — first close
    // per policy wins, and an open position never overwrites a closed one.
    const out = { ...a };
    const ap = (a.p && typeof a.p === 'object') ? a.p : {};
    const bp = (b.p && typeof b.p === 'object') ? b.p : {};
    const p = { ...ap };
    let changed = false;
    for (const k of Object.keys(bp)) {
      const cur = p[k], inc = bp[k];
      if (!inc || typeof inc !== 'object') continue;
      if (!cur) { p[k] = inc; changed = true; continue; }
      if (cur.out === undefined && inc.out !== undefined) { p[k] = inc; changed = true; }
    }
    if (changed) out.p = p;
    if (b.t && (!a.t || b.t < a.t)) { out.t = b.t; changed = true; }
    if (!a.name && b.name) { out.name = b.name; changed = true; }
    return changed ? out : null;
  }
  if (kind === 'runner' || kind === 'closed') {
    // Archives record a MOMENT (hit 2x / died). "First write wins" made the
    // result depend on who happened to push first, so two PCs kept different
    // copies forever. The earliest observation is the true one, with a
    // deterministic tie-break, so both sides converge no matter the order.
    const at = Number(a && a.t) || 0, bt = Number(b && b.t) || 0;
    if (bt && (!at || bt < at)) return { ...b };
    if (bt === at) {
      const as = JSON.stringify(a), bs = JSON.stringify(b);
      if (bs < as) return { ...b };
    }
    return null;
  }
  if (kind === 'devbook') {
    // both PCs watched the same dev independently — neither count is wrong,
    // so take the higher of each and the most recent sighting.
    const out = { ...a };
    let changed = false;
    for (const f of ['seen', 'rugged', 'ran', 'dumped']) {
      const av = Number(a[f]) || 0, bv = Number(b[f]) || 0;
      if (bv > av) { out[f] = bv; changed = true; }
    }
    if ((Number(b.last) || 0) > (Number(a.last) || 0)) { out.last = b.last; changed = true; }
    return changed ? out : null;
  }
  if (kind !== 'ledger') {
    // newest reading wins, and a flag once set is never silently unset
    const at = Number(a.t) || 0, bt = Number(b.t) || 0;
    if (bt > at) return { ...a, ...b };
    return null;
  }
  const out = { ...a };
  let changed = false;
  const num = x => (typeof x === 'number' && isFinite(x) ? x : null);
  const pa = num(a.peak), pb = num(b.peak);
  if (pb !== null && (pa === null || pb > pa)) { out.peak = pb; changed = true; }
  const wa = num(a.worst), wb = num(b.worst);
  if (wb !== null && (wa === null || wb < wa)) { out.worst = wb; changed = true; }
  if (b.t && (!a.t || b.t < a.t)) { out.t = b.t; changed = true; }
  if (!a.name && b.name) { out.name = b.name; changed = true; }
  if (!a.s && b.s) { out.s = b.s; changed = true; }
  const peak = num(out.peak), worst = num(out.worst);
  const v = (a.v === 'W' || b.v === 'W' || (peak !== null && peak >= 100)) ? 'W'
    : ((worst !== null && worst <= -50) ? 'L' : 'F');
  if (v !== a.v) { out.v = v; changed = true; }
  return changed ? out : null;
}

// ------------------------------------------------------------------
// Storage layer — Postgres or in-memory, same interface
// ------------------------------------------------------------------
let store;
function memStore() {
  const calls = new Map(); // key `${user}:${mint}` -> row
  return {
    async init() {},
    async upsertCall(c) {
      const key = c.user + ':' + c.mint;
      const ex = calls.get(key);
      if (!ex || c.t < ex.t) {
        calls.set(key, { ...c, peakPct: c.peakPct ?? 0, livePct: c.livePct ?? null, rep: (ex ? ex.rep : 0) + 1 });
      } else {
        ex.rep = (ex.rep || 1) + 1;
      }
    },
    async updatePeak(user, mint, peakPct, livePct) {
      const key = user + ':' + mint;
      const ex = calls.get(key);
      if (ex) {
        if (peakPct != null && (ex.peakPct == null || peakPct > ex.peakPct)) ex.peakPct = peakPct;
        if (livePct !== undefined) ex.livePct = livePct;
      }
    },
    async board(sinceMs, sort) {
      const rows = [...calls.values()].filter(c => c.t >= sinceMs);
      return sort === 'recent' ? rows.sort((a, b) => b.t - a.t) : rows.sort((a, b) => (b.peakPct ?? -1e9) - (a.peakPct ?? -1e9));
    },
    async wlWindow(sinceMs) { return wlFromRows([...calls.values()].filter(c => c.t >= sinceMs)); },
    // v2.2 #135 — REPLACES the stored peak (updatePeak can only raise it, so
    // a fake like Ferdinand +8725% was permanent). Repairs every user's row
    // for the mint: the fake was served to every viewer.
    async fixPeak(mint, peakPct) {
      let n = 0;
      for (const ex of calls.values()) if (ex.mint === mint) { ex.peakPct = peakPct; n++; }
      return n;
    },
    async dump(what, opt) {
      if (what === 'calls') return { what, rows: [...calls.values()] };
      if (what === 'books') return { what, kind: opt.kind, rows: ((this._books || {})[opt.kind]) || {} };
      if (what === 'sessions') return { what, note: 'mem store keeps no session payloads', rows: [] };
      return null;
    },
    async ingest(user, kind, payload) { this._ingested = (this._ingested || 0) + 1; },
    async booksPut(user, kind, rows) {
      this._books = this._books || {};
      const book = this._books[kind] = this._books[kind] || {};
      let added = 0, merged = 0;
      for (const [mint, d] of Object.entries(rows || {})) {
        if (!mint || typeof d !== 'object' || d === null) continue;
        if (!book[mint]) { book[mint] = Object.assign({}, d, { u: d.u || user }); added++; continue; }
        const out = mergeBook(kind, book[mint], d);
        if (out) { book[mint] = out; merged++; }
      }
      return { added, merged };
    },
    async booksGet(kind, since, limit) {
      const book = (this._books || {})[kind] || {};
      const out = {};
      for (const [m, d] of Object.entries(book)) if ((d.t || 0) >= (Number(since) || 0)) out[m] = d;
      return out;
    },
    // v2.8 — tiny durable KV (the paper epoch must survive a redeploy).
    async setMeta(key, val) { this._meta = this._meta || {}; this._meta[key] = val; },
    async getMeta(key) { return (this._meta || {})[key]; },
    async walletsPut(entries) {
      this._wal = this._wal || new Map();
      let n = 0;
      for (const [mint, d] of Object.entries(entries)) {
        const ex = this._wal.get(mint);
        if (!ex || (d.t || 0) > (ex.t || 0)) { this._wal.set(mint, d); n++; }
      }
      return n;
    },
    async walletsGet(sinceMs, limit) {
      this._wal = this._wal || new Map();
      const out = {};
      const rows = [...this._wal.entries()].filter(([, d]) => (d.t || 0) >= sinceMs).sort((a, b) => (b[1].t || 0) - (a[1].t || 0));
      for (const [m, d] of rows.slice(0, limit)) out[m] = d;
      return out;
    },
    async intelPut(entries) {
      this._intel = this._intel || new Map();
      let n = 0;
      for (const [mint, d] of Object.entries(entries)) {
        const ex = this._intel.get(mint);
        if (!ex || (d.t || 0) > (ex.t || 0)) { this._intel.set(mint, d); n++; }
      }
      return n;
    },
    async intelGet(sinceMs, limit) {
      this._intel = this._intel || new Map();
      const out = {};
      let rows = [...this._intel.entries()].filter(([, d]) => (d.t || 0) >= sinceMs);
      rows.sort((a, b) => (b[1].t || 0) - (a[1].t || 0));
      for (const [m, d] of rows.slice(0, limit)) out[m] = d;
      return out;
    },
    async ingestCount() { return this._ingested || 0; },
    async stats() {
      const arr = [...calls.values()];
      return { calls: arr.length, oldest: arr.length ? Math.min(...arr.map(c => c.t)) : null, newest: arr.length ? Math.max(...arr.map(c => c.t)) : null, archives: this._ingested || 0, byUser: [] };
    },
  };
}
function pgStore() {
  const { Pool } = require('pg');
  const pool = new Pool({ connectionString: DATABASE_URL, ssl: { rejectUnauthorized: false } });
  return {
    async init() {
      await pool.query(`CREATE TABLE IF NOT EXISTS calls (
        id SERIAL PRIMARY KEY,
        usr TEXT NOT NULL, mint TEXT NOT NULL, name TEXT,
        mc DOUBLE PRECISION, conv INTEGER, t BIGINT NOT NULL,
        peak_pct DOUBLE PRECISION DEFAULT 0, live_pct DOUBLE PRECISION,
        rep INTEGER DEFAULT 1,
        UNIQUE(usr, mint)
      )`);
      await pool.query('CREATE INDEX IF NOT EXISTS calls_t_idx ON calls(t)');
      // v2.1 — call-time features. Until these existed, vol/holders/pro/top10/
      // snipers never left the browser and no rule could be scored off the
      // server (Claude Code round 11 §1.3). Nullable, so old clients are fine.
      for (const col of ['vol DOUBLE PRECISION', 'holders INTEGER', 'pro INTEGER',
                         'top10 DOUBLE PRECISION', 'snipers INTEGER',
                         'age INTEGER', 'watch INTEGER']) {
        await pool.query('ALTER TABLE calls ADD COLUMN IF NOT EXISTS ' + col);
      }
      // full session archives — EVERYTHING both bots see, forever
      await pool.query(`CREATE TABLE IF NOT EXISTS sessions (
        id SERIAL PRIMARY KEY,
        usr TEXT NOT NULL, kind TEXT, t BIGINT NOT NULL,
        payload JSONB NOT NULL
      )`);
      await pool.query('CREATE INDEX IF NOT EXISTS sessions_usr_t_idx ON sessions(usr, t)');
      // crew-shared bundle intel — one row per coin, newest reading wins
      await pool.query(`CREATE TABLE IF NOT EXISTS intel (
        mint TEXT PRIMARY KEY, t BIGINT NOT NULL, data JSONB NOT NULL
      )`);
      await pool.query('CREATE INDEX IF NOT EXISTS intel_t_idx ON intel(t)');
      await pool.query(`CREATE TABLE IF NOT EXISTS wallets (
        mint TEXT PRIMARY KEY, t BIGINT NOT NULL, data JSONB NOT NULL
      )`);
      await pool.query('CREATE INDEX IF NOT EXISTS wallets_t_idx ON wallets(t)');
      // v1.7 CREW BOOKS — the permanent record, shared. kind = ledger | runner
      // | closed, one row per (kind, mint). `usr` is whoever called it first,
      // so the client can still separate MINE from CREW.
      await pool.query(`CREATE TABLE IF NOT EXISTS books (
        kind TEXT NOT NULL, mint TEXT NOT NULL, usr TEXT NOT NULL,
        t BIGINT NOT NULL, upd BIGINT NOT NULL, data JSONB NOT NULL,
        PRIMARY KEY (kind, mint)
      )`);
      await pool.query('CREATE INDEX IF NOT EXISTS books_kind_upd_idx ON books(kind, upd)');
      // v2.8 — durable KV so the paper epoch survives a redeploy (it was in RAM
      // and reset to 0 on every deploy, silently re-scoping the crew balance).
      await pool.query(`CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT NOT NULL)`);
    },
    async booksPut(user, kind, rows) {
      let added = 0, merged = 0;
      for (const [mint, d] of Object.entries(rows || {})) {
        if (!mint || typeof d !== 'object' || d === null) continue;
        const key = String(mint).slice(0, 48);
        const cur = await pool.query('SELECT data FROM books WHERE kind=$1 AND mint=$2', [kind, key]);
        if (!cur.rows.length) {
          await pool.query(
            'INSERT INTO books (kind,mint,usr,t,upd,data) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (kind,mint) DO NOTHING',
            [kind, key, String(d.u || user).slice(0, 24), Number(d.t) || Date.now(), Date.now(), JSON.stringify(d)]);
          added++;
          continue;
        }
        const out = mergeBook(kind, cur.rows[0].data || {}, d);
        if (out) {
          await pool.query('UPDATE books SET data=$1, upd=$2 WHERE kind=$3 AND mint=$4',
            [JSON.stringify(out), Date.now(), kind, key]);
          merged++;
        }
      }
      return { added, merged };
    },
    async booksGet(kind, since, limit) {
      const r = await pool.query(
        'SELECT mint, usr, t, data FROM books WHERE kind=$1 AND upd >= $2 ORDER BY upd DESC LIMIT $3',
        [kind, Number(since) || 0, Math.min(Number(limit) || 4000, 8000)]);
      const out = {};
      for (const row of r.rows) out[row.mint] = Object.assign({}, row.data, { u: (row.data && row.data.u) || row.usr });
      return out;
    },
    // v2.8 — durable KV (paper epoch survives a redeploy)
    async setMeta(key, val) {
      await pool.query(
        `INSERT INTO meta (k, v) VALUES ($1, $2) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v`,
        [key, JSON.stringify(val)]);
    },
    async getMeta(key) {
      const r = await pool.query('SELECT v FROM meta WHERE k = $1', [key]);
      return r.rows[0] ? JSON.parse(r.rows[0].v) : undefined;
    },
    async upsertCall(c) {
      await pool.query(
        `INSERT INTO calls (usr,mint,name,mc,conv,t,peak_pct,live_pct,rep,vol,holders,pro,top10,snipers,age,watch)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,1,$9,$10,$11,$12,$13,$14,$15)
         ON CONFLICT (usr,mint) DO UPDATE SET
           rep = calls.rep + 1,
           t = LEAST(calls.t, EXCLUDED.t),
           vol = COALESCE(calls.vol, EXCLUDED.vol),
           holders = COALESCE(calls.holders, EXCLUDED.holders),
           pro = COALESCE(calls.pro, EXCLUDED.pro),
           top10 = COALESCE(calls.top10, EXCLUDED.top10),
           snipers = COALESCE(calls.snipers, EXCLUDED.snipers),
           age = COALESCE(calls.age, EXCLUDED.age),
           watch = COALESCE(calls.watch, EXCLUDED.watch),
           mc = CASE WHEN EXCLUDED.t < calls.t THEN EXCLUDED.mc ELSE calls.mc END`,
        [c.user, c.mint, c.name || null, c.mc ?? null, c.conv ?? null, c.t, c.peakPct ?? 0, c.livePct ?? null,
         c.vol ?? null, c.holders ?? null, c.pro ?? null, c.top10 ?? null, c.snipers ?? null, c.age ?? null, c.watch ?? null]
      );
    },
    async updatePeak(user, mint, peakPct, livePct) {
      await pool.query(
        `UPDATE calls SET
           peak_pct = GREATEST(COALESCE(peak_pct,0), COALESCE($3,0)),
           live_pct = COALESCE($4, live_pct)
         WHERE usr=$1 AND mint=$2`,
        [user, mint, peakPct ?? null, livePct ?? null]
      );
    },
    // v2.2 #135 — REPLACES the stored peak for every user's row of the mint.
    // updatePeak's GREATEST can only raise, so a corrupted running-max could
    // never be repaired from the client side.
    async fixPeak(mint, peakPct) {
      const r = await pool.query('UPDATE calls SET peak_pct = $2 WHERE mint = $1', [mint, peakPct]);
      return r.rowCount;
    },
    // v2.2 — the READ side of the archive. /ingest has been writing both PCs'
    // sessions to Postgres for months; this is how analysis gets them back
    // out without anyone hand-exporting.
    async dump(what, opt) {
      if (what === 'calls') {
        const r = await pool.query(
          `SELECT usr AS user, mint, name, mc, conv, t, peak_pct AS "peakPct", live_pct AS "livePct", rep,
                  vol, holders, pro, top10, snipers, age, watch
           FROM calls ORDER BY t DESC LIMIT 20000`);
        return { what, rows: r.rows };
      }
      if (what === 'books') {
        const r = await pool.query('SELECT mint, usr, t, upd, data FROM books WHERE kind = $1 ORDER BY upd DESC LIMIT 20000', [opt.kind]);
        const rows = {};
        for (const x of r.rows) rows[x.mint] = Object.assign({}, x.data, { u: x.usr });
        return { what, kind: opt.kind, rows };
      }
      if (what === 'sessions') {
        const r = await pool.query(
          `SELECT id, usr, kind, t, pg_column_size(payload) AS bytes FROM sessions ORDER BY t DESC LIMIT 500`);
        return { what, rows: r.rows };
      }
      if (what === 'session') {
        const r = await pool.query('SELECT id, usr, kind, t, payload FROM sessions WHERE id = $1', [opt.id]);
        return { what, row: r.rows[0] || null };
      }
      return null;
    },
    async board(sinceMs, sort) {
      const order = sort === 'recent' ? 't DESC' : 'peak_pct DESC NULLS LAST';
      const r = await pool.query(
        `SELECT usr AS user, mint, name, mc, conv, t, peak_pct AS "peakPct", live_pct AS "livePct", rep
         FROM calls WHERE t >= $1 ORDER BY ${order} LIMIT 800`,
        [sinceMs]
      );
      return r.rows;
    },
    // W-L over EVERY call in the window, aggregated in SQL. Deliberately NOT
    // built from board() — see the note on wlFromRows.
    async wlWindow(sinceMs) {
      const r = await pool.query(
        `SELECT
           COUNT(*) FILTER (WHERE peak >= 100) AS w,
           COUNT(*) FILTER (WHERE peak < 100 AND live <= -50) AS l,
           COUNT(*) AS total
         FROM (SELECT mint, MAX(COALESCE(peak_pct,0)) AS peak, MIN(live_pct) AS live
               FROM calls WHERE t >= $1 GROUP BY mint) q`,
        [sinceMs]
      );
      const row = r.rows[0] || {};
      return { w: Number(row.w) || 0, l: Number(row.l) || 0, total: Number(row.total) || 0 };
    },
    async ingest(user, kind, payload) {
      await pool.query('INSERT INTO sessions (usr, kind, t, payload) VALUES ($1,$2,$3,$4)',
        [user, kind || 'auto', Date.now(), JSON.stringify(payload)]);
    },
    async walletsPut(entries) {
      let n = 0;
      for (const [mint, d] of Object.entries(entries)) {
        const r = await pool.query(
          `INSERT INTO wallets (mint, t, data) VALUES ($1,$2,$3)
           ON CONFLICT (mint) DO UPDATE SET t = EXCLUDED.t, data = EXCLUDED.data
           WHERE wallets.t < EXCLUDED.t`,
          [String(mint).slice(0, 48), d.t || Date.now(), JSON.stringify(d)]
        );
        n += r.rowCount;
      }
      return n;
    },
    async walletsGet(sinceMs, limit) {
      const r = await pool.query('SELECT mint, data FROM wallets WHERE t >= $1 ORDER BY t DESC LIMIT $2', [sinceMs, limit]);
      const out = {};
      for (const row of r.rows) out[row.mint] = row.data;
      return out;
    },
    async intelPut(entries) {
      let n = 0;
      for (const [mint, d] of Object.entries(entries)) {
        const r = await pool.query(
          `INSERT INTO intel (mint, t, data) VALUES ($1,$2,$3)
           ON CONFLICT (mint) DO UPDATE SET
             t = EXCLUDED.t, data = EXCLUDED.data
           WHERE intel.t < EXCLUDED.t`,
          [String(mint).slice(0, 48), d.t || Date.now(), JSON.stringify(d)]
        );
        n += r.rowCount;
      }
      return n;
    },
    async intelGet(sinceMs, limit) {
      const r = await pool.query('SELECT mint, data FROM intel WHERE t >= $1 ORDER BY t DESC LIMIT $2', [sinceMs, limit]);
      const out = {};
      for (const row of r.rows) out[row.mint] = row.data;
      return out;
    },
    async ingestCount() {
      const r = await pool.query('SELECT COUNT(*) AS n FROM sessions');
      return parseInt(r.rows[0].n);
    },
    async stats() {
      const c = await pool.query('SELECT COUNT(*) AS calls, MIN(t) AS oldest, MAX(t) AS newest FROM calls');
      const s = await pool.query('SELECT COUNT(*) AS archives FROM sessions');
      const byUser = await pool.query('SELECT usr, COUNT(*) AS n FROM calls GROUP BY usr');
      return { calls: parseInt(c.rows[0].calls), oldest: c.rows[0].oldest, newest: c.rows[0].newest, archives: parseInt(s.rows[0].archives), byUser: byUser.rows };
    },
  };
}

// ------------------------------------------------------------------
// Leaderboard shaping — dedupe by CA across users, keep best
// ------------------------------------------------------------------
// dedupe by CA across users -> one row per coin (ALL of them, caller slices)
function dedupeCoins(rows) {
  const byMint = new Map();
  for (const r of rows) {
    const g = byMint.get(r.mint);
    if (!g) {
      byMint.set(r.mint, {
        mint: r.mint, name: r.name, mc: r.mc, conv: r.conv, t: r.t,
        peakPct: r.peakPct ?? 0, livePct: r.livePct ?? null,
        callers: [r.user], reps: r.rep || 1,
      });
    } else {
      g.reps += r.rep || 1;
      if (!g.callers.includes(r.user)) g.callers.push(r.user);
      if ((r.peakPct ?? 0) > (g.peakPct ?? 0)) g.peakPct = r.peakPct;
      if (r.t < g.t) { g.t = r.t; g.mc = r.mc; g.name = r.name; } // earliest call wins the display
      if (r.livePct != null) g.livePct = r.livePct;
    }
  }
  return [...byMint.values()].sort((a, b) => (b.peakPct ?? -1e9) - (a.peakPct ?? -1e9));
}
// newest-first feed: dedupe by coin, order by the crew's FIRST call time
function dedupeRecent(rows) {
  return dedupeCoins(rows).sort((a, b) => Number(b.t) - Number(a.t));
}
// W-L over EVERY coin in the window (not just the top 25) so both PCs match
// v1.6 (dubski's scoreboard): W = 2x'd (peak >= 100). L = never 2x'd AND
// currently bled (live <= -50). Flat/pending = neither.
// v1.9 — SELECTION ON THE OUTCOME. The bug, named.
//
// This used to be called as computeWL(dedupeCoins(rows)) inside the /board
// handler, and `rows` came from `board(since, sort)` which runs
//     ORDER BY peak_pct DESC NULLS LAST LIMIT 800
// when sort=peak. So the "win rate" was computed over the 800 HIGHEST-PEAK
// calls. Measured on the live server 2026-08-23:
//     /board?window=all&sort=peak   -> wins 758, losses 0,   total 758
//     /board?window=all&sort=recent -> wins 194, losses 379, total 730
// Same database, same window, the record changes with the sort order, and the
// leaderboard was showing a 758-0 record. That is scoring a sample chosen BY
// the score — the collider bias that has already cost this project four
// hypotheses (top10, p10s, runner radar, the age cohort). It should never have
// been in the code that reports our own performance.
//
// computeWL survives only for the test suites and for callers that already
// hold an unbiased set. The /board handler now uses store.wlWindow(), which
// aggregates EVERY call in the window regardless of sort.
function computeWL(coins) {
  let w = 0, l = 0;
  for (const c of coins) {
    const peak = c.peakPct;
    if (peak != null && peak >= 100) w++;
    else if (c.livePct != null && c.livePct <= -50) l++;
  }
  return { w, l };
}
// deduped by mint, then scored — identical rules to computeWL, but the caller
// must hand it EVERY row in the window, not a slice ordered by outcome
function wlFromRows(rows) {
  const byMint = new Map();
  for (const r of rows) {
    const g = byMint.get(r.mint);
    const peak = r.peakPct ?? 0;
    const live = r.livePct;
    if (!g) { byMint.set(r.mint, { peak, live }); continue; }
    if (peak > g.peak) g.peak = peak;
    if (live != null && (g.live == null || live < g.live)) g.live = live;
  }
  let w = 0, l = 0;
  for (const g of byMint.values()) {
    if (g.peak >= 100) w++;
    else if (g.live != null && g.live <= -50) l++;
  }
  return { w, l, total: byMint.size };
}
function shapeBoard(rows, limit) { return dedupeCoins(rows).slice(0, limit || 25); }

// ------------------------------------------------------------------
// HTTP
// ------------------------------------------------------------------
function send(res, code, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(code, {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type, X-Radar-Key',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  });
  res.end(body);
}
// IDEMPOTENT. A Node request stream can be read exactly once — a second
// readBody() on the same request attaches listeners to a stream that already
// ended, so 'end' never fires and the promise never settles. That silently
// hung every POST /books from v1.7 until v2.0. Caching the promise makes the
// whole class of bug impossible rather than fixing one instance of it.
function readBody(req, cap) {
  if (req._trBody) return req._trBody;
  return (req._trBody = readBodyOnce(req, cap));
}
function readBodyOnce(req, cap) {
  const limit = cap || MAX_BODY;
  return new Promise((resolve, reject) => {
    let data = '', size = 0;
    req.on('data', ch => { size += ch.length; if (size > limit) { reject(new Error('body too large')); req.destroy(); } else data += ch; });
    req.on('end', () => { try { resolve(data ? JSON.parse(data) : {}); } catch (e) { reject(e); } });
    req.on('error', reject);
  });
}
const WINDOWS = { '24h': 24 * 3600e3, '7d': 7 * 24 * 3600e3, 'all': 3650 * 24 * 3600e3 };

// ── v2.3 — the crew's SHARED paper balance ──────────────────────────────
// dubski wanted ONE simulated balance every viewer sees, not a per-machine
// number. It is deterministic: every crew call is sized 0.25 SOL and run
// through his SCALE-OUT (sell 1/3 at 2x, 1/3 at 3x, TRAIL the rest and exit if
// it fell 30% from the peak the crew observed), reconstructed from the peak/
// live data the bots already push. No price-fetching, no capital cap — a
// strategy balance, identical for everyone because it is computed here.
const PAPER = { start: 5, unit: 0.25, cost: 0.03 };
function scaleRet(peakPct, livePct) {
  const cost = PAPER.cost;
  const peak = 1 + (Number(peakPct) || 0) / 100;
  const haveLive = livePct !== null && livePct !== undefined;
  const live = haveLive ? 1 + Number(livePct) / 100 : 1;   // no live yet → mark the rest at entry, still OPEN
  let ret = 0, sold = 0;
  const clipped = peak >= 2;                        // v2.4 (CC r18 fix) — bank HALF at 2x
  if (clipped) { ret += 0.5 * (2 - 1 - cost); sold += 0.5; }
  const rem = 1 - sold;
  const trail = peak * 0.7;
  // v2.7 — the −60% DEAD cut was REMOVED (CC round 19 §2: measured neutral-to-
  // harmful, no threshold beat "off"). The trail exists ONLY after the first
  // clip; a coin that never 2x'd rides at the live quote. A missing quote never
  // books a trail-win.
  const closed = clipped && haveLive && live <= trail;
  const exit = closed ? trail : live;
  ret += rem * (exit - 1 - cost);
  return { ret, closed, sold };
}
function paperSim(rows, dead) {
  dead = dead || new Set();                         // v2.9 — CAs the crew filed DEAD (volume gone)
  const byMint = {};                               // one leg per CA (names are copycatted)
  for (const c of rows || []) {
    if (!c || !c.mint || !(Number(c.mc) > 0)) continue;
    const m = byMint[c.mint] || (byMint[c.mint] = { mint: c.mint, name: c.name || null, t0: c.t, mc0: c.mc, peak: 0, live: null, lt: -1 });
    if (c.t < m.t0) { m.t0 = c.t; m.mc0 = c.mc; }  // earliest crew call = the entry
    if ((c.peakPct || 0) > m.peak) m.peak = c.peakPct || 0;
    if (c.livePct !== null && c.livePct !== undefined && c.t >= m.lt) { m.live = c.livePct; m.lt = c.t; }
    if (!m.name && c.name) m.name = c.name;
  }
  let pnl = 0, n = 0, wins = 0, openN = 0, deadClosed = 0;
  const open = [];
  for (const m of Object.values(byMint)) {
    const r = scaleRet(m.peak, m.live);
    pnl += PAPER.unit * r.ret; n++;
    if (r.ret > 0) wins++;
    // v2.9 (dubski) — a coin the crew filed DEAD (volume gone) STOPS holding. Its
    // return is unchanged (already marked at the live quote); it just leaves the
    // HOLDING list so the board can't stack a million zombie bags.
    const isDead = dead.has(m.mint);
    if (isDead && !r.closed) deadClosed++;
    if (!r.closed && !isDead) { openN++; open.push({ mint: m.mint, name: m.name, live: m.live, peak: m.peak, sold: r.sold >= 0.5 ? 1 : 0 }); }
  }
  open.sort((a, b) => (b.live === null ? -1e9 : b.live) - (a.live === null ? -1e9 : a.live));
  return { version: '2.9', start: PAPER.start, unit: PAPER.unit,
    balance: Math.round((PAPER.start + pnl) * 1e4) / 1e4,
    pnl: Math.round(pnl * 1e4) / 1e4, n, wins,
    winRate: n ? Math.round(100 * wins / n) : null,
    openN, open: open.slice(0, 20), deadClosed, t: Date.now() };
}
let _paperCache = { at: 0, body: null };            // recompute at most every 4s
let paperEpoch = 0;   // v2.5 — a crew RESET moves this forward; /paper only sims calls at/after it

const server = http.createServer(async (req, res) => {
  try {
    if (req.method === 'OPTIONS') return send(res, 204, {});
    const u = new URL(req.url, 'http://x');

    if (req.method === 'GET' && u.pathname === '/health') return send(res, 200, { ok: true, store: DATABASE_URL ? 'pg' : 'mem', version: '2.9' });

    if (req.method === 'GET' && u.pathname === '/stats') return send(res, 200, await store.stats());

    if (req.method === 'GET' && u.pathname === '/board') {
      const win = WINDOWS[u.searchParams.get('window')] || WINDOWS['24h'];
      const sort = u.searchParams.get('sort') === 'recent' ? 'recent' : 'peak';
      const limit = Math.min(Math.max(parseInt(u.searchParams.get('limit')) || 25, 1), 100);
      const since = Date.now() - win;
      const rows = await store.board(since, sort);
      // 'recent' = the crew's live call feed, newest first. 'peak' = leaderboard.
      const all = sort === 'recent' ? dedupeRecent(rows) : dedupeCoins(rows);
      // v1.9: the record is computed over the WHOLE window, never over the
      // slice we happen to be displaying. When sort=peak that slice is chosen
      // by peak, which reported 758 wins and 0 losses. See computeWL's note.
      const wl = await store.wlWindow(since);
      return send(res, 200, {
        window: u.searchParams.get('window') || '24h', sort,
        wins: wl.w, losses: wl.l, total: wl.total,
        shown: all.length, coins: all.slice(0, limit),
        wlBasis: 'every call in the window, deduped by mint — independent of sort',
      });
    }

    // v2.3 — the crew's SHARED paper balance (same number for everyone). Public
    // GET like /board; computed from the crew's calls + pushed peak/live, cached
    // 4s so a room full of bots polling can't hammer the store.
    if (req.method === 'GET' && u.pathname === '/paper') {
      if (Date.now() - _paperCache.at < 4000 && _paperCache.body) return send(res, 200, _paperCache.body);
      const rows = (await store.board(0, 'recent')).filter(c => !paperEpoch || (Number(c.t) || 0) >= paperEpoch);
      // v2.9 — coins the crew filed DEAD (closed book = volume gone) leave HOLDING
      let dead = new Set();
      try { dead = new Set(Object.keys(await store.booksGet('closed', 0, 20000))); } catch (e) {}
      const body = paperSim(rows, dead);
      body.epoch = paperEpoch;
      _paperCache = { at: Date.now(), body };
      return send(res, 200, body);
    }

    // crew wallet crawl — both PCs pool their top-20 holder snapshots
    if (req.method === 'GET' && u.pathname === '/wallets') {
      const since = parseInt(u.searchParams.get('since')) || 0;
      const limit = Math.min(Math.max(parseInt(u.searchParams.get('limit')) || 200, 1), 400);
      const book = await store.walletsGet(since, limit);
      return send(res, 200, { count: Object.keys(book).length, book });
    }

    // crew bundle intel — GET open like /board, capped
    // v1.7 — GET /books?kind=ledger|runner|closed&since=<ms>
    if (req.method === 'GET' && u.pathname === '/books') {
      const kind = String(u.searchParams.get('kind') || 'ledger');
      if (!BOOK_KINDS.includes(kind)) return send(res, 400, { error: 'bad kind' });
      const rows = await store.booksGet(kind, Number(u.searchParams.get('since')) || 0,
        Number(u.searchParams.get('limit')) || 4000);
      return send(res, 200, { kind, count: Object.keys(rows).length, rows });
    }
    // v2.2 — full-archive reads. Key-gated even though it is a GET: calls
    // carry wallet tags and the sessions hold whole exports.
    if (req.method === 'GET' && u.pathname === '/dump') {
      if ((req.headers['x-radar-key'] || u.searchParams.get('key') || '') !== RADAR_KEY) {
        return send(res, 401, { error: 'bad key' });
      }
      const what = String(u.searchParams.get('what') || 'calls');
      if (!['calls', 'books', 'sessions', 'session'].includes(what)) return send(res, 400, { error: 'what?' });
      if (what === 'books' && !BOOK_KINDS.includes(String(u.searchParams.get('kind') || ''))) {
        return send(res, 400, { error: 'bad kind' });
      }
      const out = await store.dump(what, {
        kind: String(u.searchParams.get('kind') || ''),
        id: parseInt(u.searchParams.get('id')) || 0,
      });
      if (!out) return send(res, 400, { error: 'dump unsupported' });
      return send(res, 200, out);
    }
    if (req.method === 'GET' && u.pathname === '/intel') {
      const since = parseInt(u.searchParams.get('since')) || 0;
      const limit = Math.min(Math.max(parseInt(u.searchParams.get('limit')) || 400, 1), 800);
      const book = await store.intelGet(since, limit);
      return send(res, 200, { count: Object.keys(book).length, book });
    }

    // writes require the shared key
    if (req.method === 'POST') {
      if ((req.headers['x-radar-key'] || '') !== RADAR_KEY) return send(res, 401, { error: 'bad key' });

      // v2.5 — CREW paper reset. Moves the epoch forward so /paper sims only
      // calls from now on: everyone's shared balance restarts at 5 SOL together.
      // No body needed; the key gate above is the auth.
      if (u.pathname === '/paper/reset') {
        paperEpoch = Date.now();
        _paperCache = { at: 0, body: null };
        await store.setMeta('paperEpoch', paperEpoch);   // v2.8 — durable, survives a redeploy
        return send(res, 200, { ok: true, epoch: paperEpoch });
      }

      // ⚠ ONE READ, AT THE TOP, FOR EVERY POST. Read the note on readBody().
      //
      // THE BUG THIS FIXES — /books has never worked, not once, since v1.7.
      // The handler order was:
      //     if (u.pathname === '/ingest') { await readBody(req, MAX_INGEST); ... }
      //     const body = await readBody(req);            // <- consumed the stream
      //     ...
      //     if (u.pathname === '/books') {
      //       const big = await readBody(req, MAX_INGEST);  // <- SECOND read
      // A Node request stream can only be read once. The second readBody added
      // 'data'/'end' listeners to a stream that had already ended, so those
      // events never fired again and the promise never settled: the handler
      // hung, the server never replied, the client timed out. Reproduced
      // against this exact file — POST /call returns 200, POST /books returns
      // nothing in 3.5s. That is why /books?kind=ledger|rug|devbook all report
      // count 0 on the live server while /board holds 758 calls.
      //
      // I previously blamed the client's booksServerOk latch and the unknown
      // `good` kind for this outage. Both were real bugs and both are fixed,
      // but NEITHER was the reason nothing synced. It was always this line.
      const body = await readBody(req, MAX_INGEST);

      // full session archive — the bot auto-uploads EVERYTHING here every 30min
      // v2.2 #135 — a locally repaired peak REPLACES the server's number
      if (u.pathname === '/fixpeak') {
        if (!body.mint || typeof body.peakPct !== 'number' || !isFinite(body.peakPct)) {
          return send(res, 400, { error: 'need mint + numeric peakPct' });
        }
        const n = await store.fixPeak(String(body.mint), body.peakPct);
        return send(res, 200, { ok: true, repaired: n, was: body.was ?? null });
      }

      if (u.pathname === '/ingest') {
        if (!body.user) return send(res, 400, { error: 'need user' });
        await store.ingest(String(body.user).slice(0, 24), body.kind, body);
        const n = await store.ingestCount();
        return send(res, 200, { ok: true, archived: n });
      }

      if (u.pathname === '/call') {
        if (!body.mint || !body.user) return send(res, 400, { error: 'need user+mint' });
        await store.upsertCall({
          user: String(body.user).slice(0, 24), mint: String(body.mint).slice(0, 48),
          name: body.name ? String(body.name).slice(0, 32) : null,
          mc: num(body.mc), conv: int(body.conv), t: int(body.t) || Date.now(),
          peakPct: num(body.peakPct), livePct: num(body.livePct),
          // v2.1 — call-time features (round 11 §1.3: these never left the
          // browser before, so no rule could be scored off the server)
          vol: num(body.vol), holders: int(body.holders), pro: int(body.pro),
          top10: num(body.top10), snipers: int(body.snipers),
          age: int(body.age), watch: int(body.watch),
        });
        return send(res, 200, { ok: true });
      }
      if (u.pathname === '/intel') {
        if (!body.entries || typeof body.entries !== 'object') return send(res, 400, { error: 'need entries' });
        const entries = {};
        let i = 0;
        for (const [mint, d] of Object.entries(body.entries)) {
          if (++i > 200) break; // cap per push
          if (!d || typeof d !== 'object') continue;
          entries[String(mint).slice(0, 48)] = {
            bundles: num(d.bundles), insiders: num(d.insiders), top10: num(d.top10),
            devHold: num(d.devHold), burned: num(d.burned),
            snipers: int(d.snipers), sniperPct: num(d.sniperPct),
            t: int(d.t) || Date.now(), name: d.name ? String(d.name).slice(0, 32) : null,
            by: body.user ? String(body.user).slice(0, 24) : null,
          };
        }
        const n = await store.intelPut(entries);
        return send(res, 200, { ok: true, updated: n });
      }
      // v1.7 — POST /books {user, kind, rows:{mint:{...}}}
      // Each PC pushes what it knows; the server merges without ever losing a
      // verdict. This is what finally makes dubski's and Tony's records equal.
      if (u.pathname === '/books') {
        if (!body.user || !body.kind) return send(res, 400, { error: 'need user+kind' });
        if (!BOOK_KINDS.includes(body.kind)) return send(res, 400, { error: 'bad kind' });
        const rows = body.rows && typeof body.rows === 'object' ? body.rows : {};
        if (Object.keys(rows).length > 6000) return send(res, 413, { error: 'too many rows' });
        const r = await store.booksPut(String(body.user).slice(0, 24), body.kind, rows);
        return send(res, 200, { ok: true, ...r });
      }
      if (u.pathname === '/wallets') {
        if (!body.entries || typeof body.entries !== 'object') return send(res, 400, { error: 'need entries' });
        const entries = {};
        let i = 0;
        for (const [mint, d] of Object.entries(body.entries)) {
          if (++i > 100) break;
          if (!d || !Array.isArray(d.top)) continue;
          entries[String(mint).slice(0, 48)] = {
            t: int(d.t) || Date.now(),
            top: d.top.slice(0, 20).map(x => [String(x[0]).slice(0, 48), num(x[1])]),
            by: body.user ? String(body.user).slice(0, 24) : null,
          };
        }
        const n = await store.walletsPut(entries);
        return send(res, 200, { ok: true, updated: n });
      }
      if (u.pathname === '/peak') {
        if (!body.mint || !body.user) return send(res, 400, { error: 'need user+mint' });
        await store.updatePeak(String(body.user).slice(0, 24), String(body.mint).slice(0, 48), num(body.peakPct), num(body.livePct));
        return send(res, 200, { ok: true });
      }
    }
    return send(res, 404, { error: 'not found' });
  } catch (e) {
    return send(res, 500, { error: String(e && e.message || e).slice(0, 120) });
  }
});
function num(v) { const n = parseFloat(v); return isFinite(n) ? n : null; }
function int(v) { const n = parseInt(v); return isFinite(n) ? n : null; }

if (require.main === module) {
  (async () => {
    store = DATABASE_URL ? pgStore() : memStore();
    await store.init();
    // v2.8 — restore the crew paper epoch so a redeploy doesn't silently re-scope
    // the shared balance (it used to live only in RAM and reset to 0 every deploy).
    try { const e = await store.getMeta('paperEpoch'); if (Number(e)) { paperEpoch = Number(e); console.log('[paper] restored epoch ' + paperEpoch); } } catch (err) { console.warn('[paper] epoch restore failed:', err && err.message); }
    server.listen(PORT, () => console.log('Trench Radar server v2.9 on :' + PORT + ' (' + (DATABASE_URL ? 'postgres' : 'memory') + ')'));
  })();
}

module.exports = { shapeBoard, dedupeCoins, dedupeRecent, computeWL, memStore }; // for offline tests