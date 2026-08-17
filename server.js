// Trench Radar — shared calls server v1.5
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
    async ingest(user, kind, payload) { this._ingested = (this._ingested || 0) + 1; },
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
    },
    async upsertCall(c) {
      await pool.query(
        `INSERT INTO calls (usr,mint,name,mc,conv,t,peak_pct,live_pct,rep)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,1)
         ON CONFLICT (usr,mint) DO UPDATE SET
           rep = calls.rep + 1,
           t = LEAST(calls.t, EXCLUDED.t),
           mc = CASE WHEN EXCLUDED.t < calls.t THEN EXCLUDED.mc ELSE calls.mc END`,
        [c.user, c.mint, c.name || null, c.mc ?? null, c.conv ?? null, c.t, c.peakPct ?? 0, c.livePct ?? null]
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
    async board(sinceMs, sort) {
      const order = sort === 'recent' ? 't DESC' : 'peak_pct DESC NULLS LAST';
      const r = await pool.query(
        `SELECT usr AS user, mint, name, mc, conv, t, peak_pct AS "peakPct", live_pct AS "livePct", rep
         FROM calls WHERE t >= $1 ORDER BY ${order} LIMIT 800`,
        [sinceMs]
      );
      return r.rows;
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
function computeWL(coins) {
  const t = Date.now();
  let w = 0, l = 0;
  for (const c of coins) {
    const peak = c.peakPct;
    if (peak != null && peak >= 25) w++;
    else if (t - Number(c.t) > 10 * 60 * 1000) l++;
  }
  return { w, l };
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
function readBody(req, cap) {
  const limit = cap || MAX_BODY;
  return new Promise((resolve, reject) => {
    let data = '', size = 0;
    req.on('data', ch => { size += ch.length; if (size > limit) { reject(new Error('body too large')); req.destroy(); } else data += ch; });
    req.on('end', () => { try { resolve(data ? JSON.parse(data) : {}); } catch (e) { reject(e); } });
    req.on('error', reject);
  });
}
const WINDOWS = { '24h': 24 * 3600e3, '7d': 7 * 24 * 3600e3, 'all': 3650 * 24 * 3600e3 };

const server = http.createServer(async (req, res) => {
  try {
    if (req.method === 'OPTIONS') return send(res, 204, {});
    const u = new URL(req.url, 'http://x');

    if (req.method === 'GET' && u.pathname === '/health') return send(res, 200, { ok: true, store: DATABASE_URL ? 'pg' : 'mem', version: '1.5' });

    if (req.method === 'GET' && u.pathname === '/stats') return send(res, 200, await store.stats());

    if (req.method === 'GET' && u.pathname === '/board') {
      const win = WINDOWS[u.searchParams.get('window')] || WINDOWS['24h'];
      const sort = u.searchParams.get('sort') === 'recent' ? 'recent' : 'peak';
      const limit = Math.min(Math.max(parseInt(u.searchParams.get('limit')) || 25, 1), 100);
      const since = Date.now() - win;
      const rows = await store.board(since, sort);
      // 'recent' = the crew's live call feed, newest first. 'peak' = leaderboard.
      const all = sort === 'recent' ? dedupeRecent(rows) : dedupeCoins(rows);
      const wl = computeWL(all);                // W-L over ALL of them (identical on every PC)
      return send(res, 200, {
        window: u.searchParams.get('window') || '24h', sort,
        wins: wl.w, losses: wl.l, total: all.length, coins: all.slice(0, limit),
      });
    }

    // crew wallet crawl — both PCs pool their top-20 holder snapshots
    if (req.method === 'GET' && u.pathname === '/wallets') {
      const since = parseInt(u.searchParams.get('since')) || 0;
      const limit = Math.min(Math.max(parseInt(u.searchParams.get('limit')) || 200, 1), 400);
      const book = await store.walletsGet(since, limit);
      return send(res, 200, { count: Object.keys(book).length, book });
    }

    // crew bundle intel — GET open like /board, capped
    if (req.method === 'GET' && u.pathname === '/intel') {
      const since = parseInt(u.searchParams.get('since')) || 0;
      const limit = Math.min(Math.max(parseInt(u.searchParams.get('limit')) || 400, 1), 800);
      const book = await store.intelGet(since, limit);
      return send(res, 200, { count: Object.keys(book).length, book });
    }

    // writes require the shared key
    if (req.method === 'POST') {
      if ((req.headers['x-radar-key'] || '') !== RADAR_KEY) return send(res, 401, { error: 'bad key' });

      // full session archive — the bot auto-uploads EVERYTHING here every 30min
      if (u.pathname === '/ingest') {
        const big = await readBody(req, MAX_INGEST);
        if (!big.user) return send(res, 400, { error: 'need user' });
        await store.ingest(String(big.user).slice(0, 24), big.kind, big);
        const n = await store.ingestCount();
        return send(res, 200, { ok: true, archived: n });
      }
      const body = await readBody(req);

      if (u.pathname === '/call') {
        if (!body.mint || !body.user) return send(res, 400, { error: 'need user+mint' });
        await store.upsertCall({
          user: String(body.user).slice(0, 24), mint: String(body.mint).slice(0, 48),
          name: body.name ? String(body.name).slice(0, 32) : null,
          mc: num(body.mc), conv: int(body.conv), t: int(body.t) || Date.now(),
          peakPct: num(body.peakPct), livePct: num(body.livePct),
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
    server.listen(PORT, () => console.log('Trench Radar server v1.5 on :' + PORT + ' (' + (DATABASE_URL ? 'postgres' : 'memory') + ')'));
  })();
}

module.exports = { shapeBoard, dedupeCoins, dedupeRecent, computeWL, memStore }; // for offline tests