// Trench Radar — shared calls server
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
    async board(sinceMs) {
      return [...calls.values()].filter(c => c.t >= sinceMs);
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
    async board(sinceMs) {
      const r = await pool.query(
        `SELECT usr AS user, mint, name, mc, conv, t, peak_pct AS "peakPct", live_pct AS "livePct", rep
         FROM calls WHERE t >= $1 ORDER BY peak_pct DESC NULLS LAST LIMIT 500`,
        [sinceMs]
      );
      return r.rows;
    },
  };
}

// ------------------------------------------------------------------
// Leaderboard shaping — dedupe by CA across users, keep best
// ------------------------------------------------------------------
function shapeBoard(rows, limit) {
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
  return [...byMint.values()]
    .sort((a, b) => (b.peakPct ?? -1e9) - (a.peakPct ?? -1e9))
    .slice(0, limit || 25);
}

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
function readBody(req) {
  return new Promise((resolve, reject) => {
    let data = '', size = 0;
    req.on('data', ch => { size += ch.length; if (size > MAX_BODY) { reject(new Error('body too large')); req.destroy(); } else data += ch; });
    req.on('end', () => { try { resolve(data ? JSON.parse(data) : {}); } catch (e) { reject(e); } });
    req.on('error', reject);
  });
}
const WINDOWS = { '24h': 24 * 3600e3, '7d': 7 * 24 * 3600e3, 'all': 3650 * 24 * 3600e3 };

const server = http.createServer(async (req, res) => {
  try {
    if (req.method === 'OPTIONS') return send(res, 204, {});
    const u = new URL(req.url, 'http://x');

    if (req.method === 'GET' && u.pathname === '/health') return send(res, 200, { ok: true, store: DATABASE_URL ? 'pg' : 'mem' });

    if (req.method === 'GET' && u.pathname === '/board') {
      const win = WINDOWS[u.searchParams.get('window')] || WINDOWS['24h'];
      const since = Date.now() - win;
      const rows = await store.board(since);
      return send(res, 200, { window: u.searchParams.get('window') || '24h', coins: shapeBoard(rows, 25) });
    }

    // writes require the shared key
    if (req.method === 'POST') {
      if ((req.headers['x-radar-key'] || '') !== RADAR_KEY) return send(res, 401, { error: 'bad key' });
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
    server.listen(PORT, () => console.log('Trench Radar server on :' + PORT + ' (' + (DATABASE_URL ? 'postgres' : 'memory') + ')'));
  })();
}

module.exports = { shapeBoard, memStore }; // for offline tests
