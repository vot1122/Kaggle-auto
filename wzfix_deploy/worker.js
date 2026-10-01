/**
 * Multi-Bot Cloudflare Worker — WZML-X Stream Router
 *
 * This Worker provides a stable URL that routes to ephemeral
 * Cloudflare quick tunnel URLs. Multiple bots can register
 * and the Worker routes based on path prefix (/bot1/, /bot2/, etc.).
 *
 * SETUP:
 * 1. Go to Cloudflare Dashboard → Workers & Pages → Create Worker
 * 2. Name it (e.g. "wzml-router")
 * 3. Paste this entire file
 * 4. Go to Settings → Variables → Add:
 *    WORKER_SECRET = your-secret-string
 * 5. (Recommended) Create KV namespace "tunnel_store", bind as TUNNEL_KV
 * 6. Deploy
 *
 * ENDPOINTS:
 *   POST /update-tunnel?bot=bot1   — Register tunnel URL (bot registers on start)
 *   GET  /tunnel-status            — Check current tunnel status
 *   GET  /health                   — Health check (Worker → tunnel → bot)
 *   GET  /bot1/*                   — Proxy to bot1's tunnel (multi-bot)
 *   GET  /*                        — Proxy to first registered bot (single-bot)
 */

// ============================================================
// In-memory tunnel store (fallback if no KV)
// ============================================================
const tunnels = {};

// ============================================================
// Helper: get tunnel URL for a bot
// ============================================================
async function getTunnel(botId, env) {
  // Try KV first (persists across isolates)
  if (env.TUNNEL_KV) {
    const kvKey = botId || '_default';
    const kvUrl = await env.TUNNEL_KV.get('tunnel:' + kvKey);
    if (kvUrl) return kvUrl;
  }
  // Fall back to memory
  return tunnels[botId || '_default'] || null;
}

// ============================================================
// Helper: set tunnel URL for a bot
// ============================================================
async function setTunnel(botId, url, env) {
  const key = botId || '_default';
  tunnels[key] = url;
  // Also store in KV if available
  if (env.TUNNEL_KV) {
    await env.TUNNEL_KV.put('tunnel:' + key, url);
  }
}

// ---- WZFIX J-24: tunnel POOL (multi-tunnel fan-out) ---------------------
async function getPool(botId, env) {
  const key = (botId || '_default') + ':pool';
  if (env.TUNNEL_KV) {
    const v = await env.TUNNEL_KV.get('pool:' + (botId || '_default'));
    if (v) { try { return JSON.parse(v); } catch (e) {} }
  }
  return Array.isArray(tunnels[key]) ? tunnels[key] : [];
}

async function setPool(botId, urls, env) {
  const key = (botId || '_default') + ':pool';
  tunnels[key] = urls;
  if (env.TUNNEL_KV) {
    await env.TUNNEL_KV.put('pool:' + (botId || '_default'), JSON.stringify(urls));
  }
}

// ============================================================
// Helper: list all registered bots
// ============================================================
async function getAllTunnels(env) {
  const result = { ...tunnels };
  if (env.TUNNEL_KV) {
    // Try to get all keys from KV
    try {
      const list = await env.TUNNEL_KV.list({ prefix: 'tunnel:' });
      for (const key of list.keys) {
        const botId = key.name.replace('tunnel:', '');
        if (!result[botId === '_default' ? '_default' : botId]) {
          result[botId] = await env.TUNNEL_KV.get(key.name);
        }
      }
    } catch (e) {
      // KV list might not be available in all plans
    }
  }
  return result;
}

// ============================================================
// Main fetch handler
// ============================================================
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;

    // -------------------------------------------------------
    // POST /update-tunnel — Bot registers its tunnel URL
    // -------------------------------------------------------
    if (path === '/update-tunnel' && request.method === 'POST') {
      // Verify secret
      const secret = request.headers.get('X-Tunnel-Secret');
      if (secret !== env.WORKER_SECRET) {
        return new Response(JSON.stringify({ error: 'unauthorized' }), {
          status: 401,
          headers: { 'Content-Type': 'application/json' }
        });
      }

      // Get bot ID from query param
      const botId = url.searchParams.get('bot') || '_default';

      // Parse tunnel URL from body
      try {
        const body = await request.json();
        const tunnelUrl = body.url;
        if (!tunnelUrl || !tunnelUrl.includes('trycloudflare.com')) {
          return new Response(JSON.stringify({ error: 'invalid tunnel URL' }), {
            status: 400,
            headers: { 'Content-Type': 'application/json' }
          });
        }

        await setTunnel(botId, tunnelUrl, env);

        // WZFIX J-24: store the whole tunnel pool too
        const _pool = Array.isArray(body.urls)
          ? body.urls.filter(u => typeof u === 'string' && u.indexOf('trycloudflare.com') >= 0)
          : [];
        if (_pool.length) await setPool(botId, _pool, env);

        // Also update the default if this is the first bot
        const all = await getAllTunnels(env);
        const botKeys = Object.keys(all).filter(k => k !== '_default');
        if (botKeys.length === 1 && botId !== '_default') {
          await setTunnel('_default', tunnelUrl, env);
          if (_pool.length) await setPool('_default', _pool, env);
        }

        return new Response(JSON.stringify({
          success: true,
          bot: botId,
          tunnel: tunnelUrl,
          pool: _pool.length,
          registered_bots: Object.keys(all)
        }), {
          headers: { 'Content-Type': 'application/json' }
        });
      } catch (e) {
        return new Response(JSON.stringify({ error: e.message }), {
          status: 400,
          headers: { 'Content-Type': 'application/json' }
        });
      }
    }

    // -------------------------------------------------------
    // GET /tunnel-status — Check registered tunnels
    // -------------------------------------------------------
    if (path === '/tunnel-status') {
      const all = await getAllTunnels(env);
      const pools = {};
      for (const bk of Object.keys(all)) {
        const pp = await getPool(bk, env);
        if (pp && pp.length) pools[bk] = pp.length;
      }
      return new Response(JSON.stringify({
        tunnels: all,
        count: Object.keys(all).length,
        pools: pools
      }), {
        headers: { 'Content-Type': 'application/json' }
      });
    }

    // -------------------------------------------------------
    // GET /health — Health check
    // -------------------------------------------------------
    if (path === '/health') {
      const all = await getAllTunnels(env);
      const defaultTunnel = all['_default'] || Object.values(all)[0];

      if (!defaultTunnel) {
        return new Response(JSON.stringify({
          bot_responding: false,
          error: 'no tunnel registered'
        }), {
          headers: { 'Content-Type': 'application/json' }
        });
      }

      try {
        // Try to reach the bot through the tunnel
        const botResponse = await fetch(defaultTunnel + '/health', {
          signal: AbortSignal.timeout(5000)
        });
        const botData = await botResponse.text();
        return new Response(JSON.stringify({
          bot_responding: botResponse.ok,
          tunnel: defaultTunnel,
          bot_status: botData.substring(0, 200)
        }), {
          headers: { 'Content-Type': 'application/json' }
        });
      } catch (e) {
        return new Response(JSON.stringify({
          bot_responding: false,
          tunnel: defaultTunnel,
          error: e.message
        }), {
          headers: { 'Content-Type': 'application/json' }
        });
      }
    }

    // -------------------------------------------------------
    // Proxy: /bot1/* or /* → tunnel
    // -------------------------------------------------------
    // Check if path starts with a bot ID prefix
    const botMatch = path.match(/^\/(bot\d+)(\/.*)?$/);
    let botId = '_default';
    let proxyPath = path;

    if (botMatch) {
      botId = botMatch[1];
      proxyPath = botMatch[2] || '/';
    }


    // WZFIX J-24: fan a file download across the tunnel pool (direct path)
    if (proxyPath.indexOf('/webdl/dl/') === 0 || proxyPath.indexOf('/dl/') === 0) {
      const pool = await getPool(botId, env);
      if (pool.length >= 2) {
        return await wzFanOut(request, pool, proxyPath, url.search, env, botId);
      }
    }

    const tunnelUrl = await getTunnel(botId, env) || await getTunnel('_default', env);

    if (!tunnelUrl) {
      return new Response('No tunnel registered. Start the bot first.', {
        status: 503,
        headers: { 'Content-Type': 'text/plain' }
      });
    }

    // Build the target URL
    const targetUrl = tunnelUrl.replace(/\/$/, '') + proxyPath + url.search;

    // Forward the request
    try {
      const proxyReq = new Request(targetUrl, request);
      // Remove headers that shouldn't be forwarded
      proxyReq.headers.delete('X-Tunnel-Secret');

      const response = await fetch(proxyReq);

      // Add retry hint header for stream client
      const newHeaders = new Headers(response.headers);
      newHeaders.set('X-Stream-Router', 'cloudflare-worker');
      newHeaders.set('X-Bot-Id', botId);

      return new Response(response.body, {
        status: response.status,
        statusText: response.statusText,
        headers: newHeaders
      });
    } catch (e) {
      return new Response(JSON.stringify({
        error: 'Tunnel unreachable',
        details: e.message,
        tunnel: tunnelUrl
      }), {
        status: 502,
        headers: { 'Content-Type': 'application/json' }
      });
    }
  }
};


// ============================================================
// WZFIX J-24: multi-tunnel fan-out for file downloads
// ============================================================
function wzTrim(t) {
  let x = String(t || '');
  while (x.length > 0 && x.charAt(x.length - 1) === '/') x = x.slice(0, -1);
  return x;
}

async function wzFanOut(request, pool, proxyPath, search, env, botId) {
  const fwd = {};
  for (const [k, v] of request.headers) {
    const lk = k.toLowerCase();
    if (lk !== 'host' && lk !== 'content-length' && lk !== 'accept-encoding' && lk !== 'x-tunnel-secret') {
      fwd[k] = v;
    }
  }
  const tgt = (t) => wzTrim(t) + proxyPath + search;

  let size = 0;
  let canRange = false;
  const cors = {};
  try {
    const h = await fetch(tgt(pool[0]), { method: 'HEAD', headers: fwd, signal: AbortSignal.timeout(10000) });
    size = parseInt(h.headers.get('content-length') || '0', 10);
    canRange = (h.headers.get('accept-ranges') || '').toLowerCase().indexOf('bytes') >= 0;
    h.headers.forEach((v, k) => {
      if (k.toLowerCase().indexOf('access-control-') === 0) cors[k] = v;
    });
  } catch (e) {}

  const N = Math.min(pool.length, 6);
  const MIN = 8 * 1024 * 1024;
  if (!canRange || size < MIN || N < 2) {
    const r = await fetch(tgt(pool[0]), { headers: fwd });
    const nh = new Headers(r.headers);
    nh.set('X-Stream-Router', 'cloudflare-worker');
    nh.set('X-Bot-Id', botId);
    return new Response(r.body, { status: r.status, headers: nh });
  }

  const step = Math.floor(size / N);
  const started = [];
  for (let i = 0; i < N; i++) {
    const lo = i * step;
    const hi = i === N - 1 ? size - 1 : lo + step - 1;
    started.push(wzRange(pool, i, proxyPath, search, fwd, lo, hi));
  }

  const stream = new ReadableStream({
    async start(controller) {
      for (const p of started) {
        let body;
        try {
          body = await p;
        } catch (e) {
          controller.error(e);
          return;
        }
        const reader = body.getReader();
        for (;;) {
          const rd = await reader.read();
          if (rd.done) break;
          controller.enqueue(rd.value);
        }
      }
      controller.close();
    }
  });

  const out = new Headers();
  out.set('content-type', 'application/octet-stream');
  out.set('content-length', String(size));
  out.set('accept-ranges', 'bytes');
  out.set('X-Stream-Router', 'cloudflare-worker');
  out.set('X-Bot-Id', botId);
  for (const k in cors) out.set(k, cors[k]);
  return new Response(stream, { status: 206, headers: out });
}

async function wzRange(pool, startIdx, proxyPath, search, fwd, lo, hi) {
  let lastErr;
  for (let k = 0; k < pool.length; k++) {
    const t = pool[(startIdx + k) % pool.length];
    try {
      const r = await fetch(wzTrim(t) + proxyPath + search, {
        headers: Object.assign({}, fwd, { Range: 'bytes=' + lo + '-' + hi })
      });
      if (r.status !== 206) throw new Error('range status ' + r.status);
      return r.body;
    } catch (e) {
      lastErr = e;
    }
  }
  throw lastErr || new Error('range failed');
}
