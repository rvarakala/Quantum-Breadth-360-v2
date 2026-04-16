/**
 * Quantum Breadth 360 — Service Worker (Sprint 3)
 *
 * Strategy:
 *   - Static assets (JS/CSS/fonts/images): Cache-first, update in background
 *   - API calls (/api/*):                  Network-first, no cache
 *   - HTML pages (/app, /auth):            Network-first, offline fallback
 *   - External CDN (chart.js, etc.):       Cache-first (versioned URLs)
 */

const CACHE_VERSION  = 'qb360-v1';
const STATIC_CACHE   = `${CACHE_VERSION}-static`;
const OFFLINE_PAGE   = '/offline.html';

// Assets to pre-cache on install
const PRECACHE_ASSETS = [
  '/static/css/tokens.css',
  '/static/css/base.css',
  '/static/css/components.css',
  '/static/css/tabs.css',
  '/static/css/responsive.css',
  '/static/js/charts.js',
  '/static/js/overview.js',
  '/static/js/data.js',
  '/static/js/autocomplete.js',
  '/static/js/app.js',
];

// ── Install: pre-cache boot assets ──────────────────────────────────────────
self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(STATIC_CACHE).then(async (cache) => {
      // Pre-cache what we can; don't fail install if some assets 404
      await Promise.allSettled(
        PRECACHE_ASSETS.map(url => cache.add(url).catch(() => {}))
      );
    })
  );
  self.skipWaiting(); // Activate immediately
});

// ── Activate: purge old caches ───────────────────────────────────────────────
self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then(keys =>
      Promise.all(
        keys
          .filter(k => k.startsWith('qb360-') && k !== STATIC_CACHE)
          .map(k => caches.delete(k))
      )
    )
  );
  self.clients.claim();
});

// ── Fetch: routing strategy ──────────────────────────────────────────────────
self.addEventListener('fetch', (event) => {
  const { request } = event;
  const url = new URL(request.url);

  // Skip non-GET and chrome-extension requests
  if (request.method !== 'GET') return;
  if (url.protocol === 'chrome-extension:') return;

  // 1. API calls — always network, never cache
  if (url.pathname.startsWith('/api/')) {
    event.respondWith(fetch(request));
    return;
  }

  // 2. Static assets with version param — cache-first, background update
  if (
    url.pathname.startsWith('/static/') ||
    url.hostname.includes('cdn.jsdelivr.net') ||
    url.hostname.includes('fonts.googleapis.com') ||
    url.hostname.includes('fonts.gstatic.com') ||
    url.hostname.includes('unpkg.com') ||
    url.hostname.includes('d3js.org')
  ) {
    event.respondWith(_cacheFirst(request));
    return;
  }

  // 3. HTML navigation — network-first, offline fallback
  if (request.mode === 'navigate' || request.headers.get('accept')?.includes('text/html')) {
    event.respondWith(_networkFirstHtml(request));
    return;
  }

  // 4. Everything else — network passthrough
  event.respondWith(fetch(request).catch(() => caches.match(request)));
});

// ── Strategy: Cache-first with background revalidation ──────────────────────
async function _cacheFirst(request) {
  const cache = await caches.open(STATIC_CACHE);
  const cached = await cache.match(request);

  // Serve from cache immediately, then refresh in background
  if (cached) {
    _refreshInBackground(cache, request);
    return cached;
  }

  // Not cached yet — fetch, store, return
  try {
    const response = await fetch(request);
    if (response.ok) cache.put(request, response.clone());
    return response;
  } catch {
    return new Response('Asset unavailable offline', { status: 503 });
  }
}

function _refreshInBackground(cache, request) {
  fetch(request).then(r => {
    if (r.ok) cache.put(request, r);
  }).catch(() => {});
}

// ── Strategy: Network-first with offline fallback for HTML ───────────────────
async function _networkFirstHtml(request) {
  try {
    const response = await fetch(request);
    if (response.ok) {
      const cache = await caches.open(STATIC_CACHE);
      cache.put(request, response.clone());
    }
    return response;
  } catch {
    const cached = await caches.match(request);
    if (cached) return cached;
    // Last resort: offline page (if it exists)
    const offline = await caches.match(OFFLINE_PAGE);
    return offline || new Response(
      `<!DOCTYPE html><html><head><title>Offline — QB360</title>
      <style>body{font-family:monospace;background:#060a14;color:#94a3b8;
      display:flex;align-items:center;justify-content:center;height:100vh;text-align:center}
      h1{color:#06b6d4;font-size:20px}p{font-size:13px;margin-top:8px}</style></head>
      <body><div><h1>⚛ Quantum Breadth 360</h1>
      <p>You appear to be offline.<br>Reconnect to continue trading.</p></div></body></html>`,
      { headers: { 'Content-Type': 'text/html' }, status: 503 }
    );
  }
}
