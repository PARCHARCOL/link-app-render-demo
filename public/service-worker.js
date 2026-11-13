const CACHE_NAME = "link-app-v101";
const CORE_ASSETS = [
  "/index.html",
  "/loading.html",
  "/manifest.webmanifest",
  "/logolink.png",
  "/icon-192.png",
  "/icon-512.png",
];

self.addEventListener("install", (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) =>
      cache.addAll(CORE_ASSETS.map((asset) => new Request(asset, { cache: "reload" }))),
    ),
  );
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(keys.filter((key) => key !== CACHE_NAME).map((key) => caches.delete(key))),
    ),
  );
  self.clients.claim();
});

self.addEventListener("message", (event) => {
  if (event.data?.type === "SKIP_WAITING") self.skipWaiting();
});

self.addEventListener("fetch", (event) => {
  const request = event.request;
  const url = new URL(request.url);
  if (request.method !== "GET" || url.origin !== self.location.origin) return;

  if (url.pathname.startsWith("/api/") || url.pathname === "/health") {
    event.respondWith(fetch(request));
    return;
  }

  if (request.mode === "navigate" && url.pathname === "/") {
    event.respondWith(
      caches.open(CACHE_NAME).then(async (cache) =>
        (await cache.match("/loading.html")) || fetch(request),
      ),
    );
    return;
  }

  if (request.mode === "navigate" && url.pathname === "/index.html") {
    event.respondWith(
      caches.open(CACHE_NAME).then(async (cache) => {
        const cached = await cache.match("/index.html");
        if (cached) return cached;
        return fetch(request);
      }),
    );
    return;
  }

  event.respondWith(
    fetch(request)
      .then((response) => {
        if (response.ok) {
          const copy = response.clone();
          caches.open(CACHE_NAME).then((cache) => cache.put(request, copy));
        }
        return response;
      })
      .catch(() => caches.match(request).then((cached) => cached || Response.error())),
  );
});
