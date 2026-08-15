import { createServer } from "node:http";
import { randomUUID } from "node:crypto";
import { mkdir, readFile, rename, stat, writeFile } from "node:fs/promises";
import { createReadStream } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const publicDir = path.join(__dirname, "public");
const dataDir = path.join(__dirname, "data");
const dataFile = path.join(dataDir, "link-data.json");
const port = Number(process.env.PORT || 3000);
const officialNewsTtlMs = Number(process.env.OFFICIAL_NEWS_TTL_MS || 30 * 60 * 1000);
const officialNewsTimeoutMs = Number(process.env.OFFICIAL_NEWS_TIMEOUT_MS || 12_000);

const emptyData = {
  news: [],
  jobs: [],
  products: [],
  threads: [],
};

const officialSources = [
  {
    entity: "Coljuegos",
    home: "https://www.coljuegos.gov.co/",
    urls: [
      "https://www.coljuegos.gov.co/",
      "https://www.coljuegos.gov.co/publicaciones/noticias/index.php",
      "https://www.coljuegos.gov.co/publicaciones/noticias/?tema=300014",
    ],
  },
  {
    entity: "UIAF",
    home: "https://www.uiaf.gov.co/",
    urls: [
      "https://www.uiaf.gov.co/noticias-y-comunicados",
      "https://www.uiaf.gov.co/sector/coljuegos",
      "https://uiaf.gov.co/index.php/Nueva-resoluci%C3%B3n-de-Coljuegos-para-el-sector-de-Juegos-de-Suerte-y-Azar",
      "https://www.uiaf.gov.co/resolucion-20195100044514",
      "https://www.uiaf.gov.co/node/1776",
    ],
  },
  {
    entity: "DIAN",
    home: "https://www.dian.gov.co/",
    urls: [
      "https://www.dian.gov.co/Prensa/Paginas/Comunicados-de-Prensa.aspx",
      "https://normograma.dian.gov.co/dian/compilacion/docs/oficio_dian_1661_2025.htm",
      "https://normograma.dian.gov.co/dian/compilacion/docs/oficio_dian_12496_2025.htm",
      "https://normograma.dian.gov.co/dian/compilacion/docs/oficio_dian_3320_2025.htm",
    ],
  },
  {
    entity: "Supersalud",
    home: "https://www.supersalud.gov.co/",
    urls: [
      "https://www.supersalud.gov.co/es-co/Noticias/listanoticias",
      "https://normograma.supersalud.gov.co/compilacion/docs/circular_supersalud_0005_2011.htm",
      "https://normograma.supersalud.gov.co/compilacion/docs/decreto_1278_2014.htm",
      "https://normograma.supersalud.gov.co/compilacion/docs/resolucion_coljuegos_7074_2025.htm",
    ],
  },
];

const casinoKeywords = [
  "juegos localizados",
  "juego localizado",
  "juegos de suerte y azar localizados",
  "casino",
  "casinos",
  "bingo",
  "bingos",
  "sillas de bingo",
  "tragamonedas",
  "maquinas tragamonedas",
  "maquinas electronicas tragamonedas",
  "maquinas electronicas de juego",
  "maquinitas",
  "mesas de casino",
  "mesas de juegos",
  "salas de juego",
  "operadores de juegos localizados",
  "establecimientos de juegos",
];

const officialHostSuffixes = [
  "coljuegos.gov.co",
  "uiaf.gov.co",
  "dian.gov.co",
  "normograma.dian.gov.co",
  "supersalud.gov.co",
  "normograma.supersalud.gov.co",
  "docs.supersalud.gov.co",
];

let officialNewsCache = {
  items: [],
  updatedAt: null,
  error: null,
  promise: null,
};

const mimeTypes = new Map([
  [".html", "text/html; charset=utf-8"],
  [".css", "text/css; charset=utf-8"],
  [".js", "text/javascript; charset=utf-8"],
  [".json", "application/json; charset=utf-8"],
  [".webmanifest", "application/manifest+json; charset=utf-8"],
  [".png", "image/png"],
  [".jpg", "image/jpeg"],
  [".jpeg", "image/jpeg"],
  [".svg", "image/svg+xml"],
  [".ico", "image/x-icon"],
]);

function json(res, status, payload) {
  const body = JSON.stringify(payload);
  res.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "cache-control": "no-store",
  });
  res.end(body);
}

function text(value, max = 500) {
  return String(value ?? "").trim().slice(0, max);
}

function nowStamp() {
  return new Date().toISOString();
}

function normalizeText(value) {
  return String(value ?? "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
}

function decodeEntities(value) {
  const named = new Map([
    ["amp", "&"],
    ["lt", "<"],
    ["gt", ">"],
    ["quot", '"'],
    ["apos", "'"],
    ["nbsp", " "],
    ["aacute", "\u00e1"],
    ["eacute", "\u00e9"],
    ["iacute", "\u00ed"],
    ["oacute", "\u00f3"],
    ["uacute", "\u00fa"],
    ["ntilde", "\u00f1"],
    ["Aacute", "\u00c1"],
    ["Eacute", "\u00c9"],
    ["Iacute", "\u00cd"],
    ["Oacute", "\u00d3"],
    ["Uacute", "\u00da"],
    ["Ntilde", "\u00d1"],
  ]);
  return String(value ?? "").replace(/&(#x?[0-9a-f]+|[a-z]+);/gi, (_, entity) => {
    const normalizedEntity = entity.toLowerCase();
    if (normalizedEntity.startsWith("#x")) {
      const code = Number.parseInt(normalizedEntity.slice(2), 16);
      return Number.isFinite(code) ? String.fromCodePoint(code) : "";
    }
    if (normalizedEntity.startsWith("#")) {
      const code = Number.parseInt(normalizedEntity.slice(1), 10);
      return Number.isFinite(code) ? String.fromCodePoint(code) : "";
    }
    return named.get(entity) || `&${entity};`;
  });
}

function htmlToText(html, max = 600) {
  return decodeEntities(String(html ?? "")
    .replace(/<!--[\s\S]*?-->/g, " ")
    .replace(/<script\b[\s\S]*?<\/script>/gi, " ")
    .replace(/<style\b[\s\S]*?<\/style>/gi, " ")
    .replace(/<noscript\b[\s\S]*?<\/noscript>/gi, " ")
    .replace(/<svg\b[\s\S]*?<\/svg>/gi, " "))
    .replace(/<[^>]+>/g, " ")
    .replace(/\b(?:class|title|href|src|alt|aria-label|role|data-[\w-]+)=["'][^"']*["']/gi, " ")
    .replace(/\b[\w-]+=["'][^"']*["']>?/g, " ")
    .replace(/https?:\/\/\S+/gi, " ")
    .replace(/-->/g, " ")
    .replace(/[<>]/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, max);
}

function extractPageTitle(html) {
  const h1 = html.match(/<h1\b[^>]*>([\s\S]*?)<\/h1>/i);
  const title = h1 || html.match(/<title\b[^>]*>([\s\S]*?)<\/title>/i);
  return htmlToText(title?.[1] || "", 180);
}

function safeUrl(value, base) {
  try {
    return new URL(decodeEntities(value), base).toString();
  } catch {
    return "";
  }
}

function isOfficialUrl(value) {
  try {
    const host = new URL(value).hostname.toLowerCase().replace(/^www\./, "");
    return officialHostSuffixes.some((suffix) => host === suffix || host.endsWith(`.${suffix}`));
  } catch {
    return false;
  }
}

function hashId(value) {
  let hash = 0;
  for (const char of String(value)) hash = (hash * 31 + char.charCodeAt(0)) >>> 0;
  return hash.toString(36);
}

function matchedCasinoKeywords(value) {
  const normalized = normalizeText(value);
  return casinoKeywords.filter((keyword) => normalized.includes(normalizeText(keyword)));
}

function isGenericLinkText(value) {
  const normalized = normalizeText(value);
  return !normalized || [
    "ver mas",
    "leer mas",
    "descargar",
    "image",
    "imagen",
    "buscar",
    "indice",
    "coljuegos",
    "juegos de suerte y azar",
    "juegos localizados",
    "juegos promocionales",
    "juegos novedosos",
    "rifas",
    "perfil del apostador colombiano",
    "compilacion juridica dian",
  ].includes(normalized);
}

function isLikelyContentTitle(value) {
  const normalized = normalizeText(value);
  if (isGenericLinkText(value)) return false;
  if (normalized.length >= 32) return true;
  return /\b(concepto|oficio|circular|decreto|resolucion|resolucion|acuerdo|comunicado)\b/i.test(normalized);
}

function isListingSourceUrl(source, sourceUrl) {
  const normalizedUrl = normalizeText(sourceUrl).replace(/\/$/, "");
  const normalizedHome = normalizeText(source.home).replace(/\/$/, "");
  if (normalizedUrl === normalizedHome) return true;
  try {
    const url = new URL(sourceUrl);
    const path = normalizeText(url.pathname);
    return /\/publicaciones\/noticias\/?(index\.php)?$/.test(path)
      || /comunicados-de-prensa\.aspx$/.test(path)
      || /listanoticias$/.test(path)
      || /noticias-y-comunicados$/.test(path);
  } catch {
    return false;
  }
}

function inferOfficialTitle(pageText, fallbackTitle, sourceUrl) {
  const textBody = String(pageText || "");
  const patterns = [
    /\bConcepto\s+\d+\s+de\s+\d{4}\s+DIAN\b/i,
    /\bOficio\s+\d+\s+de\s+\d{4}\s+DIAN\b/i,
    /\bCircular\s+\d+\s+de\s+\d{4}\s+SNS\b/i,
    /\bDecreto\s+\d+\s+de\s+\d{4}\b/i,
    /\bResolucion\s+(?:No\.?\s*)?\d+[\w.-]*\s*(?:de\s+\d{4})?\b/i,
    /\bAcuerdo\s+(?:No\.?\s*)?\d+[\w.-]*\s*(?:de\s+\d{4})?\b/i,
  ];
  for (const pattern of patterns) {
    const match = textBody.match(pattern);
    if (match?.[0]) return match[0].replace(/\s+/g, " ").trim();
  }
  const fromPath = decodeURIComponent(new URL(sourceUrl).pathname.split("/").pop() || "")
    .replace(/\.(html?|aspx)$/i, "")
    .replace(/[_-]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  if (fromPath && isLikelyContentTitle(fromPath)) return fromPath;
  return fallbackTitle;
}

function candidateScore(candidate) {
  const titleMatches = matchedCasinoKeywords(candidate.title);
  const contextMatches = matchedCasinoKeywords(candidate.context);
  if (!isLikelyContentTitle(candidate.title)) return -10;
  if (titleMatches.length === 0 && contextMatches.length === 0) return -10;

  const normalized = normalizeText(`${candidate.title} ${candidate.url}`);
  let score = titleMatches.length * 4 + contextMatches.length;
  if (/\b(noticia|noticias|comunicado|resolucion|concepto|oficio|circular|decreto|acuerdo)\b/.test(normalized)) score += 4;
  if (candidate.url.includes("/publicaciones/")) score += 2;
  if (candidate.kind === "page") score += 1;
  if (/^(las medidas|el operativo|esta iniciativa|lo anterior|en el caso)\b/.test(normalizeText(candidate.title))) score -= 5;
  if (candidate.url.includes("#")) score -= 2;
  if (candidate.url.endsWith("#")) score -= 10;
  return score;
}

function cleanSummary(value) {
  return String(value || "")
    .replace(/contraste aumentar tamano letra disminuir tamano letra/gi, " ")
    .replace(/breadcrumb\s+home\s+(?:&raquo;|»)?/gi, " ")
    .replace(/\S*\.gov\.co\/\S*/gi, " ")
    .replace(/\S*\.co\/\S*/gi, " ")
    .replace(/\b[\w-]+=["'][^"']*["']>?/g, " ")
    .replace(/[<>]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function keywordWindow(value, max = 260) {
  const original = String(value || "");
  const normalized = normalizeText(original);
  let firstIndex = -1;
  for (const keyword of casinoKeywords) {
    const index = normalized.indexOf(normalizeText(keyword));
    if (index >= 0 && (firstIndex === -1 || index < firstIndex)) firstIndex = index;
  }
  if (firstIndex < 0) return "";
  const start = Math.max(0, firstIndex - 90);
  return original.slice(start, start + max);
}

function summaryFromContext(title, context) {
  const cleanedTitle = normalizeText(title);
  const scopedContext = keywordWindow(context) || context;
  const sentences = String(scopedContext || "")
    .split(/(?<=[.!?])\s+|\s{2,}/)
    .map((part) => part.trim())
    .filter(Boolean);
  const selected = sentences.find((sentence) => {
    const normalized = normalizeText(sentence);
    return normalized !== cleanedTitle && matchedCasinoKeywords(sentence).length > 0;
  }) || sentences.find((sentence) => normalizeText(sentence) !== cleanedTitle) || "";
  return cleanSummary(selected).slice(0, 220);
}

async function fetchOfficialHtml(url) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), officialNewsTimeoutMs);
  try {
    const response = await fetch(url, {
      redirect: "follow",
      signal: controller.signal,
      headers: {
        "accept": "text/html,application/xhtml+xml,text/plain;q=0.9,*/*;q=0.2",
        "user-agent": "LinkApp/1.0",
      },
    });
    if (!response.ok) throw new Error(`${response.status} ${response.statusText}`);
    const contentType = response.headers.get("content-type") || "";
    if (!/text\/html|text\/plain|application\/xhtml\+xml/i.test(contentType)) return "";
    return response.text();
  } finally {
    clearTimeout(timer);
  }
}

function extractOfficialCandidates(source, sourceUrl, html) {
  const candidates = [];
  const pageText = htmlToText(html, 2600);
  const pageTitle = inferOfficialTitle(pageText, extractPageTitle(html), sourceUrl);
  if (!isListingSourceUrl(source, sourceUrl) && pageTitle && isLikelyContentTitle(pageTitle)) {
    candidates.push({
      entity: source.entity,
      title: pageTitle,
      url: sourceUrl,
      sourceUrl,
      context: pageText,
      kind: "page",
    });
  }

  const anchorPattern = /<a\b[^>]*href\s*=\s*(["'])(.*?)\1[^>]*>([\s\S]*?)<\/a>/gi;
  let match;
  while ((match = anchorPattern.exec(html)) !== null) {
    const url = safeUrl(match[2], sourceUrl);
    if (!url || !isOfficialUrl(url)) continue;
    if (/^(mailto|tel|javascript):/i.test(match[2])) continue;
    if (url.endsWith("#")) continue;

    const title = htmlToText(match[3], 180);
    if (!isLikelyContentTitle(title)) continue;

    const start = Math.max(0, match.index - 700);
    const end = Math.min(html.length, match.index + match[0].length + 900);
    const context = htmlToText(html.slice(start, end), 1200);
    candidates.push({
      entity: source.entity,
      title,
      url,
      sourceUrl,
      context,
      kind: "link",
    });
  }
  return candidates;
}

async function scrapeOfficialSource(source, sourceUrl) {
  const html = await fetchOfficialHtml(sourceUrl);
  if (!html) return [];
  return extractOfficialCandidates(source, sourceUrl, html)
    .map((candidate) => {
      const combined = `${candidate.title} ${candidate.context} ${candidate.url}`;
      const matchedKeywords = matchedCasinoKeywords(combined);
      const score = candidateScore(candidate);
      if (matchedKeywords.length === 0 || score < 1) return null;
      return {
        id: `${normalizeText(candidate.entity).replace(/\W+/g, "-")}-${hashId(candidate.url + candidate.title)}`,
        entity: candidate.entity,
        title: text(candidate.title, 180),
        summary: text(summaryFromContext(candidate.title, candidate.context), 240),
        url: candidate.url,
        sourceUrl: candidate.sourceUrl,
        matchedKeywords: matchedKeywords.slice(0, 4),
        score,
        fetchedAt: nowStamp(),
      };
    })
    .filter(Boolean);
}

function officialSourcesPublic() {
  return officialSources.map((source) => ({ entity: source.entity, home: source.home }));
}

function officialNewsSnapshot() {
  return {
    items: officialNewsCache.items,
    updatedAt: officialNewsCache.updatedAt,
    error: officialNewsCache.error,
    sources: officialSourcesPublic(),
  };
}

async function refreshOfficialNews() {
  const tasks = officialSources.flatMap((source) =>
    source.urls.map((sourceUrl) => scrapeOfficialSource(source, sourceUrl)),
  );
  const results = await Promise.allSettled(tasks);
  const byKey = new Map();
  const errors = [];

  for (const result of results) {
    if (result.status === "rejected") {
      errors.push(result.reason?.message || "No se pudo consultar una fuente oficial");
      continue;
    }
    for (const item of result.value) {
      const key = normalizeText(`${item.entity}|${item.url}`);
      const current = byKey.get(key);
      if (!current || item.score > current.score) byKey.set(key, item);
    }
  }

  officialNewsCache.items = [...byKey.values()]
    .sort((a, b) => b.score - a.score)
    .slice(0, 18)
    .map(({ score, ...item }) => item);
  officialNewsCache.updatedAt = nowStamp();
  officialNewsCache.error = errors.length && officialNewsCache.items.length === 0
    ? "No se pudieron consultar las fuentes oficiales en este momento."
    : null;
  return officialNewsSnapshot();
}

async function getOfficialNews(force = false) {
  const updatedAt = officialNewsCache.updatedAt ? Date.parse(officialNewsCache.updatedAt) : 0;
  if (!force && updatedAt && Date.now() - updatedAt < officialNewsTtlMs) return officialNewsSnapshot();
  if (officialNewsCache.promise) return officialNewsCache.promise;
  officialNewsCache.promise = refreshOfficialNews().finally(() => {
    officialNewsCache.promise = null;
  });
  return officialNewsCache.promise;
}

async function ensureDataFile() {
  await mkdir(dataDir, { recursive: true });
  try {
    await stat(dataFile);
  } catch {
    await writeFile(dataFile, JSON.stringify(emptyData, null, 2), "utf8");
  }
}

async function readData() {
  await ensureDataFile();
  try {
    const raw = await readFile(dataFile, "utf8");
    const parsed = JSON.parse(raw);
    return {
      news: Array.isArray(parsed.news) ? parsed.news : [],
      jobs: Array.isArray(parsed.jobs) ? parsed.jobs : [],
      products: Array.isArray(parsed.products) ? parsed.products : [],
      threads: Array.isArray(parsed.threads) ? parsed.threads : [],
    };
  } catch {
    return structuredClone(emptyData);
  }
}

async function writeData(data) {
  await mkdir(dataDir, { recursive: true });
  const tempFile = `${dataFile}.${process.pid}.tmp`;
  await writeFile(tempFile, JSON.stringify(data, null, 2), "utf8");
  await rename(tempFile, dataFile);
}

async function readBody(req) {
  const chunks = [];
  let size = 0;
  for await (const chunk of req) {
    size += chunk.length;
    if (size > 1_000_000) {
      const error = new Error("Payload too large");
      error.status = 413;
      throw error;
    }
    chunks.push(chunk);
  }
  if (chunks.length === 0) return {};
  return JSON.parse(Buffer.concat(chunks).toString("utf8"));
}

function notFound(res) {
  json(res, 404, { error: "Not found" });
}

function invalid(res, message) {
  json(res, 400, { error: message });
}

async function handleApi(req, res, url) {
  if (req.method === "GET" && url.pathname === "/api/state") {
    const data = await readData();
    const officialNews = await getOfficialNews(url.searchParams.get("refresh") === "1").catch(() => officialNewsSnapshot());
    json(res, 200, {
      ...data,
      officialNews: officialNews.items,
      officialNewsUpdatedAt: officialNews.updatedAt,
      officialNewsError: officialNews.error,
      officialSources: officialNews.sources,
    });
    return;
  }

  if (req.method === "GET" && url.pathname === "/api/official-news") {
    json(res, 200, await getOfficialNews(url.searchParams.get("refresh") === "1"));
    return;
  }

  if (req.method === "POST" && url.pathname === "/api/news") {
    const body = await readBody(req);
    const title = text(body.title, 140);
    const bodyText = text(body.body, 1200);
    if (!title || !bodyText) return invalid(res, "title and body are required");
    const data = await readData();
    const item = {
      id: randomUUID(),
      title,
      body: bodyText,
      category: text(body.category, 80) || "General",
      contact: text(body.contact, 120),
      createdAt: nowStamp(),
    };
    data.news.unshift(item);
    await writeData(data);
    json(res, 201, item);
    return;
  }

  if (req.method === "POST" && url.pathname === "/api/jobs") {
    const body = await readBody(req);
    const name = text(body.name, 120);
    const role = text(body.role, 120);
    if (!name || !role) return invalid(res, "name and role are required");
    const data = await readData();
    const item = {
      id: randomUUID(),
      name,
      role,
      city: text(body.city, 80),
      specialty: text(body.specialty, 240),
      contact: text(body.contact, 160),
      createdAt: nowStamp(),
    };
    data.jobs.unshift(item);
    await writeData(data);
    json(res, 201, item);
    return;
  }

  if (req.method === "POST" && url.pathname === "/api/products") {
    const body = await readBody(req);
    const name = text(body.name, 120);
    const price = text(body.price, 80);
    if (!name || !price) return invalid(res, "name and price are required");
    const data = await readData();
    const item = {
      id: randomUUID(),
      name,
      price,
      condition: text(body.condition, 80) || "Disponible",
      description: text(body.description, 500),
      contact: text(body.contact, 160),
      createdAt: nowStamp(),
    };
    data.products.unshift(item);
    await writeData(data);
    json(res, 201, item);
    return;
  }

  if (req.method === "POST" && url.pathname === "/api/threads") {
    const body = await readBody(req);
    const name = text(body.name, 120);
    const message = text(body.message, 1000);
    if (!name || !message) return invalid(res, "name and message are required");
    const data = await readData();
    const item = {
      id: randomUUID(),
      name,
      topic: text(body.topic, 120) || "Conversacion",
      createdAt: nowStamp(),
      messages: [
        {
          id: randomUUID(),
          author: name,
          text: message,
          createdAt: nowStamp(),
        },
      ],
    };
    data.threads.unshift(item);
    await writeData(data);
    json(res, 201, item);
    return;
  }

  const messageMatch = url.pathname.match(/^\/api\/threads\/([^/]+)\/messages$/);
  if (req.method === "POST" && messageMatch) {
    const body = await readBody(req);
    const author = text(body.author, 120) || "Link";
    const message = text(body.message, 1000);
    if (!message) return invalid(res, "message is required");
    const data = await readData();
    const thread = data.threads.find((item) => item.id === messageMatch[1]);
    if (!thread) return notFound(res);
    const item = {
      id: randomUUID(),
      author,
      text: message,
      createdAt: nowStamp(),
    };
    thread.messages.push(item);
    await writeData(data);
    json(res, 201, item);
    return;
  }

  notFound(res);
}

function safePublicPath(urlPathname) {
  const requested = urlPathname === "/" ? "/index.html" : urlPathname;
  const normalized = path.normalize(decodeURIComponent(requested)).replace(/^(\.\.[/\\])+/, "");
  const filePath = path.join(publicDir, normalized);
  if (!filePath.startsWith(publicDir)) return null;
  return filePath;
}

async function serveStatic(req, res, url) {
  const filePath = safePublicPath(url.pathname);
  if (!filePath) return notFound(res);
  try {
    const info = await stat(filePath);
    if (!info.isFile()) throw new Error("Not a file");
    const contentType = mimeTypes.get(path.extname(filePath).toLowerCase()) || "application/octet-stream";
    res.writeHead(200, {
      "content-type": contentType,
      "cache-control": filePath.endsWith("index.html") ? "no-store" : "public, max-age=3600",
    });
    createReadStream(filePath).pipe(res);
  } catch {
    if (!path.extname(url.pathname)) {
      const fallback = path.join(publicDir, "index.html");
      res.writeHead(200, {
        "content-type": "text/html; charset=utf-8",
        "cache-control": "no-store",
      });
      createReadStream(fallback).pipe(res);
      return;
    }
    notFound(res);
  }
}

const server = createServer(async (req, res) => {
  try {
    const url = new URL(req.url || "/", `http://${req.headers.host || "localhost"}`);
    if (req.method === "GET" && url.pathname === "/health") {
      json(res, 200, { ok: true });
      return;
    }
    if (url.pathname.startsWith("/api/")) {
      await handleApi(req, res, url);
      return;
    }
    await serveStatic(req, res, url);
  } catch (error) {
    const status = Number(error.status || 500);
    json(res, status, { error: status === 500 ? "Server error" : error.message });
  }
});

server.listen(port, () => {
  console.log(`Link app listening on ${port}`);
});
