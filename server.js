import { createServer } from "node:http";
import { createHash, randomBytes, randomUUID, scrypt, timingSafeEqual } from "node:crypto";
import { mkdir, readFile, rename, stat, writeFile } from "node:fs/promises";
import { createReadStream } from "node:fs";
import path from "node:path";
import { promisify } from "node:util";
import { fileURLToPath } from "node:url";
import pg from "pg";

const { Pool } = pg;
const scryptAsync = promisify(scrypt);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const publicDir = path.join(__dirname, "public");
const dataDir = path.join(__dirname, "data");
const dataFile = path.join(dataDir, "link-data.json");
const port = Number(process.env.PORT || 3000);
const databaseUrl = process.env.DATABASE_URL || "";
const officialNewsTtlMs = Number(process.env.OFFICIAL_NEWS_TTL_MS || 30 * 60 * 1000);
const officialNewsTimeoutMs = Number(process.env.OFFICIAL_NEWS_TIMEOUT_MS || 12_000);
const bodyLimitBytes = Number(process.env.BODY_LIMIT_BYTES || 6_000_000);
const sessionDays = Number(process.env.SESSION_DAYS || 30);

const emptyData = {
  users: [],
  sessions: [],
  news: [],
  jobs: [],
  resumes: [],
  vacancies: [],
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

let officialNewsCache = {
  items: [],
  updatedAt: null,
  error: null,
  promise: null,
};

let dbReady = false;
let dbError = null;
let pool = null;

if (databaseUrl) {
  const sslMode = process.env.PGSSLMODE || "";
  const needsSsl = /sslmode=require/i.test(databaseUrl) || sslMode === "require";
  pool = new Pool({
    connectionString: databaseUrl,
    max: Number(process.env.PG_POOL_MAX || 5),
    ssl: needsSsl ? { rejectUnauthorized: false } : undefined,
  });
}

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

function dataText(value, max) {
  const output = String(value ?? "").trim();
  if (!output) return "";
  if (!output.startsWith("data:")) return "";
  if (output.length > max) {
    const error = new Error("El archivo supera el tamano permitido");
    error.status = 413;
    throw error;
  }
  return output;
}

function nowStamp() {
  return new Date().toISOString();
}

function normalizeEmail(value) {
  return text(value, 160).toLowerCase();
}

function normalizeAccountType(value) {
  return value === "company" ? "company" : "person";
}

function tokenHash(token) {
  return createHash("sha256").update(String(token)).digest("hex");
}

function storageInfo() {
  return {
    mode: dbReady ? "postgres" : "json",
    dbConfigured: Boolean(databaseUrl),
    dbReady,
    dbError,
  };
}

function fail(status, message) {
  const error = new Error(message);
  error.status = status;
  throw error;
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

function htmlEscape(value) {
  return String(value ?? "").replace(/[&<>"']/g, (char) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;",
  })[char]);
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
  return /\b(concepto|oficio|circular|decreto|resolucion|acuerdo|comunicado)\b/i.test(normalized);
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
    .replace(/breadcrumb\s+home\s+(?:&raquo;|Â»)?/gi, " ")
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

function normalizeData(parsed = {}) {
  return {
    users: Array.isArray(parsed.users) ? parsed.users : [],
    sessions: Array.isArray(parsed.sessions) ? parsed.sessions : [],
    news: Array.isArray(parsed.news) ? parsed.news : [],
    jobs: Array.isArray(parsed.jobs) ? parsed.jobs : [],
    resumes: Array.isArray(parsed.resumes) ? parsed.resumes : [],
    vacancies: Array.isArray(parsed.vacancies) ? parsed.vacancies : [],
    products: Array.isArray(parsed.products) ? parsed.products : [],
    threads: Array.isArray(parsed.threads) ? parsed.threads : [],
  };
}

async function ensureDataFile() {
  await mkdir(dataDir, { recursive: true });
  try {
    await stat(dataFile);
  } catch {
    await writeFile(dataFile, JSON.stringify(emptyData, null, 2), "utf8");
  }
}

async function readJsonData() {
  await ensureDataFile();
  try {
    const raw = await readFile(dataFile, "utf8");
    return normalizeData(JSON.parse(raw));
  } catch {
    return structuredClone(emptyData);
  }
}

async function writeJsonData(data) {
  await mkdir(dataDir, { recursive: true });
  const tempFile = `${dataFile}.${process.pid}.tmp`;
  await writeFile(tempFile, JSON.stringify(normalizeData(data), null, 2), "utf8");
  await rename(tempFile, dataFile);
}

async function initDb() {
  if (!pool) return;
  await pool.query(`
    CREATE TABLE IF NOT EXISTS link_users (
      id uuid PRIMARY KEY,
      email text UNIQUE NOT NULL,
      password_hash text NOT NULL,
      account_type text NOT NULL CHECK (account_type IN ('person', 'company')),
      display_name text NOT NULL,
      phone text DEFAULT '',
      city text DEFAULT '',
      company_name text DEFAULT '',
      nit text DEFAULT '',
      role text DEFAULT '',
      created_at timestamptz NOT NULL DEFAULT now(),
      updated_at timestamptz NOT NULL DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS link_sessions (
      token_hash text PRIMARY KEY,
      user_id uuid NOT NULL REFERENCES link_users(id) ON DELETE CASCADE,
      expires_at timestamptz NOT NULL,
      created_at timestamptz NOT NULL DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS link_news (
      id uuid PRIMARY KEY,
      author_id uuid REFERENCES link_users(id) ON DELETE SET NULL,
      title text NOT NULL,
      body text NOT NULL,
      category text NOT NULL DEFAULT 'General',
      contact text DEFAULT '',
      created_at timestamptz NOT NULL DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS link_products (
      id uuid PRIMARY KEY,
      author_id uuid REFERENCES link_users(id) ON DELETE SET NULL,
      name text NOT NULL,
      price text NOT NULL,
      condition text NOT NULL DEFAULT 'Disponible',
      description text DEFAULT '',
      contact text DEFAULT '',
      created_at timestamptz NOT NULL DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS link_threads (
      id uuid PRIMARY KEY,
      author_id uuid REFERENCES link_users(id) ON DELETE SET NULL,
      name text NOT NULL,
      topic text NOT NULL DEFAULT 'Conversacion',
      created_at timestamptz NOT NULL DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS link_messages (
      id uuid PRIMARY KEY,
      thread_id uuid NOT NULL REFERENCES link_threads(id) ON DELETE CASCADE,
      author_id uuid REFERENCES link_users(id) ON DELETE SET NULL,
      author text NOT NULL,
      text text NOT NULL,
      created_at timestamptz NOT NULL DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS link_resumes (
      id uuid PRIMARY KEY,
      user_id uuid NOT NULL REFERENCES link_users(id) ON DELETE CASCADE,
      full_name text NOT NULL,
      headline text NOT NULL,
      document_id text DEFAULT '',
      city text DEFAULT '',
      phone text DEFAULT '',
      email text DEFAULT '',
      availability text DEFAULT '',
      salary text DEFAULT '',
      summary text DEFAULT '',
      experience text DEFAULT '',
      education text DEFAULT '',
      skills text DEFAULT '',
      references_text text DEFAULT '',
      photo_data text DEFAULT '',
      attachment_name text DEFAULT '',
      attachment_data text DEFAULT '',
      is_public boolean NOT NULL DEFAULT true,
      created_at timestamptz NOT NULL DEFAULT now(),
      updated_at timestamptz NOT NULL DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS link_vacancies (
      id uuid PRIMARY KEY,
      user_id uuid NOT NULL REFERENCES link_users(id) ON DELETE CASCADE,
      company text NOT NULL,
      title text NOT NULL,
      city text DEFAULT '',
      salary text DEFAULT '',
      contact text DEFAULT '',
      description text DEFAULT '',
      requirements text DEFAULT '',
      created_at timestamptz NOT NULL DEFAULT now()
    );
  `);
}

async function initializeStorage() {
  await ensureDataFile();
  if (!pool) return;
  try {
    await initDb();
    dbReady = true;
    dbError = null;
    console.log("Link app using PostgreSQL storage");
  } catch (error) {
    dbReady = false;
    dbError = error.message;
    console.error(`PostgreSQL unavailable, using JSON storage: ${error.message}`);
  }
}

function authToken(req) {
  const header = req.headers.authorization || "";
  const match = header.match(/^Bearer\s+(.+)$/i);
  return match ? match[1].trim() : "";
}

async function hashPassword(password) {
  const salt = randomBytes(16).toString("hex");
  const derived = await scryptAsync(password, salt, 64);
  return `scrypt:${salt}:${derived.toString("hex")}`;
}

async function verifyPassword(password, stored) {
  const [scheme, salt, expectedHex] = String(stored || "").split(":");
  if (scheme !== "scrypt" || !salt || !expectedHex) return false;
  const actual = await scryptAsync(password, salt, 64);
  const expected = Buffer.from(expectedHex, "hex");
  return expected.length === actual.length && timingSafeEqual(expected, actual);
}

function sanitizeUser(user) {
  if (!user) return null;
  return {
    id: user.id,
    email: user.email,
    accountType: user.accountType || user.account_type,
    displayName: user.displayName || user.display_name,
    phone: user.phone || "",
    city: user.city || "",
    companyName: user.companyName || user.company_name || "",
    nit: user.nit || "",
    role: user.role || "",
    createdAt: user.createdAt || user.created_at || null,
  };
}

function rowUser(row) {
  if (!row) return null;
  return {
    id: row.id,
    email: row.email,
    accountType: row.account_type,
    displayName: row.display_name,
    phone: row.phone || "",
    city: row.city || "",
    companyName: row.company_name || "",
    nit: row.nit || "",
    role: row.role || "",
    createdAt: row.createdAt || row.created_at,
  };
}

function publicAuthor(user) {
  return user?.displayName || user?.display_name || user?.email || "Link";
}

async function createSession(userId) {
  const token = randomBytes(32).toString("base64url");
  const hashed = tokenHash(token);
  const expiresAt = new Date(Date.now() + sessionDays * 24 * 60 * 60 * 1000).toISOString();
  if (dbReady) {
    await pool.query(
      "INSERT INTO link_sessions (token_hash, user_id, expires_at) VALUES ($1, $2, $3)",
      [hashed, userId, expiresAt],
    );
  } else {
    const data = await readJsonData();
    data.sessions = data.sessions.filter((item) => Date.parse(item.expiresAt) > Date.now());
    data.sessions.push({ tokenHash: hashed, userId, expiresAt, createdAt: nowStamp() });
    await writeJsonData(data);
  }
  return token;
}

async function getAuthUser(req) {
  const token = authToken(req);
  if (!token) return null;
  const hashed = tokenHash(token);
  if (dbReady) {
    const result = await pool.query(
      `SELECT u.id, u.email, u.account_type, u.display_name, u.phone, u.city, u.company_name, u.nit, u.role, u.created_at
       FROM link_sessions s
       JOIN link_users u ON u.id = s.user_id
       WHERE s.token_hash = $1 AND s.expires_at > now()`,
      [hashed],
    );
    return rowUser(result.rows[0]);
  }
  const data = await readJsonData();
  const session = data.sessions.find((item) => item.tokenHash === hashed && Date.parse(item.expiresAt) > Date.now());
  if (!session) return null;
  return sanitizeUser(data.users.find((item) => item.id === session.userId));
}

async function requireUser(req) {
  const user = await getAuthUser(req);
  if (!user) fail(401, "Debes iniciar sesion");
  return user;
}

async function registerUser(body) {
  const email = normalizeEmail(body.email);
  const password = String(body.password || "");
  const accountType = normalizeAccountType(body.accountType);
  const displayName = text(body.displayName, 120) || text(body.companyName, 120);
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) fail(400, "Correo invalido");
  if (password.length < 6) fail(400, "La clave debe tener minimo 6 caracteres");
  if (!displayName) fail(400, "Nombre requerido");

  const user = {
    id: randomUUID(),
    email,
    passwordHash: await hashPassword(password),
    accountType,
    displayName,
    phone: text(body.phone, 80),
    city: text(body.city, 80),
    companyName: accountType === "company" ? text(body.companyName, 140) : "",
    nit: accountType === "company" ? text(body.nit, 40) : "",
    role: text(body.role, 120),
    createdAt: nowStamp(),
  };

  if (dbReady) {
    try {
      await pool.query(
        `INSERT INTO link_users
         (id, email, password_hash, account_type, display_name, phone, city, company_name, nit, role)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
        [user.id, user.email, user.passwordHash, user.accountType, user.displayName, user.phone, user.city, user.companyName, user.nit, user.role],
      );
    } catch (error) {
      if (error.code === "23505") fail(409, "Ese correo ya esta registrado");
      throw error;
    }
  } else {
    const data = await readJsonData();
    if (data.users.some((item) => normalizeEmail(item.email) === email)) fail(409, "Ese correo ya esta registrado");
    data.users.push(user);
    await writeJsonData(data);
  }

  const token = await createSession(user.id);
  return { token, user: sanitizeUser(user) };
}

async function loginUser(body) {
  const email = normalizeEmail(body.email);
  const password = String(body.password || "");
  let user;
  let passwordHash;
  if (dbReady) {
    const result = await pool.query(
      `SELECT id, email, password_hash, account_type, display_name, phone, city, company_name, nit, role, created_at
       FROM link_users WHERE email = $1`,
      [email],
    );
    const row = result.rows[0];
    if (row) {
      user = rowUser(row);
      passwordHash = row.password_hash;
    }
  } else {
    const data = await readJsonData();
    const found = data.users.find((item) => normalizeEmail(item.email) === email);
    if (found) {
      user = sanitizeUser(found);
      passwordHash = found.passwordHash;
    }
  }
  if (!user || !await verifyPassword(password, passwordHash)) fail(401, "Correo o clave incorrectos");
  const token = await createSession(user.id);
  return { token, user };
}

async function logoutUser(req) {
  const token = authToken(req);
  if (!token) return;
  const hashed = tokenHash(token);
  if (dbReady) {
    await pool.query("DELETE FROM link_sessions WHERE token_hash = $1", [hashed]);
  } else {
    const data = await readJsonData();
    data.sessions = data.sessions.filter((item) => item.tokenHash !== hashed);
    await writeJsonData(data);
  }
}

async function readData(authUser = null) {
  if (dbReady) {
    const [
      news,
      products,
      threads,
      messages,
      resumes,
      vacancies,
    ] = await Promise.all([
      pool.query(`SELECT id, title, body, category, contact, created_at AS "createdAt" FROM link_news ORDER BY created_at DESC LIMIT 100`),
      pool.query(`SELECT id, name, price, condition, description, contact, created_at AS "createdAt" FROM link_products ORDER BY created_at DESC LIMIT 100`),
      pool.query(`SELECT id, name, topic, created_at AS "createdAt" FROM link_threads ORDER BY created_at DESC LIMIT 50`),
      pool.query(`SELECT id, thread_id AS "threadId", author, text, created_at AS "createdAt" FROM link_messages ORDER BY created_at ASC LIMIT 500`),
      pool.query(
        `SELECT id, full_name AS "fullName", headline, document_id AS "documentId", city, phone, email, availability, salary,
                summary, experience, education, skills, references_text AS "referencesText", photo_data AS "photoData",
                attachment_name AS "attachmentName", created_at AS "createdAt", updated_at AS "updatedAt"
         FROM link_resumes
         WHERE is_public = true OR ($1::uuid IS NOT NULL AND user_id = $1::uuid)
         ORDER BY updated_at DESC LIMIT 100`,
        [authUser?.id || null],
      ),
      pool.query(`SELECT id, company, title, city, salary, contact, description, requirements, created_at AS "createdAt" FROM link_vacancies ORDER BY created_at DESC LIMIT 100`),
    ]);

    const messagesByThread = new Map();
    for (const message of messages.rows) {
      const list = messagesByThread.get(message.threadId) || [];
      list.push(message);
      messagesByThread.set(message.threadId, list);
    }

    return {
      news: news.rows,
      jobs: [],
      resumes: resumes.rows,
      vacancies: vacancies.rows,
      products: products.rows,
      threads: threads.rows.map((thread) => ({ ...thread, messages: messagesByThread.get(thread.id) || [] })),
      currentUser: authUser,
      storage: storageInfo(),
    };
  }

  const data = await readJsonData();
  return {
    news: data.news,
    jobs: data.jobs,
    resumes: data.resumes,
    vacancies: data.vacancies,
    products: data.products,
    threads: data.threads,
    currentUser: authUser,
    storage: storageInfo(),
  };
}

async function saveNews(body, user) {
  const title = text(body.title, 140);
  const bodyText = text(body.body, 1200);
  if (!title || !bodyText) fail(400, "Titulo y contenido son requeridos");
  const item = {
    id: randomUUID(),
    title,
    body: bodyText,
    category: text(body.category, 80) || "General",
    contact: text(body.contact, 120),
    createdAt: nowStamp(),
  };
  if (dbReady) {
    await pool.query(
      `INSERT INTO link_news (id, author_id, title, body, category, contact) VALUES ($1, $2, $3, $4, $5, $6)`,
      [item.id, user.id, item.title, item.body, item.category, item.contact],
    );
  } else {
    const data = await readJsonData();
    data.news.unshift({ ...item, authorId: user.id });
    await writeJsonData(data);
  }
  return item;
}

async function saveProduct(body, user) {
  const name = text(body.name, 120);
  const price = text(body.price, 80);
  if (!name || !price) fail(400, "Producto y precio son requeridos");
  const item = {
    id: randomUUID(),
    name,
    price,
    condition: text(body.condition, 80) || "Disponible",
    description: text(body.description, 500),
    contact: text(body.contact, 160) || user.phone || user.email,
    createdAt: nowStamp(),
  };
  if (dbReady) {
    await pool.query(
      `INSERT INTO link_products (id, author_id, name, price, condition, description, contact)
       VALUES ($1, $2, $3, $4, $5, $6, $7)`,
      [item.id, user.id, item.name, item.price, item.condition, item.description, item.contact],
    );
  } else {
    const data = await readJsonData();
    data.products.unshift({ ...item, authorId: user.id });
    await writeJsonData(data);
  }
  return item;
}

async function saveThread(body, user) {
  const message = text(body.message, 1000);
  if (!message) fail(400, "Mensaje requerido");
  const item = {
    id: randomUUID(),
    name: publicAuthor(user),
    topic: text(body.topic, 120) || "Conversacion",
    createdAt: nowStamp(),
    messages: [
      {
        id: randomUUID(),
        author: publicAuthor(user),
        text: message,
        createdAt: nowStamp(),
      },
    ],
  };
  if (dbReady) {
    await pool.query(`INSERT INTO link_threads (id, author_id, name, topic) VALUES ($1, $2, $3, $4)`, [item.id, user.id, item.name, item.topic]);
    await pool.query(
      `INSERT INTO link_messages (id, thread_id, author_id, author, text) VALUES ($1, $2, $3, $4, $5)`,
      [item.messages[0].id, item.id, user.id, item.messages[0].author, item.messages[0].text],
    );
  } else {
    const data = await readJsonData();
    data.threads.unshift({ ...item, authorId: user.id });
    await writeJsonData(data);
  }
  return item;
}

async function saveMessage(threadId, body, user) {
  const message = text(body.message, 1000);
  if (!message) fail(400, "Mensaje requerido");
  const item = {
    id: randomUUID(),
    author: publicAuthor(user),
    text: message,
    createdAt: nowStamp(),
  };
  if (dbReady) {
    const thread = await pool.query("SELECT id FROM link_threads WHERE id = $1", [threadId]);
    if (!thread.rows[0]) fail(404, "Conversacion no encontrada");
    await pool.query(
      `INSERT INTO link_messages (id, thread_id, author_id, author, text) VALUES ($1, $2, $3, $4, $5)`,
      [item.id, threadId, user.id, item.author, item.text],
    );
  } else {
    const data = await readJsonData();
    const thread = data.threads.find((entry) => entry.id === threadId);
    if (!thread) fail(404, "Conversacion no encontrada");
    thread.messages.push({ ...item, authorId: user.id });
    await writeJsonData(data);
  }
  return item;
}

async function saveResume(body, user) {
  if (user.accountType !== "person") fail(403, "Solo persona natural puede publicar hoja de vida");
  const fullName = text(body.fullName, 140) || user.displayName;
  const headline = text(body.headline, 160);
  if (!fullName || !headline) fail(400, "Nombre y perfil profesional son requeridos");
  const item = {
    id: randomUUID(),
    userId: user.id,
    fullName,
    headline,
    documentId: text(body.documentId, 60),
    city: text(body.city, 80) || user.city,
    phone: text(body.phone, 80) || user.phone,
    email: text(body.email, 160) || user.email,
    availability: text(body.availability, 120),
    salary: text(body.salary, 80),
    summary: text(body.summary, 1200),
    experience: text(body.experience, 2000),
    education: text(body.education, 1600),
    skills: text(body.skills, 1000),
    referencesText: text(body.referencesText, 1000),
    photoData: dataText(body.photoData, 2_000_000),
    attachmentName: text(body.attachmentName, 160),
    attachmentData: dataText(body.attachmentData, 4_000_000),
    createdAt: nowStamp(),
    updatedAt: nowStamp(),
  };

  if (dbReady) {
    await pool.query("DELETE FROM link_resumes WHERE user_id = $1", [user.id]);
    await pool.query(
      `INSERT INTO link_resumes
       (id, user_id, full_name, headline, document_id, city, phone, email, availability, salary, summary, experience,
        education, skills, references_text, photo_data, attachment_name, attachment_data)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)`,
      [
        item.id, item.userId, item.fullName, item.headline, item.documentId, item.city, item.phone, item.email,
        item.availability, item.salary, item.summary, item.experience, item.education, item.skills,
        item.referencesText, item.photoData, item.attachmentName, item.attachmentData,
      ],
    );
  } else {
    const data = await readJsonData();
    data.resumes = data.resumes.filter((entry) => entry.userId !== user.id);
    data.resumes.unshift(item);
    await writeJsonData(data);
  }
  return item;
}

async function saveVacancy(body, user) {
  if (user.accountType !== "company") fail(403, "Solo empresa puede publicar vacantes");
  const company = text(body.company, 140) || user.companyName || user.displayName;
  const title = text(body.title, 140);
  if (!company || !title) fail(400, "Empresa y cargo son requeridos");
  const item = {
    id: randomUUID(),
    userId: user.id,
    company,
    title,
    city: text(body.city, 80) || user.city,
    salary: text(body.salary, 80),
    contact: text(body.contact, 160) || user.phone || user.email,
    description: text(body.description, 1200),
    requirements: text(body.requirements, 1200),
    createdAt: nowStamp(),
  };
  if (dbReady) {
    await pool.query(
      `INSERT INTO link_vacancies (id, user_id, company, title, city, salary, contact, description, requirements)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
      [item.id, item.userId, item.company, item.title, item.city, item.salary, item.contact, item.description, item.requirements],
    );
  } else {
    const data = await readJsonData();
    data.vacancies.unshift(item);
    await writeJsonData(data);
  }
  return item;
}

async function getResume(id) {
  if (dbReady) {
    const result = await pool.query(
      `SELECT id, full_name AS "fullName", headline, document_id AS "documentId", city, phone, email, availability, salary,
              summary, experience, education, skills, references_text AS "referencesText", photo_data AS "photoData",
              attachment_name AS "attachmentName", attachment_data AS "attachmentData", created_at AS "createdAt", updated_at AS "updatedAt"
       FROM link_resumes WHERE id = $1 AND is_public = true`,
      [id],
    );
    return result.rows[0] || null;
  }
  const data = await readJsonData();
  return data.resumes.find((item) => item.id === id) || null;
}

function resumePrintHtml(resume) {
  const lines = (value) => htmlEscape(value).split(/\n+/).filter(Boolean).map((line) => `<p>${line}</p>`).join("");
  return `<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Hoja de vida - ${htmlEscape(resume.fullName)}</title>
  <style>
    body{margin:0;background:#f2f2f2;color:#161616;font-family:Arial,sans-serif}
    main{max-width:850px;margin:24px auto;padding:34px;background:white}
    header{display:grid;grid-template-columns:120px 1fr;gap:24px;align-items:center;border-bottom:3px solid #caa64c;padding-bottom:20px}
    .photo{width:120px;height:140px;object-fit:cover;background:#111;border:1px solid #ddd}
    h1{margin:0;font-size:30px} h2{margin:24px 0 8px;font-size:16px;color:#7a5c15;text-transform:uppercase}
    .headline{margin:8px 0 0;font-weight:bold}.meta{display:flex;flex-wrap:wrap;gap:10px;margin-top:12px;color:#444;font-size:13px}
    p{margin:5px 0;line-height:1.42}.actions{margin:18px 0}.actions button{padding:10px 14px;border:0;background:#caa64c;font-weight:bold}
    @media print{body{background:white}main{margin:0;max-width:none}.actions{display:none}}
  </style>
</head>
<body>
  <main>
    <div class="actions"><button onclick="window.print()">Imprimir / guardar PDF</button></div>
    <header>
      ${resume.photoData ? `<img class="photo" src="${resume.photoData}" alt="Foto">` : `<div class="photo"></div>`}
      <div>
        <h1>${htmlEscape(resume.fullName)}</h1>
        <p class="headline">${htmlEscape(resume.headline)}</p>
        <div class="meta">
          ${resume.city ? `<span>${htmlEscape(resume.city)}</span>` : ""}
          ${resume.phone ? `<span>${htmlEscape(resume.phone)}</span>` : ""}
          ${resume.email ? `<span>${htmlEscape(resume.email)}</span>` : ""}
          ${resume.availability ? `<span>${htmlEscape(resume.availability)}</span>` : ""}
        </div>
      </div>
    </header>
    ${resume.summary ? `<h2>Perfil</h2>${lines(resume.summary)}` : ""}
    ${resume.experience ? `<h2>Experiencia</h2>${lines(resume.experience)}` : ""}
    ${resume.education ? `<h2>Formacion</h2>${lines(resume.education)}` : ""}
    ${resume.skills ? `<h2>Competencias</h2>${lines(resume.skills)}` : ""}
    ${resume.referencesText ? `<h2>Referencias</h2>${lines(resume.referencesText)}` : ""}
    ${resume.attachmentData ? `<h2>Anexo</h2><p>Archivo adjunto: ${htmlEscape(resume.attachmentName || "Hoja de vida")}</p>` : ""}
  </main>
</body>
</html>`;
}

async function readBody(req) {
  const chunks = [];
  let size = 0;
  for await (const chunk of req) {
    size += chunk.length;
    if (size > bodyLimitBytes) {
      const error = new Error("El archivo o formulario es muy grande");
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
    const authUser = await getAuthUser(req);
    const data = await readData(authUser);
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

  if (req.method === "GET" && url.pathname === "/api/me") {
    json(res, 200, { user: await getAuthUser(req), storage: storageInfo() });
    return;
  }

  if (req.method === "POST" && url.pathname === "/api/auth/register") {
    json(res, 201, await registerUser(await readBody(req)));
    return;
  }

  if (req.method === "POST" && url.pathname === "/api/auth/login") {
    json(res, 200, await loginUser(await readBody(req)));
    return;
  }

  if (req.method === "POST" && url.pathname === "/api/auth/logout") {
    await logoutUser(req);
    json(res, 200, { ok: true });
    return;
  }

  if (req.method === "POST" && url.pathname === "/api/news") {
    json(res, 201, await saveNews(await readBody(req), await requireUser(req)));
    return;
  }

  if (req.method === "POST" && url.pathname === "/api/resumes") {
    json(res, 201, await saveResume(await readBody(req), await requireUser(req)));
    return;
  }

  if (req.method === "POST" && url.pathname === "/api/vacancies") {
    json(res, 201, await saveVacancy(await readBody(req), await requireUser(req)));
    return;
  }

  if (req.method === "POST" && url.pathname === "/api/jobs") {
    const user = await requireUser(req);
    if (user.accountType === "company") {
      json(res, 201, await saveVacancy(await readBody(req), user));
      return;
    }
    json(res, 201, await saveResume(await readBody(req), user));
    return;
  }

  if (req.method === "POST" && url.pathname === "/api/products") {
    json(res, 201, await saveProduct(await readBody(req), await requireUser(req)));
    return;
  }

  if (req.method === "POST" && url.pathname === "/api/threads") {
    json(res, 201, await saveThread(await readBody(req), await requireUser(req)));
    return;
  }

  const messageMatch = url.pathname.match(/^\/api\/threads\/([^/]+)\/messages$/);
  if (req.method === "POST" && messageMatch) {
    json(res, 201, await saveMessage(messageMatch[1], await readBody(req), await requireUser(req)));
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
      json(res, 200, { ok: true, storage: storageInfo() });
      return;
    }
    const cvMatch = url.pathname.match(/^\/cv\/([0-9a-f-]+)$/i);
    if (req.method === "GET" && cvMatch) {
      const resume = await getResume(cvMatch[1]);
      if (!resume) return notFound(res);
      res.writeHead(200, { "content-type": "text/html; charset=utf-8", "cache-control": "no-store" });
      res.end(resumePrintHtml(resume));
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

await initializeStorage();

server.listen(port, () => {
  console.log(`Link app listening on ${port}`);
});
