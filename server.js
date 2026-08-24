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
const officialNewsTtlMs = Number(process.env.OFFICIAL_NEWS_TTL_MS || 10 * 60 * 1000);
const officialNewsTimeoutMs = Number(process.env.OFFICIAL_NEWS_TIMEOUT_MS || 12_000);
const bodyLimitBytes = Number(process.env.BODY_LIMIT_BYTES || 30_000_000);
const sessionDays = Number(process.env.SESSION_DAYS || 30);
const defaultAdminEmails = "jhonsilvadiaz@gmail.com";
const configuredAdminEmails = [process.env.LINK_ADMIN_EMAILS, process.env.ADMIN_EMAILS, defaultAdminEmails].filter(Boolean).join(",");
const adminRecoveryCode = process.env.LINK_ADMIN_RECOVERY_CODE || process.env.ADMIN_RECOVERY_CODE || "";

const emptyData = {
  users: [],
  sessions: [],
  news: [],
  jobs: [],
  resumes: [],
  vacancies: [],
  products: [],
  threads: [],
  passwordRecoveryRequests: [],
  adRequests: [],
  adCampaigns: [],
  tokenTransactions: [],
  settings: {
    logoData: "",
    tokenCvDownloadCost: 10,
    tokenVacancyCost: 25,
    tokenTrialCompanyTokens: 30,
    tokenPackageValue: 50000,
    tokenPackageTokens: 500,
    paymentInfo: "Configure en Admin el Nequi o cuenta para recargar tokens.",
    updatedAt: null,
  },
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
  {
    entity: "GAT Events",
    home: "https://www.gatevents.net/",
    urls: [
      "https://www.gatevents.net/",
      "https://www.gatevents.net/gat-expo/",
      "https://www.gatevents.net/gat-expo-cartagena/",
      "https://www.gatevents.net/gat-bogota/",
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
  "slots",
  "ruletas",
  "maquinitas",
  "mesas de casino",
  "mesas de juegos",
  "salas de juego",
  "operadores de juegos localizados",
  "establecimientos de juegos",
  "juegos de azar",
  "apuestas",
  "gaming",
  "igaming",
  "gat expo",
  "fadja",
  "feria",
];

const officialHostSuffixes = [
  "coljuegos.gov.co",
  "uiaf.gov.co",
  "dian.gov.co",
  "normograma.dian.gov.co",
  "supersalud.gov.co",
  "normograma.supersalud.gov.co",
  "docs.supersalud.gov.co",
  "gatevents.net",
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

function imageDataText(value, max) {
  const output = dataText(value, max);
  if (!output) return "";
  if (!/^data:image\/(?:png|jpe?g|webp);base64,/i.test(output)) fail(400, "El logo debe ser una imagen PNG, JPG o WebP");
  return output;
}

function mediaDataText(value, max) {
  const output = dataText(value, max);
  if (!output) return "";
  if (!/^data:(?:image\/(?:png|jpe?g|webp|gif|svg\+xml)|video\/(?:mp4|webm|quicktime));base64,/i.test(output)) {
    fail(400, "Solo se permiten fotos o videos compatibles");
  }
  return output;
}

function adBannerMediaDataText(value, max) {
  const output = mediaDataText(value, max);
  if (!output) return "";
  if (/^data:video\//i.test(output) && !/^data:video\/mp4;base64,/i.test(output)) {
    fail(400, "El video de pauta debe ser MP4 horizontal 16:9");
  }
  return output;
}

function mediaDataPayload(value) {
  const match = String(value ?? "").trim().match(/^data:([^;,]+);base64,([\s\S]+)$/i);
  if (!match) return null;
  try {
    return {
      type: match[1].toLowerCase(),
      buffer: Buffer.from(match[2], "base64"),
    };
  } catch {
    return null;
  }
}

function adMediaUrl(item) {
  if (!(item?.hasMedia || item?.mediaData || item?.media_data)) return "";
  const id = text(item.id, 80);
  if (!id) return "";
  const version = item.updatedAt || item.updated_at || item.createdAt || item.created_at || id;
  return `/api/ad-campaigns/${encodeURIComponent(id)}/media?v=${encodeURIComponent(String(version))}`;
}

function publicAdCampaign(item) {
  if (!item) return null;
  return {
    id: text(item.id, 80),
    title: text(item.title, 180),
    advertiser: text(item.advertiser, 140),
    body: text(item.body, 300),
    targetUrl: text(item.targetUrl || item.target_url, 300),
    mediaType: text(item.mediaType || item.media_type, 80),
    mediaName: text(item.mediaName || item.media_name, 160),
    mediaUrl: adMediaUrl(item),
    startsAt: item.startsAt || item.starts_at || null,
    endsAt: item.endsAt || item.ends_at || null,
    status: normalizeStatus(item.status),
    createdAt: item.createdAt || item.created_at || null,
    updatedAt: item.updatedAt || item.updated_at || null,
  };
}

function productMediaUrl(item) {
  if (!(item?.hasMedia || item?.mediaData || item?.media_data)) return "";
  const id = text(item.id, 80);
  if (!id) return "";
  const version = item.updatedAt || item.updated_at || item.createdAt || item.created_at || id;
  return `/api/products/${encodeURIComponent(id)}/media?v=${encodeURIComponent(String(version))}`;
}

function publicProduct(item) {
  if (!item) return null;
  return {
    id: text(item.id, 80),
    productType: text(item.productType || item.product_type, 80) || "Otro",
    name: text(item.name, 120),
    price: text(item.price, 80),
    condition: text(item.condition, 80),
    brand: text(item.brand, 120),
    billAcceptorType: text(item.billAcceptorType || item.bill_acceptor_type, 120),
    game: text(item.game, 120),
    description: text(item.description, 500),
    contact: text(item.contact, 160),
    mediaType: text(item.mediaType || item.media_type, 80),
    mediaName: text(item.mediaName || item.media_name, 160),
    mediaUrl: productMediaUrl(item),
    status: normalizeStatus(item.status),
    createdAt: item.createdAt || item.created_at || null,
  };
}

function logoDataText(value, max) {
  const output = dataText(value, max);
  if (!output) return "";
  if (!/^data:(?:image\/[a-z0-9.+-]+|video\/[a-z0-9.+-]+);base64,/i.test(output)) {
    fail(400, "El logo debe ser imagen, GIF, SVG o video compatible");
  }
  return output;
}

function intSetting(value, fallback, min = 0, max = 1_000_000) {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.min(max, Math.max(min, parsed));
}

function normalizeSettings(settings = {}) {
  const logoData = String(settings.logoData || "").trim();
  return {
    logoData: /^data:(?:image\/[a-z0-9.+-]+|video\/[a-z0-9.+-]+);base64,/i.test(logoData) && logoData.length <= 16_000_000 ? logoData : "",
    logoType: text(settings.logoType || settings.logo_type, 120),
    logoName: text(settings.logoName || settings.logo_name, 160),
    tokenCvDownloadCost: intSetting(settings.tokenCvDownloadCost ?? settings.token_cv_download_cost, 10, 0, 10000),
    tokenVacancyCost: intSetting(settings.tokenVacancyCost ?? settings.token_vacancy_cost, 25, 0, 10000),
    tokenTrialCompanyTokens: intSetting(settings.tokenTrialCompanyTokens ?? settings.token_trial_company_tokens, 30, 0, 100000),
    tokenPackageValue: intSetting(settings.tokenPackageValue ?? settings.token_package_value, 50000, 0, 100000000),
    tokenPackageTokens: intSetting(settings.tokenPackageTokens ?? settings.token_package_tokens, 500, 0, 1000000),
    paymentInfo: text(settings.paymentInfo || settings.payment_info, 300) || "Configure en Admin el Nequi o cuenta para recargar tokens.",
    updatedAt: settings.updatedAt || settings.updated_at || null,
  };
}

function settingsLogoUrl(settings) {
  const normalized = normalizeSettings(settings);
  if (!normalized.logoData) return "";
  const version = normalized.updatedAt || normalized.logoName || "custom";
  return `/api/settings/logo?v=${encodeURIComponent(String(version))}`;
}

function publicSettings(settings = {}) {
  const normalized = normalizeSettings(settings);
  return {
    ...normalized,
    logoData: "",
    logoUrl: settingsLogoUrl(normalized),
  };
}

function nowStamp() {
  return new Date().toISOString();
}

function normalizeEmail(value) {
  return text(value, 180).toLowerCase().replace(/\s+/g, "").slice(0, 160);
}

const adminEmails = new Set(
  configuredAdminEmails
    .split(/[,\s;]+/)
    .map((email) => normalizeEmail(email))
    .filter(Boolean),
);

function normalizeAccountType(value) {
  return value === "company" ? "company" : "person";
}

function isAdminEmail(value) {
  return adminEmails.has(normalizeEmail(value));
}

function isAdminUser(user) {
  return Boolean(user?.isAdmin || user?.is_admin || isAdminEmail(user?.email));
}

async function canRecoverAdminEmail(email) {
  if (isAdminEmail(email)) return true;
  if (dbReady) {
    const result = await pool.query("SELECT is_admin FROM link_users WHERE lower(email) = $1 LIMIT 1", [email]);
    return Boolean(result.rows[0]?.is_admin);
  }
  const data = await readJsonData();
  const found = data.users.find((item) => normalizeEmail(item.email) === email);
  return Boolean(found && isAdminUser(found));
}

function normalizeStatus(value) {
  return ["pending", "published", "hidden"].includes(value) ? value : "published";
}

function normalizeUserStatus(value) {
  return ["active", "inactive"].includes(value) ? value : "active";
}

function availabilityDisplay(value) {
  const clean = text(value, 120);
  if (!clean) return "";
  return /^disponibilidad\s*:/i.test(normalizeText(clean)) ? clean : `Disponibilidad: ${clean}`;
}

function newContentStatus(user) {
  return isAdminUser(user) ? "published" : "pending";
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

function officialItemYear(item) {
  const years = `${item.title || ""} ${item.summary || ""} ${item.url || ""} ${item.sourceUrl || ""}`.match(/\b20\d{2}\b/g) || [];
  return years.length ? Math.max(...years.map((year) => Number(year))) : new Date().getFullYear();
}

function isRecentOfficialItem(item) {
  return officialItemYear(item) >= new Date().getFullYear() - 1;
}

function cleanSummary(value) {
  return String(value || "")
    .replace(/contraste aumentar tamano letra disminuir tamano letra/gi, " ")
    .replace(/breadcrumb\s+home\s+(?:&raquo;|Â»)?/gi, " ")
    .replace(/"library"\s*:\s*"[^"]*"\s*\}?\s*,?\s*"toggle"\s*:\s*"[^"]*"\s*\}?/gi, " ")
    .replace(/"toggle"\s*:\s*"[^"]*"\s*\}?/gi, " ")
    .replace(/\bHome\s+Eventos\b/gi, "Eventos")
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

function eventNameFromPath(url) {
  try {
    const slug = new URL(url).pathname
      .split("/")
      .filter(Boolean)
      .pop() || "";
    const label = slug
      .replace(/-/g, " ")
      .replace(/\bgat\b/i, "GAT")
      .replace(/\bexpo\b/i, "Expo")
      .replace(/\bbogota\b/i, "Bogota")
      .replace(/\bcartagena\b/i, "Cartagena")
      .replace(/\bcolombia\b/i, "Colombia")
      .trim();
    return /^GAT\s+(Expo|Bogota|Cartagena)/i.test(label) ? label : "";
  } catch {
    return "";
  }
}

function officialSummary(candidate) {
  if (candidate.entity === "GAT Events") {
    const cleaned = cleanSummary(candidate.context);
    const eventName = eventNameFromPath(candidate.url)
      || cleaned.match(/\bGAT\s+Expo\s+[A-Za-zÁÉÍÓÚÜÑáéíóúüñ\s]+(?:COL|PR|RD|MX|BRA)?\s+20\d{2}\b/i)?.[0]
      || "Evento oficial GAT Events";
    const dateText = /\b\d{1,2}\s*,\s*\d{1,2}\s+de\s+[a-záéíóúñ]+\s+&\s+\d{1,2}\s+[A-Za-zÁÉÍÓÚÜÑáéíóúüñ]+\s+de\s+20\d{2}\b/i.test(candidate.title)
      ? ` Fechas: ${candidate.title}.`
      : "";
    return text(`${eventName}.${dateText} Organizador oficial: GAT Events.`, 240);
  }
  return text(summaryFromContext(candidate.title, candidate.context), 240);
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
        summary: officialSummary(candidate),
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

  const rankedItems = [...byKey.values()]
    .sort((a, b) => officialItemYear(b) - officialItemYear(a) || b.score - a.score);
  const recentItems = rankedItems.filter(isRecentOfficialItem);

  officialNewsCache.items = recentItems.slice(0, 18).map(({ score, ...item }) => item);
  officialNewsCache.updatedAt = nowStamp();
  officialNewsCache.error = officialNewsCache.items.length === 0
    ? "No se encontraron publicaciones recientes en las fuentes oficiales autorizadas."
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
    users: Array.isArray(parsed.users)
      ? parsed.users.map((item) => ({
        ...item,
        status: normalizeUserStatus(item.status),
        tokenBalance: intSetting(item.tokenBalance ?? item.token_balance, 0, 0, 1_000_000),
        lastSeenAt: item.lastSeenAt || item.createdAt || null,
        deactivatedAt: item.deactivatedAt || null,
      }))
      : [],
    sessions: Array.isArray(parsed.sessions) ? parsed.sessions : [],
    passwordRecoveryRequests: Array.isArray(parsed.passwordRecoveryRequests)
      ? parsed.passwordRecoveryRequests.map((item) => ({
        ...item,
        status: ["pending", "approved", "rejected"].includes(item.status) ? item.status : "pending",
      }))
      : [],
    news: Array.isArray(parsed.news) ? parsed.news.map((item) => ({ ...item, status: normalizeStatus(item.status) })) : [],
    jobs: Array.isArray(parsed.jobs) ? parsed.jobs : [],
    resumes: Array.isArray(parsed.resumes) ? parsed.resumes.map((item) => ({ ...item, status: normalizeStatus(item.status) })) : [],
    vacancies: Array.isArray(parsed.vacancies) ? parsed.vacancies.map((item) => ({ ...item, status: normalizeStatus(item.status) })) : [],
    products: Array.isArray(parsed.products) ? parsed.products.map((item) => ({ ...item, status: normalizeStatus(item.status) })) : [],
    threads: Array.isArray(parsed.threads) ? parsed.threads.map((item) => ({ ...item, status: normalizeStatus(item.status) })) : [],
    adRequests: Array.isArray(parsed.adRequests) ? parsed.adRequests.map((item) => ({ ...item, status: normalizeStatus(item.status) })) : [],
    adCampaigns: Array.isArray(parsed.adCampaigns) ? parsed.adCampaigns.map((item) => ({ ...item, status: normalizeStatus(item.status) })) : [],
    tokenTransactions: Array.isArray(parsed.tokenTransactions) ? parsed.tokenTransactions : [],
    settings: normalizeSettings(parsed.settings || emptyData.settings),
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

async function readSettings() {
  if (dbReady) {
    const result = await pool.query("SELECT value, updated_at FROM link_settings WHERE key = 'app'");
    const row = result.rows[0];
    return normalizeSettings(row ? { ...row.value, updatedAt: row.updated_at } : emptyData.settings);
  }
  const data = await readJsonData();
  return normalizeSettings(data.settings);
}

async function writeSettings(settings) {
  const next = normalizeSettings(settings);
  if (dbReady) {
    await pool.query(
      `INSERT INTO link_settings (key, value, updated_at)
       VALUES ('app', $1::jsonb, now())
       ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()`,
      [JSON.stringify(next)],
    );
    return readSettings();
  }
  const data = await readJsonData();
  data.settings = { ...next, updatedAt: nowStamp() };
  await writeJsonData(data);
  return data.settings;
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
      is_admin boolean NOT NULL DEFAULT false,
      token_balance integer NOT NULL DEFAULT 0,
      status text NOT NULL DEFAULT 'active',
      last_seen_at timestamptz,
      deactivated_at timestamptz,
      created_at timestamptz NOT NULL DEFAULT now(),
      updated_at timestamptz NOT NULL DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS link_settings (
      key text PRIMARY KEY,
      value jsonb NOT NULL DEFAULT '{}'::jsonb,
      updated_at timestamptz NOT NULL DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS link_password_recovery_requests (
      id uuid PRIMARY KEY,
      user_id uuid REFERENCES link_users(id) ON DELETE CASCADE,
      email text NOT NULL,
      password_hash text NOT NULL,
      status text NOT NULL DEFAULT 'pending',
      created_at timestamptz NOT NULL DEFAULT now(),
      resolved_at timestamptz
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
      status text NOT NULL DEFAULT 'published',
      created_at timestamptz NOT NULL DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS link_products (
      id uuid PRIMARY KEY,
      author_id uuid REFERENCES link_users(id) ON DELETE SET NULL,
      product_type text DEFAULT '',
      name text NOT NULL,
      price text NOT NULL,
      condition text NOT NULL DEFAULT 'Disponible',
      brand text DEFAULT '',
      bill_acceptor_type text DEFAULT '',
      game text DEFAULT '',
      description text DEFAULT '',
      contact text DEFAULT '',
      media_data text DEFAULT '',
      media_type text DEFAULT '',
      media_name text DEFAULT '',
      status text NOT NULL DEFAULT 'published',
      created_at timestamptz NOT NULL DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS link_threads (
      id uuid PRIMARY KEY,
      author_id uuid REFERENCES link_users(id) ON DELETE SET NULL,
      name text NOT NULL,
      topic text NOT NULL DEFAULT 'Conversacion',
      status text NOT NULL DEFAULT 'published',
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
      category text DEFAULT '',
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
      status text NOT NULL DEFAULT 'published',
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
      status text NOT NULL DEFAULT 'published',
      created_at timestamptz NOT NULL DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS link_ad_requests (
      id uuid PRIMARY KEY,
      requester_name text NOT NULL,
      company text DEFAULT '',
      phone text DEFAULT '',
      email text DEFAULT '',
      city text DEFAULT '',
      target_url text DEFAULT '',
      message text DEFAULT '',
      media_data text DEFAULT '',
      media_type text DEFAULT '',
      media_name text DEFAULT '',
      status text NOT NULL DEFAULT 'pending',
      created_at timestamptz NOT NULL DEFAULT now(),
      resolved_at timestamptz
    );

    CREATE TABLE IF NOT EXISTS link_ad_campaigns (
      id uuid PRIMARY KEY,
      title text NOT NULL,
      advertiser text DEFAULT '',
      body text DEFAULT '',
      target_url text DEFAULT '',
      media_data text DEFAULT '',
      media_type text DEFAULT '',
      media_name text DEFAULT '',
      starts_at timestamptz,
      ends_at timestamptz,
      status text NOT NULL DEFAULT 'published',
      created_at timestamptz NOT NULL DEFAULT now(),
      updated_at timestamptz NOT NULL DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS link_token_transactions (
      id uuid PRIMARY KEY,
      user_id uuid REFERENCES link_users(id) ON DELETE CASCADE,
      admin_id uuid REFERENCES link_users(id) ON DELETE SET NULL,
      kind text NOT NULL,
      amount integer NOT NULL,
      balance_after integer NOT NULL,
      reference_id text DEFAULT '',
      note text DEFAULT '',
      created_at timestamptz NOT NULL DEFAULT now()
    );

    ALTER TABLE link_users ADD COLUMN IF NOT EXISTS is_admin boolean NOT NULL DEFAULT false;
    ALTER TABLE link_users ADD COLUMN IF NOT EXISTS token_balance integer NOT NULL DEFAULT 0;
    ALTER TABLE link_users ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'active';
    ALTER TABLE link_users ADD COLUMN IF NOT EXISTS last_seen_at timestamptz;
    ALTER TABLE link_users ADD COLUMN IF NOT EXISTS deactivated_at timestamptz;
    ALTER TABLE link_news ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'published';
    ALTER TABLE link_products ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'published';
    ALTER TABLE link_products ADD COLUMN IF NOT EXISTS product_type text DEFAULT '';
    ALTER TABLE link_products ADD COLUMN IF NOT EXISTS brand text DEFAULT '';
    ALTER TABLE link_products ADD COLUMN IF NOT EXISTS bill_acceptor_type text DEFAULT '';
    ALTER TABLE link_products ADD COLUMN IF NOT EXISTS game text DEFAULT '';
    ALTER TABLE link_products ADD COLUMN IF NOT EXISTS media_data text DEFAULT '';
    ALTER TABLE link_products ADD COLUMN IF NOT EXISTS media_type text DEFAULT '';
    ALTER TABLE link_products ADD COLUMN IF NOT EXISTS media_name text DEFAULT '';
    ALTER TABLE link_threads ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'published';
    ALTER TABLE link_resumes ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'published';
    ALTER TABLE link_resumes ADD COLUMN IF NOT EXISTS category text DEFAULT '';
    ALTER TABLE link_vacancies ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'published';
    ALTER TABLE link_ad_requests ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'pending';
    ALTER TABLE link_ad_campaigns ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'published';
  `);

  if (adminEmails.size) {
    await pool.query(
      "UPDATE link_users SET is_admin = true WHERE lower(email) = ANY($1::text[])",
      [Array.from(adminEmails)],
    );
  }
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
  const createdAt = user.createdAt || user.created_at || null;
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
    isAdmin: isAdminUser(user),
    tokenBalance: intSetting(user.tokenBalance ?? user.token_balance, 0, 0, 1_000_000),
    status: normalizeUserStatus(user.status),
    createdAt,
    lastSeenAt: user.lastSeenAt || user.last_seen_at || createdAt,
    deactivatedAt: user.deactivatedAt || user.deactivated_at || null,
  };
}

function rowUser(row) {
  if (!row) return null;
  const createdAt = row.createdAt || row.created_at || null;
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
    isAdmin: isAdminUser(row),
    tokenBalance: intSetting(row.token_balance, 0, 0, 1_000_000),
    status: normalizeUserStatus(row.status),
    createdAt,
    lastSeenAt: row.lastSeenAt || row.last_seen_at || createdAt,
    deactivatedAt: row.deactivatedAt || row.deactivated_at || null,
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
      `SELECT u.id, u.email, u.account_type, u.display_name, u.phone, u.city, u.company_name, u.nit, u.role,
              u.is_admin, u.token_balance, u.status, u.last_seen_at, u.deactivated_at, u.created_at
       FROM link_sessions s
       JOIN link_users u ON u.id = s.user_id
       WHERE s.token_hash = $1 AND s.expires_at > now()`,
      [hashed],
    );
    const user = rowUser(result.rows[0]);
    if (!user || user.status === "inactive") return null;
    await pool.query("UPDATE link_users SET last_seen_at = now(), updated_at = now() WHERE id = $1", [user.id]);
    return { ...user, lastSeenAt: nowStamp() };
  }
  const data = await readJsonData();
  const session = data.sessions.find((item) => item.tokenHash === hashed && Date.parse(item.expiresAt) > Date.now());
  if (!session) return null;
  const found = data.users.find((item) => item.id === session.userId);
  const user = sanitizeUser(found);
  if (!user || user.status === "inactive") return null;
  found.lastSeenAt = nowStamp();
  await writeJsonData(data);
  return sanitizeUser(found);
}

async function requireUser(req) {
  const user = await getAuthUser(req);
  if (!user) fail(401, "Debes iniciar sesion");
  return user;
}

async function requireAdmin(req) {
  const user = await requireUser(req);
  if (!isAdminUser(user)) fail(403, "Solo administrador");
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
  const settings = await readSettings();
  const initialTokens = accountType === "company" ? settings.tokenTrialCompanyTokens : 0;

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
    isAdmin: isAdminEmail(email),
    tokenBalance: initialTokens,
    status: "active",
    createdAt: nowStamp(),
    lastSeenAt: nowStamp(),
    deactivatedAt: null,
  };

  if (dbReady) {
    try {
      await pool.query(
        `INSERT INTO link_users
         (id, email, password_hash, account_type, display_name, phone, city, company_name, nit, role, is_admin, token_balance, status, last_seen_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, now())`,
        [user.id, user.email, user.passwordHash, user.accountType, user.displayName, user.phone, user.city, user.companyName, user.nit, user.role, user.isAdmin, user.tokenBalance, user.status],
      );
      if (initialTokens > 0) {
        await pool.query(
          `INSERT INTO link_token_transactions (id, user_id, kind, amount, balance_after, note)
           VALUES ($1, $2, 'trial', $3, $3, 'Tokens iniciales de empresa')`,
          [randomUUID(), user.id, initialTokens],
        );
      }
    } catch (error) {
      if (error.code === "23505") fail(409, "Ese correo ya esta registrado");
      throw error;
    }
  } else {
    const data = await readJsonData();
    if (data.users.some((item) => normalizeEmail(item.email) === email)) fail(409, "Ese correo ya esta registrado");
    data.users.push(user);
    if (initialTokens > 0) {
      data.tokenTransactions.unshift({
        id: randomUUID(),
        userId: user.id,
        kind: "trial",
        amount: initialTokens,
        balanceAfter: initialTokens,
        note: "Tokens iniciales de empresa",
        createdAt: nowStamp(),
      });
    }
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
      `SELECT id, email, password_hash, account_type, display_name, phone, city, company_name, nit, role, is_admin, token_balance,
              status, last_seen_at, deactivated_at, created_at
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
  if (!user || !await verifyPassword(password, passwordHash)) {
    fail(401, "Correo o clave incorrectos. Si es administrador, use Recuperar clave con Codigo admin.");
  }
  if (user.status === "inactive") fail(403, "Usuario inactivo. Contacte al administrador.");
  if (dbReady) {
    await pool.query("UPDATE link_users SET last_seen_at = now(), updated_at = now() WHERE id = $1", [user.id]);
    user = { ...user, lastSeenAt: nowStamp() };
  } else {
    const data = await readJsonData();
    const found = data.users.find((item) => item.id === user.id);
    if (found) {
      found.lastSeenAt = nowStamp();
      await writeJsonData(data);
      user = sanitizeUser(found);
    }
  }
  const token = await createSession(user.id);
  return { token, user };
}

async function changePassword(body, user) {
  const currentPassword = String(body.currentPassword || "");
  const newPassword = String(body.newPassword || body.password || "");
  if (newPassword.length < 6) fail(400, "La nueva clave debe tener minimo 6 caracteres");
  if (currentPassword === newPassword) fail(400, "La nueva clave debe ser diferente a la actual");

  let passwordHash;
  if (dbReady) {
    const result = await pool.query("SELECT password_hash FROM link_users WHERE id = $1 AND status = 'active'", [user.id]);
    passwordHash = result.rows[0]?.password_hash || "";
  } else {
    const data = await readJsonData();
    const found = data.users.find((item) => item.id === user.id && normalizeUserStatus(item.status) === "active");
    passwordHash = found?.passwordHash || "";
  }

  if (!passwordHash || !await verifyPassword(currentPassword, passwordHash)) {
    fail(401, "La clave actual no coincide");
  }

  const nextHash = await hashPassword(newPassword);
  if (dbReady) {
    await pool.query("UPDATE link_users SET password_hash = $1, updated_at = now() WHERE id = $2", [nextHash, user.id]);
  } else {
    const data = await readJsonData();
    const found = data.users.find((item) => item.id === user.id);
    if (!found) fail(404, "Usuario no encontrado");
    found.passwordHash = nextHash;
    await writeJsonData(data);
  }
  return { ok: true, message: "Clave actualizada correctamente. Use la nueva clave en el proximo ingreso." };
}

async function requestPasswordRecovery(body) {
  const email = normalizeEmail(body.email);
  const password = String(body.password || "");
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) fail(400, "Correo invalido");
  if (password.length < 6) fail(400, "La nueva clave debe tener minimo 6 caracteres");
  if (isAdminEmail(email)) {
    fail(400, "Para cambiar la clave del administrador escriba el Codigo admin. Sin ese codigo no se cambia la clave.");
  }
  const passwordHash = await hashPassword(password);
  const request = {
    id: randomUUID(),
    email,
    passwordHash,
    status: "pending",
    createdAt: nowStamp(),
    resolvedAt: null,
  };

  if (dbReady) {
    const found = await pool.query("SELECT id FROM link_users WHERE email = $1 AND status = 'active'", [email]);
    const userId = found.rows[0]?.id;
    if (userId) {
      await pool.query(
        `INSERT INTO link_password_recovery_requests (id, user_id, email, password_hash, status)
         VALUES ($1, $2, $3, $4, 'pending')`,
        [request.id, userId, email, passwordHash],
      );
    }
    return { ok: true, message: "Solicitud enviada. El administrador debe aprobar el cambio de clave." };
  }

  const data = await readJsonData();
  const found = data.users.find((item) => normalizeEmail(item.email) === email && normalizeUserStatus(item.status) === "active");
  if (found) {
    data.passwordRecoveryRequests.unshift({
      ...request,
      userId: found.id,
      displayName: found.displayName || found.email,
    });
    await writeJsonData(data);
  }
  return { ok: true, message: "Solicitud enviada. El administrador debe aprobar el cambio de clave." };
}

async function recoverAdminPassword(body) {
  const email = normalizeEmail(body.email);
  const password = String(body.password || "");
  const recoveryCode = text(body.recoveryCode, 160);
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) fail(400, "Correo invalido");
  if (password.length < 6) fail(400, "La nueva clave debe tener minimo 6 caracteres");
  if (!await canRecoverAdminEmail(email)) fail(403, "Correo no autorizado como administrador");
  if (!adminRecoveryCode) fail(400, "Codigo admin no configurado");
  if (recoveryCode !== adminRecoveryCode) fail(403, "Codigo admin incorrecto");

  const passwordHash = await hashPassword(password);
  const displayName = email.split("@")[0] || "Admin Link";

  if (dbReady) {
    const result = await pool.query(
      `INSERT INTO link_users
       (id, email, password_hash, account_type, display_name, phone, city, company_name, nit, role, is_admin, status, last_seen_at)
       VALUES ($1, $2, $3, 'person', $4, '', '', '', '', 'Administrador', true, 'active', now())
       ON CONFLICT (email) DO UPDATE SET
         password_hash = EXCLUDED.password_hash,
         is_admin = true,
         status = 'active',
         deactivated_at = NULL,
         last_seen_at = now(),
         updated_at = now()
       RETURNING id, email, account_type, display_name, phone, city, company_name, nit, role,
                 is_admin, token_balance, status, last_seen_at, deactivated_at, created_at`,
      [randomUUID(), email, passwordHash, displayName],
    );
    const user = rowUser(result.rows[0]);
    const token = await createSession(user.id);
    return { token, user, message: "Clave admin actualizada. Ya puedes ingresar." };
  }

  const data = await readJsonData();
  let user = data.users.find((item) => normalizeEmail(item.email) === email);
  if (user) {
    user.passwordHash = passwordHash;
    user.isAdmin = true;
    user.status = "active";
    user.deactivatedAt = null;
    user.lastSeenAt = nowStamp();
  } else {
    user = {
      id: randomUUID(),
      email,
      passwordHash,
      accountType: "person",
      displayName,
      phone: "",
      city: "",
      companyName: "",
      nit: "",
      role: "Administrador",
      isAdmin: true,
      status: "active",
      createdAt: nowStamp(),
      lastSeenAt: nowStamp(),
      deactivatedAt: null,
    };
    data.users.push(user);
  }
  await writeJsonData(data);
  const token = await createSession(user.id);
  return { token, user: sanitizeUser(user), message: "Clave admin actualizada. Ya puedes ingresar." };
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
    const authUserId = authUser?.id || null;
    const [
      news,
      products,
      threads,
      messages,
      resumes,
      vacancies,
      settings,
      activeAd,
    ] = await Promise.all([
      pool.query(
        `SELECT id, title, body, category, contact, status, created_at AS "createdAt"
         FROM link_news
         WHERE status = 'published' OR ($1::uuid IS NOT NULL AND author_id = $1::uuid)
         ORDER BY created_at DESC LIMIT 100`,
        [authUserId],
      ),
      pool.query(
        `SELECT id, product_type AS "productType", name, price, condition, brand, bill_acceptor_type AS "billAcceptorType",
                game, description, contact, COALESCE(media_data, '') <> '' AS "hasMedia", media_type AS "mediaType", media_name AS "mediaName",
                status, created_at AS "createdAt"
         FROM link_products
         WHERE status = 'published' OR ($1::uuid IS NOT NULL AND author_id = $1::uuid)
         ORDER BY created_at DESC LIMIT 100`,
        [authUserId],
      ),
      pool.query(
        `SELECT id, name, topic, status, created_at AS "createdAt"
         FROM link_threads
         WHERE status = 'published' OR ($1::uuid IS NOT NULL AND author_id = $1::uuid)
         ORDER BY created_at DESC LIMIT 50`,
        [authUserId],
      ),
      pool.query(`SELECT id, thread_id AS "threadId", author, text, created_at AS "createdAt" FROM link_messages ORDER BY created_at ASC LIMIT 500`),
      pool.query(
        `SELECT id, user_id AS "userId", full_name AS "fullName", headline, category, document_id AS "documentId", city, phone, email, availability, salary,
                summary, experience, education, skills, references_text AS "referencesText", photo_data AS "photoData",
                attachment_name AS "attachmentName", status, created_at AS "createdAt", updated_at AS "updatedAt"
         FROM link_resumes
         WHERE (is_public = true AND status = 'published') OR ($1::uuid IS NOT NULL AND user_id = $1::uuid)
         ORDER BY updated_at DESC LIMIT 100`,
        [authUserId],
      ),
      pool.query(
        `SELECT id, company, title, city, salary, contact, description, requirements, status, created_at AS "createdAt"
         FROM link_vacancies
         WHERE status = 'published' OR ($1::uuid IS NOT NULL AND user_id = $1::uuid)
         ORDER BY created_at DESC LIMIT 100`,
        [authUserId],
      ),
      readSettings(),
      readActiveAdCampaign(),
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
      resumes: resumes.rows.map((item) => ({ ...item, availability: availabilityDisplay(item.availability) })),
      vacancies: vacancies.rows,
      products: products.rows.map(publicProduct),
      threads: threads.rows.map((thread) => ({ ...thread, messages: messagesByThread.get(thread.id) || [] })),
      currentUser: authUser,
      storage: storageInfo(),
      settings: publicSettings(settings),
      activeAd,
    };
  }

  const data = await readJsonData();
  const visibleAuthor = (item) => normalizeStatus(item.status) === "published" || (authUser?.id && item.authorId === authUser.id);
  const visibleOwner = (item) => normalizeStatus(item.status) === "published" || (authUser?.id && item.userId === authUser.id);
  return {
    news: data.news.filter(visibleAuthor),
    jobs: data.jobs,
    resumes: data.resumes.filter(visibleOwner).map((item) => ({ ...item, availability: availabilityDisplay(item.availability) })),
    vacancies: data.vacancies.filter(visibleOwner),
    products: data.products.filter(visibleAuthor).map(publicProduct),
    threads: data.threads.filter(visibleAuthor),
    currentUser: authUser,
    storage: storageInfo(),
    settings: publicSettings(data.settings),
    activeAd: await readActiveAdCampaign(),
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
    status: newContentStatus(user),
    createdAt: nowStamp(),
  };
  if (dbReady) {
    await pool.query(
      `INSERT INTO link_news (id, author_id, title, body, category, contact, status) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
      [item.id, user.id, item.title, item.body, item.category, item.contact, item.status],
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
    productType: text(body.productType, 80) || "Otro",
    name,
    price,
    condition: text(body.condition, 80) || "Disponible",
    brand: text(body.brand, 120),
    billAcceptorType: text(body.billAcceptorType, 120),
    game: text(body.game, 120),
    description: text(body.description, 500),
    contact: text(body.contact, 160) || user.phone || user.email,
    mediaData: mediaDataText(body.mediaData, 24_000_000),
    mediaType: text(body.mediaType, 80),
    mediaName: text(body.mediaName, 160),
    status: newContentStatus(user),
    createdAt: nowStamp(),
  };
  if (dbReady) {
    await pool.query(
      `INSERT INTO link_products
         (id, author_id, product_type, name, price, condition, brand, bill_acceptor_type, game, description, contact, media_data, media_type, media_name, status)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)`,
      [
        item.id,
        user.id,
        item.productType,
        item.name,
        item.price,
        item.condition,
        item.brand,
        item.billAcceptorType,
        item.game,
        item.description,
        item.contact,
        item.mediaData,
        item.mediaType,
        item.mediaName,
        item.status,
      ],
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
    status: newContentStatus(user),
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
    await pool.query(`INSERT INTO link_threads (id, author_id, name, topic, status) VALUES ($1, $2, $3, $4, $5)`, [item.id, user.id, item.name, item.topic, item.status]);
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
    category: text(body.category, 80),
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
    photoData: dataText(body.photoData, 4_000_000),
    attachmentName: text(body.attachmentName, 160),
    attachmentData: dataText(body.attachmentData, 14_000_000),
    status: newContentStatus(user),
    createdAt: nowStamp(),
    updatedAt: nowStamp(),
  };

  if (dbReady) {
    await pool.query("DELETE FROM link_resumes WHERE user_id = $1", [user.id]);
    await pool.query(
      `INSERT INTO link_resumes
       (id, user_id, full_name, headline, category, document_id, city, phone, email, availability, salary, summary, experience,
        education, skills, references_text, photo_data, attachment_name, attachment_data, status)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)`,
      [
        item.id, item.userId, item.fullName, item.headline, item.category, item.documentId, item.city, item.phone, item.email,
        item.availability, item.salary, item.summary, item.experience, item.education, item.skills,
        item.referencesText, item.photoData, item.attachmentName, item.attachmentData, item.status,
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

async function chargeTokensWithClient(client, user, amount, kind, referenceId, note) {
  if (amount <= 0 || isAdminUser(user)) return { charged: false, balance: user.tokenBalance || 0 };
  if (user.accountType !== "company") fail(403, "Esta accion es exclusiva para empresas con saldo de tokens");
  const current = await client.query("SELECT token_balance FROM link_users WHERE id = $1 FOR UPDATE", [user.id]);
  const balance = Number(current.rows[0]?.token_balance || 0);
  if (balance < amount) {
    fail(402, `Saldo insuficiente. Necesitas ${amount} tokens y tienes ${balance}. Solicita recarga por Nequi o cuenta.`);
  }
  const nextBalance = balance - amount;
  await client.query("UPDATE link_users SET token_balance = $1, updated_at = now() WHERE id = $2", [nextBalance, user.id]);
  await client.query(
    `INSERT INTO link_token_transactions (id, user_id, kind, amount, balance_after, reference_id, note)
     VALUES ($1, $2, $3, $4, $5, $6, $7)`,
    [randomUUID(), user.id, kind, -amount, nextBalance, referenceId || "", note || ""],
  );
  user.tokenBalance = nextBalance;
  return { charged: true, balance: nextBalance };
}

function chargeTokensInData(data, user, amount, kind, referenceId, note) {
  if (amount <= 0 || isAdminUser(user)) return { charged: false, balance: user.tokenBalance || 0 };
  if (user.accountType !== "company") fail(403, "Esta accion es exclusiva para empresas con saldo de tokens");
  const found = data.users.find((item) => item.id === user.id);
  if (!found) fail(404, "Empresa no encontrada");
  const balance = intSetting(found.tokenBalance, 0, 0, 1_000_000);
  if (balance < amount) {
    fail(402, `Saldo insuficiente. Necesitas ${amount} tokens y tienes ${balance}. Solicita recarga por Nequi o cuenta.`);
  }
  const nextBalance = balance - amount;
  found.tokenBalance = nextBalance;
  data.tokenTransactions.unshift({
    id: randomUUID(),
    userId: user.id,
    kind,
    amount: -amount,
    balanceAfter: nextBalance,
    referenceId: referenceId || "",
    note: note || "",
    createdAt: nowStamp(),
  });
  user.tokenBalance = nextBalance;
  return { charged: true, balance: nextBalance };
}

async function saveVacancy(body, user) {
  if (user.accountType !== "company") fail(403, "Solo empresa puede publicar vacantes");
  const company = text(body.company, 140) || user.companyName || user.displayName;
  const title = text(body.title, 140);
  if (!company || !title) fail(400, "Empresa y cargo son requeridos");
  const settings = await readSettings();
  const cost = settings.tokenVacancyCost;
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
    status: newContentStatus(user),
    createdAt: nowStamp(),
  };
  if (dbReady) {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      await chargeTokensWithClient(client, user, cost, "vacancy", item.id, `Publicacion de vacante: ${item.title}`);
      await client.query(
        `INSERT INTO link_vacancies (id, user_id, company, title, city, salary, contact, description, requirements, status)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
        [item.id, item.userId, item.company, item.title, item.city, item.salary, item.contact, item.description, item.requirements, item.status],
      );
      await client.query("COMMIT");
    } catch (error) {
      await client.query("ROLLBACK").catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  } else {
    const data = await readJsonData();
    chargeTokensInData(data, user, cost, "vacancy", item.id, `Publicacion de vacante: ${item.title}`);
    data.vacancies.unshift(item);
    await writeJsonData(data);
  }
  return { ...item, tokenBalance: user.tokenBalance };
}

async function getResume(id) {
  if (dbReady) {
    const result = await pool.query(
      `SELECT id, user_id AS "userId", full_name AS "fullName", headline, document_id AS "documentId", city, phone, email, availability, salary,
              summary, experience, education, skills, references_text AS "referencesText", photo_data AS "photoData",
              attachment_name AS "attachmentName", attachment_data AS "attachmentData", created_at AS "createdAt", updated_at AS "updatedAt"
       FROM link_resumes WHERE id = $1 AND is_public = true AND status = 'published'`,
      [id],
    );
    return result.rows[0] || null;
  }
  const data = await readJsonData();
  return data.resumes.find((item) => item.id === id && normalizeStatus(item.status) === "published") || null;
}

function resumeDownloadRequiresCharge(resume, user) {
  if (isAdminUser(user)) return false;
  if (user.accountType === "person" && resume.userId === user.id) return false;
  if (user.accountType !== "company") fail(403, "Solo empresas registradas pueden descargar hojas de vida de candidatos");
  return true;
}

async function downloadResume(id, user) {
  const settings = await readSettings();
  const cost = settings.tokenCvDownloadCost;
  let resume;
  if (dbReady) {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      const result = await client.query(
        `SELECT id, user_id AS "userId", full_name AS "fullName", headline, document_id AS "documentId", city, phone, email, availability, salary,
                summary, experience, education, skills, references_text AS "referencesText", photo_data AS "photoData",
                attachment_name AS "attachmentName", attachment_data AS "attachmentData", created_at AS "createdAt", updated_at AS "updatedAt"
         FROM link_resumes WHERE id = $1 AND is_public = true AND status = 'published'`,
        [id],
      );
      resume = result.rows[0];
      if (!resume) fail(404, "Hoja de vida no encontrada");
      if (resumeDownloadRequiresCharge(resume, user)) {
        await chargeTokensWithClient(client, user, cost, "cv_download", id, `Descarga HV: ${resume.fullName}`);
      }
      await client.query("COMMIT");
    } catch (error) {
      await client.query("ROLLBACK").catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  } else {
    const data = await readJsonData();
    resume = data.resumes.find((item) => item.id === id && normalizeStatus(item.status) === "published");
    if (!resume) fail(404, "Hoja de vida no encontrada");
    if (resumeDownloadRequiresCharge(resume, user)) {
      chargeTokensInData(data, user, cost, "cv_download", id, `Descarga HV: ${resume.fullName}`);
      await writeJsonData(data);
    }
  }
  return { html: resumePrintHtml(resume), tokenBalance: user.tokenBalance };
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
          ${resume.availability ? `<span>${htmlEscape(availabilityDisplay(resume.availability))}</span>` : ""}
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

function normalizeUrl(value) {
  const clean = text(value, 300);
  if (!clean) return "";
  try {
    const parsed = new URL(clean);
    return ["http:", "https:"].includes(parsed.protocol) ? parsed.toString() : "";
  } catch {
    return "";
  }
}

async function saveAdRequest(body) {
  const requesterName = text(body.requesterName || body.name, 120);
  const phone = text(body.phone, 80);
  const email = normalizeEmail(body.email);
  if (!requesterName || !phone) fail(400, "Nombre y telefono son requeridos para solicitar pauta");
  const item = {
    id: randomUUID(),
    requesterName,
    company: text(body.company, 140),
    phone,
    email,
    city: text(body.city, 80),
    targetUrl: normalizeUrl(body.targetUrl),
    message: text(body.message, 1200),
    mediaData: adBannerMediaDataText(body.mediaData, 12_000_000),
    mediaType: text(body.mediaType, 80),
    mediaName: text(body.mediaName, 160),
    status: "pending",
    createdAt: nowStamp(),
  };
  if (dbReady) {
    await pool.query(
      `INSERT INTO link_ad_requests
       (id, requester_name, company, phone, email, city, target_url, message, media_data, media_type, media_name, status)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
      [item.id, item.requesterName, item.company, item.phone, item.email, item.city, item.targetUrl, item.message, item.mediaData, item.mediaType, item.mediaName, item.status],
    );
  } else {
    const data = await readJsonData();
    data.adRequests.unshift(item);
    await writeJsonData(data);
  }
  return item;
}

async function saveAdCampaign(body, admin) {
  const title = text(body.title, 120);
  if (!title) fail(400, "Titulo de campana requerido");
  const item = {
    id: randomUUID(),
    title,
    advertiser: text(body.advertiser, 140),
    body: text(body.body, 300),
    targetUrl: normalizeUrl(body.targetUrl),
    mediaData: adBannerMediaDataText(body.mediaData, 12_000_000),
    mediaType: text(body.mediaType, 80),
    mediaName: text(body.mediaName, 160),
    startsAt: text(body.startsAt, 40) || null,
    endsAt: text(body.endsAt, 40) || null,
    status: text(body.status, 40) === "pending" ? "pending" : "published",
    createdAt: nowStamp(),
    updatedAt: nowStamp(),
    author: admin.displayName || admin.email,
  };
  if (dbReady) {
    await pool.query(
      `INSERT INTO link_ad_campaigns
       (id, title, advertiser, body, target_url, media_data, media_type, media_name, starts_at, ends_at, status)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NULLIF($9, '')::timestamptz, NULLIF($10, '')::timestamptz, $11)`,
      [item.id, item.title, item.advertiser, item.body, item.targetUrl, item.mediaData, item.mediaType, item.mediaName, item.startsAt || "", item.endsAt || "", item.status],
    );
  } else {
    const data = await readJsonData();
    data.adCampaigns.unshift(item);
    await writeJsonData(data);
  }
  return item;
}

async function readActiveAdCampaign() {
  if (dbReady) {
    const result = await pool.query(
      `SELECT id, title, advertiser, body, target_url AS "targetUrl",
              COALESCE(media_data, '') <> '' AS "hasMedia", media_type AS "mediaType", media_name AS "mediaName",
              starts_at AS "startsAt", ends_at AS "endsAt", status, created_at AS "createdAt", updated_at AS "updatedAt"
       FROM link_ad_campaigns
       WHERE status = 'published'
         AND (starts_at IS NULL OR starts_at <= now())
         AND (ends_at IS NULL OR ends_at >= now())
       ORDER BY created_at DESC LIMIT 1`,
    );
    return publicAdCampaign(result.rows[0] || null);
  }
  const now = Date.now();
  const data = await readJsonData();
  const active = data.adCampaigns.find((item) => {
    const status = normalizeStatus(item.status);
    const starts = item.startsAt ? Date.parse(item.startsAt) : 0;
    const ends = item.endsAt ? Date.parse(item.endsAt) : 0;
    return status === "published" && (!starts || starts <= now) && (!ends || ends >= now);
  }) || null;
  return publicAdCampaign(active);
}

async function readAdCampaignMedia(id) {
  const cleanId = text(id, 80);
  if (!cleanId) return null;
  if (dbReady) {
    if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(cleanId)) return null;
    const result = await pool.query(
      `SELECT media_data AS "mediaData", media_type AS "mediaType"
       FROM link_ad_campaigns
       WHERE id = $1
         AND status = 'published'
         AND (starts_at IS NULL OR starts_at <= now())
         AND (ends_at IS NULL OR ends_at >= now())
       LIMIT 1`,
      [cleanId],
    );
    const row = result.rows[0];
    const payload = mediaDataPayload(row?.mediaData);
    return payload ? { ...payload, type: payload.type || row?.mediaType || "application/octet-stream" } : null;
  }
  const now = Date.now();
  const data = await readJsonData();
  const item = data.adCampaigns.find((campaign) => {
    const starts = campaign.startsAt ? Date.parse(campaign.startsAt) : 0;
    const ends = campaign.endsAt ? Date.parse(campaign.endsAt) : 0;
    return campaign.id === cleanId && normalizeStatus(campaign.status) === "published" && (!starts || starts <= now) && (!ends || ends >= now);
  });
  const payload = mediaDataPayload(item?.mediaData);
  return payload ? { ...payload, type: payload.type || item?.mediaType || "application/octet-stream" } : null;
}

async function readSettingsLogoMedia() {
  const settings = await readSettings();
  const payload = mediaDataPayload(settings.logoData);
  return payload ? { ...payload, type: payload.type || settings.logoType || "application/octet-stream" } : null;
}

async function readProductMedia(id) {
  const cleanId = text(id, 80);
  if (!cleanId) return null;
  if (dbReady) {
    if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(cleanId)) return null;
    const result = await pool.query(
      `SELECT media_data AS "mediaData", media_type AS "mediaType"
       FROM link_products
       WHERE id = $1 AND status = 'published'
       LIMIT 1`,
      [cleanId],
    );
    const row = result.rows[0];
    const payload = mediaDataPayload(row?.mediaData);
    return payload ? { ...payload, type: payload.type || row?.mediaType || "application/octet-stream" } : null;
  }
  const data = await readJsonData();
  const item = data.products.find((product) => product.id === cleanId && normalizeStatus(product.status) === "published");
  const payload = mediaDataPayload(item?.mediaData);
  return payload ? { ...payload, type: payload.type || item?.mediaType || "application/octet-stream" } : null;
}

async function moderateAdRequest(body) {
  const id = text(body.id, 80);
  const action = text(body.action, 40);
  if (!id || !["approve", "reject", "delete"].includes(action)) fail(400, "Solicitud de pauta no valida");
  if (dbReady) {
    if (action === "delete") {
      await pool.query("DELETE FROM link_ad_requests WHERE id = $1", [id]);
      return { ok: true, status: "deleted" };
    }
    const status = action === "approve" ? "published" : "hidden";
    const result = await pool.query("UPDATE link_ad_requests SET status = $1, resolved_at = now() WHERE id = $2", [status, id]);
    if (!result.rowCount) fail(404, "Solicitud de pauta no encontrada");
    return { ok: true, status };
  }
  const data = await readJsonData();
  const index = data.adRequests.findIndex((item) => item.id === id);
  if (index < 0) fail(404, "Solicitud de pauta no encontrada");
  if (action === "delete") {
    data.adRequests.splice(index, 1);
  } else {
    data.adRequests[index].status = action === "approve" ? "published" : "hidden";
    data.adRequests[index].resolvedAt = nowStamp();
  }
  await writeJsonData(data);
  return { ok: true, status: data.adRequests[index]?.status || "deleted" };
}

const moderationTargets = {
  news: { table: "link_news", collection: "news" },
  products: { table: "link_products", collection: "products" },
  threads: { table: "link_threads", collection: "threads" },
  resumes: { table: "link_resumes", collection: "resumes" },
  vacancies: { table: "link_vacancies", collection: "vacancies" },
  adCampaigns: { table: "link_ad_campaigns", collection: "adCampaigns" },
};

function adminLabelUser(users, id) {
  const found = users.find((item) => item.id === id);
  return found?.displayName || found?.display_name || found?.email || "";
}

function statusFromAction(action) {
  if (action === "publish") return "published";
  if (action === "hide") return "hidden";
  if (action === "pending") return "pending";
  fail(400, "Accion no permitida");
}

async function readAdminState() {
  const settings = await readSettings();
  if (dbReady) {
    const [users, recovery, news, products, threads, resumes, vacancies, adRequests, adCampaigns, tokenTransactions] = await Promise.all([
      pool.query(
        `SELECT id, email, account_type AS "accountType", display_name AS "displayName", city, company_name AS "companyName",
                role, is_admin AS "isAdmin", token_balance AS "tokenBalance", status, last_seen_at AS "lastSeenAt", deactivated_at AS "deactivatedAt",
                created_at AS "createdAt"
         FROM link_users ORDER BY display_name ASC, email ASC LIMIT 300`,
      ),
      pool.query(
        `SELECT r.id, r.email, r.status, r.created_at AS "createdAt", r.resolved_at AS "resolvedAt",
                u.display_name AS "displayName"
         FROM link_password_recovery_requests r
         LEFT JOIN link_users u ON u.id = r.user_id
         ORDER BY CASE WHEN r.status = 'pending' THEN 0 ELSE 1 END, r.created_at DESC
         LIMIT 100`,
      ),
      pool.query(
        `SELECT n.id, n.title, n.category, n.status, n.created_at AS "createdAt", u.display_name AS author
         FROM link_news n LEFT JOIN link_users u ON u.id = n.author_id
         ORDER BY n.created_at DESC LIMIT 200`,
      ),
      pool.query(
        `SELECT p.id, p.name AS title, p.condition AS category, p.status, p.created_at AS "createdAt", u.display_name AS author
         FROM link_products p LEFT JOIN link_users u ON u.id = p.author_id
         ORDER BY p.created_at DESC LIMIT 200`,
      ),
      pool.query(
        `SELECT t.id, t.topic AS title, 'Conversacion' AS category, t.status, t.created_at AS "createdAt", u.display_name AS author
         FROM link_threads t LEFT JOIN link_users u ON u.id = t.author_id
         ORDER BY t.created_at DESC LIMIT 200`,
      ),
      pool.query(
        `SELECT r.id, r.full_name AS title, COALESCE(NULLIF(r.category, ''), r.headline) AS category, r.status, r.updated_at AS "createdAt", u.display_name AS author
         FROM link_resumes r LEFT JOIN link_users u ON u.id = r.user_id
         ORDER BY r.updated_at DESC LIMIT 200`,
      ),
      pool.query(
        `SELECT v.id, v.title, v.company AS category, v.status, v.created_at AS "createdAt", u.display_name AS author
         FROM link_vacancies v LEFT JOIN link_users u ON u.id = v.user_id
         ORDER BY v.created_at DESC LIMIT 200`,
      ),
      pool.query(
        `SELECT id, requester_name AS "requesterName", company, phone, email, city, target_url AS "targetUrl",
                message, media_name AS "mediaName", status, created_at AS "createdAt", resolved_at AS "resolvedAt"
         FROM link_ad_requests
         ORDER BY CASE WHEN status = 'pending' THEN 0 ELSE 1 END, created_at DESC LIMIT 200`,
      ),
      pool.query(
        `SELECT id, title, advertiser, body, target_url AS "targetUrl", media_name AS "mediaName",
                starts_at AS "startsAt", ends_at AS "endsAt", status, created_at AS "createdAt", updated_at AS "updatedAt"
         FROM link_ad_campaigns
         ORDER BY created_at DESC LIMIT 200`,
      ),
      pool.query(
        `SELECT t.id, t.user_id AS "userId", t.admin_id AS "adminId", t.kind, t.amount, t.balance_after AS "balanceAfter",
                t.reference_id AS "referenceId", t.note, t.created_at AS "createdAt", u.display_name AS "userName", u.email AS "userEmail"
         FROM link_token_transactions t
         LEFT JOIN link_users u ON u.id = t.user_id
         ORDER BY t.created_at DESC LIMIT 200`,
      ),
    ]);
    return {
      settings: publicSettings(settings),
      users: users.rows,
      passwordRecoveryRequests: recovery.rows,
      content: {
        news: news.rows,
        products: products.rows,
        threads: threads.rows,
        resumes: resumes.rows,
        vacancies: vacancies.rows,
        adCampaigns: adCampaigns.rows.map((item) => ({ ...item, category: item.advertiser || "Pauta" })),
      },
      adRequests: adRequests.rows,
      tokenTransactions: tokenTransactions.rows,
      storage: storageInfo(),
    };
  }

  const data = await readJsonData();
  const users = data.users
    .map((item) => sanitizeUser(item))
    .sort((a, b) => String(a.displayName || a.email).localeCompare(String(b.displayName || b.email), "es", { sensitivity: "base" }));
  const author = (item) => adminLabelUser(users, item.authorId || item.userId);
  const recoveryRequests = data.passwordRecoveryRequests.map((item) => ({
    id: item.id,
    email: item.email,
    displayName: item.displayName || adminLabelUser(users, item.userId),
    status: item.status || "pending",
    createdAt: item.createdAt || null,
    resolvedAt: item.resolvedAt || null,
  }));
  const summarize = (items, titleKey, categoryKey) => items.map((item) => ({
    id: item.id,
    title: item[titleKey] || item.title || item.name || "",
    category: item[categoryKey] || item.category || "",
    status: normalizeStatus(item.status),
    author: author(item),
    createdAt: item.updatedAt || item.createdAt || null,
  }));
  return {
    settings: publicSettings(data.settings),
    users,
    passwordRecoveryRequests: recoveryRequests,
    content: {
      news: summarize(data.news, "title", "category"),
      products: summarize(data.products, "name", "condition"),
      threads: summarize(data.threads, "topic", "name"),
      resumes: summarize(data.resumes, "fullName", "headline"),
      vacancies: summarize(data.vacancies, "title", "company"),
      adCampaigns: summarize(data.adCampaigns, "title", "advertiser"),
    },
    adRequests: data.adRequests,
    tokenTransactions: data.tokenTransactions,
    storage: storageInfo(),
  };
}

async function moderateContent(body) {
  const type = text(body.type, 40);
  const id = text(body.id, 80);
  const action = text(body.action, 40);
  const target = moderationTargets[type];
  if (!target || !id) fail(400, "Publicacion no valida");

  if (dbReady) {
    if (action === "delete") {
      await pool.query(`DELETE FROM ${target.table} WHERE id = $1`, [id]);
      return { ok: true };
    }
    const status = statusFromAction(action);
    const result = await pool.query(`UPDATE ${target.table} SET status = $1 WHERE id = $2`, [status, id]);
    if (!result.rowCount) fail(404, "Publicacion no encontrada");
    return { ok: true, status };
  }

  const data = await readJsonData();
  const list = data[target.collection];
  const index = list.findIndex((item) => item.id === id);
  if (index < 0) fail(404, "Publicacion no encontrada");
  if (action === "delete") {
    list.splice(index, 1);
  } else {
    list[index].status = statusFromAction(action);
  }
  await writeJsonData(data);
  return { ok: true, status: list[index]?.status || "deleted" };
}

async function moderateUser(body, admin) {
  const id = text(body.id, 80);
  const action = text(body.action, 40);
  if (!id) fail(400, "Usuario no valido");
  if (id === admin.id && ["deactivate", "delete"].includes(action)) {
    fail(400, "No puedes dar de baja o eliminar tu propia cuenta de administrador");
  }

  if (dbReady) {
    if (action === "delete") {
      const result = await pool.query("DELETE FROM link_users WHERE id = $1", [id]);
      if (!result.rowCount) fail(404, "Usuario no encontrado");
      return { ok: true, status: "deleted" };
    }
    if (action === "deactivate") {
      const result = await pool.query(
        "UPDATE link_users SET status = 'inactive', deactivated_at = now(), updated_at = now() WHERE id = $1",
        [id],
      );
      if (!result.rowCount) fail(404, "Usuario no encontrado");
      return { ok: true, status: "inactive" };
    }
    if (action === "activate") {
      const result = await pool.query(
        "UPDATE link_users SET status = 'active', deactivated_at = NULL, updated_at = now() WHERE id = $1",
        [id],
      );
      if (!result.rowCount) fail(404, "Usuario no encontrado");
      return { ok: true, status: "active" };
    }
    fail(400, "Accion de usuario no permitida");
  }

  const data = await readJsonData();
  const index = data.users.findIndex((item) => item.id === id);
  if (index < 0) fail(404, "Usuario no encontrado");
  if (action === "delete") {
    data.users.splice(index, 1);
    data.sessions = data.sessions.filter((item) => item.userId !== id);
    await writeJsonData(data);
    return { ok: true, status: "deleted" };
  }
  if (action === "deactivate") {
    data.users[index].status = "inactive";
    data.users[index].deactivatedAt = nowStamp();
    await writeJsonData(data);
    return { ok: true, status: "inactive" };
  }
  if (action === "activate") {
    data.users[index].status = "active";
    data.users[index].deactivatedAt = null;
    await writeJsonData(data);
    return { ok: true, status: "active" };
  }
  fail(400, "Accion de usuario no permitida");
}

async function moderatePasswordRecovery(body) {
  const id = text(body.id, 80);
  const action = text(body.action, 40);
  if (!id || !["approve", "reject", "delete"].includes(action)) fail(400, "Solicitud no valida");

  if (dbReady) {
    if (action === "delete") {
      await pool.query("DELETE FROM link_password_recovery_requests WHERE id = $1", [id]);
      return { ok: true, status: "deleted" };
    }
    const found = await pool.query(
      `SELECT id, user_id, password_hash, status
       FROM link_password_recovery_requests WHERE id = $1`,
      [id],
    );
    const request = found.rows[0];
    if (!request) fail(404, "Solicitud no encontrada");
    if (request.status !== "pending") fail(400, "La solicitud ya fue procesada");
    if (action === "approve") {
      await pool.query("BEGIN");
      try {
        await pool.query("UPDATE link_users SET password_hash = $1, updated_at = now() WHERE id = $2", [request.password_hash, request.user_id]);
        await pool.query("UPDATE link_password_recovery_requests SET status = 'approved', resolved_at = now() WHERE id = $1", [id]);
        await pool.query("COMMIT");
      } catch (error) {
        await pool.query("ROLLBACK");
        throw error;
      }
      return { ok: true, status: "approved" };
    }
    await pool.query("UPDATE link_password_recovery_requests SET status = 'rejected', resolved_at = now() WHERE id = $1", [id]);
    return { ok: true, status: "rejected" };
  }

  const data = await readJsonData();
  const index = data.passwordRecoveryRequests.findIndex((item) => item.id === id);
  if (index < 0) fail(404, "Solicitud no encontrada");
  const request = data.passwordRecoveryRequests[index];
  if (action === "delete") {
    data.passwordRecoveryRequests.splice(index, 1);
    await writeJsonData(data);
    return { ok: true, status: "deleted" };
  }
  if (request.status !== "pending") fail(400, "La solicitud ya fue procesada");
  if (action === "approve") {
    const user = data.users.find((item) => item.id === request.userId);
    if (!user) fail(404, "Usuario no encontrado");
    user.passwordHash = request.passwordHash;
    request.status = "approved";
    request.resolvedAt = nowStamp();
  } else {
    request.status = "rejected";
    request.resolvedAt = nowStamp();
  }
  await writeJsonData(data);
  return { ok: true, status: request.status };
}

async function updateAdminSettings(body) {
  const current = await readSettings();
  const next = { ...current };
  if (body.clearLogo) {
    next.logoData = "";
    next.logoType = "";
    next.logoName = "";
  } else if (body.logoData) {
    next.logoData = logoDataText(body.logoData, 16_000_000);
    next.logoType = text(body.logoType, 120);
    next.logoName = text(body.logoName, 160);
  } else {
    fail(400, "Seleccione un archivo de logo antes de guardar");
  }
  next.updatedAt = nowStamp();
  return writeSettings(next);
}

async function updateTokenSettings(body) {
  const current = await readSettings();
  const next = {
    ...current,
    tokenCvDownloadCost: intSetting(body.tokenCvDownloadCost, current.tokenCvDownloadCost, 0, 10000),
    tokenVacancyCost: intSetting(body.tokenVacancyCost, current.tokenVacancyCost, 0, 10000),
    tokenTrialCompanyTokens: intSetting(body.tokenTrialCompanyTokens, current.tokenTrialCompanyTokens, 0, 100000),
    tokenPackageValue: intSetting(body.tokenPackageValue, current.tokenPackageValue, 0, 100000000),
    tokenPackageTokens: intSetting(body.tokenPackageTokens, current.tokenPackageTokens, 0, 1000000),
    paymentInfo: text(body.paymentInfo, 300) || current.paymentInfo,
    updatedAt: nowStamp(),
  };
  return writeSettings(next);
}

async function adminLoadTokens(body, admin) {
  const userId = text(body.userId, 80);
  const amount = intSetting(body.amount, 0, 0, 1_000_000);
  const note = text(body.note || body.paymentRef, 240) || "Recarga manual de tokens";
  if (!userId || amount <= 0) fail(400, "Empresa y cantidad de tokens son requeridos");
  if (dbReady) {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      const found = await client.query(
        "SELECT id, account_type, token_balance FROM link_users WHERE id = $1 FOR UPDATE",
        [userId],
      );
      const company = found.rows[0];
      if (!company) fail(404, "Empresa no encontrada");
      if (company.account_type !== "company") fail(400, "Solo se cargan tokens a cuentas de empresa");
      const nextBalance = Number(company.token_balance || 0) + amount;
      await client.query("UPDATE link_users SET token_balance = $1, updated_at = now() WHERE id = $2", [nextBalance, userId]);
      await client.query(
        `INSERT INTO link_token_transactions (id, user_id, admin_id, kind, amount, balance_after, note)
         VALUES ($1, $2, $3, 'admin_load', $4, $5, $6)`,
        [randomUUID(), userId, admin.id, amount, nextBalance, note],
      );
      await client.query("COMMIT");
      return { ok: true, tokenBalance: nextBalance };
    } catch (error) {
      await client.query("ROLLBACK").catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  }

  const data = await readJsonData();
  const company = data.users.find((item) => item.id === userId);
  if (!company) fail(404, "Empresa no encontrada");
  if (normalizeAccountType(company.accountType) !== "company") fail(400, "Solo se cargan tokens a cuentas de empresa");
  const nextBalance = intSetting(company.tokenBalance, 0, 0, 1_000_000) + amount;
  company.tokenBalance = nextBalance;
  data.tokenTransactions.unshift({
    id: randomUUID(),
    userId,
    adminId: admin.id,
    kind: "admin_load",
    amount,
    balanceAfter: nextBalance,
    note,
    createdAt: nowStamp(),
  });
  await writeJsonData(data);
  return { ok: true, tokenBalance: nextBalance };
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

function sendMedia(res, req, media) {
  const total = media.buffer.length;
  const headers = {
    "content-type": media.type || "application/octet-stream",
    "cache-control": "public, max-age=3600",
    "accept-ranges": "bytes",
  };
  const range = req.headers.range || "";
  const match = String(range).match(/^bytes=(\d*)-(\d*)$/);
  if (match && total > 0) {
    let start = match[1] ? Number.parseInt(match[1], 10) : 0;
    let end = match[2] ? Number.parseInt(match[2], 10) : total - 1;
    if (!match[1] && match[2]) {
      const suffixLength = Number.parseInt(match[2], 10);
      start = Math.max(total - suffixLength, 0);
      end = total - 1;
    }
    if (Number.isInteger(start) && Number.isInteger(end) && start >= 0 && end >= start && start < total) {
      end = Math.min(end, total - 1);
      res.writeHead(206, {
        ...headers,
        "content-length": end - start + 1,
        "content-range": `bytes ${start}-${end}/${total}`,
      });
      res.end(media.buffer.subarray(start, end + 1));
      return;
    }
    res.writeHead(416, { ...headers, "content-range": `bytes */${total}` });
    res.end();
    return;
  }
  res.writeHead(200, { ...headers, "content-length": total });
  res.end(media.buffer);
}

async function handleApi(req, res, url) {
  if (req.method === "GET" && url.pathname === "/api/settings/logo") {
    const media = await readSettingsLogoMedia();
    if (!media) {
      notFound(res);
      return;
    }
    sendMedia(res, req, media);
    return;
  }

  const productMediaMatch = url.pathname.match(/^\/api\/products\/([^/]+)\/media$/);
  if (req.method === "GET" && productMediaMatch) {
    const media = await readProductMedia(decodeURIComponent(productMediaMatch[1]));
    if (!media) {
      notFound(res);
      return;
    }
    sendMedia(res, req, media);
    return;
  }

  const adMediaMatch = url.pathname.match(/^\/api\/ad-campaigns\/([^/]+)\/media$/);
  if (req.method === "GET" && adMediaMatch) {
    const media = await readAdCampaignMedia(decodeURIComponent(adMediaMatch[1]));
    if (!media) {
      notFound(res);
      return;
    }
    sendMedia(res, req, media);
    return;
  }

  if (req.method === "GET" && url.pathname === "/api/state") {
    const authUser = await getAuthUser(req);
    const data = await readData(authUser);
    const forceOfficialRefresh = url.searchParams.get("refresh") === "1";
    let officialNews;
    if (forceOfficialRefresh) {
      officialNews = await getOfficialNews(true).catch(() => officialNewsSnapshot());
    } else {
      officialNews = officialNewsSnapshot();
      const updatedAt = officialNews.updatedAt ? Date.parse(officialNews.updatedAt) : 0;
      if (!officialNewsCache.promise && (!updatedAt || Date.now() - updatedAt >= officialNewsTtlMs)) {
        getOfficialNews(false).catch(() => {});
      }
    }
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

  if (req.method === "GET" && url.pathname === "/api/admin/state") {
    await requireAdmin(req);
    json(res, 200, await readAdminState());
    return;
  }

  if (req.method === "POST" && url.pathname === "/api/admin/moderate") {
    await requireAdmin(req);
    json(res, 200, await moderateContent(await readBody(req)));
    return;
  }

  if (req.method === "POST" && url.pathname === "/api/admin/users") {
    const admin = await requireAdmin(req);
    json(res, 200, await moderateUser(await readBody(req), admin));
    return;
  }

  if (req.method === "POST" && url.pathname === "/api/admin/recovery") {
    await requireAdmin(req);
    json(res, 200, await moderatePasswordRecovery(await readBody(req)));
    return;
  }

  if (req.method === "POST" && url.pathname === "/api/admin/settings") {
    await requireAdmin(req);
    json(res, 200, { settings: publicSettings(await updateAdminSettings(await readBody(req))) });
    return;
  }

  if (req.method === "POST" && url.pathname === "/api/admin/token-settings") {
    await requireAdmin(req);
    json(res, 200, { settings: publicSettings(await updateTokenSettings(await readBody(req))) });
    return;
  }

  if (req.method === "POST" && url.pathname === "/api/admin/tokens/load") {
    const admin = await requireAdmin(req);
    json(res, 200, await adminLoadTokens(await readBody(req), admin));
    return;
  }

  if (req.method === "POST" && url.pathname === "/api/admin/ad-campaigns") {
    const admin = await requireAdmin(req);
    json(res, 201, await saveAdCampaign(await readBody(req), admin));
    return;
  }

  if (req.method === "POST" && url.pathname === "/api/admin/ad-requests") {
    await requireAdmin(req);
    json(res, 200, await moderateAdRequest(await readBody(req)));
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

  if (req.method === "POST" && url.pathname === "/api/auth/recover") {
    json(res, 200, await requestPasswordRecovery(await readBody(req)));
    return;
  }

  if (req.method === "POST" && url.pathname === "/api/auth/change-password") {
    json(res, 200, await changePassword(await readBody(req), await requireUser(req)));
    return;
  }

  if (req.method === "POST" && url.pathname === "/api/auth/admin-recover") {
    json(res, 200, await recoverAdminPassword(await readBody(req)));
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

  if (req.method === "POST" && url.pathname === "/api/ad-requests") {
    json(res, 201, await saveAdRequest(await readBody(req)));
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

  const resumeDownloadMatch = url.pathname.match(/^\/api\/resumes\/([^/]+)\/download$/);
  if (req.method === "POST" && resumeDownloadMatch) {
    json(res, 200, await downloadResume(resumeDownloadMatch[1], await requireUser(req)));
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

async function serveIndex(res) {
  const filePath = path.join(publicDir, "index.html");
  const settings = await readSettings();
  const bootstrap = JSON.stringify({ settings: publicSettings(settings) }).replace(/</g, "\\u003c");
  const html = (await readFile(filePath, "utf8")).replace(
    "window.__LINK_BOOTSTRAP_SETTINGS__ = window.__LINK_BOOTSTRAP_SETTINGS__ || null;",
    `window.__LINK_BOOTSTRAP_SETTINGS__ = ${bootstrap};`,
  );
  res.writeHead(200, {
    "content-type": "text/html; charset=utf-8",
    "cache-control": "no-store",
  });
  res.end(html);
}

async function serveStatic(req, res, url) {
  const filePath = safePublicPath(url.pathname);
  if (!filePath) return notFound(res);
  try {
    const info = await stat(filePath);
    if (!info.isFile()) throw new Error("Not a file");
    if (filePath.endsWith("index.html")) {
      await serveIndex(res);
      return;
    }
    const contentType = mimeTypes.get(path.extname(filePath).toLowerCase()) || "application/octet-stream";
    res.writeHead(200, {
      "content-type": contentType,
      "cache-control": filePath.endsWith("index.html") ? "no-store" : "public, max-age=3600",
    });
    createReadStream(filePath).pipe(res);
  } catch {
    if (!path.extname(url.pathname)) {
      await serveIndex(res);
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
      res.writeHead(403, { "content-type": "text/html; charset=utf-8", "cache-control": "no-store" });
      res.end("<!doctype html><html lang=\"es\"><meta charset=\"utf-8\"><title>Descarga protegida</title><body><h1>Descarga protegida</h1><p>Ingresa a Link y usa el boton Descargar CV. Las empresas necesitan saldo de tokens.</p></body></html>");
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
