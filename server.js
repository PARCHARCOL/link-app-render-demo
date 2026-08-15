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

const emptyData = {
  news: [],
  jobs: [],
  products: [],
  threads: [],
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
    json(res, 200, await readData());
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
