"use strict";

// BotHost entrypoint:
// - always serves static files from ./public for Mini App hosting
// - starts node_bot.js by default, unless explicitly forced into static-only mode

const http = require("node:http");
const fs = require("node:fs");
const path = require("node:path");

const ROOT = __dirname;
const PUBLIC_DIR = path.join(ROOT, "public");
const PORT = Number.parseInt(process.env.PORT || "3000", 10) || 3000;

const mimeTypes = {
  ".html": "text/html; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".js": "application/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".gif": "image/gif",
  ".ico": "image/x-icon",
  ".svg": "image/svg+xml",
  ".webp": "image/webp",
  ".txt": "text/plain; charset=utf-8"
};

function clean(v) {
  return String(v ?? "").trim();
}

function resolvePublicPath(reqUrl) {
  const baseUrl = new URL(reqUrl || "/", "http://localhost");
  let urlPath = decodeURIComponent(baseUrl.pathname || "/");

  // Support old paths like /miniapp/index.html and /miniapp/assets/*
  if (urlPath === "/miniapp" || urlPath === "/miniapp/") {
    urlPath = "/index.html";
  } else if (urlPath.startsWith("/miniapp/")) {
    urlPath = urlPath.slice("/miniapp".length);
  }

  if (urlPath === "/" || urlPath === "") {
    urlPath = "/index.html";
  }

  const fullPath = path.normalize(path.join(PUBLIC_DIR, urlPath));
  if (!fullPath.startsWith(PUBLIC_DIR)) {
    return null;
  }

  if (fs.existsSync(fullPath) && fs.statSync(fullPath).isDirectory()) {
    return path.join(fullPath, "index.html");
  }

  return fullPath;
}

function startStaticServer() {
  if (!fs.existsSync(PUBLIC_DIR)) {
    fs.mkdirSync(PUBLIC_DIR, { recursive: true });
  }

  const server = http.createServer((req, res) => {
    const method = clean(req.method || "GET").toUpperCase();
    const reqPath = clean(req.url || "/") || "/";
    console.info(`[miniapp] ${method} ${reqPath}`);

    if (method !== "GET" && method !== "HEAD") {
      res.writeHead(405, { "content-type": "text/plain; charset=utf-8" });
      res.end("Method not allowed");
      return;
    }

    const filePath = resolvePublicPath(req.url || "/");
    if (!filePath) {
      res.writeHead(403, { "content-type": "text/plain; charset=utf-8" });
      res.end("Forbidden");
      return;
    }

    fs.readFile(filePath, (error, content) => {
      if (error) {
        if (error.code === "ENOENT") {
          res.writeHead(404, { "content-type": "text/plain; charset=utf-8" });
          res.end("Файл не найден");
          return;
        }
        res.writeHead(500, { "content-type": "text/plain; charset=utf-8" });
        res.end(`Ошибка сервера: ${error.code || "unknown"}`);
        return;
      }

      const extname = path.extname(filePath).toLowerCase();
      const contentType = mimeTypes[extname] || "application/octet-stream";
      res.writeHead(200, { "content-type": contentType });
      if (method === "HEAD") {
        res.end();
        return;
      }
      res.end(content);
    });
  });

  server.listen(PORT, "0.0.0.0", () => {
    console.info(`✅ Mini App static server started on port ${PORT}`);
  });
}

function shouldStartNodeBot() {
  const staticOnly = clean(process.env.STATIC_ONLY || process.env.MINIAPP_STATIC_MODE).toLowerCase();
  if (["1", "true", "yes", "on"].includes(staticOnly)) {
    return false;
  }

  const appMode = clean(process.env.APP_MODE).toLowerCase();
  if (appMode === "static") {
    return false;
  }
  if (appMode === "bot") {
    return true;
  }

  // BotHost free plan usually just runs `node app.js` with BOT_TOKEN only.
  // In that setup the bot must start automatically without extra env flags.
  return fs.existsSync(path.join(ROOT, "node_bot.js"));
}

startStaticServer();

if (shouldStartNodeBot() && fs.existsSync(path.join(ROOT, "node_bot.js"))) {
  console.info("[entry] starting app.js -> node_bot.js");
  try {
    require("./node_bot.js");
  } catch (error) {
    console.error(`[entry] failed to start node_bot.js: ${clean(error?.message || error)}`);
  }
} else {
  console.info("[entry] running static Mini App mode");
}
