"use strict";

// Unified BotHost entrypoint.
// Default mode keeps current behavior (starts node_bot.js).
// Optional static modes serve Mini App from ./public to follow BotHost manual.

const fs = require("node:fs");
const http = require("node:http");
const path = require("node:path");

const ROOT_DIR = __dirname;
const PUBLIC_DIR = path.join(ROOT_DIR, "public");
const PORT = Number.parseInt(process.env.PORT || "3000", 10) || 3000;
const APP_MODE = String(process.env.APP_MODE || "").trim().toLowerCase();
const PUBLIC_BASE_URL = String(process.env.PUBLIC_BASE_URL || process.env.BOT_PUBLIC_DOMAIN || "").trim();
const WEBAPP_URL = String(process.env.WEBAPP_URL || "").trim();

const MIME_TYPES = {
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

function resolvePublicPath(reqUrl) {
  const parsed = new URL(reqUrl || "/", "http://localhost");
  let pathname = decodeURIComponent(parsed.pathname || "/");

  // Backward compatibility for old miniapp paths.
  if (pathname === "/miniapp" || pathname === "/miniapp/") {
    pathname = "/index.html";
  } else if (pathname.startsWith("/miniapp/")) {
    pathname = pathname.slice("/miniapp".length);
  }

  if (pathname === "/" || pathname === "") {
    pathname = "/index.html";
  }

  const fullPath = path.normalize(path.join(PUBLIC_DIR, pathname));
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
    const method = String(req.method || "GET").toUpperCase();
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
          res.end("File not found");
          return;
        }
        res.writeHead(500, { "content-type": "text/plain; charset=utf-8" });
        res.end(`Server error: ${error.code || "unknown"}`);
        return;
      }

      const ext = path.extname(filePath).toLowerCase();
      res.writeHead(200, { "content-type": MIME_TYPES[ext] || "application/octet-stream" });
      if (method === "HEAD") {
        res.end();
        return;
      }
      res.end(content);
    });
  });

  server.listen(PORT, "0.0.0.0", () => {
    // eslint-disable-next-line no-console
    console.info(`[web] static mini app server started: http://0.0.0.0:${PORT} (dir: ${PUBLIC_DIR})`);
    // eslint-disable-next-line no-console
    console.info(`[web] public base: ${PUBLIC_BASE_URL || "(not configured)"}`);
    // eslint-disable-next-line no-console
    console.info(`[web] webapp url: ${WEBAPP_URL || "(not configured)"}`);
  });
}

function startNodeBot() {
  const nodeBotPath = path.join(ROOT_DIR, "node_bot.js");
  if (!fs.existsSync(nodeBotPath)) {
    // eslint-disable-next-line no-console
    console.error("[entry] node_bot.js not found");
    process.exitCode = 1;
    return;
  }

  // eslint-disable-next-line no-console
  console.info("[entry] starting app.js -> node_bot.js");
  require("./node_bot.js");
}

if (APP_MODE === "static" || APP_MODE === "miniapp") {
  startStaticServer();
} else if (APP_MODE === "both") {
  startStaticServer();
  startNodeBot();
} else {
  // Default: keep existing stable behavior.
  startNodeBot();
}
