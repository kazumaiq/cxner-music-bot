"use strict";

// BotHost entrypoint for the bot only.
// Mini App hosting is disabled here to avoid port conflicts and keep long polling stable.

// На нодах с другой локалью (например Амстердам) без UTF-8 текст может уходить в Telegram кракозябрами.
// Принудительно задаём UTF-8, чтобы русские строки отображались корректно.
if (!process.env.LANG || process.env.LANG === "C" || process.env.LANG === "POSIX") {
  process.env.LANG = "en_US.UTF-8";
}
if (!process.env.LC_ALL || process.env.LC_ALL === "C" || process.env.LC_ALL === "POSIX") {
  process.env.LC_ALL = "en_US.UTF-8";
}

function clean(v) {
  return String(v ?? "").trim();
}

console.info("[entry] starting app.js -> node_bot.js");

try {
  require("./node_bot.js");
} catch (error) {
  console.error(`[entry] failed to start node_bot.js: ${clean(error?.message || error)}`);
  process.exit(1);
}
