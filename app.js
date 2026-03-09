"use strict";

// BotHost entrypoint for the bot only.
// Mini App hosting is disabled here to avoid port conflicts and keep long polling stable.

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
