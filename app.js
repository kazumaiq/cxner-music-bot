"use strict";

// Stable Node entrypoint for hosting panels that can run only one JS file.
// Runs Telegram bot in Node-only environments (without Python runtime).
// eslint-disable-next-line no-console
console.info("[entry] starting app.js -> node_bot.js");
require("./node_bot.js");
