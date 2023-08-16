import { createRequire } from "module";
const require = createRequire(import.meta.url);
const pino = require("pino");

export default ({ appName = "main", level = "info" } = {}) =>
  pino({
    mixin() {
      return { appName };
    },
    level,
    timestamp: () => `,"time":"${new Date().toISOString()}"`,
    transport: {
      target: "pino-pretty",
      options: {
        colorize: true,
        translateTime: 'yyyy-mm-dd"T"hh:MM:ss.l  Z',
      },
    },
  });
