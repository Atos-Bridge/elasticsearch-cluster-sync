import { createRequire } from "module";
const require = createRequire(import.meta.url);
require("dotenv").config();
import Bree from "bree";
const express = require("express");

const pino = require("pino");

const logger = pino({
  level: "info",
  timestamp: () => `,"time":"${new Date().toISOString()}"`,
  transport: {
    target: "pino-pretty",
    options: {
      colorize: true,
      translateTime: 'yyyy-mm-dd"T"hh:MM:ss.l  Z',
    },
  },
});

const app = express();
const bree = new Bree({
  logger: logger,
  jobs: [
    {
      name: "cmdb-cis",
      interval: "1m",
      worker: {
        workerData: {
          foo: "bar",
          beep: "boop",
        },
      },
    },
  ],
});

try {
  await bree.start();
  await bree.run();
} catch (e) {
  console.error("error" + e);
}

app.listen(3000, () => {
  logger.info("application listening..... on port 3000");
});
