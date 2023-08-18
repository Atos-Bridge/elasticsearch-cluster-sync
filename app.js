import { createRequire } from "module";
const require = createRequire(import.meta.url);
require("dotenv").config();
import * as fs from "node:fs";
import _ from "lodash-es";
import Logger from "./lib/Logger.js";
import Bree from "bree";
import { Command, Option } from "commander";
const express = require("express");
import Config from "./lib/Config.js";
const flatten = require("flat");
const pino = require("pino");
const getPackage = () => {
  const file = `${process.cwd()}/package.json`;
  return JSON.parse(fs.readFileSync(file));
};

const Package = getPackage();

const program = new Command();
program
  .name("elasticseaarch-cluster-sync")
  .description("CLI to some JavaScript string utilities")
  .version("1.0.0")
  .addOption(
    new Option("-l, --level <loglevel>", "default info")
      .default("info")
      .choices(_.keys(pino.levels.values))
  );
const options = program.parse().opts();

const logger = Logger(options);
const jobConfig = Config.transform(options);

if (_.keys(jobConfig).length == 0) {
  throw new Error("No jobs loaded ...! You have to set jobs as environment.");
}
const app = express();
const bree = new Bree({
  logger: logger,
  jobs: _.keys(_.get(jobConfig, "JOB", {})).map((job) => {
    return {
      name: job.toLowerCase(),
      outputWorkerMetadata: true,
      interval:
        jobConfig.JOB[job].SCHEDULE.INTERVAL ||
        jobConfig.JOB[job].SCHEDULE.CRON,
      worker: {
        workerData: { ...jobConfig.JOB[job].WORKER, Package },
      },
    };
  }),
  errorHandler: (error, workerMetadata) => {
    // workerMetadata will be populated with extended worker information only if
    // Bree instance is initialized with parameter `workerMetadata: true`
    if (workerMetadata.threadId) {
      logger.info(
        `There was an error while running a worker ${workerMetadata.name} with thread ID: ${workerMetadata.threadId}`
      );
    } else {
      logger.info(
        `There was an error while running a worker ${workerMetadata.name}`
      );
    }

    logger.error(error);
  },
});

try {
  await bree.start();
  await bree.run();
} catch (e) {
  logger.error("error" + e);
}

process.on("unhandledRejection", (promise) => {
  logger.error(promise);
  process.exit(42);
});

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

app.get("/jobs", async (req, res, next) => {
  res.json({
    jobs: bree.config.jobs.map(({ worker, ...job }) => {
      let flattenData = flatten(worker);
      const regex = new RegExp(/password/, "ig");
      Object.keys(flattenData)
        .filter((k) => regex.test(k))
        .forEach((k) => delete flattenData[k]);

      return {
        ...job,

        workerData: { ...flattenData },
      };
    }),
  });
});

app.listen(3000, () => {
  logger.info("application listening..... on port 3000");
});
