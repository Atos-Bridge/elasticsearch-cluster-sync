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
import { el } from "@faker-js/faker";
import { workerData } from "node:worker_threads";
const flatten = require("flat");
const pino = require("pino");
const getPackage = () => {
  const file = `${process.cwd()}/package.json`;
  return JSON.parse(fs.readFileSync(file));
};

const Package = getPackage();

const program = new Command();
program
  .name("elasticsearch-cluster-sync")
  .description("CLI to some JavaScript string utilities")
  .version("1.0.0")
  .addOption(
    new Option("-l, --level <loglevel>", "default info")
      .default("info")
      .choices(_.keys(pino.levels.values)),
  );
const options = program.parse().opts();

const logger = Logger(options);
const jobConfig = Config.transform(options);

if (_.keys(jobConfig).length === 0) {
  throw new Error("No jobs loaded ...! You have to set jobs as environment.");
}
const jobsAll = _.flatten(
  _.keys(_.get(jobConfig, "JOB", {})).map((job) => {
    let jobs = jobConfig.JOB[job];
    return _.keys(jobs).map((instance_name) => {
      const instance = jobs[instance_name];
      const elkConfig = _.get(instance, ["WORKER", "ELASTICSEARCH"]);
      if (!("TARGET" in elkConfig) || !("SOURCE" in elkConfig))
        throw new Error(
          "BAD CONFIGURATION: ELASTICSEARCH - TARGET/SOURCE not found!",
        );
      _.keys(elkConfig).map((direction) => {
        if ("INSTANCE" in elkConfig[direction]) {
          const elkInstance = _.get(jobConfig, [
            "ELASTICSEARCH",
            elkConfig[direction].INSTANCE,
          ]);
          if (!elkInstance)
            throw new Error(
              `BAD CONFIGURATION: INSTANCE ${elkConfig[direction].INSTANCE} not found!`,
            );
          _.keys(elkInstance).forEach((k) => {
            _.set(
              instance,
              ["WORKER", "ELASTICSEARCH", direction, k],
              elkInstance[k],
            );
          });
        }
      });

      return {
        name: [job.toLowerCase(), instance_name.toLowerCase()].join("."),
        outputWorkerMetadata: true,
        interval: instance.SCHEDULE.INTERVAL || instance.SCHEDULE.CRON,
        worker: {
          workerData: {
            IS_ACTIVE: _.get(instance, "ACTIVE", true),
            ...instance.WORKER,
            Package,
            job_type: job.toLowerCase(),
            job_name: instance_name.toLowerCase(),
          },
        },
      };
    });
  }),
);

const jobs = jobsAll.filter((job) =>
  _.get(job, ["worker", "workerData", "IS_ACTIVE"]),
);

if (jobs.length === 0 && jobsAll.length > 0) {
  throw new Error("No Active JOBS found!");
} else if (jobsAll.length === 0) {
  throw new Error("No JOBS found!");
}
logger.debug(JSON.stringify(jobs, null, 4));

const app = express();
const bree = new Bree({
  logger: logger,
  outputWorkerMetadata: true,
  workerMessageHandler: ({ name, message, worker } = {}) => {
    if (_.isPlainObject(message)) {
      let { level, message: text } = message;
      text = `job: ${name}, ${text}`;
      if (_.has(logger, level)) {
        logger[level](text);
      } else {
        logger.info(text);
      }
    } else {
      logger.info(`job: ${name}, ${message}`);
    }
  },
  jobs,
  errorHandler: (error, workerMetadata) => {
    // workerMetadata will be populated with extended worker information only if
    // Bree instance is initialized with parameter `workerMetadata: true`
    if (workerMetadata.threadId) {
      logger.info(
        `There was an error while running a worker ${workerMetadata.name} with thread ID: ${workerMetadata.threadId}`,
      );
    } else {
      logger.info(
        `There was an error while running a worker ${workerMetadata.name}`,
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
