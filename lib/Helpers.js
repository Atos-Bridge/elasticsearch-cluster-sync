import { SocksProxyAgent } from "socks-proxy-agent";
import * as es8 from "es8";
import * as es7 from "es7";
import _ from "lodash";
import * as fs from "node:fs";
import fingerPrint from "fprint";
import { parentPort, workerData } from "node:worker_threads";

const clients = {
  es7,
  es8,
};

export const Logger = (parentPort) => ({
  info: (message) =>
    parentPort.postMessage({
      message: `type: ${workerData.job_type}, message: ${message}`,
      level: "info",
    }),
  debug: (message) =>
    parentPort.postMessage({
      message: `type: ${workerData.job_type}, message: ${message}`,
      level: "debug",
    }),
  warning: (message) =>
    parentPort.postMessage({
      message: `type: ${workerData.job_type}, message: ${message}`,
      level: "warning",
    }),
  error: (message) =>
    parentPort.postMessage({
      message: `type: ${workerData.job_type}, message: ${message}`,
      level: "error",
    }),
});
export const sha256Check = async (file) => {
  let sha256 = "";
  let currentSha256 = "";
  let sha256File = `${file}.sha256`;
  if (fs.existsSync(file)) {
    if (fs.existsSync(sha256File)) {
      currentSha256 = fs.readFileSync(sha256File, "utf-8");
    }

    sha256 = await fingerPrint.createFingerprint(file, "sha256");

    if (sha256 === currentSha256) {
      return false;
    } else {
      fs.writeFileSync(sha256File, sha256);
      return true;
    }
  }
  return true;
};

export const measureTime = () => {
  let mark;
  const seconds = 1000;
  const minutes = 60000;
  const init = () => {
    let startTime = 0;
    let endTime = 0;
    mark = {
      startTime,
      endTime,
      started: false,
      elapsed: null,
      unit: "ms",
      timestamp: null,
    };
  };

  const toSeconds = () => {
    mark.unit = "s";
    return Math.round(((mark.endTime - mark.startTime) / 1000) * 100) / 100;
  };

  const toMinutes = () => {
    mark.unit = "m";
    return Math.round(((mark.endTime - mark.startTime) / 60000) * 100) / 100;
  };

  const toMs = () => {
    mark.unit = "ms";
    return Math.round((mark.endTime - mark.startTime) * 100) / 100;
  };

  return {
    start: () => {
      init();
      mark.startTime = performance.now();
      mark.started = true;
      mark.startDate = new Date().toISOString();
      mark.timestamp = new Date().toISOString();
      return mark;
    },
    end: (unit = "ms") => {
      if (mark.started === false)
        throw new Error("measureTime as not been started.");

      mark.endTime = performance.now();
      mark.timestamp = new Date().toISOString();
      mark.endDate = new Date().toISOString();

      switch (unit) {
        case "auto":
          let ms = mark.endTime - mark.startTime;
          if (ms > minutes) {
            mark.elapsed = toMinutes();
          } else if (ms > seconds) {
            mark.elapsed = toSeconds();
          } else {
            mark.elapsed = toMs();
          }
          break;
        case "m":
          mark.elapsed = toMinutes();
          break;
        case "s":
          mark.elapsed = toSeconds();
          break;
        default:
          mark.elapsed = toMs();
      }

      return mark;
    },
  };
};

export const connect = async (instance = "SOURCE", workerData, logger) => {
  const esClientVersion =
    workerData.ELASTICSEARCH[instance].ES_CLIENT_VERSION || "es8";
  logger.info(`Try to use ${esClientVersion} for ${instance}`);
  const esClient = _.get(clients, esClientVersion, es8);

  const opts = {
    node: workerData.ELASTICSEARCH[instance].URL,
    auth: {
      username: workerData.ELASTICSEARCH[instance].USERNAME,
      password: workerData.ELASTICSEARCH[instance].PASSWORD,
    },
  };
  if (!_.isNil(workerData.ELASTICSEARCH[instance].SOCKS_PROXY)) {
    opts.agent = () =>
      new SocksProxyAgent(workerData.ELASTICSEARCH[instance].SOCKS_PROXY);
    opts.Connection = es8.HttpConnection;
  }
  opts[`${esClientVersion === "es8" ? "tls" : "ssl"}`] = {
    rejectUnauthorized: false,
  };

  try {
    const e = new esClient.Client(opts);
    logger.info("Connected to:" + workerData.ELASTICSEARCH[instance].URL);
    return e;
  } catch (e) {
    logger.error(
      "Cannot connect to: " + workerData.ELASTICSEARCH[instance].URL,
    );
    logger.debug(JSON.stringify(opts, null, 4));
    logger.error(e.message);
    throw e;
  }
};
