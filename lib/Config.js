import * as _ from "lodash-es";
import * as fs from "node:fs";
import Logger from "../lib/Logger.js";

const logger = Logger({ appName: "Config" });

export default {
  obj: {},
  transform: function ({ level = "info" } = {}) {
    logger.info("Transform env ...");
    logger.level = level;
    const obj = {};
    Object.keys(process.env)
      .filter((e) => e.startsWith("JOB."))
      .map((env) => {
        let attr = env;
        let value = process.env[env];
        if (env.toLowerCase().endsWith("_file")) {
          value = this.readFromFile(value);
          attr = attr.replace(/\_file$/i, "");
        }
        _.set(obj, `${attr}`, value);
      });
    this.obj = obj;
    logger.debug(JSON.stringify(obj, null, 4));
    return obj;
  },
  readFromFile: function (file) {
    try {
      return fs.readFileSync(file, "utf8").trim();
    } catch (err) {
      logger.error(`Cannot read file ${file}`);
      throw err;
    }
  },
};
