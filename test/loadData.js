import { Client } from "@elastic/elasticsearch";
import { Command } from "commander";
import * as fs from "node:fs";

const connect = async (instance = "SOURCE") => {
  try {
    const client = new Client({
      node: "https://localhost:9200",
      auth: {
        username: process.env.user,
        password: process.env.password,
      },
      tls: {
        rejectUnauthorized: false,
      },
    });

    return client;
  } catch (e) {
    console.log(e.message);
    throw new Error("Ups");
  }
};

const program = new Command();
program
  .name("string-util")
  .description("CLI to some JavaScript string utilities")
  .version("0.8.0")
  .requiredOption("-i, --input <file>");

const options = program.parse().opts();
if (!fs.existsSync(options.input)) {
  throw new Error(`cannot open file ${options.input}`);
}

const data = JSON.parse(fs.readFileSync(options.input, "utf-8"));

const client = await connect();
const result = await client.helpers.bulk({
  datasource: data.map(({ _source, _id }) => ({ ..._source, _id })),
  onDocument(doc) {
    const _id = doc._id;
    delete doc._id;
    return {
      index: { _index: "cmdb-cis", _id },
    };
  },
});
