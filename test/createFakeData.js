import { Client } from "@elastic/elasticsearch";
import { faker } from "@faker-js/faker";
import { Command } from "commander";

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

export function createRandomUser() {
  return {
    userId: faker.string.uuid(),
    username: faker.internet.userName(),
    email: faker.internet.email(),
    avatar: faker.image.avatar(),
    password: faker.internet.password(),
    birthdate: faker.date.birthdate(),
    registeredAt: faker.date.past(),
    group_id: faker.helpers.arrayElement([3, 2, 1]),
    "@timestamp": faker.date.between({
      from: "2023-08-15T07:37:13.493Z",
    }),
  };
}

const program = new Command();
program
  .name("string-util")
  .description("CLI to some JavaScript string utilities")
  .version("0.8.0")
  .option("-d, --docs <int>", 500);

const options = program.parse().opts();

const User = faker.helpers.multiple(createRandomUser, {
  count: options.docs * 1 || 500,
});

console.log("Store ... ");
console.log(User[0]);
const client = await connect();
const result = await client.helpers.bulk({
  datasource: User,
  onDocument(doc) {
    return {
      index: { _index: "users" },
    };
  },
});
