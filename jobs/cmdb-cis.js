import { parentPort, workerData } from "node:worker_threads";
import process from "node:process";

console.log("Hello ESM!");
console.log(workerData);
console.log(process.env.SCHEDULE);

// signal to parent that the job is done
if (parentPort) parentPort.postMessage("done");
// eslint-disable-next-line unicorn/no-process-exit
else process.exit(0);
