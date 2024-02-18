import { TaskExecutor } from "@golem-sdk/golem-js";
import {
  ArgInput,
  ExecutableToFiles,
  ExecutableToStdout,
  FileInput,
  FsCheckpointer,
  Stage,
} from ".";

async function main() {
  const executorJs = await TaskExecutor.create({
    package: "golem/node:20-alpine",
    yagnaOptions: { apiKey: "try_golem" },
    maxTaskRetries: 0,
    minCpuCores: 1,
    minCpuThreads: 1,
    minMemGib: 1,
  });

  // const executorMultiThreaded = await TaskExecutor.create({
  //   package: "golem/alpine:3.18.2",
  //   proposalFilter: (proposal) => {
  //     return proposal.properties["golem.inf.cpu.architecture"] === "x86_64";
  //   },
  //   yagnaOptions: { apiKey: "try_golem" },
  //   maxTaskRetries: 0,
  //   minCpuCores: 4,
  //   minCpuThreads: 8,
  //   minMemGib: 1,
  // });
  try {
    const checkpointer = new FsCheckpointer();

    const partitionByTitle = new Stage("partition_by_letter", executorJs, [
      new ExecutableToFiles(
        "I",
        new FileInput("golem_tasks/partition_by.js"),
        [
          new ArgInput(["I,you,We"]),
          new FileInput("inputs/t8.shakespeare-1.txt"),
        ],
        "node",
      ),
    ]);

    // const analyzeSentiment = new Stage("sentiment",

    partitionByTitle.checkPointer = checkpointer;
    await partitionByTitle.run();
  } catch (err) {
    console.error("An error occurred:", err);
  } finally {
    await executorJs.shutdown();
  }
}

main();
//
//        I                you                all
// analyze sentiment  analyze sentiment      sentiment
//       sum                sum
//                          stats

// budowanie reuzywlnych klockow na wyzszmy poziome
// latwo przeniesc do przegladarki i udostepnic innym i pozwolic budowac z insitejacych klockow
// deklaratywne podejscie
// latwiej analizowac kod

// issues streams doesnt work
// beforeEach doesnt work so redundant downloads

// url: http://127.0.0.1:7465/ya-client/#/
// what is app-key
// type Input = "file";
// type Output = "file" | "byteStream";

// new Pipeline([
//   new Stage([fileChunk1, fileChunk2, fileChunk3]),
//   [partitionBy, partitionBy, partitionBy],
//   [checkPoint],
//   new Stage([Chunk]),
//   [hash, hash, hash],
//   [checkPoint],
// ]);
