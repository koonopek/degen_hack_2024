import { TaskExecutor } from "@golem-sdk/golem-js";
import _, { range } from "lodash";
import { readFileSync } from "node:fs";
import {
  ArgInput,
  ExecutableToFiles,
  ExecutableToStdout,
  FileInput,
  FsCheckpointer,
  Pipeline,
  Stage,
} from ".";

async function main() {
  const executorJs = await TaskExecutor.create({
    package: "golem/node:20-alpine",
    yagnaOptions: { apiKey: "try_golem" },
    maxTaskRetries: 2,
    minCpuCores: 1,
    minCpuThreads: 1,
    minMemGib: 1,
  });

  const executorMultiThreaded = await TaskExecutor.create({
    package: "golem/alpine:3.18.2",
    proposalFilter: (proposal) => {
      return proposal.properties["golem.inf.cpu.architecture"] === "x86_64";
    },
    yagnaOptions: { apiKey: "try_golem" },
    maxTaskRetries: 2,
    minCpuCores: 2,
    minMemGib: 1,
  });

  try {
    const wordsToAnalyze = [
      "I",
      "you",
      "We",
      "god",
      "devil",
      "mother",
      "father",
    ];

    const partitionByTitle = new Stage("partition_by", executorJs, [
      new ExecutableToFiles(
        "letter_1",
        new FileInput("golem_tasks/partition_by.js"),
        [
          new ArgInput([wordsToAnalyze.join(",")]),
          new FileInput("inputs/t8.shakespeare-1.txt"),
        ],
        "node",
      ),
      new ExecutableToFiles(
        "letter_2",
        new FileInput("golem_tasks/partition_by.js"),
        [
          new ArgInput([wordsToAnalyze.join(",")]),
          new FileInput("inputs/t8.shakespeare-2.txt"),
        ],
        "node",
      ),
    ]);

    const sentiment = new Stage(
      "sentiment",
      executorMultiThreaded,
      range(Object.values(partitionByTitle.outputs).length).flatMap(
        (outputIndex) =>
          range(wordsToAnalyze.length).flatMap(
            (wordIndex) =>
              new ExecutableToStdout(
                `sentiment_${outputIndex}_${wordsToAnalyze[wordIndex]}`,
                new FileInput("golem_tasks/go/main"),
                [
                  new FileInput(
                    partitionByTitle.outputs[outputIndex].then(
                      (o) => o[wordIndex],
                    ),
                  ),
                ],
              ),
          ),
      ),
    );

    const reduce = new Stage("reduce", executorJs, [
      new ExecutableToStdout(
        "stats",
        new FileInput("golem_tasks/stats.js"),
        range(Object.values(sentiment.outputs).length).flatMap(
          (i) => new FileInput(sentiment.outputs[i].then((o) => o[0])),
        ),
        "node",
      ),
    ]);

    const pipeline = new Pipeline("test-2", new FsCheckpointer("outputs"), [
      partitionByTitle,
      sentiment,
      reduce,
    ]);

    await pipeline.run();

    reduce.outputs[0].then((result) => {
      console.log("RESULT RAPORT");
      console.log(
        JSON.stringify(JSON.parse(readFileSync(result[0]).toString()), null, 4),
      );
    });
  } catch (err) {
    console.error("An error occurred:", err);
  } finally {
    await executorJs.shutdown();
    await executorMultiThreaded.shutdown();
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
//An error occurred: GolemWorkError: Unable to execute task. Error: Failed to upload outputs/test-2/sentiment::sentiment_01.data: Local service error: State error: Busy: StatePair(Ready, Some(Ready))

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
