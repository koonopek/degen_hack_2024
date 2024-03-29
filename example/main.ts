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
} from "../src";

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

  const wordsToAnalyze = ["I", "you", "We", "god", "devil", "mother", "father"];
  try {

    // buidling partition by title stage
    const partitionByTitle = new Stage("partition_by", executorJs, [
      new ExecutableToFiles(
        "letter_1",
        new FileInput("golem_tasks/partition_by.js"),
        [
          new ArgInput([wordsToAnalyze.join(",")]),
          new FileInput("inputs/t8.shakespeare-1.txt"),
        ],
        "node"
      ),
      new ExecutableToFiles(
        "letter_2",
        new FileInput("golem_tasks/partition_by.js"),
        [
          new ArgInput([wordsToAnalyze.join(",")]),
          new FileInput("inputs/t8.shakespeare-2.txt"),
        ],
        "node"
      ),
    ]);

    // building stage sentiment
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
                [new FileInput(partitionByTitle.outputs[outputIndex].then((o) => o[wordIndex]))] // task will start as soon as outputs[outputIndex] promise resolves
              ) 
          )
      )
    );

    // building stage reduce
    const reduce = new Stage("reduce", executorJs, [
      new ExecutableToStdout(
        "stats",
        new FileInput("golem_tasks/stats.js"),
        range(Object.values(sentiment.outputs).length).flatMap(
          (i) => new FileInput(sentiment.outputs[i].then((o) => o[0])) // task will start as soon as outputs[outputIndex] promise resolves
        ),
        "node"
      ),
    ]);

    // consolidating into pipeline
    const pipeline = new Pipeline("test-2", new FsCheckpointer("outputs"), [partitionByTitle, sentiment, reduce]);
    
    // trigger all
    await pipeline.run();

    // print results
    printResult(reduce);
  } catch (err) {
    console.error("An error occurred:", err);
  } finally {
    await executorJs.shutdown();
    await executorMultiThreaded.shutdown();
  }
}

function printResult(reduce: Stage) {
  reduce.outputs[0].then((result) => {
    console.log("RESULT RAPORT");
    console.log(
      JSON.stringify(JSON.parse(readFileSync(result[0]).toString()), null, 4)
    );
  });
}

main();
