import { TaskExecutor, WorkContext } from "@golem-sdk/golem-js";
import logger from "npmlog";
import * as path from "node:path";
import fs from "node:fs";
import _ from "lodash";

export interface Checkpointer {
  saveCheckpoint(name: string, result: ExecutableResult): Promise<string[]>;
  doCheckPointExists(name: string): Promise<boolean>;
  readCheckpoint(name: string): Promise<string[]>;
  init(id: string): Promise<void>;
}

export interface Input {
  uploadToProvider(ctx: WorkContext): Promise<string>;
}

const CONCAT_CHAR = "::";

export type ExecutableResult =
  | { type: "urls"; data: string[] }
  | { type: "stdout"; data: string };

export interface Executable {
  run(ctx: WorkContext): Promise<ExecutableResult>;
  fullName: string;
  setPrefix(prefix: string): void;
}

export class FsCheckpointer implements Checkpointer {
  public id!: string;

  constructor(public baseDir: string) {}

  async init(id: string): Promise<void> {
    this.baseDir = path.join(this.baseDir, id);

    if (!fs.existsSync(this.baseDir)) {
      await fs.promises.mkdir(this.baseDir);
    }
  }

  async readCheckpoint(name: string): Promise<string[]> {
    const checkpointPath = path.join(this.baseDir, name);
    return JSON.parse((await fs.promises.readFile(checkpointPath)).toString());
  }

  async saveCheckpoint(name: string, toSave: ExecutableResult) {
    const checkpointPath = path.join(this.baseDir, name);

    if (toSave.type === "stdout") {
      const dataPath = checkpointPath + ".data";
      await fs.promises.writeFile(dataPath, toSave.data);
      await fs.promises.writeFile(checkpointPath, JSON.stringify([dataPath]));
      return [dataPath];
    } else if (toSave.type === "urls") {
      await fs.promises.writeFile(checkpointPath, JSON.stringify(toSave.data));
      return toSave.data;
    } else {
      throw new Error("Unsuporteed executable result");
    }
  }

  async doCheckPointExists(name: string): Promise<boolean> {
    return fs.existsSync(path.join(this.baseDir, name));
  }
}

export class ArgInput implements Input {
  constructor(public values: string[]) {}

  async uploadToProvider(_ctx: WorkContext): Promise<string> {
    return this.values.join(" ");
  }
}

export class FileInput implements Input {
  constructor(private readonly hostPath: string | Promise<string>) {}

  async uploadToProvider(ctx: WorkContext) {
    return await this.uploadFile(ctx, await this.hostPath);
  }

  private async uploadFile(
    ctx: WorkContext,
    hostFilePath: string,
  ): Promise<string> {
    const fileName = path.basename(hostFilePath);
    const dstPath = path.join("/golem/input/", fileName);
    const uploadResult = await retry(
      () => ctx.uploadFile(path.join(__dirname, hostFilePath), dstPath),
      3,
    );

    if (uploadResult.message) {
      throw Error(`Failed to upload ${hostFilePath}: ${uploadResult.message}`);
    }

    logger.info(
      "FileInput",
      `Uploaded requestor:${hostFilePath} to provider:${dstPath}`,
    );

    return dstPath;
  }
}

export class ExecutableToStdout implements Executable {
  prefix: string = "";

  constructor(
    public name: string,
    protected execPath: Input,
    protected inputs: Input[],
    private interpreter?: string,
  ) {}

  get fullName() {
    return `${this.prefix}${CONCAT_CHAR}${this.name}`;
  }

  setPrefix(prefix: string): void {
    this.prefix = prefix;
  }

  getCommand(providerExecPath: string, args: string[]): string {
    if (!this.interpreter) {
      return `chmod +x ${providerExecPath} && chmod 777 ${providerExecPath} && ${providerExecPath} ${args.join(" ")}`;
    }
    return `${this.interpreter} ${providerExecPath} ${args.join(" ")}`;
  }

  async run(ctx: WorkContext) {
    const providerExecPath = await this.execPath.uploadToProvider(ctx);
    const args: string[] = [];

    for (const input of this.inputs) {
      args.push(await input.uploadToProvider(ctx));
    }

    const start = performance.now();
    const result = await ctx.run(this.getCommand(providerExecPath, args));

    logger.info(this.fullName, "stderr", result.stderr);

    if (result.message) {
      logger.error(
        this.fullName,
        `Failed to exectue task:  errorMessage=${result.stdout} duration=${performance.now() - start}[ms]`,
      );
      throw new Error("Failed when executing task");
    } else {
      logger.info(
        this.fullName,
        `Successfully executed task duration=${performance.now() - start}[ms]`,
      );
    }

    return {
      type: "stdout",
      data: result.stdout as string,
    } as ExecutableResult;
  }
}

/**
 * Works only in nodejs
 **/
export class ExecutableToFiles
  extends ExecutableToStdout
  implements Executable
{
  async run(ctx: WorkContext) {
    const result = await super.run(ctx);

    if (result.type !== "stdout") {
      throw new Error("Wrong answer from task, expected json");
    }

    const providerPaths = result.data.split("\n").filter((p) => p !== "");

    // golem-skd fails when we try to fetch in parralel
    const requestorPaths: string[] = [];
    for (const path of providerPaths) {
      const requestorPath = await this.downloadFile(ctx, path);
      requestorPaths.push(requestorPath);
    }

    return { type: "urls", data: requestorPaths } as const;
  }

  private async downloadFile(ctx: WorkContext, pathProvider: string) {
    const requestorPath = path.join(
      "outputs",
      this.fullName,
      path.basename(pathProvider),
    );
    const result = await retry(
      () => ctx.downloadFile(pathProvider, requestorPath),
      3,
    );

    if (result.message) {
      logger.error(
        this.fullName,
        `Failed to download file from provider:${pathProvider} to requestor${requestorPath}:  errorMessage=${result.message}`,
      );
      throw new Error("Failed when executing task");
    } else {
      logger.info(
        this.fullName,
        `Successfully downloaded file from provider:${pathProvider} to requestor:${requestorPath}`,
      );
    }

    return requestorPath;
  }
}

export class Stage {
  checkPointer!: Checkpointer;
  outputs: Record<number, Promise<string[]>> = {};
  private deferedOutputs: Record<string, (data: string[]) => void> = {};

  constructor(
    public name: string,
    public taskExecutor: TaskExecutor,
    public runnables: Executable[],
  ) {
    this.runnables.forEach((r, index) => {
      r.setPrefix(this.fullName);
      const deferedPromise = createDeferedPromise<string[]>();
      this.outputs[index] = deferedPromise.promise;
      this.deferedOutputs[r.fullName] = deferedPromise.resolve;
    });
  }

  get fullName() {
    return `${this.name}`;
  }

  async run() {
    logger.info(this.fullName, `Starting stage `);

    const start = performance.now();
    await Promise.all(this.runnables.map((op) => this.runOperation(op)));

    logger.info(
      this.fullName,
      `Finished stage duration=${performance.now() - start}[ms]`,
    );
  }

  async runOperation(op: Executable) {
    if (await this.checkPointer.doCheckPointExists(op.fullName)) {
      logger.info(op.fullName, `Skipping execution - checkpoint found`);
      this.deferedOutputs[op.fullName](
        await this.checkPointer.readCheckpoint(op.fullName),
      );
      return;
    }

    const result = await this.taskExecutor.run(async (ctx) => {
      return await op.run(ctx);
    });

    // polimorphism possible
    const urls = await this.checkPointer.saveCheckpoint(op.fullName, result);
    this.deferedOutputs[op.fullName](urls);
  }
}

export class Pipeline {
  constructor(
    public id: string,
    public checkPointer: Checkpointer,
    public stages: Stage[],
  ) {
    for (const stage of stages) {
      stage.checkPointer = checkPointer;
    }
  }

  async run() {
    await this.checkPointer.init(this.id);
    const start = performance.now();
    await Promise.all(this.stages.map((s) => s.run()));

    logger.info(
      this.id,
      `Successfully executed pipeline duration=${performance.now() - start}[ms]`,
    );
  }
}

function createDeferedPromise<T>() {
  let resolve: (data: T) => void;
  let reject: (reason?: unknown) => void;

  const promise = new Promise<T>((resolve_, reject_) => {
    resolve = resolve_;
    reject = reject_;
  });

  return { promise, resolve: resolve!, reject: reject! };
}

export function cartesianProductLodash<X, Y>(arr1: X[], arr2: Y[]) {
  return _.flatMap(arr1, (x) => arr2.map((y) => [x, y])) as [X, Y][];
}

async function retry<T extends Function>(fn: T, retries: number) {
  try {
    return await fn();
  } catch (error) {
    if (retries > 0) {
      return retry(fn, retries - 1);
    } else {
      throw error;
    }
  }
}
