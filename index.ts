import { Job, Result, TaskExecutor, WorkContext } from "@golem-sdk/golem-js";
import logger from "npmlog";
import * as path from "node:path";
import fs from "node:fs";
import { Readable } from "node:stream";

interface Checkpointer {
  saveCheckpoint(name: string, stream: Readable | string): Promise<string>;
  doCheckPointExists(name: string): Promise<boolean>;
}

interface Input {
  uploadToProvider(ctx: WorkContext): Promise<string>;
}

type ExecutableResult =
  | { type: "urls"; data: string[] }
  | { type: "stdout"; data: string };

interface Executable {
  run(ctx: WorkContext): Promise<ExecutableResult>;
  fullName: string;
  setPrefix(prefix: string): void;
}

export class FsCheckpointer implements Checkpointer {
  constructor(public baseDir = "outputs") {}

  async saveCheckpoint(
    name: string,
    toSave: Readable | string,
  ): Promise<string> {
    const fileName = path.join(this.baseDir, name);
    if (typeof toSave === "string") {
      await fs.promises.writeFile(fileName, toSave, "utf8");
    } else {
      const checkPointFile = fs.createWriteStream(path.join("outputs", name));
      toSave.pipe(checkPointFile);
      toSave.on("end", () => checkPointFile.close());
    }

    return fileName;
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
    const uploadResult = await ctx.uploadFile(
      path.join(__dirname, hostFilePath),
      dstPath,
    );

    if (uploadResult.message) {
      throw Error(`Failed to upload ${hostFilePath}`);
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
    return `${this.prefix}::${this.name}`;
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
    const args = await Promise.all(
      this.inputs.map((i) => i.uploadToProvider(ctx)),
    );

    const start = performance.now();
    const result = await ctx.run(this.getCommand(providerExecPath, args));

    logger.info(this.fullName, "stderr", result.stderr);
    logger.info(this.fullName, "stdout", result.stdout);

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

    const paths = result.data.split("\n").filter((p) => p !== "");

    // golem-skd fails when we try to fetch in parralel
    for (const path of paths) {
      await this.downloadFile(ctx, path);
    }

    return { type: "urls", data: paths } as const;
  }

  private async downloadFile(ctx: WorkContext, pathProvider: string) {
    const requestorPath = path.join("outputs", pathProvider);
    console.log(requestorPath);
    const result = await ctx.downloadFile(pathProvider, requestorPath);

    if (result.message) {
      logger.error(
        this.fullName,
        `Failed to download file from provider:${pathProvider} to requestor${requestorPath}:  errorMessage=${result.message}`,
      );
      throw new Error("Failed when executing task");
    } else {
      logger.info(
        this.fullName,
        `Successfully downloaded file from provider:${pathProvider} to requestor${requestorPath}`,
      );
    }
  }
}

export class Stage {
  prefix = "";
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
      const deferedPromise = createDeferredPromise<string[]>();
      this.outputs[index] = deferedPromise.promise;
      this.deferedOutputs[r.fullName] = deferedPromise.resolve;
    });
  }

  setPrefix(prefix: string) {
    this.prefix = prefix;
  }

  get fullName() {
    return `${this.prefix}::${this.name}`;
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
      return;
    }

    const result = await this.taskExecutor.run(async (ctx) => {
      return await op.run(ctx);
    });

    // polimorphism possible
    if (result.type === "urls") {
      await this.checkPointer.saveCheckpoint(
        op.fullName,
        result.data.join(","),
      );
      this.deferedOutputs[op.fullName](result.data);
    } else {
      await this.checkPointer.saveCheckpoint(op.fullName, result.data);
      this.deferedOutputs[op.fullName]([result.data]);
    }
  }
}

function createDeferredPromise<T>() {
  let resolve!: (data: T) => void;

  const promise = new Promise<T>((res) => {
    resolve = res;
  });

  return {
    promise,
    resolve,
  };
}
