import { WorkContext } from "@golem-sdk/golem-js";
import logger from "npmlog";
import { retry } from "./helpers";
import * as path from "node:path";
import _ from "lodash";
import { Input } from "./Inputs";

const CONCAT_CHAR = "::";

export type ExecutableResult =
  | { type: "urls"; data: string[] }
  | { type: "stdout"; data: string };

export interface Executable {
  run(ctx: WorkContext): Promise<ExecutableResult>;
  fullName: string;
  setPrefix(prefix: string): void;
}

export class ExecutableToStdout implements Executable {
  prefix: string = "";

  constructor(
    public name: string,
    protected execPath: Input,
    protected inputs: Input[],
    private interpreter?: string
  ) {}

  get fullName() {
    return `${this.prefix}${CONCAT_CHAR}${this.name}`;
  }

  setPrefix(prefix: string): void {
    this.prefix = prefix;
  }

  getCommand(providerExecPath: string, args: string[]): string {
    if (!this.interpreter) {
      return `chmod +x ${providerExecPath} && chmod 777 ${providerExecPath} && ${providerExecPath} ${args.join(
        " "
      )}`;
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
        `Failed to exectue task:  errorMessage=${result.stdout} duration=${
          performance.now() - start
        }[ms]`
      );
      throw new Error("Failed when executing task");
    } else {
      logger.info(
        this.fullName,
        `Successfully executed task duration=${performance.now() - start}[ms]`
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
      path.basename(pathProvider)
    );
    const result = await retry(
      () => ctx.downloadFile(pathProvider, requestorPath),
      3
    );

    if (result.message) {
      logger.error(
        this.fullName,
        `Failed to download file from provider:${pathProvider} to requestor${requestorPath}:  errorMessage=${result.message}`
      );
      throw new Error("Failed when executing task");
    } else {
      logger.info(
        this.fullName,
        `Successfully downloaded file from provider:${pathProvider} to requestor:${requestorPath}`
      );
    }

    return requestorPath;
  }
}
