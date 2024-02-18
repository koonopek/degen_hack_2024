import { WorkContext } from "@golem-sdk/golem-js";
import * as path from "node:path";
import logger from "npmlog";
import { retry } from "./helpers";

export interface Input {
  uploadToProvider(ctx: WorkContext): Promise<string>;
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
    hostFilePath: string
  ): Promise<string> {
    const fileName = path.basename(hostFilePath);
    const dstPath = path.join("/golem/input/", fileName);
    const uploadResult = await retry(
      () => ctx.uploadFile(path.join(__dirname, hostFilePath), dstPath),
      3
    );

    if (uploadResult.message) {
      throw Error(`Failed to upload ${hostFilePath}: ${uploadResult.message}`);
    }

    logger.info(
      "FileInput",
      `Uploaded requestor:${hostFilePath} to provider:${dstPath}`
    );

    return dstPath;
  }
}
