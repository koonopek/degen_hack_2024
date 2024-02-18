import * as path from "node:path";
import fs from "node:fs";
import _ from "lodash";
import { ExecutableResult } from "./Executables";

export interface Checkpointer {
  saveCheckpoint(name: string, result: ExecutableResult): Promise<string[]>;
  doCheckPointExists(name: string): Promise<boolean>;
  readCheckpoint(name: string): Promise<string[]>;
  init(id: string): Promise<void>;
}

export class FsCheckpointer implements Checkpointer {
  public id!: string;

  constructor(public baseDir: string) {}

  async init(id: string): Promise<void> {
    this.baseDir = path.join(this.baseDir, id);

    if (!fs.existsSync(this.baseDir)) {
      await fs.promises.mkdir(this.baseDir,{recursive:true});
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
