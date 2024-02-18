import { TaskExecutor } from "@golem-sdk/golem-js";
import logger from "npmlog";
import { Checkpointer } from "./Checkpointers";
import { Executable } from "./Executables";
import { createDeferedPromise } from "./helpers";

export class Stage {
  checkPointer!: Checkpointer;
  outputs: Record<number, Promise<string[]>> = {};
  private deferedOutputs: Record<string, (data: string[]) => void> = {};

  constructor(
    public name: string,
    public taskExecutor: TaskExecutor,
    public runnables: Executable[]
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
      `Finished stage duration=${performance.now() - start}[ms]`
    );
  }

  async runOperation(op: Executable) {
    if (await this.checkPointer.doCheckPointExists(op.fullName)) {
      logger.info(op.fullName, `Skipping execution - checkpoint found`);
      this.deferedOutputs[op.fullName](
        await this.checkPointer.readCheckpoint(op.fullName)
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
