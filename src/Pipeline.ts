import logger from "npmlog";
import { Checkpointer } from "./Checkpointers";
import { Stage } from "./Stage";

export class Pipeline {
  constructor(
    public id: string,
    public checkPointer: Checkpointer,
    public stages: Stage[]
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
      `Successfully executed pipeline duration=${performance.now() - start}[ms]`
    );
  }

  async visualize() {}
}
