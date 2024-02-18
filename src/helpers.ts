export function createDeferedPromise<T>() {
  let resolve: (data: T) => void;
  let reject: (reason?: unknown) => void;

  const promise = new Promise<T>((resolve_, reject_) => {
    resolve = resolve_;
    reject = reject_;
  });

  return { promise, resolve: resolve!, reject: reject! };
}

export async function retry<T extends Function>(fn: T, retries: number) {
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
