import {
  Worker,
  setEnvironmentData,
}           from "node:worker_threads";

export const RedlockWorker = (REDIS_URL: string): Worker => {
  setEnvironmentData('REDIS_URL', REDIS_URL);
  return new Worker(require.resolve('./redlock_worker.js'));
};

export { default } from './redlock'
export *           from './redlock';
