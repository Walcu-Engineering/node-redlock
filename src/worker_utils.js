import crypto from 'node:crypto';
import {
  Worker,
  setEnvironmentData,
  MessageChannel,
}             from "node:worker_threads";

export const freeLock = worker => async ({ lock_id }) => {
  // Another exclusive channel for unlocking
  const { port1: worker_port_unlock, port2: main_port_unlock } = new MessageChannel();
  worker.postMessage({ port: worker_port_unlock, request_id: lock_id, action: 'unlock', data: {
    lock_id,
  }}, [worker_port_unlock]);
  const unlock_result = await (new Promise((res) => main_port_unlock.on('message', res)));
  if (unlock_result.result === 'fail') {
    throw unlock_result.error;
  }
  return unlock_result;
};

export const getLock = worker => async (locks, duration, settings) => {

  const lock_id = settings?.lock_id ?? crypto.randomUUID();

  // worker.postMessage is in a way a broadcast, it uses a channel shared between all
  // lockedSection usages, so we create a new channel that will be used only for this lock
  const { port1: worker_port_lock, port2: main_port_lock } = new MessageChannel();

  // We pass the worker_port for the worker to respond on it instead of the shared channel
  worker.postMessage({ port: worker_port_lock, request_id: lock_id, action: 'lock', data: {
    lock_id,
    locks,
    duration,
    settings,
    max_extensions: settings?.max_extensions,
  }}, [worker_port_lock]);

  // Promisify the onMessage and just get the first one, either it succedded or failed,
  // but as the channel is unique, no other messages could be sent here
  const lock_result = await new Promise((res) => main_port_lock.on('message', res));

  // If locking fails, propagate the error
  if (lock_result.result === 'fail') {
    throw lock_result.error;
  }

  // Add these two properties to create a lock like interface for ease of use
  lock_result.lock_id = lock_id;
  lock_result.release = () => freeLock(worker)({ lock_id });

  return lock_result;
};

export const generateLockedSection = worker => async (locks, duration = 1000, settingsOrCallback, optionalCallback) => {

  const settings = typeof settingsOrCallback === "function" ? {} : settingsOrCallback;
  const callback = optionalCallback ?? settingsOrCallback;

  // Generate a random identifier that will be used internally for locking and unlocking
  const lock_id = crypto.randomUUID();

  // Get the lock
  const lock_result = await getLock(worker)(locks, duration, { ...settings, lock_id });

  // Create an AbortController for stopping the callback in the case the lock fails to extend
  const controller = new AbortController();

  // The lock action also returns a new channel where it will send errors during lock extension
  // In that case, just abort the controller
  lock_result.data.extend_port.on('message', () => {
    controller.abort();
  })

  try {
    // Execute the callback propagating the abort controller signal
    return await callback(controller.signal);
  } finally {
    // We do not care if the callback crashes here, just make sure the lock is unlocked
    await freeLock(worker)({ lock_id });
  }
};

export const RedlockWorker = (REDIS_URL) => {
  setEnvironmentData('REDIS_URL', REDIS_URL);
  return new Worker(require.resolve('./redlock_worker'));
};

export const RedlockWithWorker = (REDIS_URL) => {
  const redlock_worker = RedlockWorker(REDIS_URL);
  return {
    worker: redlock_worker,
    acquire: getLock(redlock_worker),
    release: freeLock(redlock_worker),
    using: generateLockedSection(redlock_worker),
  }
}
