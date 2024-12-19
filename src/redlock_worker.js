import {
  parentPort,
  MessageChannel,
  getEnvironmentData,
}              from 'node:worker_threads'
import Redis   from 'ioredis';
import Redlock from './redlock';

const redis = new Redis(getEnvironmentData('REDIS_URL'));
const redlock = new Redlock([redis], {
  retryCount: 10,
  retryDelay: 500,
});

// We have a locks cache to access them from unlock and extend methods, and
// also to avoid locking the same UUID twice (mistake more likely than a collision)
const redlock_cache = new Map();

// Tries to acquire the lock and indefinitely extends (or max_extensions) it until the main thread
// calls unlock
// The max_extensions setting is necessary when not using a lockedSection, but locking and unlocking
// in different sections where one can not be totally sure that 'unlock' will eventually be called
const attempLock = async (port, { lock_id, locks, duration, max_extensions = null, settings = {} }) => {
  // Avoid locking the same UUID twice, as that is more than likely a mistake by the main thread
  if (redlock_cache.has(lock_id)) {
    port.postMessage({ action: 'lock', result: 'fail', error: new Error(`Lock id ${lock_id} already exists`) });
    port.close();
    return;
  }
  const extension_threshold = settings.automaticExtensionThreshold || redlock.settings.automaticExtensionThreshold;
  if (duration - extension_threshold < 100) {
    throw new Error('Lock `duration` must be at least 100ms greater than the `automaticExtensionThreshold` setting');
  }
  try {
    // Try to lock the resource
    const lock = await redlock.acquire(locks, duration, settings);

    // Create a new channel to send any failure during lock extension and pass it to main
    const { port1: worker_port_extend, port2: main_port_extend } = new MessageChannel();
    port.postMessage({ action: 'lock', result: 'success', data: { lock_id, extend_port: main_port_extend } }, [main_port_extend]);
    port.close();

    // We have to manage the lock extension manually
    const interval = setInterval(async () => {
      try {
        // This could happen if releasing in a race condition between interval creation and the
        // clearInterval, should not affect the lock safety
        if (!redlock_cache.has(lock_id)) return;
        const { lock, interval, number_of_extensions } = redlock_cache.get(lock_id);
        if (max_extensions && number_of_extensions >= max_extensions)
          throw new Error(`Maximum number of extensions (${max_extensions}) reached`);

        const new_lock = await lock.extend(duration);
        redlock_cache.set(lock_id, { lock: new_lock, interval, number_of_extensions: ++number_of_extensions });
      } catch (err) {
        // Extending the lock failed, so we clear the interval here and message the main
        // thread that the locked section is no longer safe
        worker_port_extend.postMessage({ action: 'extend', result: 'fail', error: err });
        worker_port_extend.close();
        clearInterval(interval);
      }
    }, duration - extension_threshold);

    // Finally, add the lock to the cache, with the interval to clear when necessary
    redlock_cache.set(lock_id, { interval, lock, number_of_extensions: 0 });
  } catch (err) {
    port.postMessage({ action: 'lock', result: 'fail', error: err });
    port.close();
  }
};

const attempUnlock = async (port, { lock_id }) => {
  if (!redlock_cache.has(lock_id)) {
    port.postMessage({ action: 'unlock', result: 'fail', error: new Error(`Lock id ${lock_id} does not exist`) });
    port.close();
    return;
  }
  try {
    // Release the lock and clear the extending interval
    const { lock, interval } = redlock_cache.get(lock_id);
    await redlock.release(lock);
    clearInterval(interval);
    redlock_cache.delete(lock_id);

    port.postMessage({ action: 'unlock', result: 'success' });
    port.close();
  } catch (err) {
    port.postMessage({ action: 'unlock', result: 'fail', error: err });
    port.close();
  }
};

parentPort.on('message', ({ port, action, data }) => {
  switch (action) {
    case 'lock':
      attempLock(port, data);
      return;
    case 'unlock':
      attempUnlock(port, data);
      return;
    default:
      console.log(`[WORKER] Handler for action ${action} does not exists`);
  }
});
