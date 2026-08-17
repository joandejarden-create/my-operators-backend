/**
 * Production Census scalable queue registry + runner scaffolding.
 * Queues are confidence-gated; Brand Explorer / owner / dates stay forbidden.
 */

import { ROOMS_KEYS_QUEUE } from "./production-census-rooms-keys-queue.js";

export const QUEUE_ENGINE_VERSION = "production-census-queue-engine-v1";

/** @type {Record<string, object>} */
export const CENSUS_QUEUES = Object.freeze({
  rooms_keys_missing: ROOMS_KEYS_QUEUE,
});

export function listCensusQueues() {
  return Object.keys(CENSUS_QUEUES).map((id) => ({
    id,
    name: CENSUS_QUEUES[id].name,
    purpose: CENSUS_QUEUES[id].purpose,
    early: Boolean(CENSUS_QUEUES[id].early),
  }));
}

export function getCensusQueue(queueId) {
  const q = CENSUS_QUEUES[queueId];
  if (!q) {
    throw new Error(
      `Unknown census queue "${queueId}". Available: ${Object.keys(CENSUS_QUEUES).join(", ")}`
    );
  }
  return q;
}

export function parseQueueRunArgs(argv = process.argv.slice(2)) {
  const get = (flag, fallback = null) => {
    const i = argv.indexOf(flag);
    if (i >= 0 && argv[i + 1] && !argv[i + 1].startsWith("--")) return argv[i + 1];
    return fallback;
  };
  const getNum = (flag, fallback) => {
    const n = Number(get(flag, String(fallback)));
    return Number.isFinite(n) ? n : fallback;
  };
  const queue = get("--queue");
  return {
    queue,
    dryRun: argv.includes("--dry-run") || !argv.includes("--apply"),
    apply: argv.includes("--apply"),
    limit: getNum("--limit", 100),
    listQueues: argv.includes("--list-queues"),
    confirms: {
      targeted: argv.includes("--confirm-targeted-queue-apply"),
      roomsKeysOnly: argv.includes("--confirm-rooms-keys-only"),
      officialOnly: argv.includes("--confirm-official-source-room-counts-only"),
      noMixedUse: argv.includes("--confirm-no-mixed-use-unit-confusion"),
      noOwner: argv.includes("--confirm-no-owner-operator-writes"),
      noDates: argv.includes("--confirm-no-date-writes"),
      noBe: argv.includes("--confirm-no-brand-explorer-writes"),
    },
  };
}
