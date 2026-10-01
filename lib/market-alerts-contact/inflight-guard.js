/**
 * In-flight idempotency for on-demand Surfe actions.
 * Memory/request-scoped only — never persists Surfe results.
 */

const inflight = new Map();

function keyOf(alertId, stakeholderId, action) {
  return `${String(alertId || "")}::${String(stakeholderId || "")}::${String(action || "")}`;
}

/**
 * @param {string} alertId
 * @param {string} stakeholderId
 * @param {"FIND_PERSON"|"GET_CONTACT_DETAILS"} action
 * @param {() => Promise<any>} fn
 */
export async function withInflightGuard(alertId, stakeholderId, action, fn) {
  const key = keyOf(alertId, stakeholderId, action);
  const existing = inflight.get(key);
  if (existing) return existing;

  const promise = (async () => {
    try {
      return await fn();
    } finally {
      // Drop after completion — do not retain Surfe contact data
      inflight.delete(key);
    }
  })();

  inflight.set(key, promise);
  return promise;
}

export function clearInflightGuardsForTests() {
  inflight.clear();
}

export function inflightGuardSizeForTests() {
  return inflight.size;
}
