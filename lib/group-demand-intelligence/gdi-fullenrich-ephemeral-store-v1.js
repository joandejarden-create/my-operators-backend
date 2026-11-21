/**
 * Short-lived in-memory FullEnrich result store for GDI.
 * Never writes email/phone to disk, Airtable, or logs.
 * Explicit TTL + delete on read/expiry. Drawer-close safe.
 */

export const GDI_FULLENRICH_EPHEMERAL_STORE_V1 =
  "gdi-fullenrich-ephemeral-store-v1";

const DEFAULT_TTL_MS = 5 * 60 * 1000;

/** @type {Map<string, { expiresAt: number, value: object }>} */
const store = new Map();
/** @type {Map<string, { startedAt: number, enrichmentId: string|null }>} */
const inFlight = new Map();

function now() {
  return Date.now();
}

function purgeExpired() {
  const t = now();
  for (const [k, row] of store.entries()) {
    if (!row || row.expiresAt <= t) store.delete(k);
  }
}

export function fullEnrichEphemeralKey({
  hotelId,
  opportunityId,
  contactName,
  channel = "email",
} = {}) {
  return [
    String(hotelId || "").trim(),
    String(opportunityId || "").trim(),
    String(contactName || "")
      .trim()
      .toLowerCase(),
    String(channel || "email").toLowerCase(),
  ].join("|");
}

export function putFullEnrichEphemeral(key, value, ttlMs = DEFAULT_TTL_MS) {
  purgeExpired();
  const k = String(key || "").trim();
  if (!k) return false;
  store.set(k, {
    expiresAt: now() + Math.max(1000, Number(ttlMs) || DEFAULT_TTL_MS),
    value: value && typeof value === "object" ? value : {},
  });
  return true;
}

export function getFullEnrichEphemeral(key, { consume = false } = {}) {
  purgeExpired();
  const k = String(key || "").trim();
  const row = store.get(k);
  if (!row) return null;
  if (row.expiresAt <= now()) {
    store.delete(k);
    return null;
  }
  if (consume) store.delete(k);
  return row.value;
}

export function deleteFullEnrichEphemeral(key) {
  store.delete(String(key || "").trim());
}

export function beginFullEnrichInFlight(key, enrichmentId = null) {
  purgeExpired();
  const k = String(key || "").trim();
  if (!k) return { ok: false, error: "INVALID_KEY" };
  if (inFlight.has(k)) {
    return {
      ok: false,
      error: "LOOKUP_IN_FLIGHT",
      message:
        "A FullEnrich lookup is already in progress for this contact. Wait for it to finish.",
      enrichmentId: inFlight.get(k)?.enrichmentId || null,
    };
  }
  inFlight.set(k, { startedAt: now(), enrichmentId: enrichmentId || null });
  return { ok: true };
}

export function endFullEnrichInFlight(key) {
  inFlight.delete(String(key || "").trim());
}

export function isFullEnrichInFlight(key) {
  return inFlight.has(String(key || "").trim());
}

export function clearFullEnrichEphemeralStoreForTests() {
  store.clear();
  inFlight.clear();
}
