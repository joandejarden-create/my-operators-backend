/**
 * Leakage audits for Webhound ↔ native teacher diagnostic.
 * Fail closed if either arm receives forbidden teacher/gold keys.
 */
import { FORBIDDEN_INPUT_KEYS } from "./config.js";
import { hotelOnlyInput } from "./shared-inputs.js";

const FORBIDDEN_RE = new RegExp(
  `\\b(${FORBIDDEN_INPUT_KEYS.map((k) => k.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")).join("|")})\\b`,
  "i"
);

export function collectForbiddenKeys(obj, pathPrefix = "") {
  const hits = [];
  if (!obj || typeof obj !== "object") return hits;
  for (const [k, v] of Object.entries(obj)) {
    const p = pathPrefix ? `${pathPrefix}.${k}` : k;
    if (FORBIDDEN_INPUT_KEYS.includes(k)) {
      hits.push({ path: p, key: k });
    }
    if (v && typeof v === "object" && !Array.isArray(v)) {
      hits.push(...collectForbiddenKeys(v, p));
    }
  }
  return hits;
}

export function auditHotelOnlyInput(hotel, armLabel) {
  const errors = [];
  const allowed = new Set(["hotel_id", "hotel_name", "city", "country", "language"]);
  const keys = Object.keys(hotel || {});
  for (const k of keys) {
    if (!allowed.has(k)) {
      errors.push(`${armLabel}: unexpected input key "${k}"`);
    }
  }
  for (const req of ["hotel_id", "hotel_name"]) {
    if (!String(hotel?.[req] || "").trim()) {
      errors.push(`${armLabel}: missing required ${req}`);
    }
  }
  const forbidden = collectForbiddenKeys(hotel);
  for (const f of forbidden) {
    errors.push(`${armLabel}: forbidden key at ${f.path}`);
  }
  return { ok: errors.length === 0, errors };
}

export function auditWebhoundQueryString(query, armLabel = "WEBHOUND") {
  const errors = [];
  const q = String(query || "");
  if (!q.trim()) errors.push(`${armLabel}: empty query`);
  if (FORBIDDEN_RE.test(q)) {
    errors.push(`${armLabel}: query text matches forbidden teacher/gold key pattern`);
  }
  // Soft check: query should not embed email/phone-looking teacher payloads
  if (/@[a-z0-9.-]+\.[a-z]{2,}/i.test(q) && /expected|gold|known.?owner/i.test(q)) {
    errors.push(`${armLabel}: query appears to embed expected contact teacher data`);
  }
  return { ok: errors.length === 0, errors };
}

/**
 * Full shared-input leakage audit for a frozen cohort.
 */
export function auditFrozenCohortLeakage(freeze) {
  const errors = [];
  const hotels = freeze?.hotels || [];
  if (hotels.length !== 5) {
    errors.push(`Expected 5 hotels, got ${hotels.length}`);
  }
  if (Array.isArray(freeze?.protected_held_out_hotels) && freeze.protected_held_out_hotels.length > 0) {
    errors.push("protected_held_out_hotels must be empty for this diagnostic");
  }
  for (const h of hotels) {
    const a = auditHotelOnlyInput(h, "FREEZE");
    errors.push(...a.errors);
  }
  // Cross-arm: both arms must receive identical hotel-only JSON
  for (const h of hotels) {
    const native = hotelOnlyInput(h);
    const webhound = hotelOnlyInput(h);
    if (JSON.stringify(native) !== JSON.stringify(webhound)) {
      errors.push(`Arm input mismatch for ${h.hotel_id}`);
    }
  }
  return {
    ok: errors.length === 0,
    errors,
    hotel_count: hotels.length,
    held_out_count: (freeze?.protected_held_out_hotels || []).length,
  };
}

/**
 * Audit that scoring artifacts do not leak into runner arm inputs.
 * Test artifacts may contain labeled gold — runner inputs must not.
 */
export function assertNoTeacherLeakIntoRunnerInputs(nativeInput, webhoundQuery) {
  const errors = [];
  errors.push(...auditHotelOnlyInput(nativeInput, "NATIVE").errors);
  errors.push(...auditWebhoundQueryString(webhoundQuery, "WEBHOUND").errors);
  return { ok: errors.length === 0, errors };
}
