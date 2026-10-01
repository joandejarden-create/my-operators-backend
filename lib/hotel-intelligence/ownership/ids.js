/**
 * Stable Ownership Intelligence IDs (ULID-style, Crockford Base32).
 * dle_ entity · dor_ relationship · doe_ evidence · ownrun_ research run · doa_ alias
 */

import { randomBytes } from "node:crypto";

export const OWNERSHIP_IDS_VERSION = "ownership-ids-v1";

const CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";

function encodeCrockford(bytes, length) {
  let bits = 0n;
  for (const b of bytes) bits = (bits << 8n) | BigInt(b);
  const totalBits = BigInt(bytes.length * 8);
  const need = BigInt(length * 5);
  if (totalBits < need) bits <<= need - totalBits;
  else if (totalBits > need) bits >>= totalBits - need;
  let out = "";
  for (let i = length - 1; i >= 0; i -= 1) {
    const idx = Number((bits >> BigInt(i * 5)) & 31n);
    out += CROCKFORD[idx];
  }
  return out;
}

function generateUlidBody(nowMs = Date.now()) {
  const time = Math.max(0, Number(nowMs) || Date.now());
  let t = BigInt(time) & 0xffffffffffffn;
  const timeBuf = new Uint8Array(6);
  for (let i = 5; i >= 0; i -= 1) {
    timeBuf[i] = Number(t & 0xffn);
    t >>= 8n;
  }
  return `${encodeCrockford(timeBuf, 10)}${encodeCrockford(randomBytes(10), 16)}`;
}

function makeId(prefix, nowMs) {
  return `${prefix}_${generateUlidBody(nowMs)}`;
}

function isPrefixedUlid(prefix, value) {
  const re = new RegExp(`^${prefix}_[0-9A-HJKMNP-TV-Z]{26}$`, "i");
  return re.test(String(value || "").trim());
}

export function generateEntityId(nowMs) {
  return makeId("dle", nowMs);
}
export function isEntityId(value) {
  return isPrefixedUlid("dle", value);
}

export function generateRelationshipId(nowMs) {
  return makeId("dor", nowMs);
}
export function isRelationshipId(value) {
  return isPrefixedUlid("dor", value);
}

export function generateEvidenceId(nowMs) {
  return makeId("doe", nowMs);
}
export function isEvidenceId(value) {
  return isPrefixedUlid("doe", value);
}

export function generateAliasId(nowMs) {
  return makeId("doa", nowMs);
}
export function isAliasId(value) {
  return isPrefixedUlid("doa", value);
}

export function generateResearchRunId(nowMs) {
  return makeId("ownrun", nowMs);
}
export function isResearchRunId(value) {
  return /^ownrun_[0-9A-HJKMNP-TV-Z]{26}$/i.test(String(value || "").trim());
}

export function generateObservationId(nowMs) {
  return makeId("dio", nowMs);
}
export function isObservationId(value) {
  return isPrefixedUlid("dio", value);
}

export function generateDossierId(nowMs) {
  return makeId("dod", nowMs);
}
export function isDossierId(value) {
  return isPrefixedUlid("dod", value);
}

/** Intelligence Claim (Packet 2.5) — atomic evidence-backed assertion. */
export function generateClaimId(nowMs) {
  return makeId("dic", nowMs);
}
export function isClaimId(value) {
  return isPrefixedUlid("dic", value);
}

/** Relationship candidate (pre-promotion). */
export function generateRelationshipCandidateId(nowMs) {
  return makeId("drc", nowMs);
}
export function isRelationshipCandidateId(value) {
  return isPrefixedUlid("drc", value);
}

/**
 * Normalize company/alias names for lookup indexes (not merge keys).
 * @param {string} name
 */
export function normalizeEntityName(name) {
  return String(name || "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim()
    .replace(/\s+/g, " ");
}
