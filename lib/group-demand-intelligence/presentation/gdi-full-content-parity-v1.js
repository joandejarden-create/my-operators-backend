/**
 * GDI universal 100% full-content parity.
 *
 * Recursive field-by-field comparison of the complete customer-facing
 * presentation object discovered from the approved local opportunity —
 * not a manually maintained shortlist.
 *
 * Matching hashes alone are insufficient; callers must also verify rendered
 * customer-visible text when certifying production delivery.
 */
import { createHash } from "node:crypto";
import {
  dedupeOpportunityContacts,
  validateCustomerPresentationIntegrity,
} from "./gdi-customer-presentation-integrity-v1.js";
import {
  discoverCustomerFacingSchema,
  extractCustomerFacingPresentationPayload,
  getAtPath,
  NON_COMMERCIAL_PARITY_EXCLUSIONS,
} from "./gdi-customer-facing-schema-discovery-v1.js";
import { projectOpportunityForPresentation } from "./gdi-universal-presentation-pipeline-v1.js";
import { GDI_PRESENTATION_SURFACES } from "../contracts/gdi-presentation-contract-v1.js";
import { applyUnifiedWhoToContactStandard } from "../unified-who-to-contact-v1.js";

export const GDI_FULL_CONTENT_PARITY_V1 = "gdi-full-content-parity-v1";

export const FULL_CONTENT_PARITY_RESULT = Object.freeze({
  FULL_CONTENT_PARITY_PASS: "FULL_CONTENT_PARITY_PASS",
  FULL_CONTENT_PARITY_FAIL: "FULL_CONTENT_PARITY_FAIL",
  INTEGRITY_BLOCKED: "INTEGRITY_BLOCKED",
  TILE_DRAWER_FIELD_SUBSET_PASS: "TILE_DRAWER_FIELD_SUBSET_PASS",
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function isPlainObject(v) {
  return v != null && typeof v === "object" && !Array.isArray(v);
}

/**
 * Normalize for semantic compare: collapse whitespace; sort object keys;
 * treat null/undefined/"" as empty for leaf strings only when both empty.
 */
export function normalizeFullContentValue(value) {
  if (value == null) return null;
  if (typeof value === "string") {
    const t = text(value);
    return t === "" ? null : t;
  }
  if (typeof value === "number" || typeof value === "boolean") return value;
  if (Array.isArray(value)) {
    return value.map((v) => normalizeFullContentValue(v));
  }
  if (isPlainObject(value)) {
    const out = {};
    for (const k of Object.keys(value).sort()) {
      if (NON_COMMERCIAL_PARITY_EXCLUSIONS.includes(k)) continue;
      const n = normalizeFullContentValue(value[k]);
      if (n === undefined) continue;
      out[k] = n;
    }
    return out;
  }
  return value;
}

export function stableFullContentHash(payload = {}) {
  return createHash("sha256")
    .update(JSON.stringify(normalizeFullContentValue(payload)))
    .digest("hex");
}

/**
 * Prepare the complete approved customer presentation object for one opportunity.
 * Uses detail projection so nested drawer/contact fields are present, then
 * extracts the customer-facing payload (exclusions only).
 */
export function buildApprovedFullContentPresentation(
  opportunity = {},
  opts = {},
) {
  const hotelId = opts.hotelId || opportunity.hotelId || null;
  let base = dedupeOpportunityContacts({ ...opportunity, hotelId });
  try {
    const projected = projectOpportunityForPresentation(base, {
      hotelId,
      surface:
        opts.surface ||
        (opts.lock || base.publishedPresentationLocked
          ? GDI_PRESENTATION_SURFACES.EXTERNAL_SHARE
          : GDI_PRESENTATION_SURFACES.LOCAL_AUTH),
      view: "detail",
      forceLocked: opts.lock === true || base.publishedPresentationLocked === true,
    });
    if (projected?.dto) {
      base = dedupeOpportunityContacts({
        ...base,
        ...projected.prepared,
        ...projected.dto,
        id: base.id,
        hotelId,
      });
    }
  } catch {
    /* keep base */
  }
  if (!base.scalingTwoContactPresentation?.primary) {
    try {
      const withTwo = applyUnifiedWhoToContactStandard(base, { force: true });
      base = { ...base, ...withTwo };
    } catch {
      /* keep */
    }
  }
  const integrity = validateCustomerPresentationIntegrity(base);
  const payload = extractCustomerFacingPresentationPayload(base);
  const schema = discoverCustomerFacingSchema([payload]);
  const contentHash = stableFullContentHash(payload);
  return {
    version: GDI_FULL_CONTENT_PARITY_V1,
    hotelId,
    opportunityId: String(base.id || opportunity.id || ""),
    integrity,
    schema,
    payload,
    contentHash,
    opportunity: {
      ...base,
      customerFacingPresentationHash: contentHash,
      publishedPresentationLocked:
        opts.lock === true || base.publishedPresentationLocked === true,
    },
  };
}

function valuesEqual(a, b) {
  return (
    JSON.stringify(normalizeFullContentValue(a)) ===
    JSON.stringify(normalizeFullContentValue(b))
  );
}

/**
 * Recursive union-path comparison. Any path present on either side
 * (after exclusions) must match. Missing / extra / changed → fail.
 */
export function compareFullContentPayloads(left = {}, right = {}, opts = {}) {
  const leftPayload = extractCustomerFacingPresentationPayload(left);
  const rightPayload = extractCustomerFacingPresentationPayload(right);
  const schema = discoverCustomerFacingSchema([leftPayload, rightPayload]);
  const diffs = [];

  for (const path of schema.paths) {
    // Skip pure container paths that are only prefixes of deeper leaves when
    // both sides are objects/arrays — leaf paths catch content.
    const a = getAtPath(leftPayload, path);
    const b = getAtPath(rightPayload, path);
    const aObj = isPlainObject(a) || Array.isArray(a);
    const bObj = isPlainObject(b) || Array.isArray(b);
    if (aObj && bObj) continue;

    if (a === undefined && b === undefined) continue;
    const aEmpty =
      a === null ||
      a === "" ||
      (Array.isArray(a) && !a.length) ||
      (isPlainObject(a) && !Object.keys(a).length);
    const bEmpty =
      b === null ||
      b === "" ||
      (Array.isArray(b) && !b.length) ||
      (isPlainObject(b) && !Object.keys(b).length);
    if (a === undefined && bEmpty) continue;
    if (b === undefined && aEmpty) continue;
    if (aEmpty && bEmpty) continue;
    if (a === undefined) {
      diffs.push({
        path,
        kind: "extra_on_right",
        left: null,
        right: summarizeDiffValue(b),
      });
      continue;
    }
    if (b === undefined) {
      diffs.push({
        path,
        kind: "missing_on_right",
        left: summarizeDiffValue(a),
        right: null,
      });
      continue;
    }
    if (!valuesEqual(a, b)) {
      diffs.push({
        path,
        kind: "changed",
        left: summarizeDiffValue(a),
        right: summarizeDiffValue(b),
      });
    }
  }

  function topLevelMeaningful(v) {
    if (v == null || v === "") return false;
    if (Array.isArray(v) && v.length === 0) return false;
    if (isPlainObject(v) && Object.keys(v).length === 0) return false;
    return true;
  }

  // Top-level key presence: approved keys must exist on production payload.
  const leftKeys = new Set(Object.keys(leftPayload));
  const rightKeys = new Set(Object.keys(rightPayload));
  for (const k of leftKeys) {
    if (!rightKeys.has(k) && topLevelMeaningful(leftPayload[k])) {
      // Nested content may still be present under aliases — flag missing tops.
      if (
        !diffs.some((d) => d.path === k || String(d.path).startsWith(`${k}.`))
      ) {
        diffs.push({
          path: k,
          kind: "missing_top_level_on_right",
          left: summarizeDiffValue(leftPayload[k]),
          right: null,
        });
      }
    }
  }
  for (const k of rightKeys) {
    if (!leftKeys.has(k) && topLevelMeaningful(rightPayload[k])) {
      diffs.push({
        path: k,
        kind: "extra_top_level_on_right",
        left: null,
        right: summarizeDiffValue(rightPayload[k]),
      });
    }
  }

  const leftHash = stableFullContentHash(leftPayload);
  const rightHash = stableFullContentHash(rightPayload);
  const ok = diffs.length === 0;
  const classification = classifyFullContentDiffs(diffs);

  return {
    version: GDI_FULL_CONTENT_PARITY_V1,
    result: ok
      ? FULL_CONTENT_PARITY_RESULT.FULL_CONTENT_PARITY_PASS
      : FULL_CONTENT_PARITY_RESULT.FULL_CONTENT_PARITY_FAIL,
    ok,
    leftHash,
    rightHash,
    hashMatch: leftHash === rightHash,
    schema,
    diffCount: diffs.length,
    diffs: opts.limitDiffs
      ? diffs.slice(0, Number(opts.limitDiffs) || 100)
      : diffs,
    classification,
    // Real customer-visible gaps only — never suppress content diffs.
    contentDiffCount: classification.content.length,
    note: ok
      ? "Complete recursive customer-facing content matched."
      : "Full-content differences detected. Do not declare FULL_CONTENT_PARITY_PASS.",
  };
}

/**
 * Classify diffs for operator reporting. Content diffs always fail certification.
 * Structural / internal / formatting labels are diagnostic only — they do not
 * erase content failures.
 */
export function classifyFullContentDiffs(diffs = []) {
  const content = [];
  const structural = [];
  const internalExcluded = [];
  const formatting = [];
  for (const d of diffs || []) {
    const path = String(d?.path || "");
    const kind = String(d?.kind || "");
    if (NON_COMMERCIAL_PARITY_EXCLUSIONS.some((k) => path === k || path.startsWith(`${k}.`))) {
      internalExcluded.push(d);
      continue;
    }
    if (
      kind === "missing_top_level_on_right" ||
      kind === "extra_top_level_on_right"
    ) {
      // Top-level presence still customer-facing unless excluded above.
      content.push(d);
      continue;
    }
    if (
      typeof d?.left === "string" &&
      typeof d?.right === "string" &&
      text(d.left).toLowerCase() === text(d.right).toLowerCase()
    ) {
      formatting.push(d);
      continue;
    }
    if (kind === "changed" && isPlainObject(d?.left) && isPlainObject(d?.right)) {
      structural.push(d);
      continue;
    }
    content.push(d);
  }
  return { content, structural, internalExcluded, formatting };
}

function summarizeDiffValue(v) {
  if (v == null) return null;
  if (typeof v === "string") return text(v).slice(0, 400);
  if (typeof v === "number" || typeof v === "boolean") return v;
  try {
    return JSON.stringify(normalizeFullContentValue(v)).slice(0, 400);
  } catch {
    return String(v).slice(0, 200);
  }
}

/**
 * Compare two opportunities end-to-end after building approved full-content
 * presentations for each side.
 */
export function compareApprovedFullContent(
  leftOpportunity = {},
  rightOpportunity = {},
  opts = {},
) {
  const left = buildApprovedFullContentPresentation(leftOpportunity, opts);

  // Prefer the immutable frozen approved detail when present (published path).
  const rightFrozen =
    rightOpportunity?.approvedCustomerFacingDetail &&
    typeof rightOpportunity.approvedCustomerFacingDetail === "object"
      ? rightOpportunity.approvedCustomerFacingDetail
      : null;
  const right = rightFrozen
    ? {
        version: GDI_FULL_CONTENT_PARITY_V1,
        opportunityId: String(rightOpportunity.id || left.opportunityId || ""),
        integrity: validateCustomerPresentationIntegrity({
          ...rightOpportunity,
          ...rightFrozen,
        }),
        payload: extractCustomerFacingPresentationPayload(rightFrozen),
        contentHash: stableFullContentHash(
          extractCustomerFacingPresentationPayload(rightFrozen),
        ),
      }
    : buildApprovedFullContentPresentation(rightOpportunity, {
        ...opts,
        lock: true,
      });

  if (!left.integrity.ok || !right.integrity.ok) {
    return {
      version: GDI_FULL_CONTENT_PARITY_V1,
      result: FULL_CONTENT_PARITY_RESULT.INTEGRITY_BLOCKED,
      ok: false,
      leftHash: left.contentHash,
      rightHash: right.contentHash,
      leftIntegrity: left.integrity,
      rightIntegrity: right.integrity,
      diffs: [],
      diffCount: 0,
      schema: left.schema,
    };
  }

  const cmp = compareFullContentPayloads(left.payload, right.payload, opts);
  return {
    ...cmp,
    leftIntegrity: left.integrity,
    rightIntegrity: right.integrity,
    leftOpportunityId: left.opportunityId,
    rightOpportunityId: right.opportunityId,
    usedFrozenApprovedDetail: Boolean(rightFrozen),
  };
}

/**
 * Contact multiplicity + identity checks for rendered/API contact blocks.
 */
export function assertContactMultiplicity(opportunity = {}, expected = {}) {
  const fails = [];
  const d = dedupeOpportunityContacts(opportunity);
  const primary = text(d.primaryContact?.name);
  const secondary = text(d.secondaryContact?.name);
  if (expected.primary && primary !== expected.primary) {
    fails.push(`primary_mismatch:${primary || "(none)"}`);
  }
  if (expected.secondary && secondary !== expected.secondary) {
    fails.push(`secondary_mismatch:${secondary || "(none)"}`);
  }
  const names = [
    primary,
    secondary,
    ...(d.backupContacts || []).map((b) => text(b.name)),
  ].filter(Boolean);
  const counts = {};
  for (const n of names) {
    const k = n.toLowerCase();
    counts[k] = (counts[k] || 0) + 1;
  }
  for (const [k, c] of Object.entries(counts)) {
    if (c > 1) fails.push(`duplicate_contact:${k}:x${c}`);
  }
  const two = d.scalingTwoContactPresentation || opportunity.scalingTwoContactPresentation;
  if (expected.requireTwoContact && !two?.primary?.customerFacingNamed) {
    fails.push("scalingTwoContactPresentation_missing_or_unnamed");
  }
  return { ok: fails.length === 0, fails, primary, secondary, counts };
}

export function labelSubsetOnlyPass(subsetName = "tile_drawer") {
  return {
    result: FULL_CONTENT_PARITY_RESULT.TILE_DRAWER_FIELD_SUBSET_PASS,
    scope: subsetName,
    upgradesToFullContentParity: false,
    note: "Selected-field match only. Not 100% full-content parity.",
  };
}
