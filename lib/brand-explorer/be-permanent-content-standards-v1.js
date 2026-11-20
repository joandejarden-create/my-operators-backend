/**
 * Brand Explorer — permanent content / formatting / research / collection standards.
 * Loads the founder-required machine-readable contract and exposes validation helpers.
 * Gate 2 content regeneration must remain blocked until Gate 1 founder approval.
 */
import { readFileSync, existsSync } from "fs";
import { join } from "path";
import { repoRoot } from "./canonical/product-shell.js";

export const BE_PERMANENT_CONTENT_STANDARDS_V1 = "BE_PERMANENT_CONTENT_STANDARDS_CONTRACT_V1";

const CONTRACT_REL = "configs/brand-explorer-permanent-content-standards-contract-v1.json";
const MATRIX_REL = "reports/brand-explorer/permanent-standards/00-validation-matrix.json";
const CARDINALITY_REL = "configs/brand-explorer-collection-cardinality-contract.json";

let _contract = null;
let _matrix = null;

export function loadPermanentContentStandards({ reload = false } = {}) {
  if (_contract && !reload) return _contract;
  const p = join(repoRoot(), CONTRACT_REL);
  _contract = JSON.parse(readFileSync(p, "utf8"));
  return _contract;
}

export function loadValidationMatrix({ reload = false } = {}) {
  if (_matrix && !reload) return _matrix;
  const p = join(repoRoot(), MATRIX_REL);
  _matrix = JSON.parse(readFileSync(p, "utf8"));
  return _matrix;
}

export function loadCollectionCardinality() {
  return JSON.parse(readFileSync(join(repoRoot(), CARDINALITY_REL), "utf8"));
}

/** Gate 2 hard stop until founder marks Gate 1 approved (explicit flag file or env). */
export function isGate1FounderApproved({ env = process.env } = {}) {
  if (String(env.BE_GATE1_FOUNDER_APPROVED || "").trim() === "1") return true;
  const flag = join(repoRoot(), "reports/brand-explorer/phase2h-a-renderer-parity/GATE1_FOUNDER_APPROVED.json");
  if (!existsSync(flag)) return false;
  try {
    const j = JSON.parse(readFileSync(flag, "utf8"));
    return j?.founderApproved === true && j?.gate === 1;
  } catch {
    return false;
  }
}

export function assertGate2Allowed() {
  const c = loadPermanentContentStandards();
  if (c.acceptanceSequence?.gate2?.blockedUntil && !isGate1FounderApproved()) {
    const err = new Error(
      "Gate 2 content reconstruction is blocked until Gate 1 renderer parity is founder-approved (BE_PERMANENT_CONTENT_STANDARDS_V1).",
    );
    err.code = "GATE2_BLOCKED_UNTIL_GATE1";
    throw err;
  }
  return true;
}

export function getFixedCollection(id) {
  const c = loadPermanentContentStandards();
  return c.fixedCollections?.[id] || null;
}

export function getForbiddenCustomerFacingPatterns() {
  const c = loadPermanentContentStandards();
  return c.typographyAndWriting?.forbiddenCustomerFacingPatterns || [];
}

export function textViolatesEditorialStandards(text) {
  const raw = text == null ? "" : String(text);
  const hits = [];
  if (/—/.test(raw)) hits.push("em_dash");
  if (/\u00a0/.test(raw) || /&nbsp;/i.test(raw)) hits.push("nbsp_artifact");
  for (const pat of getForbiddenCustomerFacingPatterns()) {
    if (pat === "—" || pat === "\\u00a0") continue;
    try {
      if (new RegExp(pat, "i").test(raw)) hits.push(`pattern:${pat}`);
    } catch {
      if (raw.includes(pat)) hits.push(`literal:${pat}`);
    }
  }
  return hits;
}

/** Structural self-check: contract ↔ cardinality ↔ validation matrix alignment. */
export function validateStandardsContractIntegrity() {
  const c = loadPermanentContentStandards();
  const matrix = loadValidationMatrix();
  const card = loadCollectionCardinality();
  const failures = [];

  if (c.version !== BE_PERMANENT_CONTENT_STANDARDS_V1) {
    failures.push(`unexpected_contract_version:${c.version}`);
  }
  if (c.forbidBrandSpecificExceptions !== true) failures.push("must_forbid_brand_specific_exceptions");
  if (c.acceptanceSequence?.gate2?.blockedUntil !== "gate1.founderApproved === true") {
    failures.push("gate2_must_block_until_gate1");
  }

  const lifecycle = c.fixedCollections?.supportAcrossTheLifecycle;
  if (!lifecycle || lifecycle.count !== 6 || lifecycle.orderedCanonicalTitles?.length !== 6) {
    failures.push("lifecycle_must_be_exact_6");
  }
  const opening = c.fixedCollections?.openingAndConversionPath;
  if (!opening || opening.count !== 5 || opening.orderedCanonicalTitles?.length !== 5) {
    failures.push("opening_path_must_be_exact_5");
  }
  const benefits = c.fixedCollections?.loyaltyKeyBenefits;
  if (!benefits || benefits.count !== 6) failures.push("loyalty_benefits_must_be_6");

  const lifeCard = (card.collections || []).find((x) => x.id === "lifecycle");
  if (!lifeCard || lifeCard.mode !== "EXACT" || lifeCard.count !== 6) {
    failures.push("cardinality_lifecycle_mismatch");
  }
  const openCard = (card.collections || []).find((x) => x.id === "economicsItems");
  if (!openCard || openCard.mode !== "EXACT" || openCard.count !== 5) {
    failures.push("cardinality_opening_path_mismatch");
  }
  const loyCard = (card.collections || []).find((x) => x.id === "loyaltyBenefits");
  if (!loyCard || loyCard.count !== 6) {
    failures.push("cardinality_loyalty_benefits_must_be_6");
  }

  const reqIds = new Set((matrix.rows || []).map((r) => r.requirementId));
  for (const id of [
    "R1_TITLE_CASE_HEADINGS",
    "R1_NO_EM_DASH",
    "R2_LIFECYCLE_SIX_STAGES",
    "R2_OPENING_PATH_FIVE_STAGES",
    "R4_LOYALTY_SIX_BENEFITS",
    "R7_GALLERY_PROPERTY_PHOTOS",
    "R8_SIMILAR_BRANDS_ONE_PER_CARD",
    "R11_GATE1_BEFORE_GATE2",
  ]) {
    if (!reqIds.has(id)) failures.push(`matrix_missing:${id}`);
  }

  if (isGate1FounderApproved()) {
    // informational only
  } else if (matrix.gate2ContentReconstruction !== "BLOCKED_UNTIL_GATE1_APPROVED") {
    failures.push("matrix_gate2_block_flag_missing");
  }

  return {
    ok: failures.length === 0,
    failures,
    gate1FounderApproved: isGate1FounderApproved(),
    gate2Allowed: false, // always false until explicit founder flag
    contractVersion: c.version,
    matrixRows: (matrix.rows || []).length,
  };
}

export function evaluateFixedCollectionTitles(blocks, collectionId) {
  const def = getFixedCollection(collectionId);
  if (!def) return { ok: false, failures: ["unknown_collection"] };
  const list = Array.isArray(blocks) ? blocks : [];
  const failures = [];
  const titles = def.orderedCanonicalTitles || [];
  const keys = def.slotKeys || [];
  keys.forEach((slotKey, i) => {
    const want = titles[i];
    const row = list.find((b) => String(b.slotKey || "").trim() === slotKey);
    if (!row) {
      failures.push(`missing_slot:${slotKey}`);
      return;
    }
    const title = String(row.title || "").trim();
    if (want && title !== want) failures.push(`title_mismatch:${slotKey}:got=${title}:want=${want}`);
    const body = String(row.body || "").trim();
    if (!body) failures.push(`empty_body:${slotKey}`);
    failures.push(...textViolatesEditorialStandards(body).map((h) => `${slotKey}:${h}`));
  });
  return { ok: failures.length === 0, failures, expectedCount: keys.length };
}
