/**
 * Parent-aware Brand Explorer carryover / wrong-brand copy detection v28F.
 *
 * Resolves parent-company family per target brand and classifies cross-brand
 * references instead of applying Tribute-oriented markers globally.
 */
export const CARRYOVER_LOGIC_VERSION = "28F";

const POSITIONING_BASICS = [
  "brandTaglineMotto",
  "brandPositioning",
  "brandCustomerPromise",
  "brandFamily",
  "parentCompany",
];

const SIBLING_CONTEXT_RE =
  /\b(below|above|sibling|tier|ladder|spectrum|compared|versus|vs\.|not the|upper-upscale|portfolio|collection|family|band|corridor|positioned|between|core upscale|premium band)\b/i;

const PORTFOLIO_SLOTS_RE =
  /portfolio_context|footprint\.region|overview\.portfolio|commercial\.|loyalty\.|overview\.differentiators/i;

const COMPETITOR_CONTEXT_RE =
  /\b(competitive|comp set|versus marriott|vs\. marriott|compared to marriott|alternative to marriott|versus hilton|vs\. hilton)\b/i;

const COMPARABLE_CONTEXT_SLOTS_RE =
  /insight\.similar|insight\.compare|competitive|comp_set|similar_brands/i;

/** @typedef {'marriott'|'choice'|'hilton'|'ihg'|'unknown'} ParentFamily */

/**
 * @type {Array<{
 *   id: string,
 *   pattern: RegExp,
 *   sourceFamily: ParentFamily,
 *   severity?: string,
 *   sibling?: boolean,
 * }>}
 */
export const PARENT_AWARE_CARRYOVER_MARKERS = [
  { id: "tribute_portfolio", pattern: /\btribute portfolio\b/i, sourceFamily: "marriott", severity: "critical" },
  { id: "marriott", pattern: /\bmarriott\b/i, sourceFamily: "marriott", severity: "critical" },
  { id: "bonvoy", pattern: /\b(marriott bonvoy|bonvoy)\b/i, sourceFamily: "marriott", severity: "critical" },
  { id: "autograph_collection", pattern: /\bautograph collection\b/i, sourceFamily: "marriott", severity: "critical" },
  { id: "design_hotels", pattern: /\bdesign hotels\b/i, sourceFamily: "marriott", severity: "high" },
  { id: "curio_collection", pattern: /\bcurio collection\b/i, sourceFamily: "hilton", severity: "critical" },
  { id: "curio_phrase", pattern: /exactly like nothing else/i, sourceFamily: "hilton", severity: "critical" },
  { id: "hilton_honors", pattern: /\bhilton honors\b/i, sourceFamily: "hilton", severity: "critical" },
  { id: "tapestry_collection", pattern: /\btapestry collection\b/i, sourceFamily: "hilton", severity: "high" },
  { id: "choice_hotels_international", pattern: /\bchoice hotels international\b/i, sourceFamily: "choice", severity: "critical" },
  { id: "choice_hotels", pattern: /\bchoice hotels\b/i, sourceFamily: "choice", severity: "high" },
  { id: "choice_privileges", pattern: /\bchoice privileges\b/i, sourceFamily: "choice", severity: "medium" },
  { id: "radisson_rewards", pattern: /\bradisson rewards\b/i, sourceFamily: "choice", severity: "medium" },
  { id: "radisson_blu", pattern: /\bradisson blu\b/i, sourceFamily: "choice", severity: "high", sibling: true },
  { id: "ascend_hotel", pattern: /\bascend hotel collection\b/i, sourceFamily: "choice", severity: "high", sibling: true },
  { id: "radisson_by_choice", pattern: /\bradisson by choice\b/i, sourceFamily: "choice", severity: "medium", sibling: true },
  { id: "kimpton", pattern: /\bkimpton\b/i, sourceFamily: "ihg", severity: "critical" },
  { id: "ihg_one_rewards", pattern: /\b(ihg one rewards|one rewards)\b/i, sourceFamily: "ihg", severity: "high" },
];

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function hasVal(v) {
  if (v == null) return false;
  if (Array.isArray(v)) return v.length > 0;
  return nz(v) !== "";
}

/**
 * @param {object} brand
 * @param {object} brandTarget
 * @returns {ParentFamily}
 */
export function resolveParentFamily(brand = {}, brandTarget = {}) {
  const haystack = [
    brand.parentCompany,
    brand.name,
    brand.brandFamily,
    brandTarget?.name,
    brandTarget?.slug,
    brandTarget?.resolution?.parentCompany,
  ]
    .map(nz)
    .join(" ")
    .toLowerCase();

  if (/\bchoice hotels\b|\bradisson\b|\bascend hotel collection\b/.test(haystack)) return "choice";
  if (/\bmarriott\b|\btribute portfolio\b|\bautograph collection\b|\bdesign hotels\b/.test(haystack)) {
    return "marriott";
  }
  if (/\bhilton\b|\bcurio collection\b|\btapestry collection\b|\bwaldorf\b/.test(haystack)) return "hilton";
  if (/\bihg\b|\bkimpton\b|\bone rewards\b|\bvoco\b|\bvignette\b/.test(haystack)) return "ihg";
  return "unknown";
}

export function collectBrandTextSurfaces(brand) {
  const surfaces = [];
  const basics = brand || {};
  for (const key of POSITIONING_BASICS) {
    if (hasVal(basics[key])) {
      surfaces.push({
        surface: `basics.${key}`,
        text: nz(basics[key]),
        recordId: brand.id || null,
        slotKey: null,
      });
    }
  }
  const blocks = Array.isArray(brand?.brandExplorer?.blocks) ? brand.brandExplorer.blocks : [];
  for (const block of blocks) {
    const parts = [nz(block.title), nz(block.body)].filter(Boolean).join("\n");
    if (!parts) continue;
    surfaces.push({
      surface: `presentation.${block.slotKey}`,
      text: parts,
      recordId: block.recordId || null,
      slotKey: block.slotKey || null,
    });
  }
  return surfaces;
}

function isSelfBrandNameMatch(text, brandTarget, markerId) {
  const name = nz(brandTarget?.name).toLowerCase();
  if (!name) return false;
  if (markerId === "curio_collection" && name.includes("curio")) return true;
  if (markerId === "radisson_by_choice" && name.includes("radisson by choice")) return true;
  if (markerId === "radisson_blu" && name.includes("radisson blu")) return true;
  if (markerId === "ascend_hotel" && name.includes("ascend")) return true;
  if (markerId === "kimpton" && name.includes("kimpton")) return true;
  if (markerId === "tribute_portfolio" && name.includes("tribute")) return true;
  if ((markerId === "hilton_honors" || markerId === "choice_privileges") && name.includes(nz(markerId))) {
    return false;
  }
  if (markerId === "choice_hotels_international" && /choice hotels international/i.test(name)) return false;
  if (/\bby hilton\b/i.test(name) && markerId === "hilton_honors") return false;
  if (/\bby choice\b/i.test(name) && (markerId === "choice_hotels" || markerId === "choice_hotels_international")) {
    return false;
  }
  return nz(text).toLowerCase() === name;
}

function isSiblingContextAllowed(surface, brandTarget) {
  const targetName = nz(brandTarget?.name).toLowerCase();
  const slotKey = nz(surface.slotKey);
  const text = nz(surface.text);

  if (targetName.includes("radisson blu") && /\bradisson blu\b/i.test(text)) return true;
  if (targetName.includes("ascend") && /\bascend hotel collection\b/i.test(text)) return true;
  if (targetName.includes("radisson by choice") && /\bradisson by choice\b/i.test(text)) return true;
  if (SIBLING_CONTEXT_RE.test(text)) return true;
  if (PORTFOLIO_SLOTS_RE.test(slotKey)) return true;
  if (surface.surface === "basics.parentCompany") return true;
  return false;
}

/**
 * @returns {{
 *   classification: 'allowed_parent_reference'|'sibling_context_allowed'|'potential_wrong_brand_copy'|'competitor_context_review',
 *   severity: string|null,
 *   message: string,
 * }}
 */
export function classifyCarryoverMatch(marker, surface, targetFamily, brandTarget) {
  const text = nz(surface.text);
  const slotKey = nz(surface.slotKey);
  if (isSelfBrandNameMatch(text, brandTarget, marker.id)) {
    return {
      classification: "allowed_parent_reference",
      severity: null,
      message: `allowed_parent_reference: brand name contains ${marker.id}`,
    };
  }

  if (marker.sourceFamily === targetFamily) {
    if (marker.sibling) {
      if (isSiblingContextAllowed(surface, brandTarget)) {
        return {
          classification: "sibling_context_allowed",
          severity: null,
          message: `sibling_context_allowed: ${marker.id} in portfolio or comparison context`,
        };
      }
      return {
        classification: "potential_wrong_brand_copy",
        severity: marker.severity || "high",
        message: `potential_wrong_brand_copy: ${marker.id} appears brand-specific, not sibling/portfolio context`,
      };
    }
    return {
      classification: "allowed_parent_reference",
      severity: null,
      message: `allowed_parent_reference: ${marker.id} matches ${targetFamily} family`,
    };
  }

  if (COMPARABLE_CONTEXT_SLOTS_RE.test(slotKey) || COMPETITOR_CONTEXT_RE.test(text)) {
    return {
      classification: "competitor_context_review",
      severity: "medium",
      message: `competitor_context_review: ${marker.id} in comparable-brand or competitive context — founder review optional`,
    };
  }

  return {
    classification: "potential_wrong_brand_copy",
    severity: marker.severity || "critical",
    message: `potential_wrong_brand_copy: ${marker.id} (${marker.sourceFamily}) on ${targetFamily || "unknown"} brand`,
  };
}

/**
 * @param {object} brand
 * @param {object} brandTarget
 * @returns {Array<object>}
 */
export function scanParentAwareCarryover(brand, brandTarget = {}) {
  const targetFamily = resolveParentFamily(brand, brandTarget);
  const surfaces = collectBrandTextSurfaces(brand);
  const findings = [];

  for (const surface of surfaces) {
    for (const marker of PARENT_AWARE_CARRYOVER_MARKERS) {
      if (!marker.pattern.test(surface.text)) continue;
      const verdict = classifyCarryoverMatch(marker, surface, targetFamily, brandTarget);
      findings.push({
        markerId: marker.id,
        sourceFamily: marker.sourceFamily,
        targetFamily,
        surface: surface.surface,
        slotKey: surface.slotKey,
        recordId: surface.recordId,
        excerpt: surface.text.slice(0, 160),
        classification: verdict.classification,
        severity: verdict.severity,
        message: verdict.message,
      });
    }
  }

  return findings;
}

/**
 * Defects that should block visual / Final QA readiness.
 * @param {object} brand
 * @param {object} brandTarget
 */
export function detectBlockingCarryoverFindings(brand, brandTarget = {}) {
  return scanParentAwareCarryover(brand, brandTarget).filter(
    (f) => f.classification === "potential_wrong_brand_copy"
  );
}
