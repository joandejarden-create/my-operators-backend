/**
 * Owner portfolio confidence audit — CoStar True Owner rollups with operator-alignment checks.
 * Used before outreach to avoid pitching the wrong hotels or mis-attributed SPV portfolios.
 */
import { normalizeOwnerKey } from "./normalize.js";
import { inferEntityBridgeStrategy } from "./registry-contact-config.js";
import { isBrandDecisionEligibleProperty } from "./branding-owner-context.js";
import { isCalaCountry } from "./cala-footprint.js";
import { resolveCorporateWebSeed } from "./adapters/corporate-web-seeds-resolver.js";
import { pickLeadProperty } from "./owner-lead-asset.js";

/** @typedef {"high" | "medium" | "low" | "blocked"} PortfolioConfidenceLevel */

export const MAP_PORTFOLIO_AUDIT = {
  /** Operator field matches owner tokens on at least this share → supports "high". */
  operatorAlignHighMinRate: 0.55,
  /** Below this operator-align rate without integrated-operator pattern → downgrade. */
  operatorAlignLowMaxRate: 0.25,
  /** Junk building name if length below this (after trim). */
  junkBuildingNameMaxLen: 4,
};

/** Manual research overrides — worst-case floor for portfolio confidence. */
export const MANUAL_PORTFOLIO_AUDIT = {
  "arotesa servicions integrales sa de cv": {
    level: "blocked",
    flags: ["entity_mismatch", "portfolio_over_attribution"],
    guidance:
      "CoStar lists Arotesa SPV; Balear Inmobiliaria is recorded owner of Paraiso de la Bonita. Verify each asset before outreach.",
  },
  "galicott and macari": {
    level: "low",
    flags: ["opaque_spv", "entity_unverified"],
    guidance:
      "CoStar alias likely maps to Ganzi S. de R.L. de C.V. Confirm legal rep before portfolio pitch.",
  },
  "park mizgal s c": {
    level: "medium",
    flags: ["possible_rollup_noise"],
    guidance:
      "Sunscape Ixtapa fits family owner story; ibis Tlalnepantla/Mérida may be CoStar rollup — verify asset-by-asset.",
  },
  "grupo questro": {
    level: "medium",
    flags: ["operator_mismatch_mixed"],
    guidance:
      "Los Cabos developer portfolio mixes ALG/Hyatt/Marriott operators — pitch lead asset or confirm ownership per hotel.",
  },
  essendi: {
    level: "medium",
    flags: ["divestiture_risk"],
    guidance:
      "Essendi divesting LATAM assets — confirm current ownership before multi-asset pitch.",
  },
  "club viva international inc": {
    level: "medium",
    flags: ["data_quality"],
    guidance: "Review property rows with junk building names before mail-merge.",
  },
};

const COMPOUND_OWNER_RE = /\bowner\s+\d+\s*:/i;
const OPAQUE_OWNER_RE =
  /\b(spv|fideicomiso|fideicomisos|trust|holding|partners|inc\s*\||\|\s*owner)\b/i;

const OWNER_STOP = new Set([
  "grupo",
  "group",
  "hotels",
  "hotel",
  "hoteles",
  "resorts",
  "resort",
  "hospitality",
  "international",
  "owner",
  "corp",
  "sa",
  "de",
  "cv",
  "sl",
  "inc",
  "llc",
  "ltda",
  "the",
  "and",
]);

/**
 * @param {string} ownerName
 * @returns {string[]}
 */
export function ownerMatchTokens(ownerName) {
  return normalizeOwnerKey(ownerName)
    .split(/\s+/)
    .filter((t) => t.length >= 4 && !OWNER_STOP.has(t));
}

/**
 * @param {string} ownerName
 * @param {string} operator
 */
export function operatorAlignsWithOwner(ownerName, operator) {
  const opKey = normalizeOwnerKey(operator);
  if (!opKey) return false;
  const ownerKey = normalizeOwnerKey(ownerName);
  if (opKey === ownerKey || opKey.includes(ownerKey) || ownerKey.includes(opKey)) return true;
  const tokens = ownerMatchTokens(ownerName);
  if (!tokens.length) return false;
  const matched = tokens.filter((t) => opKey.includes(t));
  return matched.length >= Math.min(2, tokens.length) || (tokens.length === 1 && matched.length === 1);
}

/**
 * @param {string} buildingName
 * @param {string} [country]
 */
export function isJunkBuildingName(buildingName, country = "") {
  const name = String(buildingName || "").trim();
  if (!name) return true;
  if (name.length <= MAP_PORTFOLIO_AUDIT.junkBuildingNameMaxLen) return true;
  const norm = normalizeOwnerKey(name);
  const countryNorm = normalizeOwnerKey(country);
  if (countryNorm && (norm === countryNorm || norm.endsWith(` ${countryNorm}`))) return true;
  if (/^(mexico|peru|colombia|brazil|chile|cuba|dominican republic|puerto rico|caicos islands)$/i.test(name)) {
    return true;
  }
  return false;
}

/**
 * @param {PortfolioConfidenceLevel} a
 * @param {PortfolioConfidenceLevel} b
 */
function minConfidence(a, b) {
  const order = { high: 0, medium: 1, low: 2, blocked: 3 };
  return (order[a] ?? 2) >= (order[b] ?? 2) ? a : b;
}

/**
 * @param {object} ownerRow branding / owner target context
 * @param {object[]} properties CALA properties for owner
 * @returns {object}
 */
export function buildOwnerPortfolioAudit(ownerRow, properties) {
  const ownerName = String(ownerRow.ownerName || "").trim();
  const ownerKey = normalizeOwnerKey(ownerName);
  const calaProps = (properties || []).filter((p) => isCalaCountry(p.country));
  const seed = resolveCorporateWebSeed(ownerName);
  const bridgeStrategy = inferEntityBridgeStrategy(ownerName);

  /** @type {string[]} */
  const flags = [];
  let level /** @type {PortfolioConfidenceLevel} */ = "high";

  if (COMPOUND_OWNER_RE.test(ownerName)) flags.push("compound_owner_string");
  if (bridgeStrategy === "rnt_bridge" || OPAQUE_OWNER_RE.test(ownerName)) flags.push("opaque_spv_name");
  if (seed?.entityType === "opaque_spv") flags.push("seed_opaque_spv");

  const junkRows = calaProps.filter((p) => isJunkBuildingName(p.buildingName, p.country));
  if (junkRows.length) flags.push("junk_building_names");

  const operatorAligned = calaProps.filter((p) =>
    operatorAlignsWithOwner(ownerName, p.hotelOperator || p.trueOwner)
  );
  const operatorMatchRate = calaProps.length ? operatorAligned.length / calaProps.length : 0;

  if (calaProps.length === 0) {
    level = "low";
    flags.push("no_cala_properties");
  } else if (operatorMatchRate < MAP_PORTFOLIO_AUDIT.operatorAlignLowMaxRate) {
    flags.push("low_operator_alignment");
    level = minConfidence(level, "medium");
  } else if (operatorMatchRate < MAP_PORTFOLIO_AUDIT.operatorAlignHighMinRate) {
    flags.push("mixed_operator_alignment");
    level = minConfidence(level, "medium");
  }

  const uniqueOperators = [...new Set(calaProps.map((p) => p.hotelOperator).filter(Boolean))];
  if (uniqueOperators.length >= 4) flags.push("many_distinct_operators");

  const pitchEligible = calaProps.filter((p) =>
    isBrandDecisionEligibleProperty(p, {
      ownerName,
      icpSegment: ownerRow.icpSegment || "",
    })
  );

  const propertyRows = calaProps.map((p) => {
    const operatorAlignedRow = operatorAlignsWithOwner(ownerName, p.hotelOperator || p.trueOwner);
    const pitchEligibleRow = isBrandDecisionEligibleProperty(p, {
      ownerName,
      icpSegment: ownerRow.icpSegment || "",
    });
    /** @type {string[]} */
    const rowFlags = [];
    if (isJunkBuildingName(p.buildingName, p.country)) rowFlags.push("junk_building_name");
    if (!operatorAlignedRow) rowFlags.push("operator_mismatch");
    if (!pitchEligibleRow) rowFlags.push("not_brand_decision_eligible");
    return {
      buildingName: p.buildingName,
      brand: p.brandAffiliation || "",
      city: p.city || "",
      country: p.country || "",
      hotelOperator: p.hotelOperator || "",
      parentCompany: p.parentCompany || "",
      costarPropertyId: p.costarPropertyId || "",
      pitchEligible: pitchEligibleRow,
      operatorAligned: operatorAlignedRow,
      rowFlags,
    };
  });

  const leadPitch =
    pickLeadProperty(
      pitchEligible.map((p) => ({
        ...p,
        operatorAligned: operatorAlignsWithOwner(ownerName, p.hotelOperator || p.trueOwner),
      })),
      ownerName
    ) ||
    pitchEligible[0] ||
    calaProps[0] ||
    null;
  const manual = MANUAL_PORTFOLIO_AUDIT[ownerKey];
  if (manual) {
    flags.push(...manual.flags);
    level = minConfidence(level, manual.level);
  }

  const outreachSafe =
    level === "high" ||
    (level === "medium" && pitchEligible.length > 0 && !flags.includes("entity_mismatch"));

  return {
    ownerTargetId: ownerRow.ownerTargetId || ownerRow.id || null,
    ownerName,
    priorityTier: ownerRow.priorityTier || "",
    icpSegment: ownerRow.icpSegment || "",
    outreachReady: Boolean(ownerRow.outreachReady),
    enrichmentPriority: ownerRow.enrichmentPriority || "",
    contactName: ownerRow.contact?.name || ownerRow.contactName || "",
    portfolioConfidence: level,
    outreachSafe,
    operatorMatchRate: Math.round(operatorMatchRate * 100) / 100,
    calaPropertyCount: calaProps.length,
    pitchEligibleCount: pitchEligible.length,
    junkPropertyCount: junkRows.length,
    distinctOperatorCount: uniqueOperators.length,
    bridgeStrategy,
    corporateSeedSlug: seed?.slug || "",
    flags: [...new Set(flags)],
    outreachGuidance:
      manual?.guidance ||
      (level === "high"
        ? "CoStar portfolio aligns with operator fields — safe to reference portfolio in intro."
        : level === "medium"
          ? "Use lead asset only until operator/entity alignment is confirmed."
          : "Do not pitch full portfolio — verify entity and asset ownership first."),
    leadPitchAsset: leadPitch
      ? [leadPitch.buildingName, leadPitch.city, leadPitch.country].filter(Boolean).join(" — ")
      : "",
    operatorsSummary: uniqueOperators.slice(0, 8).join("; "),
    countriesSummary: [...new Set(calaProps.map((p) => p.country).filter(Boolean))].join("; "),
    properties: propertyRows,
  };
}

/**
 * @param {object[]} audits
 */
export function summarizePortfolioAudits(audits) {
  const byLevel = { high: 0, medium: 0, low: 0, blocked: 0 };
  let outreachSafe = 0;
  for (const a of audits) {
    byLevel[a.portfolioConfidence] = (byLevel[a.portfolioConfidence] || 0) + 1;
    if (a.outreachSafe) outreachSafe++;
  }
  return {
    total: audits.length,
    byConfidence: byLevel,
    outreachSafe,
    needsAudit: audits.filter((a) => !a.outreachSafe).length,
  };
}
