/**
 * Brand activation research mode — proposed readiness only; never activates.
 */

import { RESEARCH_MODES, ACTIVATION_STATUSES } from "./research-modes.js";
import { checkHotelFreshness } from "./check-hotel-freshness.js";
import { computeDirectoryGaps } from "./directory-gaps.js";
import { resolveBrandFamily, canonicalizeObservedBrand, defaultParentForFamily } from "./brand-family.js";
import { fetchText } from "./adapters/adapter-utils.js";

/**
 * Hard gates that block "Ready for Activation Review" regardless of percentage.
 */
export const ACTIVATION_HARD_GATES = Object.freeze([
  "brand_identity_current_name",
  "parent_company",
  "brand_exists_current",
  "source_authority_official",
  "mexico_or_cala_census_when_claimed",
]);

/**
 * @param {object} brandTarget
 * @param {object} context
 */
export async function runBrandActivationResearch(brandTarget, context = {}) {
  const started = Date.now();
  const brandName = brandTarget.name || brandTarget.brandName;
  const slug = brandTarget.slug || "";
  const brandStatus = brandTarget.brandStatus || brandTarget.status || "Under Review";
  const parentHint = brandTarget.parentCompany || "";
  const officialUrl = brandTarget.officialUrl || brandTarget.developmentUrl || "";

  /** Independent research result (before reconciliation) */
  const independent = {
    brandName,
    slug,
    brandStatusDealality: brandStatus,
    parentObserved: null,
    brandExists: null,
    officialSiteReachable: null,
    segment: brandTarget.segment || null,
    positioning: brandTarget.positioning || null,
    developmentModel: brandTarget.developmentModel || null,
    loyaltyProgram: brandTarget.loyaltyProgram || null,
    distributionPlatform: brandTarget.distributionPlatform || null,
    notes: [],
  };

  // Census slice first — Open/Pipeline census is existence evidence even when bot fetches are blocked
  const censusHotels = (context.censusHotels || []).filter((h) =>
    brandMatchesHotel(brandName, slug, h)
  );
  const mexico = censusHotels.filter((h) => /Mexico/i.test(h.country || ""));
  const cala = censusHotels.filter((h) => !/Mexico/i.test(h.country || ""));

  const directoryHits = countDirectoryBrandHits(brandName, context);
  independent.parentObserved =
    parentHint ||
    defaultParentForFamily(resolveBrandFamily({ name: brandName, parentCompany: parentHint })) ||
    null;

  // Brand existence: never treat HTTP 403/bot-block or mere 404 as "discontinued"
  const existence = await assessBrandExistence({
    officialUrl,
    parentHint,
    brandName,
    censusHotels,
    directoryHits,
  });
  independent.officialSiteReachable = existence.siteReachable;
  independent.brandExists = existence.exists; // true | false | null (unknown)
  independent.existenceBasis = existence.basis;
  independent.discontinuationEvidence = existence.discontinuationEvidence;
  for (const n of existence.notes) independent.notes.push(n);
  if (existence.parentObserved) independent.parentObserved = existence.parentObserved;

  /** @type {object[]} */
  const hotelChecks = [];
  for (const hotel of censusHotels.slice(0, context.maxHotels ?? 12)) {
    try {
      const r = await checkHotelFreshness(hotel, {
        ihgDirectoryRows: context.ihgDirectoryRows,
        marriottDirectoryRows: context.marriottDirectoryRows,
        choiceDirectoryRows: context.choiceDirectoryRows,
        fetchDelayMs: context.fetchDelayMs ?? 250,
      });
      hotelChecks.push({
        hotelId: hotel.hotelId || hotel.recordId,
        hotelName: hotel.name,
        country: hotel.country,
        dealalityStatus: hotel.currentStatus || hotel.status,
        observedStatus: r.observation?.operatingStatus,
        match: r.entityMatch?.level,
        hotelFound: r.observation?.hotelFound,
        officialUrl: r.observation?.officialUrl,
        material: (r.proposedCorrections || []).filter((c) =>
          ["Proposed Status Change", "Proposed Reflag"].includes(c.recommended_action)
        ),
      });
    } catch (err) {
      hotelChecks.push({
        hotelId: hotel.hotelId,
        hotelName: hotel.name,
        error: err?.message || String(err),
      });
    }
  }

  // Directory gaps for brand family
  const family = resolveBrandFamily({
    affiliation: brandName,
    parentCompany: independent.parentObserved || parentHint,
    name: brandName,
  });
  let directoryGaps = { missingCensusCandidates: [], censusNotInDirectory: [] };
  if (family === "ihg" && context.ihgDirectoryRows) {
    directoryGaps = computeDirectoryGaps(censusHotels, context.ihgDirectoryRows, {
      brandFamily: "ihg",
      countryFilter: /Mexico|Colombia|Panama|Peru|Barbados|Puerto Rico|Cayman|Brazil|Chile|Argentina|Dominican|Costa Rica|Honduras|Jamaica|Bahamas|Ecuador/i,
      brandFilter: (row) => brandRowMatches(brandName, row),
    });
  } else if (family === "marriott" && context.marriottDirectoryRows) {
    directoryGaps = computeDirectoryGaps(censusHotels, context.marriottDirectoryRows, {
      brandFamily: "marriott",
      countryFilter: /Mexico|mexico|Barbados|Colombia|Costa Rica|Argentina|Peru|Chile|Panama|Dominican|Jamaica|Cayman|Brazil|Puerto Rico/i,
      brandFilter: (row) => brandRowMatches(brandName, row),
    });
  } else if (family === "hilton" && context.hiltonDirectoryHint) {
    // Hilton activation uses census-only + GraphQL when codes exist — gap via name inventory not available without directory extract
    directoryGaps = { missingCensusCandidates: [], censusNotInDirectory: [], note: "Hilton gap scan limited without full directory extract" };
  }

  // Cross-check: census hotels but no Active BE profile → activation candidate flag
  const beActive = Boolean(brandTarget.brandExplorerActive);
  const brandActivationCandidate = censusHotels.length > 0 && !beActive;

  const scorecard = buildActivationScorecard({
    independent,
    brandTarget,
    censusHotels,
    mexico,
    cala,
    hotelChecks,
    directoryGaps,
    brandActivationCandidate,
    beActive,
  });

  const recommendation = decideActivationStatus(scorecard);

  // Reconciliation note (preserve independent first)
  const reconciliation = {
    censusCount: censusHotels.length,
    mexicoCount: mexico.length,
    calaCount: cala.length,
    beActive,
    brandActivationCandidate,
    parentDealality: parentHint || null,
    parentObserved: independent.parentObserved,
    parentAlign: parentsAlign(parentHint, independent.parentObserved),
  };

  return {
    researchMode: RESEARCH_MODES.BRAND_ACTIVATION,
    generatedAt: new Date().toISOString(),
    elapsedMs: Date.now() - started,
    brandTarget: {
      name: brandName,
      slug,
      recordId: brandTarget.recordId || null,
      brandStatus,
      selectionReason: brandTarget.selectionReason || null,
    },
    independent,
    hotelChecks,
    directoryGaps: {
      missing: directoryGaps.missingCensusCandidates || [],
      censusNotInDirectory: directoryGaps.censusNotInDirectory || [],
    },
    reconciliation,
    scorecard,
    recommendation,
    hardGatesFailed: scorecard.hardGatesFailed,
    activationReadinessPct: scorecard.activationReadinessPct,
    note: "NEVER activates automatically — proposed readiness only",
  };
}

function brandMatchesHotel(brandName, slug, hotel) {
  const blob = `${hotel.name || ""} ${hotel.currentBrand || hotel.affiliation || ""}`.toLowerCase();
  const b = String(brandName || "").toLowerCase();
  if (/avani/.test(b)) return /\bavani\b/.test(blob);
  if (/tapestry/.test(b)) return /tapestry/.test(blob);
  if (/four points flex/.test(b)) return /four points flex/.test(blob);
  if (/radisson collection/.test(b)) return /radisson collection/.test(blob);
  if (/spark/.test(b)) return /spark by hilton|\bspark\b/.test(blob);
  if (/curio/.test(b)) return /curio/.test(blob);
  if (slug && blob.includes(slug.replace(/-/g, " "))) return true;
  return blob.includes(b.split(" ")[0]);
}

function brandRowMatches(brandName, row) {
  const blob = `${row.brand || ""} ${row.name || ""} ${row.propertyName || ""} ${row.propertyUrl || ""}`.toLowerCase();
  const b = String(brandName || "").toLowerCase();
  if (/indigo/.test(b)) return /hotelindigo|indigo/.test(blob);
  if (/kimpton/.test(b)) return /kimpton/.test(blob);
  if (/tribute/.test(b)) return /tribute/.test(blob);
  if (/tapestry/.test(b)) return /tapestry/.test(blob);
  if (/spark/.test(b)) return /spark/.test(blob);
  if (/four points flex/.test(b)) return /four.?points.?flex|fourpointsexpress/.test(blob);
  if (/avani/.test(b)) return /avani/.test(blob);
  if (/radisson collection/.test(b)) return /radisson.?collection/.test(blob);
  return false;
}

function inferParentFromHtmlOrUrl(url, html, hint) {
  const blob = `${url} ${html?.slice?.(0, 2000) || ""} ${hint}`.toLowerCase();
  if (/marriott/.test(blob)) return "Marriott International";
  if (/hilton/.test(blob)) return "Hilton";
  if (/ihg|intercontinental hotels/.test(blob)) return "IHG Hotels & Resorts";
  if (/choice/.test(blob)) return "Choice Hotels International, Inc.";
  if (/minor/.test(blob)) return "Minor Hotel Group Limited";
  return hint || null;
}

function parentsAlign(a, b) {
  if (!a || !b) return null;
  const na = String(a).toLowerCase();
  const nb = String(b).toLowerCase();
  if (na.includes("marriott") && nb.includes("marriott")) return true;
  if (na.includes("hilton") && nb.includes("hilton")) return true;
  if (na.includes("ihg") && nb.includes("ihg")) return true;
  if (na.includes("choice") && nb.includes("choice")) return true;
  if (na.includes("minor") && nb.includes("minor")) return true;
  return na === nb;
}

function buildActivationScorecard(input) {
  const checks = {
    identity_completeness: Boolean(input.independent.brandName),
    parent_relationship: Boolean(input.independent.parentObserved || input.brandTarget.parentCompany),
    current_existence: input.independent.brandExists === true,
    source_authority:
      input.independent.officialSiteReachable === true ||
      input.independent.existenceBasis === "census_open_or_pipeline" ||
      input.independent.existenceBasis === "official_directory_rows" ||
      input.independent.existenceBasis === "official_site_ok",
    brand_profile_completeness: Boolean(input.brandTarget.hasPresentationRows),
    census_completeness: input.censusHotels.length > 0,
    mexico_cala_coverage: input.mexico.length + input.cala.length > 0,
    pipeline_validation: input.hotelChecks.some((h) => h.hotelFound || h.dealalityStatus),
    contradiction_status: !input.hotelChecks.some((h) => (h.material || []).length > 0),
    image_integrity: input.brandTarget.imageIntegrityOk !== false, // unknown → soft pass
    mandatory_evidence_gates: Boolean(input.brandTarget.mandatoryGatesPass),
    unresolved_unknowns: (input.independent.notes || []).filter((n) => /unverified|failed|No official/i.test(n)).length === 0,
  };

  const weights = {
    identity_completeness: 10,
    parent_relationship: 15,
    current_existence: 15,
    source_authority: 10,
    brand_profile_completeness: 8,
    census_completeness: 12,
    mexico_cala_coverage: 10,
    pipeline_validation: 5,
    contradiction_status: 5,
    image_integrity: 5,
    mandatory_evidence_gates: 3,
    unresolved_unknowns: 2,
  };

  let score = 0;
  let max = 0;
  /** @type {object[]} */
  const breakdown = [];
  for (const [k, ok] of Object.entries(checks)) {
    const w = weights[k] || 0;
    max += w;
    if (ok) score += w;
    breakdown.push({ dimension: k, pass: ok, weight: w });
  }

  const hardGatesFailed = [];
  if (!checks.identity_completeness) hardGatesFailed.push("brand_identity_current_name");
  if (!checks.parent_relationship) hardGatesFailed.push("parent_company");
  // Only hard-fail existence when strong discontinuation evidence — unknown/blocked probe is NOT a fail
  if (input.independent.brandExists === false && input.independent.discontinuationEvidence) {
    hardGatesFailed.push("brand_exists_current");
  } else if (input.independent.brandExists == null && input.censusHotels.length === 0 && !input.independent.existenceBasis?.includes?.("directory")) {
    hardGatesFailed.push("brand_exists_unverified");
  }
  if (!checks.source_authority && input.censusHotels.length === 0) hardGatesFailed.push("source_authority_official");
  if (input.brandTarget.requiresCala !== false && !checks.mexico_cala_coverage && input.beActive === false) {
    if (input.censusHotels.length === 0) hardGatesFailed.push("mexico_or_cala_census_when_claimed");
  }

  return {
    checks,
    breakdown,
    activationReadinessPct: max ? Math.round((score / max) * 100) : 0,
    hardGatesFailed,
    brandActivationCandidate: input.brandActivationCandidate,
    censusMexico: input.mexico.length,
    censusCala: input.cala.length,
    missingDirectoryCandidates: (input.directoryGaps.missingCensusCandidates || []).length,
    existenceBasis: input.independent.existenceBasis || null,
  };
}

function decideActivationStatus(scorecard) {
  // Discontinued ONLY with strong current evidence — never from 403/bot-block alone
  if (
    scorecard.hardGatesFailed.includes("brand_exists_current") &&
    scorecard.existenceBasis === "discontinuation_language"
  ) {
    return {
      status: "Brand Appears Inactive / Discontinued",
      rationale: "Strong discontinuation language on official source; not a fetch-block inference",
    };
  }
  if (scorecard.hardGatesFailed.length) {
    return {
      status: "Hold — Insufficient Current Evidence",
      rationale: `Hard gates failed: ${scorecard.hardGatesFailed.join(", ")}`,
    };
  }
  if (!scorecard.checks.contradiction_status) {
    return {
      status: "Hold — Conflicting Evidence",
      rationale: "Hotel-level material contradictions unresolved",
    };
  }
  if (scorecard.activationReadinessPct >= 85 && scorecard.checks.mandatory_evidence_gates) {
    return {
      status: "Ready for Activation Review",
      rationale: "Hard gates clear and readiness ≥85% with mandatory evidence gates",
    };
  }
  if (scorecard.activationReadinessPct >= 55) {
    return {
      status: "Targeted Remediation Required",
      rationale: "Most dimensions present; specific gaps remain before activation review",
    };
  }
  if (scorecard.brandActivationCandidate) {
    return {
      status: "Deep Research Required",
      rationale: "Census hotels exist without Active Brand Explorer profile — activation candidate needs full pack",
    };
  }
  return {
    status: "Deep Research Required",
    rationale: "Material intelligence gaps remain",
  };
}

/**
 * Assess brand existence without treating bot-blocks as discontinuation.
 * @returns {Promise<{exists: boolean|null, siteReachable: boolean|null, basis: string, discontinuationEvidence: boolean, parentObserved: string|null, notes: string[]}>}
 */
async function assessBrandExistence({ officialUrl, parentHint, brandName, censusHotels, directoryHits }) {
  const notes = [];
  const openOrPipeline = censusHotels.filter((h) =>
    /open|pipeline|under construction|soft open/i.test(String(h.currentStatus || h.status || ""))
  );

  // Strong positive: Dealality census already has branded Open/Pipeline hotels
  if (openOrPipeline.length > 0) {
    notes.push(
      `Existence supported by ${openOrPipeline.length} Dealality census hotel(s) with Open/Pipeline status (independent of homepage fetch)`
    );
  }
  if (directoryHits > 0) {
    notes.push(`Existence supported by ${directoryHits} official directory row(s) for brand`);
  }

  let siteReachable = null;
  let pageOk = false;
  let pageText = "";
  let pageUrl = "";
  let parentObserved = null;

  if (officialUrl) {
    try {
      const page = await fetchText(officialUrl);
      pageUrl = page.url;
      pageText = page.text || "";
      if (page.ok && page.status < 400) {
        siteReachable = true;
        pageOk = true;
        parentObserved = inferParentFromHtmlOrUrl(page.url, page.text, parentHint);
      } else if (page.status === 403 || page.status === 429) {
        siteReachable = null;
        notes.push(
          `Official URL returned HTTP ${page.status} (bot/rate block) — does NOT prove brand inactive`
        );
      } else if (page.status === 404 || page.status === 410) {
        siteReachable = false;
        notes.push(`Official URL returned HTTP ${page.status} — weak signal only; check census/directory`);
      } else {
        siteReachable = false;
        notes.push(`Official URL returned HTTP ${page.status}`);
      }
    } catch (err) {
      siteReachable = null;
      notes.push(`Official URL probe failed: ${err?.message || err}`);
    }
  } else {
    notes.push("No official URL provided — existence unverified via primary homepage");
  }

  const disc = pageOk && hasDiscontinuationLanguage(pageText);
  if (disc) {
    return {
      exists: false,
      siteReachable,
      basis: "discontinuation_language",
      discontinuationEvidence: true,
      parentObserved: parentObserved || parentHint || null,
      notes: [...notes, "Official page contains discontinuation / brand-closed language"],
    };
  }

  if (pageOk) {
    return {
      exists: true,
      siteReachable: true,
      basis: "official_site_ok",
      discontinuationEvidence: false,
      parentObserved: parentObserved || parentHint || null,
      notes,
    };
  }
  if (openOrPipeline.length > 0) {
    return {
      exists: true,
      siteReachable,
      basis: "census_open_or_pipeline",
      discontinuationEvidence: false,
      parentObserved: parentHint || null,
      notes,
    };
  }
  if (directoryHits > 0) {
    return {
      exists: true,
      siteReachable,
      basis: "official_directory_rows",
      discontinuationEvidence: false,
      parentObserved: parentHint || null,
      notes,
    };
  }

  return {
    exists: null,
    siteReachable,
    basis: siteReachable === false ? "url_unreachable_no_corroboration" : "unverified",
    discontinuationEvidence: false,
    parentObserved: parentHint || null,
    notes,
  };
}

function hasDiscontinuationLanguage(html) {
  const t = String(html || "").toLowerCase();
  if (!t || t.length < 80) return false;
  return (
    /brand (has been |was )?(discontinued|retired|sunset)/.test(t) ||
    /no longer (accepting|offering|franchising)/.test(t) ||
    /this brand (is|has been) (closed|inactive|discontinued)/.test(t)
  );
}

function countDirectoryBrandHits(brandName, context) {
  let n = 0;
  for (const row of context.ihgDirectoryRows || []) {
    if (brandRowMatches(brandName, row)) n++;
  }
  for (const row of context.marriottDirectoryRows || []) {
    if (brandRowMatches(brandName, row)) n++;
  }
  for (const row of context.choiceDirectoryRows || []) {
    if (brandRowMatches(brandName, row)) n++;
  }
  return n;
}

export { ACTIVATION_STATUSES, RESEARCH_MODES };
