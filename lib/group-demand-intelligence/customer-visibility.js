/**
 * Customer-facing GDI opportunity visibility.
 * Excludes synthetic test/fixture/sample rows AND inactive/invalid
 * customer-surface rows (past, chrome, promotions, failed promotion gate).
 *
 * Convergence V1: live list = surface-eligible AND
 *   (strict-ready OR temporary legacyVisibilityPreserved cohort).
 * New opportunities must pass isGdiCustomerOpportunityReady — no legacy stamp on create.
 *
 * GDI 2.0 Phase 2A: visibility is NOT commercial eligibility and NOT publication
 * authorization. For A/B/C separation use
 * `publication-decision-model-v1.js` → `evaluatePublicationDecision` /
 * `explainCustomerVisibilityBasis` (shadow classification). Do not treat
 * reviewed/legacy/inventory/W-Rome/flag paths as proof of sales-ready eligibility.
 */

import { isCustomerSurfaceActiveEligible } from "./customer-surface-revalidation-v1.js";
import { isGdiCustomerOpportunityReady } from "./customer-readiness-gate-v1.js";
import { isLegacyVisibilityCompatible } from "./legacy-visibility-compatibility-v1.js";
import { isWRomePilotQualifiedCustomerVisible } from "./expansion-pilot/w-rome-pilot-visibility-v0.js";
import { isReviewedProspectCustomerVisible } from "./reviewed-prospect-publication-v1.js";
import {
  isGdiQualifiedCustomerVisibilityV1Enabled,
  isGroupDemandIntelligenceLocalDev,
} from "./feature-flag.js";
import { isInventoryCustomerVisible } from "./inventory-visibility-v1.js";
import { isHistoricalOnlyCustomerExclusion } from "./commercial-inventory/gdi-unified-commercial-inventory-contract-v1.js";

const TEST_ID_RE =
  /_test_|fixture|sample|link_only|canary_promo|schema_repair|gdi_pe_test|v13_test|temp_validation/i;

const SYNTHETIC_TITLE_RE =
  /\bpe link test\b|\[test\]|\[schema repair\]|(?:^|:\s*)\S+\s+(?:GARDEN|WEDDING(?:\s+VENUE)?|COUNTRY(?:\s+CLUB)?|BANQUET(?:\s+HALL)?|EVENT(?:\s+VENUE)?|MUSEUM|RELIGIOUS|HISTORIC|PRIVATE|WINERY)\s+\d+/i;

/**
 * True when the opportunity must never appear on customer auth/share surfaces.
 */
export function isGdiTestOrFixtureOpportunity(opp = {}) {
  if (opp == null || typeof opp !== "object") return false;
  if (opp.isTestData === true) return true;
  // Local shadow / live-campaign preview rows may set customerVisible=false to mean
  // "not production-published" while remaining founder-local review inventory.
  // Those must not be treated as test fixtures (list + detail).
  if (opp.customerVisible === false) {
    const localPreview =
      opp.localShadowOnly === true ||
      opp.localShadowPortfolio === true ||
      opp.liveCampaignV1Shadow === true ||
      opp.stageEShadowPreview === true ||
      opp.bethesdaInternalResearchInventory === true ||
      opp.gdiInternalUnionOnly === true;
    if (!localPreview) return true;
  }
  const id = String(opp.id || opp.opportunityId || "");
  if (TEST_ID_RE.test(id)) return true;
  const title = String(opp.title || opp.opportunityName || "");
  const org = String(opp.organizationName || "");
  if (SYNTHETIC_TITLE_RE.test(title) || SYNTHETIC_TITLE_RE.test(org)) return true;
  if (opp.peVenueId === "pev_test" || /^pev_test/i.test(String(opp.peVenueId || ""))) {
    return true;
  }
  const sources = Array.isArray(opp.sources)
    ? opp.sources.map((s) => s?.url || "").join(" ")
    : "";
  if (/\.example\b/i.test(sources)) return true;
  return false;
}

/**
 * Active customer surface eligibility (excludes test + past + invalid entity + DQ).
 * Does NOT apply the readiness gate — use isCustomerFacingOpportunity for list visibility.
 * Pass { includeHistorical: true } only for explicit history endpoints.
 */
export function isActiveCustomerOpportunity(opp = {}, opts = {}) {
  if (isGdiTestOrFixtureOpportunity(opp)) return false;
  if (opts.includeHistorical === true) {
    // Historical view may include past/closed; still never test rows
    return true;
  }
  return isCustomerSurfaceActiveEligible(opp, opts);
}

/**
 * Canonical customer-list visibility (readiness convergence V1).
 *
 * visible ⇔ surface-active AND (strict-ready OR temporary legacy compatibility)
 *
 * opts.surfaceOnly=true — audit/freeze escape hatch (pre-convergence surface definition)
 */
/**
 * Generator / campaign shells are internal research objects — never customer opportunity cards.
 */
export function isGeneratorOnlyCustomerRecord(opp = {}) {
  if (opp == null || typeof opp !== "object") return false;
  if (opp.isDemandGenerator === true || opp.demandGeneratorOnly === true) return true;
  const family = String(opp.demandFamily || "").toUpperCase();
  if (family === "DEMAND_GENERATOR" || family === "DEMAND_CAMPAIGN") return true;
  const type = String(opp.opportunityType || "").toUpperCase();
  if (type === "DEMAND_GENERATOR" || type === "DEMAND_CAMPAIGN") return true;
  // Explicit campaign shell without account child admission
  if (
    opp.gdiCampaignShell === true &&
    !opp.childEntityId &&
    !opp.childAdmissionClass
  ) {
    return true;
  }
  return false;
}

export function isCustomerFacingOpportunity(opp = {}, opts = {}) {
  if (isGeneratorOnlyCustomerRecord(opp)) return false;
  if (opp.isTestData === true) return false;

  // Unified commercial inventory: PAST_CLOSED / historical-only stay in research
  // ledger only — never Customer Opportunities (unless research view opts in).
  if (
    opts.includeHistoricalCommercialExclusions !== true &&
    isHistoricalOnlyCustomerExclusion(opp, opts)
  ) {
    return false;
  }

  // Waterstone clean primary sales inventory — founder-approved local facing set.
  // Wins over prior wave customerVisible=false / superseded stamps.
  if (
    opp.waterstonePrimarySalesInventory === true &&
    opp.waterstoneInventoryLane === "PRIMARY_SALES_READY" &&
    opp.archived !== true &&
    opp.isArchived !== true &&
    opp.prospectSuppressed !== true &&
    !isHistoricalOnlyCustomerExclusion(opp, opts)
  ) {
    return true;
  }

  // GDI_PUBLISHED_INVENTORY_REGISTRY_V1 — founder-authorized published versions only.
  // Separates research eligibility from publication. ACCOUNT_DEVELOPMENT is first-class.
  if (
    opp.gdiPublishedInventoryRegistry === true &&
    opp.customerVisible !== false &&
    opp.archived !== true &&
    opp.isArchived !== true &&
    opp.prospectSuppressed !== true &&
    String(opp.publicationState || "PUBLISHED").toUpperCase() !== "REVOKED" &&
    (String(opp.commercialInventoryClass || "").toUpperCase() === "SALES_READY" ||
      String(opp.commercialInventoryClass || "").toUpperCase() ===
        "ACCOUNT_DEVELOPMENT")
  ) {
    return true;
  }

  // Durable local archive / explicit suppression — never customer-list resurrect.
  if (
    opp.archived === true ||
    opp.isArchived === true ||
    opp.customerVisible === false ||
    opp.prospectSuppressed === true ||
    opp.campaignCustomerFacingDisabled === true ||
    opp.founderCustomerVisibilityDisable === true
  ) {
    return false;
  }

  // Chapultepec Scaling active local cohort: legacy Scaling rows stay in history
  // stores but must not appear on the active local list (not deleted).
  if (
    opp.localActiveInventory === false ||
    opp.localInventorySuperseded === true
  ) {
    return false;
  }

  // Local-only certification fixtures (Segment subtype FullE2E browser gates).
  // Never customer share / production — founder local GDI UI only.
  if (
    opp.certificationFixtureOnly === true &&
    (opp.localShadowOnly === true || opp.localShadowPortfolio === true) &&
    opp.doesNotAffectProduction === true &&
    opp.customerVisible !== false &&
    isGroupDemandIntelligenceLocalDev(opts.env || process.env)
  ) {
    return true;
  }

  // Founder local live-campaign / Stage-E shadow preview inventory.
  // Surfaces researched rows for local GDI review without production publication
  // or inventing Ready/ACTIONABLE. Commercial disposition/stage remain authoritative.
  if (
    (opp.liveCampaignV1Shadow === true || opp.stageEShadowPreview === true) &&
    opp.localShadowOnly === true &&
    opp.doesNotAffectProduction === true &&
    opp.customerVisible !== false &&
    !isGdiTestOrFixtureOpportunity(opp)
  ) {
    return true;
  }

  // W Rome expansion pilot V0: honest QUALIFIED (modeled, lodging unverified) may surface
  // when GDI_WROME_EXPANSION_PILOT_V0=1 — does NOT grant ACTIONABLE / Ready and does not
  // lower the readiness gate. Checked before surface-active traps that reject sponsor shells.
  if (isWRomePilotQualifiedCustomerVisible(opp, opts.env || process.env)) {
    return true;
  }

  // Reviewed prospect publication (scoped hotels + REVIEWED_PACKAGE_V1 stamp).
  // Contact-ready sales prospects may surface without Ready/ACTIONABLE or
  // inventing housing-control evidence. Does not lower Ready.
  if (isReviewedProspectCustomerVisible(opp, opts)) {
    return true;
  }

  // Inventory Visibility V1 (Waterstone-first): status-labeled hotel inventory
  // without inventing Ready/ACTIONABLE. Does not lower the readiness gate.
  if (isInventoryCustomerVisible(opp, opts)) {
    return true;
  }

  if (!isActiveCustomerOpportunity(opp, opts)) return false;
  if (opts.surfaceOnly === true) return true;
  if (opts.includeHistorical === true) return true;
  if (isGdiCustomerOpportunityReady(opp, opts).ok) return true;
  if (isLegacyVisibilityCompatible(opp)) return true;

  // Maturity Funnel V1 — QUALIFIED visibility (feature-flagged; default OFF).
  // Prepared for Phase 2; must not expose weak rows until migration audit passes.
  if (
    isGdiQualifiedCustomerVisibilityV1Enabled(opts.env || process.env) &&
    String(opp.gdiMaturityState || "") === "QUALIFIED" &&
    !isGdiTestOrFixtureOpportunity(opp)
  ) {
    return true;
  }

  return false;
}

export function filterCustomerFacingOpportunities(opportunities = [], opts = {}) {
  return (opportunities || []).filter((o) => isCustomerFacingOpportunity(o, opts));
}

/**
 * Mark synthetic validation writes so they cannot leak to customers.
 */
export function markAsTestOpportunity(opp = {}) {
  return {
    ...opp,
    isTestData: true,
    customerVisible: false,
  };
}
