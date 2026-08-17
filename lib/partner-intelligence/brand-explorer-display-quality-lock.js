/**
 * v38 — Brand Explorer external display quality lock.
 *
 * Connects display state, DOM proof scanning, and external owner readiness scoring.
 */
import {
  FULL_PROFILE_DISPLAY_STATES,
  resolveBrandExplorerDisplayState,
  shouldRenderFullBrandExplorerProfile,
} from "./brand-explorer-display-state.js";
import {
  FORBIDDEN_EXTERNAL_DISPLAY_STRINGS,
  FORBIDDEN_STANDALONE_BULLET_PATTERNS,
  FORBIDDEN_URL_IN_STAGING_PATTERNS,
  scanRenderedHtmlForForbiddenStrings,
} from "./brand-explorer-external-display-forbidden-patterns.js";

export const V38_QUALITY_LOCK_VERSION = "v38";

/** v38 expanded forbidden strings for incomplete external profiles. */
export const V38_FORBIDDEN_INCOMPLETE_EXTERNAL_STRINGS = Object.freeze([
  ...FORBIDDEN_EXTERNAL_DISPLAY_STRINGS,
  "Slots materials.gallery",
  "Output Note",
  "internal review",
  "supports internal review",
  "source data",
  "disclosure document",
  "confirm every line",
  "Internal preview",
  "Not owner-ready",
]);

export const V38_FORBIDDEN_ROUGH_BULLET_PATTERNS = Object.freeze([
  ...FORBIDDEN_STANDALONE_BULLET_PATTERNS,
  /<li>\s*neighborhood focus\s*<\/li>/i,
  /<li>\s*boutique design\s*<\/li>/i,
  /<li>\s*conversion-friendly\.?\s*<\/li>/i,
]);

/** Known fallback paths — classification for audit reports. */
export const FALLBACK_RENDERER_INVENTORY = Object.freeze([
  {
    id: "overview_scenario_helper",
    file: "public/js/brand-explorer-atelier-from-api.js",
    renderer: "renderAtelierOverview",
    classification: "must_suppress_when_incomplete",
    pattern: "Scenario cards will appear",
  },
  {
    id: "overview_why_value_basics",
    file: "public/js/brand-explorer-atelier-from-api.js",
    renderer: "renderAtelierOverview",
    classification: "must_suppress_when_incomplete",
    pattern: "brandValueProposition / keyBrandDifferentiators fallback bullets",
  },
  {
    id: "overview_proof_fallback_grid",
    file: "public/js/brand-explorer-atelier-from-api.js",
    renderer: "renderAtelierOverview",
    classification: "must_suppress_when_incomplete",
    pattern: "proofFallbackBodies when no overview.proof rows",
  },
  {
    id: "materials_gallery_slot_helper",
    file: "public/js/brand-explorer-atelier-from-api.js",
    renderer: "renderBrandMaterials",
    classification: "must_suppress_when_incomplete",
    pattern: "Slots materials.gallery",
  },
  {
    id: "standards_output_note",
    file: "public/js/brand-explorer-atelier-from-api.js",
    renderer: "renderStandardsOwnerConsiderations",
    classification: "internal_preview_only",
    pattern: "Output Note",
  },
  {
    id: "standards_placeholder_checklist",
    file: "public/js/brand-explorer-atelier-from-api.js",
    renderer: "renderStandardsOwnerConsiderations",
    classification: "must_suppress_when_incomplete",
    pattern: "No owner planning checklist is published",
  },
  {
    id: "insight_basics_summary",
    file: "public/js/brand-explorer-atelier-from-api.js",
    renderer: "dealalitySummaryFromBrand",
    classification: "must_suppress_when_incomplete",
    pattern: "Brand Basics brandValueProposition fallback",
  },
  {
    id: "empty_card_nbsp_shells",
    file: "public/js/brand-explorer-atelier-from-api.js",
    renderer: "multiple",
    classification: "must_suppress_when_incomplete",
    pattern: "oe-dd--empty / &nbsp; padded list shells",
  },
]);

const ATELIER_TAB_IDS = Object.freeze([
  "atelier-overview",
  "atelier-value-owners",
  "atelier-ops",
  "atelier-standards-owner",
  "atelier-commercial",
  "atelier-economics",
  "atelier-loyalty",
  "atelier-footprint",
  "atelier-materials",
  "atelier-insight",
]);

function nz(v) {
  return v == null ? "" : String(v);
}

function countMatches(html, re) {
  const m = nz(html).match(re);
  return m ? m.length : 0;
}

/**
 * Scan rendered HTML for v38 quality lock violations on incomplete brands.
 */
export function scanExternalQualityLockHtml(html, options = {}) {
  const text = nz(html);
  const companyValidated = options.companyValidated === true;
  const expectLocked = options.expectLocked === true;
  const expectFull = options.expectFull === true;

  const baseScan = scanRenderedHtmlForForbiddenStrings(text, { companyValidated });

  const extraForbidden = [];
  const extraMatches = [];

  for (const needle of V38_FORBIDDEN_INCOMPLETE_EXTERNAL_STRINGS) {
    if (FORBIDDEN_EXTERNAL_DISPLAY_STRINGS.includes(needle)) continue;
    if (needle === "Brand-Verified Content" && companyValidated) continue;
    if (expectFull && (needle === "Internal preview" || needle === "Not owner-ready")) continue;
    if (text.includes(needle)) {
      extraForbidden.push(needle);
      extraMatches.push({ pattern: needle, snippet: needle });
    }
  }

  for (const re of V38_FORBIDDEN_ROUGH_BULLET_PATTERNS) {
    if (FORBIDDEN_STANDALONE_BULLET_PATTERNS.includes(re)) continue;
    const hit = text.match(re);
    if (hit) {
      extraForbidden.push(hit[0]);
      extraMatches.push({ pattern: re.source, snippet: hit[0] });
    }
  }

  if (expectLocked) {
    if (text.includes("—")) {
      extraForbidden.push("—");
      extraMatches.push({ pattern: "em-dash placeholder", snippet: "—" });
    }
    if (/\&nbsp;/.test(text)) {
      extraForbidden.push("&nbsp;");
      extraMatches.push({ pattern: "&nbsp;", snippet: "&nbsp;" });
    }
  }

  for (const re of FORBIDDEN_URL_IN_STAGING_PATTERNS) {
    const hit = text.match(re);
    if (hit) {
      extraForbidden.push("http/https in staging section");
      extraMatches.push({ pattern: re.source, snippet: hit[0].slice(0, 120) });
    }
  }

  const forbiddenStringsFound = [...new Set([...baseScan.forbiddenStringsFound, ...extraForbidden])];
  const matches = [...baseScan.matches, ...extraMatches];

  const emptyCardsFound =
    countMatches(text, /oe-dd--empty/g) +
    countMatches(text, /scenario-card__visual--empty/g) +
    countMatches(text, /explorer-detail-card__body oe-dd--empty/g);

  const helperTextFound =
    text.includes("Scenario cards will appear") ||
    text.includes("Slots materials.gallery") ||
    text.includes("No owner planning checklist");

  const internalNotesFound =
    text.includes("Output Note") ||
    text.includes("supports internal review") ||
    text.includes("internal review");

  const profileInPreparationCount = countMatches(
    text,
    /data-be-display-gate="profile-in-preparation"/g
  );
  const profileInPreparationRendered = profileInPreparationCount > 0;

  const tabsRendered = ATELIER_TAB_IDS.filter((id) => text.includes(`data-atelier-panel="${id}"`));
  const tabPanelsRendered = countMatches(text, /data-atelier-panel="/g);

  return {
    forbiddenStringsFound,
    forbidden_strings_found: forbiddenStringsFound.length,
    matches,
    emptyCardsFound,
    empty_cards_found: emptyCardsFound,
    helperTextFound,
    helper_text_found: helperTextFound,
    internalNotesFound,
    internal_notes_found: internalNotesFound,
    profileInPreparationRendered,
    profile_in_preparation_count: profileInPreparationCount,
    tabsRenderedExternally: tabsRendered,
    tabPanelsRendered,
    externalQualityLockPass: evaluateDomPass({
      expectLocked,
      expectFull,
      forbiddenStringsFound,
      emptyCardsFound,
      helperTextFound,
      internalNotesFound,
      profileInPreparationRendered,
      profileInPreparationCount,
      tabsRendered,
      tabPanelsRendered,
      text,
    }),
  };
}

function evaluateDomPass(ctx) {
  if (ctx.expectLocked) {
    if (ctx.forbiddenStringsFound.length > 0) return false;
    if (ctx.emptyCardsFound > 0) return false;
    if (ctx.helperTextFound) return false;
    if (ctx.internalNotesFound) return false;
    if (!ctx.profileInPreparationRendered) return false;
    if (ctx.profileInPreparationCount !== 1) return false;
    if (ctx.tabsRendered.length > 1) return false;
    if (ctx.tabPanelsRendered > 1) return false;
    return true;
  }

  if (ctx.expectFull) {
    if (ctx.forbiddenStringsFound.length > 0) return false;
    if (ctx.profileInPreparationRendered) return false;
    if (ctx.helperTextFound) return false;
    if (ctx.internalNotesFound) return false;
    if (ctx.tabsRendered.length < 5) return false;
    return true;
  }

  return ctx.forbiddenStringsFound.length === 0;
}

/**
 * Apply DOM failure to external owner readiness — hard fail; cannot override.
 */
export function applyDomFailureToExternalOwnerReadiness(externalOwnerScore, domScan) {
  if (!externalOwnerScore) return { pass: false, blockers: ["no_external_owner_score"] };
  if (domScan?.externalQualityLockPass) {
    return { ...externalOwnerScore, domVerified: true };
  }
  const blockers = [...(externalOwnerScore.blockers || []), "external_dom_quality_lock_fail"];
  if (domScan?.forbiddenStringsFound?.length) {
    blockers.push(`forbidden_strings:${domScan.forbiddenStringsFound.join(",")}`);
  }
  if (domScan?.emptyCardsFound) blockers.push(`empty_cards:${domScan.emptyCardsFound}`);
  if (domScan?.helperTextFound) blockers.push("helper_text_visible");
  if (domScan?.internalNotesFound) blockers.push("internal_notes_visible");
  return {
    ...externalOwnerScore,
    pass: false,
    numericScore: Math.min(externalOwnerScore.numericScore ?? 100, 40),
    domVerified: false,
    domQualityLockPass: false,
    blockers: [...new Set(blockers)],
    categories: [...new Set([...(externalOwnerScore.categories || []), "blocked_by_external_dom"])],
  };
}

export function evaluateBrandExternalQualityLock(brand, renderedHtml, options = {}) {
  // Prefer API-resolved display state when present. Re-resolving without Brand Basics
  // approval fields incorrectly downgrades active_profile_ready brands to draft/locked.
  const hasApiDisplayState =
    brand?.brandExplorerDisplayState != null || brand?.shouldRenderFullProfile != null;
  const displayMeta = hasApiDisplayState
    ? {
        brandExplorerDisplayState: brand.brandExplorerDisplayState,
        shouldRenderFullProfile: brand.shouldRenderFullProfile === true,
        shouldSuppressIncompleteExternalSections: brand.shouldSuppressIncompleteExternalSections,
        shouldHideExternalProfile: brand.shouldHideExternalProfile,
        completeness: brand.brandExplorerDisplayCompleteness || null,
        blockers: brand.brandExplorerDisplayBlockers || [],
      }
    : resolveBrandExplorerDisplayState(brand, options);

  const shouldRenderFullProfile = shouldRenderFullBrandExplorerProfile({
    ...brand,
    brandExplorerDisplayState: displayMeta.brandExplorerDisplayState,
    shouldRenderFullProfile:
      displayMeta.shouldRenderFullProfile === true || brand?.shouldRenderFullProfile === true,
  });
  const expectLocked = !shouldRenderFullProfile;
  const expectFull = shouldRenderFullProfile;

  const domScan = scanExternalQualityLockHtml(renderedHtml, {
    companyValidated:
      brand?.governance?.companyValidated === true ||
      brand?.brandExplorerDisplayCompleteness?.companyValidated === true,
    expectLocked,
    expectFull,
  });

  const actualRenderedFullProfile =
    domScan.tabsRenderedExternally.length >= 5 && !domScan.profileInPreparationRendered;

  return {
    brandSlug: options.brandSlug || brand?.slug || "",
    displayState: displayMeta.brandExplorerDisplayState,
    shouldRenderFullProfile,
    actualRenderedFullProfile,
    tabsRenderedExternally: domScan.tabsRenderedExternally,
    forbiddenStringsFound: domScan.forbidden_strings_found,
    forbiddenMatches: domScan.matches,
    emptyCardsFound: domScan.empty_cards_found,
    helperTextFound: domScan.helper_text_found,
    internalNotesFound: domScan.internal_notes_found,
    profileInPreparationRendered: domScan.profileInPreparationRendered,
    profileInPreparationCount: domScan.profile_in_preparation_count,
    externalQualityLockPass: domScan.externalQualityLockPass,
    displayMeta,
    domScan,
    allowedNextAction: resolveAllowedNextAction(displayMeta, domScan),
  };
}

function resolveAllowedNextAction(displayMeta, domScan) {
  if (!domScan.externalQualityLockPass) return "fix_external_dom_quality_lock";
  if (displayMeta.brandExplorerDisplayState === "hidden_incomplete") return "complete_asset_pack_and_presentation";
  if (displayMeta.brandExplorerDisplayState === "draft_applied_with_defects") return "remediation_before_founder_review";
  if (displayMeta.brandExplorerDisplayState === "founder_review_ready") return "founder_visual_review";
  if (FULL_PROFILE_DISPLAY_STATES.includes(displayMeta.brandExplorerDisplayState)) {
    return "maintain_active_profile";
  }
  return "audit_display_state";
}

export { FULL_PROFILE_DISPLAY_STATES, resolveBrandExplorerDisplayState, shouldRenderFullBrandExplorerProfile };
