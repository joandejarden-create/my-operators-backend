/**
 * Founder-only Sheraton Guadalajara Expo drawer comparison inventory.
 * LEFT = original (no Brief v2 / Fact Selection). RIGHT = proposed cleaned.
 * Does not mutate publication packages or Airtable.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { prepareOpportunityForUniversalPresentation } from "./gdi-universal-presentation-pipeline-v1.js";
import { createGdiPublicationPackageStore } from "../publication-package/gdi-publication-package-store-v1.js";

export const GDI_SHERATON_DRAWER_COMPARISON_V1 =
  "gdi-sheraton-drawer-comparison-v1";

export const SHERATON_GUADALAJARA_EXPO_HOTEL_ID = "recCAGqkERSXgiku4";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(__dirname, "../../..");

const FULL_E2E_VERSION =
  "gdi_pub_recCAGqkERSXgiku4_20261113_full_e2e_v1";

function text(v) {
  return String(v == null ? "" : v).trim();
}

function oppId(o = {}) {
  return text(o.id || o.opportunityId || o.discoveryId);
}

function titleOf(o = {}) {
  return (
    text(o.displayTitle) ||
    text(o.drawerHeaderTitle) ||
    text(o.title) ||
    text(o.organizationName) ||
    oppId(o) ||
    "Untitled"
  );
}

function cloneRaw(o) {
  return JSON.parse(JSON.stringify(o || {}));
}

function stripPipelineMarks(o) {
  const next = cloneRaw(o);
  delete next._briefV2Applied;
  delete next._briefV2Score;
  delete next._briefV2Changes;
  delete next._factSelectionApplied;
  delete next.briefV2;
  delete next.factSelectionV1;
  delete next.gdiCustomerPresentationSanitizedV1;
  delete next.publishedPresentationLocked;
  delete next.gdiPublishedInventoryRegistry;
  return next;
}

function readJson(file) {
  try {
    if (!fs.existsSync(file)) return null;
    return JSON.parse(fs.readFileSync(file, "utf8"));
  } catch {
    return null;
  }
}

function loadLocalOpportunities(hotelId) {
  const file = path.join(
    REPO_ROOT,
    "data/group-demand-intelligence/hotels",
    hotelId,
    "opportunities.json",
  );
  const doc = readJson(file);
  const ops = Array.isArray(doc?.opportunities) ? doc.opportunities : [];
  return { file, opportunities: ops, count: ops.length };
}

function loadPackageOpportunities(hotelId, publicationVersion) {
  const store = createGdiPublicationPackageStore();
  const loaded = store.loadPackage(hotelId, publicationVersion);
  if (!loaded.ok) {
    return {
      ok: false,
      error: loaded.error,
      publicationVersion,
      opportunities: [],
      count: 0,
    };
  }
  const ops = Array.isArray(loaded.package?.opportunities)
    ? loaded.package.opportunities
    : [];
  return {
    ok: true,
    publicationVersion,
    publicationId: loaded.package.publicationId,
    contentFingerprintSha256: loaded.package.contentFingerprintSha256,
    lifecycleState: loaded.package.lifecycleState,
    opportunities: ops,
    count: ops.length,
    path: loaded.path,
  };
}

function classifyStatus({ inStaged, inLocal, inHistorical }) {
  if (inStaged && inLocal && inHistorical) return "STAGED_LOCAL_AND_HISTORICAL";
  if (inStaged && inLocal) return "STAGED_AND_LOCAL";
  if (inStaged && inHistorical) return "STAGED_AND_HISTORICAL";
  if (inLocal && inHistorical) return "LOCAL_AND_HISTORICAL_NOT_STAGED";
  if (inStaged) return "CURRENT_STAGED";
  if (inLocal) return "LOCAL_RESEARCH_NOT_STAGED";
  if (inHistorical) return "HISTORICAL_FULL_E2E_ONLY";
  return "UNKNOWN";
}

function pickCanonicalRaw({ stagedOpp, localOpp, historicalOpp, status }) {
  if (stagedOpp) {
    return {
      raw: stagedOpp,
      contentSource: "CURRENT_STAGED_PACKAGE",
      status,
    };
  }
  if (localOpp) {
    return {
      raw: localOpp,
      contentSource: "LOCAL_RESEARCH_STORE",
      status,
    };
  }
  return {
    raw: historicalOpp,
    contentSource: "HISTORICAL_FULL_E2E_PACKAGE",
    status,
  };
}

function fieldBlob(o = {}) {
  const keys = [
    "whatIsHappening",
    "potentialHotelBusiness",
    "potentialBusiness",
    "whyThisHotel",
    "whyActNow",
    "recommendedNextStep",
    "recommendedAction",
    "factsAndUncertainties",
    "summaryWhat",
  ];
  return keys.map((k) => text(o[k])).join("\n");
}

function summarizeChanges(before, after) {
  const fields = [
    ["whatIsHappening", "What is happening"],
    ["potentialHotelBusiness", "Potential hotel business"],
    ["potentialBusiness", "Potential business"],
    ["whyThisHotel", "Why this hotel"],
    ["whyActNow", "Why act now"],
    ["recommendedNextStep", "Recommended next step"],
    ["recommendedAction", "Recommended action"],
  ];
  const changed = [];
  for (const [key, label] of fields) {
    const a = text(before?.[key]);
    const b = text(after?.[key]);
    if (a !== b && (a || b)) {
      changed.push({
        field: key,
        label,
        kind: !a && b ? "ADDED" : a && !b ? "REMOVED" : "REVISED",
      });
    }
  }
  const beforeSources = Array.isArray(before?.sources) ? before.sources.length : 0;
  const afterSources = Array.isArray(after?.sources) ? after.sources.length : 0;
  const factSel = after?.factSelectionV1 || null;
  const brief = after?.briefV2 || null;
  return {
    changedFieldCount: changed.length,
    changedFields: changed,
    sourcesBefore: beforeSources,
    sourcesAfter: afterSources,
    briefV2Applied: Boolean(after?._briefV2Applied || brief),
    factSelectionApplied: Boolean(after?._factSelectionApplied || factSel),
    level1Count: Array.isArray(factSel?.level1) ? factSel.level1.length : null,
    level2Count: Array.isArray(factSel?.level2) ? factSel.level2.length : null,
    textLengthDelta: fieldBlob(after).length - fieldBlob(before).length,
  };
}

/**
 * Build deduplicated founder inventory for Sheraton Guadalajara Expo.
 */
export function buildSheratonDrawerComparisonInventory(opts = {}) {
  const hotelId = text(opts.hotelId) || SHERATON_GUADALAJARA_EXPO_HOTEL_ID;
  if (hotelId !== SHERATON_GUADALAJARA_EXPO_HOTEL_ID) {
    return {
      ok: false,
      error: "HOTEL_NOT_AUTHORIZED_FOR_COMPARISON",
      message:
        "This founder drawer comparison is authorized only for Sheraton Guadalajara Expo (recCAGqkERSXgiku4).",
      hotelId,
    };
  }

  const store = createGdiPublicationPackageStore();
  const stagedPointer = store.readPointer(hotelId, "STAGED");
  const stagedVersion = text(stagedPointer?.publicationVersion);
  if (!stagedVersion) {
    return {
      ok: false,
      error: "STAGED_POINTER_MISSING",
      hotelId,
    };
  }

  const stagedPkg = loadPackageOpportunities(hotelId, stagedVersion);
  const historicalPkg = loadPackageOpportunities(hotelId, FULL_E2E_VERSION);
  const local = loadLocalOpportunities(hotelId);

  const stagedById = new Map(
    (stagedPkg.opportunities || []).map((o) => [oppId(o), o]).filter(([id]) => id),
  );
  const localById = new Map(
    (local.opportunities || []).map((o) => [oppId(o), o]).filter(([id]) => id),
  );
  const histById = new Map(
    (historicalPkg.opportunities || [])
      .map((o) => [oppId(o), o])
      .filter(([id]) => id),
  );

  const allIds = new Set([
    ...stagedById.keys(),
    ...localById.keys(),
    ...histById.keys(),
  ]);

  const statusCounts = {};
  const items = [];
  for (const id of [...allIds].sort()) {
    const stagedOpp = stagedById.get(id) || null;
    const localOpp = localById.get(id) || null;
    const historicalOpp = histById.get(id) || null;
    const inStaged = Boolean(stagedOpp);
    const inLocal = Boolean(localOpp);
    const inHistorical = Boolean(historicalOpp);
    const status = classifyStatus({ inStaged, inLocal, inHistorical });
    statusCounts[status] = (statusCounts[status] || 0) + 1;
    const picked = pickCanonicalRaw({
      stagedOpp,
      localOpp,
      historicalOpp,
      status,
    });
    items.push({
      id,
      title: titleOf(picked.raw),
      organizationName: text(picked.raw?.organizationName || picked.raw?.displayCompanyName),
      status,
      contentSource: picked.contentSource,
      inCurrentStaged: inStaged,
      inLocalResearch: inLocal,
      inHistoricalFullE2e: inHistorical,
      customerVisibleEligible:
        picked.raw?.customerVisibleEligible !== false &&
        picked.raw?.researchWatchOnly !== true,
      raw: picked.raw,
    });
  }

  items.sort((a, b) => {
    const rank = (s) =>
      s.startsWith("STAGED") || s === "CURRENT_STAGED"
        ? 0
        : s.includes("LOCAL")
          ? 1
          : 2;
    const d = rank(a.status) - rank(b.status);
    if (d !== 0) return d;
    return String(a.title).localeCompare(String(b.title));
  });

  return {
    ok: true,
    version: GDI_SHERATON_DRAWER_COMPARISON_V1,
    hotelId,
    hotelName: "Sheraton Guadalajara Expo",
    surface: "FOUNDER_INTERNAL_DRAWER_COMPARISON",
    customerRouteSafe: false,
    publicationMutation: false,
    airtableWrites: false,
    counts: {
      unionDeduped: items.length,
      currentStagedPackage: stagedPkg.count,
      localResearchStore: local.count,
      historicalFullE2ePackage: historicalPkg.count,
      byStatus: statusCounts,
    },
    staged: {
      publicationVersion: stagedVersion,
      publicationId: stagedPointer?.publicationId || stagedPkg.publicationId || null,
      contentFingerprintSha256:
        stagedPointer?.contentFingerprintSha256 ||
        stagedPkg.contentFingerprintSha256 ||
        null,
      lifecycleState: stagedPointer?.lifecycleState || stagedPkg.lifecycleState || null,
      opportunityCount: stagedPkg.count,
    },
    historicalFullE2e: {
      publicationVersion: FULL_E2E_VERSION,
      opportunityCount: historicalPkg.count,
      note: "Prior 62-op package retained for reconciliation only; not substituted as sole inventory.",
    },
    localResearch: {
      path: local.file,
      opportunityCount: local.count,
    },
    items,
  };
}

/**
 * When packages lack secondaryContact but the primary's verified About/team
 * page lists another named person, attach that person as an optional secondary.
 * Never invents email/phone — profileUrl points at the same public page.
 */
async function enrichSecondaryFromTeamPageEvidence(opportunity = {}, opts = {}) {
  if (!opportunity || typeof opportunity !== "object") return opportunity;
  if (opportunity.secondaryContact?.name) return opportunity;
  if (opportunity.scalingTwoContactPresentation?.secondary?.customerFacingNamed) {
    return opportunity;
  }

  const {
    classifyPersonSourceUrl,
    collectPersonSourceUrlCandidates,
    normalizePersonSourceUrl,
  } = await import("./gdi-person-source-link-v1.js");
  const { fetchTeamPageSecondaryCandidates } = await import(
    "./gdi-team-page-secondary-candidates-v1.js"
  );
  const { attachSecondaryNamedContact } = await import(
    "../secondary-named-contact-v1.js"
  );
  const { applyUnifiedWhoToContactStandard } = await import(
    "../unified-who-to-contact-v1.js"
  );

  const primarySlot = opportunity.scalingTwoContactPresentation?.primary || {};
  const primaryName =
    text(primarySlot.name) ||
    text(opportunity.primaryContact?.name) ||
    text(opportunity.contactName);
  if (!primaryName) return opportunity;

  const org =
    text(primarySlot.organization) ||
    text(opportunity.displayCompanyName) ||
    text(opportunity.organizationName);

  const candidates = collectPersonSourceUrlCandidates(
    {
      linkedInUrl: primarySlot.linkedInUrl,
      profileUrl: primarySlot.profileUrl || primarySlot.personSourceUrl,
      sourceUrl: primarySlot.personSourceUrl,
      officialContactUrl: primarySlot.officialContactUrl,
    },
    opportunity,
  );

  const aboutUrls = candidates.filter((url) => {
    const kind = classifyPersonSourceUrl(url);
    return kind === "ORG_ABOUT_OR_TEAM" || kind === "PROFILE_OR_SOURCE";
  });

  for (const url of aboutUrls) {
    const fetched = await fetchTeamPageSecondaryCandidates(url, {
      primaryName,
      organization: org,
      timeoutMs: opts.timeoutMs || 7000,
    });
    if (!fetched.ok || !fetched.candidates?.length) continue;

    let next = attachSecondaryNamedContact(opportunity, {
      force: true,
      allowSavedEvidence: true,
      candidates: fetched.candidates,
    });
    if (!next.secondaryContact?.name) continue;

    // Prefer the team page as secondary profile/source evidence.
    next.secondaryContact = {
      ...next.secondaryContact,
      profileUrl:
        next.secondaryContact.profileUrl || normalizePersonSourceUrl(url),
      sourceUrl: next.secondaryContact.sourceUrl || normalizePersonSourceUrl(url),
      secondaryFromTeamPageUrl: normalizePersonSourceUrl(url),
      secondaryFromTeamPageVersion: "gdi-team-page-secondary-candidates-v1",
    };
    next = applyUnifiedWhoToContactStandard(next, { force: true });
    return next;
  }
  return opportunity;
}

async function attachVerifiedPersonSourceLinks(opportunity = {}, opts = {}) {
  if (!opportunity || typeof opportunity !== "object") return opportunity;
  const { resolveVerifiedPersonSourceLink } = await import(
    "./gdi-person-source-link-v1.js"
  );
  const next = cloneRaw(opportunity);
  const two = next.scalingTwoContactPresentation;
  if (two?.primary?.customerFacingNamed) {
    const resolved = await resolveVerifiedPersonSourceLink(two.primary, next, {
      timeoutMs: opts.timeoutMs || 7000,
    });
    if (resolved.ok && resolved.link) {
      two.primary = {
        ...two.primary,
        personSourceUrl: resolved.link.url,
        personSourceLabel: resolved.link.label,
        personSourceKind: resolved.link.kind,
        personSourceVerifiedVia: resolved.link.verifiedVia,
        personSourceSkipReason: null,
      };
      // Keep legacy fields in sync when LinkedIn/profile verified
      if (/linkedin/i.test(resolved.link.kind)) {
        two.primary.linkedInUrl = resolved.link.url;
        // Avoid UI falling back to an unverified About page beside LinkedIn.
        if (
          two.primary.profileUrl &&
          !/linkedin\.com\/in\//i.test(String(two.primary.profileUrl))
        ) {
          two.primary.profileUrl = null;
        }
      } else {
        two.primary.profileUrl = resolved.link.url;
      }
    } else {
      two.primary = {
        ...two.primary,
        personSourceUrl: null,
        personSourceLabel: null,
        personSourceKind: null,
        personSourceVerifiedVia: null,
        personSourceSkipReason: resolved.reason || "NO_VERIFIED_LINK",
        // Suppress UI fallback to unverified / broken profile URLs.
        linkedInUrl: /linkedin\.com\/in\//i.test(String(two.primary.linkedInUrl || ""))
          ? two.primary.linkedInUrl
          : null,
        profileUrl: null,
      };
    }
  }
  if (two?.secondary?.customerFacingNamed) {
    const resolved = await resolveVerifiedPersonSourceLink(two.secondary, next, {
      timeoutMs: opts.timeoutMs || 7000,
    });
    if (resolved.ok && resolved.link) {
      two.secondary = {
        ...two.secondary,
        personSourceUrl: resolved.link.url,
        personSourceLabel: resolved.link.label,
        personSourceKind: resolved.link.kind,
        personSourceVerifiedVia: resolved.link.verifiedVia,
        personSourceSkipReason: null,
      };
      if (/linkedin/i.test(resolved.link.kind)) {
        two.secondary.linkedInUrl = resolved.link.url;
      } else {
        two.secondary.profileUrl = resolved.link.url;
      }
    } else {
      two.secondary = {
        ...two.secondary,
        personSourceUrl: null,
        personSourceLabel: null,
        personSourceKind: null,
        personSourceVerifiedVia: null,
        personSourceSkipReason: resolved.reason || "NO_VERIFIED_LINK",
        linkedInUrl: /linkedin\.com\/in\//i.test(String(two.secondary.linkedInUrl || ""))
          ? two.secondary.linkedInUrl
          : null,
        profileUrl: null,
      };
    }
  }
  next.scalingTwoContactPresentation = two;
  return next;
}

export async function projectDrawerComparisonPair(rawOpportunity, opts = {}) {
  const hotelId = text(opts.hotelId) || SHERATON_GUADALAJARA_EXPO_HOTEL_ID;
  const base = stripPipelineMarks(rawOpportunity);
  let before = prepareOpportunityForUniversalPresentation(cloneRaw(base), {
    hotelId,
    applyBriefV2: false,
    applyFactSelection: false,
  });
  let after = prepareOpportunityForUniversalPresentation(cloneRaw(base), {
    hotelId,
    applyBriefV2: true,
    applyFactSelection: true,
  });
  // Apply native public channel recovery only on AFTER so LEFT stays the
  // original packaged contact and RIGHT shows cleaned hierarchy (A–G demos).
  if (opts.recoverPublicContactChannels !== false) {
    const { recoverPublicContactChannels } = await import(
      "./gdi-public-contact-channel-hierarchy-v1.js"
    );
    const { applyUnifiedWhoToContactStandard } = await import(
      "../unified-who-to-contact-v1.js"
    );
    const recovered = await recoverPublicContactChannels(after, {
      timeoutMs: opts.timeoutMs || 7000,
      maxUrls: 3,
    });
    // Always rebuild two-contact presentation after recovery attempt so
    // primaryContact.email/phone/org land on scalingTwoContactPresentation
    // (stale presentation was the visual miss when recovery ran but UI read
    // the pre-recovery slot object).
    after = applyUnifiedWhoToContactStandard(recovered.opportunity || after, {
      force: true,
    });
    after.publicChannelRecoveryMeta = {
      reason: recovered.reason || null,
      changed: Boolean(recovered.changed),
      found: recovered.found || null,
    };
  }
  // Secondary from team pages only when evidence lists a distinct person —
  // never invent a second contact merely to fill the slot.
  if (opts.enrichSecondaryFromTeamPage === true) {
    before = await enrichSecondaryFromTeamPageEvidence(before, opts);
    after = await enrichSecondaryFromTeamPageEvidence(after, opts);
  }
  if (opts.verifyPersonLinks !== false) {
    before = await attachVerifiedPersonSourceLinks(before, opts);
    after = await attachVerifiedPersonSourceLinks(after, opts);
  }
  return {
    before,
    after,
    changeSummary: summarizeChanges(before, after),
  };
}

export async function getSheratonDrawerComparisonPayload(opts = {}) {
  const inventory = buildSheratonDrawerComparisonInventory(opts);
  if (!inventory.ok) return inventory;

  const wantId = text(opts.opportunityId);
  const selected =
    (wantId && inventory.items.find((i) => i.id === wantId)) ||
    inventory.items.find((i) => i.inCurrentStaged) ||
    inventory.items[0] ||
    null;

  if (!selected) {
    return {
      ok: false,
      error: "NO_OPPORTUNITIES",
      hotelId: inventory.hotelId,
      counts: inventory.counts,
    };
  }

  const pair = await projectDrawerComparisonPair(selected.raw, {
    hotelId: inventory.hotelId,
    verifyPersonLinks: opts.verifyPersonLinks !== false,
  });

  const personLink =
    pair.after?.scalingTwoContactPresentation?.primary?.personSourceUrl ||
    pair.before?.scalingTwoContactPresentation?.primary?.personSourceUrl ||
    null;

  return {
    ok: true,
    version: GDI_SHERATON_DRAWER_COMPARISON_V1,
    hotelId: inventory.hotelId,
    hotelName: inventory.hotelName,
    surface: inventory.surface,
    customerRouteSafe: false,
    publicationMutation: false,
    airtableWrites: false,
    counts: inventory.counts,
    staged: inventory.staged,
    historicalFullE2e: inventory.historicalFullE2e,
    localResearch: inventory.localResearch,
    selector: inventory.items.map((i) => ({
      id: i.id,
      title: i.title,
      organizationName: i.organizationName,
      status: i.status,
      contentSource: i.contentSource,
      inCurrentStaged: i.inCurrentStaged,
      inLocalResearch: i.inLocalResearch,
      inHistoricalFullE2e: i.inHistoricalFullE2e,
      customerVisibleEligible: i.customerVisibleEligible,
    })),
    selected: {
      id: selected.id,
      title: selected.title,
      status: selected.status,
      contentSource: selected.contentSource,
      inCurrentStaged: selected.inCurrentStaged,
      inLocalResearch: selected.inLocalResearch,
      inHistoricalFullE2e: selected.inHistoricalFullE2e,
      personSourceUrl: personLink,
    },
    before: pair.before,
    after: pair.after,
    changeSummary: pair.changeSummary,
  };
}

export default {
  GDI_SHERATON_DRAWER_COMPARISON_V1,
  SHERATON_GUADALAJARA_EXPO_HOTEL_ID,
  buildSheratonDrawerComparisonInventory,
  projectDrawerComparisonPair,
  getSheratonDrawerComparisonPayload,
};
