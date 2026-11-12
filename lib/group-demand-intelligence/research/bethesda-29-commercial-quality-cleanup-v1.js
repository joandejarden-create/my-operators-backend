/**
 * Bethesda 29 — commercial quality cleanup + category restoration.
 *
 * Updates research-stage reconstructed bodies only.
 * Never writes opportunities.json / share registries / paid research.
 * Never promotes Sales Ready without verified named commercial contact + hotel-fit thesis.
 */

import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  BETHESDA_CANONICAL_HOTEL_ID,
  BETHESDA_CANONICAL_HOTEL_NAME,
} from "./bethesda-full-demand-database-reconciliation-v1.js";
import { BETHESDA_29_ATTESTED_IDS } from "./bethesda-historical-attestation-bag-v1.js";
import {
  CURATED_HISTORICAL_ATTESTATIONS,
  RECOVERY_CATEGORY,
} from "./bethesda-historical-opportunity-recovery-v1.js";
import {
  applyHistoricalCommercialCategoryProjection,
  buildCategoryCrosswalkRow,
  BETHESDA_HISTORICAL_COMMERCIAL_CATEGORY_MAP_V1,
} from "./bethesda-historical-commercial-category-map-v1.js";
import {
  classifyOpportunityTypeFilter,
  OPPORTUNITY_TYPE_FILTER_LABEL,
  countByOpportunityTypeFilter,
} from "../opportunity-type-filter-v1.js";
import {
  GDI_RESEARCH_STAGE_PERSISTENCE_V1,
  RECOVERY_DISPOSITION,
  persistResearchStageOpportunityBag,
  researchStageHotelDir,
  writeJsonAtomic,
} from "./gdi-research-stage-persistence-v1.js";
import {
  BETHESDA_29_STANDARDIZED_TITLES,
  isGenericHistoricalTitle,
  resolveStandardizedTitle,
} from "./bethesda-29-title-standardization-v1.js";
import { ACCEPTABLE_59_ROW_SHA256S } from "./gdi-hotel-store-integrity-guard-v1.js";

export const BETHESDA_29_COMMERCIAL_QUALITY_CLEANUP_V1 =
  "gdi-bethesda-29-commercial-quality-cleanup-v1";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const FIXTURE_REL = "fixtures/gdi/bethesda-29-historical-reconstruction-v1.json";
const REPORT_DIR_REL = "reports/gdi-bethesda-29-commercial-quality-cleanup-v1";
const EXPECTED_STORE_SHA = ACCEPTABLE_59_ROW_SHA256S[0];
const ACCEPTABLE_STORE_SHAS = ACCEPTABLE_59_ROW_SHA256S;

function loadStagedOrFixtureBag({ root = ROOT } = {}) {
  const staged = path.join(
    researchStageHotelDir(BETHESDA_CANONICAL_HOTEL_ID, { root }),
    "reconstructed-opportunities-v1.json"
  );
  if (fs.existsSync(staged)) {
    return JSON.parse(fs.readFileSync(staged, "utf8"));
  }
  const fixture = path.join(root, FIXTURE_REL);
  if (fs.existsSync(fixture)) {
    return JSON.parse(fs.readFileSync(fixture, "utf8"));
  }
  return null;
}

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function curatedById() {
  return new Map(CURATED_HISTORICAL_ATTESTATIONS.map((r) => [r.id, r]));
}

function isGenericBatchTitle(title, org) {
  return isGenericHistoricalTitle(title, org);
}

function titleFor(row, curated, projected) {
  const org = text(row.organizationName) || text(curated?.organizationName) || row.id;
  // Prefer evidence-grounded standardized titles over org—category templates.
  const standardized = resolveStandardizedTitle({
    ...row,
    organizationName: org,
    title: curated?.title || row.title,
  });
  if (standardized && !isGenericHistoricalTitle(standardized, org)) {
    return standardized;
  }
  const existing = text(curated?.title || row.title);
  if (existing && !isGenericBatchTitle(existing, org)) return existing;

  // Last-resort fallback: organization only — never bolt taxonomy category onto the title.
  return org;
}

function overviewFor(row, curated, projected) {
  const org = text(row.organizationName) || "Organization";
  const thesis = text(curated?.demandThesis || curated?.evidenceNotes);
  if (thesis) return thesis;

  const existing = text(row.whatIsHappening);
  if (
    existing &&
    !/^Historical attestation only for /i.test(existing) &&
    existing !== org &&
    existing.length > 40
  ) {
    return existing;
  }

  const cat = row.researchCategory;
  const discovery = text(curated?.discoveryDate || row.historicalAttestationRef?.discoveryDate);
  const when = discovery ? ` (historical research ${discovery})` : "";

  if (cat === RECOVERY_CATEGORY.LOCAL_CATERING) {
    return `${org} was historically researched as a local meetings / catering prospect for Bethesda Marriott${when}. Original research body is missing from recoverable artifacts; independent current-cycle demand dates and a verified commercial contact are required before outreach.`;
  }
  if (cat === RECOVERY_CATEGORY.POTENTIAL_PARTNERSHIPS) {
    return `${org} was historically researched as a potential partnership / referral channel for Bethesda Marriott${when}. No contractual preferred-hotel or planner agreement is evidenced in recovered artifacts; treat as relationship development, not an existing partnership.`;
  }
  if (cat === RECOVERY_CATEGORY.SPORTS_HOUSING) {
    return `${org} was historically researched as sports / tournament lodging demand near Montgomery County${when}. Confirm housing partner path, dates, and Bethesda Marriott eligibility before pursuit.`;
  }
  if (cat === RECOVERY_CATEGORY.RELOCATION_EXTENDED_STAY) {
    return `${org} was historically researched as relocation / extended-stay demand tied to the NIH / Bethesda corridor${when}. Programme activity is attested; a named commercial entry point and booking path remain unresolved.`;
  }
  if (cat === RECOVERY_CATEGORY.ASSOCIATION_EVENTS) {
    return `${org} was historically researched as association / conference demand relevant to the Bethesda corridor${when}. Verify host venue, dates, and whether Bethesda Marriott is primary, overflow, or not eligible.`;
  }
  if (cat === RECOVERY_CATEGORY.CORPORATE_GROUP) {
    return `${org} was historically researched as corporate / group meetings demand for the Bethesda corridor${when}. Confirm meeting activity, lodging need, and a verified commercial contact before outreach.`;
  }
  return `${org} — historical research attestation retained; commercial demand thesis incomplete in recovered artifacts.`;
}

function hotelFitFor(row, curated, projected) {
  const curatedFit = text(curated?.hotelFitRationale);
  if (curatedFit) return curatedFit;

  const disp = text(row.historicalRecoveryDisposition);
  if (disp === "OVERFLOW_WATCH" || disp === RECOVERY_DISPOSITION.OVERFLOW_WATCH) {
    return "Competitor-host activity near the Bethesda corridor — do not treat as available Bethesda Marriott demand until independent overflow / guestroom need is verified.";
  }
  if (disp === "PARTNERSHIP_WATCH" || disp === RECOVERY_DISPOSITION.PARTNERSHIP_WATCH) {
    if (projected.preferredLodgingPartnershipV1) {
      return "Independent venue / destination partner near Bethesda — investigate preferred-hotel lodging referral for visiting guests; relationship not contractually evidenced.";
    }
    if (projected.professionalEventPlannerPartnershipV1) {
      return "Professional planner / producer that may route private or social events into the Bethesda corridor — introduction path only; no established hotel partnership evidenced.";
    }
    return "Travel-trade / DMC intermediary that may route association or corporate programmes into the Bethesda / NIH corridor — partnership watch only.";
  }
  if (projected.demandSegmentId === "local_catering_day_meetings") {
    return "Bethesda Marriott banquet and day-meeting inventory is a plausible local venue for chapter, civic, and community programmes in the Bethesda–Rockville corridor when independent demand and a commercial contact are verified. Guestrooms are not required for a catering-led opportunity.";
  }
  if (projected.demandSegmentId === "corporate_celebrations_private_events") {
    return "Bethesda Marriott can compete for celebration / gala / awards banquet space when the programme is not already competitor-committed; verify open venue path and planner contact before pursuit.";
  }
  if (projected.demandSegmentId === "relocation_extended_stay") {
    return "Bethesda Marriott sits adjacent to NIH / federal research employment corridors — plausible extended-stay / temporary housing partner when a booking path and named entry point are verified.";
  }
  if (projected.demandSegmentId === "sports_entertainment") {
    return "Montgomery County lodging with transport to Boyds / SoccerPlex-class venues — team housing / overflow pattern; not an automatic host-hotel win.";
  }
  if (projected.demandSegmentId === "corporate_meetings_incentives") {
    return "Same-market corporate / scientific meeting activity can support Bethesda Marriott guestrooms and meeting space when the buyer path and dates are verified.";
  }
  if (projected.demandSegmentId === "associations_conferences_events") {
    return "Association / conference demand in the DMV may support primary blocks or overflow lodging at Bethesda Marriott depending on host venue and housing partner — eligibility must be verified case by case.";
  }
  return "Bethesda Marriott / NIH corridor relevance was historically indicated; detailed hotel-fit narrative remains incomplete in recovered artifacts.";
}

function proposedUseFor(projected, row) {
  if (projected.preferredLodgingPartnershipV1 || projected.professionalEventPlannerPartnershipV1) {
    return "Partnership / referral development — not a single confirmed booking block.";
  }
  if (projected.demandSegmentId === "local_catering_day_meetings") {
    return "Local catering, day meetings, banquet or reception use at Bethesda Marriott (subject to evidence). Guestrooms optional.";
  }
  if (projected.demandSegmentId === "corporate_celebrations_private_events") {
    return "Celebration / gala / awards banquet or reception space (subject to open venue path).";
  }
  if (projected.demandSegmentId === "relocation_extended_stay") {
    return "Extended-stay / temporary housing rooms for fellows, relocating staff, or visiting researchers (subject to evidence).";
  }
  if (projected.demandSegmentId === "sports_entertainment") {
    return "Team / tournament guestroom blocks with possible F&B; overflow pattern when housing partner routes demand.";
  }
  if (text(row.historicalRecoveryDisposition) === "OVERFLOW_WATCH") {
    return "Overflow / residual lodging watch only while primary event remains competitor-hosted.";
  }
  return "Group guestrooms and/or meeting space at Bethesda Marriott (subject to evidence).";
}

function nextActionFor(row, contact) {
  const disp = text(row.historicalRecoveryDisposition);
  if (disp === "OVERFLOW_WATCH") {
    return "Retain Overflow Watch — do not pursue as primary Sales Ready demand. Confirm whether any residual guestroom / hospitality need is independently accessible to Bethesda Marriott.";
  }
  if (disp === "PARTNERSHIP_WATCH") {
    return "Continue Partnership Watch — identify a named commercial entry point and a specific referral mechanism; do not present an unestablished relationship as an existing partnership.";
  }
  if (contact?.contactClass === "FUNCTIONAL_INBOX" && contact?.email) {
    return `Research continuation — use recovered functional route (${contact.email}) to identify the named commercial owner; verify current demand before outreach.`;
  }
  if (contact?.usableRoute) {
    return "Research continuation — convert the recovered organization contact path into a named commercially relevant person, then confirm hotel-fit and timing.";
  }
  return "Research continuation — establish a verified named commercial contact and an evidence-backed Bethesda Marriott demand thesis before sales outreach.";
}

function maturityFor(row) {
  const disp = text(row.historicalRecoveryDisposition);
  if (disp === "OVERFLOW_WATCH") {
    return {
      commercialMaturity: "OVERFLOW_WATCH",
      commercialStagePill: "Overflow Watch",
      outreachReadiness: "OVERFLOW_WATCH",
      priorityPillLabel: "Overflow Watch",
      salesReady: false,
      accountDevelopmentEligible: false,
    };
  }
  if (disp === "PARTNERSHIP_WATCH") {
    return {
      commercialMaturity: "PARTNERSHIP_WATCH",
      commercialStagePill: "Partnership Watch",
      outreachReadiness: "PARTNERSHIP_WATCH",
      priorityPillLabel: "Partnership Watch",
      salesReady: false,
      accountDevelopmentEligible: false,
    };
  }
  // Historical reconstruction never auto-elevates to Sales Ready / AD without
  // verified named contact + independent demand thesis (both still missing on most rows).
  return {
    commercialMaturity: "RESEARCH_CONTINUATION",
    commercialStagePill: "Research Continuation",
    outreachReadiness: "RESEARCH_CONTINUATION",
    priorityPillLabel: "Research Continuation",
    salesReady: false,
    accountDevelopmentEligible: false,
  };
}

function scorecardFor(cleaned, before) {
  const contact = cleaned.contactRecovery || {};
  const missing = cleaned.missingResearchRequirements || [];
  const hasThesis =
    text(cleaned.whatIsHappening).length >= 80 &&
    !/^Historical attestation only for /i.test(cleaned.whatIsHappening);
  const hasFit = text(cleaned.hotelFitRationale).length >= 60;
  const hasAction = text(cleaned.recommendedAction).length >= 40;
  const named = contact.namedPerson === true;
  const filterOk =
    classifyOpportunityTypeFilter(cleaned) !== "OTHER_UNCLASSIFIED";
  const commercialBar =
    hasThesis &&
    hasFit &&
    hasAction &&
    filterOk &&
    (named || contact.usableRoute === true) &&
    !missing.includes("ORIGINAL_RESEARCH_BODY");

  return {
    id: cleaned.id,
    organizationName: cleaned.organizationName,
    originalCategory: before.researchCategory || null,
    correctedCategory: cleaned.opportunityTypeLabel,
    filterCategory: classifyOpportunityTypeFilter(cleaned),
    filterLabel: OPPORTUNITY_TYPE_FILTER_LABEL[classifyOpportunityTypeFilter(cleaned)],
    sourceEvidenceRecovered: (cleaned.sources || []).length,
    reconstructionStatus: cleaned.reconstructionStatus,
    bodyStatus: cleaned.bodyStatus,
    verifiedContacts: named
      ? "NAMED_PERSON"
      : contact.usableRoute
        ? contact.contactClass || "FUNCTIONAL_OR_ROUTE"
        : "UNVERIFIED",
    hotelFitQuality: hasFit ? "EVIDENCE_GROUNDED_OR_HONEST_CORRIDOR" : "INCOMPLETE",
    demandMaturity: cleaned.commercialMaturity,
    descriptionQuality: hasThesis ? "COMMERCIAL_NARRATIVE" : "ATTESTATION_PLACEHOLDER",
    nextActionQuality: hasAction ? "ACTIONABLE_RESEARCH_STEP" : "WEAK",
    missingResearch: missing.filter((m) => m !== "CUSTOMER_PUBLICATION_NOT_AUTHORIZED"),
    meetsCommercialQualityBar: commercialBar,
    salesReadyEligible: false,
    accountDevelopmentEligible: false,
    readinessDecision: cleaned.commercialMaturity,
    notes:
      commercialBar
        ? "Structurally commercial for research-stage internal use; not Sales Ready."
        : "Below full commercial qualification — preserve as research continuation / watch.",
  };
}

/**
 * Clean one historical research-stage opportunity for commercial presentation.
 */
export function cleanOneHistoricalOpportunity(row = {}, { curated = null } = {}) {
  const before = { ...row };
  const projected = applyHistoricalCommercialCategoryProjection(row);
  const contact = row.contactRecovery || {};
  const maturity = maturityFor(row);

  const title = titleFor(row, curated, projected);
  const what = overviewFor(row, curated, projected);
  const fit = hotelFitFor(row, curated, projected);
  const use = proposedUseFor(projected, row);
  const action = nextActionFor(row, contact);

  const cleaned = {
    ...projected,
    title,
    displayTitle: title,
    drawerHeaderTitle: title,
    whatIsHappening: what,
    summaryWhat: what,
    tileCustomerSummary: what,
    hotelFitRationale: fit,
    whyThisHotel: fit,
    summaryWhyHotel: fit,
    hotelOpportunityThesis: fit,
    localConnection: fit,
    proposedHotelUse: use,
    recommendedAction: action,
    nextBestAction: action,
    ...maturity,
    customerFacing: false,
    customerVisible: false,
    publicationApproved: false,
    publication: "NOT_AUTHORIZED",
    promoteToOpportunitiesJson: false,
    commercialCleanupVersion: BETHESDA_29_COMMERCIAL_QUALITY_CLEANUP_V1,
    commercialCleanupAt: new Date().toISOString(),
    inventedFields: false,
    paidResearch: false,
    furtherResearchRequired: true,
    unresolvedResearchGaps: (row.missingResearchRequirements || []).filter(
      (m) => m !== "CUSTOMER_PUBLICATION_NOT_AUTHORIZED"
    ),
    provenance: {
      ...(row.provenance || {}),
      commercialCleanup: BETHESDA_29_COMMERCIAL_QUALITY_CLEANUP_V1,
      categoryMap: BETHESDA_HISTORICAL_COMMERCIAL_CATEGORY_MAP_V1,
      fieldSources: {
        ...((row.provenance && row.provenance.fieldSources) || {}),
        title:
          BETHESDA_29_STANDARDIZED_TITLES[text(row.id || row.opportunityId)] === title
            ? "bethesda-29-title-standardization-v1"
            : curated?.title && !isGenericBatchTitle(curated.title, row.organizationName)
              ? "curated_attestation.title"
              : "commercial_cleanup_fallback_title",
        whatIsHappening: text(curated?.demandThesis || curated?.evidenceNotes)
          ? "curated_attestation.demandThesis_or_evidenceNotes"
          : "commercial_cleanup_historical_posture_narrative",
        hotelFitRationale: text(curated?.hotelFitRationale)
          ? "curated_attestation.hotelFitRationale"
          : "commercial_cleanup_corridor_fit_template",
        categoryFields: "bethesda-historical-commercial-category-map-v1",
      },
    },
  };

  // Keep missing list honest.
  const missing = new Set(row.missingResearchRequirements || []);
  if (!contact.namedPerson) missing.add("VERIFIED_NAMED_COMMERCIAL_CONTACT");
  if (text(cleaned.reconstructionStatus) === "STILL_MISSING_RESEARCH") {
    missing.add("ORIGINAL_RESEARCH_BODY");
  }
  missing.add("CUSTOMER_PUBLICATION_NOT_AUTHORIZED");
  cleaned.missingResearchRequirements = [...missing];

  cleaned.qualityScorecard = scorecardFor(cleaned, before);
  return cleaned;
}

export function buildBethesda29CommercialQualityCleanup({ root = ROOT } = {}) {
  const staged = loadStagedOrFixtureBag({ root });
  const rows = staged?.opportunities || [];
  const byId = new Map(rows.map((r) => [r.id || r.opportunityId, r]));
  const curatedMap = curatedById();

  const beforeDistribution = {};
  const afterRows = [];
  const crosswalk = [];
  const scorecards = [];

  for (const id of BETHESDA_29_ATTESTED_IDS) {
    const before = byId.get(id) || {
      id,
      organizationName: id,
      researchCategory: RECOVERY_CATEGORY.OTHER,
      whatIsHappening: "Missing reconstructed body",
      historicalRecoveryDisposition: "ATTESTED_BODY_MISSING",
      missingResearchRequirements: ["ORIGINAL_RESEARCH_BODY"],
      sources: [],
      contactRecovery: { namedPerson: false, usableRoute: false },
    };
    // Before filter as currently projected historically (recovery enum defect).
    const beforeProj = {
      ...before,
      opportunityType: before.researchCategory || before.opportunityType,
      demandSegmentId: before.demandSegmentId || null,
      preferredLodgingPartnershipV1: before.preferredLodgingPartnershipV1 === true,
      professionalEventPlannerPartnershipV1:
        before.professionalEventPlannerPartnershipV1 === true,
    };
    const bf = classifyOpportunityTypeFilter(beforeProj);
    beforeDistribution[bf] = (beforeDistribution[bf] || 0) + 1;

    const cleaned = cleanOneHistoricalOpportunity(before, {
      curated: curatedMap.get(id) || null,
    });
    afterRows.push(cleaned);
    crosswalk.push(buildCategoryCrosswalkRow(beforeProj, cleaned));
    scorecards.push(cleaned.qualityScorecard);
  }

  const afterDistribution = countByOpportunityTypeFilter(afterRows);
  const maturityCounts = {
    SALES_READY: 0,
    ACCOUNT_DEVELOPMENT: 0,
    RESEARCH_CONTINUATION: 0,
    PARTNERSHIP_WATCH: 0,
    OVERFLOW_WATCH: 0,
    OTHER: 0,
  };
  for (const r of afterRows) {
    const m = text(r.commercialMaturity);
    if (maturityCounts[m] != null) maturityCounts[m] += 1;
    else maturityCounts.OTHER += 1;
  }

  const meetingBar = scorecards.filter((s) => s.meetsCommercialQualityBar).length;
  const needsPaidOrAdditional = scorecards.filter(
    (s) =>
      !s.meetsCommercialQualityBar ||
      (s.missingResearch || []).includes("VERIFIED_NAMED_COMMERCIAL_CONTACT") ||
      (s.missingResearch || []).includes("ORIGINAL_RESEARCH_BODY") ||
      (s.missingResearch || []).includes("INDEPENDENT_OVERFLOW_DEMAND_EVIDENCE")
  ).length;

  const oppPath = path.join(
    root,
    "data/group-demand-intelligence/hotels",
    BETHESDA_CANONICAL_HOTEL_ID,
    "opportunities.json"
  );
  const storeSha = fs.existsSync(oppPath)
    ? crypto.createHash("sha256").update(fs.readFileSync(oppPath)).digest("hex")
    : null;

  return {
    version: BETHESDA_29_COMMERCIAL_QUALITY_CLEANUP_V1,
    generatedAt: new Date().toISOString(),
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    hotelName: BETHESDA_CANONICAL_HOTEL_NAME,
    safety: {
      paidResearch: false,
      opportunitiesJsonWrites: false,
      shareRegistryWrites: false,
      salesReadyPromotions: 0,
      customerFacing: false,
      researchStageOnly: true,
    },
    counts: {
      attestedIds: 29,
      cleaned: afterRows.length,
      meetingCommercialQualityBar: meetingBar,
      salesReadyEligible: 0,
      accountDevelopmentEligible: 0,
      researchContinuation: maturityCounts.RESEARCH_CONTINUATION,
      partnershipWatch: maturityCounts.PARTNERSHIP_WATCH,
      overflowWatch: maturityCounts.OVERFLOW_WATCH,
      stillRequiringAdditionalOrPaidResearch: needsPaidOrAdditional,
      otherUnclassifiedAfter: afterDistribution.OTHER_UNCLASSIFIED || 0,
    },
    maturityCounts,
    beforeFilterDistribution: beforeDistribution,
    afterFilterDistribution: afterDistribution,
    crosswalk,
    scorecards,
    opportunities: afterRows,
    opportunityStore: {
      pathRel: `data/group-demand-intelligence/hotels/${BETHESDA_CANONICAL_HOTEL_ID}/opportunities.json`,
      sha256: storeSha,
      expectedSha256: EXPECTED_STORE_SHA,
      acceptableSha256s: [...ACCEPTABLE_STORE_SHAS],
      untouched: ACCEPTABLE_STORE_SHAS.includes(storeSha),
    },
    proposedPaidResearch: {
      authorized: false,
      note: "STOP — no paid calls without founder approval. Suggested first batch only if zero-paid contact recovery stalls.",
      maxUsdSuggested: 25,
      provider: "Exa (targeted contact + current-cycle demand verification)",
      candidateIds: scorecards
        .filter((s) =>
          [
            "disp_recLuxvwwxID7U2B8_greater_bethesda_chamber_of_commerce",
            "disp_recLuxvwwxID7U2B8_montgomery_county_shrm",
            "disp_recLuxvwwxID7U2B8_pmi_montgomery_county_maryland_chapter",
            "disp_recLuxvwwxID7U2B8_parenteral_drug_association_pda_",
            "disp_recLuxvwwxID7U2B8_iwlca_elite_tournaments_iwlca_champions_cup",
            "gdi_pep_live_c165c897d233",
            "disp_recLuxvwwxID7U2B8_cohera_formerly_csi_dmc_and_360dg_",
          ].includes(s.id)
        )
        .map((s) => s.id),
      questions: [
        "Named commercially relevant contact (events / housing / partnership owner)",
        "Current or next-cycle demand date / recurring pattern",
        "Independent source URL confirming activity near Bethesda corridor",
        "Whether competitor-host programmes leave accessible overflow for Bethesda Marriott",
      ],
      expectedCalls: 7,
      expectedImprovement:
        "Move 4–7 records from attestation-only narrative toward Account Development / stronger Partnership Watch with named entry points — still not automatic Sales Ready.",
    },
  };
}

export function stageBethesda29CommercialQualityCleanup({
  root = ROOT,
  writeFixture = true,
  writeReports = true,
} = {}) {
  const beforePath = path.join(
    researchStageHotelDir(BETHESDA_CANONICAL_HOTEL_ID, { root }),
    "reconstructed-opportunities-v1.json"
  );
  const beforeSnapDir = path.join(root, REPORT_DIR_REL, "before-snapshot");
  fs.mkdirSync(beforeSnapDir, { recursive: true });
  let beforeSha = null;
  if (fs.existsSync(beforePath)) {
    const raw = fs.readFileSync(beforePath);
    beforeSha = crypto.createHash("sha256").update(raw).digest("hex");
    fs.writeFileSync(
      path.join(beforeSnapDir, "reconstructed-opportunities-v1.before.json"),
      raw
    );
  }

  const doc = buildBethesda29CommercialQualityCleanup({ root });
  const persist = persistResearchStageOpportunityBag({
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    targetPath: beforePath,
    incomingDoc: {
      version: BETHESDA_29_COMMERCIAL_QUALITY_CLEANUP_V1,
      hotelId: BETHESDA_CANONICAL_HOTEL_ID,
      hotelName: BETHESDA_CANONICAL_HOTEL_NAME,
      safety: doc.safety,
      counts: doc.counts,
      opportunityStore: doc.opportunityStore,
      generatedAt: doc.generatedAt,
      commercialCleanup: true,
      opportunities: doc.opportunities,
    },
    protectedIds: [...BETHESDA_29_ATTESTED_IDS],
    root,
    label: "bethesda-29-commercial-quality-cleanup",
    mode: "merge",
  });

  let fixtureWrite = null;
  if (writeFixture) {
    const fixturePath = path.join(root, FIXTURE_REL);
    // Merge cleanup fields into fixture opportunities while preserving safety envelope.
    const prev = fs.existsSync(fixturePath)
      ? JSON.parse(fs.readFileSync(fixturePath, "utf8"))
      : {};
    fixtureWrite = writeJsonAtomic(fixturePath, {
      ...prev,
      version: BETHESDA_29_COMMERCIAL_QUALITY_CLEANUP_V1,
      generatedAt: doc.generatedAt,
      safety: { ...(prev.safety || {}), ...doc.safety },
      counts: {
        ...(prev.counts || {}),
        ...doc.counts,
        reconstructedRecords: 29,
      },
      commercialCleanup: {
        version: BETHESDA_29_COMMERCIAL_QUALITY_CLEANUP_V1,
        categoryMap: BETHESDA_HISTORICAL_COMMERCIAL_CATEGORY_MAP_V1,
      },
      opportunityStore: doc.opportunityStore,
      opportunities: doc.opportunities,
    });
  }

  const rollbackManifest = {
    version: BETHESDA_29_COMMERCIAL_QUALITY_CLEANUP_V1,
    beforeSha256: beforeSha,
    beforeSnapshotRel: `${REPORT_DIR_REL}/before-snapshot/reconstructed-opportunities-v1.before.json`,
    restoreCommand:
      "Copy before-snapshot reconstructed-opportunities-v1.before.json over data/group-demand-intelligence/research-stage/recLuxvwwxID7U2B8/reconstructed-opportunities-v1.json",
    opportunitiesJsonUntouched: true,
  };

  if (writeReports) {
    const outDir = path.join(root, REPORT_DIR_REL);
    fs.mkdirSync(outDir, { recursive: true });
    fs.writeFileSync(
      path.join(outDir, "01-category-crosswalk.json"),
      `${JSON.stringify({ before: doc.beforeFilterDistribution, after: doc.afterFilterDistribution, crosswalk: doc.crosswalk }, null, 2)}\n`
    );
    fs.writeFileSync(
      path.join(outDir, "02-quality-scorecard.json"),
      `${JSON.stringify({ counts: doc.counts, maturityCounts: doc.maturityCounts, scorecards: doc.scorecards }, null, 2)}\n`
    );
    fs.writeFileSync(
      path.join(outDir, "03-proposed-paid-research.json"),
      `${JSON.stringify(doc.proposedPaidResearch, null, 2)}\n`
    );
    fs.writeFileSync(
      path.join(outDir, "04-rollback-manifest.json"),
      `${JSON.stringify(rollbackManifest, null, 2)}\n`
    );
    fs.writeFileSync(
      path.join(outDir, "00-cleanup-summary.json"),
      `${JSON.stringify(
        {
          version: doc.version,
          generatedAt: doc.generatedAt,
          counts: doc.counts,
          maturityCounts: doc.maturityCounts,
          beforeFilterDistribution: doc.beforeFilterDistribution,
          afterFilterDistribution: doc.afterFilterDistribution,
          opportunityStore: doc.opportunityStore,
          persistOk: persist.ok === true,
          safety: doc.safety,
        },
        null,
        2
      )}\n`
    );
  }

  return {
    ok: persist.ok === true && doc.counts.otherUnclassifiedAfter === 0,
    persist,
    fixtureWrite,
    doc,
    rollbackManifest,
    stagedPath: beforePath,
  };
}

export { GDI_RESEARCH_STAGE_PERSISTENCE_V1 };
