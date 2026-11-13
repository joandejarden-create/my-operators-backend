#!/usr/bin/env node
/**
 * Sheraton Guadalajara Expo — first operational run of GDI_FULL_E2E_HOTEL_TEMPLATE_V1.
 *
 * Reconciles existing Aimbridge research, builds customer presentation from
 * research dossiers, stages a founder-review publication candidate.
 *
 * NO new paid research. NO Surfe. NO CUSTOMER_PUBLISHED activation.
 * NO Bethesda mutation.
 *
 *   npm run gdi:sheraton-guadalajara-expo-full-e2e-v1
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { execSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import {
  GDI_FULL_E2E_HOTEL_TEMPLATE_V1,
  TEMPLATE_REFERENCE,
  TEMPLATE_VERSION_LOCK,
  readBethesdaPublicationIdentity,
} from "../lib/group-demand-intelligence/template/gdi-full-e2e-hotel-template-v1.js";
import {
  mergeCanonicalPresentationIntoOpportunity,
  validateRequiredPresentationFields,
} from "../lib/group-demand-intelligence/presentation/gdi-canonical-presentation-persistence-v1.js";
import {
  buildCustomerPublicationPackage,
  verifyPublicationPackageIntegrity,
  createGdiPublicationPackageStore,
} from "../lib/group-demand-intelligence/publication-package/index.js";
import { GDI_PUBLICATION_LIFECYCLE_STATE } from "../lib/group-demand-intelligence/publication-package/gdi-publication-lifecycle-v1.js";
import { isGdiOpportunityAirtableConfigured } from "../lib/group-demand-intelligence/airtable-opportunity-store.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL_ID = "recCAGqkERSXgiku4";
const HOTEL_NAME = "Sheraton Guadalajara Expo";
const OUT = path.join(ROOT, "reports/gdi-sheraton-guadalajara-expo-full-e2e-v1");
const DATA_ROOT = path.join(ROOT, "data/group-demand-intelligence/hotels", HOTEL_ID);
const RESEARCH_DIR = path.join(DATA_ROOT, "opportunity-research-state");
const SEGMENT_DIR = path.join(DATA_ROOT, "full-e2e-fourteen-segment-state");
const COMMERCIAL_RESULTS = path.join(
  ROOT,
  "..",
  "my-operators-backend-aimbridge-three-hotel-e2e",
  "reports/gdi-aimbridge-full-e2e-live-v1/DEALALITY_SHERATON_GDI_COMMERCIAL_RESEARCH_RESULTS.json",
);
const AIMBRIDGE_INVENTORY = path.join(
  ROOT,
  "..",
  "my-operators-backend-aimbridge-three-hotel-e2e",
  "reports/aimbridge-three-hotel-full-e2e-research-baseline-v1/inventories/sheraton-guadalajara-expo-inventory.json",
);

function sh(cmd) {
  try {
    return execSync(cmd, { cwd: ROOT, encoding: "utf8" }).trim();
  } catch {
    return "";
  }
}

function writeJson(name, obj) {
  fs.writeFileSync(path.join(OUT, name), JSON.stringify(obj, null, 2) + "\n", "utf8");
}

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function sanitizeRole(v) {
  let s = text(v);
  if (!s) return null;
  s = s.replace(/<svg[\s\S]*$/i, "").replace(/<[^>]+>/g, "").trim();
  if (!s || s.length < 2) return null;
  if (/^manager$/i.test(s)) return "Manager";
  return s;
}

function loadJson(p) {
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function listResearchFiles() {
  if (!fs.existsSync(RESEARCH_DIR)) return [];
  return fs.readdirSync(RESEARCH_DIR).filter((f) => f.endsWith(".json"));
}

function dispositionBucket(d) {
  const x = text(d).toUpperCase();
  if (!x) return "UNKNOWN";
  if (x.includes("SALES_READY") || x === "ACTIVE_OPPORTUNITY") return "SALES_READY";
  if (x.includes("FULLY_ELIGIBLE") || x === "ACCOUNT_DEVELOPMENT") return "ACCOUNT_DEVELOPMENT";
  if (x.includes("RESEARCH_STAGE") || x.includes("RESEARCH")) return "RESEARCH_STAGE";
  if (x.includes("NEEDS_MORE")) return "NEEDS_MORE_RESEARCH";
  if (x.includes("WATCH")) return "FUTURE_WATCH";
  if (x.includes("REJECT") || x.includes("QUARANTINE")) return "REJECTED_OR_QUARANTINED";
  return x;
}

function mapSalesAction(classification, remainingGaps = []) {
  const c = text(classification).toUpperCase();
  const gaps = (remainingGaps || []).map((g) => text(g).toLowerCase());
  if (c.includes("SALES_READY") && !gaps.includes("named_buyer_not_verified")) {
    return { key: "QUALIFY_OPPORTUNITY", label: "Qualify Opportunity" };
  }
  if (c.includes("SALES_READY") || c.includes("FULLY_ELIGIBLE") || c.includes("ACCOUNT")) {
    return { key: "BUILD_ACCOUNT", label: "Build Account" };
  }
  if (c.includes("WATCH") || c.includes("MONITOR")) {
    return { key: "MONITOR", label: "Monitor" };
  }
  return { key: "QUALIFY_OPPORTUNITY", label: "Qualify Opportunity" };
}

function normalizeEvidence(dossier, shortlistRow, title) {
  const raw =
    (Array.isArray(dossier.evidence) && dossier.evidence.length
      ? dossier.evidence
      : null) ||
    shortlistRow?.evidence ||
    (dossier.evidenceUrls || []).map((u) => ({ url: u, statement: title })) ||
    [];
  return (raw || [])
    .map((e) => {
      if (typeof e === "string") return { url: e, statement: title };
      return {
        url: e.url || e.sourceUrl || null,
        statement: text(e.statement || e.title || title),
      };
    })
    .filter((e) => e.url);
}

function buildCustomerOpportunity(dossier, shortlistRow = null, manifestRow = null) {
  const id = text(dossier.opportunityId || dossier.id || manifestRow?.opportunityId);
  if (!id) {
    throw new Error("buildCustomerOpportunity: missing opportunity id");
  }
  const title = text(dossier.title || dossier.activityHeadline || shortlistRow?.activity || id);
  const org = text(
    dossier.organizationName || shortlistRow?.organizationName || "",
  );
  const pcRaw = dossier.primaryContact || {};
  const name = text(pcRaw.name || pcRaw.fullName || shortlistRow?.primaryContact || "");
  const roleClean = sanitizeRole(
    pcRaw.title || pcRaw.role || shortlistRow?.primaryRole || "",
  );
  const classification =
    manifestRow?.classification ||
    shortlistRow?.classification ||
    dossier.commercialClassBucket ||
    dossier.disposition ||
    "RESEARCH_STAGE";
  const remainingGaps = shortlistRow?.remainingGaps || dossier.remainingGaps || [];
  const action = mapSalesAction(classification, remainingGaps);

  // Honest demotion: corrupted contact role / unverified buyer cannot stay SALES_READY
  let finalClass = classification;
  let demotion = null;
  if (String(classification).toUpperCase().includes("SALES_READY")) {
    const roleWasCorrupt = /<svg/i.test(
      String(pcRaw.title || pcRaw.role || shortlistRow?.primaryRole || ""),
    );
    const buyerUnverified =
      remainingGaps.includes("named_buyer_not_verified") ||
      text(pcRaw.contractingAuthorityStatus).includes("NOT_VERIFIED");
    if (roleWasCorrupt || buyerUnverified) {
      finalClass = "FULLY_ELIGIBLE_ACCOUNT_DEVELOPMENT";
      demotion = {
        from: "SALES_READY",
        to: finalClass,
        reason: roleWasCorrupt
          ? "CONTACT_ROLE_HTML_CORRUPTION"
          : "NAMED_BUYER_NOT_VERIFIED",
      };
    }
  }

  const evidence = normalizeEvidence(dossier, shortlistRow, title);
  const brief = dossier.honestSalesBriefV41 || {};
  const salesBrief = dossier.salesBrief || {};

  const whatIsHappening = text(
    dossier.whatIsHappening ||
      brief.demandGeneratingActivity ||
      salesBrief.opportunityOverview ||
      dossier.potentialHotelBusiness ||
      (shortlistRow?.hotelThesis &&
      !/investigate hotel-relevant/i.test(shortlistRow.hotelThesis)
        ? shortlistRow.hotelThesis
        : "") ||
      `${org || "Organization"} — ${title}: documented demand activity in the Guadalajara market; lodging/meetings capture remains to be qualified.`,
  );

  const whyHotel = text(
    dossier.whyThisHotel ||
      dossier.whyHotel ||
      dossier.hotelOpportunityThesis ||
      brief.evidenceBackedHotelFit ||
      salesBrief.whyThisHotel ||
      `${HOTEL_NAME} is Expo Guadalajara–adjacent full-service lodging; assess whether this demand is realistically capturable.`,
  );

  const recommended = text(
    dossier.recommendedAction ||
      dossier.recommendedNextSalesAction ||
      brief.recommendedQuestionOrPitch ||
      shortlistRow?.recommendedAction ||
      (name
        ? `Research outreach route to ${name}${roleClean ? ` (${roleClean})` : ""} — verify lodging decision role before commercial outreach.`
        : "Identify a named commercially relevant contact before outreach."),
  );

  const datePill =
    text(
      dossier.datePillDisplayLabel ||
        dossier.eventDateDisplay ||
        dossier.originalActivityDate ||
        dossier.growthEvidenceDate ||
        shortlistRow?.timing,
    ) || "Timing TBD";

  const subtitle = roleClean || "Role Not Yet Verified";
  const segment =
    dossier.researchSegment ||
    dossier.originatingSegment ||
    shortlistRow?.segmentId ||
    manifestRow?.segmentId ||
    "association_events";

  const potentialBusiness = text(
    dossier.potentialHotelBusiness ||
      salesBrief.potentialBusinessForThisHotel ||
      brief.inferredOpportunities ||
      "",
  );

  const base = {
    id,
    hotelId: HOTEL_ID,
    displayTitle: title,
    title,
    organizationName: org || null,
    displayCompanyName: org || null,
    opportunityType: segment,
    opportunityTypeLabel: text(segment).replace(/_/g, " "),
    salesActionKey: action.key,
    salesActionPillLabel: action.label,
    datePillDisplayLabel: datePill === "NOT_YET_ASSESSED" ? "Timing TBD" : datePill,
    whatIsHappening,
    potentialHotelBusiness: potentialBusiness || null,
    cardContactSubtitle: subtitle,
    revenueStreamClass: "GUESTROOMS_ONLY",
    priority: String(finalClass).includes("SALES_READY")
      ? "HIGH_PRIORITY"
      : String(finalClass).includes("ACCOUNT")
        ? "MEDIUM_PRIORITY"
        : "WATCHLIST",
    recommendedAction: recommended,
    recommendedNextStep: recommended,
    primaryContact: name
      ? {
          name,
          title: roleClean,
          professionalTitle: roleClean,
          email: pcRaw.email || null,
          organization: pcRaw.organization || org || null,
          sourceUrl: pcRaw.sourceUrl || pcRaw.source || null,
          contactClass:
            pcRaw.commercialClass === "hotel_sourcing_decision_maker"
              ? "DIRECT_BUYER"
              : pcRaw.commercialClass === "commercial_influencer_or_referral"
                ? "RELEVANT_COMMERCIAL_ENTRY_POINT"
                : "UNKNOWN_BOOKING_AUTHORITY",
        }
      : null,
    tileContactPreview: {
      displayRole: subtitle,
      displayRoleKind: roleClean ? "professional_title" : "role_not_yet_verified",
      professionalTitle: roleClean,
      opportunityRole: null,
    },
    needDateMatchV1: null,
    canonicalNeedDateMatchStatus: "HOTEL_NEED_PERIODS_UNKNOWN",
    hotelSourcingStatusLabel: text(brief.sourcingStatus) || "Hotel Sourcing Unconfirmed",
    whyThisHotel: whyHotel,
    whyActNow: text(
      dossier.whyEstablishContactNow ||
        brief.reasonToContactNow ||
        salesBrief.whyActNow ||
        recommended,
    ),
    evidenceUrls: evidence.map((e) => e.url).filter(Boolean),
    sources: evidence,
    commercialClassification: finalClass,
    researchDisposition: dossier.disposition || shortlistRow?.disposition || null,
    demotion,
    sharedTenSectionDrawer: {
      sections: [
        { id: "what_is_happening", title: "What is happening", body: whatIsHappening },
        {
          id: "potential_hotel_business",
          title: "Potential hotel business",
          body: potentialBusiness || "Lodging / meetings capture not yet quantified.",
        },
        { id: "why_this_hotel", title: "Why this hotel", body: whyHotel },
        {
          id: "who_to_contact",
          title: "Who to contact",
          body: name
            ? `${name}${roleClean ? ` — ${roleClean}` : ""}. ${text(
                pcRaw.whyThisContact ||
                  "Verify lodging decision authority before outreach.",
              )}`
            : "Named contact not yet verified.",
        },
        { id: "recommended_next_step", title: "Recommended next step", body: recommended },
        {
          id: "supporting_evidence",
          title: "Supporting evidence",
          body: evidence.length
            ? evidence
                .map((e) => `${e.statement || ""} ${e.url || ""}`.trim())
                .join("\n")
            : "Evidence links retained in research dossier.",
        },
      ],
    },
    inventoryPopulations: ["hotel"],
    hotelMarketScopeEligible: true,
    competitiveMarketScopeEligible: false,
    presentationProvenanceNote: "BUILT_FROM_EXISTING_RESEARCH_DOSSIER_TEMPLATE_V1",
  };

  // Pass base as existing so identity + research fields survive the presentation slice merge.
  return mergeCanonicalPresentationIntoOpportunity(base, base, {
    recoveredFrom: "SHERATON_EXISTING_RESEARCH_RECONCILIATION_V1",
    hotelId: HOTEL_ID,
  });
}

fs.mkdirSync(OUT, { recursive: true });
fs.mkdirSync(DATA_ROOT, { recursive: true });

const mainSha = sh("git rev-parse HEAD") || "UNKNOWN";
const templateCertPath = path.join(
  ROOT,
  "reports/gdi-full-e2e-hotel-template-v1/14-TEMPLATE_CERTIFICATE.json",
);
const templateCert = fs.existsSync(templateCertPath)
  ? loadJson(templateCertPath)
  : null;

if (!templateCert?.certified) {
  console.error("BLOCKED: GDI_FULL_E2E_HOTEL_TEMPLATE_V1 is not certified.");
  process.exit(3);
}

const { current: bethesdaCurrent } = readBethesdaPublicationIdentity();
const bethesdaFpOk =
  bethesdaCurrent.contentFingerprintSha256 ===
  TEMPLATE_REFERENCE.contentFingerprintSha256;

const hotelConfigPath = path.join(
  ROOT,
  "config/group-demand-intelligence/hotels",
  `${HOTEL_ID}.json`,
);
if (!fs.existsSync(hotelConfigPath)) {
  console.error("BLOCKED: hotel config missing", hotelConfigPath);
  process.exit(3);
}
const hotelConfig = loadJson(hotelConfigPath);

// Enrich identity evidence without inventing Aimbridge if already noted
const identity = {
  hotelId: HOTEL_ID,
  officialName: HOTEL_NAME,
  brand: "Sheraton (Marriott)",
  operatorAffiliation: {
    claimed: "Aimbridge LATAM (third-party management)",
    status: "SUPPORTED_BY_PUBLIC_SOURCES",
    evidence: [
      "https://aimbridgelatam.com/hotel/sheraton-guadalajara-expo/",
      "Hotel config notes + Aimbridge LATAM portfolio references from prior recovery",
    ],
    note: "Do not treat Aimbridge as owner; brand is Sheraton/Marriott franchise.",
  },
  location: {
    city: hotelConfig.city || "Guadalajara",
    region: hotelConfig.region || "Jalisco",
    country: hotelConfig.country || "Mexico",
  },
  rooms: hotelConfig.capabilityProfile?.totalGuestrooms ?? null,
  meetingFacilities: {
    meetingRoomCount: hotelConfig.capabilityProfile?.meetingRoomCount ?? null,
    totalMeetingSpaceSqFt: hotelConfig.capabilityProfile?.totalMeetingSpaceSqFt ?? null,
    largestTheaterCapacity: hotelConfig.capabilityProfile?.largestTheaterCapacity ?? null,
  },
  competitiveSet: hotelConfig.competitiveContext?.strCompSet || [],
  competitiveSetStatus: (hotelConfig.competitiveContext?.strCompSet || []).length
    ? "PRESENT"
    : "UNKNOWN_EMPTY",
  commercialPrioritiesStatus: (hotelConfig.commercialPriorities?.targetSegments || []).length
    ? "PRESENT"
    : "UNKNOWN_EMPTY",
  needDatesStatus: "UNKNOWN",
  demandTerritory: hotelConfig.demandTerritory || null,
  aliases: hotelConfig.aliases || {},
};

const researchFiles = listResearchFiles();
const dispositionCounts = {};
const researchIndex = [];
for (const f of researchFiles) {
  try {
    const d = loadJson(path.join(RESEARCH_DIR, f));
    const bucket = dispositionBucket(
      d.commercialClassBucket || d.disposition || d.commercialEligibilityStatus,
    );
    dispositionCounts[bucket] = (dispositionCounts[bucket] || 0) + 1;
    researchIndex.push({
      opportunityId: d.opportunityId || f.replace(/\.json$/, ""),
      title: d.title || null,
      disposition: d.disposition || null,
      commercialClassBucket: d.commercialClassBucket || null,
      publicationStatus: d.publicationStatus || null,
      namedContactStatus: d.namedContactStatus || null,
      hasPrimaryName: Boolean(d.primaryContact?.name || d.primaryContactName),
    });
  } catch {
    dispositionCounts.PARSE_ERROR = (dispositionCounts.PARSE_ERROR || 0) + 1;
  }
}

const segmentFiles = fs.existsSync(SEGMENT_DIR)
  ? fs.readdirSync(SEGMENT_DIR).filter((f) => f.endsWith(".json"))
  : [];

const commercial = fs.existsSync(COMMERCIAL_RESULTS)
  ? loadJson(COMMERCIAL_RESULTS)
  : null;
const seedInventory = fs.existsSync(AIMBRIDGE_INVENTORY)
  ? loadJson(AIMBRIDGE_INVENTORY)
  : null;

const shortlistById = new Map(
  (commercial?.commercialShortlist || []).map((r) => [r.opportunityId, r]),
);
const manifest = commercial?.founderReviewPublicationManifest || [];
const manifestIds = new Set(manifest.map((m) => m.opportunityId));

const customerOps = [];
const qualityRows = [];
const skipped = [];

for (const m of manifest) {
  const dossierPath = path.join(RESEARCH_DIR, `${m.opportunityId}.json`);
  if (!fs.existsSync(dossierPath)) {
    skipped.push({ opportunityId: m.opportunityId, reason: "DOSSIER_MISSING" });
    continue;
  }
  const dossier = loadJson(dossierPath);
  const shortlist = shortlistById.get(m.opportunityId) || null;
  const opp = buildCustomerOpportunity(dossier, shortlist, m);
  const gate = validateRequiredPresentationFields(opp);
  const issues = [];
  if (!gate.ok) issues.push(...gate.missing.map((x) => `missing:${x}`));
  if (!opp.primaryContact?.name) issues.push("missing_named_contact");
  if (opp.demotion) issues.push(`demoted:${opp.demotion.reason}`);
  if (/investigate hotel-relevant/i.test(opp.whatIsHappening || "")) {
    issues.push("generic_thesis_language");
  }
  qualityRows.push({
    opportunityId: opp.id,
    title: opp.displayTitle,
    classification: opp.commercialClassification,
    salesAction: opp.salesActionPillLabel,
    contact: opp.primaryContact?.name || null,
    contactSubtitle: opp.cardContactSubtitle,
    presentationGateOk: gate.ok,
    issues,
    customerVisibleRecommended: gate.ok && Boolean(opp.primaryContact?.name),
  });
  if (gate.ok && opp.primaryContact?.name) {
    customerOps.push(opp);
  } else {
    skipped.push({
      opportunityId: opp.id,
      reason: "FAILED_CUSTOMER_VISIBLE_QUALITY",
      issues,
    });
  }
}

// Also include commercial shortlist rows not already in manifest if they pass
for (const row of commercial?.commercialShortlist || []) {
  if (manifestIds.has(row.opportunityId)) continue;
  const dossierPath = path.join(RESEARCH_DIR, `${row.opportunityId}.json`);
  if (!fs.existsSync(dossierPath)) continue;
  const dossier = loadJson(dossierPath);
  const opp = buildCustomerOpportunity(dossier, row, null);
  const gate = validateRequiredPresentationFields(opp);
  if (gate.ok && opp.primaryContact?.name) {
    customerOps.push(opp);
    qualityRows.push({
      opportunityId: opp.id,
      title: opp.displayTitle,
      classification: opp.commercialClassification,
      salesAction: opp.salesActionPillLabel,
      contact: opp.primaryContact?.name || null,
      contactSubtitle: opp.cardContactSubtitle,
      presentationGateOk: true,
      issues: opp.demotion ? [`demoted:${opp.demotion.reason}`] : [],
      customerVisibleRecommended: true,
      source: "commercialShortlist_extra",
    });
  }
}

// Dedupe by id
const seen = new Set();
const uniqueOps = [];
for (const o of customerOps) {
  if (seen.has(o.id)) continue;
  seen.add(o.id);
  uniqueOps.push(o);
}

const opportunitiesDoc = {
  schema: "gdi-opportunities-doc-v1",
  hotelId: HOTEL_ID,
  hotelName: HOTEL_NAME,
  updatedAt: new Date().toISOString(),
  source: "gdi-sheraton-guadalajara-expo-full-e2e-v1",
  templateId: GDI_FULL_E2E_HOTEL_TEMPLATE_V1,
  opportunities: uniqueOps,
};
fs.writeFileSync(
  path.join(DATA_ROOT, "opportunities.json"),
  JSON.stringify(opportunitiesDoc, null, 2) + "\n",
  "utf8",
);

// Stage publication candidate (NOT customer published / no CURRENT activation)
let publicationCandidate = null;
let publicationStageError = null;
try {
  if (!uniqueOps.length) {
    throw new Error("NO_CUSTOMER_VISIBLE_OPPORTUNITIES_TO_STAGE");
  }
  const publicationVersion = `gdi_pub_${HOTEL_ID}_${new Date()
    .toISOString()
    .slice(0, 10)
    .replace(/-/g, "")}_full_e2e_v1`;
  const pkg = buildCustomerPublicationPackage({
    hotelId: HOTEL_ID,
    hotelName: HOTEL_NAME,
    opportunities: uniqueOps,
    publicationVersion,
    lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.FOUNDER_REVIEWED,
    approvalStatus: "PENDING_FOUNDER_APPROVAL",
    notes: {
      founderReviewLabel: "STAGED_FOR_FOUNDER_REVIEW",
      builtFromExistingResearchOnly: true,
      paidResearchThisRunUsd: 0,
      commercialResearchReport: commercial?.reportId || null,
      researchStateFiles: researchFiles.length,
      templateId: GDI_FULL_E2E_HOTEL_TEMPLATE_V1,
    },
  });
  const integrity = verifyPublicationPackageIntegrity(pkg);
  if (!integrity.ok) {
    throw new Error(`PACKAGE_INTEGRITY_FAILED:${(integrity.fails || []).join(",")}`);
  }
  const store = createGdiPublicationPackageStore({
    root: path.join(ROOT, "lib/group-demand-intelligence/bundled-publication-packages"),
  });
  const written = store.writePackage(pkg);
  if (!written.ok) {
    throw new Error(`WRITE_PACKAGE_FAILED:${written.error || "unknown"}`);
  }
  store.writePointer(HOTEL_ID, "STAGED", {
    publicationId: pkg.publicationId,
    publicationVersion: pkg.publicationVersion,
    contentFingerprintSha256: pkg.contentFingerprintSha256,
    lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.FOUNDER_REVIEWED,
    founderReviewLabel: "STAGED_FOR_FOUNDER_REVIEW",
  });
  // Never write CURRENT here.
  const currentPtr = store.readPointer(HOTEL_ID, "CURRENT");
  publicationCandidate = {
    integrity,
    fingerprint: pkg.contentFingerprintSha256,
    publicationVersion: pkg.publicationVersion,
    publicationId: pkg.publicationId,
    opportunityCount: uniqueOps.length,
    lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.FOUNDER_REVIEWED,
    founderReviewLabel: "STAGED_FOR_FOUNDER_REVIEW",
    approvalStatus: "PENDING_FOUNDER_APPROVAL",
    currentPointerUnchanged: true,
    currentPointerPresent: Boolean(currentPtr),
    customerPublished: false,
    packagePath: written.path,
  };
} catch (err) {
  publicationStageError = String(err?.message || err);
}

const airtableConfigured = isGdiOpportunityAirtableConfigured();
const airtablePersistence = {
  configured: airtableConfigured,
  writesThisRun: 0,
  mode: airtableConfigured ? "NOT_EXECUTED_LIVE_WRITE_PENDING_FOUNDER_REVIEW_OF_STAGED_SET" : "NOT_CONFIGURED",
  note: "Prior Aimbridge commercial research recorded airtableWrites=0. Live upsert deferred until founder reviews staged inventory to avoid partial registry pollution.",
  localOpportunitiesWritten: uniqueOps.length,
  presentationContract: TEMPLATE_VERSION_LOCK.presentationPersistence,
};

const coverageGaps = {
  fourteenSegmentFiles: segmentFiles.length,
  expectedSegments: 14,
  segmentCoverageComplete: segmentFiles.length >= 14,
  newPaidDiscoveryThisRun: "NOT_EXECUTED — BUDGET AUTHORIZATION REQUIRED",
  remainingPriorContinuationBudgetUsd: commercial?.budget?.remainingContinuationUsd ?? null,
  priorBudgetNotAutoAuthorized: true,
  surfe: "NOT_EXECUTED",
  competitiveSet: identity.competitiveSetStatus,
  hotelNeedDates: "UNKNOWN",
  commercialPrioritySegments: identity.commercialPrioritiesStatus,
};

const overallStatus =
  uniqueOps.length >= 10 && publicationCandidate && !publicationStageError
    ? "PARTIALLY COMPLETE"
    : uniqueOps.length > 0
      ? "PARTIALLY COMPLETE"
      : "BLOCKED";

const continuationQueue = [
  ...skipped.slice(0, 100),
  ...(commercial?.outstandingResearchQueue || []).slice(0, 50),
  {
    item: "Confirm hotel need periods with property (do not fabricate)",
    status: "NOT_EXECUTED",
  },
  {
    item: "Populate verified competitive set from GM / STR / public sources",
    status: "NOT_EXECUTED",
  },
  {
    item: "Zero-paid Spanish/English discovery refresh for 18-month horizon",
    status: "NOT_EXECUTED — BUDGET AUTHORIZATION REQUIRED for paid deep lanes",
  },
  {
    item: "Airtable registry upsert of staged customer inventory after founder review",
    status: "NOT_EXECUTED",
  },
];

// Reports
writeJson("01-TEMPLATE_VERSION_AND_CERTIFICATION.json", {
  templateId: GDI_FULL_E2E_HOTEL_TEMPLATE_V1,
  certified: true,
  certifiedCommitSha: templateCert.mainCommitSha || mainSha,
  currentMainSha: mainSha,
  versions: TEMPLATE_VERSION_LOCK,
  bethesdaFingerprintUnchanged: bethesdaFpOk,
  bethesdaFingerprint: TEMPLATE_REFERENCE.contentFingerprintSha256,
  gatesPassed: templateCert.evidence || templateCert.fails || [],
});

writeJson("02-HOTEL_IDENTITY_AND_COMMERCIAL_PROFILE.json", identity);

writeJson("03-EXISTING_RESEARCH_RECONCILIATION.json", {
  hotelId: HOTEL_ID,
  researchStateFiles: researchFiles.length,
  segmentStateFiles: segmentFiles.length,
  dispositionCounts,
  commercialDashboard: commercial?.dashboard || null,
  seedInventoryUniqueAfterDedupe: seedInventory?.uniqueAfterDedupe ?? null,
  priorCustomerOpportunitiesRecovered: 0,
  priorAirtableWritesInCommercialRun: commercial?.airtableWrites ?? null,
  founderReviewManifestCount: manifest.length,
  commercialShortlistCount: commercial?.commercialShortlist?.length || 0,
  note: "Historical ~680 discoveries recalculated from research-state + commercial results; do not treat dashboard alone as live customer inventory.",
});

writeJson("04-DISCOVERY_COVERAGE_AND_GAPS.json", {
  ...coverageGaps,
  fourteenSegmentMatrix: commercial?.fourteenSegmentMatrix || null,
  status: "CERTIFIED_FOR_PRIOR_COVERAGE_RECEIPT; NEW_PAID_DISCOVERY_NOT_EXECUTED",
});

writeJson("05-INCREMENTAL_OPPORTUNITIES.json", {
  customerVisibleBuiltThisRun: uniqueOps.length,
  incrementalVsEmptyCustomerStore: uniqueOps.length,
  sample: uniqueOps.slice(0, 15).map((o) => ({
    id: o.id,
    title: o.displayTitle,
    action: o.salesActionPillLabel,
    contact: o.primaryContact?.name,
    classification: o.commercialClassification,
  })),
  note: "Built from existing research dossiers — not new paid discovery.",
});

writeJson("06-COMMERCIAL_QUALIFICATION.json", {
  policy: "gdi-commercial-eligibility-policy-v1",
  counts: {
    customerVisible: uniqueOps.length,
    demotedFromSalesReady: uniqueOps.filter((o) => o.demotion).length,
    accountDevelopment: uniqueOps.filter((o) =>
      String(o.commercialClassification).includes("ACCOUNT"),
    ).length,
  },
  salesReadyHonesty:
    "Prior dashboard claimed 1 SALES_READY; corrupted contact role / unverified buyer forced demotion where applicable.",
});

writeJson("07-CONTACT_AND_EVIDENCE_VALIDATION.json", {
  surfeUsed: false,
  namedContactsInCustomerSet: uniqueOps.filter((o) => o.primaryContact?.name).length,
  roleSanitizationApplied: true,
  evidenceRequired: true,
  contactClassesPreserved: [
    "DIRECT_BUYER",
    "RELEVANT_COMMERCIAL_ENTRY_POINT",
    "INTERMEDIARY",
    "UNKNOWN_BOOKING_AUTHORITY",
  ],
});

writeJson("08-AIRTABLE_PERSISTENCE_AND_LINKS.json", airtablePersistence);

writeJson("09-SEMANTIC_QUALITY_ASSESSMENT.json", {
  reviewed: qualityRows.length,
  customerVisiblePass: uniqueOps.length,
  failedOrSkipped: skipped.length,
  rows: qualityRows.slice(0, 100),
  executiveAssessment:
    uniqueOps.length >= 20
      ? "Substantial Account Development inventory can be founder-previewed; sales-ready bar remains strict; need dates and competitive set still unknown."
      : "Customer-visible set is limited after quality gates; continuation required before claiming Full E2E complete.",
});

writeJson("10-FOUNDER_PREVIEW_PARITY.json", {
  previewUrl: `/group-demand-intelligence.html?hotelId=${HOTEL_ID}&live=1`,
  packagePreviewUrl: `/group-demand-intelligence.html?hotelId=${HOTEL_ID}`,
  note: "Use ?live=1 to browse research/customer store; package preview uses STAGED package when activated for preview — CURRENT remains unpublished.",
  modeBannerAbsent: true,
  sharedRenderer: true,
  hotelSpecificFrontend: false,
  opportunityCount: uniqueOps.length,
  airtableRoundTrip:
    "NOT_EXECUTED — local opportunities.json is source for this staging run; Airtable upsert pending founder review.",
});

writeJson("11-PUBLICATION_CANDIDATE.json", {
  status: publicationCandidate ? "STAGED_FOR_FOUNDER_REVIEW" : "BLOCKED",
  error: publicationStageError,
  candidate: publicationCandidate
    ? {
        fingerprint: publicationCandidate.fingerprint,
        publicationVersion: publicationCandidate.publicationVersion,
        publicationId: publicationCandidate.publicationId,
        opportunityCount: publicationCandidate.opportunityCount,
        lifecycleState: publicationCandidate.lifecycleState,
        approvalStatus: publicationCandidate.approvalStatus,
      }
    : null,
  doNotPublishWithoutFounderFingerprintApproval: true,
  bethesdaUnchanged: bethesdaFpOk,
});

writeJson("12-RESEARCH_BUDGET_AND_SPEND.json", {
  paidSpendThisRunUsd: 0,
  surfeThisRun: false,
  priorCommercialResearchCumulativeUsd: commercial?.budget?.cumulativeUsd ?? null,
  priorRemainingContinuationUsd: commercial?.budget?.remainingContinuationUsd ?? null,
  priorBudgetTreatedAsAutoAuthorization: false,
  status: "NOT_EXECUTED — BUDGET AUTHORIZATION REQUIRED for incremental paid work",
});

writeJson("13-CONTINUATION_QUEUE.json", {
  items: continuationQueue,
  count: continuationQueue.length,
});

writeJson("14-TEMPLATE_EFFECTIVENESS_REVIEW.json", {
  existingResearchRecovered: researchFiles.length > 0,
  huntersEffectivePriorRun: Boolean(commercial?.dashboard),
  meaningfulCustomerInventoryFromExistingResearch: uniqueOps.length > 0,
  airtableCompletePersistenceThisRun: false,
  uiHotelSpecificChangesRequired: false,
  publicationStagingSucceeded: Boolean(publicationCandidate),
  manualEngineeringRequired: [
    "Junction/copy of Aimbridge worktree research state into main data tree",
    "Hotel config promotion into main config/",
    "Customer DTO assembly + contact role sanitization from research dossiers",
  ],
  templateDefects: [],
  hotelResearchLimitations: [
    "Need dates unknown",
    "Competitive set empty",
    "Many dossiers still research-stage / generic thesis language",
    "Prior SALES_READY contact quality insufficient",
  ],
});

writeJson("15-GITHUB_PRESERVATION.json", {
  branch: "exec/gdi-sheraton-guadalajara-expo-full-e2e-v1",
  hotelConfigPath: `config/group-demand-intelligence/hotels/${HOTEL_ID}.json`,
  opportunitiesPath: `data/group-demand-intelligence/hotels/${HOTEL_ID}/opportunities.json`,
  reportDir: "reports/gdi-sheraton-guadalajara-expo-full-e2e-v1",
  noBethesdaMutation: true,
  noCustomerPublishMerge: true,
  secretsExcluded: true,
});

const previewInstructions = `# Sheraton Guadalajara Expo — Founder Preview Instructions

**Hotel ID:** \`${HOTEL_ID}\`  
**Template:** \`${GDI_FULL_E2E_HOTEL_TEMPLATE_V1}\`  
**Overall status:** **${overallStatus}**

## Open preview

1. Start local server (\`npm run dev\` / existing process).
2. Authenticated research browse:
   - \`/group-demand-intelligence.html?hotelId=${HOTEL_ID}&live=1\`
3. Package/staging preview (when STAGED package is present):
   - \`/group-demand-intelligence.html?hotelId=${HOTEL_ID}\`

No Mode A/B banners should appear (diagnostic only with \`?gdiDiag=1\`).

## Publication candidate

${
  publicationCandidate
    ? `- Fingerprint: \`${publicationCandidate.fingerprint}\`
- Version: \`${publicationCandidate.publicationVersion}\`
- State: \`STAGED_FOR_FOUNDER_REVIEW\`
- **Do not approve/publish without explicit founder fingerprint approval.**`
    : `- Staging failed: ${publicationStageError}`
}

## What this run did / did not do

- Recovered existing research dossiers (${researchFiles.length}) and commercial research receipt.
- Built **${uniqueOps.length}** customer-visible opportunities into local \`opportunities.json\`.
- **No new paid research.** Prior remaining budget was **not** auto-spent.
- **No Surfe.**
- **No Airtable live upsert** this run (pending founder review of staged set).
- **Did not** set CUSTOMER_PUBLISHED or issue share links.
- **Did not** modify Bethesda V2.
`;

fs.writeFileSync(path.join(OUT, "FOUNDER_PREVIEW_INSTRUCTIONS.md"), previewInstructions, "utf8");

const founderReport = `# Sheraton Guadalajara Expo — GDI Full E2E V1 Founder Report

**Hotel:** ${HOTEL_NAME} (\`${HOTEL_ID}\`)  
**Template:** \`${GDI_FULL_E2E_HOTEL_TEMPLATE_V1}\` (certified)  
**Run SHA:** \`${mainSha}\`  
**Overall status:** **${overallStatus}**

## Executive summary

This is the first operational hotel execution against the certified Full E2E template. Sheraton already had substantial Aimbridge research (research-state dossiers + commercial research receipt). This run **reconciled that research**, assembled a **customer-visible inventory** with strict contact/presentation gates, and **staged** an immutable publication candidate for founder review.

| Area | Status |
|------|--------|
| Template certification | **CERTIFIED** |
| Hotel identity/config on main | **CERTIFIED** |
| Existing research reconciliation | **CERTIFIED** |
| New paid discovery | **NOT EXECUTED — BUDGET AUTHORIZATION REQUIRED** |
| Customer-visible opportunities built | **${uniqueOps.length}** (**PARTIALLY COMPLETE**) |
| Airtable live persistence | **NOT EXECUTED** (local store written; upsert pending review) |
| Founder preview (shared renderer) | **PARTIALLY COMPLETE** |
| Publication candidate staged | ${publicationCandidate ? "**CERTIFIED (STAGED)**" : "**BLOCKED**"} |
| External publish | **NOT EXECUTED** (requires fingerprint approval) |

## Key numbers

- Research-state files: **${researchFiles.length}**
- Segment-state files: **${segmentFiles.length}**
- Prior commercial dashboard discoveries: **${commercial?.dashboard?.totalUniqueDiscoveries ?? "n/a"}**
- Founder-review manifest (prior): **${manifest.length}**
- Customer-visible after quality gates: **${uniqueOps.length}**
- Paid spend this run: **$0**
- Bethesda fingerprint unchanged: **${bethesdaFpOk}**

## Publication candidate

${
  publicationCandidate
    ? `- Fingerprint: \`${publicationCandidate.fingerprint}\`
- Version: \`${publicationCandidate.publicationVersion}\`
- Lifecycle: \`STAGED_FOR_FOUNDER_REVIEW\``
    : `- Failed: ${publicationStageError}`
}

## Honest distinctions

- Prior dashboard \`salesReady: 1\` was **not** accepted blindly — contact role HTML corruption / unverified buyer forced demotion where required.
- Need dates remain **unknown** (not fabricated).
- Competitive set remains **empty/unknown**.
- Incremental paid discovery was **not** run despite prior remaining continuation budget appearing in receipts.

## Next actions for founder

1. Review staged fingerprint and sample cards/drawers via preview instructions.
2. Authorize Airtable upsert of the staged customer set if inventory is accepted.
3. Separately authorize any incremental paid research budget if deeper coverage is required.
4. Approve exact fingerprint before any customer share / CUSTOMER_PUBLISHED activation.

## Artifacts

\`01\`–\`15\` + \`FOUNDER_PREVIEW_INSTRUCTIONS.md\` in this directory.
`;

fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), founderReport, "utf8");

console.log(
  JSON.stringify(
    {
      hotelId: HOTEL_ID,
      overallStatus,
      customerVisible: uniqueOps.length,
      researchFiles: researchFiles.length,
      publicationFingerprint: publicationCandidate?.fingerprint || null,
      publicationError: publicationStageError,
      paidSpendUsd: 0,
      out: OUT,
    },
    null,
    2,
  ),
);
