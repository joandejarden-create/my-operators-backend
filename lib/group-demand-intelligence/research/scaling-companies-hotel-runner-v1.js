/**
 * Hotel-generic Scaling Companies GDI runner V1.
 *
 * Reuses existing modules (V5 discovery, scaling-companies-research-v1,
 * prospect-publication-eligibility, ORS deep research, public buyer intelligence,
 * live spending ledger). Does NOT create a parallel research engine.
 *
 * Defaults: no paid calls, no publication, no production writes.
 * Never auto-executes Surfe, PDL, Parallel, outreach, or weekly scheduling.
 *
 * Operational reference:
 *   reports/gdi-bethesda-scaling-companies-production-verification-v1/
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  AUTONOMOUS_DEMAND_DISCOVERY_V5,
  DISCOVERY_PATHWAY,
  DEMAND_MECHANISM_CLASS,
  generateOrganizationLedStrategies,
  generateTriggerLedStrategies,
  generateEventLedStrategies,
  loadHotelGdiConfig,
} from "./autonomous-demand-discovery-v5.js";
import { resolveHotelGdiProfileForResearch } from "../hotel-gdi-profile-resolution-v1.js";
import {
  assertLiveDeepResearchLedger,
  loadOrCreateLiveDeepResearchLedger,
  preflightLiveDeepResearchPricing,
  maybeExaSearch,
  reconcileLiveLedgerReservations,
  createLiveDeepResearchResearcher,
} from "./deep-research-gdi-live-certification-v1.js";
import {
  WATERSTONE_HOTEL_ID,
  CHAPULTEPEC_HOTEL_ID,
  campaignAggregateLedgerPath as wcCampaignAggregateLedgerPath,
  assertPaidExecutionAllowed as assertWcPaidExecutionAllowed,
  readLedgerSnapshot as readWcLedgerSnapshot,
} from "./waterstone-chapultepec-shared-budget-v1.js";
import {
  assertPaidContactProvidersDisabled,
  classifyContactOwnership,
  drainRelatedEntityFollowUpsThroughOrs,
} from "./commercial-spine-restoration-v41.js";
import {
  runDeepResearchOnExistingGdiOpportunity,
} from "./deep-research-gdi-integration-v1.js";
import { STOP_REASONS } from "./research-constants-v1.js";
import {
  assessScalingCompaniesApplicability,
  buildScalingCompanyResearchQueries,
  isScalingCompanyOpportunity,
  resolveScalingCompaniesResearchCity,
  SCALING_COMPANIES_RESEARCH_VERSION,
} from "../workflow/scaling-companies-research-v1.js";
import {
  evaluateProspectPublicationEligibility,
  hasUsableContactRoute,
} from "../prospect-publication-eligibility-v1.js";
import {
  evaluateReviewedProspectProductionListPreflight,
  isReviewedProspectHotelEnabled,
} from "../reviewed-prospect-publication-v1.js";
import {
  assessGdiHotelPreviewReadiness,
  assessHotelAuthorizationStates,
} from "../adp-gdi-universal-hotel-roster-v1.js";
import {
  loadOpportunitiesCanonical,
  getGdiOpportunityPersistenceMode,
} from "../opportunity-persistence.js";
import {
  isCleanRoomActive,
  resolveExistingOpportunitiesForResearch,
} from "./clean-room-v26-isolation-v1.js";
import {
  assessScalingCommercialPublicationGate,
  auditContactRoute,
} from "./bethesda-scaling-commercial-completion-v1.js";
import {
  FUNNEL_STAGE,
  runScalingDiscoveryHandoffPipeline,
} from "./scaling-companies-discovery-handoff-v1.js";
import {
  runScalingFullE2ePublicationStage,
  SCALING_FULL_E2E_PUBLICATION_V1,
} from "./scaling-companies-full-e2e-publication-v1.js";
import {
  SCALING_INTELLIGENT_LIFECYCLE_V1,
  buildScalingIntelligentLifecyclePlan,
  selectLifecycleResearchTargets,
  summarizeLifecyclePlan,
} from "./scaling-companies-intelligent-lifecycle-v1.js";
import { attachSecondaryNamedContact } from "../secondary-named-contact-v1.js";
import { applyScalingSalesPresentation } from "../scaling-companies-sales-presentation-v1.js";
import { applyScalingTwoContactStandard } from "../scaling-two-contact-standard-v1.js";
import {
  auditScalingClaimToSource,
  requalifyScalingOpportunity,
} from "../scaling-commercial-evidence-integrity-v1.js";
import {
  runInvestigationLoopOnMaturedRows,
  SCALING_INTELLIGENT_INVESTIGATION_LOOP_V1,
} from "./scaling-intelligent-investigation-loop-v1.js";
import {
  buildAdaptiveScalingDiscoveryStrategies,
  detectScalingHotelDemandMode,
  SCALING_DESTINATION_DEMAND_DISCOVERY_V1,
  SCALING_HOTEL_DEMAND_MODE,
} from "./scaling-destination-demand-discovery-v1.js";

export const SCALING_COMPANIES_HOTEL_RUNNER_V1 =
  "gdi-scaling-companies-hotel-runner-v1";

export const SCALING_COMPANIES_FULL_E2E_STAGES = Object.freeze([
  "hotel_profile",
  "discovery",
  "organization_identity_resolution",
  "net_new_reconciliation",
  "commercial_mechanism_screening",
  "ors_longform_research",
  "buyer_and_contact_intelligence",
  "intelligent_investigation_loop",
  "commercial_qualification",
  "gold_standard_brief_generation",
  "canonical_opportunity_payload",
  "airtable_publication",
  "customer_share_verification",
]);

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function writeJson(p, obj) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, `${JSON.stringify(obj, null, 2)}\n`);
}

function writeMd(p, md) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, md.endsWith("\n") ? md : `${md}\n`);
}

function truthy(v) {
  if (v === true) return true;
  const s = String(v ?? "").trim().toLowerCase();
  return s === "1" || s === "true" || s === "yes";
}

function ledgerSpent(ledgerPath) {
  if (!fs.existsSync(ledgerPath)) return 0;
  return Number(JSON.parse(fs.readFileSync(ledgerPath, "utf8")).estimatedSpentUsd || 0);
}

/**
 * Fail-closed authorization + run config from PowerShell JSON / CLI.
 */
export function normalizeScalingCompaniesRunnerConfig(raw = {}) {
  const auth = raw.authorization && typeof raw.authorization === "object" ? raw.authorization : {};
  const hotelId = text(raw.hotelId || raw.HotelId || raw.canonicalHotelId);
  const dryRunFlag = truthy(raw.dryRun) || truthy(raw.DryRun);
  let modeRaw = text(raw.mode || raw.Mode || "").toLowerCase().replace(/_/g, "-");
  if (!modeRaw) {
    modeRaw = "preview";
  }
  // Preview / dry-run / preflight are unpaid planning modes.
  if (dryRunFlag && modeRaw !== "full-e2e" && modeRaw !== "fulle2e") {
    modeRaw = "preview";
  }
  let mode;
  if (
    modeRaw === "full-e2e" ||
    modeRaw === "fulle2e" ||
    modeRaw === "full"
  ) {
    mode = "full-e2e";
  } else if (modeRaw === "execute" || modeRaw === "run") {
    // Legacy alias — treat as full-e2e orchestration (publication still independently gated)
    mode = "full-e2e";
  } else if (modeRaw === "preflight") {
    mode = "preflight";
  } else if (modeRaw === "dry-run" || modeRaw === "dryrun") {
    mode = "preview";
  } else {
    mode = "preview";
  }

  const authorizePaidResearch =
    truthy(raw.authorizePaidResearch) || truthy(auth.paidResearch);
  const authorizePublication =
    truthy(raw.authorizePublication) || truthy(auth.customerPublication);
  // Production writes: explicit flag OR publication authorization (Airtable path still needs applyProductionWrites)
  const authorizeProductionWrites =
    truthy(raw.authorizeProductionWrites) ||
    truthy(auth.productionWrites) ||
    authorizePublication;
  const applyProductionWrites =
    truthy(raw.applyProductionWrites) && authorizeProductionWrites === true;
  const probeLiveShare = truthy(raw.probeLiveShare) || truthy(raw.ProbeLiveShare);
  // Hard-deny dangerous providers regardless of JSON claims —
  // runner still refuses to execute them even if set.
  const authorizeSurfe = truthy(raw.authorizeSurfe) || truthy(auth.surfe);
  const authorizePdl = truthy(raw.authorizePdl) || truthy(auth.pdl);
  const authorizeParallel = truthy(raw.authorizeParallel) || truthy(auth.parallel);
  const authorizeOutreach =
    truthy(raw.authorizeOutreach) || truthy(auth.prospectOutreach);
  const authorizeWeeklyScheduling =
    truthy(raw.authorizeWeeklyScheduling) || truthy(auth.weeklyActivation);

  let budgetCeilingUsd = Number(
    raw.budgetCeilingUsd ?? raw.BudgetCeilingUsd ?? raw.budgetUSD ?? raw.BudgetUSD ?? 0,
  );
  if (!Number.isFinite(budgetCeilingUsd) || budgetCeilingUsd < 0) budgetCeilingUsd = 0;
  if (!authorizePaidResearch) budgetCeilingUsd = 0;

  const maxCandidates = Math.max(
    0,
    Math.min(50, Number(raw.maxCandidates ?? raw.MaxCandidates ?? 8) || 8),
  );

  const runId =
    text(raw.runId) ||
    `sc_run_${hotelId || "unknown"}_${new Date().toISOString().replace(/[:.]/g, "-")}`;

  const reportDirRaw = text(raw.reportDir || raw.outputDirectory || "");
  const reportDir = reportDirRaw
    ? path.isAbsolute(reportDirRaw)
      ? reportDirRaw
      : path.join(ROOT, reportDirRaw)
    : path.join(
        ROOT,
        "reports/gdi-scaling-companies-hotel-runner-v1",
        runId,
      );

  const savedDiscoveryHits = Array.isArray(raw.savedDiscoveryHits)
    ? raw.savedDiscoveryHits
    : Array.isArray(raw.SavedDiscoveryHits)
      ? raw.SavedDiscoveryHits
      : null;
  const savedDiscoveryPath = text(raw.savedDiscoveryPath || raw.SavedDiscoveryPath || "");
  const sharedLedgerPathRaw = text(
    raw.sharedLedgerPath || raw.SharedLedgerPath || raw.ledgerPath || "",
  );
  const useSharedCampaignLedger =
    truthy(raw.useSharedCampaignLedger)
    || truthy(process.env.GDI_WC_LIVE_CAMPAIGN)
    || (
      authorizePaidResearch
      && (hotelId === WATERSTONE_HOTEL_ID || hotelId === CHAPULTEPEC_HOTEL_ID)
    );

  const blockers = [];
  if (!hotelId) blockers.push("hotelId_required");
  if (authorizeSurfe) blockers.push("surfe_not_permitted_by_runner");
  if (authorizePdl) blockers.push("pdl_not_permitted_by_runner");
  if (authorizeParallel) blockers.push("parallel_not_permitted_by_runner");
  if (authorizeOutreach) blockers.push("outreach_not_permitted_by_runner");
  if (authorizeWeeklyScheduling) blockers.push("weekly_scheduling_not_permitted_by_runner");
  // Independent gates: publication does NOT require paid research.
  // Paid research requires a positive ceiling when FullE2E intends live discovery providers.
  if (mode === "full-e2e" && authorizePaidResearch && budgetCeilingUsd <= 0) {
    blockers.push("paid_full_e2e_requires_budgetCeilingUsd_gt_0");
  }
  if (authorizePublication && !authorizeProductionWrites) {
    blockers.push("publication_requires_authorizeProductionWrites");
  }
  if (applyProductionWrites && !authorizePublication) {
    blockers.push("applyProductionWrites_requires_authorizePublication");
  }

  return {
    hotelId,
    mode,
    authorizePaidResearch,
    authorizePublication,
    authorizeProductionWrites,
    applyProductionWrites,
    probeLiveShare,
    authorizeSurfe: false,
    authorizePdl: false,
    authorizeParallel: false,
    authorizeOutreach: false,
    authorizeWeeklyScheduling: false,
    requestedForbidden: {
      authorizeSurfe,
      authorizePdl,
      authorizeParallel,
      authorizeOutreach,
      authorizeWeeklyScheduling,
    },
    budgetCeilingUsd,
    maxCandidates,
    runId,
    reportDir,
    savedDiscoveryHits,
    savedDiscoveryPath,
    sharedLedgerPath: sharedLedgerPathRaw || null,
    useSharedCampaignLedger,
    stages: SCALING_COMPANIES_FULL_E2E_STAGES,
    blockers,
    configOk: blockers.length === 0,
  };
}

function hotelProfileSummary(hotel) {
  const cap = hotel.capabilityProfile || {};
  return {
    hotelId: hotel.hotelId,
    displayName: hotel.displayName || hotel.name || null,
    city: hotel.city || null,
    country: hotel.country || null,
    demandTerritory: hotel.demandTerritory || null,
    totalGuestrooms: cap.totalGuestrooms ?? null,
    meetingRoomsIndoor: cap.meetingRoomsIndoor ?? cap.meetingRoomCount ?? null,
    totalMeetingSpaceSqFt: cap.totalMeetingSpaceSqFt ?? null,
    demandAnchors: hotel.commercialPriorities?.demandAnchorFocus || [],
    targetSegments: hotel.commercialPriorities?.targetSegments || [],
  };
}

function marketLocalTerms(hotel) {
  const terms = new Set();
  const add = (v) => {
    const t = text(v);
    if (t) terms.add(t.toLowerCase());
  };
  add(hotel.city);
  add(hotel.displayName);
  for (const a of hotel.demandTerritory?.includes || []) add(a);
  add(hotel.demandTerritory?.label);
  for (const a of hotel.commercialPriorities?.demandAnchorFocus || []) {
    // keep short geo tokens from anchors
    String(a)
      .split(/[/,·|]/)
      .map((s) => s.trim())
      .filter((s) => s.length >= 3 && s.length <= 40)
      .forEach(add);
  }
  return [...terms].slice(0, 24);
}

export function buildHotelScalingStrategies(hotel) {
  const researchCity = resolveScalingCompaniesResearchCity(hotel);
  const hotelForStrategies = {
    ...hotel,
    city: hotel.city || researchCity || hotel.city,
  };
  const demandMode = detectScalingHotelDemandMode(hotelForStrategies);
  const adaptive = buildAdaptiveScalingDiscoveryStrategies(hotelForStrategies);

  // Legacy V5 / query builders remain available as secondary fill — but destination
  // hotels no longer lead with "opens office" / local HQ expansion.
  const org = generateOrganizationLedStrategies(hotelForStrategies, {
    maxPerSegment: 2,
  }).filter((s) => s.segment === "scaling_companies");
  const trig = generateTriggerLedStrategies(hotelForStrategies, {
    maxPerSegment: 2,
  }).filter((s) => s.segment === "scaling_companies");
  const evt = generateEventLedStrategies(hotelForStrategies, {
    maxPerSegment: 1,
  }).filter((s) => s.segment === "scaling_companies");
  const hotelCfg = {
    city: hotelForStrategies.city,
    demandTerritory: hotel.demandTerritory,
    displayName: hotel.displayName,
    capabilityProfile: hotel.capabilityProfile,
    commercialPriorities: hotel.commercialPriorities,
    country: hotel.country,
  };
  const extra = buildScalingCompanyResearchQueries(hotelCfg)
    .filter((q) => q.query)
    .map((q) => ({
      pathway: DISCOVERY_PATHWAY.TRIGGER_LED,
      segment: "scaling_companies",
      researchQuestion: q.query,
      sourceTypes: ["corp_announcements", "facility_openings"],
      intent: q.intent,
    }));

  const destinationPrimary =
    demandMode === SCALING_HOTEL_DEMAND_MODE.DESTINATION_CORPORATE_MEETINGS;

  const secondary = destinationPrimary
    ? [
        ...extra.filter((s) =>
          /retreat|kickoff|training|incentiv|offsite|meeting|reconocimiento|capacitación|retiro/i.test(
            s.researchQuestion || "",
          ),
        ),
        // Drop V5 org/trig templates that still encode Cancún "scale-up / opens office"
        // geography — destination adaptive set already covers program demand.
      ]
    : [...org, ...trig, ...evt, ...extra];

  const all = [...adaptive, ...secondary];
  const seen = new Set();
  const out = [];
  for (const s of all) {
    const k = text(s.researchQuestion).toLowerCase();
    if (!k || seen.has(k)) continue;
    if (
      destinationPrimary &&
      (/opens?\s+office|opening an office|new headquarters|nueva oficina|abre oficinas|local_hq_expansion|local_office_expansion|apertura de oficinas|inbound_expansion|facility_expansion|hiring_location/i.test(
        `${k} ${s.intent || ""}`,
      ) ||
        (/\b(scale-up|expanding company|expanding workforce)\b/i.test(k) &&
          !/retreat|kickoff|incentiv|training|offsite|recognition|capacitación|convenci|programa/i.test(
            k,
          )))
    ) {
      continue;
    }
    if (
      hotel.hotelId !== "recLuxvwwxID7U2B8" &&
      /\bbethesda\b|\brockville\b|\bnih\b|\bwalter reed\b/i.test(k)
    ) {
      continue;
    }
    if (
      hotel.hotelId !== "recnPB3Q02NO8Ma1o" &&
      /\bsan diego\b|\bnorth harbor\b|\bemarcadero\b/i.test(k)
    ) {
      continue;
    }
    if (
      hotel.hotelId !== "recgMYovrrZDJMqzX" &&
      /\bboca raton\b|\bpalm beach\b|\bwaterstone\b/i.test(k)
    ) {
      continue;
    }
    if (
      /\bmarina\b/i.test(k) &&
      !(hotel.capabilityProfile?.marinaAccess === true)
    ) {
      continue;
    }
    seen.add(k);
    out.push({
      ...s,
      demandMode: s.demandMode || demandMode,
      destinationDiscoveryVersion: SCALING_DESTINATION_DEMAND_DISCOVERY_V1,
    });
  }
  return out.slice(0, 16);
}

function loadSegmentResearchMemory(hotelId) {
  const p = path.join(
    ROOT,
    "data/group-demand-intelligence/hotels",
    hotelId,
    "segment-research-state/scaling_companies.json",
  );
  if (!fs.existsSync(p)) return { path: p, exists: false, memory: null };
  try {
    return { path: p, exists: true, memory: JSON.parse(fs.readFileSync(p, "utf8")) };
  } catch (err) {
    return { path: p, exists: true, memory: null, error: err.message };
  }
}

function verifyOrsCallPath() {
  const integrationPath = path.join(
    ROOT,
    "lib/group-demand-intelligence/research/deep-research-gdi-integration-v1.js",
  );
  const orsPath = path.join(
    ROOT,
    "lib/group-demand-intelligence/research/opportunity-research-supervisor-v1.js",
  );
  const integrationSrc = fs.readFileSync(integrationPath, "utf8");
  const orsSrc = fs.readFileSync(orsPath, "utf8");
  const checks = {
    runDeepResearchExported:
      /export async function runDeepResearchOnExistingGdiOpportunity/.test(integrationSrc),
    matureOpportunityCalled: /await matureOpportunity\s*\(/.test(integrationSrc),
    matureOpportunityExported: /export async function matureOpportunity/.test(orsSrc),
    runDeepResearchFn: typeof runDeepResearchOnExistingGdiOpportunity === "function",
  };
  return {
    ok: Object.values(checks).every(Boolean),
    checks,
    entrypoint: "runDeepResearchOnExistingGdiOpportunity → matureOpportunity",
  };
}

function summarizeScalingOpportunity(o) {
  return {
    id: o.id || o.opportunityId,
    organizationName: o.organizationName || o.company,
    researchSegment: o.researchSegment || o.segment,
    customerVisible: o.customerVisible,
    prospectHotelFacing: o.prospectHotelFacing,
    prospectPublicationBasis: o.prospectPublicationBasis,
    contactName: o.contactName || o.primaryContact?.name || null,
    contactEmail:
      o.contactEmail ||
      o.functionalContactEmail ||
      o.organizationContactEmail ||
      o.primaryContact?.email ||
      null,
    contactPhone: o.contactPhone || o.companyPhone || o.primaryContact?.phone || null,
    guestroomStatus: o.guestroomStatus,
    recommendedAction: text(o.recommendedAction || o.recommendedNextStep).slice(0, 240),
  };
}

function evaluateInventoryRow(o, hotelId) {
  const elig = evaluateProspectPublicationEligibility(o, { hotelId });
  const route = hasUsableContactRoute(o);
  const email =
    o.contactEmail ||
    o.functionalContactEmail ||
    o.organizationContactEmail ||
    o.primaryContact?.email ||
    null;
  const ownership = classifyContactOwnership({
    email,
    pageUrl: o.contactUrl || o.officialSource || o.sources?.[0]?.url || "",
    organizationName: o.organizationName || o.company || "",
    personName: o.contactName || o.primaryContact?.name || null,
    roleTitle: o.contactRole || o.contactResponsibility || null,
  });
  const contactAudit = auditContactRoute({
    email,
    ownershipClass: ownership.ownershipClass,
    namedStakeholder: {
      name: o.contactName || o.primaryContact?.name || null,
      title: o.contactRole || o.contactResponsibility || null,
    },
    sourceUrl: o.contactUrl || o.officialSource || "",
  });
  const mechanismClass =
    Array.isArray(o.inferredHotelDemand) && o.inferredHotelDemand.length
      ? DEMAND_MECHANISM_CLASS.INFERRED_DEMAND
      : text(o.guestroomStatus) === "POTENTIAL_ADDITIONAL_USE"
        ? DEMAND_MECHANISM_CLASS.INFERRED_DEMAND
        : DEMAND_MECHANISM_CLASS.UNSUPPORTED_DEMAND;
  const gate = assessScalingCommercialPublicationGate({
    mechanismClass,
    contactAuditClass: contactAudit.auditClass,
    orsDeepResearchCompleted: Boolean(
      o.orsDeepResearchCompleted ||
        o.deepResearchCompleted ||
        o.maturedAt ||
        o.researchVersion,
    ),
    localConnectionSpecific: Boolean(text(o.localConnection || o.localPresence)),
    hotelUseSpecific: Boolean(
      text(o.proposedHotelUse || o.hotelOpportunityThesis || o.whyThisHotel),
    ),
  });
  return {
    ...summarizeScalingOpportunity(o),
    eligibility: {
      hotelFacing: elig.hotelFacing === true,
      eligibility: elig.eligibility,
      contactCompletenessGroup: elig.contactCompletenessGroup,
    },
    usableContactRoute: route?.ok === true,
    contactAuditClass: contactAudit.auditClass || null,
    publicationGate: {
      commerciallyPublicationReady: gate.commerciallyPublicationReady === true,
      accountDevelopmentEligible: gate.accountDevelopmentEligible === true,
      alreadyCustomerFacing:
        o.prospectHotelFacing === true &&
        o.prospectPublicationBasis === "REVIEWED_PACKAGE_V1",
      failReasons: gate.failReasons || [],
    },
  };
}

/**
 * Resolve hotel + inventory + strategies. Throws if hotel profile missing.
 * Uses universal GDI profile resolution (config + HPC + ADP + verified inventory).
 */
export async function resolveScalingCompaniesHotelContext(config) {
  const cfg = normalizeScalingCompaniesRunnerConfig(config);
  if (!cfg.hotelId) {
    const err = new Error("scaling_companies_runner_hotelId_required");
    err.code = "HOTEL_ID_REQUIRED";
    throw err;
  }
  const resolved = await resolveHotelGdiProfileForResearch(cfg.hotelId, {
    skipHpc: cfg.skipHpc === true,
  });
  const hotel = resolved.ok ? resolved.hotel : loadHotelGdiConfig(cfg.hotelId);
  if (!hotel || typeof hotel !== "object") {
    const err = new Error(
      `scaling_companies_runner_hotel_profile_unresolved:${cfg.hotelId}`,
    );
    err.code = "HOTEL_PROFILE_UNRESOLVED";
    err.hotelId = cfg.hotelId;
    err.resolution = resolved?.resolution || null;
    throw err;
  }
  if (resolved?.resolution?.blockers?.includes("HOTEL_IDENTITY_MISMATCH_CONFIG_VS_HPC")) {
    const err = new Error(
      `scaling_companies_runner_hotel_identity_mismatch:${cfg.hotelId}`,
    );
    err.code = "HOTEL_IDENTITY_MISMATCH";
    err.hotelId = cfg.hotelId;
    err.resolution = resolved.resolution;
    throw err;
  }
  if (!hotel.hotelId) hotel.hotelId = cfg.hotelId;
  const researchCity = resolveScalingCompaniesResearchCity(hotel);
  if (!hotel.city && researchCity) hotel.city = researchCity;

  const applicability = assessScalingCompaniesApplicability({
    ...hotel,
    hotelId: hotel.hotelId,
    displayName: hotel.displayName,
    city: hotel.city,
    demandTerritory: hotel.demandTerritory,
    capabilityProfile: hotel.capabilityProfile,
    commercialPriorities: hotel.commercialPriorities,
    identityExclusions: hotel.identityExclusions,
  });

  const persistenceMode = getGdiOpportunityPersistenceMode();
  const doc = await loadOpportunitiesCanonical(cfg.hotelId);
  const opportunities = doc.opportunities || [];
  const scalingOpps = opportunities.filter((o) => isScalingCompanyOpportunity(o));
  const researchMemory = loadSegmentResearchMemory(cfg.hotelId);
  const strategies = buildHotelScalingStrategies(hotel);
  const localTerms = marketLocalTerms(hotel);

  return {
    config: cfg,
    hotel,
    profile: hotelProfileSummary(hotel),
    profileResolution: resolved?.resolution || null,
    applicability,
    persistenceMode,
    opportunitySource: doc.source || doc.persistence || persistenceMode,
    opportunityCount: opportunities.length,
    scalingInventory: scalingOpps.map((o) => summarizeScalingOpportunity(o)),
    scalingInventoryEvaluated: scalingOpps.map((o) => evaluateInventoryRow(o, cfg.hotelId)),
    researchMemory,
    strategies,
    localTerms,
    reviewedProspectHotelEnabled: isReviewedProspectHotelEnabled(cfg.hotelId),
  };
}

function initLedger(reportDir, ceilingUsd, runId, { sharedLedgerPath = null, useSharedCampaignLedger = false, hotelId = "" } = {}) {
  // Waterstone + Chapultepec paid FullE2E must meter against the existing $20
  // aggregate ledger — never a second authorization or isolated paid ledger.
  if (sharedLedgerPath) {
    const abs = path.isAbsolute(sharedLedgerPath)
      ? sharedLedgerPath
      : path.join(ROOT, sharedLedgerPath);
    assertLiveDeepResearchLedger(abs);
    if (!fs.existsSync(abs)) {
      throw Object.assign(new Error(`shared_ledger_missing:${abs}`), {
        code: "shared_ledger_missing",
      });
    }
    const allowed = assertWcPaidExecutionAllowed(abs);
    if (!allowed.ok) {
      throw Object.assign(
        new Error(`shared_ledger_paid_blocked:${allowed.reason}`),
        { code: "shared_ledger_paid_blocked", detail: allowed },
      );
    }
    return abs;
  }
  if (
    useSharedCampaignLedger
    && (hotelId === WATERSTONE_HOTEL_ID || hotelId === CHAPULTEPEC_HOTEL_ID)
    && Number(ceilingUsd) > 0
  ) {
    const abs = wcCampaignAggregateLedgerPath(
      "reports/gdi-waterstone-chapultepec-live-campaign-v1",
    );
    assertLiveDeepResearchLedger(abs);
    if (!fs.existsSync(abs)) {
      throw Object.assign(new Error(`wc_shared_ledger_missing:${abs}`), {
        code: "wc_shared_ledger_missing",
      });
    }
    const snap = readWcLedgerSnapshot(abs);
    if (!snap) {
      throw Object.assign(new Error("wc_shared_ledger_unreadable"), {
        code: "wc_shared_ledger_unreadable",
      });
    }
    const allowed = assertWcPaidExecutionAllowed(abs);
    if (!allowed.ok) {
      throw Object.assign(
        new Error(`wc_shared_ledger_paid_blocked:${allowed.reason}`),
        { code: "wc_shared_ledger_paid_blocked", detail: allowed },
      );
    }
    return abs;
  }

  const safeRun = text(runId).replace(/[^\w.-]+/g, "_").slice(0, 80) || `run_${Date.now()}`;
  const ledgerPath = path.join(
    reportDir,
    "ledgers",
    `scaling-companies-run-ledger-${safeRun}.json`,
  );
  fs.mkdirSync(path.dirname(ledgerPath), { recursive: true });
  assertLiveDeepResearchLedger(ledgerPath);
  // Unpaid dry-run/preflight still needs an isolated ledger file; use a tiny
  // positive metadata ceiling (never authorizes paid spend without authorizePaidResearch).
  const paidCeiling = Math.max(0, Number(ceilingUsd) || 0);
  const ledgerBudget = paidCeiling > 0 ? paidCeiling : 0.001;
  loadOrCreateLiveDeepResearchLedger(ledgerPath, {
    budgetUsd: ledgerBudget,
    version: `${SCALING_COMPANIES_HOTEL_RUNNER_V1}:${safeRun}`,
  });
  reconcileLiveLedgerReservations(ledgerPath);
  return ledgerPath;
}

/**
 * Main entry — dry-run / preflight / execute.
 */
export async function runScalingCompaniesHotelRunner(rawConfig = {}) {
  const startedAt = new Date().toISOString();
  let cfg = normalizeScalingCompaniesRunnerConfig(rawConfig);

  // Forbidden provider flags always fail closed before any work.
  if (
    cfg.requestedForbidden.authorizeSurfe ||
    cfg.requestedForbidden.authorizePdl ||
    cfg.requestedForbidden.authorizeParallel ||
    cfg.requestedForbidden.authorizeOutreach ||
    cfg.requestedForbidden.authorizeWeeklyScheduling
  ) {
    const report = {
      ok: false,
      executed: false,
      version: SCALING_COMPANIES_HOTEL_RUNNER_V1,
      startedAt,
      finishedAt: new Date().toISOString(),
      config: cfg,
      blockers: cfg.blockers,
      reason: "forbidden_authorization_flags",
    };
    if (cfg.reportDir) {
      writeJson(path.join(cfg.reportDir, "SUMMARY.json"), report);
    }
    return report;
  }

  let ctx;
  try {
    ctx = await resolveScalingCompaniesHotelContext(cfg);
    cfg = ctx.config;
  } catch (err) {
    const report = {
      ok: false,
      executed: false,
      version: SCALING_COMPANIES_HOTEL_RUNNER_V1,
      startedAt,
      finishedAt: new Date().toISOString(),
      config: cfg,
      blockers: [...cfg.blockers, err.code || err.message],
      error: err.message,
      reason: err.code || "resolve_failed",
    };
    if (cfg.reportDir) {
      fs.mkdirSync(cfg.reportDir, { recursive: true });
      writeJson(path.join(cfg.reportDir, "SUMMARY.json"), report);
    }
    return report;
  }

  const reportDir = cfg.reportDir;
  fs.mkdirSync(reportDir, { recursive: true });
  let ledgerPath;
  try {
    ledgerPath = initLedger(
      reportDir,
      cfg.authorizePaidResearch ? cfg.budgetCeilingUsd : 0,
      cfg.runId,
      {
        sharedLedgerPath: cfg.sharedLedgerPath,
        useSharedCampaignLedger: cfg.useSharedCampaignLedger === true,
        hotelId: cfg.hotelId,
      },
    );
  } catch (err) {
    const report = {
      ok: false,
      executed: false,
      version: SCALING_COMPANIES_HOTEL_RUNNER_V1,
      startedAt,
      finishedAt: new Date().toISOString(),
      config: cfg,
      blockers: [...cfg.blockers, err.code || err.message],
      error: err.message,
      reason: err.code || "ledger_init_failed",
      detail: err.detail || null,
    };
    writeJson(path.join(reportDir, "SUMMARY.json"), report);
    return report;
  }

  // Soft BudgetUSD is this-run spend. Shared WC aggregate may already have prior
  // spend — gate paid work on incremental run spend, while reserve/commit still
  // enforces the hard $20 aggregate ceiling on the shared ledger.
  const ledgerBaselineSpentUsd = ledgerSpent(ledgerPath);
  const runSpentUsd = () =>
    Number(Math.max(0, ledgerSpent(ledgerPath) - ledgerBaselineSpentUsd).toFixed(6));

  const paidGuard = assertPaidContactProvidersDisabled({
    authorizeSurfe: false,
    authorizePdl: false,
  });
  const pricing = preflightLiveDeepResearchPricing();
  const orsPath = verifyOrsCallPath();

  // Always evaluate allowlist readiness during onboarding/Preview — never present
  // a hotel as Full E2E / customer-share ready solely because profile/inventory pass.
  // Local built-in allowlist ≠ Railway production share list eligibility.
  const reviewedProspectLocalPreflight = evaluateReviewedProspectProductionListPreflight(
    cfg.hotelId,
    { env: process.env, requireExplicitEnvAllowlist: false },
  );
  const reviewedProspectProductionSharePreflight =
    evaluateReviewedProspectProductionListPreflight(cfg.hotelId, {
      env: process.env,
      requireExplicitEnvAllowlist: true,
      targetEnvironmentLabel: "Railway production",
    });

  const preflight = {
    hotelResolved: true,
    applicabilityOk: ctx.applicability?.applicable === true,
    // Empty existing scaling inventory is legitimate for first-run hotels.
    strategiesPresent: ctx.strategies.length >= 3,
    pathwaysPresent:
      ctx.strategies.length >= 3 &&
      (new Set(ctx.strategies.map((s) => s.pathway)).size >= 1 ||
        ctx.strategies.some((s) => s.intent)),
    pricingOk: pricing.ok === true,
    paidContactGuardOk: paidGuard.ok !== false,
    orsCallPathOk: orsPath.ok === true,
    reviewedProspectHotelEnabled: ctx.reviewedProspectHotelEnabled === true,
    reviewedProspectLocalPreflight,
    reviewedProspectProductionSharePreflight,
    emptyScalingInventoryOk: true,
    existingScalingCount: ctx.scalingInventory.length,
    ledgerPath,
    ledgerSpentUsd: ledgerSpent(ledgerPath),
    ledgerBaselineSpentUsd,
    runSpentUsd: runSpentUsd(),
    budgetCeilingUsd: cfg.authorizePaidResearch ? cfg.budgetCeilingUsd : 0,
  };

  const profileReadyForResearch =
    preflight.applicabilityOk &&
    preflight.strategiesPresent &&
    preflight.pathwaysPresent &&
    preflight.orsCallPathOk &&
    preflight.paidContactGuardOk;

  const authStates = assessHotelAuthorizationStates(cfg.hotelId, {
    env: process.env,
    run: {
      authorizePaidResearch: cfg.authorizePaidResearch === true,
      budgetCeilingUsd: cfg.budgetCeilingUsd,
      authorizePublication: cfg.authorizePublication === true,
      probeLiveShare: cfg.probeLiveShare === true,
    },
    profileResolution: { ok: true },
  });
  const universalPreviewReadiness = assessGdiHotelPreviewReadiness(cfg.hotelId, {
    env: process.env,
    run: {
      authorizePaidResearch: cfg.authorizePaidResearch === true,
      budgetCeilingUsd: cfg.budgetCeilingUsd,
      authorizePublication: cfg.authorizePublication === true,
      probeLiveShare: cfg.probeLiveShare === true,
    },
    profileResolution: { ok: true },
    // Railway env is probed by dedicated onboarding scripts; Preview still
    // surfaces that local allowlist ≠ production until that probe is attached.
    railwayEnv: null,
  });

  const fullE2ePublicationGate = {
    version: "gdi-full-e2e-publication-gate-v1",
    profileReadyForResearch,
    localReviewedProspectAllowlistReady: reviewedProspectLocalPreflight.ok === true,
    productionExplicitAllowlistReady:
      reviewedProspectProductionSharePreflight.ok === true,
    readyForAuthorizedFullE2ePublication:
      profileReadyForResearch === true &&
      reviewedProspectLocalPreflight.ok === true,
    readyForLiveCustomerShareClaims:
      profileReadyForResearch === true &&
      reviewedProspectProductionSharePreflight.ok === true,
    authorizationStates: authStates.states,
    inUniversalAdpGdiRoster: authStates.inUniversalRoster === true,
    inAdpCommissioned: authStates.inAdpCommissioned === true,
    universalPreviewReadiness,
    note:
      "ADP membership enables profile onboarding only. PUBLICATION_AUTHORIZED requires reviewed-prospect allowlist — never auto-append from ADP. RESEARCH_AUTHORIZED requires explicit run budget. WEEKLY_ACTIVE is separate.",
    missingConfiguration: [
      ...(reviewedProspectLocalPreflight.ok
        ? []
        : [
            reviewedProspectLocalPreflight.requiredAction ||
              "Enable hotel on local REVIEWED_PROSPECT_HOTEL_IDS / GDI_REVIEWED_PROSPECT_HOTEL_IDS",
          ]),
      ...(reviewedProspectProductionSharePreflight.ok
        ? []
        : [
            reviewedProspectProductionSharePreflight.requiredAction ||
              "Append hotel to Railway GDI_REVIEWED_PROSPECT_HOTEL_IDS (preserve existing IDs) and keep GDI_REVIEWED_PROSPECT_PUBLICATION_V1=1",
          ]),
      ...(universalPreviewReadiness.missingPrerequisites || []).map(
        (m) => m.action || m.code
      ),
    ].filter((v, i, a) => v && a.indexOf(v) === i),
    blockerCodes: [
      reviewedProspectLocalPreflight.blockerCode,
      reviewedProspectProductionSharePreflight.blockerCode,
      ...(universalPreviewReadiness.missingPrerequisites || []).map((m) => m.code),
    ].filter(Boolean),
  };

  preflight.authorizationStates = authStates.states;
  preflight.inUniversalAdpGdiRoster = authStates.inUniversalRoster === true;
  preflight.universalPreviewReadiness = {
    shareReady: universalPreviewReadiness.share?.ready === true,
    missingPrerequisites: universalPreviewReadiness.missingPrerequisites,
  };

  const blockers = [...(cfg.blockers || [])];
  if (!preflight.applicabilityOk) blockers.push("scaling_segment_not_applicable_for_hotel_profile");
  if (!preflight.strategiesPresent) blockers.push("insufficient_scaling_strategies");
  if (!preflight.pathwaysPresent) blockers.push("insufficient_discovery_pathways");
  if (!preflight.orsCallPathOk) blockers.push("ors_call_path_unverified");
  if (!preflight.paidContactGuardOk) blockers.push("paid_contact_guard_failed");
  if (cfg.mode === "full-e2e" && cfg.authorizePaidResearch && !preflight.pricingOk) {
    blockers.push("live_pricing_preflight_failed");
  }
  if (cfg.authorizePublication && !ctx.reviewedProspectHotelEnabled) {
    blockers.push("hotel_not_on_reviewed_prospect_allowlist");
  }
  // Preview/onboarding: surface allowlist gap as a readiness warning, not a Preview fail.
  // Full E2E + AuthorizePublication already hard-blocks above.
  if (
    (cfg.mode === "preview" || cfg.mode === "preflight") &&
    !fullE2ePublicationGate.readyForAuthorizedFullE2ePublication
  ) {
    preflight.fullE2eNotReadyWarning =
      fullE2ePublicationGate.missingConfiguration.join(" | ") ||
      "Hotel profile may pass Preview while reviewed-prospect allowlist still blocks Full E2E publication.";
  }

  // Intelligent lifecycle: classify CONTINUE/REFRESH/REQUALIFY/MONITOR/EXCLUDE/NO_ACTION
  // before discovery so published accounts are not rediscovered and HOLD work is prioritized.
  const fullDocForLifecycle = await loadOpportunitiesCanonical(cfg.hotelId, {
    bypassCache: true,
  });
  const lifecyclePlan = buildScalingIntelligentLifecyclePlan({
    hotelId: cfg.hotelId,
    hotelProfile: ctx.profile,
    scalingOpportunities: (fullDocForLifecycle.opportunities || []).filter((o) =>
      isScalingCompanyOpportunity(o),
    ),
    researchMemory: ctx.researchMemory,
    maxResearchQueue: cfg.maxCandidates,
  });
  writeJson(path.join(reportDir, "03b-intelligent-lifecycle-plan.json"), lifecyclePlan);
  const lifecycleTargets = selectLifecycleResearchTargets(lifecyclePlan, {
    max: cfg.maxCandidates,
  });
  const lifecycleSummary = summarizeLifecyclePlan(lifecyclePlan);

  // Reconcile: existing scaling inventory is NEVER NET_NEW
  const reconciliation = {
    existingScalingCount: ctx.scalingInventory.length,
    existingPublished: ctx.scalingInventory.filter(
      (o) => o.prospectHotelFacing === true && o.prospectPublicationBasis === "REVIEWED_PACKAGE_V1",
    ),
    existingOther: ctx.scalingInventory.filter(
      (o) => !(o.prospectHotelFacing === true && o.prospectPublicationBasis === "REVIEWED_PACKAGE_V1"),
    ),
    netNewThisRun: [],
    lifecycle: lifecycleSummary,
    doNotRediscoverAsNetNew: lifecyclePlan.discoveryPlan?.doNotRediscoverAsNetNew || [],
    note:
      "Zero existing scaling opportunities is a valid starting state and does not block discovery. Dry-run/preflight does not mint NET_NEW. Published orgs are excluded from NET_NEW via lifecycle doNotRediscoverAsNetNew + findExistingScalingCompanyMatches. HOLD/CONTINUE/REFRESH decisions drive research queue before new discovery.",
  };

  const publicationReadiness = ctx.scalingInventoryEvaluated.map((row) => ({
    id: row.id,
    organizationName: row.organizationName,
    hotelFacing: row.eligibility?.hotelFacing,
    usableContactRoute: row.usableContactRoute,
    contactAuditClass: row.contactAuditClass,
    gate: row.publicationGate,
  }));

  const discovery = {
    attempted: false,
    searches: [],
    hits: 0,
    skippedReason: null,
    rawHits: [],
  };

  let handoff = null;
  const orsResults = [];
  const buyerIntelResults = [];
  /** Lifecycle ORS matured rows (same choke points as discovery handoff). */
  const lifecycleMaturedRows = [];
  let publicationStage = null;

  const isFullE2e = cfg.mode === "full-e2e";
  const isPreview = cfg.mode === "preview" || cfg.mode === "preflight";
  const allowPaid =
    isFullE2e &&
    cfg.authorizePaidResearch === true &&
    cfg.budgetCeilingUsd > 0;

  // Load saved discovery hits (path or inline) — Preview and FullE2E
  let processSavedHits = Array.isArray(cfg.savedDiscoveryHits)
    ? cfg.savedDiscoveryHits
    : Array.isArray(rawConfig.savedDiscoveryHits)
      ? rawConfig.savedDiscoveryHits
      : null;
  if ((!processSavedHits || !processSavedHits.length) && cfg.savedDiscoveryPath) {
    const abs = path.isAbsolute(cfg.savedDiscoveryPath)
      ? cfg.savedDiscoveryPath
      : path.join(ROOT, cfg.savedDiscoveryPath);
    if (fs.existsSync(abs)) {
      const doc = JSON.parse(fs.readFileSync(abs, "utf8").replace(/^\uFEFF/, ""));
      const rows =
        doc.netNewThisRun ||
        doc.rawHits ||
        doc.hits ||
        doc.savedDiscoveryHits ||
        [];
      if (Array.isArray(rows) && rows.length) {
        processSavedHits = rows.map((row) => ({
          title: text(row.title || row.organizationName || ""),
          organizationName: text(row.organizationName || row.title || ""),
          url: row.url || row.sourceUrl || null,
          sourceUrl: row.sourceUrl || row.url || null,
          excerpt: text(row.excerpt || row.note || ""),
          pathway: row.pathway || "TRIGGER_LED",
        }));
      }
    }
  }

  // Full E2E always runs discovery→ORS handoff when hits exist (paid or saved).
  // Preview runs handoff for saved hits so zero-inventory hotels prove the pipeline offline.
  const runHandoffMaturation =
    (isFullE2e && blockers.length === 0) ||
    truthy(rawConfig.processDiscoveryHandoff) ||
    Boolean(processSavedHits?.length);

  // Prefer completing HOLD/CONTINUE/REFRESH work before burning budget on NET_NEW discovery.
  const lifecyclePrefersContinuation =
    lifecycleTargets.some((t) => t.unpublishedHold || t.decision === "CONTINUE_RESEARCH") &&
    (lifecyclePlan.discoveryPlan?.netNewSlots || 0) === 0;

  if (
    isFullE2e &&
    blockers.length === 0 &&
    allowPaid &&
    !processSavedHits?.length &&
    lifecyclePlan.discoveryPlan?.scheduleNetNewDiscovery === true &&
    !lifecyclePrefersContinuation
  ) {
    discovery.attempted = true;
    const discoveryCap = Math.min(cfg.budgetCeilingUsd * 0.35, cfg.budgetCeilingUsd);
    for (const strategy of ctx.strategies) {
      if (runSpentUsd() >= discoveryCap - 0.01) {
        discovery.skippedReason = "discovery_phase_cap";
        break;
      }
      if (runSpentUsd() >= cfg.budgetCeilingUsd - 0.01) {
        discovery.skippedReason = "run_budget_ceiling";
        break;
      }
      const before = ledgerSpent(ledgerPath);
      const exa = await maybeExaSearch(strategy.researchQuestion, ledgerPath);
      const costUsd = Number((ledgerSpent(ledgerPath) - before).toFixed(6));
      const hits = exa.hits || [];
      discovery.hits += hits.length;
      discovery.searches.push({
        pathway: strategy.pathway,
        researchQuestion: strategy.researchQuestion,
        status: exa.status,
        costUsd,
        hitCount: hits.length,
      });
      for (const hit of hits) {
        discovery.rawHits.push({
          ...hit,
          pathway: strategy.pathway,
          researchQuestion: strategy.researchQuestion,
        });
      }
    }
  } else if (
    isFullE2e &&
    allowPaid &&
    !processSavedHits?.length &&
    lifecyclePlan.discoveryPlan?.scheduleNetNewDiscovery !== true
  ) {
    discovery.skippedReason = "lifecycle_deferred_net_new_in_favor_of_continuation";
  } else if (processSavedHits?.length) {
    discovery.attempted = false;
    discovery.skippedReason = "using_saved_discovery_hits";
    discovery.rawHits = processSavedHits;
    discovery.hits = processSavedHits.length;
  } else if (isFullE2e && !allowPaid) {
    discovery.skippedReason = "paid_discovery_not_authorized_use_saved_hits_or_authorize";
  } else {
    discovery.skippedReason = "preview_or_preflight";
  }

  // Discovery → identity → mechanism → ORS/WHO handoff (works with ZERO existing scaling inventory)
  if (runHandoffMaturation && blockers.length === 0 && discovery.rawHits.length) {
    const existingResolved = resolveExistingOpportunitiesForResearch(
      cfg.hotelId,
      () => {
        // Sync stub — async canonical load only when clean-room inactive.
        return { opportunities: [] };
      },
    );
    let existingForHandoff = existingResolved.opportunities || [];
    if (!isCleanRoomActive()) {
      const fullDoc = await loadOpportunitiesCanonical(cfg.hotelId);
      existingForHandoff = fullDoc.opportunities || [];
    }
    let handoffResearcher = null;
    if (allowPaid && runSpentUsd() < cfg.budgetCeilingUsd - 0.05) {
      handoffResearcher = createLiveDeepResearchResearcher({ ledgerPath });
    }
    try {
      handoff = await runScalingDiscoveryHandoffPipeline({
        rawHits: discovery.rawHits,
        hotel: ctx.hotel,
        existingOpportunities: existingForHandoff,
        maxCandidates: cfg.maxCandidates,
        runOrs: true,
        researcher: handoffResearcher,
        remainingBudgetUsd: Math.max(
          0,
          Number(cfg.budgetCeilingUsd || 0) - runSpentUsd(),
        ),
      });
    } catch (err) {
      if (err?.code === "PRE_ORS_V2_REQUIRED") {
        blockers.push({
          code: "PRE_ORS_V2_REQUIRED",
          message: String(err.message || err),
          stage: "discovery_handoff_pre_ors_v2",
        });
        handoff = {
          assessment: null,
          maturation: { matured: [], preOrsScreening: { available: false, required: true, failed: true } },
          honestMetrics: { PRE_ORS_V2_FAILED: 1 },
        };
      } else {
        throw err;
      }
    }
    if (blockers.some((b) => b.code === "PRE_ORS_V2_REQUIRED")) {
      // Fail closed — do not continue ORS maturation on this run.
    } else {
    reconciliation.netNewThisRun = (handoff.maturation?.matured || [])
      .filter((r) => r.countsAsGenuinelyNetNew)
      .map((r) => ({
        organizationName: r.organizationName,
        sourceUrl: r.sourceUrl,
        status: FUNNEL_STAGE.GENUINELY_NET_NEW,
        publicationAuditLabel: r.publicationAuditLabel,
        opportunityId: r.opportunityPayload?.id || null,
      }));
    reconciliation.funnel = handoff.honestMetrics;
    reconciliation.note =
      "NET_NEW requires identity + mechanism + research eligibility. RAW_DISCOVERY_HIT is never counted as NET_NEW.";

    for (const row of handoff.maturation?.matured || []) {
      if (row.ors?.attempted) {
        orsResults.push({
          id: row.opportunityPayload?.id || row.candidateId,
          organizationName: row.organizationName,
          ok: row.ors.ok,
          offline: row.ors.offline,
          contactEmail: row.contactEmail,
          error: row.ors.error || null,
        });
      }
      if (row.whoStatus && row.whoStatus !== "NOT_RUN") {
        buyerIntelResults.push({
          id: row.candidateId,
          organizationName: row.organizationName,
          ok: true,
          stakeholderCount: (row.namedStakeholders || []).length,
          whoResearchStatus: row.whoStatus,
          contactEmail: row.contactEmail,
        });
      }
    }
    }
  }

  // Lifecycle continuation runs even when net-new discovery also ran — do not
  // starve existing HOLD / published-but-unqualified buyer gaps (e.g. PMG).
  if (
    isFullE2e &&
    blockers.length === 0 &&
    (lifecycleTargets.length ||
      (!discovery.rawHits.length && ctx.scalingInventory.length))
  ) {
    // Lifecycle-driven continuation/refresh/requalify (not blind refresh of all inventory)
    const fullDoc = fullDocForLifecycle;
    const byId = new Map((fullDoc.opportunities || []).map((o) => [o.id, o]));
    const byOrg = new Map(
      (fullDoc.opportunities || []).map((o) => [
        text(o.organizationName).toLowerCase(),
        o,
      ]),
    );
    let researcher = null;
    if (allowPaid && runSpentUsd() < cfg.budgetCeilingUsd - 0.05) {
      researcher = createLiveDeepResearchResearcher({ ledgerPath });
    }
    const alreadyMaturedOrgs = new Set(
      [
        ...(handoff?.maturation?.matured || []),
        ...lifecycleMaturedRows,
      ]
        .map((r) => text(r.organizationName || r.opportunity?.organizationName).toLowerCase())
        .filter(Boolean),
    );
    const targets = (
      lifecycleTargets.length > 0
        ? lifecycleTargets
        : ctx.scalingInventory.slice(0, cfg.maxCandidates).map((s) => ({
            opportunityId: s.id,
            organizationName: s.organizationName,
            decision: "REFRESH",
            unpublishedHold: false,
          }))
    ).filter(
      (t) =>
        !alreadyMaturedOrgs.has(text(t.organizationName).toLowerCase()) &&
        t.decision !== "NO_ACTION" &&
        t.decision !== "MONITOR" &&
        t.decision !== "EXCLUDE",
    );
    for (const target of targets) {
      // Unpublished HOLD without Airtable id: plan-only in unpaid/preview; paid path
      // continues via discovery handoff when saved hits include the org.
      const opp =
        (target.opportunityId && byId.get(target.opportunityId)) ||
        byOrg.get(text(target.organizationName).toLowerCase()) ||
        null;
      if (!opp) {
        orsResults.push({
          id: target.opportunityId || null,
          organizationName: target.organizationName,
          ok: true,
          offline: true,
          lifecycleDecision: target.decision,
          skippedLiveOrs: true,
          note: target.gateBeforeContactEnrichment
            ? "identity_gate_before_contact_enrichment"
            : "hold_candidate_queued_without_canonical_row",
        });
        continue;
      }
      if (target.decision === "NO_ACTION" || target.decision === "MONITOR") continue;
      try {
        let deep = null;
        let deepError = null;
        try {
          const remainingBudgetUsd = Math.max(
            0,
            Number(cfg.budgetCeilingUsd || 0) - runSpentUsd(),
          );
          deep = await runDeepResearchOnExistingGdiOpportunity({
            opportunity: opp,
            researcher,
            offlineLongform: !researcher,
            options: {
              authorizeSurfe: false,
              authorizePdl: false,
              // run-050 addendum: match Association FullE2E — live researcher +
              // authorized budget must unset paidBlocked or Exa/Firecrawl never run.
              paidBlocked: !(allowPaid && remainingBudgetUsd > 0.05 && researcher),
              remainingBudgetUsd,
              maxIterations: 6,
              executeLeadFollowUps: true,
              maxLeadFollowUpsPerOpportunity: 4,
              allowReplan: true,
              allowVoiContinue: true,
              ledgerPath,
              allowResearchActionContract: true,
            },
          });
        } catch (deepErr) {
          // Unpaid / corpus-missing ORS must not drop the row — investigation
          // loop still closes buyer/demand/brief gaps from held evidence.
          deepError = deepErr;
        }
        let matured =
          deep?.after?.opportunity || deep?.opportunity || deep?.matured || opp;

        // Shared related-entity drain (same as Association runner) when ORS
        // leaves an executable PENDING next step after adaptation.
        let pendingGuard = 0;
        while (
          pendingGuard < 2 &&
          researcher?.execute &&
          allowPaid &&
          matured?.stopReason === STOP_REASONS.RELATED_ENTITY_FOLLOWUPS_PENDING &&
          matured?.nextResearchStep?.executable === true &&
          runSpentUsd() < cfg.budgetCeilingUsd - 0.05
        ) {
          pendingGuard += 1;
          const drain = await drainRelatedEntityFollowUpsThroughOrs({
            state: matured,
            researcher,
            targetHotel: ctx.hotel?.displayName || opp.hotelName || "",
            remainingBudgetUsd: Math.max(
              0,
              Number(cfg.budgetCeilingUsd || 0) - runSpentUsd(),
            ),
            maxFollowUps: 4,
            paidBlocked: false,
          });
          matured = {
            ...matured,
            ...drain.state,
            id: matured.id || opp.id,
            notes: [
              ...(matured.notes || []),
              ...(drain.state?.notes || []),
              `scaling_runner_related_entity_drain_wave:${pendingGuard}`,
            ],
          };
        }
        // Same presentation + two-contact + evidence choke points as discovery handoff
        // (lifecycle refresh must not bypass named-person / claim-to-source gates).
        matured = attachSecondaryNamedContact(
          {
            ...matured,
            hotelId: cfg.hotelId,
            researchSegment: "scaling_companies",
            namedStakeholders:
              matured.namedStakeholders || opp.namedStakeholders || [],
          },
          { allowSavedEvidence: true },
        );
        matured = applyScalingTwoContactStandard(
          applyScalingSalesPresentation(matured, {
            hotelDisplayName:
              ctx.hotel?.displayName || ctx.profile?.displayName || null,
            hotel: ctx.hotel || ctx.profile || null,
            applyCommercialIntelligenceBrief: true,
          }),
          { force: true },
        );
        const evidenceAudit = auditScalingClaimToSource(matured, {
          hotelId: cfg.hotelId,
          city: ctx.hotel?.city || ctx.profile?.city || null,
        });
        const requal = requalifyScalingOpportunity(matured, evidenceAudit);
        matured = {
          ...matured,
          scalingEvidenceIntegrityVersion: evidenceAudit.version,
          scalingEvidenceFindings: evidenceAudit.findings,
          scalingEvidenceIntegrityPass: evidenceAudit.evidenceIntegrityPass,
          scalingCommercialDisposition:
            requal.disposition || matured.scalingCommercialDisposition || null,
        };
        if (
          requal.customerFacingEligible === false &&
          /HOLD_|EXCLUDE|BETTER_FIT/i.test(requal.disposition || "")
        ) {
          matured.customerVisible = false;
          matured.hotelFacingEligible = false;
        }
        lifecycleMaturedRows.push({
          opportunity: matured,
          publicationAuditLabel:
            requal.disposition === "PUBLISHABLE_SCALING_ACCOUNT"
              ? "PUBLICATION_READY"
              : requal.disposition || "RESEARCH_CONTINUATION",
          hotelFacingEligible: requal.customerFacingEligible !== false,
          lifecycleDecision: target.decision,
          countsAsGenuinelyNetNew: false,
        });
        orsResults.push({
          id: opp.id,
          organizationName: opp.organizationName,
          ok: deepError ? false : deep?.ok !== false,
          offline: !researcher,
          lifecycleDecision: target.decision,
          gateBeforeContactEnrichment: target.gateBeforeContactEnrichment === true,
          contactEmail: matured.contactEmail || matured.primaryContact?.email || null,
          primaryNamed: matured.contactName || matured.primaryContact?.name || null,
          secondaryNamed: matured.secondaryContact?.name || null,
          coverage: matured.scalingContactCoverage || null,
          error: deepError ? String(deepError.message || deepError) : null,
          continuedViaInvestigationLoop: Boolean(deepError),
        });
      } catch (err) {
        // Last resort: still queue held opportunity for investigation loop
        lifecycleMaturedRows.push({
          opportunity: {
            ...opp,
            hotelId: cfg.hotelId,
            researchSegment: "scaling_companies",
          },
          publicationAuditLabel: "RESEARCH_CONTINUATION",
          hotelFacingEligible: false,
          lifecycleDecision: target.decision,
          countsAsGenuinelyNetNew: false,
        });
        orsResults.push({
          id: opp.id,
          ok: false,
          error: err.message,
          lifecycleDecision: target.decision,
          continuedViaInvestigationLoop: true,
        });
      }
      if (allowPaid && runSpentUsd() >= cfg.budgetCeilingUsd - 0.01) break;
    }
  }

  // Adaptive investigation loop — close commercial/buyer gaps before publication
  // (free public + saved evidence; never Surfe/PDL/Parallel; never invent contacts).
  let investigationStage = null;
  let publicationMaturedRows = [
    ...(handoff?.maturation?.matured || []),
    ...lifecycleMaturedRows,
  ];
  if (
    blockers.length === 0 &&
    publicationMaturedRows.length > 0 &&
    (isFullE2e || truthy(rawConfig.processDiscoveryHandoff))
  ) {
    const investigationPaidSearch =
      allowPaid && runSpentUsd() < cfg.budgetCeilingUsd - 0.05
        ? async ({ query }) => {
            if (runSpentUsd() >= cfg.budgetCeilingUsd - 0.02) {
              return { status: "budget_exhausted", hits: [], costUsd: 0 };
            }
            const before = ledgerSpent(ledgerPath);
            const exa = await maybeExaSearch(query, ledgerPath);
            return {
              ...exa,
              costUsd: Number((ledgerSpent(ledgerPath) - before).toFixed(6)),
            };
          }
        : null;
    investigationStage = await runInvestigationLoopOnMaturedRows({
      maturedRows: publicationMaturedRows,
      hotel: ctx.hotel || ctx.profile || {},
      // Free public fetches always; paid Exa only when research authorized + budget remains.
      allowFreeFetch: isFullE2e || truthy(rawConfig.allowInvestigationFetch),
      maxIterations: Number(rawConfig.investigationMaxIterations) || 8,
      maxPageFetches: Number(rawConfig.investigationMaxPageFetches) || 10,
      paidSearch: investigationPaidSearch,
    });
    publicationMaturedRows = investigationStage.maturedRows || publicationMaturedRows;
    if (reportDir) {
      writeJson(
        path.join(reportDir, "16-intelligent-investigation-loop.json"),
        {
          version: SCALING_INTELLIGENT_INVESTIGATION_LOOP_V1,
          results: investigationStage.results,
          buyerStrategiesCatalog: investigationStage.buyerStrategiesCatalog,
        },
      );
      // Persist full post-investigation commercial rows (gold briefs + contacts)
      // so local-shadow ingest and drawer validation do not lose research depth.
      writeJson(
        path.join(reportDir, "16b-post-investigation-matured.json"),
        {
          version: `${SCALING_INTELLIGENT_INVESTIGATION_LOOP_V1}:post-investigation-matured`,
          count: publicationMaturedRows.length,
          maturedRows: publicationMaturedRows,
        },
      );
    }
  }

  // Publication + customer-share verification (independent of paid research)
  if (blockers.length === 0 && (isFullE2e || truthy(rawConfig.processDiscoveryHandoff))) {
    publicationStage = await runScalingFullE2ePublicationStage({
      hotelId: cfg.hotelId,
      maturedRows: publicationMaturedRows,
      authorizePublication: cfg.authorizePublication === true,
      authorizeProductionWrites: cfg.authorizeProductionWrites === true,
      applyProductionWrites: cfg.applyProductionWrites === true,
      reportDir,
      probeLiveShare: cfg.probeLiveShare === true,
      authorizedBy: SCALING_COMPANIES_HOTEL_RUNNER_V1,
    });
  } else if (isPreview) {
    publicationStage = {
      version: SCALING_FULL_E2E_PUBLICATION_V1,
      status: "PREVIEW_SKIPPED",
      authorizePublication: cfg.authorizePublication === true,
      note: "Preview mode does not publish. Re-run Mode FullE2E with -AuthorizePublication to publish.",
      fullE2eComplete: false,
    };
  }

  const spentUsd = runSpentUsd();
  const aggregateSpentUsd = ledgerSpent(ledgerPath);
  const honest = handoff?.honestMetrics || null;
  const fullE2eComplete =
    isFullE2e &&
    blockers.length === 0 &&
    Boolean(handoff || orsResults.length) &&
    publicationStage?.fullE2eComplete === true &&
    cfg.authorizePublication === true;

  const summary = {
    ok: blockers.length === 0,
    executed:
      ((isFullE2e || Boolean(handoff)) && blockers.length === 0),
    fullE2eComplete,
    fullE2eCompleteReason: fullE2eComplete
      ? "full_sequence_with_authorized_publication_and_share_verification"
      : !isFullE2e
        ? "mode_not_full_e2e"
        : !cfg.authorizePublication
          ? "publication_not_authorized"
          : publicationStage?.status || "publication_or_research_incomplete",
    version: SCALING_COMPANIES_HOTEL_RUNNER_V1,
    discoveryVersion: AUTONOMOUS_DEMAND_DISCOVERY_V5,
    researchVersion: SCALING_COMPANIES_RESEARCH_VERSION,
    publicationVersion: SCALING_FULL_E2E_PUBLICATION_V1,
    stages: SCALING_COMPANIES_FULL_E2E_STAGES,
    startedAt,
    finishedAt: new Date().toISOString(),
    config: {
      hotelId: cfg.hotelId,
      mode: cfg.mode,
      authorizePaidResearch: cfg.authorizePaidResearch,
      authorizePublication: cfg.authorizePublication,
      authorizeProductionWrites: cfg.authorizeProductionWrites,
      applyProductionWrites: cfg.applyProductionWrites,
      probeLiveShare: cfg.probeLiveShare,
      budgetCeilingUsd: cfg.budgetCeilingUsd,
      maxCandidates: cfg.maxCandidates,
      runId: cfg.runId,
      reportDir,
    },
    hotel: ctx.profile,
    applicability: ctx.applicability,
    persistenceMode: ctx.persistenceMode,
    opportunitySource: ctx.opportunitySource,
    opportunityCount: ctx.opportunityCount,
    preflight,
    fullE2ePublicationGate,
    blockers,
    reconciliation,
    intelligentLifecycle: {
      version: SCALING_INTELLIGENT_LIFECYCLE_V1,
      summary: lifecycleSummary,
      researchQueue: lifecycleTargets,
      discoveryPlan: lifecyclePlan.discoveryPlan,
    },
    funnel: honest,
    genuinelyNetNewCount: honest?.genuinelyNetNew ?? reconciliation.netNewThisRun.length,
    publicationReadyInternalCount: honest?.publicationReadyInternal ?? 0,
    publicationReadiness,
    discovery: {
      ...discovery,
      rawHits: undefined,
      rawHitCount: discovery.rawHits?.length || discovery.hits || 0,
    },
    handoff: handoff
      ? {
          shortlistCount: handoff.assessment?.shortlist?.length || 0,
          maturedCount: handoff.maturation?.matured?.length || 0,
          metrics: honest,
        }
      : null,
    ors: {
      path: orsPath,
      results: orsResults,
      plannedOnly: !handoff && !isFullE2e,
    },
    buyerIntelligence: buyerIntelResults,
    investigationLoop: investigationStage
      ? {
          version: SCALING_INTELLIGENT_INVESTIGATION_LOOP_V1,
          results: investigationStage.results || [],
          qualifiedAfterLoop: (investigationStage.results || []).filter(
            (r) => r.publicationQualified,
          ).length,
          continuationAfterLoop: (investigationStage.results || []).filter(
            (r) => !r.publicationQualified,
          ).length,
        }
      : null,
    contacts: publicationReadiness.map((r) => ({
      id: r.id,
      organizationName: r.organizationName,
      contactAuditClass: r.contactAuditClass,
      usableContactRoute: r.usableContactRoute,
    })),
    spending: {
      ledgerPath,
      ceilingUsd: cfg.authorizePaidResearch ? cfg.budgetCeilingUsd : 0,
      spentUsd,
      runSpentUsd: spentUsd,
      ledgerBaselineSpentUsd,
      aggregateSpentUsd,
      sharedCampaignLedger:
        ledgerPath.includes("campaign-aggregate-20usd.json")
        || cfg.useSharedCampaignLedger === true,
      remainingUsd: Number(
        ((cfg.authorizePaidResearch ? cfg.budgetCeilingUsd : 0) - spentUsd).toFixed(6),
      ),
    },
    publication: publicationStage,
    productionWrite: {
      attempted: publicationStage?.upserts?.some((u) => u.upsert?.attempted) === true,
      applied: publicationStage?.upserts?.some((u) => u.upsert?.applied) === true,
      publishedIds: publicationStage?.publishedIds || [],
      status: publicationStage?.status || null,
    },
    strategiesSample: ctx.strategies.slice(0, 8).map((s) => ({
      pathway: s.pathway,
      researchQuestion: s.researchQuestion,
    })),
    localTerms: ctx.localTerms,
  };

  writeJson(path.join(reportDir, "SUMMARY.json"), summary);
  writeJson(path.join(reportDir, "01-hotel-profile.json"), ctx.profile);
  writeJson(path.join(reportDir, "02-applicability.json"), ctx.applicability);
  writeJson(path.join(reportDir, "03-strategies.json"), ctx.strategies);
  writeJson(path.join(reportDir, "04-scaling-inventory.json"), ctx.scalingInventoryEvaluated);
  writeJson(path.join(reportDir, "05-reconciliation.json"), reconciliation);
  writeJson(path.join(reportDir, "06-publication-readiness.json"), publicationReadiness);
  writeJson(path.join(reportDir, "07-preflight.json"), preflight);
  writeJson(path.join(reportDir, "07b-full-e2e-publication-gate.json"), fullE2ePublicationGate);
  writeJson(path.join(reportDir, "08-discovery.json"), {
    ...discovery,
    rawHits: (discovery.rawHits || []).slice(0, 100),
  });
  writeJson(path.join(reportDir, "09-ors-and-buyer-intelligence.json"), {
    orsPath,
    orsResults,
    buyerIntelResults,
  });
  writeJson(path.join(reportDir, "10-spending.json"), summary.spending);
  if (handoff) {
    writeJson(path.join(reportDir, "11-discovery-handoff-assessment.json"), handoff.assessment);
    writeJson(path.join(reportDir, "12-discovery-handoff-maturation.json"), handoff.maturation);
    writeJson(path.join(reportDir, "13-honest-funnel-metrics.json"), honest);
  }
  if (publicationStage) {
    writeJson(path.join(reportDir, "17-publication-stage.json"), publicationStage);
  }
  writeJson(path.join(reportDir, "18-full-e2e-acceptance.json"), {
    fullE2eComplete: summary.fullE2eComplete,
    reason: summary.fullE2eCompleteReason,
    mode: cfg.mode,
    authorizePaidResearch: cfg.authorizePaidResearch,
    authorizePublication: cfg.authorizePublication,
    applyProductionWrites: cfg.applyProductionWrites,
    genuinelyNetNewCount: summary.genuinelyNetNewCount,
    orsCount: orsResults.length,
    buyerIntelCount: buyerIntelResults.length,
    publicationStatus: publicationStage?.status || null,
    publishedIds: publicationStage?.publishedIds || [],
    fullE2ePublicationGate,
    spentUsd,
  });
  writeMd(
    path.join(reportDir, "RUN_REPORT.md"),
    [
      `# Scaling Companies Hotel Runner — ${ctx.profile.displayName || cfg.hotelId}`,
      "",
      `- **Mode:** ${cfg.mode}`,
      `- **Hotel ID:** ${cfg.hotelId}`,
      `- **OK:** ${summary.ok}`,
      `- **Executed:** ${summary.executed}`,
      `- **FULL E2E COMPLETE:** ${summary.fullE2eComplete}`,
      `- **Full E2E reason:** ${summary.fullE2eCompleteReason}`,
      `- **Ready for authorized Full E2E publication:** ${fullE2ePublicationGate.readyForAuthorizedFullE2ePublication}`,
      `- **Ready for live customer-share claims:** ${fullE2ePublicationGate.readyForLiveCustomerShareClaims}`,
      `- **Local reviewed-prospect allowlist:** ${fullE2ePublicationGate.localReviewedProspectAllowlistReady}`,
      `- **Production explicit allowlist:** ${fullE2ePublicationGate.productionExplicitAllowlistReady}`,
      `- **Existing scaling opportunities:** ${reconciliation.existingScalingCount}`,
      `- **Raw discovery hits:** ${honest?.rawDiscoveryHits ?? discovery.hits}`,
      `- **Genuinely NET_NEW:** ${summary.genuinelyNetNewCount}`,
      `- **Publication-ready (internal):** ${summary.publicationReadyInternalCount}`,
      `- **Publication status:** ${publicationStage?.status || "n/a"}`,
      `- **Published IDs:** ${(publicationStage?.publishedIds || []).join(", ") || "(none)"}`,
      `- **Spent USD:** ${spentUsd} / ${summary.spending.ceilingUsd}`,
      `- **Blockers:** ${blockers.length ? blockers.join(", ") : "none"}`,
      ...(fullE2ePublicationGate.missingConfiguration.length
        ? [
            "",
            "## Full E2E publication gate — missing configuration",
            "",
            ...fullE2ePublicationGate.missingConfiguration.map((m) => `- ${m}`),
            "",
            "_Allowlist permission ≠ commercial qualification of an opportunity. Founder authorization required to add hotels to production allowlists._",
          ]
        : []),
      "",
      "## Stages",
      "",
      ...SCALING_COMPANIES_FULL_E2E_STAGES.map((s) => `- ${s}`),
      "",
      "## Funnel (honest)",
      "",
      honest
        ? Object.entries(honest)
            .filter(([k]) => k !== "note")
            .map(([k, v]) => `- **${k}:** ${v}`)
            .join("\n")
        : "- (no discovery handoff this run)",
      "",
      "## Notes",
      "",
      "- RAW_DISCOVERY_HIT is never counted as NET_NEW or opportunities.",
      "- Defaults remain fail-closed: no Surfe/PDL/Parallel/outreach/weekly.",
      "- Paid research and publication are independent authorizations.",
      "- Preview profile pass does not imply Full E2E / customer-share readiness without reviewed-prospect allowlist.",
      "- FULL E2E COMPLETE requires authorized publication + share verification (or zero qualifying records).",
      "- Airtable write alone is not external customer-share proof.",
      "",
    ].join("\n"),
  );

  return summary;
}

export function buildDefaultConfigPath(hotelId) {
  const safe = text(hotelId).replace(/[^\w-]+/g, "_") || "hotel";
  return path.join(
    ROOT,
    "reports/gdi-scaling-companies-hotel-runner-v1/configs",
    `${safe}.json`,
  );
}
