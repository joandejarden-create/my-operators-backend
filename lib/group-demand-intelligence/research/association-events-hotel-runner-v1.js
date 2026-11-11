/**
 * Association / Conference / Event-Driven Demand hotel runner V1.
 *
 * Reuses Scaling Companies research services (ledger, Exa, ORS, investigation loop,
 * publication stage helpers) with Event-to-Demand taxonomy + association-specific
 * discovery, qualification, and gold briefs.
 *
 * Does NOT create a duplicate RESEARCH_SEGMENT — primary axis is associations_government
 * with medical_academic / exhibitor / planners mapping per event domain.
 */

import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";
import { resolveHotelGdiProfileForResearch } from "../hotel-gdi-profile-resolution-v1.js";
import {
  loadOrCreateLiveDeepResearchLedger,
  assertLiveDeepResearchLedger,
  preflightLiveDeepResearchPricing,
  maybeExaSearch,
  createLiveDeepResearchResearcher,
  reconcileLiveLedgerReservations,
} from "./deep-research-gdi-live-certification-v1.js";
import {
  WATERSTONE_HOTEL_ID,
  CHAPULTEPEC_HOTEL_ID,
  campaignAggregateLedgerPath as wcCampaignAggregateLedgerPath,
  assertPaidExecutionAllowed as assertWcPaidExecutionAllowed,
  readLedgerSnapshot as readWcLedgerSnapshot,
} from "./waterstone-chapultepec-shared-budget-v1.js";
import { assertPaidContactProvidersDisabled } from "./commercial-spine-restoration-v41.js";
import { runDeepResearchOnExistingGdiOpportunity } from "./deep-research-gdi-integration-v1.js";
import {
  evaluateProspectPublicationEligibility,
} from "../prospect-publication-eligibility-v1.js";
import {
  assessGdiHotelPreviewReadiness,
  assessHotelAuthorizationStates,
} from "../adp-gdi-universal-hotel-roster-v1.js";
import { loadOpportunitiesCanonical } from "../opportunity-persistence.js";
import { isCleanRoomActive } from "./clean-room-v26-isolation-v1.js";
import {
  attachSecondaryNamedContact,
  looksLikePersonName,
} from "../secondary-named-contact-v1.js";
import {
  runInvestigationLoopOnMaturedRows,
} from "./scaling-intelligent-investigation-loop-v1.js";
import {
  runScalingFullE2ePublicationStage,
} from "./scaling-companies-full-e2e-publication-v1.js";
import {
  normalizeVerifiedEvent,
  eventDerivedOpportunityId,
} from "./event-to-demand/event-identity-v1.js";
import {
  EVENT_DEMAND_DOMAIN,
  EVENT_DEMAND_MECHANISM,
  primarySegmentForDomain,
} from "./event-to-demand/event-to-demand-constants-v1.js";
import { assessEventHotelFit } from "./event-to-demand/event-hotel-fit-v1.js";
import { normalizeOrgIdentityKey } from "../workflow/reuse-prior-intelligence-v1.js";
import {
  ASSOCIATION_EVENTS_DISCOVERY_V1,
  ASSOCIATION_EVENTS_LAUNCHER_SEGMENT,
  buildAssociationEventsDiscoveryStrategies,
  hotelAssociationEventsFitProfile,
} from "./association-events-discovery-strategies-v1.js";
import {
  ASSOCIATION_EVENTS_QUALIFICATION_V1,
  evaluateAssociationEventsPublicationQualification,
  applyAssociationEventsGateToEligibility,
} from "./association-events-qualification-v1.js";
import { attachGdi2ShadowEvaluation } from "../gdi2-shadow-publication-evaluation-v1.js";
import { planBuyerRecovery } from "../buyer-recovery-planner-v1.js";
import {
  ASSOCIATION_EVENTS_GOLD_BRIEF_V1,
  applyAssociationEventsGoldBrief,
  scoreAssociationEventsGoldBrief,
} from "./association-events-gold-brief-v1.js";
import {
  ASSOCIATION_EVENTS_BUYER_RECOVERY_V1,
  cleanAssociationOrganizationName,
  runAssociationEventsBuyerRecovery,
} from "./association-events-buyer-recovery-v1.js";
import {
  ASSOCIATION_EVENTS_LONG_FORM_INVESTIGATION_V1,
  runAssociationEventsLongFormOnMaturedRows,
} from "./association-events-long-form-investigation-v1.js";
import {
  scrubAssociationEventsContactFields,
  scrubAssociationEventsPhone,
  assessAssociationEventsBuyerCandidate,
} from "./association-events-buyer-integrity-v1.js";
import {
  applyAssociationEventsChronology,
} from "./association-events-chronology-v1.js";
import {
  applyEvidenceLedgerToOpportunity,
} from "./association-events-evidence-ledger-v1.js";
import { normalizeAssociationEventsOpportunityIdentity } from "./association-events-identity-normalize-v1.js";
import { drainRelatedEntityFollowUpsThroughOrs } from "./commercial-spine-restoration-v41.js";
import {
  STOP_REASONS,
} from "./research-constants-v1.js";

export const ASSOCIATION_EVENTS_HOTEL_RUNNER_V1 =
  "gdi-association-events-hotel-runner-v1";

export const ASSOCIATION_EVENTS_FULL_E2E_STAGES = Object.freeze([
  "hotel_profile",
  "continuous_campaign_budget_reconcile",
  "continuous_campaign_wave_loop",
  "event_discovery",
  "entity_verification",
  "event_chronology",
  "organizer_identity_resolution",
  "net_new_reconciliation",
  "event_demand_mechanism_screening",
  "ors_longform_research",
  "buyer_and_contact_intelligence",
  "deep_sourcing_investigation",
  "named_buyer_research",
  "contact_verification",
  "alternative_lodging_catering_research",
  "hotel_specific_fit",
  "intelligent_investigation_loop",
  "association_long_form_investigation",
  "research_gap_reassessment",
  "commercial_qualification",
  "gold_standard_brief_generation",
  "canonical_opportunity_payload",
  "airtable_publication",
  "customer_share_verification",
]);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function writeJson(p, obj) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  let body;
  try {
    body = `${JSON.stringify(obj, null, 2)}\n`;
  } catch (err) {
    // Guard against recursive/bloated opportunity graphs (notes/supervisor dumps).
    const compact = (v, depth = 0) => {
      if (v == null || typeof v !== "object") return v;
      if (depth > 6) return "[max_depth]";
      if (Array.isArray(v)) return v.slice(0, 40).map((x) => compact(x, depth + 1));
      const out = {};
      for (const [k, val] of Object.entries(v)) {
        if (k === "notes" && typeof val === "string" && val.length > 4000) {
          out[k] = `${val.slice(0, 4000)}…[truncated]`;
        } else if (k === "supervisorV2" || k === "heldEvidenceCorpus" || k === "orsCorpus") {
          out[k] = { _omitted: true };
        } else {
          out[k] = compact(val, depth + 1);
        }
      }
      return out;
    };
    body = `${JSON.stringify(compact(obj), null, 2)}\n`;
    if (!body || body.length < 3) {
      throw err;
    }
  }
  if (body.length > 40_000_000) {
    // Prefer a truncated artifact over aborting a live paid campaign mid-wave.
    const truncated = {
      _truncated: true,
      reason: "association_runner_artifact_too_large",
      originalBytes: body.length,
      fileName: path.basename(p),
      preview: typeof obj === "object" && obj
        ? {
            keys: Object.keys(obj).slice(0, 40),
            count: Array.isArray(obj) ? obj.length : undefined,
            opportunitiesCount: Array.isArray(obj.opportunities)
              ? obj.opportunities.length
              : undefined,
            resultsCount: Array.isArray(obj.results) ? obj.results.length : undefined,
          }
        : null,
    };
    body = `${JSON.stringify(truncated, null, 2)}\n`;
  }
  fs.writeFileSync(p, body, "utf8");
}

function hashKey(s) {
  return createHash("sha256").update(String(s || "")).digest("hex").slice(0, 12);
}

function ledgerSpent(ledgerPath) {
  try {
    if (!fs.existsSync(ledgerPath)) return 0;
    const j = JSON.parse(fs.readFileSync(ledgerPath, "utf8"));
    return Number(j.estimatedSpentUsd || j.spentUsd || j.totalSpentUsd || 0) || 0;
  } catch {
    return 0;
  }
}

function initLedger(reportDir, budgetUsd, runId, { hotelId = "" } = {}) {
  const paidCeiling = Math.max(0, Number(budgetUsd) || 0);
  // Waterstone + Chapultepec Association FullE2E must meter against the existing
  // shared $20 aggregate — never a second authorization / isolated paid ledger.
  if (
    paidCeiling > 0 &&
    (hotelId === WATERSTONE_HOTEL_ID || hotelId === CHAPULTEPEC_HOTEL_ID)
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
    `association-events-run-ledger-${safeRun}.json`,
  );
  fs.mkdirSync(path.dirname(ledgerPath), { recursive: true });
  const ledgerBudget = paidCeiling > 0 ? paidCeiling : 0.001;
  loadOrCreateLiveDeepResearchLedger(ledgerPath, {
    budgetUsd: ledgerBudget,
    version: `${ASSOCIATION_EVENTS_HOTEL_RUNNER_V1}:${safeRun}`,
  });
  try {
    reconcileLiveLedgerReservations(ledgerPath);
  } catch {
    /* non-fatal */
  }
  return ledgerPath;
}

function extractEventishFromHit(hit = {}, strategy = {}, hotel = {}) {
  const title = text(hit.title || hit.name || "");
  const excerpt = text(hit.text || hit.excerpt || hit.summary || "");
  const url = hit.url || hit.sourceUrl || null;
  const blob = `${title} ${excerpt}`;
  const year = (blob.match(/\b(202[5-7])\b/) || [])[1] || null;
  const orgMatch =
    blob.match(
      /\b((?:Asociaci[oó]n|Asociación|Colegio|Sociedad|C[aá]mara|Federation|Society|Association|Institute|College)[^.\n|]{2,80})/i,
    ) ||
    blob.match(/\bby\s+([A-Z][^.\n|]{3,70})/);
  const organizer = text(orgMatch?.[1] || "").replace(/\s{2,}/g, " ").slice(0, 120) || null;
  const eventName = title.length >= 8 ? title.slice(0, 160) : null;
  if (!eventName && !organizer) return null;

  const domain = strategy.domain || EVENT_DEMAND_DOMAIN.ASSOCIATION_PROFESSIONAL_CONGRESS;
  const event = normalizeVerifiedEvent({
    eventName: eventName || `${organizer} professional meeting`,
    name: eventName,
    organizer: organizer || eventName,
    city: hotel.city || null,
    eventStartDate: year ? `${year}-01-01` : null,
    dateWindow: year || null,
    verified: Boolean(url && (eventName || organizer)),
    sources: url ? [{ url, sourceFamily: "trade_press", excerpt: excerpt.slice(0, 400) }] : [],
    sourceFamilies: ["trade_press", "industry_calendar"],
    domain,
    discoveryIntent: strategy.intent || null,
  });
  return { event, organizer, excerpt, url, domain };
}

function buildOpportunityFromHit({
  hit,
  strategy,
  hotel,
  existing = [],
  fitProfile,
} = {}) {
  const parsed = extractEventishFromHit(hit, strategy, hotel);
  if (!parsed) return null;
  const { event, organizer, excerpt, url, domain } = parsed;
  const orgName = organizer || event.eventName;
  const orgKey = normalizeOrgIdentityKey(orgName);
  const eventKey = text(event.eventName).toLowerCase();

  const duplicate = (existing || []).find((o) => {
    const sameOrg =
      normalizeOrgIdentityKey(o.organizationName || "") === orgKey ||
      orgNamesLoose(o.organizationName, orgName);
    if (!sameOrg) return false;
    const blob = `${o.eventName || ""} ${o.title || ""} ${o.localConnection || ""}`.toLowerCase();
    return !eventKey || blob.includes(eventKey.slice(0, 40)) || eventKey.includes(text(o.eventName).toLowerCase().slice(0, 40));
  });

  const mechanism =
    fitProfile.positioning === "destination_all_inclusive_meetings"
      ? EVENT_DEMAND_MECHANISM.HOST_EVENT
      : EVENT_DEMAND_MECHANISM.OFFSITE_MEETING;
  const fit = assessEventHotelFit({
    event,
    hotel,
    mechanism,
    evidence: { sourceUrl: url },
  });

  const proposedHotelUse =
    fitProfile.positioning === "destination_all_inclusive_meetings"
      ? `Destination residential meeting / delegate and speaker lodging opportunity at ${hotel.displayName} for ${event.eventName} — verify whether host venue and room blocks are already contracted.`
      : `Mid-size association meeting, board/committee, speaker lodging, or ancillary event demand near ${hotel.displayName} (ballroom ≤~220 / 7 meeting rooms profile) for ${event.eventName}.`;

  const id = eventDerivedOpportunityId({
    organizationName: orgName,
    event,
    mechanism,
  });

  return {
    id,
    hotelId: hotel.hotelId,
    organizationName: orgName,
    eventName: event.eventName,
    eventStartDate: event.eventStartDate || null,
    organizer: organizer || orgName,
    researchSegment: strategy.researchSegment || primarySegmentForDomain(domain),
    discoverySource: "association_events_powershell_v1",
    eventToDemandDomain: domain,
    eventDemandMechanism: mechanism,
    whatIsHappening: excerpt || event.eventName,
    localConnection: `${event.city || hotel.city || ""} — ${event.eventName}`,
    proposedHotelUse,
    hotelFitRationale: (fit.reasons || []).join("; ") || fitProfile.strengths.slice(0, 2).join("; "),
    sourceUrl: url,
    sources: event.sources || [],
    namedStakeholders: [],
    crossSegmentDuplicate: Boolean(duplicate),
    duplicateOf: duplicate?.id || null,
    researchContinuation: true,
    preserveOpportunity: true,
    hotelFitAssess: fit,
    discoveryIntent: strategy.intent || null,
  };
}

function orgNamesLoose(a, b) {
  const na = text(a).toLowerCase();
  const nb = text(b).toLowerCase();
  if (!na || !nb) return false;
  return na.includes(nb) || nb.includes(na);
}

function extractNamedPeopleFromText(blob = "") {
  const people = [];
  const patterns = [
    /([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ'’-]+){1,3})\s*[,–-]\s*([^.\n|]{0,60}?(?:Director|Manager|Presidente|President|Secretari[oa]|Coordinator|Organizer|Gerente)[^.\n|]{0,40})/g,
    /(?:Director|Manager|President|Presidente|Secretario|Secretaria|Coordinator)\s+([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ'’-]+){1,3})/g,
  ];
  for (const re of patterns) {
    let m;
    const r = new RegExp(re.source, re.flags);
    while ((m = r.exec(blob))) {
      const name = text(m[1]);
      const title = text(m[2] || m[0]).slice(0, 80);
      if (looksLikePersonName(name)) people.push({ name, title });
    }
  }
  const seen = new Set();
  return people.filter((p) => {
    const k = p.name.toLowerCase();
    if (seen.has(k)) return false;
    seen.add(k);
    return true;
  }).slice(0, 5);
}

function extractContactRoutes(blob = "", url = null) {
  const email = (blob.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i) || [])[0] || null;
  const rawPhone = (blob.match(/(?:\+?\d[\d\s().-]{7,}\d)/) || [])[0] || null;
  // Dates like 2026-10-28 match digit/dash patterns — scrub before promotion.
  const phone = scrubAssociationEventsPhone(rawPhone);
  return {
    contactEmail: email,
    contactPhone: phone || null,
    _rejectedContactPhone: rawPhone && !phone ? text(rawPhone) : null,
    contactUrl: url,
  };
}

export function normalizeAssociationEventsRunnerConfig(raw = {}) {
  const modeRaw = String(raw.mode || "preview").toLowerCase();
  const weeklyIncremental = raw.weeklyIncremental === true;
  const certifyOnly =
    raw.certifyOnly === true ||
    modeRaw === "certify-only" ||
    modeRaw === "certifyonly";
  const mode =
    certifyOnly
      ? "certify-only"
      : modeRaw === "full-e2e" ||
          modeRaw === "execute" ||
          modeRaw === "fulle2e" ||
          modeRaw === "weeklyincremental"
        ? "full-e2e"
        : "preview";
  const hotelId = text(raw.hotelId || raw.hotel_id);
  const budgetCeilingUsd = Number(raw.budgetCeilingUsd ?? raw.budgetUSD ?? 0) || 0;
  const authorizePaidResearch = raw.authorizePaidResearch === true;
  const authorizePublication = raw.authorizePublication === true;
  const applyProductionWrites =
    raw.applyProductionWrites === true && authorizePublication === true;
  // Weekly incremental suppresses continuous multi-wave by default (edition refresh, not wave hunt).
  const continuousCampaign =
    !weeklyIncremental &&
    !certifyOnly &&
    (raw.continuousCampaign === true ||
      (raw.continuousCampaign !== false &&
        mode === "full-e2e" &&
        authorizePaidResearch === true &&
        raw.continuousCampaignWave !== true));
  return {
    schemaVersion: "gdi-association-events-run-request-v1",
    runId:
      text(raw.runId) ||
      `assoc-${new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19)}-${hashKey(hotelId).slice(0, 6)}`,
    hotelId,
    hotelName: text(raw.hotelName || ""),
    researchSegment: ASSOCIATION_EVENTS_LAUNCHER_SEGMENT,
    customerSegmentUmbrella: "Association / Conference / Events",
    mode,
    weeklyIncremental,
    certifyOnly,
    budgetCeilingUsd,
    budgetUSD: budgetCeilingUsd,
    authorizePaidResearch,
    authorizePublication,
    authorizeProductionWrites: raw.authorizeProductionWrites === true || authorizePublication,
    applyProductionWrites,
    probeLiveShare: raw.probeLiveShare === true,
    savedDiscoveryPath: raw.savedDiscoveryPath || null,
    maxCandidates: Math.min(Number(raw.maxCandidates) || 12, 20),
    targetNewOpportunities: Number(raw.targetNewOpportunities) || null,
    reportDir: raw.reportDir || raw.outputDirectory || null,
    stages: ASSOCIATION_EVENTS_FULL_E2E_STAGES,
    continuousCampaign,
    continuousCampaignWave: raw.continuousCampaignWave === true,
    waveIndex: Number(raw.waveIndex) || null,
    campaignAllowDiscoveryWithSavedEvidence:
      raw.campaignAllowDiscoveryWithSavedEvidence === true,
    campaignDiscoveryBudgetUsd:
      Number(raw.campaignDiscoveryBudgetUsd) > 0
        ? Number(raw.campaignDiscoveryBudgetUsd)
        : null,
    campaignExpandedStrategies: Array.isArray(raw.campaignExpandedStrategies)
      ? raw.campaignExpandedStrategies
      : null,
    campaignStrategyIntents: Array.isArray(raw.campaignStrategyIntents)
      ? raw.campaignStrategyIntents
      : null,
    buyerRecoveryMaxQueriesPerOpp:
      Math.min(Math.max(Number(raw.buyerRecoveryMaxQueriesPerOpp) || 3, 1), 8),
    maxWaves: Number(raw.maxWaves) || null,
    mockProviderFullE2e:
      raw.mockProviderFullE2e === true ||
      String(process.env.GDI_EVENTS_MOCK_PROVIDER_FULL_E2E || "") === "1",
    providersAllowed: Array.isArray(raw.providersAllowed)
      ? raw.providersAllowed.map(String)
      : null,
    providersForbidden: Array.isArray(raw.providersForbidden)
      ? raw.providersForbidden.map(String)
      : null,
    forbidFirecrawl:
      raw.forbidFirecrawl === true ||
      (Array.isArray(raw.providersForbidden) &&
        raw.providersForbidden.map(String).includes("firecrawl")),
    authorization: {
      paidResearch: authorizePaidResearch,
      customerPublication: authorizePublication,
      surfe: false,
      pdl: false,
      parallel: false,
      prospectOutreach: false,
      // Launcher never auto-activates weekly scheduling; flag is informational for weeklyIncremental runs.
      weeklyActivation: false,
      weeklyIncremental,
    },
  };
}

export async function resolveAssociationEventsHotelContext(cfg) {
  const resolved = await resolveHotelGdiProfileForResearch(cfg.hotelId, {
    preferLive: true,
  });
  const profile = resolved?.hotel || resolved?.profile || (resolved?.ok === false ? null : resolved);
  const hotel = {
    hotelId: cfg.hotelId,
    displayName:
      profile?.displayName ||
      profile?.name ||
      cfg.hotelName ||
      cfg.hotelId,
    city: profile?.city || null,
    country: profile?.country || null,
    demandTerritory: profile?.demandTerritory || null,
    capabilityProfile: profile?.capabilityProfile || null,
    commercialPriorities: profile?.commercialPriorities || null,
    totalGuestrooms:
      profile?.totalGuestrooms ||
      profile?.capabilityProfile?.totalGuestrooms ||
      null,
  };
  // Founder-provided Mexico meeting attributes when profile lacks them
  const fitProfile = hotelAssociationEventsFitProfile(hotel);
  if (!hotel.capabilityProfile) hotel.capabilityProfile = {};
  if (fitProfile.meetingRooms && !hotel.capabilityProfile.meetingRoomsIndoor) {
    hotel.capabilityProfile.meetingRoomsIndoor = fitProfile.meetingRooms;
  }
  if (fitProfile.eventSpaceSqFt && !hotel.capabilityProfile.totalMeetingSpaceSqFt) {
    hotel.capabilityProfile.totalMeetingSpaceSqFt = fitProfile.eventSpaceSqFt;
  }
  const strategies = buildAssociationEventsDiscoveryStrategies(hotel);
  return {
    profile,
    profileResolution: resolved?.resolution || null,
    hotel,
    strategies,
    fitProfile,
  };
}

export async function runAssociationEventsHotelRunner(rawConfig = {}) {
  const cfg = normalizeAssociationEventsRunnerConfig(rawConfig);
  if (!cfg.hotelId) {
    return { ok: false, error: "hotelId_required", version: ASSOCIATION_EVENTS_HOTEL_RUNNER_V1 };
  }

  // Zero-paid mock Exa Agent FullE2E — same Discovery/Deep Research contracts, no paid spend.
  if (cfg.mockProviderFullE2e === true) {
    if (cfg.authorizePaidResearch === true || cfg.budgetCeilingUsd > 0) {
      return {
        ok: false,
        error: "mock_provider_incompatible_with_paid",
        version: ASSOCIATION_EVENTS_HOTEL_RUNNER_V1,
      };
    }
    const {
      runEventsFullIntegrationZeroPaid,
      writeEventsIntegrationDeliverablesPack,
    } = await import("./events-full-integration-zero-paid-orchestrator-v1.js");
    const packDir =
      cfg.reportDir ||
      path.join(
        ROOT,
        "reports/gdi-multisegment-fulle2e-v1/events-full-integration-zero-paid",
      );
    const result = await runEventsFullIntegrationZeroPaid({
      hotelId: cfg.hotelId,
      reportDir: packDir,
    });
    writeEventsIntegrationDeliverablesPack(packDir, result);
    return {
      ok: Object.values(result.gates).every((g) => g.status !== "FAIL"),
      version: ASSOCIATION_EVENTS_HOTEL_RUNNER_V1,
      mockProviderFullE2e: true,
      packDir,
      summary: result.summary,
      gates: result.gates,
      stopAtPaidCheckpoint: true,
    };
  }

  // Delegate FullE2E + paid to continuous multi-wave campaign (avoid single shallow batch).
  if (cfg.continuousCampaign === true && cfg.continuousCampaignWave !== true) {
    const { runAssociationEventsContinuousCampaign } = await import(
      "./association-events-continuous-campaign-v1.js"
    );
    return runAssociationEventsContinuousCampaign(rawConfig);
  }

  const reportDir =
    cfg.reportDir ||
    path.join(
      ROOT,
      "reports/gdi-association-events-runs",
      cfg.runId,
    );
  fs.mkdirSync(reportDir, { recursive: true });
  cfg.reportDir = reportDir;

  const startedAt = new Date().toISOString();
  const ctx = await resolveAssociationEventsHotelContext(cfg);
  writeJson(path.join(reportDir, "01-hotel-profile.json"), {
    ...ctx.hotel,
    fitProfile: ctx.fitProfile,
    profileResolved: Boolean(ctx.profile),
  });
  writeJson(path.join(reportDir, "02-applicability.json"), {
    segment: ASSOCIATION_EVENTS_LAUNCHER_SEGMENT,
    applicable: true,
    discoveryVersion: ASSOCIATION_EVENTS_DISCOVERY_V1,
    note: "Event-to-Demand pathway maps into associations_government / medical_academic / related axes — no duplicate segment invented.",
  });
  // Campaign wave may inject expanded / unused strategies
  if (Array.isArray(cfg.campaignExpandedStrategies) && cfg.campaignExpandedStrategies.length) {
    const byIntent = new Map(ctx.strategies.map((s) => [s.intent, s]));
    for (const s of cfg.campaignExpandedStrategies) {
      if (s?.intent) byIntent.set(s.intent, s);
    }
    ctx.strategies = [...byIntent.values()].sort(
      (a, b) => (b.discoveryWeight || 0) - (a.discoveryWeight || 0),
    );
  }
  if (Array.isArray(cfg.campaignStrategyIntents) && cfg.campaignStrategyIntents.length) {
    const allow = new Set(cfg.campaignStrategyIntents);
    const filtered = ctx.strategies.filter((s) => allow.has(s.intent));
    if (filtered.length) ctx.strategies = filtered;
  }
  writeJson(path.join(reportDir, "03-strategies.json"), ctx.strategies);

  // Zero-paid Exa Agent Events discovery payload preview (no network).
  // Live Agent discovery remains gated behind authorizePaidResearch + explicit opt-in.
  try {
    const {
      buildEventsDiscoveryAgentPayloadPreview,
    } = await import("./event-discovery-contract-v1.js");
    const {
      redactExaEventsPayloadForReport,
    } = await import("./exa-agent/exa-agent-events-contracts-v1.js");
    const agentPreview = buildEventsDiscoveryAgentPayloadPreview(ctx.hotel, {
      limit: Math.min(cfg.maxCandidates || 12, 15),
      horizonMonths: 18,
      exclusions: [],
    });
    writeJson(path.join(reportDir, "03b-exa-events-agent-discovery-payload-preview.json"), {
      version: "gdi-exa-events-discovery-payload-preview-v1",
      liveCallAuthorized: false,
      note: "Payload constructed for audit/integration. Live Exa Agent requires separate paid authorization.",
      contractId: agentPreview.contractId,
      strategyId: agentPreview.strategyId,
      redacted: redactExaEventsPayloadForReport(agentPreview),
      useExaAgentDiscovery: cfg.useExaAgentDiscovery === true,
    });
  } catch (err) {
    writeJson(path.join(reportDir, "03b-exa-events-agent-discovery-payload-preview.json"), {
      ok: false,
      error: String(err?.message || err),
    });
  }

  const paidGuard = assertPaidContactProvidersDisabled({
    authorizeSurfe: false,
    authorizePdl: false,
  });
  const pricing = preflightLiveDeepResearchPricing();
  const ledgerPath = initLedger(
    reportDir,
    cfg.authorizePaidResearch ? cfg.budgetCeilingUsd : 0,
    cfg.runId,
    { hotelId: cfg.hotelId },
  );

  const authStates = assessHotelAuthorizationStates(cfg.hotelId, {
    env: process.env,
    run: {
      authorizePaidResearch: cfg.authorizePaidResearch,
      budgetCeilingUsd: cfg.budgetCeilingUsd,
      authorizePublication: cfg.authorizePublication,
      probeLiveShare: cfg.probeLiveShare,
    },
    profileResolution: { ok: true },
  });
  const universalPreviewReadiness = assessGdiHotelPreviewReadiness(cfg.hotelId, {
    env: process.env,
    run: {
      authorizePaidResearch: cfg.authorizePaidResearch,
      budgetCeilingUsd: cfg.budgetCeilingUsd,
      authorizePublication: cfg.authorizePublication,
      probeLiveShare: cfg.probeLiveShare,
    },
    profileResolution: { ok: true },
    railwayEnv: null,
  });

  const preflight = {
    hotelResolved: Boolean(ctx.hotel.displayName),
    // Campaign waves may intentionally run a smaller filtered strategy set (≥1).
    strategiesPresent:
      ctx.strategies.length >= 3 ||
      (cfg.continuousCampaignWave === true && ctx.strategies.length >= 1),
    pricingOk: pricing.ok === true,
    paidContactGuardOk: paidGuard.ok !== false,
    authorizationStates: authStates.states,
    universalPreviewReadiness,
    ledgerPath,
    budgetCeilingUsd: cfg.authorizePaidResearch ? cfg.budgetCeilingUsd : 0,
    ledgerSpentUsd: ledgerSpent(ledgerPath),
  };
  writeJson(path.join(reportDir, "07-preflight.json"), preflight);

  const blockers = [];
  if (!preflight.hotelResolved) blockers.push("hotel_unresolved");
  if (!preflight.strategiesPresent) blockers.push("insufficient_strategies");
  if (!preflight.paidContactGuardOk) blockers.push("paid_contact_guard_failed");
  if (cfg.mode === "full-e2e" && cfg.authorizePaidResearch && !preflight.pricingOk) {
    blockers.push("live_pricing_preflight_failed");
  }

  const isFullE2e = cfg.mode === "full-e2e";
  const allowPaid =
    isFullE2e && cfg.authorizePaidResearch === true && cfg.budgetCeilingUsd > 0;

  // V26 clean-room: do not feed historical inventory into discovery/qualification.
  const existing = isCleanRoomActive()
    ? []
    : ((await loadOpportunitiesCanonical(cfg.hotelId, { bypassCache: true }))
        .opportunities || []);
  const existingAssocLike = existing.filter(
    (o) =>
      /associations_government|medical_academic|exhibitor_sponsor|event_to_demand|association_events/i.test(
        `${o.researchSegment || ""} ${o.discoverySource || ""} ${o.id || ""}`,
      ) ||
      /congress|conference|congreso|association|asociación/i.test(
        `${o.eventName || ""} ${o.title || ""} ${o.organizationName || ""}`,
      ),
  );
  writeJson(path.join(reportDir, "04-existing-event-inventory.json"), {
    totalOpportunities: existing.length,
    associationEventLikeCount: existingAssocLike.length,
    sample: existingAssocLike.slice(0, 20).map((o) => ({
      id: o.id,
      organizationName: o.organizationName,
      eventName: o.eventName || o.title || null,
      researchSegment: o.researchSegment || null,
      discoverySource: o.discoverySource || null,
    })),
  });

  const discovery = {
    attempted: false,
    searches: [],
    hits: 0,
    rawHits: [],
    skippedReason: null,
  };

  // Paid discovery runs even when SavedDiscoveryPath is set (campaign deepen + discover).
  // Preview / unpaid FullE2E may still use saved evidence alone.
  const skipPaidDiscovery =
    cfg.continuousCampaignWave === true &&
    cfg.campaignAllowDiscoveryWithSavedEvidence !== true &&
    Boolean(cfg.savedDiscoveryPath);
  if (isFullE2e && allowPaid && blockers.length === 0 && !skipPaidDiscovery) {
    discovery.attempted = true;
    // Campaign waves use explicit discovery budget; legacy single-pass keeps 40% phase cap.
    const discoveryCap =
      cfg.campaignDiscoveryBudgetUsd != null
        ? Math.min(cfg.campaignDiscoveryBudgetUsd, cfg.budgetCeilingUsd)
        : Math.min(cfg.budgetCeilingUsd * 0.55, cfg.budgetCeilingUsd);
    for (const strategy of ctx.strategies) {
      if (ledgerSpent(ledgerPath) >= discoveryCap - 0.01) {
        discovery.skippedReason = "discovery_phase_cap";
        break;
      }
      if (ledgerSpent(ledgerPath) >= cfg.budgetCeilingUsd - 0.01) {
        discovery.skippedReason = "aggregate_ceiling";
        break;
      }
      const before = ledgerSpent(ledgerPath);
      const exa = await maybeExaSearch(strategy.researchQuestion, ledgerPath);
      const costUsd = Number((ledgerSpent(ledgerPath) - before).toFixed(6));
      const hits = exa.hits || [];
      discovery.hits += hits.length;
      discovery.searches.push({
        intent: strategy.intent,
        domain: strategy.domain,
        researchQuestion: strategy.researchQuestion,
        status: exa.status,
        costUsd,
        hitCount: hits.length,
      });
      for (const hit of hits) {
        discovery.rawHits.push({
          ...hit,
          intent: strategy.intent,
          domain: strategy.domain,
          researchSegment: strategy.researchSegment,
          researchQuestion: strategy.researchQuestion,
        });
      }
    }
  } else if (!allowPaid) {
    discovery.skippedReason = isFullE2e
      ? "paid_discovery_not_authorized"
      : "preview_or_preflight";
  } else if (skipPaidDiscovery) {
    discovery.skippedReason = "campaign_deepen_only_wave";
  }

  // Saved-evidence replay / research continuation seed (no paid discovery required)
  const savedSeedOpportunities = [];
  if (cfg.savedDiscoveryPath && fs.existsSync(cfg.savedDiscoveryPath)) {
    try {
      const saved = JSON.parse(fs.readFileSync(cfg.savedDiscoveryPath, "utf8"));
      const rows = Array.isArray(saved)
        ? saved
        : saved.opportunities ||
          saved.qualified ||
          saved.candidates ||
          saved.matured ||
          [];
      for (const row of rows) {
        if (!row || typeof row !== "object") continue;
        if (row.hotelId && row.hotelId !== cfg.hotelId) continue;
        savedSeedOpportunities.push({
          ...row,
          id: row.id || row.opportunityId,
          discoverySource: row.discoverySource || "association_events_saved_evidence_replay",
        });
      }
      discovery.savedEvidencePath = cfg.savedDiscoveryPath;
      discovery.savedEvidenceCount = savedSeedOpportunities.length;
      if (!discovery.attempted && savedSeedOpportunities.length) {
        discovery.skippedReason = "using_saved_evidence_replay";
      } else if (discovery.attempted && savedSeedOpportunities.length) {
        discovery.note = "saved_evidence_plus_paid_discovery";
      }
    } catch (err) {
      discovery.savedEvidenceError = err.message;
    }
  }
  writeJson(path.join(reportDir, "08-discovery.json"), discovery);

  // Build candidates from hits
  const candidates = [];
  const rejected = [];
  const seenKeys = new Set();
  for (const seed of savedSeedOpportunities) {
    if (!seed.id || seenKeys.has(seed.id)) continue;
    seenKeys.add(seed.id);
    candidates.push(
      scrubAssociationEventsContactFields(applyAssociationEventsChronology(seed)),
    );
  }
  for (const hit of discovery.rawHits) {
    const strategy =
      ctx.strategies.find((s) => s.intent === hit.intent) || {
        intent: hit.intent,
        domain: hit.domain,
        researchSegment: hit.researchSegment,
      };
    const opp = buildOpportunityFromHit({
      hit,
      strategy,
      hotel: ctx.hotel,
      existing: [...existing, ...candidates],
      fitProfile: ctx.fitProfile,
    });
    if (!opp) {
      rejected.push({ reason: "unparseable_hit", title: hit.title || null, url: hit.url || null });
      continue;
    }
    if (seenKeys.has(opp.id)) {
      rejected.push({ reason: "intra_run_duplicate", id: opp.id, organizationName: opp.organizationName });
      continue;
    }
    seenKeys.add(opp.id);
    // Enrich people/routes from excerpt
    const blob = `${hit.title || ""} ${hit.text || hit.excerpt || ""}`;
    const people = extractNamedPeopleFromText(blob);
    const routes = extractContactRoutes(blob, hit.url || hit.sourceUrl);
    if (people.length) {
      opp.namedStakeholders = people;
      opp.contactName = people[0].name;
      opp.contactRole = people[0].title;
    }
    Object.assign(opp, routes);
    candidates.push(opp);
  }

  writeJson(path.join(reportDir, "05-reconciliation.json"), {
    rawHits: discovery.hits,
    candidates: candidates.length,
    rejected: rejected.length,
    duplicatesFlagged: candidates.filter((c) => c.crossSegmentDuplicate).length,
    note: "Cross-segment duplicates flagged against existing hotel opportunities; not deleted.",
  });

  // ORS + investigation on top candidates
  const matured = [];
  const orsResults = [];
  let researcher = null;
  if (allowPaid && blockers.length === 0 && ledgerSpent(ledgerPath) < cfg.budgetCeilingUsd - 0.05) {
    const forbidFirecrawl =
      cfg.forbidFirecrawl === true ||
      (Array.isArray(cfg.providersForbidden) &&
        cfg.providersForbidden.map(String).includes("firecrawl"));
    researcher = createLiveDeepResearchResearcher({ ledgerPath, forbidFirecrawl });
  }

  const workset = candidates
    .filter((c) => !c.crossSegmentDuplicate)
    .slice(0, cfg.maxCandidates);

  for (const opp of workset) {
    let maturedOpp = normalizeAssociationEventsOpportunityIdentity({ ...opp });
    let ors = { attempted: false, ok: false, offline: true };
    if (researcher && ledgerSpent(ledgerPath) < cfg.budgetCeilingUsd - 0.05) {
      try {
        const remainingBudgetUsd = Math.max(
          0,
          Number(cfg.budgetCeilingUsd || 0) - ledgerSpent(ledgerPath),
        );
        const deep = await runDeepResearchOnExistingGdiOpportunity({
          opportunity: {
            ...maturedOpp,
            hotelId: cfg.hotelId,
            hotelName: ctx.hotel.displayName,
          },
          researcher,
          options: {
            authorizeSurfe: false,
            authorizePdl: false,
            // run-049: paidBlocked previously defaulted true and starved Exa/Firecrawl
            // even when the Association FullE2E campaign authorized paid research.
            paidBlocked: !(allowPaid && remainingBudgetUsd > 0.05),
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
        ors = {
          attempted: true,
          ok: deep?.ok !== false,
          offline: false,
          live: true,
          stopReason:
            deep?.after?.orsStopReason ||
            deep?.stopReason ||
            deep?.stoppingReason ||
            null,
        };
        // Deep-research result shape is `{ after: { opportunity } }` (same as Scaling handoff).
        // Prior `deep?.opportunity` silently dropped ORS maturation into Association matured rows.
        const deepOpp =
          deep?.after?.opportunity || deep?.opportunity || deep?.matured || null;
        if (deepOpp) {
          maturedOpp = { ...maturedOpp, ...deepOpp, id: maturedOpp.id };
        }

        // run-049: when ORS leaves RELATED_ENTITY_FOLLOWUPS_PENDING with an executable
        // next step, drain through the same researcher — do not treat worker exit as done.
        let pendingGuard = 0;
        while (
          pendingGuard < 2 &&
          maturedOpp.stopReason === STOP_REASONS.RELATED_ENTITY_FOLLOWUPS_PENDING &&
          maturedOpp.nextResearchStep?.executable === true &&
          researcher?.execute &&
          ledgerSpent(ledgerPath) < cfg.budgetCeilingUsd - 0.05
        ) {
          pendingGuard += 1;
          const drain = await drainRelatedEntityFollowUpsThroughOrs({
            state: maturedOpp,
            researcher,
            targetHotel: ctx.hotel.displayName,
            remainingBudgetUsd: Math.max(
              0,
              Number(cfg.budgetCeilingUsd || 0) - ledgerSpent(ledgerPath),
            ),
            maxFollowUps: 4,
            paidBlocked: !(allowPaid && remainingBudgetUsd > 0.05),
          });
          maturedOpp = {
            ...maturedOpp,
            ...drain.state,
            id: maturedOpp.id,
            notes: [
              ...(maturedOpp.notes || []),
              ...(drain.state?.notes || []),
              `association_runner_related_entity_drain_wave:${pendingGuard}`,
            ],
          };
          ors.stopReason = maturedOpp.stopReason || ors.stopReason;
          ors.relatedEntityDrainWaves = pendingGuard;
          ors.relatedEntityLeadsFollowed = drain.relatedEntityLeadsFollowed || 0;
        }

        // Pull people from ORS dossier text when present — never promote unrelated agency/PR names.
        const dossierBlob = JSON.stringify(
          deep?.dossierResult?.dossier || deep?.dossier || deepOpp || {},
        ).slice(0, 8000);
        const morePeople = extractNamedPeopleFromText(dossierBlob);
        if (morePeople.length) {
          const orgName = maturedOpp.organizationName || maturedOpp.organizer || "";
          const assessedPeople = morePeople.map((p) => ({
            ...p,
            _buyerAssessment: assessAssociationEventsBuyerCandidate(p, {
              eventBlob: `${maturedOpp.eventName || ""} ${dossierBlob.slice(0, 400)}`,
              organizerName: orgName,
            }),
          }));
          maturedOpp.namedStakeholders = [
            ...(maturedOpp.namedStakeholders || []),
            ...assessedPeople.filter((p) => p._buyerAssessment?.ok !== false || looksLikePersonName(p.name)),
          ];
          const eligible = assessedPeople.find((p) => p._buyerAssessment?.ok);
          if (eligible && !looksLikePersonName(maturedOpp.contactName)) {
            maturedOpp.contactName = eligible.name;
            maturedOpp.contactRole = eligible.title;
          } else if (
            looksLikePersonName(maturedOpp.contactName) &&
            !assessAssociationEventsBuyerCandidate(
              { name: maturedOpp.contactName, title: maturedOpp.contactRole },
              { organizerName: orgName, eventBlob: maturedOpp.eventName || "" },
            ).ok
          ) {
            maturedOpp._rejectedContactName = maturedOpp.contactName;
            maturedOpp._rejectedContactRole = maturedOpp.contactRole;
            maturedOpp.contactName = null;
            maturedOpp.contactRole = null;
            maturedOpp.notes = [
              ...(maturedOpp.notes || []),
              "association_runner_rejected_unrelated_named_contact",
            ];
          }
        }
        const moreRoutes = extractContactRoutes(dossierBlob, maturedOpp.sourceUrl);
        if (moreRoutes.contactEmail && !maturedOpp.contactEmail) {
          maturedOpp.contactEmail = moreRoutes.contactEmail;
        }
        if (moreRoutes.contactPhone && !maturedOpp.contactPhone) {
          maturedOpp.contactPhone = moreRoutes.contactPhone;
        } else if (moreRoutes._rejectedContactPhone) {
          maturedOpp._rejectedContactPhone =
            maturedOpp._rejectedContactPhone || moreRoutes._rejectedContactPhone;
        }
      } catch (err) {
        ors = { attempted: true, ok: false, offline: false, live: true, error: err.message };
      }
    }
    maturedOpp = scrubAssociationEventsContactFields(maturedOpp);
    maturedOpp = normalizeAssociationEventsOpportunityIdentity(maturedOpp);
    maturedOpp = attachSecondaryNamedContact(maturedOpp, {
      candidates: maturedOpp.namedStakeholders || [],
    });
    orsResults.push({
      id: maturedOpp.id,
      organizationName: maturedOpp.organizationName,
      eventName: maturedOpp.eventName,
      ...ors,
      relatedEntityFollowUpDispositions:
        maturedOpp.relatedEntityFollowUpDispositions || null,
      nextResearchStep: maturedOpp.nextResearchStep || null,
    });
    matured.push(maturedOpp);
  }

  writeJson(path.join(reportDir, "09-ors-and-buyer-intelligence.json"), {
    orsResults,
    maturedCount: matured.length,
  });

  // Scaling investigation loop — free always; paid Exa only when authorized
  let investigation = { attempted: false, maturedRows: [], results: [] };
  if (isFullE2e && matured.length) {
    try {
      const paidSearch =
        allowPaid && ledgerSpent(ledgerPath) < cfg.budgetCeilingUsd - 0.08
          ? async (query) => {
              if (ledgerSpent(ledgerPath) >= cfg.budgetCeilingUsd - 0.02) {
                return { status: "budget_exhausted", hits: [], costUsd: 0 };
              }
              return maybeExaSearch(query, ledgerPath);
            }
          : null;
      investigation = await runInvestigationLoopOnMaturedRows({
        maturedRows: matured.map((o) => ({
          opportunityPayload: o,
          opportunity: o,
          organizationName: o.organizationName,
          countsAsGenuinelyNetNew: !o.crossSegmentDuplicate,
        })),
        hotel: ctx.hotel,
        allowFreeFetch: true,
        maxIterations: 4,
        maxPageFetches: 6,
        paidSearch,
      });
      investigation.attempted = true;
      if (Array.isArray(investigation?.maturedRows)) {
        for (let i = 0; i < matured.length; i++) {
          const updated =
            investigation.maturedRows[i]?.opportunityPayload ||
            investigation.maturedRows[i]?.opportunity;
          if (updated) matured[i] = { ...matured[i], ...updated, id: matured[i].id };
        }
      }
    } catch (err) {
      investigation = { attempted: true, error: err.message, maturedRows: [], results: [] };
    }
  }
  writeJson(path.join(reportDir, "16-intelligent-investigation-loop.json"), investigation);

  // Association-specific buyer recovery (paid Exa when authorized; skipped otherwise)
  let buyerRecovery = { attempted: false, version: ASSOCIATION_EVENTS_BUYER_RECOVERY_V1 };
  if (allowPaid && matured.length && ledgerSpent(ledgerPath) < cfg.budgetCeilingUsd - 0.05) {
    try {
      const paidSearch = async (query) => {
        if (ledgerSpent(ledgerPath) >= cfg.budgetCeilingUsd - 0.02) {
          return { status: "budget_exhausted", hits: [], costUsd: 0 };
        }
        return maybeExaSearch(query, ledgerPath);
      };
      buyerRecovery = await runAssociationEventsBuyerRecovery({
        opportunities: matured.map((o) => ({
          ...o,
          organizationName: cleanAssociationOrganizationName(o.organizationName || o.organizer || ""),
        })),
        hotel: ctx.hotel,
        paidSearch,
        maxOpportunities: Math.min(matured.length, cfg.maxCandidates),
        maxQueriesPerOpp: cfg.buyerRecoveryMaxQueriesPerOpp || 3,
      });
      buyerRecovery.attempted = true;
      for (let i = 0; i < matured.length; i++) {
        const row = buyerRecovery.results?.find((r) => r.id === matured[i].id);
        if (row?.opportunity) {
          matured[i] = {
            ...matured[i],
            ...row.opportunity,
            id: matured[i].id,
          };
        }
      }
    } catch (err) {
      buyerRecovery = {
        attempted: true,
        version: ASSOCIATION_EVENTS_BUYER_RECOVERY_V1,
        error: err.message,
      };
    }
  }
  writeJson(path.join(reportDir, "16b-association-buyer-recovery.json"), buyerRecovery);

  // Association long-form investigation — ALWAYS before qualification (free public)
  let longForm = {
    attempted: false,
    version: ASSOCIATION_EVENTS_LONG_FORM_INVESTIGATION_V1,
  };
  if (isFullE2e && matured.length) {
    try {
      const paidSearch =
        allowPaid && ledgerSpent(ledgerPath) < cfg.budgetCeilingUsd - 0.05
          ? async (query) => {
              if (ledgerSpent(ledgerPath) >= cfg.budgetCeilingUsd - 0.02) {
                return { status: "budget_exhausted", hits: [], costUsd: 0 };
              }
              return maybeExaSearch(query, ledgerPath);
            }
          : null;
      longForm = await runAssociationEventsLongFormOnMaturedRows({
        maturedRows: matured.map((o) => ({
          opportunityPayload: scrubAssociationEventsContactFields(
            applyAssociationEventsChronology(o),
          ),
          opportunity: o,
        })),
        hotel: ctx.hotel,
        allowFreeFetch: true,
        maxIterations: 5,
        maxPageFetches: 8,
        paidSearch,
        prioritize: true,
      });
      longForm.attempted = true;
      if (Array.isArray(longForm.maturedRows)) {
        for (let i = 0; i < matured.length; i++) {
          const match =
            longForm.maturedRows.find((r) => r.opportunity?.id === matured[i].id) ||
            longForm.maturedRows[i];
          const updated = match?.opportunityPayload || match?.opportunity;
          if (updated) matured[i] = { ...matured[i], ...updated, id: matured[i].id };
        }
      }
    } catch (err) {
      longForm = {
        attempted: true,
        version: ASSOCIATION_EVENTS_LONG_FORM_INVESTIGATION_V1,
        error: err.message,
      };
    }
  }
  writeJson(path.join(reportDir, "16c-association-long-form-investigation.json"), longForm);

  // Gold briefs + qualification (after long-form investigation)
  const qualified = [];
  const continuation = [];
  const briefs = [];
  for (let i = 0; i < matured.length; i++) {
    matured[i] = scrubAssociationEventsContactFields(
      applyAssociationEventsChronology({
        ...matured[i],
        organizationName:
          cleanAssociationOrganizationName(
            matured[i].organizationName || matured[i].organizer || "",
          ) || matured[i].organizationName,
      }),
    );
    // Evidence ledger: block rejected identities / restore protected claims (LARIM etc.)
    matured[i] = applyEvidenceLedgerToOpportunity(cfg.hotelId, matured[i]);
    let opp = applyAssociationEventsGoldBrief(matured[i], { hotel: ctx.hotel });
    const baseElig = evaluateProspectPublicationEligibility(opp, { hotelId: cfg.hotelId });
    const elig = applyAssociationEventsGateToEligibility(baseElig, opp, ctx.hotel);
    const qual = elig.associationEventsQualification;
    opp = {
      ...opp,
      associationEventsQualification: qual,
      hotelFacingEligible: qual.qualified === true,
      customerVisible: false,
      prospectPublicationBasis: qual.qualified ? "REVIEWED_PACKAGE_V1_STAGED" : null,
    };
    // Phase 2B shadow — does not change qualified/continuation membership.
    const shadowWrap = attachGdi2ShadowEvaluation(
      {
        opportunity: opp,
        hotelFacingEligible: qual.qualified === true,
        researchContinuation: qual.researchContinuation === true,
        associationEventsQualification: qual,
        eligibility: elig,
      },
      {
        hotel: ctx.hotel,
        segmentFamily: "associations_events",
        existingSalesReady: qual.qualified === true,
        existingResearchContinuation: qual.researchContinuation === true,
        existingEligibility: elig,
        authorizePublication: false,
      },
    );
    opp = {
      ...opp,
      gdi2ShadowEvaluationV1: shadowWrap.gdi2ShadowEvaluationV1,
      gdi2BuyerRecoveryPlanV1:
        qual.qualified === true
          ? null
          : planBuyerRecovery(opp, {
              hotel: ctx.hotel,
              attempts: opp.buyerResearchAttempts || [],
              paidResearchAuthorized: false,
            }),
    };
    const score = scoreAssociationEventsGoldBrief(opp);
    briefs.push({
      id: opp.id,
      organizationName: opp.organizationName,
      eventName: opp.eventName,
      qualificationStatus: qual.status,
      goldScore: score,
      chronologyStatus: opp.associationEventsChronology?.status || null,
      researchGapClass: opp.associationEventsResearchGapClass || null,
      brief: opp.associationEventsGoldBrief,
      gdi2ShadowDisagreement: opp.gdi2ShadowEvaluationV1?.disagreement?.code || null,
    });
    matured[i] = opp;
    if (qual.qualified) qualified.push(opp);
    else if (qual.researchContinuation) continuation.push(opp);
  }

  writeJson(path.join(reportDir, "11-gold-standard-briefs.json"), briefs);
  writeJson(path.join(reportDir, "06-publication-readiness.json"), {
    qualifiedCount: qualified.length,
    continuationCount: continuation.length,
    holdCount: matured.length - qualified.length - continuation.length,
    qualificationVersion: ASSOCIATION_EVENTS_QUALIFICATION_V1,
    goldBriefVersion: ASSOCIATION_EVENTS_GOLD_BRIEF_V1,
    qualified: qualified.map((o) => ({
      id: o.id,
      organizationName: o.organizationName,
      eventName: o.eventName,
      contactName: o.contactName,
      contactEmail: o.contactEmail || null,
    })),
    continuation: continuation.map((o) => ({
      id: o.id,
      organizationName: o.organizationName,
      eventName: o.eventName,
      reasons: o.associationEventsQualification?.reasons || [],
    })),
  });

  // Publication stage — staging only (no production writes unless explicitly applied)
  let publicationStage = null;
  if (isFullE2e && blockers.length === 0) {
    try {
      publicationStage = await runScalingFullE2ePublicationStage({
        hotelId: cfg.hotelId,
        hotel: ctx.hotel,
        maturedRows: matured.map((o) => ({
          opportunityPayload: o,
          organizationName: o.organizationName,
          publicationAuditLabel: o.associationEventsQualification?.status,
          countsAsGenuinelyNetNew: !o.crossSegmentDuplicate,
        })),
        authorizePublication: cfg.authorizePublication === true,
        applyProductionWrites: cfg.applyProductionWrites === true,
        probeLiveShare: cfg.probeLiveShare === true,
        reportDir,
        // run-052 Issue 8: never stamp Association rows as scaling_companies.
        segmentFamily: "associations_events",
        preserveResearchSegment: true,
        authorizedBy: "gdi-association-events-hotel-runner-v1",
      });
    } catch (err) {
      publicationStage = {
        status: "publication_stage_error",
        error: err.message,
        note: "Staging/publication helper raised — no production write assumed.",
      };
    }
  } else {
    publicationStage = {
      status: isFullE2e ? "blocked" : "PREVIEW_SKIPPED",
      authorizePublication: cfg.authorizePublication,
      applyProductionWrites: false,
    };
  }
  writeJson(path.join(reportDir, "17-publication-stage.json"), publicationStage);

  try {
    reconcileLiveLedgerReservations(ledgerPath);
  } catch {
    /* non-fatal */
  }
  const spentUsd = ledgerSpent(ledgerPath);
  const spending = {
    spentUsd,
    budgetCeilingUsd: cfg.budgetCeilingUsd,
    remainingUsd: Math.max(0, Number((cfg.budgetCeilingUsd - spentUsd).toFixed(4))),
    authorizePaidResearch: cfg.authorizePaidResearch,
    ledgerPath,
    providersUsed: spentUsd > 0 ? ["exa"] : [],
    forbiddenNotUsed: [
      "surfe",
      "pdl",
      "parallel",
      ...(cfg.forbidFirecrawl === true ||
      (Array.isArray(cfg.providersForbidden) &&
        cfg.providersForbidden.map(String).includes("firecrawl"))
        ? ["firecrawl"]
        : []),
    ],
  };
  writeJson(path.join(reportDir, "10-spending.json"), spending);

  const summary = {
    ok: blockers.length === 0,
    executed: isFullE2e,
    fullE2eComplete: false,
    fullE2eCompleteReason: cfg.authorizePublication
      ? "publication_authorized_but_production_writes_not_applied_in_this_task"
      : "publication_not_authorized",
    version: ASSOCIATION_EVENTS_HOTEL_RUNNER_V1,
    discoveryVersion: ASSOCIATION_EVENTS_DISCOVERY_V1,
    qualificationVersion: ASSOCIATION_EVENTS_QUALIFICATION_V1,
    goldBriefVersion: ASSOCIATION_EVENTS_GOLD_BRIEF_V1,
    stages: ASSOCIATION_EVENTS_FULL_E2E_STAGES,
    startedAt,
    finishedAt: new Date().toISOString(),
    config: {
      hotelId: cfg.hotelId,
      mode: cfg.mode,
      budgetCeilingUsd: cfg.budgetCeilingUsd,
      authorizePaidResearch: cfg.authorizePaidResearch,
      authorizePublication: cfg.authorizePublication,
      applyProductionWrites: cfg.applyProductionWrites,
      maxCandidates: cfg.maxCandidates,
    },
    hotel: ctx.hotel,
    fitProfile: ctx.fitProfile,
    blockers,
    funnel: {
      rawDiscoveryHits: discovery.hits,
      candidates: candidates.length,
      matured: matured.length,
      qualified: qualified.length,
      researchContinuation: continuation.length,
      crossSegmentDuplicates: candidates.filter((c) => c.crossSegmentDuplicate).length,
      rejected: rejected.length,
    },
    discovery,
    ors: { count: orsResults.filter((r) => r.attempted).length, results: orsResults },
    investigationLoop: {
      attempted: investigation.attempted === true,
      error: investigation.error || null,
    },
    spending,
    publication: publicationStage,
    productionWrite: { applied: false, reason: "not_authorized_in_founder_directive" },
    reportDir,
  };

  writeJson(path.join(reportDir, "SUMMARY.json"), summary);
  writeJson(path.join(reportDir, "18-full-e2e-acceptance.json"), {
    associationEventsPowershellExecutable: true,
    researchExecuted: discovery.attempted === true || matured.length > 0,
    buyerResearchAttempted: orsResults.some((r) => r.attempted) || investigation.attempted,
    goldBriefsGenerated: briefs.length,
    qualifiedCount: qualified.length,
    withinBudget: spentUsd <= cfg.budgetCeilingUsd + 1e-9,
    productionWrites: false,
  });
  fs.writeFileSync(
    path.join(reportDir, "RUN_REPORT.md"),
    `# Association Events run ${cfg.runId}\n\nHotel: ${ctx.hotel.displayName} (${cfg.hotelId})\nMode: ${cfg.mode}\nSpent: $${spentUsd} / $${cfg.budgetCeilingUsd}\nQualified: ${qualified.length}\nContinuation: ${continuation.length}\nMatured: ${matured.length}\nRaw hits: ${discovery.hits}\n`,
    "utf8",
  );

  return summary;
}
