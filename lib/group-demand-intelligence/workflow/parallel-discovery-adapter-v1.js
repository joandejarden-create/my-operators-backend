/**
 * Optional Parallel discovery adapter for GDI workflow.
 *
 * Default: dry-run / replay from in-repo fixtures (fixtures/gdi-parallel-san-diego).
 * Optional developer lab root via GDI_PARALLEL_LAB_ROOT — production must not
 * depend on the lab folder.
 * Never writes customer-visible opportunities from provider output.
 * Live paid calls require --run-discovery + --authorize-paid-discovery +
 * GDI_PARALLEL_ENABLED=1, hotel allowlist, and an explicit spend cap.
 *
 * Webhound is retired — this adapter does not invoke it.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { WEBHOUND_PROVIDER_STATUS } from "./webhound-retired-v1.js";
import { assessIndependentHotelFit } from "./independent-hotel-fit-v1.js";
import { normalizeOrgIdentityKey } from "./reuse-prior-intelligence-v1.js";
import {
  isGdiParallelEnabled,
  isGdiParallelHotelAllowed,
  isGdiParallelKillSwitchOn,
  getGdiParallelFlagState,
  GDI_PARALLEL_SAN_DIEGO_HOTEL_ID,
} from "../parallel/feature-flag.js";
import { loadSanDiegoFixtureCards } from "../parallel/fixture-pack-v1.js";
import { attachClaimEvidence } from "../parallel/claim-evidence-v1.js";
import { dedupeAccountTripCandidates } from "../parallel/identity-dedupe-v1.js";
import {
  beginOrResumeParallelJob,
  markParallelJobSubmitted,
  markParallelJobCompleted,
  markParallelJobUncertain,
  createParallelJobKey,
} from "../parallel/job-store-v1.js";
import {
  PARALLEL_PRICE_ESTIMATES_USD,
  reserveParallelBudget,
  commitParallelBudgetReservation,
  releaseParallelBudgetReservation,
} from "../parallel/budget-ledger-v1.js";
import { createParallelClient } from "../../hotel-intelligence/research/providers/parallel/client.js";
import { mapNativeCandidateRow } from "../native-blind-discovery.js";
import {
  quarantineWrongMarketCandidates,
  GEOGRAPHIC_RELEVANCE_GATE_VERSION,
} from "./geographic-relevance-gate-v1.js";

export const PARALLEL_DISCOVERY_ADAPTER_VERSION = "gdi-parallel-discovery-adapter-v1.3";
/** Job segment for corrected identity-lock live validation — must not resume contaminated bounded-live-v1. */
export const PARALLEL_IDENTITY_LOCK_JOB_SEGMENT = "bounded-live-v1-idlock";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

export const DEFAULT_LAB_ROOT = path.resolve(ROOT, "../gdi-parallel-lab");

export const DISCOVERY_MODE = Object.freeze({
  REPLAY: "replay",
  DRY_RUN_PLAN: "dry_run_plan",
  LIVE_MOCK: "live_mock",
  LIVE: "live",
});

/**
 * Mock Parallel transport for live path testing (no paid calls).
 * Simulates submit → poll → result mapping with fixture payloads.
 */
export function createMockParallelTransport(opts = {}) {
  const calls = [];
  return {
    kind: "mock",
    calls,
    async submitTask(payload) {
      const runId = `mock_run_${Date.now().toString(36)}`;
      calls.push({ op: "submit", runId, payload });
      return { ok: true, runId, status: "queued", paid: false };
    },
    async pollTask(runId) {
      calls.push({ op: "poll", runId });
      return { ok: true, runId, status: "completed", paid: false };
    },
    async fetchResults(runId) {
      calls.push({ op: "fetchResults", runId });
      const pack = loadSanDiegoCardsAndReview(opts);
      return {
        ok: true,
        runId,
        paid: false,
        cards: (pack.cards || []).slice(0, opts.maxCards || 3),
        source: "mock_transport_mapped_from_fixtures",
      };
    },
  };
}

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

export function resolveLabRoot(override) {
  if (override) return path.resolve(override);
  if (process.env.GDI_PARALLEL_LAB_ROOT) {
    return path.resolve(process.env.GDI_PARALLEL_LAB_ROOT);
  }
  return DEFAULT_LAB_ROOT;
}

function labExists(labRoot) {
  return fs.existsSync(path.join(labRoot, "package.json"));
}

function loadJson(p) {
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function listSanDiegoCardsFromLab(labRoot) {
  const cardsDir = path.join(labRoot, "data/sandiego-bayside/cards");
  if (!fs.existsSync(cardsDir)) return [];
  return fs
    .readdirSync(cardsDir)
    .filter((f) => /^sd_card_\d+\.json$/.test(f))
    .sort()
    .map((f) => {
      const card = loadJson(path.join(cardsDir, f));
      return { ...card, _file: f };
    });
}

/** Prefer in-repo fixtures; fall back to lab for developers. */
function loadSanDiegoCardsAndReview(opts = {}) {
  const fixtures = loadSanDiegoFixtureCards(opts);
  if (fixtures.available && fixtures.cards.length) {
    return {
      source: "in_repo_fixtures",
      cards: fixtures.cards,
      reviewPackage: fixtures.reviewPackage,
      root: fixtures.root,
    };
  }
  const labRoot = resolveLabRoot(opts.labRoot);
  if (labExists(labRoot)) {
    const reviewPkg =
      loadJson(
        path.join(labRoot, "data/sandiego-bayside/reports/latest-review-package.json")
      ) ||
      loadJson(
        path.join(
          labRoot,
          "data/sandiego-bayside/reports/review-package-deduped-2026-10-14T19-13-18-848Z.json"
        )
      );
    return {
      source: "optional_lab_root",
      cards: listSanDiegoCardsFromLab(labRoot),
      reviewPackage: reviewPkg,
      root: labRoot,
    };
  }
  return { source: "missing", cards: [], reviewPackage: null, root: fixtures.root };
}

/**
 * Map a frozen lab card → internal discovery candidate (not customer-facing).
 */
export function mapLabCardToDiscoveryCandidate(card = {}, hotelId) {
  const org = text(card.account);
  const start = card.timing?.start || null;
  const c = card.contact || {};
  let mapped = {
    candidateId: card.cardId || card.dedupeKey,
    hotelId,
    organizationName: org,
    organizationIdentityKey: normalizeOrgIdentityKey(org),
    title: text(card.timing?.raw) || org,
    eventStartDate: start,
    eventEndDate: card.timing?.end || null,
    travelingCohort: card.travelingCohort || null,
    dedupeKey: card.dedupeKey || null,
    demandSignal: card.demandSignal || null,
    hotelOpportunityThesis: card.whyWyndhamFits || null,
    sourcingStatus: card.sourcingStatus || "UNKNOWN",
    venueSourcingStatus: "UNKNOWN",
    recommendedAction: card.recommendedFirstAction || null,
    evidenceLinks: Array.isArray(card.evidenceLinks) ? card.evidenceLinks : [],
    officialSource: (card.evidenceLinks || [])[0] || null,
    retrievedTextAvailable: false,
    retrievalDate: card.completedAt || null,
    /** Peak rooms stay distinct from total room nights / hotel capacity. */
    peakRooms: null,
    totalRoomNights: null,
    hotelTotalGuestrooms: null,
    provenance: {
      provider: "parallel",
      mode: "fixture_or_lab_frozen_replay",
      labCardId: card.cardId || null,
      noveltyVsBaseline: card.noveltyVsBaseline || "UNKNOWN",
      researchQuality: card.researchQuality || null,
      note: "Replayed frozen result — not a fresh discovery success claim",
    },
    primaryContact: {
      name: c.name || null,
      email: c.email || c.functionalEmail || null,
      phone: c.phone || c.switchboard || null,
      role: c.role || null,
      identityKind: c.identityKind || null,
      source: c.source || null,
      verificationStatus: c.verificationStatus || "UNKNOWN",
      emailVerification: {
        status: c.emailQuality || c.verificationStatus || "UNKNOWN",
        source: c.source || null,
        method: c.source || null,
      },
      phoneVerification: {
        status: c.phoneQuality || "UNKNOWN",
        source: c.source || null,
        method: c.source || null,
      },
    },
    materialLimitations: card.materialLimitations || [],
    peakRoomEstimate: null,
    customerVisible: false,
    opportunityQualification: null,
    publicationBlocked: true,
    publicationBlockReason:
      "Provider/fixture output cannot write directly to customer-visible opportunities",
  };
  mapped = attachClaimEvidence(mapped, card.claimBuckets || {}, {
    evidenceLinks: card.evidenceLinks,
    retrievedAt: card.completedAt,
  });
  return mapped;
}

/**
 * Build a bounded live-run plan without submitting Parallel tasks.
 */
export function buildBoundedLiveDiscoveryPlan(hotelId, opts = {}) {
  const maxUsd = Number(opts.maxUsd ?? 2);
  const maxTasks = Number(opts.maxTasks ?? 8);
  const maxPages = Number(opts.maxPages ?? 40);
  const processor = opts.processor || "base";
  const estPerTask =
    PARALLEL_PRICE_ESTIMATES_USD[processor] ?? PARALLEL_PRICE_ESTIMATES_USD.base;
  const flags = getGdiParallelFlagState(opts.env || process.env);
  return {
    version: PARALLEL_DISCOVERY_ADAPTER_VERSION,
    mode: DISCOVERY_MODE.DRY_RUN_PLAN,
    hotelId,
    provider: "parallel",
    webhound: WEBHOUND_PROVIDER_STATUS,
    flags,
    hotelAllowed: isGdiParallelHotelAllowed(hotelId, opts.env || process.env),
    pricingNote: PARALLEL_PRICE_ESTIMATES_USD.docsNote,
    pricingAsOf: PARALLEL_PRICE_ESTIMATES_USD.asOf,
    limits: {
      maxUsd,
      maxTasks,
      maxPages,
      processor,
      estimatedUsdIfFullyUsed: Math.min(maxUsd, maxTasks * estPerTask),
      estimatedDistinctFromActualBilled: true,
    },
    steps: [
      "Reuse prior intelligence ledger (no paid calls)",
      "Read authoritative opportunity baseline; novelty UNKNOWN if unreadable",
      "Identify incomplete_coverage gaps only",
      "Reserve budget atomically; persist job + provider run_id before poll",
      "Never resubmit known run_id (AASA duplicate lesson)",
      "Map candidates → account/trip dedupe → independent hotel fit → eligibility",
      "No auto-publish; kill switch blocks new submissions",
    ],
    cachePolicy: "resume_known_run_id_no_duplicate_paid_work",
    customerWritePolicy: "forbidden_from_provider_response",
    note: "Plan only — no live Parallel calls in this artifact",
  };
}

/**
 * Replay frozen San Diego package from in-repo fixtures (lab optional).
 */
export function replayFrozenParallelDiscovery(hotelId, opts = {}) {
  const id = text(hotelId);
  const pack = loadSanDiegoCardsAndReview(opts);
  const out = {
    version: PARALLEL_DISCOVERY_ADAPTER_VERSION,
    mode: DISCOVERY_MODE.REPLAY,
    hotelId: id,
    provider: "parallel",
    fixtureSource: pack.source,
    fixtureRoot: pack.root,
    labRoot: resolveLabRoot(opts.labRoot),
    flags: getGdiParallelFlagState(opts.env || process.env),
    paidCalls: 0,
    freshDiscoveryClaimed: false,
    customerVisibleWrites: 0,
    incompleteCoverage: null,
    candidates: [],
    reviewPackage: null,
    fitAssessments: [],
    identityDedupe: null,
    plan: null,
  };

  if (pack.source === "missing" || !pack.cards.length) {
    out.incompleteCoverage = {
      reason: "parallel_fixture_pack_missing",
      fixtureRoot: pack.root,
      note: "Expected fixtures/gdi-parallel-san-diego/cards; optional lab root is developer-only",
    };
    out.plan = buildBoundedLiveDiscoveryPlan(id, opts);
    return out;
  }

  const cards = pack.cards;
  const reviewPkg = pack.reviewPackage;
  const targetHotel = id || GDI_PARALLEL_SAN_DIEGO_HOTEL_ID;
  const isSanDiego = targetHotel === GDI_PARALLEL_SAN_DIEGO_HOTEL_ID;

  if (!isSanDiego) {
    out.note =
      "Frozen San Diego package used only to prove adapter wiring; candidates are not auto-fit for other hotels.";
    out.plan = buildBoundedLiveDiscoveryPlan(id, opts);
    out.adapterProof = {
      sanDiegoCardCount: cards.length,
      reviewPackageAccepted: reviewPkg?.counts?.acceptedProspects ?? null,
      transferableMethod:
        "configured segments → named accounts/occasions → deepen → durable evidence → hotel-specific assessment",
      nonTransferable: [
        "Bethesda-only sports assumptions",
        "Claiming fresh discovery from replay",
        "Auto-publishing fixture cards",
      ],
    };
    out.candidates = cards.slice(0, 3).map((c) => ({
      ...mapLabCardToDiscoveryCandidate(c, targetHotel),
      hotelId: targetHotel,
      fitProbe: assessIndependentHotelFit(
        mapLabCardToDiscoveryCandidate(c, targetHotel),
        targetHotel
      ),
      publicationBlocked: true,
      note: "Cross-hotel replay sample — not qualified for this hotel",
    }));
    out.reviewPackage = {
      source: `${pack.source}_adapter_proof_only`,
      acceptedProspects: reviewPkg?.counts?.acceptedProspects ?? cards.length,
      baselineNovelty: reviewPkg?.baselineNovelty || "UNKNOWN",
    };
    return out;
  }

  const mapped = cards.map((c) => {
    const m = mapLabCardToDiscoveryCandidate(c, targetHotel);
    const fit = assessIndependentHotelFit(m, targetHotel);
    return {
      ...m,
      independentHotelFit: fit,
      gdiClassifierResult: "NOT_AUTO_QUALIFIED",
    };
  });
  const deduped = dedupeAccountTripCandidates(mapped);
  out.candidates = deduped.unique;
  out.identityDedupe = {
    duplicateCount: deduped.duplicateCount,
    sharedHousingPathways: deduped.sharedHousingPathways,
  };
  out.fitAssessments = out.candidates.map((c) => ({
    candidateId: c.candidateId,
    organizationName: c.organizationName,
    outcome: c.independentHotelFit?.outcome || null,
  }));
  out.reviewPackage = {
    source: pack.source,
    acceptedProspects: reviewPkg?.counts?.acceptedProspects ?? cards.length,
    baselineNovelty: reviewPkg?.baselineNovelty || "UNKNOWN",
    parallelEstimatedSpendUsd:
      reviewPkg?.parallel?.estimatedSpentThisRunUsd ?? null,
    note: "Replay of frozen results — do not claim fresh discovery success; no confirmed open demand",
  };
  out.plan = buildBoundedLiveDiscoveryPlan(id, {
    ...opts,
    maxUsd: opts.maxUsd ?? 2,
    maxTasks: opts.maxTasks ?? 6,
  });
  return out;
}

/** Strip other-market hotel names from gap text so prompts cannot prime wrong markets. */
function sanitizeGapLineForHotel(line, hotelCfg = {}) {
  let s = text(line);
  if (!s) return "";
  const hotelName = text(hotelCfg.displayName || hotelCfg.name).toLowerCase();
  const isSanDiego =
    /san diego/i.test(hotelName) ||
    text(hotelCfg.city).toLowerCase() === "san diego" ||
    text(hotelCfg.hotelId) === "recnPB3Q02NO8Ma1o";
  if (isSanDiego) {
    // Avoid priming Bethesda/DMV from reused research gaps or prior-hotel copy.
    if (
      /\bbethesda\b|\bmaryland\b|\bnih\b|\bdmv\b|\bmayflower\b|\bpooks\s*hill\b/i.test(
        s
      )
    ) {
      return "";
    }
  }
  return s;
}

function textPromptGaps(opts = {}) {
  const gaps = Array.isArray(opts.researchGaps) ? opts.researchGaps : [];
  const peGaps = Array.isArray(opts.privateEventGaps) ? opts.privateEventGaps : [];
  const feedbackHints = Array.isArray(opts.feedbackQueryHints)
    ? opts.feedbackQueryHints
    : [];
  const hotelCfg = opts.hotelConfig || {};
  const lines = [];
  for (const g of gaps.slice(0, 8)) {
    const raw =
      typeof g === "string" ? g : g.label || g.query || JSON.stringify(g);
    const cleaned = sanitizeGapLineForHotel(raw, hotelCfg);
    if (cleaned) lines.push(`- ${cleaned}`);
  }
  for (const g of peGaps.slice(0, 4)) {
    const raw =
      typeof g === "string" ? g : g.label || g.query || JSON.stringify(g);
    const cleaned = sanitizeGapLineForHotel(raw, hotelCfg);
    if (cleaned) lines.push(`- Private-event gap: ${cleaned}`);
  }
  for (const h of feedbackHints.slice(0, 4)) {
    const cleaned = sanitizeGapLineForHotel(h, hotelCfg);
    if (cleaned) lines.push(`- Feedback-driven: ${cleaned}`);
  }
  if (!lines.length) {
    const territory =
      hotelCfg?.demandTerritory?.label ||
      hotelCfg?.city ||
      "the hotel's configured demand territory";
    lines.push(
      `- Identify credible 2026–2027 association / corporate / university / private-event lodging occasions in ${territory} with travel cohort and open procurement evidence`
    );
  }
  return lines.join("\n");
}

/**
 * Map Parallel JSON rows → candidates, then quarantine wrong-market BEFORE fit.
 */
function mapParallelLiveCandidates(list, hotelId, hotelConfig = null) {
  const cfg = hotelConfig || loadHotelConfigForParallel(hotelId);
  const mappedRows = [];
  const rows = Array.isArray(list) ? list : [];
  for (let i = 0; i < rows.length; i += 1) {
    const raw = rows[i] || {};
    const mapped =
      mapNativeCandidateRow(
        {
          ...raw,
          eventName: raw.eventName || raw.title || raw.opportunityName,
          organization: raw.organization || raw.organizationName,
          researchProvider: "parallel",
        },
        hotelId,
        `par_live_${i}`
      ) || null;
    const candidate = mapped
      ? {
          ...mapLabCardToDiscoveryCandidate(
            {
              cardId: mapped.id || `par_live_${i}`,
              account: mapped.organizationName || raw.organization,
              timing: {
                start: mapped.eventStartDate || raw.startDate,
                end: mapped.eventEndDate || raw.endDate,
                raw: mapped.title || raw.eventName,
              },
              demandSignal: mapped.demandSignalType || raw.opportunityType,
              whyWyndhamFits:
                mapped.hotelOpportunityThesis ||
                raw.whyRelevantToHotel ||
                raw.hotelThesis,
              sourcingStatus: raw.sourcingStatus || "UNCONFIRMED",
              recommendedAction: raw.recommendedAction || "Qualify lodging fit",
              evidenceLinks: [
                raw.officialSource,
                ...(Array.isArray(raw.supportingSources)
                  ? raw.supportingSources
                  : []),
              ].filter(Boolean),
              contact: raw.whoClues || raw.primaryContact || null,
              materialLimitations: raw.materialLimitations || null,
            },
            hotelId
          ),
          researchProvider: "parallel",
          discoverySource: "gdi_parallel_bounded_live_v1",
          independentHotelFit: null,
          location: raw.location || mapped.eventLocationSummary || null,
          venue: raw.venue || null,
          rawParallel: raw,
        }
      : {
          ...mapLabCardToDiscoveryCandidate(
            {
              cardId: `par_live_${i}`,
              account: raw.organization || raw.organizationName,
              timing: {
                start: raw.startDate,
                end: raw.endDate,
                raw: raw.eventName,
              },
              whyWyndhamFits: raw.whyRelevantToHotel,
              evidenceLinks: [raw.officialSource].filter(Boolean),
            },
            hotelId
          ),
          location: raw.location || null,
          venue: raw.venue || null,
          rawParallel: raw,
        };
    mappedRows.push(candidate);
  }

  const geo = quarantineWrongMarketCandidates(mappedRows, cfg || {}, {
    hotelId,
  });
  const out = [];
  for (const c of geo.accepted) {
    const fit = assessIndependentHotelFit(c, hotelId);
    out.push({
      ...c,
      independentHotelFit: fit,
      gdiClassifierResult: "NOT_AUTO_QUALIFIED",
      publicationBlocked: true,
    });
  }
  // Quarantined rows never receive hotel-fit scoring or card promotion.
  for (const c of geo.quarantined) {
    out.push({
      ...c,
      independentHotelFit: null,
      fitSkippedReason: "wrong_market_quarantined_before_fit",
      publicationBlocked: true,
    });
  }
  out._geographicGate = {
    version: GEOGRAPHIC_RELEVANCE_GATE_VERSION,
    acceptedCount: geo.acceptedCount,
    quarantinedCount: geo.quarantinedCount,
  };
  return out;
}

/**
 * Bounded live Parallel submit/poll/map with budget reserve + job resume.
 * Hard cap: opts.maxUsd (default $2). Does not auto-publish.
 */
export async function executeBoundedLiveParallelDiscovery(hotelId, opts = {}) {
  const id = text(hotelId);
  const env = opts.env || process.env;
  const maxUsd = Math.min(Number(opts.maxUsd ?? 2), 2);
  const maxTasks = Math.min(Number(opts.maxTasks ?? 2), 2);
  const processor = opts.processor || "base";
  const timeoutMs = Number(opts.timeoutMs || env.GDI_PARALLEL_TIMEOUT_MS || 180000);
  const hotelCfg = opts.hotelConfig || loadHotelConfigForParallel(id);
  const plan = buildBoundedLiveDiscoveryPlan(id, {
    ...opts,
    hotelConfig: hotelCfg,
    maxUsd,
    maxTasks,
    processor,
  });
  // Default remains bounded-live-v1 (resume known contaminated run — no resubmit).
  // Corrected identity-lock validation uses PARALLEL_IDENTITY_LOCK_JOB_SEGMENT.
  const jobSegment = text(opts.jobSegment) || "bounded-live-v1";
  const jobKey = createParallelJobKey(id, jobSegment);
  const resume = beginOrResumeParallelJob({
    jobKey,
    hotelId: id,
    label: `bounded_live_parallel_discovery:${jobSegment}`,
    env,
  });

  if (!resolveParallelKeySafe(env)) {
    return {
      version: PARALLEL_DISCOVERY_ADAPTER_VERSION,
      status: "incomplete_coverage",
      reason: "parallel_key_missing",
      mode: DISCOVERY_MODE.LIVE,
      executionKind: "blocked_missing_key",
      paidCalls: 0,
      freshDiscoveryClaimed: false,
      job: resume,
      plan,
      note: "PARALLEL_KEY / PARALLEL_API_KEY required for live Parallel submit",
    };
  }

  // Resume known run — poll only, never resubmit
  if (resume.shouldSubmit === false && resume.providerRunId) {
    try {
      const client = createParallelClient({ env });
      const result = await client.pollUntilComplete(resume.providerRunId, {
        timeoutMs,
      });
      const mapped = extractLiveCandidates(result, id, hotelCfg);
      const geoMeta = mapped._geographicGate || null;
      const candidates = mapped.filter ? mapped : mapped;
      // strip helper meta if present on array
      if (candidates._geographicGate) delete candidates._geographicGate;
      markParallelJobCompleted(
        jobKey,
        {
          candidateCount: candidates.length,
          acceptedInTerritory: geoMeta?.acceptedCount ?? null,
          quarantinedWrongMarket: geoMeta?.quarantinedCount ?? null,
          paidCalls: 1,
          mode: DISCOVERY_MODE.LIVE,
          providerRunId: resume.providerRunId,
          resumed: true,
          hotelIdentity: {
            hotelId: id,
            displayName: hotelCfg?.displayName || null,
            city: hotelCfg?.city || null,
            territory: hotelCfg?.demandTerritory?.label || null,
          },
        },
        env
      );
      return {
        version: PARALLEL_DISCOVERY_ADAPTER_VERSION,
        status: "completed_live_resume",
        mode: DISCOVERY_MODE.LIVE,
        executionKind: "live_resume_poll_existing_run",
        paidCalls: 1,
        freshDiscoveryClaimed: false,
        providerRunId: resume.providerRunId,
        job: { ...resume, completed: true },
        candidates,
        geographicGate: geoMeta,
        plan,
        note: "Resumed existing Parallel run_id — no duplicate submit; wrong-market rows quarantined before fit",
      };
    } catch (err) {
      return {
        version: PARALLEL_DISCOVERY_ADAPTER_VERSION,
        status: "incomplete_coverage",
        reason: err?.message || "resume_poll_failed",
        mode: DISCOVERY_MODE.LIVE,
        executionKind: "live_resume_failed",
        paidCalls: 0,
        freshDiscoveryClaimed: false,
        providerRunId: resume.providerRunId,
        job: resume,
        plan,
      };
    }
  }

  let reservation = null;
  const estPerTask =
    Math.max(
      PARALLEL_PRICE_ESTIMATES_USD[processor] ?? 0.5,
      0.5
    ); // conservative floor for live reserve
  const estimatedUsd = Math.min(maxUsd, maxTasks * estPerTask);

  try {
    reservation = reserveParallelBudget({
      label: "bounded_live_parallel_discovery",
      processor,
      estimatedUsd,
      jobKey,
      hotelId: id,
      maxUsd,
      env,
    });
  } catch (err) {
    return {
      version: PARALLEL_DISCOVERY_ADAPTER_VERSION,
      status: "incomplete_coverage",
      reason: err?.code || err?.message || "budget_reserve_failed",
      mode: DISCOVERY_MODE.LIVE,
      executionKind: "blocked_budget",
      paidCalls: 0,
      freshDiscoveryClaimed: false,
      job: resume,
      plan,
      note: "Budget reserve failed — no Parallel submit attempted",
    };
  }

  const gapBlock = textPromptGaps({ ...opts, hotelConfig: hotelCfg });
  const hotelName =
    hotelCfg?.displayName || hotelCfg?.name || "the target hotel";
  const city = hotelCfg?.city || hotelCfg?.capabilityProfile?.city || "";
  const territory =
    hotelCfg?.demandTerritory?.label ||
    hotelCfg?.demandTerritory?.includes?.[0] ||
    city ||
    "";
  const territoryIncludes = Array.isArray(hotelCfg?.demandTerritory?.includes)
    ? hotelCfg.demandTerritory.includes.slice(0, 4).join("; ")
    : "";
  const rooms = hotelCfg?.capabilityProfile?.totalGuestrooms || null;
  const meetingRooms =
    hotelCfg?.capabilityProfile?.meetingRoomCount ||
    hotelCfg?.capabilityProfile?.meetingRoomsIndoor ||
    null;
  const address =
    hotelCfg?.geo?.note ||
    hotelCfg?.capabilityProfile?.notes?.slice?.(0, 80) ||
    "";
  const softBrand = hotelCfg?.capabilityProfile?.softBrand || "";
  const classification = hotelCfg?.capabilityProfile?.classification || "";
  const taskPrompt = `You are researching group lodging demand for ONE hotel only.

TARGET HOTEL (identity lock — do not substitute another property or market):
- hotelId: ${id}
- name: ${hotelName}
- brand/soft-brand: ${softBrand || "n/a"}
- city/market: ${city || "see territory"}
- address/context: ${address || "n/a"}
- demand territory label: ${territory || "hotel-configured catchment only"}
- territory includes: ${territoryIncludes || "see hotel config"}
- property class: ${classification || "n/a"}
- guestrooms: ${rooms ?? "unknown"}
- meeting rooms: ${meetingRooms ?? "unknown"}

HARD RULES:
- Research ONLY occasions whose travel catchment is relevant to ${hotelName} in ${territory || city || "its configured territory"}.
- The hotelId above is the canonical Dealality hotel identity. Never invent or substitute a different hotel name (e.g. do not treat this hotelId as Bethesda Marriott or any other property).
- Do NOT return Bethesda, Maryland, DMV, NIH, Pooks Hill, Mayflower/DC, or other-market events for this San Diego / North Harbor Drive hotel.
- Only return occasions supported by first-party or high-authority sources with an explicit location in or relevant to ${territory || city}.
- Prefer dated 2026–2027 occasions with travel cohort and lodging/housing evidence.
- Do not invent contacts, emails, or confirmed sourcing.
- Distinguish venue-referral vs specific-event lodging vs ancillary hospitality vs background.
- If unsure of geographic relevance, omit the candidate.

RESEARCH GAPS TO FILL (bounded — must stay inside this hotel's territory):
${gapBlock}

Return JSON only:
{ "candidates": [ {
  "eventName", "organization", "startDate", "endDate", "location", "venue",
  "sourcingStatus", "attendance", "peakRoomEstimate", "roomDemandEvidence",
  "housingEvidence", "officialSource", "supportingSources",
  "whyRelevantToHotel", "whyNow", "whoClues", "opportunityType",
  "recommendedAction", "materialLimitations"
} ] }
Max ${Math.max(8, maxTasks * 6)} candidates. Quality over quantity.`;

  let runId = null;
  let costUsd = 0;
  try {
    const client = createParallelClient({ env });
    const created = await client.createTaskRun({
      processor,
      input: taskPrompt,
      // Parallel requires a real json_schema when task_spec is supplied —
      // omit task_spec and ask for JSON in the prompt (proven createTaskRun shape).
    });
    runId =
      created.structured?.run_id ||
      created.structured?.id ||
      created.structured?.run?.id ||
      null;
    if (!runId) {
      releaseParallelBudgetReservation(reservation.reservationId, {
        env,
        reason: "parallel_run_id_missing",
      });
      return {
        version: PARALLEL_DISCOVERY_ADAPTER_VERSION,
        status: "incomplete_coverage",
        reason: "parallel_run_id_missing",
        mode: DISCOVERY_MODE.LIVE,
        executionKind: "live_submit_failed",
        paidCalls: 0,
        freshDiscoveryClaimed: false,
        job: resume,
        plan,
        raw: created.structured || null,
      };
    }
    markParallelJobSubmitted(
      jobKey,
      {
        providerRunId: runId,
        reservationId: reservation.reservationId,
      },
      env
    );

    const result = await client.pollUntilComplete(runId, { timeoutMs });
    const structured = result.structured || {};
    costUsd = Number(
      structured.cost?.total ||
        structured.usage?.cost_usd ||
        structured.run?.cost ||
        estimatedUsd
    );
    const mappedAll = extractLiveCandidates(result, id, hotelCfg);
    const geoMeta = mappedAll._geographicGate || null;
    if (mappedAll._geographicGate) delete mappedAll._geographicGate;
    // Fit/dedupe only in-territory accepted rows; keep quarantined separately.
    const accepted = mappedAll.filter(
      (c) => c.quarantineStatus !== "WRONG_MARKET_OR_UNRESOLVED_GEO"
    );
    const quarantined = mappedAll.filter(
      (c) => c.quarantineStatus === "WRONG_MARKET_OR_UNRESOLVED_GEO"
    );
    const deduped = dedupeAccountTripCandidates(accepted);

    commitParallelBudgetReservation(reservation.reservationId, {
      env,
      providerRunId: runId,
    });
    markParallelJobCompleted(
      jobKey,
      {
        candidateCount: deduped.unique.length + quarantined.length,
        acceptedInTerritory: deduped.unique.length,
        quarantinedWrongMarket: quarantined.length,
        paidCalls: 1,
        mode: DISCOVERY_MODE.LIVE,
        providerRunId: runId,
        estimatedUsd,
        actualBilledUsd: costUsd || null,
        hotelIdentity: {
          hotelId: id,
          displayName: hotelCfg?.displayName || null,
          city: hotelCfg?.city || null,
          territory: hotelCfg?.demandTerritory?.label || null,
          jobSegment,
        },
      },
      env
    );

    return {
      version: PARALLEL_DISCOVERY_ADAPTER_VERSION,
      status: "completed_live",
      mode: DISCOVERY_MODE.LIVE,
      executionKind: "live_submit_poll_map",
      paidCalls: 1,
      freshDiscoveryClaimed: true,
      providerRunId: runId,
      estimatedUsd,
      actualBilledUsd: costUsd || null,
      budgetReservationId: reservation.reservationId,
      job: { ...resume, completed: true, providerRunId: runId },
      candidates: deduped.unique,
      quarantinedCandidates: quarantined,
      geographicGate: geoMeta,
      identityDedupe: {
        duplicateCount: deduped.duplicateCount,
        sharedHousingPathways: deduped.sharedHousingPathways,
      },
      plan,
      note:
        "Live Parallel submit/poll/map completed within $2 cap. Wrong-market quarantined before fit. Candidates are NOT auto-published.",
    };
  } catch (err) {
    if (runId) {
      markParallelJobUncertain(jobKey, err?.message || "live_submit_or_poll_error", env);
    } else if (reservation?.reservationId) {
      releaseParallelBudgetReservation(reservation.reservationId, {
        env,
        reason: err?.message || "live_failed_before_run_id",
      });
    }
    return {
      version: PARALLEL_DISCOVERY_ADAPTER_VERSION,
      status: "incomplete_coverage",
      reason: err?.code || err?.message || "live_parallel_failed",
      mode: DISCOVERY_MODE.LIVE,
      executionKind: "live_failed",
      paidCalls: runId ? 1 : 0,
      freshDiscoveryClaimed: false,
      providerRunId: runId,
      job: resume,
      plan,
      note: "Live Parallel attempt failed — check job store before retry; do not resubmit known run_id",
    };
  }
}

function resolveParallelKeySafe(env = process.env) {
  return Boolean(
    String(env.PARALLEL_KEY || env.PARALLEL_API_KEY || "").trim()
  );
}

function loadHotelConfigForParallel(hotelId) {
  try {
    const p = path.join(
      ROOT,
      "config/group-demand-intelligence/hotels",
      `${String(hotelId || "").trim()}.json`
    );
    if (!fs.existsSync(p)) return null;
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return null;
  }
}

function extractLiveCandidates(result, hotelId, hotelConfig = null) {
  const structured = result?.structured || result || {};
  let output = structured.output || structured.result || structured;

  // Parallel Task API often returns { type:"json", content:{ output: "<json string>" } }
  if (output?.content?.output != null) {
    output = output.content.output;
  } else if (output?.content != null && typeof output.content === "string") {
    output = output.content;
  }

  if (typeof output === "string") {
    try {
      output = JSON.parse(output);
    } catch {
      output = { raw: output };
    }
  }
  // Nested string again after first parse
  if (typeof output?.output === "string") {
    try {
      output = JSON.parse(output.output);
    } catch {
      /* keep */
    }
  }

  const list = Array.isArray(output?.candidates)
    ? output.candidates
    : Array.isArray(output)
      ? output
      : [];
  return mapParallelLiveCandidates(list, hotelId, hotelConfig);
}

/**
 * Entry used by workflow discover_gaps stage.
 */
export async function runParallelDiscoveryStage(hotelId, opts = {}) {
  const mode = opts.mode || DISCOVERY_MODE.REPLAY;
  const authorizePaid = opts.authorizePaidDiscovery === true;
  const runDiscovery = opts.runDiscovery === true;
  const env = opts.env || process.env;

  if (!runDiscovery) {
    return {
      version: PARALLEL_DISCOVERY_ADAPTER_VERSION,
      status: "incomplete_coverage",
      reason: "run_discovery_not_requested",
      mode: DISCOVERY_MODE.DRY_RUN_PLAN,
      executionKind: "plan_only",
      paidCalls: 0,
      freshDiscoveryClaimed: false,
      flags: getGdiParallelFlagState(env),
      plan: buildBoundedLiveDiscoveryPlan(hotelId, opts),
      note: "Pass --run-discovery --discovery-mode replay|live-mock|live to execute; without --run-discovery this is plan-only",
    };
  }

  // Property scoping: live path requires flag + allowlist; replay allowed for verify.
  if (
    (mode === DISCOVERY_MODE.LIVE || authorizePaid) &&
    (!isGdiParallelEnabled(env) ||
      isGdiParallelKillSwitchOn(env) ||
      !isGdiParallelHotelAllowed(hotelId, env))
  ) {
    return {
      version: PARALLEL_DISCOVERY_ADAPTER_VERSION,
      status: "incomplete_coverage",
      reason: "parallel_flag_or_hotel_scope_blocked",
      paidCalls: 0,
      freshDiscoveryClaimed: false,
      flags: getGdiParallelFlagState(env),
      plan: buildBoundedLiveDiscoveryPlan(hotelId, opts),
      note: "GDI_PARALLEL_ENABLED default OFF; San Diego allowlist required for live",
    };
  }

  // Live mock: exercise submit/poll/map without paid Parallel calls
  if (mode === DISCOVERY_MODE.LIVE_MOCK) {
    const transport = opts.transport || createMockParallelTransport(opts);
    const jobKey = createParallelJobKey(hotelId, "live-mock");
    const resume = beginOrResumeParallelJob({
      jobKey,
      hotelId,
      label: "live_mock_transport",
      env,
    });
    const submitted = await transport.submitTask({
      hotelId,
      mode: DISCOVERY_MODE.LIVE_MOCK,
      maxUsd: opts.maxUsd ?? 2,
    });
    const polled = await transport.pollTask(submitted.runId);
    const fetched = await transport.fetchResults(submitted.runId);
    const candidates = (fetched.cards || []).map((c) =>
      mapLabCardToDiscoveryCandidate(c, hotelId)
    );
    markParallelJobCompleted(
      jobKey,
      {
        candidateCount: candidates.length,
        paidCalls: 0,
        mode: DISCOVERY_MODE.LIVE_MOCK,
        runId: submitted.runId,
      },
      env
    );
    return {
      version: PARALLEL_DISCOVERY_ADAPTER_VERSION,
      status: "completed_live_mock",
      mode: DISCOVERY_MODE.LIVE_MOCK,
      executionKind: "live_mock_transport",
      paidCalls: 0,
      freshDiscoveryClaimed: false,
      job: { ...resume, completed: true, providerRunId: submitted.runId },
      transportCalls: transport.calls,
      submit: submitted,
      poll: polled,
      candidates,
      plan: buildBoundedLiveDiscoveryPlan(hotelId, opts),
      note:
        "Live mock executed submit/poll/result mapping with fixture-backed transport — not a paid Parallel call; not a fresh discovery success claim",
    };
  }

  if (mode === DISCOVERY_MODE.LIVE || authorizePaid) {
    return executeBoundedLiveParallelDiscovery(hotelId, opts);
  }

  const jobKey = createParallelJobKey(hotelId, "fixture-replay");
  const resume = beginOrResumeParallelJob({
    jobKey,
    hotelId,
    label: "san_diego_fixture_replay",
    env,
  });
  const replay = replayFrozenParallelDiscovery(hotelId, opts);
  markParallelJobCompleted(
    jobKey,
    {
      candidateCount: (replay.candidates || []).length,
      paidCalls: 0,
      mode: DISCOVERY_MODE.REPLAY,
    },
    env
  );
  return {
    ...replay,
    status: "completed_replay",
    mode: DISCOVERY_MODE.REPLAY,
    executionKind: "replay_frozen_fixtures",
    job: { ...resume, completed: true },
    note: "Frozen fixture replay complete — not a fresh discovery success claim",
  };
}

export function writeDiscoveryStageArtifacts(result, outDir) {
  fs.mkdirSync(outDir, { recursive: true });
  const main = path.join(outDir, "parallel-discovery-stage.json");
  fs.writeFileSync(main, JSON.stringify(result, null, 2) + "\n");
  if (result.plan) {
    fs.writeFileSync(
      path.join(outDir, "bounded-live-discovery-plan.json"),
      JSON.stringify(result.plan, null, 2) + "\n"
    );
  }
  return main;
}
