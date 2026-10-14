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
  markParallelJobCompleted,
  createParallelJobKey,
} from "../parallel/job-store-v1.js";
import { PARALLEL_PRICE_ESTIMATES_USD } from "../parallel/budget-ledger-v1.js";

export const PARALLEL_DISCOVERY_ADAPTER_VERSION = "gdi-parallel-discovery-adapter-v1.1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

export const DEFAULT_LAB_ROOT = path.resolve(ROOT, "../gdi-parallel-lab");

export const DISCOVERY_MODE = Object.freeze({
  REPLAY: "replay",
  DRY_RUN_PLAN: "dry_run_plan",
  LIVE: "live",
});

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
      paidCalls: 0,
      freshDiscoveryClaimed: false,
      flags: getGdiParallelFlagState(env),
      plan: buildBoundedLiveDiscoveryPlan(hotelId, opts),
      note: "Pass --run-discovery to execute fixture replay (default) or bounded live plan",
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

  if (mode === DISCOVERY_MODE.LIVE || authorizePaid) {
    const jobKey = createParallelJobKey(hotelId, "bounded-live-plan");
    const resume = beginOrResumeParallelJob({
      jobKey,
      hotelId,
      label: "bounded_live_discovery_plan_only",
      env,
    });
    const plan = buildBoundedLiveDiscoveryPlan(hotelId, opts);
    return {
      version: PARALLEL_DISCOVERY_ADAPTER_VERSION,
      status: "incomplete_coverage",
      reason: "live_paid_discovery_plan_only_no_api_calls",
      paidCalls: 0,
      freshDiscoveryClaimed: false,
      job: resume,
      plan,
      note:
        "Bounded live-run plan + job resume guard recorded. No Parallel API calls. Use replay for saved-response verify.",
      replayHint: "Omit --authorize-paid-discovery to force frozen fixture replay",
    };
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
