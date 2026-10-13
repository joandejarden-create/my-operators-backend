/**
 * W Rome cross-market resume — ADP stewardship gate + peer candidates + Jev shadow sample.
 * Does NOT invent certified peers or run ADP baseline without peer approval.
 *
 *   node scripts/gdi-cross-market-w-rome-adp-stewardship.mjs
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { loadPropertyProfile, loadAllPeriods } from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { getBrandPortfolioPeerSet } from "../lib/ai-demand-positioning/brand-portfolio/brand-portfolio-peer-set-v1.js";
import { selectLatestCertifiedOfficialPeriod } from "../lib/ai-demand-positioning/period-eligibility-v1.js";
import { loadPublishedManifest } from "../lib/ai-demand-positioning/published-snapshot.js";
import { getAdpPersistencePolicy } from "../lib/ai-demand-positioning/adp-persistence-policy.js";
import { describeAdpJevShadowIntegrationPoints } from "../lib/group-demand-intelligence/research-coverage/seed-jev-shadow.js";
import { runSeedJevShadowComparison } from "../lib/group-demand-intelligence/research-coverage/seed-jev-shadow.js";
import { listTargetsForHotel } from "../lib/group-demand-intelligence/research-coverage/airtable-stores.js";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import { summarizeContactCoverage } from "../lib/group-demand-intelligence/contact-tiers-v1-2.js";
import { decide } from "../lib/group-demand-intelligence/jev/jev-decision-service.js";
import { JEV_DECISION_TYPE } from "../lib/group-demand-intelligence/jev/jev-types.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/group-demand-intelligence/cross-market-replication-v1"
);
const ADP = "adp_w_rome";
const HOTEL = "rece0or38cxo3Fymb";
const BETH = "recLuxvwwxID7U2B8";
const REN = "recG66DQJKP2c0UNh";

/**
 * Peer candidates from official Marriott Rome Bonvoy evidence (DATA for stewardship).
 * Not written into certified peer-set until founder approval.
 */
const PEER_CANDIDATES = [
  {
    candidate: "Rome Marriott Hotel",
    reason: "L1 · open Bonvoy · Rome center",
    geoRelevance: "Rome / Centro",
    productRelevance: "Marriott Hotels · full-service",
    brandServiceRelevance: "Marriott Bonvoy",
    evidence: "marriott.com rommc",
    recommendation: "INCLUDE_RECOMMENDED",
  },
  {
    candidate: "The St. Regis Rome",
    reason: "L1 · open Bonvoy · luxury · Centro",
    geoRelevance: "Rome / Centro",
    productRelevance: "St. Regis · luxury",
    brandServiceRelevance: "Marriott Bonvoy",
    evidence: "marriott.com romxr",
    recommendation: "INCLUDE_RECOMMENDED",
  },
  {
    candidate: "Hotel Eden, a Dorchester Collection Hotel (Rome)",
    reason: "Lifestyle/luxury competitive set · Centro",
    geoRelevance: "Rome / Via Veneto corridor",
    productRelevance: "Independent luxury (Dorchester)",
    brandServiceRelevance: "Independent / collection (non-Bonvoy)",
    evidence: "dorchestercollection.com hotel-eden",
    recommendation: "INCLUDE_REVIEW — independent lifestyle competitor",
  },
  {
    candidate: "Hotel de Russie, a Rocco Forte Hotel",
    reason: "Lifestyle luxury · centro · Piazza del Popolo",
    geoRelevance: "Rome / Centro",
    productRelevance: "Rocco Forte lifestyle luxury",
    brandServiceRelevance: "Independent / soft collection",
    evidence: "roccofortehotels.com hotel-de-russie",
    recommendation: "INCLUDE_REVIEW",
  },
  {
    candidate: "JW Marriott Rome",
    reason: "L2 · Bonvoy · Rome metro overflow",
    geoRelevance: "Rome metro (EUR / peripheral)",
    productRelevance: "JW Marriott · upper luxury",
    brandServiceRelevance: "Marriott Bonvoy",
    evidence: "marriott.com romjw (verify open + distance)",
    recommendation: "EXCLUDE_PENDING_GEO — verify catchment before include",
  },
];

async function gdiSnap(hotelId) {
  const bag = await loadOpportunitiesCanonical(hotelId);
  const all = bag?.opportunities || [];
  const visible = filterCustomerFacingOpportunities(all);
  const contacts = summarizeContactCoverage(visible);
  return {
    hotelId,
    total: all.length,
    visible: visible.length,
    contact: contacts,
  };
}

async function sampleJevGdiShadow(targets, limit = 8) {
  const sample = (targets || []).slice(0, limit);
  const decisions = [];
  let jevBetter = 0;
  let currentBetter = 0;
  let same = 0;
  let unknown = 0;
  let highConfWrong = 0;

  for (const t of sample) {
    const existing = t.priority === "HIGH" ? "RESEARCH_NOW" : "DEFER";
    const result = await decide({
      decisionType: JEV_DECISION_TYPE.TARGET_RESEARCH_PRIORITY,
      choices: ["RESEARCH_NOW", "DEFER", "RETIRE_CANDIDATE"],
      existingDecision: existing,
      context: {
        hotelId: HOTEL,
        targetType: t.targetType,
        displayName: t.displayName || t.canonicalName,
        priority: t.priority,
        sourceUrl: t.primarySourceUrl,
      },
      shadow: true,
    });
    const jev = result.selected || result.effectiveDecision || null;
    const agreement = jev != null && String(jev) === String(existing);
    let outcome = "UNKNOWN";
    if (agreement) {
      same += 1;
      outcome = "SAME";
    } else if (jev == null || result.technicalFallback || result.policyFallback) {
      unknown += 1;
      outcome = "UNKNOWN";
    } else {
      unknown += 1;
      outcome = "UNKNOWN_NO_OUTCOME_LINK";
    }
    if (
      result.confidence != null &&
      result.confidence >= 0.75 &&
      !agreement &&
      outcome !== "SAME"
    ) {
      // Without outcome-link cannot mark wrong — count as high-conf disagreement only
      highConfWrong += 0;
    }
    decisions.push({
      targetId: t.targetId,
      currentRoute: existing,
      jevRoute: jev,
      jevConfidence: result.confidence ?? null,
      actualExecutedRoute: existing,
      actualResult: "SHADOW_NO_APPLY",
      outcome,
      technicalFallback: Boolean(result.technicalFallback),
      policyFallback: Boolean(result.policyFallback),
    });
  }

  return {
    calls: decisions.length,
    jevBetter,
    currentBetter,
    same,
    unknown,
    highConfWrong,
    apply: false,
    decisions,
  };
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const profile = loadPropertyProfile(ADP);
  const peers = getBrandPortfolioPeerSet(ADP);
  const scenarios = buildScenarioUniverse(profile || {});
  const periods = loadAllPeriods(ADP);
  const latest = selectLatestCertifiedOfficialPeriod(periods);
  const manifest = loadPublishedManifest(ADP);
  const policy = getAdpPersistencePolicy();
  const targets = await listTargetsForHotel(HOTEL);

  const stewardship = {
    hotelProfile: profile ? "AVAILABLE" : "BLOCKING",
    censusLink:
      profile?.censusRecordId === HOTEL ? "AVAILABLE" : "BLOCKING",
    brand: profile?.brand ? "AVAILABLE" : "BLOCKING",
    website: profile?.website ? "AVAILABLE" : "BLOCKING",
    rooms: profile?.rooms ? "AVAILABLE" : "BLOCKING",
    meeting: profile?.meetingSpace ? "AVAILABLE" : "STEWARDSHIP_REQUIRED",
    market: profile?.market ? "AVAILABLE" : "BLOCKING",
    competitiveContext: profile?.declaredCompSet?.length
      ? "AVAILABLE"
      : "STEWARDSHIP_REQUIRED",
    peerPack:
      peers?.adequacy?.status === "ADEQUATE"
        ? "AVAILABLE"
        : "STEWARDSHIP_REQUIRED",
    scenarioUniverse: scenarios.length ? "AVAILABLE" : "BLOCKING",
    certifiedPeriod: latest ? "AVAILABLE" : "MISSING",
    publishedManifest: manifest?.latestPeriodId ? "AVAILABLE" : "MISSING",
    entityRegistry: "DERIVABLE",
  };

  const blockers = Object.entries(stewardship)
    .filter(([, v]) => v === "BLOCKING")
    .map(([k]) => k);

  // Peer approval is required before baseline — STEWARDSHIP_REQUIRED on peerPack blocks apply.
  const peerApprovalPending = stewardship.peerPack === "STEWARDSHIP_REQUIRED";
  const gate =
    blockers.length || peerApprovalPending
      ? "ADP_STEWARDSHIP_BLOCKED"
      : "ADP_STEWARDSHIP_PASS";

  const jevGdi = await sampleJevGdiShadow(targets, 8);
  const adpJev = describeAdpJevShadowIntegrationPoints();
  const seedJev = runSeedJevShadowComparison(
    targets.slice(0, 5).map((t) => ({
      targetId: t.targetId,
      priority: t.priority,
      researchCadence: t.researchCadence || "WEEKLY",
      playbookHint: t.targetType,
    })),
    { shadow: true }
  );

  const gdi = {
    wRome: await gdiSnap(HOTEL),
    renaissance: await gdiSnap(REN),
    bethesda: await gdiSnap(BETH),
  };

  const report = {
    generatedAt: new Date().toISOString(),
    hotelId: HOTEL,
    propertyId: ADP,
    stewardship,
    blockers,
    peerApprovalPending,
    gate,
    peers: {
      certifiedCount: peers?.included?.length || 0,
      candidates: PEER_CANDIDATES,
      note: "Candidates are stewardship DATA — not certified peer-set rows.",
    },
    scenarios: {
      count: scenarios.length,
      sources: scenarios.reduce((a, s) => {
        a[s.source || "?"] = (a[s.source || "?"] || 0) + 1;
        return a;
      }, {}),
      sample: scenarios.slice(0, 6).map((s) => ({
        scenarioId: s.scenarioId,
        intent: s.intent,
        query: s.query,
        source: s.source,
      })),
    },
    certifiedPeriod: latest?.periodId || null,
    publishedLatestPeriodId: manifest?.latestPeriodId || null,
    persistencePolicy: policy,
    baselineAuthorized: false,
    baselineSkipReason:
      gate === "ADP_STEWARDSHIP_BLOCKED"
        ? "Peer pack founder approval required; certified period missing"
        : null,
    jevGdi,
    seedJevSummary: {
      calls: seedJev?.decisions?.length || 0,
      jevBetter: seedJev?.jevBetter || 0,
      same: seedJev?.same || 0,
      unknown: seedJev?.unknown || 0,
      shadow: true,
      apply: false,
    },
    adpJev,
    gdi,
  };

  const outPath = path.join(OUT, "PHASE7_ADP_STEWARDSHIP.json");
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log(
    JSON.stringify(
      {
        ok: true,
        gate,
        scenarios: scenarios.length,
        peerCandidates: PEER_CANDIDATES.length,
        certifiedPeers: peers?.included?.length || 0,
        jevCalls: jevGdi.calls,
        blockers,
        outPath,
      },
      null,
      2
    )
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
