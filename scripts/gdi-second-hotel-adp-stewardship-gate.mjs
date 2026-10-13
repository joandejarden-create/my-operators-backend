/**
 * Second-hotel replication V1 — ADP stewardship gate + GDI quality compare (read-mostly).
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandPortfolioPeerSet } from "../lib/ai-demand-positioning/brand-portfolio/brand-portfolio-peer-set-v1.js";
import { loadPropertyProfile, loadAllPeriods } from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { selectLatestCertifiedOfficialPeriod } from "../lib/ai-demand-positioning/period-eligibility-v1.js";
import { loadPublishedManifest } from "../lib/ai-demand-positioning/published-snapshot.js";
import { getAdpPersistencePolicy } from "../lib/ai-demand-positioning/adp-persistence-policy.js";
import { describeAdpJevShadowIntegrationPoints } from "../lib/group-demand-intelligence/research-coverage/seed-jev-shadow.js";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { summarizeContactCoverage } from "../lib/group-demand-intelligence/contact-tiers-v1-2.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/group-demand-intelligence/second-hotel-replication-v1");
const RTS = "adp_renaissance_times_square";
const BETH = "recLuxvwwxID7U2B8";
const REN = "recG66DQJKP2c0UNh";

function classifyPeer(p, declared) {
  const inDeclared = (declared || []).some(
    (d) => String(d).toLowerCase() === String(p.peerHotel || "").toLowerCase()
  );
  return {
    candidate: p.peerHotel,
    reason: p.inclusionReason,
    marketRelevance: p.marketRelevance,
    productRelevance: p.brand,
    locationRelevance: p.submarket,
    brandServiceRelevance: p.affiliationMatch,
    includeExclude: "INCLUDE_RECOMMENDED",
    evidence: p.evidenceSource,
    inDeclaredCompSet: inDeclared,
  };
}

async function hotelGdiSnapshot(hotelId) {
  const bag = await loadOpportunitiesCanonical(hotelId);
  const all = bag?.opportunities || [];
  const visible = filterCustomerFacingOpportunities(all);
  const contacts = summarizeContactCoverage(visible);
  const actionable = visible.filter((o) => o.priority !== "DISQUALIFIED" && o.priority !== "WATCHLIST");
  let bethesdaActionBleed = 0;
  for (const o of all) {
    if (/\bbethesda marriott\b/i.test(String(o.recommendedAction || ""))) bethesdaActionBleed += 1;
  }
  const withOfficial = all.filter((o) =>
    /official|marriott\.com|\.org|\.gov/i.test(
      `${o.primarySourceUrl || ""} ${(o.sourceUrls || []).join(" ")}`
    )
  ).length;
  return {
    hotelId,
    total: all.length,
    visible: visible.length,
    actionable: actionable.length,
    contact: contacts,
    bethesdaActionBleed,
    officialSourceApproxPct: all.length ? Math.round((100 * withOfficial) / all.length) : 0,
  };
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const profile = loadPropertyProfile(RTS);
  const peers = getBrandPortfolioPeerSet(RTS);
  const scenarios = buildScenarioUniverse(profile);
  const scenarioList = scenarios?.scenarios || scenarios || [];
  const periods = loadAllPeriods(RTS);
  const latest = selectLatestCertifiedOfficialPeriod(periods);
  const manifest = loadPublishedManifest(RTS);
  const policy = getAdpPersistencePolicy();

  const peerTable = (peers?.included || []).map((p) =>
    classifyPeer(p, profile?.declaredCompSet)
  );

  const stewardship = {
    hotelProfile: profile ? "AVAILABLE" : "BLOCKING",
    censusLink: "AVAILABLE",
    brand: profile?.brand ? "AVAILABLE" : "BLOCKING",
    website: profile?.website ? "AVAILABLE" : "BLOCKING",
    rooms: profile?.rooms ? "AVAILABLE" : "BLOCKING",
    meeting: profile?.meetingSpace ? "AVAILABLE" : "BLOCKING",
    market: profile?.market ? "AVAILABLE" : "BLOCKING",
    competitiveContext: profile?.declaredCompSet?.length ? "AVAILABLE" : "STEWARDSHIP_REQUIRED",
    peerPack: peers?.adequacy?.status === "ADEQUATE" ? "AVAILABLE" : "STEWARDSHIP_REQUIRED",
    scenarioUniverse: scenarioList.length ? "AVAILABLE" : "BLOCKING",
    certifiedPeriod: latest ? "AVAILABLE" : "STEWARDSHIP_REQUIRED",
    publishedManifest: manifest?.latestPeriodId ? "AVAILABLE" : "DERIVABLE",
  };

  const blockers = Object.entries(stewardship)
    .filter(([, v]) => v === "BLOCKING")
    .map(([k]) => k);

  const gdiRen = await hotelGdiSnapshot(REN);
  const gdiBeth = await hotelGdiSnapshot(BETH);

  const report = {
    generatedAt: new Date().toISOString(),
    adpStewardship: {
      propertyId: RTS,
      stewardship,
      blockers,
      gate: blockers.length ? "FAIL" : "PASS",
      peers: {
        count: peerTable.length,
        adequacy: peers?.adequacy,
        table: peerTable,
        manualReviewRequired: true,
        note: "Peers already encoded in brand-portfolio-peer-set-v1; commercial approval boundary retained.",
      },
      scenarios: {
        count: scenarioList.length,
        sample: scenarioList.slice(0, 8).map((s) => ({
          scenarioId: s.scenarioId,
          intent: s.intent,
          demandTerritory: s.query,
          hotelRelevance: s.source,
          expectedUtility: s.frame,
          sourceBasis: s.source,
        })),
      },
      latestCertifiedPeriodId: latest?.periodId || null,
      publishedLatestPeriodId: manifest?.latestPeriodId || null,
      persistencePolicy: policy,
      adpJev: describeAdpJevShadowIntegrationPoints(),
    },
    gdiCompare: {
      renaissance: gdiRen,
      bethesda: gdiBeth,
      qualityParityNote:
        "Compare quality/contacts/bleed — not raw volume. Renaissance first weekly TRUE=0 matches Bethesda V1.2 weekly TRUE=0 pattern (CQ bar held).",
      differences: [
        {
          class: "EXPECTED_MARKET_DIFFERENCE",
          detail: "Renaissance urban Times Square mix vs Bethesda medical/federal DMV mix",
        },
        {
          class: gdiRen.bethesdaActionBleed === 0 ? "QUALITY_GAP" : "GENERALIZATION_DEFECT",
          detail:
            gdiRen.bethesdaActionBleed === 0
              ? "Renaissance opportunity inventory smaller / newer than Bethesda mature bag"
              : "Bethesda recommendedAction bleed on Renaissance",
        },
      ],
    },
  };

  fs.writeFileSync(path.join(OUT, "PHASE3_ADP_STEWARDSHIP.json"), JSON.stringify(report, null, 2));
  console.log(
    JSON.stringify(
      {
        ok: true,
        adpGate: report.adpStewardship.gate,
        peers: peerTable.length,
        scenarios: scenarioList.length,
        gdiRen: { total: gdiRen.total, actionable: gdiRen.actionable, bleed: gdiRen.bethesdaActionBleed },
        gdiBeth: { total: gdiBeth.total, actionable: gdiBeth.actionable },
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
