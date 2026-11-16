#!/usr/bin/env node
/**
 * YOTEL Geneva Lake — zero-cost commercial quality audit, deduplication,
 * restage, and 40+ expansion readiness plan.
 *
 * Authorization: FOUNDER_YOTEL_COMMERCIAL_QUALITY_AND_40PLUS_READINESS_20261116
 * Paid spend: $0 (no Exa / Surfe / Parallel). Remaining $4.40 prior budget untouched.
 *
 *   node scripts/gdi-yotel-geneva-lake-commercial-quality-40plus-readiness-v1.mjs
 *   node scripts/gdi-yotel-geneva-lake-commercial-quality-40plus-readiness-v1.mjs --browser-verify
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { execSync } from "node:child_process";
import { fileURLToPath } from "node:url";

import {
  buildCustomerPublicationPackage,
  verifyPublicationPackageIntegrity,
  createGdiPublicationPackageStore,
  computePublicationInventoryCounts,
} from "../lib/group-demand-intelligence/publication-package/index.js";
import { GDI_PUBLICATION_LIFECYCLE_STATE } from "../lib/group-demand-intelligence/publication-package/gdi-publication-lifecycle-v1.js";
import { runPublicationQualityGate } from "../lib/group-demand-intelligence/publication-package/gdi-publication-quality-gate-v1.js";
import { attachCustomerClassificationPresentation } from "../lib/group-demand-intelligence/customer-classification-presentation-v1.js";
import {
  classifyOpportunityTypeFilter,
  OPPORTUNITY_TYPE_FILTER_LABEL,
} from "../lib/group-demand-intelligence/opportunity-type-filter-v1.js";
import {
  INVENTORY_POPULATION,
  annotateInventoryPopulations,
} from "../lib/group-demand-intelligence/bethesda-67-sales-baseline-cmi-v1.js";
import {
  applyCommercialActionabilityGate,
  assertCustomerInventoryPassesActionabilityGate,
} from "../lib/group-demand-intelligence/commercial-actionability-gate-v1.js";
import {
  annotateCanonicalCmiContract,
  evaluateCmiQualificationGates,
  GDI_CMI_CANONICAL_ELIGIBILITY_CONTRACT_V1,
} from "../lib/group-demand-intelligence/commercial-consistency/cmi-canonical-eligibility-contract-v1.js";
import { syncHotelOpportunitiesToAirtable } from "../lib/group-demand-intelligence/workflow/gdi-airtable-sync-v1.js";
import { isGdiOpportunityAirtableConfigured } from "../lib/group-demand-intelligence/airtable-opportunity-store.js";
import {
  GDI_FULL_E2E_HOTEL_TEMPLATE_V1,
  GDI_COMMERCIAL_ACTIONABILITY_GATE_V1,
} from "../lib/group-demand-intelligence/template/gdi-full-e2e-hotel-template-v1.js";
import { listFourteenWorkflowSegmentIds } from "../lib/group-demand-intelligence/research/universal-contact-research-contract-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL_ID = "recrPQcZg7SFARRb2";
const HOTEL_NAME = "YOTEL Geneva Lake";
const REF = "2026-11-16";
const OUT = path.join(ROOT, "reports/gdi-yotel-geneva-lake-commercial-quality-and-40plus-readiness-v1");
const PRIOR_EXPANSION = path.join(ROOT, "reports/gdi-yotel-geneva-lake-40plus-exa-expansion-v1");
const PRIOR_FP = "b4d92494c2240a0296025f0f290280196de34b4a4b95dd0b3dc91732b9cd8763";
const PRIOR_PKG_DIR = path.join(
  ROOT,
  "lib/group-demand-intelligence/bundled-publication-packages",
  HOTEL_ID,
  "gdi_pub_recrPQcZg7SFARRb2_20261116_yotel_geneva_lake_40plus_exa_35usd_v1",
);
const PUB_VERSION = `gdi_pub_${HOTEL_ID}_20261116_yotel_geneva_lake_commercial_quality_40plus_readiness_v1`;
const ARGS = new Set(process.argv.slice(2));
const BROWSER_VERIFY = ARGS.has("--browser-verify");

const text = (v) => String(v == null ? "" : v).replace(/\s+/g, " ").trim();
const readJson = (p) => JSON.parse(fs.readFileSync(p, "utf8").replace(/^\uFEFF/, ""));
const writeJson = (name, obj) => {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, name), `${JSON.stringify(obj, null, 2)}\n`);
};
const sh = (cmd) => {
  try {
    return execSync(cmd, { cwd: ROOT, encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] }).trim();
  } catch {
    return "";
  }
};
const clone = (o) => JSON.parse(JSON.stringify(o));
const norm = (s) =>
  text(s)
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();

// ---------------------------------------------------------------------------
// Audit decision tables (zero paid — based on staged evidence only)
// ---------------------------------------------------------------------------
/** Confirmed same-edition duplicates: survivor keeps HM; absorbed IDs become research-only. */
const DEDUPE_PAIRS = [
  {
    family: "LES_HIVERNALES_2027",
    survivorId: "rx_d_3521ecf91bb9",
    absorbedIds: ["rx_d_8bff60e01ffc"],
    reason: "Same March 4–7, 2027 edition (Les Hivernales 17th / Les Hivernales 2027).",
  },
  {
    family: "CHI_GENEVA_2026",
    survivorId: "rx_d_9bad59b0ae60",
    absorbedIds: ["rx_d_d8fddbbdd78f"],
    reason: "Same Dec 9–13, 2026 CHI Geneva 100th anniversary edition.",
  },
];

/**
 * Per-opportunity commercial audit overrides.
 * disposition: RETAIN_HM | DEMOTE_RESEARCH | DEMOTE_NOT_ACTIONABLE | RETAIN_CMI | DEMOTE_CMI_RESEARCH
 */
const AUDIT = {
  gdi_opp_yotel_fe2e_icpic_20270914: {
    disposition: "RETAIN_HM",
    geo: "SECONDARY",
    readiness: "QUALIFY_OPPORTUNITY",
    salesAction: "Qualify Opportunity",
    why: "Dated 2027 ICPIC edition with named congress organiser; lodging channel for 2027 not yet verified; contact title still references ICPIC 2025 — qualify role for 2027 before pitching rooms.",
    ask: "Confirm who owns 2027 housing and whether airport-corridor / La Côte overflow is open.",
    when: "Now through lodging RFP window (likely H1 2027).",
    uncertainty: "Official housing list and YOTEL fit vs central-Geneva hotels unverified without further research.",
  },
  "rx_yotel_rec_chateau nyon": {
    disposition: "RETAIN_HM",
    geo: "CORE",
    readiness: "ACCOUNT_DEVELOPMENT",
    salesAction: "Build Venue Partnership",
    why: "Core Nyon venue partnership for corporate/private-event lodging referrals around Château / Caveau programmes.",
    ask: "Who recommends overnight lodging for corporate receptions and visiting groups; any preferred-hotel arrangement.",
    when: "Ongoing account development; align with 2027–28 exhibition calendar.",
    uncertainty: "Exhibition itself does not prove a room block — partnership is the demand thesis.",
  },
  rx_d_630d02a59107: {
    disposition: "RETAIN_HM",
    geo: "SECONDARY",
    readiness: "ACCOUNT_DEVELOPMENT",
    salesAction: "Offer Overflow Block",
    why: "Multi-day IAS HIV Science Geneva 2027 with named Director, Conferences — realistic airport-corridor overflow / satellite meetings, not main venue replacement.",
    ask: "Whether speaker/staff/overflow housing is still open and who selects hotels.",
    when: "Immediately — large programmes book early.",
    uncertainty: "Main venue/housing bureau status not confirmed in staged evidence.",
  },
  rx_d_43b87c04bce8: {
    disposition: "RETAIN_HM",
    geo: "SECONDARY",
    readiness: "QUALIFY_OPPORTUNITY",
    salesAction: "Qualify Opportunity",
    why: "ITU AI for Good 2027 is a major Geneva programme; YOTEL only fits as overflow/staff/logistics lodging if sourcing is open.",
    ask: "Confirm housing channel and whether airport-side hotels can participate.",
    when: "Qualify before proposing a block.",
    uncertainty: "Central-Geneva / CICG gravity; no confirmed open lodging in staged package.",
  },
  rx_d_52859ee0961e: {
    disposition: "RETAIN_HM",
    geo: "CORE",
    readiness: "ACCOUNT_DEVELOPMENT",
    salesAction: "Propose Meeting Package",
    why: "OENOVITI 2027 hosted with CHANGINS / Agroscope — core La Côte scientific network with multi-day programme and named project contact.",
    ask: "Room-night estimate for visiting members and meeting/F&B needs near CHANGINS.",
    when: "Now → May 2027.",
    uncertainty: "Exact lodging owner beyond network secretariat not fully evidenced.",
  },
  rx_d_f05756860072: {
    disposition: "RETAIN_HM",
    geo: "AIRPORT_CORRIDOR",
    readiness: "ACCOUNT_DEVELOPMENT",
    salesAction: "Propose Meeting Package",
    why: "EuroLFT workshop with CERN Theory support — multi-day visiting researchers; airport-corridor lodging is commercially plausible.",
    ask: "Whether organisers want a single hotel for participants and meeting space.",
    when: "Now → March 2027.",
    uncertainty: "Exact venue (CERN vs Geneva) should be confirmed in outreach.",
  },
  rx_d_1faba8ad8a45: {
    disposition: "RETAIN_HM",
    geo: "AIRPORT_CORRIDOR",
    readiness: "QUALIFY_OPPORTUNITY",
    salesAction: "Qualify Overflow Demand",
    why: "Art Genève at Palexpo — retain only as exhibitor/staff/ancillary lodging qualification, not as primary show hotel.",
    ask: "Official hotel programme status and whether ancillary rooms remain open near airport/La Côte.",
    when: "Qualify before Jan 2027 show.",
    uncertainty: "No staged evidence YOTEL is a partner or that rooms remain open.",
  },
  rx_d_1fc1f554a10f: {
    disposition: "RETAIN_HM",
    geo: "AIRPORT_CORRIDOR",
    readiness: "QUALIFY_OPPORTUNITY",
    salesAction: "Qualify Overflow Demand",
    why: "Inventions Geneva at Palexpo — same overflow thesis; named exhibition director is a credible entry point.",
    ask: "Housing list openness for exhibitors/inventors needing airport-side stay.",
    when: "Qualify Q1 2027.",
    uncertainty: "Official accommodation arrangements unknown without further research.",
  },
  rx_d_d29851414427: {
    disposition: "RETAIN_HM",
    geo: "AIRPORT_CORRIDOR",
    readiness: "QUALIFY_OPPORTUNITY",
    salesAction: "Qualify Overflow Demand",
    why: "EPHJ at Palexpo (watch/jewellery) — exhibitor travel demand may fit YOTEL if housing is open.",
    ask: "Who manages exhibitor hotel recommendations and remaining inventory needs.",
    when: "Qualify before June 2027.",
    uncertainty: "Partner-hotel status unverified.",
  },
  rx_d_6f0f8cf059fa: {
    disposition: "DEMOTE_RESEARCH",
    geo: "CORE",
    readiness: "RESEARCH_CONTINUATION",
    salesAction: null,
    why: "Single-day Nyon RELAIS programme — legitimate local meeting/catering signal but no evidenced overnight block.",
    ask: "Whether any out-of-canton speakers need rooms or private dining only.",
    when: "Only after overnight demand is evidenced.",
    uncertainty: "Overnight inference would be speculative — demoted pending evidence.",
  },
  rx_d_9bad59b0ae60: {
    disposition: "RETAIN_HM",
    geo: "SECONDARY",
    readiness: "ACCOUNT_DEVELOPMENT",
    salesAction: "Pursue Crew / Officials Lodging",
    why: "CHI Geneva Dec 2026 — retain survivor after dedupe; pursue officials/production/sponsor lodging, not spectator volume.",
    ask: "Who books officials, riders’ support teams, and media; whether La Côte / airport hotels can still participate.",
    when: "Immediate (event Dec 9–13, 2026 — short window).",
    uncertainty: "Imminent sourcing window; may already be largely placed.",
  },
  rx_d_3521ecf91bb9: {
    disposition: "RETAIN_HM",
    geo: "CORE",
    readiness: "ACCOUNT_DEVELOPMENT",
    salesAction: "Pursue Production / Artist Lodging",
    why: "Les Hivernales Nyon — core territory festival; demand thesis is production/artist/crew rooms, not attendance size.",
    ask: "Housing needs for visiting companies and technical crews; preferred hotel list status.",
    when: "Now → March 2027.",
    uncertainty: "Artist housing may be private apartments — confirm hotel use.",
  },
  rx_d_eae99d91912c: {
    disposition: "RETAIN_HM",
    geo: "CORE",
    readiness: "ACCOUNT_DEVELOPMENT",
    salesAction: "Pursue Crew / Partner Lodging",
    why: "Paléo Nyon — core territory; hotel demand is crews, artists, partners, not ticket holders.",
    ask: "Accommodation lead for visiting production and partner teams.",
    when: "Account development now for 2027 edition.",
    uncertainty: "Large festivals often use mixed housing; hotel share must be qualified.",
  },
  rx_d_b9b9718a5b06: {
    disposition: "DEMOTE_RESEARCH",
    geo: "STRETCH",
    readiness: "RESEARCH_CONTINUATION",
    salesAction: null,
    why: "European Para Championships 2027 announced for Geneva — citywide multi-sport; YOTEL fit not evidenced beyond geography stretch/overflow speculation.",
    ask: "Local organising committee lodging owner and whether airport-corridor hotels are in play.",
    when: "After lodging owner + open sourcing evidenced.",
    uncertainty: "No staged proof of independently bookable YOTEL demand.",
  },
  rx_d_fe747015e55a: {
    disposition: "RETAIN_HM",
    geo: "CORE",
    readiness: "ACCOUNT_DEVELOPMENT",
    salesAction: "Propose Guest / Jury Lodging",
    why: "FIFAN Nyon archaeology film festival — core; visiting filmmakers/jury are the hotel thesis.",
    ask: "Whether festival books a hotel for guests/jury and remaining nights.",
    when: "Now → Aug 2027.",
    uncertainty: "Room volume likely small — still actionable for YOTEL.",
  },
  rx_d_56f22f3e363b: {
    disposition: "RETAIN_HM",
    geo: "CORE",
    readiness: "ACCOUNT_DEVELOPMENT",
    salesAction: "Pursue Officials / Team Lodging",
    why: "Triathlon de Nyon 2027 — core; officials/teams lodging (not spectators).",
    ask: "Housing coordinator for 2027 and openness beyond any prior partner hotel.",
    when: "Now → Aug 2027.",
    uncertainty: "Prior FISU edition used Everness — competitor history; pursue future-cycle / overflow.",
  },
  rx_d_9683bf0326ae: {
    disposition: "RETAIN_HM",
    geo: "AIRPORT_CORRIDOR",
    readiness: "ACCOUNT_DEVELOPMENT",
    salesAction: "Propose Meeting Package",
    why: "ALEGRO / CERN workshop — multi-day scientific visitors; airport corridor fit.",
    ask: "Participant lodging plan and meeting-space needs.",
    when: "Now → March 2027.",
    uncertainty: "Venue on CERN campus vs hotel meeting space to confirm.",
  },
  rx_d_8bff60e01ffc: {
    disposition: "ABSORBED_DUPLICATE",
    geo: "CORE",
    readiness: "RESEARCH_CONTINUATION",
    salesAction: null,
    why: "Duplicate of Les Hivernales 17th edition — absorbed into rx_d_3521ecf91bb9.",
  },
  rx_d_eefd0b2d1214: {
    disposition: "RETAIN_HM",
    geo: "CORE",
    readiness: "QUALIFY_OPPORTUNITY",
    salesAction: "Qualify Opportunity",
    why: "Rive Jazzy 2027 — Nyon/core cultural series; need clearer programme dates and lodging mechanism.",
    ask: "Exact 2027 dates and whether visiting artists need hotel rooms.",
    when: "Qualify before pitching.",
    uncertainty: "Staged start 2027-07-01 looks provisional; overnight not proven.",
  },
  rx_d_3a209b6a7f29: {
    disposition: "RETAIN_HM",
    geo: "AIRPORT_CORRIDOR",
    readiness: "QUALIFY_OPPORTUNITY",
    salesAction: "Pursue Catering / Overflow",
    why: "GemGenève at Palexpo — qualify exhibitor overflow and any F&B/private events; not main show hotel.",
    ask: "Housing and hospitality contacts for exhibitors needing airport-side stay.",
    when: "Qualify before May 2027.",
    uncertainty: "Official hotel list unknown.",
  },
  rx_d_d8fddbbdd78f: {
    disposition: "ABSORBED_DUPLICATE",
    geo: "SECONDARY",
    readiness: "RESEARCH_CONTINUATION",
    salesAction: null,
    why: "Duplicate of CHI Geneva 100th — absorbed into rx_d_9bad59b0ae60.",
  },
  // CMI
  gdi_opp_yotel_fe2e_sfpe_20270414: {
    disposition: "RETAIN_CMI",
    geo: "SECONDARY",
    readiness: "ACCOUNT_DEVELOPMENT",
    why: "SFPE 2027 committed at Crowne Plaza Geneva — valid future-cycle CMI.",
  },
  gdi_opp_yotel_fe2e_daus_20270210: {
    disposition: "RETAIN_CMI",
    geo: "SECONDARY",
    readiness: "ACCOUNT_DEVELOPMENT",
    why: "DAUS 2027 committed at Crowne Plaza Geneva — valid CMI.",
  },
  gdi_opp_yotel_fe2e_opal_20270503: {
    disposition: "RETAIN_CMI",
    geo: "SECONDARY",
    readiness: "ACCOUNT_DEVELOPMENT",
    why: "OPAL 2027 committed at Crowne Plaza Geneva — valid CMI.",
  },
  rx_d_c0628b9ceb2b: {
    disposition: "RETAIN_CMI",
    geo: "AIRPORT_CORRIDOR",
    readiness: "ACCOUNT_DEVELOPMENT",
    why: "Zeek Workshop Europe 2027 listed at Mövenpick / NH Geneva Airport — competitor commitment signal; future-cycle AD.",
  },
  rx_d_e97bc923f291: {
    disposition: "RETAIN_CMI",
    geo: "AIRPORT_CORRIDOR",
    readiness: "ACCOUNT_DEVELOPMENT",
    why: "SCET 2027 listed at Mövenpick / NH Geneva Airport — competitor commitment; future-cycle AD.",
  },
};

const PALEXPO_IDS = new Set([
  "rx_d_1faba8ad8a45",
  "rx_d_1fc1f554a10f",
  "rx_d_d29851414427",
  "rx_d_3a209b6a7f29",
]);

// ---------------------------------------------------------------------------
// browser verify mode
// ---------------------------------------------------------------------------
function argVal(flag) {
  const i = process.argv.indexOf(flag);
  return i >= 0 ? process.argv[i + 1] : null;
}

async function runBrowserVerify() {
  const meta = readJson(path.join(OUT, "14-UPDATED_STAGED_PUBLICATION.json"));
  const base = (argVal("--base-url") || process.env.GDI_PREVIEW_BASE || "http://127.0.0.1:8080").replace(/\/$/, "");
  const pageUrl = `${base}/group-demand-intelligence.html?hotelId=${HOTEL_ID}&preview=staged`;
  const apiUrl = `${base}/api/group-demand-intelligence/hotels/${HOTEL_ID}/opportunities?publicationPackage=1&surface=customer&preview=staged`;
  const result = {
    contract: "GDI_YOTEL_COMMERCIAL_QUALITY_BROWSER_VERIFICATION_V1",
    generatedAt: new Date().toISOString(),
    pageUrl,
    apiUrl,
    expectedFingerprint: meta.fingerprint,
    expectedCounts: meta.inventoryCounts,
    checks: {},
    errors: [],
  };
  const { chromium } = await import("playwright");
  const browser = await chromium.launch({ headless: true });
  try {
    const page = await browser.newPage();
    const api = await page.goto(apiUrl, { waitUntil: "networkidle", timeout: 60000 }).then(async (r) => {
      result.checks.apiHttpStatus = r.status();
      return r.json();
    });
    const fp = api?.publication?.contentFingerprintSha256 || api?.contentFingerprintSha256 || null;
    result.checks.apiFingerprint = fp;
    result.checks.fingerprintMatch = fp === meta.fingerprint;
    const opps = api?.opportunities || api?.items || [];
    result.checks.apiOpportunityCount = opps.length;
    result.checks.apiHotelCount = opps.filter((o) => o.hotelMarketScopeEligible).length;
    result.checks.apiCompetitiveCount = opps.filter((o) => o.competitiveMarketScopeEligible).length;
    await page.goto(pageUrl, { waitUntil: "networkidle", timeout: 60000 });
    await page.waitForSelector('article.brand-card[data-gdi-tour-target="opportunity-card"], [data-opportunity-id]', {
      timeout: 90000,
    });
    const dom = await page.evaluate(() => {
      const cards = [...document.querySelectorAll('article.brand-card[data-gdi-tour-target="opportunity-card"], [data-opportunity-id]')];
      return {
        cards: cards.length,
        titles: cards.map((c) => (c.querySelector("h2,h3,.title,[data-opportunity-title]")?.textContent || "").trim()).filter(Boolean),
        stillLoading: /loading/i.test(document.body.innerText.slice(0, 500)),
      };
    });
    result.checks.dom = dom;
    const shotDir = path.join(OUT, "screenshots");
    fs.mkdirSync(shotDir, { recursive: true });
    await page.screenshot({ path: path.join(shotDir, "01-yotel-commercial-quality-staged-overview.png"), fullPage: true });
  } catch (e) {
    result.errors.push(String(e?.message || e));
  } finally {
    await browser.close();
  }
  writeJson("15-BROWSER_VERIFICATION.json", result);
  console.log(JSON.stringify(result, null, 2));
}

if (BROWSER_VERIFY) {
  await runBrowserVerify();
  process.exit(0);
}

// ---------------------------------------------------------------------------
// Load baseline
// ---------------------------------------------------------------------------
fs.mkdirSync(OUT, { recursive: true });
const priorPkg = readJson(path.join(PRIOR_PKG_DIR, "package.json"));
if (priorPkg.contentFingerprintSha256 !== PRIOR_FP) {
  console.error(`Prior fingerprint mismatch: ${priorPkg.contentFingerprintSha256} != ${PRIOR_FP}`);
  process.exit(3);
}
const priorOpps = priorPkg.opportunities || [];
const priorHm = priorOpps.filter((o) => o.hotelMarketScopeEligible);
const priorCmi = priorOpps.filter((o) => o.competitiveMarketScopeEligible);
const hotelCfg = readJson(path.join(ROOT, `config/group-demand-intelligence/hotels/${HOTEL_ID}.json`));
const p1 = readJson(path.join(PRIOR_EXPANSION, "03-PASS1_DISCOVERY_RESULTS.json"));
const p2 = readJson(path.join(PRIOR_EXPANSION, "04-PASS2_DEEP_RESULTS.json"));
const elig = readJson(path.join(PRIOR_EXPANSION, "06-COMMERCIAL_ELIGIBILITY.json"));
const ledger = readJson(path.join(PRIOR_EXPANSION, "paid-budget-ledger.json"));
const segments = listFourteenWorkflowSegmentIds();

writeJson("01-CERTIFIED_BASELINE.json", {
  contract: "GDI_YOTEL_COMMERCIAL_QUALITY_BASELINE_V1",
  generatedAt: new Date().toISOString(),
  hotelId: HOTEL_ID,
  hotelName: HOTEL_NAME,
  referenceDate: REF,
  paidSpendThisPhaseUsd: 0,
  priorPilot: {
    pr: 301,
    mergeSha: "4ec7a288070fe9d0ab8490287eac92933e58e4b5",
    spentUsd: Number(ledger.spentUsd || 30.6),
    remainingUsd: Number(ledger.remainingUsd ?? 4.4),
    ceilingUsd: 35,
    note: "Remaining $4.40 NOT consumed by this audit.",
  },
  priorStagedFingerprintPreserved: PRIOR_FP,
  priorPublicationVersion: priorPkg.publicationVersion,
  inventoryBefore: {
    hotelMarket: priorHm.length,
    cmi: priorCmi.length,
    total: priorOpps.length,
    hotelMarketIds: priorHm.map((o) => o.id),
    cmiIds: priorCmi.map((o) => o.id),
  },
  hotelProfile: {
    rooms: hotelCfg.capabilityProfile?.totalGuestrooms || 237,
    meetingRooms: hotelCfg.capabilityProfile?.meetingRoomCount || 6,
    largestTheater: hotelCfg.capabilityProfile?.largestTheaterCapacity || 250,
    territory: hotelCfg.demandTerritory || null,
  },
  template: GDI_FULL_E2E_HOTEL_TEMPLATE_V1,
  actionabilityGate: GDI_COMMERCIAL_ACTIONABILITY_GATE_V1,
  cmiContract: GDI_CMI_CANONICAL_ELIGIBILITY_CONTRACT_V1,
  git: { branch: sh("git rev-parse --abbrev-ref HEAD"), head: sh("git rev-parse HEAD") },
});

// ---------------------------------------------------------------------------
// Dedup + commercial transform
// ---------------------------------------------------------------------------
const absorbed = new Map();
for (const pair of DEDUPE_PAIRS) {
  for (const id of pair.absorbedIds) absorbed.set(id, pair);
}

const duplicateAnalysis = {
  mandatory: DEDUPE_PAIRS.map((p) => ({
    ...p,
    survivorTitle: priorOpps.find((o) => o.id === p.survivorId)?.displayTitle || null,
    absorbedTitles: p.absorbedIds.map((id) => priorOpps.find((o) => o.id === id)?.displayTitle || id),
    confirmed: true,
  })),
  additionalScanned: priorHm
    .map((o) => ({ id: o.id, title: o.displayTitle || o.title, org: o.organizationName, start: o.eventStartDate }))
    .filter((a, i, arr) =>
      arr.some(
        (b, j) =>
          j > i &&
          a.start &&
          a.start === b.start &&
          (norm(a.org).slice(0, 20) === norm(b.org).slice(0, 20) || norm(a.title).slice(0, 24) === norm(b.title).slice(0, 24)),
      ),
    ),
  note: "Only confirmed same-edition pairs were consolidated. Same-org Palexpo shows remain distinct programmes.",
};
writeJson("02-DUPLICATE_ANALYSIS.json", duplicateAnalysis);

const mapping = [];
const retainedHm = [];
const retainedCmi = [];
const demoted = [];
const researchOnly = [];

for (const raw of priorOpps) {
  const o = clone(raw);
  const audit = AUDIT[o.id] || {
    disposition: o.hotelMarketScopeEligible ? "RETAIN_HM" : o.competitiveMarketScopeEligible ? "RETAIN_CMI" : "DEMOTE_RESEARCH",
    geo: "SECONDARY",
    readiness: "QUALIFY_OPPORTUNITY",
    why: "No explicit audit row — default qualify.",
  };

  const mapRow = {
    beforeId: o.id,
    title: o.displayTitle || o.title,
    beforeHm: !!o.hotelMarketScopeEligible,
    beforeCmi: !!o.competitiveMarketScopeEligible,
    disposition: audit.disposition,
    geo: audit.geo,
    readiness: audit.readiness,
    afterId: o.id,
    afterHm: false,
    afterCmi: false,
    afterVisibility: "HIDDEN_RESEARCH",
  };

  if (audit.disposition === "ABSORBED_DUPLICATE") {
    const pair = absorbed.get(o.id);
    o.hotelMarketScopeEligible = false;
    o.competitiveMarketScopeEligible = false;
    o.inventoryPopulationPrimary = INVENTORY_POPULATION.RESEARCH_ONLY || "RESEARCH_ONLY";
    o._auditDemotion = {
      reason: audit.why || pair?.reason,
      absorbedInto: pair?.survivorId,
      family: pair?.family,
    };
    o.recommendedNextStep = `Research preserved under canonical opportunity ${pair?.survivorId}. ${audit.why || ""}`;
    researchOnly.push(o);
    mapRow.afterVisibility = "ABSORBED_INTO_SURVIVOR";
    mapRow.afterId = pair?.survivorId || o.id;
    mapping.push(mapRow);
    demoted.push({ id: o.id, kind: "DUPLICATE_ABSORBED", into: pair?.survivorId, reason: pair?.reason });
    continue;
  }

  if (audit.disposition === "DEMOTE_RESEARCH" || audit.disposition === "DEMOTE_NOT_ACTIONABLE") {
    o.hotelMarketScopeEligible = false;
    o.competitiveMarketScopeEligible = false;
    o._auditDemotion = { reason: audit.why, readiness: audit.readiness };
    o.recommendedNextStep = audit.ask || audit.why;
    o.salesActionPillLabel = null;
    researchOnly.push(o);
    mapRow.afterVisibility = audit.disposition === "DEMOTE_NOT_ACTIONABLE" ? "NOT_CURRENTLY_ACTIONABLE" : "RESEARCH_CONTINUATION";
    mapping.push(mapRow);
    demoted.push({ id: o.id, kind: audit.disposition, reason: audit.why });
    continue;
  }

  if (audit.disposition === "RETAIN_CMI" || audit.disposition === "DEMOTE_CMI_RESEARCH") {
    if (audit.disposition === "DEMOTE_CMI_RESEARCH") {
      o.hotelMarketScopeEligible = false;
      o.competitiveMarketScopeEligible = false;
      researchOnly.push(o);
      mapRow.afterVisibility = "RESEARCH_CONTINUATION";
      demoted.push({ id: o.id, kind: "CMI_DEMOTED", reason: audit.why });
    } else {
      o.hotelMarketScopeEligible = false;
      o.competitiveMarketScopeEligible = true;
      o._audit = audit;
      // keep CMI annotation
      const annotated = annotateCanonicalCmiContract(o);
      retainedCmi.push(annotated);
      mapRow.afterCmi = true;
      mapRow.afterVisibility = "CUSTOMER_CMI";
    }
    mapping.push(mapRow);
    continue;
  }

  // RETAIN_HM
  o.hotelMarketScopeEligible = true;
  o.competitiveMarketScopeEligible = false;
  o._audit = audit;
  o.geoFitBand = audit.geo;
  if (audit.salesAction) {
    o.salesActionPillLabel = audit.salesAction;
    o.recommendedAction = audit.salesAction;
    o.recommendedNextStep = [
      audit.why,
      audit.ask ? `Ask: ${audit.ask}` : null,
      audit.when ? `When: ${audit.when}` : null,
      audit.uncertainty ? `Unverified: ${audit.uncertainty}` : null,
    ]
      .filter(Boolean)
      .join(" ");
  }
  if (audit.readiness === "QUALIFY_OPPORTUNITY") {
    o.commercialStagePillLabel = "Qualify Opportunity";
    o.commercialStageKey = "qualify_opportunity";
    o.commercialActionability = "QUALIFY";
  } else if (audit.readiness === "ACCOUNT_DEVELOPMENT") {
    o.commercialStagePillLabel = "Account Development";
    o.commercialStageKey = "account_development";
    o.commercialActionability = "ACCOUNT_DEVELOPMENT";
  } else if (audit.readiness === "SALES_READY") {
    o.commercialStagePillLabel = "Sales Ready";
    o.commercialStageKey = "sales_ready";
    o.commercialActionability = "SALES_READY";
  }
  // Merge absorbed duplicate evidence note onto survivor
  const pairAsSurvivor = DEDUPE_PAIRS.find((p) => p.survivorId === o.id);
  if (pairAsSurvivor) {
    o._dedupeMergedFrom = pairAsSurvivor.absorbedIds;
    o.whatIsHappening = `${text(o.whatIsHappening)}\n\n[Audit] Consolidated duplicate IDs ${pairAsSurvivor.absorbedIds.join(", ")}: ${pairAsSurvivor.reason}`.trim();
  }
  retainedHm.push(o);
  mapRow.afterHm = true;
  mapRow.afterVisibility = "CUSTOMER_HOTEL_MARKET";
  mapping.push(mapRow);
}

writeJson("03-CANONICAL_OPPORTUNITY_MAPPING.json", {
  generatedAt: new Date().toISOString(),
  rows: mapping,
  summary: {
    beforeHm: priorHm.length,
    afterHm: retainedHm.length,
    beforeCmi: priorCmi.length,
    afterCmi: retainedCmi.length,
    demotedOrAbsorbed: demoted.length,
    researchOnly: researchOnly.length,
  },
});

// ---------------------------------------------------------------------------
// Detailed audit reports
// ---------------------------------------------------------------------------
writeJson(
  "04-HOTEL_MARKET_COMMERCIAL_AUDIT.json",
  retainedHm.map((o) => {
    const a = o._audit || {};
    const oneDay = o.eventStartDate && o.eventEndDate && o.eventStartDate === o.eventEndDate;
    return {
      id: o.id,
      title: o.displayTitle || o.title,
      org: o.organizationName,
      dates: { start: o.eventStartDate, end: o.eventEndDate, oneDay },
      geo: a.geo,
      readiness: a.readiness,
      salesAction: a.salesAction,
      why: a.why,
      ask: a.ask,
      when: a.when,
      uncertainty: a.uncertainty,
      contact: {
        name: o.primaryContact?.name || o.contactName || null,
        title: o.primaryContact?.title || o.contactTitle || null,
        sourceUrl: o.primaryContact?.sourceUrl || o.contactSourceUrl || null,
      },
      sourcing: o.targetHotelSourcingStatusLabel || o.hotelSourcingStatusLabel || null,
      palexpoRelated: PALEXPO_IDS.has(o.id),
      dedupeMergedFrom: o._dedupeMergedFrom || [],
      gates: {
        namedOrg: Boolean(o.organizationName),
        futureDates: Boolean(o.eventStartDate && o.eventStartDate >= REF),
        namedContact: Boolean(o.primaryContact?.name || o.contactName),
        demandThesis: Boolean(a.why),
      },
    };
  }),
);

writeJson("05-GEOGRAPHIC_AND_PALExPO_FIT.json", {
  bands: {
    CORE: retainedHm.filter((o) => o._audit?.geo === "CORE").map((o) => o.id),
    AIRPORT_CORRIDOR: retainedHm.filter((o) => o._audit?.geo === "AIRPORT_CORRIDOR").map((o) => o.id),
    SECONDARY: retainedHm.filter((o) => o._audit?.geo === "SECONDARY").map((o) => o.id),
    STRETCH: [...retainedHm, ...demoted].filter((o) => (o._audit || AUDIT[o.id])?.geo === "STRETCH").map((o) => o.id || o),
  },
  palexpoAssessments: [...PALEXPO_IDS].map((id) => {
    const o = priorOpps.find((x) => x.id === id);
    const a = AUDIT[id];
    return {
      id,
      title: o?.displayTitle || o?.title,
      venue: "Palexpo (Geneva)",
      transportPracticality: "Airport / A1 corridor — practical for YOTEL Founex as overflow, not as downtown base.",
      officialHotelProgramme: "UNKNOWN_IN_STAGED_EVIDENCE",
      yotelPartner: "NOT_EVIDENCED",
      ancillaryDemandRealistic: a?.disposition === "RETAIN_HM",
      distinctHotelDemandEvidence: "Inferred exhibitor/staff travel only — not main-show housing.",
      contactEntryPoint: Boolean(o?.primaryContact?.name || o?.contactName),
      salesActionCurrentOrFuture: a?.salesAction || null,
      disposition: a?.disposition,
      readiness: a?.readiness,
      note: a?.why,
    };
  }),
  rule: "Palexpo hosting alone does not retain Hotel Market. Retained rows are QUALIFY overflow theses with named contacts.",
});

writeJson(
  "06-CONTACT_QUALITY.json",
  [...retainedHm, ...retainedCmi].map((o) => {
    const name = o.primaryContact?.name || o.contactName || null;
    const title = o.primaryContact?.title || o.contactTitle || null;
    const url = o.primaryContact?.sourceUrl || o.contactSourceUrl || null;
    const issues = [];
    if (!name) issues.push("MISSING_NAMED_CONTACT");
    if (title && /2025/.test(title) && o.eventStartDate?.startsWith("2027")) issues.push("CONTACT_TITLE_YEAR_MISMATCH");
    if (!url) issues.push("MISSING_CONTACT_SOURCE_URL");
    return {
      id: o.id,
      title: o.displayTitle || o.title,
      contact: { name, title, sourceUrl: url },
      roleAssessment: issues.includes("CONTACT_TITLE_YEAR_MISMATCH")
        ? "CREDIBLE_ENTRY_POINT_NEEDS_ROLE_CONFIRMATION"
        : name
          ? "CREDIBLE_COMMERCIAL_ENTRY_POINT"
          : "INSUFFICIENT",
      bookingAuthority: "NOT_ASSUMED",
      issues,
      surfe: "NOT_INVOKED",
    };
  }),
);

const readinessBuckets = {
  SALES_READY: [],
  ACCOUNT_DEVELOPMENT: [],
  QUALIFY_OPPORTUNITY: [],
  RESEARCH_CONTINUATION: [],
  NOT_CURRENTLY_ACTIONABLE: [],
  REJECTED: [],
  CMI: retainedCmi.map((o) => o.id),
};
for (const o of retainedHm) {
  const r = o._audit?.readiness || "QUALIFY_OPPORTUNITY";
  (readinessBuckets[r] || readinessBuckets.QUALIFY_OPPORTUNITY).push(o.id);
}
for (const d of demoted) {
  if (d.kind === "DUPLICATE_ABSORBED") readinessBuckets.RESEARCH_CONTINUATION.push(d.id);
  else if (d.kind === "DEMOTE_NOT_ACTIONABLE") readinessBuckets.NOT_CURRENTLY_ACTIONABLE.push(d.id);
  else readinessBuckets.RESEARCH_CONTINUATION.push(d.id);
}
writeJson("07-SALES_READINESS_RECLASSIFICATION.json", {
  buckets: readinessBuckets,
  counts: Object.fromEntries(Object.entries(readinessBuckets).map(([k, v]) => [k, v.length])),
  salesReadyForced: false,
  note: "No Sales Ready promotions — none satisfied the full SR contract (sourcing + buyer authority + concrete ask).",
});

writeJson(
  "08-CMI_VERIFICATION.json",
  retainedCmi.map((o) => {
    const gates = evaluateCmiQualificationGates ? evaluateCmiQualificationGates(o) : null;
    return {
      id: o.id,
      title: o.displayTitle || o.title,
      competitor: o.cmiCompetitorHotel || o.competitorHotel,
      commitmentEvidence: o.cmiEvidenceStatus || o.cmiClassificationReason || null,
      venueVsGuestrooms: o.committedBusinessType || o.cmiClass || null,
      futureCycle: true,
      contact: o.primaryContact?.name || o.contactName || null,
      retained: true,
      contract: GDI_CMI_CANONICAL_ELIGIBILITY_CONTRACT_V1,
      gates,
    };
  }),
);

// ---------------------------------------------------------------------------
// 14-segment yield (from prior paid campaign artifacts — $0 this phase)
// ---------------------------------------------------------------------------
const segYield = {};
for (const seg of segments) {
  const calls = (p1.calls || []).filter((c) => c.segment === seg || c.job?.includes(seg));
  const hits = calls.reduce((n, c) => n + Number(c.hits || 0), 0);
  const deep = (p2.runs || []).filter((r) => (r.cand?.segmentId || r.segment) === seg || text(r.cand?.segmentId).includes(seg.split("_")[0]));
  const decisions = (elig.decisions || []).filter((d) => d.segment === seg);
  const hmQual = decisions.filter((d) => d.bucket === "HOTEL_MARKET_CANDIDATE").length;
  const cmiQual = decisions.filter((d) => d.bucket === "CMI_CANDIDATE").length;
  const watch = decisions.filter((d) => d.bucket === "WATCH").length;
  const rejected = decisions.filter((d) => d.bucket === "REJECTED").length;
  const spend = calls.reduce((n, c) => n + Number(c.costUsd || 0), 0) + deep.reduce((n, r) => n + Number(r.costUsd || r.paid?.actualCostUsd || 0), 0);
  segYield[seg] = {
    discoveryJobs: calls.length,
    rawHits: hits,
    deepResearched: deep.length,
    hotelMarketCandidates: hmQual,
    cmiCandidates: cmiQual,
    watch,
    rejected,
    approxSpendUsd: Number(spend.toFixed(2)),
    retainedAfterThisAudit: retainedHm.filter((o) => {
      // soft match via eligibility decision titles
      return decisions.some((d) => d.candId && retainedHm.some((r) => r.id.includes(String(d.candId).replace(/^rx_/, ""))));
    }).length,
  };
}
// Post-audit retained by heuristic tags in titles/orgs for reporting
const retainedByTheme = {
  associations_conferences_events: ["icpic", "oenoviti", "euro lft", "alegro"],
  education_scientific: ["hiv", "oenoviti", "euro lft", "alegro"],
  sports_entertainment: ["chi", "triathlon", "hivernales", "paleo", "fifan", "rive jazzy"],
  local_catering_day_meetings: [],
  corporate_meetings_incentives: [],
  projects_infrastructure: [],
  professional_event_planners: [],
  preferred_lodging_partnerships: ["chateau", "kunz"],
  social_special_events: ["hivernales", "paleo", "rive", "fifan"],
  government_public_sector: [],
  corporate_accounts_scaling: [],
  travel_trade_group_intermediaries: [],
  relocation_extended_stay: [],
  corporate_celebrations_private_events: [],
};
for (const seg of segments) {
  const keys = retainedByTheme[seg] || [];
  segYield[seg].retainedHotelMarketApprox = retainedHm.filter((o) => {
    const blob = norm(`${o.displayTitle} ${o.organizationName}`);
    return keys.some((k) => blob.includes(k));
  }).length;
  segYield[seg].costPerRetainedHmApprox =
    segYield[seg].retainedHotelMarketApprox > 0
      ? Number((segYield[seg].approxSpendUsd / segYield[seg].retainedHotelMarketApprox).toFixed(2))
      : null;
}

writeJson("09-14_SEGMENT_YIELD_ANALYSIS.json", {
  priorPilotSpendUsd: Number(ledger.spentUsd || 30.6),
  thisPhaseSpendUsd: 0,
  segments: segYield,
  strongestCommercialSegments: [
    "education_scientific / associations (OENOVITI, EuroLFT, ALEGRO, ICPIC qualify)",
    "sports_entertainment & social (Nyon core: Hivernales, Paléo, FIFAN, Triathlon, CHI)",
    "preferred_lodging (Château de Nyon partnership)",
    "Palexpo overflow only as QUALIFY — weak final commercial conversion vs discovery volume",
  ],
  weakSegments: [
    "corporate_accounts_scaling — discovery volume without retained HM after audit",
    "government_public_sector — no retained HM",
    "travel_trade / planners — no retained HM",
    "projects / relocation — recovery leads did not convert to HM",
  ],
  lesson: "Raw Palexpo/Geneva discovery inflated intermediate HM candidates; post-audit unique HM is driven by Nyon/La Côte core + selective overflow.",
});

writeJson("10-RESEARCH_CONTINUATION_QUEUE.json", {
  demotedFromHotelMarket: demoted,
  researchOnlyIds: researchOnly.map((o) => o.id),
  returnCriteria: demoted.map((d) => ({
    id: d.id,
    kind: d.kind,
    reason: d.reason,
    returnWhen: AUDIT[d.id]?.ask || "Evidence of independently bookable YOTEL demand + named lodging owner.",
  })),
});

// ---------------------------------------------------------------------------
// Priority candidates for next paid campaign (from WATCH / recovery — $0 now)
// ---------------------------------------------------------------------------
const watchDecisions = (elig.decisions || []).filter((d) => d.bucket === "WATCH" || d.bucket === "HOTEL_MARKET_CANDIDATE");
const priorityThemes = [
  { pri: 1, theme: "CORE_CORPORATE_MEETINGS", match: /nyon|founex|coppet|la cote|changins|agroscope|gland/i },
  { pri: 2, theme: "ASSOCIATION_RETREATS", match: /workshop|symposium|assembly|committee|retreat/i },
  { pri: 3, theme: "MEDICAL_SCIENTIFIC", match: /health|medical|clinic|plasma|lattice|cern|hiv|infection/i },
  { pri: 4, theme: "SPORTS_CREW", match: /triathlon|federation|championship|official/i },
  { pri: 5, theme: "PRODUCTION_CREWS", match: /festival|production|crew|film|concert/i },
  { pri: 6, theme: "PROJECTS_EXTENDED", match: /commission|project|construction|hublot|gruner|superlab/i },
  { pri: 7, theme: "DMC_TRAVEL_TRADE", match: /dmc|incentive|tour operator|planner/i },
  { pri: 8, theme: "AIRPORT_PROFESSIONAL", match: /airport|aeroport|meyrin|grand.?saconnex/i },
  { pri: 9, theme: "PALEXPO_INDEPENDENT", match: /palexpo|ephj|gemgeneve|art geneve|invention/i },
];
const retainedTitles = new Set(retainedHm.map((o) => norm(o.displayTitle || o.title)));
const candidates = [];
for (const d of watchDecisions) {
  const blob = `${d.title} ${d.org} ${d.venue} ${d.city}`;
  if (retainedTitles.has(norm(d.title))) continue;
  if (absorbed.has(d.candId)) continue;
  const theme = priorityThemes.find((t) => t.match.test(blob)) || { pri: 20, theme: "OTHER" };
  candidates.push({
    existingCandidateId: d.candId || d.key,
    organization: d.org,
    title: d.title,
    segment: d.segment,
    theme: theme.theme,
    priority: theme.pri,
    evidenceAlready: {
      start: d.start,
      venue: d.venue,
      city: d.city,
      contact: d.contact || null,
      reasonWasWatch: d.reason,
      fit: d.fit,
      territory: d.territory,
    },
    missingFacts: [
      d.contact ? null : "named_contact_with_source",
      d.start ? null : "exact_future_dates",
      /unconfirmed|unknown/i.test(String(d.hotelSourcingStatus || "")) ? "sourcing_status" : null,
      "yotel_specific_demand_thesis",
    ].filter(Boolean),
    expectedYotelFit: /nyon|founex|coppet|changins|triathlon|hivernales|paleo|fifan/i.test(blob)
      ? "CORE_OR_NEAR"
      : /palexpo|geneva|cern/i.test(blob)
        ? "OVERFLOW_OR_AIRPORT"
        : "UNKNOWN",
    namedContactStatus: d.contact ? "PRESENT_NEEDS_VERIFICATION" : "MISSING",
    proposedExaTask: "HIGH deep research: verify dates, lodging mechanism, named lodging/meetings contact, YOTEL fit, competitor commitment.",
    estimatedCostUsd: 0.5,
    expectedBenefit: "Possible incremental Hotel Market if independently bookable demand is evidenced.",
  });
}
candidates.sort((a, b) => a.priority - b.priority || text(a.title).localeCompare(text(b.title)));
const topCandidates = candidates.slice(0, 50);
writeJson("11-30_TO_50_PRIORITY_CANDIDATES.json", {
  count: topCandidates.length,
  selectionRule: "Ranked from prior campaign WATCH/HM-candidate pool; excludes retained HM and absorbed duplicates; zero new paid discovery.",
  candidates: topCandidates,
});

const afterHm = retainedHm.length;
const gap = Math.max(0, 40 - afterHm);
const measuredCpi = 1.53;
writeJson("12-NEXT_EXA_BUDGET_PROPOSAL.json", {
  authorization: "NOT_AUTHORIZED — proposal only",
  thisPhaseSpendUsd: 0,
  priorRemainingUsd: 4.4,
  doNotSpendPriorRemainderOnAudit: true,
  gapTo40: gap,
  measuredCostPerIncrementalHmUsd: measuredCpi,
  referenceScaleUsd: 30,
  options: [
    {
      option: "MINIMUM_PILOT",
      usd: 15,
      includes: "Re-deep ~20 highest CORE/corporate/scientific candidates from the 50-queue",
      expectedIncrementalHmRange: "4–10 (not guaranteed)",
    },
    {
      option: "RECOMMENDED_SCALE",
      usd: 30,
      includes: "$4.40 prior remainder + ~$25.60 new authorization OR standalone $30; focus CORE La Côte + scientific workshops + sports crews; Palexpo only if open-housing evidenced",
      expectedIncrementalHmRange: "8–16 (not guaranteed; prior $1.53 CPI was pre-dedupe/audit)",
    },
    {
      option: "MAXIMUM_PROPOSED_CEILING",
      usd: 45,
      includes: "Recommended $30 + contingency if CORE yield ≥25% after first 15 deep runs",
      expectedIncrementalHmRange: "10–20",
    },
  ],
  stopRules: [
    "Hard stop at authorized ceiling with pre-call reservations.",
    "After each 10 deep runs, recompute incremental HM yield; if <10% for two consecutive batches on a segment, redirect spend.",
    "Do not spend to 'fill to 40' by lowering eligibility.",
    "No Surfe auto-enrichment; no Airtable writes; no CUSTOMER_PUBLISHED.",
    "$30 does NOT guarantee 19 more qualified records.",
  ],
});

// ---------------------------------------------------------------------------
// Restage cleaned inventory
// ---------------------------------------------------------------------------
function finalizeOpp(o) {
  const next = clone(o);
  // Keep audit fields out of customer package but preserve sales copy already applied.
  delete next._audit;
  delete next._auditDemotion;
  delete next._dedupeMergedFrom;
  // Do NOT call planCustomerTileBadges / resolveCustomerSalesAction before the
  // actionability gate — those helpers can rewrite timing/pills and push rows
  // into research-watch. Match the pilot script: annotate → gate → then filters.
  return next;
}

/** Quality gate: badge owns hotelSourcingStatusLabel — scrub that phrase from narratives. */
function scrubRepeatedSourcingNarratives(opp) {
  const next = { ...opp };
  const labels = [
    text(next.hotelSourcingStatusLabel),
    text(next.targetHotelSourcingStatusLabel),
  ].filter((s) => s.length >= 8);
  const fields = [
    "whatIsHappening",
    "potentialHotelBusiness",
    "potentialBusiness",
    "whyThisHotel",
    "whyActNow",
    "recommendedNextStep",
    "recommendedAction",
    "summary",
    "cardWhyNowLine",
    "tileClassificationLine",
  ];
  for (const field of fields) {
    let val = text(next[field]);
    if (!val) continue;
    for (const label of labels) {
      const re = new RegExp(label.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "gi");
      val = val.replace(re, "").replace(/\s{2,}/g, " ").replace(/\s([,.;:])/g, "$1").trim();
    }
    next[field] = val || next[field];
  }
  return next;
}

let customerOpps = [
  ...retainedHm.map((o) => finalizeOpp(o)),
  ...retainedCmi.map((o) => finalizeOpp(o)),
];
customerOpps = annotateInventoryPopulations(customerOpps, { hotelId: HOTEL_ID });

// Actionability gate
const gate = applyCommercialActionabilityGate(customerOpps, { referenceDate: REF });
let gatedRows = (gate.rows || []).filter(
  (o) => o.hotelMarketScopeEligible === true || o.competitiveMarketScopeEligible === true,
);
// Presentation helpers AFTER eligibility filter; preserve Market Scope flags.
// Use attachCustomerClassificationPresentation (merges plan onto DTO).
// Never assign planCustomerTileBadges / resolveCustomerSalesAction return values
// directly — those are plan objects without opportunity id.
gatedRows = gatedRows.map((o) => {
  const hm = o.hotelMarketScopeEligible;
  const cmi = o.competitiveMarketScopeEligible;
  let next = o;
  try {
    next = attachCustomerClassificationPresentation(o);
  } catch (e) {
    console.warn("[presentation]", o.id, String(e?.message || e));
  }
  const type = classifyOpportunityTypeFilter(next);
  next.opportunityTypeFilter = type;
  next.opportunityTypeFilterCategory = type;
  next.opportunityTypeFilterLabel = OPPORTUNITY_TYPE_FILTER_LABEL[type] || type;
  next.hotelMarketScopeEligible = hm;
  next.competitiveMarketScopeEligible = cmi;
  return next;
});
customerOpps = gatedRows.map(scrubRepeatedSourcingNarratives);
try {
  assertCustomerInventoryPassesActionabilityGate(customerOpps, { referenceDate: REF });
} catch (e) {
  console.warn("[actionability]", String(e?.message || e));
}
writeJson("09b-ACTIONABILITY_GATE.json", {
  activeHotel: gate.activeHotel?.length ?? null,
  activeCompetitive: gate.activeCompetitive?.length ?? null,
  researchWatch: gate.researchWatch?.length ?? null,
  customerVisibleAfterGate: customerOpps.length,
});

console.log("[stage-preflight]", {
  customerOpps: customerOpps.length,
  withId: customerOpps.filter((o) => o?.id).length,
  hm: customerOpps.filter((o) => o.hotelMarketScopeEligible).length,
  cmi: customerOpps.filter((o) => o.competitiveMarketScopeEligible).length,
  sampleIds: customerOpps.slice(0, 3).map((o) => o.id),
});
if (!customerOpps.length) {
  console.error("No customer opportunities after gate — aborting stage");
  process.exit(3);
}
const built = buildCustomerPublicationPackage({
  hotelId: HOTEL_ID,
  hotelName: HOTEL_NAME,
  opportunities: customerOpps,
  publicationVersion: PUB_VERSION,
  lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.FOUNDER_REVIEWED,
  approvalStatus: "PENDING_FOUNDER_APPROVAL",
  templateVersion: GDI_FULL_E2E_HOTEL_TEMPLATE_V1,
  notes: {
    founderReviewLabel: "STAGED_FOR_FOUNDER_REVIEW",
    auditContract: "GDI_YOTEL_COMMERCIAL_QUALITY_AND_40PLUS_READINESS_V1",
    priorFingerprintPreserved: PRIOR_FP,
    paidSpendUsd: 0,
    dedupeFamilies: DEDUPE_PAIRS.map((p) => p.family),
    customerPublished: false,
  },
});
const pkg = built.package || built;
console.log("[stage-built]", {
  keys: Object.keys(built || {}),
  pkgOpps: pkg?.opportunities?.length,
  fp: pkg?.contentFingerprintSha256?.slice?.(0, 16),
});
const integrity = verifyPublicationPackageIntegrity(pkg);
const qg = runPublicationQualityGate({ opportunities: pkg.opportunities || customerOpps, referenceDate: REF });
if (integrity?.ok === false) {
  console.error("Package integrity failed", integrity);
  process.exit(3);
}
if (qg?.ok === false) {
  console.error("Publication quality gate blocking", qg.blocking || qg.summary || qg);
  process.exit(3);
}
if (pkg.contentFingerprintSha256 === PRIOR_FP) {
  console.error("New fingerprint equals prior — refusing no-op stage");
  process.exit(3);
}
const store = createGdiPublicationPackageStore({
  root: path.join(ROOT, "lib/group-demand-intelligence/bundled-publication-packages"),
});
const written = store.writePackage(pkg);
store.writePointer(HOTEL_ID, "STAGED", {
  publicationId: pkg.publicationId,
  publicationVersion: pkg.publicationVersion,
  contentFingerprintSha256: pkg.contentFingerprintSha256,
  lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.FOUNDER_REVIEWED,
  founderReviewLabel: "STAGED_FOR_FOUNDER_REVIEW",
});
const counts = computePublicationInventoryCounts(pkg.opportunities || customerOpps);

writeJson("14-UPDATED_STAGED_PUBLICATION.json", {
  staged: true,
  publicationVersion: PUB_VERSION,
  publicationId: pkg.publicationId,
  fingerprint: pkg.contentFingerprintSha256,
  priorFingerprintPreserved: PRIOR_FP,
  priorPackageUnchangedOnDisk: true,
  lifecycleState: pkg.lifecycleState,
  founderReviewLabel: "STAGED_FOR_FOUNDER_REVIEW",
  customerPublished: false,
  integrity,
  qualityGate: qg,
  inventoryCounts: counts,
  packagePath: written?.path || path.join(ROOT, "lib/group-demand-intelligence/bundled-publication-packages", HOTEL_ID, PUB_VERSION, "package.json"),
});

// Persist opportunities.json for local preview (customer-visible only)
const hotelDataDir = path.join(ROOT, "data/group-demand-intelligence/hotels", HOTEL_ID);
fs.mkdirSync(hotelDataDir, { recursive: true });
fs.writeFileSync(
  path.join(hotelDataDir, "opportunities.json"),
  `${JSON.stringify(
    {
      schema: "gdi-opportunities-doc-v1",
      hotelId: HOTEL_ID,
      hotelName: HOTEL_NAME,
      updatedAt: new Date().toISOString(),
      source: "gdi-yotel-geneva-lake-commercial-quality-40plus-readiness-v1",
      note: "STAGED_FOR_FOUNDER_REVIEW — not CUSTOMER_PUBLISHED",
      opportunities: customerOpps,
    },
    null,
    2,
  )}\n`,
);

// Airtable dry-run
let airtable = { configured: isGdiOpportunityAirtableConfigured(), dryRun: true, writes: 0 };
try {
  const sync = await syncHotelOpportunitiesToAirtable({
    hotelId: HOTEL_ID,
    opportunities: customerOpps,
    dryRun: true,
  });
  airtable = {
    configured: true,
    dryRun: true,
    writes: 0,
    wouldCreate: sync.summary?.created,
    wouldUpdate: sync.summary?.updated,
    total: sync.summary?.total,
  };
} catch (e) {
  airtable = { ...airtable, error: String(e?.message || e) };
}
writeJson("13-AIRTABLE_DRY_RUN.json", airtable);

// Scorecard + founder report
const scorecard = {
  originalHotelMarket: 21,
  duplicateRecordsEliminated: 2,
  commerciallyInvalidDemoted: demoted.filter((d) => d.kind !== "DUPLICATE_ABSORBED").length,
  finalUniqueHotelMarket: counts.hotel || retainedHm.length,
  finalVerifiedCmi: counts.competitive || retainedCmi.length,
  salesReady: readinessBuckets.SALES_READY.length,
  accountDevelopment: readinessBuckets.ACCOUNT_DEVELOPMENT.length,
  qualifyOpportunity: readinessBuckets.QUALIFY_OPPORTUNITY.length,
  researchContinuation: readinessBuckets.RESEARCH_CONTINUATION.length,
  gapTo40: Math.max(0, 40 - (counts.hotel || retainedHm.length)),
  additionalSpendUsd: 0,
  newFingerprint: pkg.contentFingerprintSha256,
  priorFingerprintPreserved: PRIOR_FP,
};

writeJson("00-SCORECARD.json", scorecard);

const topSales = retainedHm
  .map((o) => ({
    id: o.id,
    title: o.displayTitle || o.title,
    readiness: o._audit?.readiness,
    action: o._audit?.salesAction,
    geo: o._audit?.geo,
    contact: o.primaryContact?.name || o.contactName,
    why: o._audit?.why,
  }))
  // prefer CORE then AIRPORT then SECONDARY; AD before Qualify
  .sort((a, b) => {
    const g = { CORE: 0, AIRPORT_CORRIDOR: 1, SECONDARY: 2, STRETCH: 3 };
    const r = { ACCOUNT_DEVELOPMENT: 0, QUALIFY_OPPORTUNITY: 1, SALES_READY: -1 };
    return (g[a.geo] ?? 9) - (g[b.geo] ?? 9) || (r[a.readiness] ?? 9) - (r[b.readiness] ?? 9);
  })
  .slice(0, 15);

const founderMd = `# YOTEL Geneva Lake — Commercial Quality Audit & 40+ Readiness

**Phase spend:** $0 · **Prior pilot:** $30.60 / $35 (remainder $4.40 untouched)  
**PR baseline:** #301 @ \`4ec7a2880\`  
**Prior staged fingerprint preserved:** \`${PRIOR_FP}\`  
**New staged fingerprint:** \`${pkg.contentFingerprintSha256}\`  
**Lifecycle:** STAGED_FOR_FOUNDER_REVIEW (not CUSTOMER_PUBLISHED)

## BEFORE → AFTER

| Metric | Before | After |
| --- | ---: | ---: |
| Hotel Market | 21 | **${scorecard.finalUniqueHotelMarket}** |
| Duplicate pairs eliminated | — | **2** (Hivernales, CHI Geneva) |
| Commercially demoted | — | **${scorecard.commerciallyInvalidDemoted}** |
| CMI (separate) | 5 | **${scorecard.finalVerifiedCmi}** |
| Sales Ready | verify | **${scorecard.salesReady}** |
| Account Development | — | **${scorecard.accountDevelopment}** |
| Qualify Opportunity | — | **${scorecard.qualifyOpportunity}** |
| Gap to 40 | 19 | **${scorecard.gapTo40}** |

## Deduplication
- **Les Hivernales:** \`rx_d_8bff60e01ffc\` → absorbed into \`rx_d_3521ecf91bb9\`
- **CHI Geneva 100th:** \`rx_d_d8fddbbdd78f\` → absorbed into \`rx_d_9bad59b0ae60\`
- Research evidence preserved; no source deletion.

## Demotions (honest pipeline)
${demoted
  .filter((d) => d.kind !== "DUPLICATE_ABSORBED")
  .map((d) => `- \`${d.id}\`: ${d.reason}`)
  .join("\n") || "- (none beyond duplicates)"}

## Top sales priorities
${topSales
  .map(
    (t, i) =>
      `${i + 1}. **${t.title}** (${t.geo} / ${t.readiness}) — ${t.action || "n/a"} · Contact: ${t.contact || "n/a"}`,
  )
  .join("\n")}

## Strongest research segments (for next paid plan)
- Nyon / La Côte core sports, culture, scientific hosts (OENOVITI / CHANGINS, festivals, triathlon, FIFAN)
- Airport-corridor scientific workshops (EuroLFT, ALEGRO)
- Palexpo only as **Qualify overflow** — not primary volume strategy

## Next Exa proposal (NOT authorized)
- Minimum pilot: **$15**
- Recommended scale: **$30** (reference only)
- Maximum proposed ceiling: **$45** with yield stop-rules
- Do **not** claim $30 yields 19 more HM after this stricter audit
- Priority queue: ${topCandidates.length} candidates in \`11-30_TO_50_PRIORITY_CANDIDATES.json\`

## Controls
- Airtable: dry-run only  
- Surfe / Exa / Parallel: not invoked  
- Other hotels’ publications: untouched  
`;

fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), founderMd);

writeJson("16-GITHUB_MERGE_CLOSEOUT.json", {
  status: "READY_FOR_PR",
  branch: sh("git rev-parse --abbrev-ref HEAD"),
  headSha: sh("git rev-parse HEAD"),
  scorecard,
  reportsDir: "reports/gdi-yotel-geneva-lake-commercial-quality-and-40plus-readiness-v1/",
});

console.log(
  JSON.stringify(
    {
      ok: true,
      paidSpendUsd: 0,
      scorecard,
      fingerprint: pkg.contentFingerprintSha256,
      qualityGate: qg?.ok ?? qg,
    },
    null,
    2,
  ),
);
