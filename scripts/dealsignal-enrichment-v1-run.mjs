#!/usr/bin/env node
/**
 * DealSignal Enrichment V1 — deepen + Exa-selective + persist enrichment findings.
 * Ceiling: $5 Exa Search Lite. CUSTOMER_BLOCKED.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import { createExaContractTimingClient } from "../lib/hotel-intelligence/sec-edgar/exa-contract-timing-client-v1.js";
import { createContractTimingV2Budget, PROVIDER_UNIT_COST_USD } from "../lib/hotel-intelligence/sec-edgar/contract-timing-v2-budget-v1.js";
import {
  GRAPH_BASE_ID_CANONICAL,
  TABLE_HI_DISCOVERY_FINDINGS,
  MAP_DISCOVERY_FINDINGS,
} from "../lib/hotel-intelligence/sec-edgar/discovery-inventory-airtable-contract-v23.js";
import {
  buildCommercialFindingFields,
  classifyWriteCategory,
  WRITE_CATEGORY,
} from "../lib/hotel-intelligence/sec-edgar/hif-foundation-persistence-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) {
    dotenv.config({ path: p, override: false });
    break;
  }
}

const OUT = path.join(ROOT, "reports/dealsignal-enrichment-v1");
fs.mkdirSync(OUT, { recursive: true });
const APPLY = process.argv.includes("--apply");
const CEILING = 5.0;
const SAFETY = 4.5;

const baseline = JSON.parse(fs.readFileSync(path.join(OUT, "BASELINE_AIRTABLE_AUDIT.json"), "utf8"));
const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
const ids = JSON.parse(
  fs.readFileSync(path.join(ROOT, "lib/hotel-intelligence/sec-edgar/discovery-inventory-table-ids.generated.json"), "utf8"),
);
const findingsTableId = ids.tableIds[TABLE_HI_DISCOVERY_FINDINGS];

const ledgerPath = path.join(OUT, "EXA_BUDGET_LEDGER.json");
const budget = createContractTimingV2Budget({
  ceilingUsd: CEILING,
  ledgerPath,
  cwd: ROOT,
});
// Force ceiling to wave-specific $5 (constructor may load old ledger)
budget.state && (budget.state.ceilingUsd = CEILING);

const exa = createExaContractTimingClient({
  budget: {
    canSpend: (usd) => {
      if (budget.spentUsd() + usd > SAFETY) {
        return { ok: false, error: "DEALSIGNAL_SAFETY_MARGIN", remaining: Math.max(0, SAFETY - budget.spentUsd()) };
      }
      return budget.canSpend(usd);
    },
    fingerprint: (provider, operation, payload) => budget.fingerprint(provider, operation, payload),
    record: (meta) => budget.record(meta),
  },
});

/** Free-research dossiers compiled from CB5 + official sources (no Exa). */
const freeDeepen = [
  {
    id: "lakeville-inn-mn-bct",
    findingId: "find_cb1_lakeville_bct_brand_search_2026",
    discoveryPropertyId: "disc_cb1_lakeville_inn_mn_2026",
    hotelName: "Lakeville Inn",
    country: "US",
    type: "BCT",
    originalStatus: "VERIFIED",
    commercialStatus: "VERIFIED",
    researchMaturity: "VETTED",
    thesis:
      "Independent Lakeville Inn (ex-Hampton) under Elevate/JAMP ownership publicly targeting a new brand agreement by end-2026.",
    ownership: {
      confirmed: ["Elevate Hospitality LLC", "JAMP Hotels", "Jaymin Patel (named in local coverage)"],
      engagementRoute: "Owner/operator — local hospitality ownership",
      gaps: ["Ultimate beneficial ownership chain beyond Elevate/JAMP not fully mapped"],
    },
    decisionMakers: [
      {
        name: "Jaymin Patel",
        role: "Owner representative / Elevate Hospitality (per Star Tribune / chamber coverage)",
        verification: "SECONDARY_PRESS",
      },
    ],
    timing: { window: "By end-2026 (owner-stated goal)", exactDeadline: null },
    rooms: null,
    hotels: 1,
    freeSources: [
      {
        url: "https://www.startribune.com/hotel-sold-at-discount-lost-hampton-inn-flag-during-ice-surge/601664022",
        publisher: "Star Tribune",
        date: "2026-04-09",
        type: "SECONDARY_PRESS",
        claim: "Asset sold after Hilton flag loss; new ownership exploring brand options with year-end goal",
      },
      {
        url: "https://www.bizjournals.com/twincities/news/2026/04/07/lakeville-inn-jamp-hotels-sale.html",
        publisher: "Minneapolis / St. Paul Business Journal",
        date: "2026-04-07",
        type: "SECONDARY_PRESS",
        claim: "Lakeville Inn sold for $8.5M to JAMP Hotels after losing Hampton banner",
      },
    ],
    gaps: [
      "Exact franchise negotiation status as of Nov 2026",
      "Named brand shortlist",
      "Direct contact email",
      "Whether post-sale brand search remains active (no public completion found)",
    ],
    recommendedAction:
      "Approach Elevate/JAMP ownership with soft-brand / select-service franchise options before year-end 2026 goal; confirm brand status remains open.",
    moreUseful: true,
    usefulnessNote:
      "Ownership change is context, not closure — BCT remains prospective under Elevate/JAMP; sale titles are not contradictory completion evidence.",
  },
  {
    id: "shorebreak-huntington-bct",
    findingId: "find_cb2b_shorebreak_huntington_bct_2026",
    discoveryPropertyId: "disc_cb2b_shorebreak_huntington_2026",
    hotelName: "Kimpton Shorebreak Huntington Beach Resort",
    country: "US",
    type: "BCT",
    originalStatus: "VERIFIED",
    commercialStatus: "VERIFIED",
    researchMaturity: "VETTED",
    thesis:
      "DRH publicly evaluating brand vs independent for Shorebreak after Kimpton agreement expired to month-to-month.",
    ownership: {
      confirmed: ["DiamondRock Hospitality Company"],
      engagementRoute: "DRH asset management / brand development",
      gaps: ["Exact Kimpton MTM fee terms"],
    },
    decisionMakers: [
      { name: "Jeffrey Donnelly", role: "CEO, DiamondRock", verification: "PRIMARY_EARNINGS" },
      { name: "Justin Leonard", role: "DRH executive (earnings commentary)", verification: "PRIMARY_EARNINGS" },
    ],
    timing: { window: "Near-term / active MTM", exactDeadline: null },
    rooms: null,
    hotels: 1,
    freeSources: [
      {
        url: "https://www.fool.com/earnings/call-transcripts/2026/11/04/diamondrock-drh-q3-2026-earnings-call-transcript/",
        publisher: "Motley Fool / DRH Q3 2026",
        date: "2026-11-04",
        type: "PRIMARY_EARNINGS",
        claim: "Shorebreak expired / MTM; multi-path brand evaluation continues",
      },
    ],
    gaps: ["Whether a brand RFP is formally underway", "Independent conversion CapEx plan"],
    recommendedAction: "Engage DRH asset management on Shorebreak brand alternatives (soft brand / independent economics).",
    moreUseful: true,
    usefulnessNote: "Named REIT executives + fresh Q3 2026 confirmation of open multi-path decision.",
  },
  {
    id: "courtyard-denver-bct",
    findingId: "find_cb2b_courtyard_denver_bct_2026",
    discoveryPropertyId: "disc_cb2b_courtyard_denver_downtown_2026",
    hotelName: "Courtyard Denver Downtown",
    country: "US",
    type: "BCT",
    originalStatus: "VERIFIED",
    commercialStatus: "VERIFIED",
    researchMaturity: "VETTED",
    thesis: "Franchise expires 2027; DRH evaluating renew / upbrand / independent / expand / sell.",
    ownership: {
      confirmed: ["DiamondRock Hospitality Company"],
      engagementRoute: "DRH asset management",
      gaps: ["Operator (Sage) change not indicated as open"],
    },
    decisionMakers: [{ name: "Jeffrey Donnelly", role: "CEO, DiamondRock", verification: "PRIMARY_EARNINGS" }],
    timing: { window: "Franchise expiry 2027", exactDeadline: "2027 (year)" },
    rooms: 177,
    hotels: 1,
    freeSources: [
      {
        url: "https://www.fool.com/earnings/call-transcripts/2026/11/04/diamondrock-drh-q3-2026-earnings-call-transcript/",
        publisher: "DRH Q3 2026",
        date: "2026-11-04",
        type: "PRIMARY_EARNINGS",
        claim: "Courtyard Denver named as near-term multi-path franchise decision",
      },
    ],
    gaps: ["Exact franchise expiration day within 2027", "Formal brand RFP status"],
    recommendedAction: "Monitor DRH 2027 guidance; engage on upbrand / soft-brand paths before expiry.",
    moreUseful: true,
    usefulnessNote: "Room count + multi-path language + named owner executives.",
  },
  {
    id: "hilton-trinidad-mct",
    findingId: "find_cfa_hilton_trinidad_mct_hma_sale_2026",
    discoveryPropertyId: "disc_cfa_hilton_trinidad_pos_2026",
    hotelName: "Hilton Trinidad & Conference Centre",
    country: "TT",
    type: "MCT",
    originalStatus: "VERIFIED",
    commercialStatus: "VERIFIED",
    researchMaturity: "VETTED",
    thesis:
      "State-linked ownership restructuring with short-term Hilton HMA; EOI for HotelTT acquisition; operator talks expected after buyer process.",
    ownership: {
      confirmed: ["eTecK", "HotelTT Asset Management Company", "Government of Trinidad and Tobago"],
      engagementRoute: "Public EOI / government asset management",
      gaps: ["Buyer identity if EOI awarded", "Exact HMA start/end day beyond reported structure"],
    },
    decisionMakers: [
      {
        name: "Government / eTecK leadership (ministerial statements)",
        role: "Public-sector decision authority",
        verification: "SECONDARY_PRESS",
      },
    ],
    timing: { window: "EOI submissions reported due 2027-01-29; HMA short-term", exactDeadline: "2027-01-29 (EOI, press)" },
    rooms: null,
    hotels: 1,
    freeSources: [
      {
        url: "https://eteck.co.tt/index.php/2026/10/30/request-for-expressions-of-interest-for-the-acquisition-of-the-shares-and-assets-of-the-hoteltt-asset-management-company-limited/",
        publisher: "eTecK",
        date: "2026-10-30",
        type: "PRIMARY_OWNER",
        claim: "Official RF EOI for acquisition of HotelTT Asset Management Company shares and assets",
      },
      {
        url: "https://www.ttt.live/government-finalises-new-ownership-and-management-structure-for-hilton-trinidad/",
        publisher: "TTT Live",
        date: "2026-09",
        type: "SECONDARY_PRESS",
        claim: "New ownership/management structure finalized; HMA with Hilton",
      },
      {
        url: "https://www.guardian.co.tt/business/hilton-talks-advanced-says-minister-6.2.2641489.4eb302edc9",
        publisher: "Trinidad Guardian",
        date: "2026-11-17",
        type: "SECONDARY_PRESS",
        claim: "Minister states Hilton talks advanced — does not alone close MCT",
      },
    ],
    gaps: [
      "Full EOI PDF contents (landing page confirms RF EOI; PDF link present on eTecK page)",
      "Named commercial contact beyond info@eteck.co.tt / switchboard",
      "Whether Hilton HMA renews with buyer or new operator is selected",
    ],
    recommendedAction:
      "Track HotelTT EOI outcome via eTecK; prepare operator proposals contingent on buyer selection; engage public-sector pathway.",
    moreUseful: true,
    usefulnessNote:
      "Primary eTecK EOI landing page retrieved; Guardian coverage that Hilton talks advanced is secondary timing context — opportunity remains prospective.",
  },
  {
    id: "embassy-one-bengaluru-mct",
    findingId: "find_cb3_embassy_one_bengaluru_mct_2026",
    discoveryPropertyId: "disc_cb3_embassy_one_bengaluru_2026",
    hotelName: "Four Seasons Hotel Bengaluru at Embassy ONE",
    country: "IN",
    type: "MCT",
    originalStatus: "VERIFIED",
    commercialStatus: "VERIFIED",
    researchMaturity: "VETTED",
    thesis: "Embassy REIT ending Four Seasons management 2027-02-28 and evaluating/soliciting replacement operators.",
    ownership: {
      confirmed: ["Embassy Office Parks REIT / Embassy Group"],
      engagementRoute: "Embassy REIT asset management / IR",
      gaps: ["Named hospitality asset manager contact"],
    },
    decisionMakers: [
      {
        name: "Embassy REIT management (earnings disclosures)",
        role: "Owner decision authority",
        verification: "PRIMARY_OWNER",
      },
    ],
    timing: { window: "Management ends 2027-02-28", exactDeadline: "2027-02-28" },
    rooms: null,
    hotels: 1,
    freeSources: [
      {
        url: "https://www.embassyofficeparks.com/news-media/embassy-reit-reports-17-yoy-growth-in-revenue-and-net-operating-income-leases-1-3-million-square-feet-in-q1-fy2027/",
        publisher: "Embassy REIT",
        date: "2026-07-30",
        type: "PRIMARY_OWNER",
        claim: "Evaluating/soliciting new operators after FS exit",
      },
      {
        url: "https://press.fourseasons.com/news-releases/2026/bengaluru/",
        publisher: "Four Seasons",
        date: "2026-07-30",
        type: "PRIMARY_OPERATOR",
        claim: "Management end timing aligned with owner disclosure",
      },
    ],
    gaps: ["Operator shortlist", "Whether brand will remain Four Seasons under new manager"],
    recommendedAction: "Engage Embassy REIT hospitality asset management with operator proposals before Feb 2027 exit.",
    moreUseful: true,
    usefulnessNote: "Primary owner + operator press corroboration; hard exit date.",
  },
  {
    id: "ahip-aimbridge-mct",
    findingId: "find_cb4_ahip_aimbridge_mct_2026",
    discoveryPropertyId: "disc_cb4_ahip_remaining_portfolio_mct_2026",
    hotelName: "AHIP Remaining Premium Branded Portfolio",
    country: "US",
    type: "MCT",
    originalStatus: "VERIFIED",
    commercialStatus: "VERIFIED",
    researchMaturity: "VETTED",
    thesis: "Portfolio-level Aimbridge master HMA terminates 2027-04-30; AHIP must retain replacement manager; no announcement yet.",
    ownership: {
      confirmed: ["American Hotel Income Properties REIT LP"],
      engagementRoute: "info@ahipreit.com / Vancouver HQ / Baird Strategic Review context",
      gaps: ["Which of 9 PSA hotels close before exit (affects retained set)"],
    },
    decisionMakers: [
      { name: "John O'Neill", role: "CEO", verification: "PRIMARY_OWNER" },
      { name: "Bruce Pittet", role: "COO / SVP Asset Management", verification: "PRIMARY_OWNER" },
      { name: "Travis Beatty", role: "CFO", verification: "PRIMARY_OWNER" },
      { name: "Robert W. Baird & Co.", role: "Strategic Review adviser", verification: "PRIMARY_OWNER" },
    ],
    timing: { window: "Replacement needed by 2027-04-30", exactDeadline: "2027-04-30" },
    rooms: 2018,
    hotels: 19,
    freeSources: [
      {
        url: "https://www.globenewswire.com/news-release/2026/04/20/3276955/0/en/american-hotel-income-properties-reit-lp-announces-settlement-with-its-hotel-management-company.html",
        publisher: "GlobeNewswire / AHIP",
        date: "2026-04-20",
        type: "PRIMARY_OWNER",
        claim: "Settlement sets Aimbridge end date",
      },
      {
        url: "https://www.ahipreit.com/management-team",
        publisher: "AHIP",
        date: "2026-11-21",
        type: "PRIMARY_OWNER",
        claim: "Named executives O'Neill / Beatty / Pittet",
      },
      {
        url: "reports/sec-edgar-hif-v6-track-a-cb5-trigger-first-portfolio/AHIP_PORTFOLIO_MCT_DOSSIER.json",
        publisher: "Dealality CB5 dossier",
        date: "2026-11-21",
        type: "INTERNAL_SYNTHESIS",
        claim: "19 hotels / 2,018 rooms affected inventory",
      },
    ],
    gaps: ["Replacement manager identity", "Whether one manager for all remaining hotels"],
    recommendedAction: "Approach AHIP COO/CEO with portfolio management proposals ahead of Apr 2027 Aimbridge exit.",
    moreUseful: true,
    usefulnessNote: "Named executives + verified room/hotel counts + engagement email — one parent MCT.",
  },
];

const emergingFocus = (baseline.topEmergingCandidates || [])
  .filter((c) => !/PREFERRED_BRAND|IC#544|COMMITTED|AWARD_PENDING|LEGACY/i.test(String(c.valueObserved) + String(c.notes)))
  .slice(0, 18)
  .map((c) => ({
    findingId: c.findingId,
    airtableId: c.airtableId,
    fieldName: c.fieldName,
    sourceUrl: c.sourceUrl,
    snippet: c.valueObserved,
    notes: c.notes,
    rank: c.rank,
    researchMaturity: "INVESTIGATED",
    commercialStatus: /EMERGING|EVIDENCE_SUPPORTED_POTENTIAL|PRECURSOR/i.test(c.valueObserved)
      ? "EMERGING"
      : /VERIFIED_RELEVANT/i.test(c.valueObserved)
        ? "EMERGING"
        : "WATCHLIST",
    promotion: false,
    promotionReason: "No new affirmative prospective-decision evidence in this wave beyond existing classification",
  }));

/** Exa queries — targeted, capped */
const exaQueries = [
  {
    oppId: "lakeville-inn-mn-bct",
    query: "Lakeville Inn Elevate Hospitality JAMP Hotels brand franchise 2026 OR 2027",
    purpose: "status_update_ownership",
  },
  {
    oppId: "shorebreak-huntington-bct",
    query: "Kimpton Shorebreak Huntington Beach DiamondRock brand independent 2026 2027",
    purpose: "status_update",
  },
  {
    oppId: "courtyard-denver-bct",
    query: "Courtyard Denver Downtown DiamondRock franchise expiration brand 2027",
    purpose: "status_update",
  },
  {
    oppId: "hilton-trinidad-mct",
    query: "Hilton Trinidad HotelTT EOI eTecK management operator 2026 2027",
    purpose: "timing_ownership",
  },
  {
    oppId: "embassy-one-bengaluru-mct",
    query: "Embassy ONE Bengaluru Four Seasons operator replacement Embassy REIT 2026 2027",
    purpose: "status_update",
  },
  {
    oppId: "ahip-aimbridge-mct",
    query: "American Hotel Income Properties Aimbridge replacement manager OR new manager 2026 2027",
    purpose: "replacement_announcement_check",
  },
  {
    oppId: "den-westin-emerging",
    query: "Westin Denver International Airport hotel management RFP OR operator selection 2026 2027",
    purpose: "emerging_status",
  },
  {
    oppId: "hotel-murta-emerging",
    query: "Hotel Murta Xàtiva concesión OR licitación 2026 2027",
    purpose: "emerging_status",
  },
  {
    oppId: "new-discovery-trigger",
    query: "hotel management agreement termination replacement manager REIT earnings 2026 OR 2027",
    purpose: "new_discovery",
  },
  {
    oppId: "new-discovery-bct",
    query: "franchise nonrenewal OR brand conversion OR \"evaluating brands\" hotel owner 2026 2027",
    purpose: "new_discovery",
  },
];

const exaResults = [];
let spent = 0;

for (const q of exaQueries) {
  if (budget.spentUsd() >= SAFETY) {
    exaResults.push({ ...q, skipped: true, reason: "SAFETY_MARGIN" });
    continue;
  }
  const res = await exa.search({
    query: q.query,
    type: "lite",
    numResults: 5,
    startPublishedDate: "2025-01-01",
  });
  // Client already records EXA spend via budget.record — do not double-count here
  const cost = res.fromCache ? 0 : res.costUsd ?? PROVIDER_UNIT_COST_USD.EXA_SEARCH_LITE;
  spent = budget.spentUsd();
  exaResults.push({
    ...q,
    ok: res.ok,
    fromCache: Boolean(res.fromCache),
    error: res.error || null,
    costUsd: cost,
    results: (res.results || res.json?.results || [])
      .slice(0, 5)
      .map((r) => ({
        title: r.title || null,
        url: r.url || null,
        publishedDate: r.publishedDate || null,
        score: r.score || null,
      })),
  });
  await new Promise((r) => setTimeout(r, 300));
}

/** Interpret Exa for contradictions / new sources */
const exaInsights = [];
for (const r of exaResults) {
  if (!r.ok || r.skipped) continue;
  const urls = (r.results || []).map((x) => x.url).filter(Boolean);
  const titles = (r.results || []).map((x) => x.title).filter(Boolean);
  let insight = {
    oppId: r.oppId,
    newUrls: urls,
    titles,
    contradiction: false,
    replacementAnnounced: false,
    note: null,
  };
  const blob = (titles.join(" ") + " " + urls.join(" ")).toLowerCase();
  if (r.oppId === "ahip-aimbridge-mct") {
    insight.replacementAnnounced = /replacement manager named|appoints .* manager|selects .* hospitality/i.test(blob);
    insight.note = insight.replacementAnnounced
      ? "Possible replacement announcement — inspect primary source before status change"
      : "No clear public replacement-manager announcement in Exa lite results";
  }
  if (/renews franchise|retains brand|signs new management|awarded management/i.test(blob) && /shorebreak|courtyard denver|lakeville|embassy one/i.test(blob)) {
    insight.contradiction = true;
    insight.note = "Possible completion language in titles — requires primary-source inspection";
  }
  exaInsights.push(insight);
}

/** Cohort C — curated discoveries (primary-inspected where possible; no manufactured quota) */
const newDiscoveries = [
  {
    title: "Braemar Hotels & Resorts — self-managed REIT / Remington master HMA terminated",
    url: "https://www.hotelmanagement.net/business-strategy/braemar-transitions-self-managed-reit",
    publishedDate: "2026-11-11",
    classification: "CLOSED",
    reason:
      "Primary-adjacent trade press confirms advisory + Remington master HMA terminated; Stonebridge (Sofitel Chicago Mag Mile) and Avion (Cameo Beverly Hills) already appointed for two assets. Not an open prospective MCT — preserve as historical/closed intelligence.",
    researchMaturity: "INVESTIGATED",
    opportunityType: "MCT",
    country: "US",
    hotels: 2,
    namedParties: ["Braemar Hotels & Resorts", "Richard Stockton (CEO)", "Remington Hospitality (exited)", "Stonebridge Companies", "Avion Hospitality"],
    recommendedAction: "Do not pursue as open MCT; monitor residual third-party operator needs if any remaining assets seek managers.",
  },
  {
    title: "Hotel Murta (Xàtiva) — fourth operating concession licitation forthcoming",
    url: "https://www.levante-emv.com/costera/2026/11/06/tercer-reabrir-hotel-murta-xativa-136115285.html",
    publishedDate: "2026-11-06",
    classification: "EMERGING",
    reason:
      "Exa + existing Airtable adjacent signal corroborated: third 2026 tender deserted; Ayuntamiento intends fourth licitation ASAP for 20-year hotel+restaurant exploitation. Strengthens existing emerging concession thesis; not a first-100 BCT/MCT promotion.",
    researchMaturity: "INVESTIGATED",
    opportunityType: "CONCESSION_ADJACENT",
    country: "ES",
    parentFindingId: "find_cb2b_hotel_murta_concession_2026",
    recommendedAction: "Monitor Ayuntamiento Xàtiva / Hellotender for fourth licitation publication; treat as emerging adjacent operating opportunity.",
  },
];
// Retain only non-guide, non-duplicate Exa hits as WATCHLIST research leads
for (const r of exaResults.filter((x) => x.purpose === "new_discovery" && x.ok)) {
  for (const hit of r.results || []) {
    if (!hit.url) continue;
    const t = hit.title || "";
    if (
      /how to choose|brand conversion guide|hma expiry into owner|embassy reit|american hotel income|apple hospitality|rlj lodging|braemar|westin boston|custer state|christie|marvelbros|hospitality net\/opinion/i.test(
        t + " " + hit.url,
      )
    ) {
      continue;
    }
    newDiscoveries.push({
      title: hit.title,
      url: hit.url,
      publishedDate: hit.publishedDate,
      classification: "WATCHLIST",
      reason: "Trigger-search hit — not yet independently verified as prospective open BCT/MCT",
      researchMaturity: "IDENTIFIED",
    });
  }
}

const report = {
  asOf: new Date().toISOString(),
  publicationState: "CUSTOMER_BLOCKED",
  wave: "DEALSIGNAL_ENRICHMENT_V1",
  exa: {
    ceilingUsd: CEILING,
    safetyUsd: SAFETY,
    spentUsd: budget.spentUsd(),
    unitCostSearchLite: PROVIDER_UNIT_COST_USD.EXA_SEARCH_LITE,
    queriesAttempted: exaQueries.length,
    results: exaResults,
    insights: exaInsights,
  },
  cohortA: freeDeepen,
  cohortB: emergingFocus,
  cohortC: newDiscoveries.slice(0, 15),
  metrics: {
    existingDeepened: freeDeepen.length,
    emergingInvestigated: emergingFocus.length,
    newCredibleOpportunitiesVerified: newDiscoveries.filter((d) => d.classification === "VERIFIED").length,
    newEmergingSignals: newDiscoveries.filter((d) => d.classification === "EMERGING").length,
    newClosedHistorical: newDiscoveries.filter((d) => d.classification === "CLOSED").length,
    newWatchlistSignals: newDiscoveries.filter((d) => d.classification === "WATCHLIST").length,
    promotions: 0,
    withNewOwnershipIntelligence: freeDeepen.filter((d) => d.ownership?.confirmed?.length).length,
    withNamedExecutives: freeDeepen.filter((d) => d.decisionMakers?.length).length,
    withImprovedTiming: freeDeepen.filter((d) => d.timing?.window).length,
    contradictoryEvidenceCases: exaInsights.filter((i) => i.contradiction || i.replacementAnnounced).length,
    additionalUniqueSources: freeDeepen.reduce((n, d) => n + (d.freeSources?.length || 0), 0) + exaResults.reduce((n, r) => n + (r.results?.length || 0), 0),
  },
};

fs.writeFileSync(path.join(OUT, "ENRICHMENT_RESEARCH_PACKAGE.json"), JSON.stringify(report, null, 2));

/** Persist enrichment as discrete OTHER findings (idempotent) — ownership/decision-maker notes */
async function findByFormula(formula) {
  const qs = new URLSearchParams({ filterByFormula: formula, maxRecords: "1" });
  const res = await fetch(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${findingsTableId}?${qs}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const json = await res.json();
  if (!res.ok) throw new Error(JSON.stringify(json));
  return json.records?.[0] || null;
}

async function createFinding(fields) {
  const res = await fetch(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${findingsTableId}`, {
    method: "POST",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify({ fields, typecast: true }),
  });
  return { ok: res.ok, json: await res.json() };
}

const persistLedger = {
  asOf: new Date().toISOString(),
  mode: APPLY ? "APPLY" : "DRY_RUN",
  publicationState: "CUSTOMER_BLOCKED",
  operations: [],
  summary: { creates: 0, noops: 0, errors: 0 },
  readBack: [],
  errors: [],
};

const enrichPersists = freeDeepen.map((d) => ({
  findingId: `find_ds_enrich_v1_${d.id.replace(/[^a-z0-9]+/gi, "_").toLowerCase()}_ownership_2026`,
  discoveryPropertyId: d.discoveryPropertyId,
  parentFindingId: d.findingId,
  hotelName: d.hotelName,
  commercialCategory: d.type === "BCT" ? "BRAND_CONVERSION" : "MANAGEMENT",
  commercialTrigger: `DealSignal Enrichment V1 — ownership/decision-maker/timing package for ${d.hotelName}`,
  decisionEvidence: [
    `Owners: ${(d.ownership?.confirmed || []).join("; ")}`,
    `Decision-makers: ${(d.decisionMakers || []).map((m) => `${m.name} (${m.role})`).join("; ")}`,
    `Timing: ${d.timing?.window || "unknown"}`,
    `Gaps: ${(d.gaps || []).join("; ")}`,
    `Recommended: ${d.recommendedAction}`,
  ].join(" | "),
  commercialRelevance: "VERIFIED_RELEVANT",
  commercialTiming: d.timing?.window || null,
  commercialAccessibility: "ACCESSIBLE_UNKNOWN",
  evidencePathway: "OWNER_DISCLOSURE_AND_PRESS",
  engagementRoute: d.ownership?.engagementRoute || null,
  decisionParty: (d.ownership?.confirmed || [])[0] || null,
  sourceUrl: d.freeSources?.[0]?.url || null,
  sources: d.freeSources,
  priority: "PRIORITY_B",
  countsTowardFirst100: false,
  notes: `CUSTOMER_BLOCKED | DEALSIGNAL_ENRICHMENT_V1 | researchMaturity=${d.researchMaturity} | commercialStatus=${d.commercialStatus} | parent=${d.findingId} | hotels=${d.hotels} | rooms=${d.rooms ?? "UNK"}`,
}));

// Also persist 2 strongest emerging status-check enrichment findings if Exa returned URLs
for (const e of emergingFocus.slice(0, 5)) {
  const hit = exaInsights.find((i) => e.findingId?.includes(i.oppId?.split("-")[0] || "___")) || null;
  enrichPersists.push({
    findingId: `find_ds_enrich_v1_emerging_${String(e.findingId || e.airtableId).replace(/[^a-z0-9]+/gi, "_").toLowerCase()}_status_2026`,
    discoveryPropertyId: null,
    parentFindingId: e.findingId,
    hotelName: e.findingId,
    commercialCategory: "OTHER",
    commercialTrigger: `DealSignal Enrichment V1 — emerging candidate status review: ${e.findingId}`,
    decisionEvidence: `Preserved classification ${e.commercialStatus}; promotion=${e.promotion}; ${e.promotionReason}`,
    commercialRelevance: "EVIDENCE_SUPPORTED_POTENTIAL",
    sourceUrl: e.sourceUrl,
    priority: "PRIORITY_B",
    countsTowardFirst100: false,
    notes: `CUSTOMER_BLOCKED | DEALSIGNAL_ENRICHMENT_V1 | EMERGING_STATUS_CHECK | parent=${e.findingId}`,
    skipPersist: true, // no discoveryPropertyId — research package only
  });
}

const persistable = enrichPersists.filter((p) => p.discoveryPropertyId && !p.skipPersist);

for (const p of persistable) {
  const existing = await findByFormula(`{${MAP_DISCOVERY_FINDINGS.findingId}}="${p.findingId}"`);
  const payload = {
    findingId: p.findingId,
    id: p.findingId,
    commercialCategory: p.commercialCategory,
    commercialThesis: p.commercialTrigger,
    commercialTrigger: p.commercialTrigger,
    commercialRelevance: p.commercialRelevance,
    commercialTiming: p.commercialTiming,
    commercialAccessibility: p.commercialAccessibility || "ACCESSIBLE_UNKNOWN",
    evidencePathway: p.evidencePathway || "ENRICHMENT",
    engagementRoute: p.engagementRoute,
    decisionParty: p.decisionParty,
    decisionEvidence: p.decisionEvidence,
    sourceUrl: p.sourceUrl,
    sources: p.sources,
    priority: p.priority,
    countsTowardFirst100: false,
    evidenceConfidence: "HIGH",
    verificationStatus: "EVIDENCE_SUPPORTED",
    verified: true,
    evidenceSupported: true,
    destination: TABLE_HI_DISCOVERY_FINDINGS,
    notes: p.notes,
    existingAirtableId: existing?.id,
    fieldsUnchanged: Boolean(existing),
  };
  const cls = classifyWriteCategory(payload);
  if (cls.category !== WRITE_CATEGORY.AUTO_PERSIST_ELIGIBLE) {
    persistLedger.operations.push({ id: p.findingId, action: "NOT_ELIGIBLE", reason: cls.reason });
    continue;
  }
  const buildEnrichmentFields = () => {
    const fields = buildCommercialFindingFields(
      { ...payload, commercialCategory: "OTHER" },
      null,
    );
    // Enrichment packages are OTHER findings — do not create duplicate principal BCT/MCT rows
    fields[MAP_DISCOVERY_FINDINGS.findingType] = "OTHER";
    fields[MAP_DISCOVERY_FINDINGS.fieldName] = "COMMERCIAL_SIGNAL_OTHER";
    return fields;
  };
  if (!APPLY) {
    persistLedger.operations.push({
      id: p.findingId,
      action: existing ? "NOOP_DRY" : "CREATE_DRY",
      fieldsPreview: buildEnrichmentFields(),
    });
    if (existing) persistLedger.summary.noops += 1;
    else persistLedger.summary.creates += 1;
    continue;
  }
  if (existing) {
    persistLedger.operations.push({ id: p.findingId, action: "NOOP", airtableId: existing.id });
    persistLedger.summary.noops += 1;
  } else {
    const fields = buildEnrichmentFields();
    const created = await createFinding(fields);
    if (!created.ok) {
      persistLedger.errors.push({ findingId: p.findingId, err: created.json });
      persistLedger.summary.errors += 1;
    } else {
      persistLedger.operations.push({ id: p.findingId, action: "CREATE", airtableId: created.json.id });
      persistLedger.summary.creates += 1;
      persistLedger.readBack.push({ findingId: p.findingId, airtableId: created.json.id });
      await new Promise((r) => setTimeout(r, 250));
    }
  }
}

fs.writeFileSync(
  path.join(OUT, APPLY ? "PERSISTENCE_LEDGER_APPLY.json" : "PERSISTENCE_LEDGER_DRY_RUN.json"),
  JSON.stringify(persistLedger, null, 2),
);

console.log(
  JSON.stringify(
    {
      exaSpentUsd: budget.spentUsd(),
      deepened: freeDeepen.length,
      emerging: emergingFocus.length,
      newWatchlistHits: newDiscoveries.length,
      persistMode: persistLedger.mode,
      persistSummary: persistLedger.summary,
      errors: persistLedger.errors.length,
    },
    null,
    2,
  ),
);
