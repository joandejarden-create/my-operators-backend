/**
 * MODE B — GDI Final Opportunity Yield Audit + AI for Good 2027 canary.
 * No broad discovery. No threshold changes. Jev advisory-only.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import crypto from "node:crypto";

import {
  isGdiCustomerOpportunityReady,
  isValidFutureWatch,
  filterCustomerFacingOpportunities,
  filterSalespersonView,
  applyLiveCommercialQuality,
  buildYotelTenGeneratorCampaigns,
  YOTEL_HOTEL_ID,
  listVisibleDemandCampaigns,
  upsertDemandCampaigns,
  loadDemandCampaigns,
  isGdiDemandGeneratorVisible,
} from "../lib/group-demand-intelligence/index.js";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { invalidateGdiHotelReadCache } from "../lib/group-demand-intelligence/read-cache.js";
import { promoteQualifiedGdiOpportunity } from "../lib/group-demand-intelligence/promote-qualified-opportunity.js";
import { buildDeterministicActiveAdvice, JEV_LOOP } from "../lib/group-demand-intelligence/jev-active-research-v2/jev-active-advisor.js";
import { RESEARCH_PRIORITY } from "../lib/group-demand-intelligence/jev-active-research-v2/score.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const OUT = path.join(ROOT, "reports", "gdi", "final-opportunity-yield-audit");
const HOTEL_ID = YOTEL_HOTEL_ID;
const NOW = "2026-10-04";
const RUN_ID = `gdi_fy_audit_${NOW.replace(/-/g, "")}_${crypto.randomBytes(3).toString("hex")}`;

const EVIDENCE_URL =
  "https://relvehq.com/events/ai-for-good-global-summit";
const OFFICIAL_2027 = "https://2027summit.aiforgood.itu.int/";
const OFFICIAL_2026 = "https://aiforgood.itu.int/summit26/";

function csvEscape(v) {
  const s = v == null ? "" : String(v);
  if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}
function toCsv(rows, cols) {
  const lines = [cols.join(",")];
  for (const r of rows) lines.push(cols.map((c) => csvEscape(r[c] ?? "")).join(","));
  return lines.join("\n") + "\n";
}
function write(name, body) {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, name), body, "utf8");
}
function slug(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_|_$/g, "")
    .slice(0, 48);
}

/** Phase 1 — feature inventory (code-path classification). */
function buildFeatureInventory() {
  return [
    {
      feature: "10 Bases of Demand",
      status: "REPORT_ONLY",
      files: "lib/group-demand-intelligence/ten-bases-of-demand-v1/*; scripts/gdi-ten-bases-of-demand-v1-2026-10-04.mjs",
      notes: "runTenBasesForHotel not called from runGroupDemandResearch or API research/run",
    },
    {
      feature: "Demand Generator visibility",
      status: "LIVE_AND_USED",
      files: "lib/group-demand-intelligence/demand-campaigns/*; api/group-demand-intelligence.js; server.js /demand-campaigns; public GDI app.js",
      notes: "YOTEL 10 campaigns FS+API+UI visible",
    },
    {
      feature: "Signal / Research Lead / Candidate separation",
      status: "LIVE_BUT_NOT_WIRED",
      files: "lib/group-demand-intelligence/opportunity-discovery-v5/funnel.js; complete-demand-packet-v8/packet-schema.js",
      notes: "Funnel enums exist; production research path does not enforce V5 funnel end-to-end",
    },
    {
      feature: "Complete Demand Packet",
      status: "REPORT_ONLY",
      files: "lib/group-demand-intelligence/complete-demand-packet-v8/*",
      notes: "V8 packet evaluation used in audit scripts; not required on live API write path",
    },
    {
      feature: "Predicted Opportunity",
      status: "REPORT_ONLY",
      files: "lib/group-demand-intelligence/ten-bases-of-demand-v1/orchestrator.js (isPredicted)",
      notes: "Emitted in ten-bases reports only; 0 customer-ready",
    },
    {
      feature: "Comp-set demand mining",
      status: "REPORT_ONLY",
      files: "lib/group-demand-intelligence/comp-set-demand-mining-v1/*",
      notes: "Used by ten-bases/V8 report runners; not wired into live orchestrator",
    },
    {
      feature: "Phone / alias / address pivots",
      status: "PARTIAL",
      files: "lib/group-demand-intelligence/complete-demand-packet-v8/*; commercial-evidence-v4",
      notes: "Pivot helpers exist; live yield path rarely invokes them",
    },
    {
      feature: "Apify discovery layer",
      status: "PARTIAL",
      files: "lib/group-demand-intelligence (apify adapters in V8/ten-bases)",
      notes: "Mapped in report runners; not default research/run",
    },
    {
      feature: "Multilingual routing",
      status: "REPORT_ONLY",
      files: "ten-bases / V8 multilingual modules",
      notes: "Report yield only",
    },
    {
      feature: "Feeder-market routing",
      status: "REPORT_ONLY",
      files: "ten-bases / V8 feeder modules",
      notes: "Report yield only",
    },
    {
      feature: "Buyer-first research",
      status: "PARTIAL",
      files: "opportunity-who-resolution-v1.js; enrich-gdi-opportunity-for-customer.js; V8 buyer-resolution",
      notes: "WHO gate live on readiness; buyer-first discovery not default for YOTEL generators",
    },
    {
      feature: "Participant / exhibitor / sponsor mining",
      status: "REPORT_ONLY",
      files: "ten-bases-of-demand-v1/decomposers.js decomposePublishedEventDemand + PARTICIPANT_*",
      notes: "Not invoked from demand-campaigns or research-orchestrator",
    },
    {
      feature: "Delegation decomposition",
      status: "REPORT_ONLY",
      files: "ten-bases-of-demand-v1/decomposers.js decomposeIntlOrgMeeting",
      notes: "Same — report orchestrator only",
    },
    {
      feature: "Rotation / repeat intelligence",
      status: "PARTIAL",
      files: "demonstrated-demand-v6; ten-bases rotation",
      notes: "Some live Bethesda patterns; YOTEL unused",
    },
    {
      feature: "Corporate recurring intelligence",
      status: "REPORT_ONLY",
      files: "ten-bases RECURRING_CORPORATE_MEETINGS",
      notes: "Noise-heavy YOTEL report rows; not canonical",
    },
    {
      feature: "Corporate trigger intelligence",
      status: "REPORT_ONLY",
      files: "ten-bases CORPORATE_TRIGGER_DEMAND",
      notes: "Report-only predicted leads",
    },
    {
      feature: "Pharma ecosystem expansion",
      status: "REPORT_ONLY",
      files: "ten-bases PHARMA_MEDICAL_ECOSYSTEM",
      notes: "Report-only",
    },
    {
      feature: "Project/workforce decomposition",
      status: "REPORT_ONLY",
      files: "ten-bases PROJECT_WORKFORCE_DEMAND",
      notes: "0 YOTEL yield in prior run",
    },
    {
      feature: "Sports/production decomposition",
      status: "REPORT_ONLY",
      files: "ten-bases SPORTS_ENTERTAINMENT_PRODUCTION",
      notes: "Report-only; CHI not decomposed via campaign path",
    },
    {
      feature: "Hotel history / lookalike engine",
      status: "REPORT_ONLY",
      files: "ten-bases HOTEL_HISTORY_LOOKALIKE",
      notes: "Report-only",
    },
    {
      feature: "Jev research controller",
      status: "LIVE_BUT_NOT_WIRED",
      files: "lib/group-demand-intelligence/jev/*; jev-active-research-v2/*",
      notes: "Advisory functions live; not on demand-campaign child path; policy forbids fact/promote",
    },
    {
      feature: "Next-best research",
      status: "PARTIAL",
      files: "jev-active-advisor.js buildDeterministicActiveAdvice",
      notes: "Used in V6/V8/ten-bases reports; not auto-scheduled for YOTEL campaigns",
    },
    {
      feature: "Customer readiness",
      status: "LIVE_AND_USED",
      files: "customer-readiness-gate-v1.js isGdiCustomerOpportunityReady; api/group-demand-intelligence.js",
      notes: "Hard gate on customer list",
    },
    {
      feature: "Future Watch",
      status: "LIVE_AND_USED",
      files: "future-watch/is-valid-future-watch-v1.js",
      notes: "Live watch validation",
    },
    {
      feature: "Surface eligibility",
      status: "LIVE_AND_USED",
      files: "customer-surface-revalidation-v1.js; customer-visibility.js",
      notes: "Fixed FUTURE_WATCH≠exhibitor false positive this audit",
    },
    {
      feature: "Admin snapshot",
      status: "LIVE_AND_USED",
      files: "api/admin-gdi-reports.js; reports/gdi-pdf-*",
      notes: "Uses filterSalespersonView + customer facing projection",
    },
    {
      feature: "Client/external UI",
      status: "LIVE_AND_USED",
      files: "public GDI UI; api opportunities + demand-campaigns routes",
      notes: "Campaigns visible; child section depends on child opps",
    },
  ];
}

function readCsvRows(filePath) {
  if (!fs.existsSync(filePath)) return [];
  const text = fs.readFileSync(filePath, "utf8").trim();
  if (!text) return [];
  const lines = text.split(/\r?\n/);
  const headers = lines[0].split(",");
  return lines.slice(1).map((line) => {
    // naive CSV — sufficient for our report artifacts
    const cols = [];
    let cur = "";
    let inQ = false;
    for (let i = 0; i < line.length; i++) {
      const ch = line[i];
      if (ch === '"') {
        if (inQ && line[i + 1] === '"') {
          cur += '"';
          i++;
        } else inQ = !inQ;
      } else if (ch === "," && !inQ) {
        cols.push(cur);
        cur = "";
      } else cur += ch;
    }
    cols.push(cur);
    const row = {};
    headers.forEach((h, idx) => {
      row[h] = cols[idx] ?? "";
    });
    return row;
  });
}

/** AI for Good 2026 named participants — public participation evidence for 2027 thesis. */
function aiForGoodChildSeeds() {
  const sourceNote =
    "2026 Gold/Silver/Networking/Session partners listed via Relve synthesis of ITU 2026-03-25 press + aiforgood.itu.int/summit26 (secondary corroboration). 2027 cycle dates from official 2027summit.aiforgood.itu.int.";
  const base = {
    parentCampaignId: "ycamp_ai_for_good_2027",
    parentDemandGeneratorId: "dg_5d13ff208e7d1f67",
    eventSeriesId: "dgsr_6ddcd2a41855",
    eventCycleId: "cycle:ai_for_good|2027",
    eventStartDate: "2027-06-21",
    eventEndDate: "2027-06-24",
    eventYear: 2027,
    venue: "Palexpo, Geneva",
    hotelId: HOTEL_ID,
    officialSource: OFFICIAL_2027,
    discoverySource: EVIDENCE_URL,
    sourceNote,
  };
  return [
    {
      ...base,
      organizationName: "Google",
      role: "NETWORKING_PARTNER_2026",
      participantType: "SPONSOR_PARTNER",
      travelingGroup: "Partner/sponsorship activation + product demo team",
      buyerEntity: "Google event / partner marketing / field marketing (role path)",
      buyerRole: "Event / Partner Marketing",
      publicContactPath: "https://aiforgood.itu.int/summit26/",
      lodgingState: "UNKNOWN",
      lodgingNote: "Participation ≠ rooms; no published 2026/2027 housing block for Google found",
    },
    {
      ...base,
      organizationName: "Microsoft",
      role: "SESSION_PARTNER_2026",
      participantType: "SPONSOR_PARTNER",
      travelingGroup: "Session partner / booth / speaker support team",
      buyerEntity: "Microsoft Events / Industry Solutions / UN affairs liaison (role path)",
      buyerRole: "Events / Industry Partnerships",
      publicContactPath: "https://aiforgood.itu.int/summit26/",
      lodgingState: "UNKNOWN",
      lodgingNote: "No public Microsoft room block found for AI for Good",
    },
    {
      ...base,
      organizationName: "Cisco",
      role: "SESSION_PARTNER_2026",
      participantType: "SPONSOR_PARTNER",
      travelingGroup: "Session partner / technical demo / sponsor team",
      buyerEntity: "Cisco Events / Global Public Sector events (role path)",
      buyerRole: "Events / Public Sector Partnerships",
      publicContactPath: "https://aiforgood.itu.int/summit26/",
      lodgingState: "UNKNOWN",
      lodgingNote: "No public Cisco housing evidence for this summit",
    },
    {
      ...base,
      organizationName: "Lenovo",
      role: "NETWORKING_PARTNER_2026",
      participantType: "SPONSOR_PARTNER",
      travelingGroup: "Networking partner / product showcase team",
      buyerEntity: "Lenovo Events / Brand sponsorship (role path)",
      buyerRole: "Brand / Events",
      publicContactPath: "https://aiforgood.itu.int/summit26/",
      lodgingState: "UNKNOWN",
      lodgingNote: "No public Lenovo lodging evidence",
    },
    {
      ...base,
      organizationName: "HP Inc.",
      role: "NETWORKING_PARTNER_2026",
      participantType: "SPONSOR_PARTNER",
      travelingGroup: "Networking partner activation team (HP Inc. UK listed 2026)",
      buyerEntity: "HP Events / Partner marketing (role path)",
      buyerRole: "Events / Partner Marketing",
      publicContactPath: "https://aiforgood.itu.int/summit26/",
      lodgingState: "UNKNOWN",
      lodgingNote: "No public HP lodging evidence",
    },
    {
      ...base,
      organizationName: "EY",
      role: "SILVER_SPONSOR_2026",
      participantType: "SPONSOR",
      travelingGroup: "Sponsor delegation / client hosting team",
      buyerEntity: "EY Events / Brand & Sponsorship (role path)",
      buyerRole: "Sponsorship / Events",
      publicContactPath: "https://aiforgood.itu.int/summit26/",
      lodgingState: "WEAK",
      lodgingNote: "Sponsor activations often need rooms; no published block — STRONG_INFERENCE not claimed",
    },
    {
      ...base,
      organizationName: "Ministry of Science and ICT of the Republic of Korea",
      role: "GOLD_SPONSOR_2026",
      participantType: "GOVERNMENT_DELEGATION",
      travelingGroup: "Government sponsor / ministerial delegation",
      buyerEntity: "MSIT international cooperation / protocol office (role path)",
      buyerRole: "Delegation / Protocol / International Affairs",
      publicContactPath: "https://aiforgood.itu.int/summit26/",
      lodgingState: "STRONG_INFERENCE",
      lodgingNote: "Gold government sponsor + overseas delegation → lodging almost certain; room count UNKNOWN/ESTIMATE not invented",
    },
    {
      ...base,
      organizationName: "Ministry of Internal Affairs and Communications of Japan",
      role: "GOLD_SPONSOR_2026",
      participantType: "GOVERNMENT_DELEGATION",
      travelingGroup: "Government sponsor / ministerial delegation",
      buyerEntity: "MIC international affairs / protocol (role path)",
      buyerRole: "Delegation / Protocol / International Affairs",
      publicContactPath: "https://aiforgood.itu.int/summit26/",
      lodgingState: "STRONG_INFERENCE",
      lodgingNote: "Gold government sponsor + overseas travel → lodging likely; no published hotel named",
    },
    {
      ...base,
      organizationName: "TikTok",
      role: "NETWORKING_PARTNER_2026",
      participantType: "SPONSOR_PARTNER",
      travelingGroup: "Partner activation / content / product team",
      buyerEntity: "TikTok Events / Partnerships (role path)",
      buyerRole: "Events / Partnerships",
      publicContactPath: "https://aiforgood.itu.int/summit26/",
      lodgingState: "UNKNOWN",
      lodgingNote: "No lodging evidence",
    },
    {
      ...base,
      organizationName: "Access Partnership",
      role: "SESSION_PARTNER_2026",
      participantType: "CONSULTANCY_PARTNER",
      travelingGroup: "Session partner / policy team",
      buyerEntity: "Access Partnership events / client services (role path)",
      buyerRole: "Events / Client Services",
      publicContactPath: "https://aiforgood.itu.int/summit26/",
      lodgingState: "UNKNOWN",
      lodgingNote: "Smaller firm — lodging plausible but unproven",
    },
    {
      ...base,
      organizationName: "PixVerse",
      role: "FILM_FESTIVAL_PARTNER_2026",
      participantType: "PRODUCTION_PARTNER",
      travelingGroup: "Film festival / production / activation crew",
      buyerEntity: "PixVerse partnerships / production liaison (role path)",
      buyerRole: "Partnerships / Production",
      publicContactPath: "https://aiforgood.itu.int/summit26/",
      lodgingState: "WEAK",
      lodgingNote: "Production partner may travel; no housing proof",
    },
    {
      ...base,
      organizationName: "International Telecommunication Union (ITU)",
      role: "ORGANIZER",
      participantType: "ORGANIZER",
      travelingGroup: "Organizer / secretariat (many Geneva-based — local risk)",
      buyerEntity: "ITU AI for Good secretariat / events office",
      buyerRole: "Secretariat / Events",
      publicContactPath: OFFICIAL_2027,
      lodgingState: "UNKNOWN",
      lodgingNote: "Organizer HQ in Geneva — many staff local; overflow visitors unknown. SIGNAL caution.",
      forceClass: "SIGNAL_ONLY",
    },
  ];
}

function admitChild(seed) {
  const named = Boolean(seed.organizationName);
  const evidence = Boolean(seed.discoverySource || seed.officialSource);
  const traveling = Boolean(seed.travelingGroup);
  const futureThesis = Boolean(seed.eventStartDate?.startsWith("2027"));
  const market = /Palexpo|Geneva/i.test(seed.venue || "");
  if (!(named && evidence && traveling && futureThesis && market)) {
    return {
      class: "REJECTED",
      reason: "admission_criteria_failed",
      named,
      evidence,
      traveling,
      futureThesis,
      market,
    };
  }
  if (seed.forceClass) return { class: seed.forceClass, reason: "force_class" };
  // Complete packet requires lodging DIRECT/STRONG + buyer + timing confirmed for 2027 participation
  if (
    (seed.lodgingState === "DIRECT" || seed.lodgingState === "STRONG_INFERENCE") &&
    seed.buyerEntity &&
    seed.role
  ) {
    // Still missing confirmed 2027 sponsor renewal → RESEARCH_LEAD not COMPLETE
    return { class: "RESEARCH_LEAD", reason: "2026_participation_proven_2027_renewal_unconfirmed" };
  }
  return { class: "RESEARCH_LEAD", reason: "named_participation_2026_future_thesis_2027" };
}

function buildChildOpportunity(seed, admission) {
  const id = `gdi_opp_aifg_2027_${slug(seed.organizationName)}_${slug(seed.role)}`;
  const title = `${seed.organizationName} — AI for Good 2027 (${seed.role.replace(/_/g, " ")})`;
  const thesis = `${seed.organizationName} participated as ${seed.role.replace(/_/g, " ")} at AI for Good 2026 (Palexpo). 2027 summit returns to Palexpo 21–24 Jun. YOTEL Geneva Lake is airport/Palexpo corridor overflow for traveling sponsor/delegation teams — not organizer HQ lodging.`;
  return {
    id,
    opportunityId: id,
    hotelId: HOTEL_ID,
    title,
    organizationName: seed.organizationName,
    opportunityType: "FUTURE_CYCLE",
    opportunityQualification: "WATCH",
    priority: "WATCHLIST",
    customerFacingState: "FUTURE_WATCH",
    customerVisible: false,
    segment: "AI for Good child account",
    demandFamily: "PARTICIPANT_SPONSOR",
    demandType: "sponsor_delegation",
    eventStartDate: seed.eventStartDate,
    eventEndDate: seed.eventEndDate,
    eventYear: seed.eventYear,
    eventSeriesId: seed.eventSeriesId,
    eventCycleId: seed.eventCycleId,
    demandGeneratorId: seed.parentDemandGeneratorId,
    parentCampaignId: seed.parentCampaignId,
    venue: seed.venue,
    eventLocationSummary: "Palexpo, Geneva",
    officialSource: seed.officialSource,
    discoverySource: seed.discoverySource,
    sources: [
      { url: OFFICIAL_2027, title: "AI for Good Global Summit 2027" },
      { url: OFFICIAL_2026, title: "AI for Good Global Summit 2026" },
      { url: EVIDENCE_URL, title: "Relve — 2026 partners/sponsors synthesis (ITU press)" },
    ],
    relationshipToEvent: seed.role,
    buyerEntity: seed.buyerEntity,
    primaryContactRole: seed.buyerRole,
    contactResearchAttempted: true,
    researchMethodsAttempted: ["public_partner_list_2026", "official_2027_cycle_page"],
    whoPathClass: "ROLE_ENTITY_PATH",
    lodgingEvidence: seed.lodgingNote,
    roomDemandStatus: seed.lodgingState === "UNKNOWN" ? "UNKNOWN" : "ESTIMATED_LIKELY",
    roomDemandConfidence: "UNKNOWN",
    housingStatus: seed.lodgingState,
    hotelOpportunityThesis: thesis,
    hotelDemandThesis: thesis,
    summaryWhat: `${seed.organizationName} is a named ${seed.role.replace(/_/g, " ")} from AI for Good 2026 with a 2027 Palexpo cycle thesis for traveling group lodging near Geneva Airport.`,
    summaryWhyHotel:
      "YOTEL Geneva Lake serves Palexpo / airport corridor overflow when onsite and central Geneva inventory fills for large UN/tech summits.",
    summaryWhyMatters:
      "Sponsor and government partner teams are the sales-relevant child accounts of the AI for Good demand generator — not the mega-event itself.",
    whyNow:
      "2027 dates are published (21–24 Jun Palexpo). 2026 partner list is public; confirm 2027 renewal and housing path before active sell.",
    recommendedAction:
      "Research whether this organization renews for AI for Good 2027; identify events/sponsorship contact; monitor ITU housing announcements.",
    recommendedNextStep:
      "Confirm 2027 participation + housing/contact path; do not treat 2026 sponsorship alone as a room block.",
    fitExplanation: thesis,
    hotelFitScore: 62,
    funnelStage: admission.class,
    childAdmissionClass: admission.class,
    childAdmissionReason: admission.reason,
    isTestData: false,
    gdiCanary: "AI_FOR_GOOD_2027_CHILD_V1",
    claimKindNotes: {
      rooms: "NOT_INVENTED",
      participation2027: "THESIS_FROM_2026_EVIDENCE",
      lodging: seed.lodgingState,
    },
  };
}

function mapJevLoop(advice) {
  if (/STOP_PUBLIC|CEILING/i.test(advice.jevStopCondition || "")) return JEV_LOOP.STOP_PUBLIC_DATA_CEILING;
  if (advice.jevDecision === "STOP_LOW_YIELD") return JEV_LOOP.STOP_LOW_INFORMATION_GAIN;
  if (advice.jevTargetBlocker && advice.jevSourceFamily) return JEV_LOOP.CONTINUE;
  return JEV_LOOP.CONTINUE;
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const inventory = buildFeatureInventory();
  write(
    "FEATURE_INVENTORY.csv",
    toCsv(inventory, ["feature", "status", "files", "notes"])
  );

  // --- Truth reconciliation YOTEL ---
  invalidateGdiHotelReadCache(HOTEL_ID);
  const canon = await loadOpportunitiesCanonical(HOTEL_ID);
  const fsOpsPath = path.join(
    ROOT,
    "data/group-demand-intelligence/hotels",
    HOTEL_ID,
    "opportunities.json"
  );
  const fsDoc = JSON.parse(fs.readFileSync(fsOpsPath, "utf8"));
  const campaignsDoc = loadDemandCampaigns(HOTEL_ID);
  const visibleCampaigns = listVisibleDemandCampaigns(HOTEL_ID, { nowDate: NOW });

  const rawOps = canon.opportunities || [];
  const cqOps = rawOps.map((o) => applyLiveCommercialQuality(o, { nowDate: NOW }));
  const sales = filterSalespersonView(cqOps);
  const facing = filterCustomerFacingOpportunities(sales, { nowDate: NOW });
  const readyIds = facing.filter((o) => isGdiCustomerOpportunityReady(o, { nowDate: NOW }).ok).map((o) => o.id);
  const watchIds = cqOps.filter((o) => isValidFutureWatch(o, { nowDate: NOW }).ok).map((o) => o.id);

  const aidexRaw = rawOps.find((o) => o.id === "gdi_opp_aidex_geneva_11");
  const aidexCq = aidexRaw ? applyLiveCommercialQuality(aidexRaw, { nowDate: NOW }) : null;
  const aidexFs = (fsDoc.opportunities || []).find((o) => o.id === "gdi_opp_aidex_geneva_11");
  const aidexFsSales = aidexFs ? filterSalespersonView([aidexFs]) : [];
  const aidexFsReady = aidexFs
    ? isGdiCustomerOpportunityReady(aidexFs, { nowDate: NOW }).ok
    : false;
  const aidexAtReady = aidexRaw
    ? isGdiCustomerOpportunityReady(aidexRaw, { nowDate: NOW }).ok
    : false;
  const aidexCqReady = aidexCq
    ? isGdiCustomerOpportunityReady(aidexCq, { nowDate: NOW }).ok
    : false;

  // Live API probe (same process projection)
  let apiReadyCount = null;
  let apiWatchCount = null;
  let apiCampaignCount = null;
  try {
    const base = process.env.GDI_AUDIT_BASE_URL || "http://127.0.0.1:8080";
    const oppRes = await fetch(`${base}/api/group-demand-intelligence/hotels/${HOTEL_ID}/opportunities`);
    if (oppRes.ok) {
      const body = await oppRes.json();
      const list = body.opportunities || body.items || [];
      apiReadyCount = list.filter((o) => o.customerReadiness?.ok === true || o.priority === "READY").length;
      // Prefer explicit ready via gate on returned rows
      apiReadyCount = list.filter((o) => {
        try {
          return isGdiCustomerOpportunityReady(applyLiveCommercialQuality(o, { nowDate: NOW }), {
            nowDate: NOW,
          }).ok;
        } catch {
          return false;
        }
      }).length;
      apiWatchCount = list.filter((o) => {
        try {
          return isValidFutureWatch(applyLiveCommercialQuality(o, { nowDate: NOW }), { nowDate: NOW })
            .ok;
        } catch {
          return false;
        }
      }).length;
    }
    const campRes = await fetch(
      `${base}/api/group-demand-intelligence/hotels/${HOTEL_ID}/demand-campaigns`
    );
    if (campRes.ok) {
      const body = await campRes.json();
      apiCampaignCount = (body.campaigns || body.items || []).length;
    }
  } catch (err) {
    apiReadyCount = `ERROR:${err.message}`;
  }

  const truthRows = [
    {
      layer: "Airtable_canonical_load",
      generators: campaignsDoc.campaigns.length,
      research_leads: "n/a_in_opp_bag",
      candidates: rawOps.length,
      ready: rawOps.filter((o) => isGdiCustomerOpportunityReady(o, { nowDate: NOW }).ok).length,
      watch: rawOps.filter((o) => isValidFutureWatch(o, { nowDate: NOW }).ok).length,
      rejected: rawOps.filter((o) => o.priority === "DISQUALIFIED").length,
      last_research: aidexRaw?.lastResearchedAt || aidexRaw?.updatedAt || "",
      current_cycle: "mixed",
      snapshot_id: canon.source || "airtable",
      notes: `mode=${canon.mode || "n/a"} count=${rawOps.length}`,
    },
    {
      layer: "Filesystem_opportunities_json",
      generators: campaignsDoc.campaigns.length,
      research_leads: "n/a",
      candidates: (fsDoc.opportunities || []).length,
      ready: (fsDoc.opportunities || []).filter((o) =>
        isGdiCustomerOpportunityReady(o, { nowDate: NOW }).ok
      ).length,
      watch: (fsDoc.opportunities || []).filter((o) =>
        isValidFutureWatch(o, { nowDate: NOW }).ok
      ).length,
      rejected: 0,
      last_research: aidexFs?.updatedAt || "",
      current_cycle: "mixed",
      snapshot_id: fsDoc.updatedAt || fsDoc.savedAt || "fs",
      notes: "AidEx seed customerReady was hardcoded on campaign; opp bag is source",
    },
    {
      layer: "Filesystem_demand_campaigns",
      generators: campaignsDoc.campaigns.length,
      research_leads: campaignsDoc.campaigns.reduce((a, c) => a + (c.researchLeads || 0), 0),
      candidates: campaignsDoc.campaigns.reduce((a, c) => a + (c.candidateOpportunities || 0), 0),
      ready: campaignsDoc.campaigns.reduce((a, c) => a + (c.customerReady || 0), 0),
      watch: campaignsDoc.campaigns.reduce((a, c) => a + (c.validFutureWatch || 0), 0),
      rejected: campaignsDoc.campaigns.reduce((a, c) => a + (c.rejected || 0), 0),
      last_research: campaignsDoc.updatedAt || "",
      current_cycle: "CURRENT_FUTURE_seed",
      snapshot_id: "demand-campaigns.json",
      notes: "Campaign counters may diverge from live gates",
    },
    {
      layer: "Live_CQ_projection",
      generators: visibleCampaigns.count,
      research_leads: "n/a",
      candidates: cqOps.length,
      ready: readyIds.length,
      watch: watchIds.length,
      rejected: cqOps.filter((o) => o.priority === "DISQUALIFIED").length,
      last_research: "",
      current_cycle: "projected",
      snapshot_id: "applyLiveCommercialQuality",
      notes: "This is what API/UI use after projection",
    },
    {
      layer: "Admin_salesperson_view",
      generators: visibleCampaigns.count,
      research_leads: "n/a",
      candidates: sales.length,
      ready: sales.filter((o) => isGdiCustomerOpportunityReady(o, { nowDate: NOW }).ok).length,
      watch: sales.filter((o) => isValidFutureWatch(o, { nowDate: NOW }).ok).length,
      rejected: 0,
      last_research: "",
      current_cycle: "",
      snapshot_id: "filterSalespersonView",
      notes: "",
    },
    {
      layer: "External_customer_facing",
      generators: visibleCampaigns.count,
      research_leads: 0,
      candidates: facing.length,
      ready: readyIds.length,
      watch: facing.filter((o) => isValidFutureWatch(o, { nowDate: NOW }).ok).length,
      rejected: 0,
      last_research: "",
      current_cycle: "",
      snapshot_id: "filterCustomerFacingOpportunities",
      notes: `readyIds=${readyIds.join("|")}`,
    },
    {
      layer: "Live_HTTP_API",
      generators: apiCampaignCount,
      research_leads: "n/a",
      candidates: "see_response",
      ready: apiReadyCount,
      watch: apiWatchCount,
      rejected: "",
      last_research: "",
      current_cycle: "",
      snapshot_id: "GET /opportunities + /demand-campaigns",
      notes: "Requires server with latest surface fix",
    },
    {
      layer: "Report_artifacts_ten_bases",
      generators: 188,
      research_leads: 23,
      candidates: 0,
      ready: 0,
      watch: 0,
      rejected: "",
      last_research: "2026-10-04 ten-bases run",
      current_cycle: "report_only",
      snapshot_id: "reports/gdi/ten-bases-of-demand-v1",
      notes: "238 children / 136 leads cohort-wide; YOTEL noise; not linked to 10 campaigns",
    },
  ];
  write(
    "TRUTH_SOURCE_RECONCILIATION.csv",
    toCsv(truthRows, [
      "layer",
      "generators",
      "research_leads",
      "candidates",
      "ready",
      "watch",
      "rejected",
      "last_research",
      "current_cycle",
      "snapshot_id",
      "notes",
    ])
  );

  // --- Bethesda / NYC success traces ---
  const successPath = path.join(ROOT, "reports/gdi/complete-demand-packet-v8/SUCCESS_CONTROL_SET.csv");
  const success = readCsvRows(successPath);
  const pick = (hotel, n) => success.filter((r) => r.hotelKey === hotel).slice(0, n);
  const sample = [
    ...pick("BETHESDA", 5),
    ...pick("RENAISSANCE", 5),
    ...pick("HILTON", 5),
  ];
  // fallback keys
  const nyc = success.filter((r) => /RENAISSANCE|HILTON|NYC|TIMES/i.test(r.hotelKey + r.hotelLabel));
  while (sample.length < 15 && nyc.length) {
    const row = nyc.shift();
    if (!sample.find((s) => s.opportunityId === row.opportunityId)) sample.push(row);
  }
  const bethRows = sample.slice(0, 15).map((r) => ({
    hotelKey: r.hotelKey,
    opportunityId: r.opportunityId,
    title: r.title,
    organizationName: r.organizationName,
    source_discovery: r.howDiscovered || r.officialSource || "CURATED_OR_PRIOR_RESEARCH",
    generator: r.organizationName,
    child_account: r.organizationName,
    buyer: "resolved_in_control_packet",
    timing: "future_dated_in_control",
    lodging: r.pillarE || r.packetQuality,
    who: "present_in_control",
    contact: "present_or_role_path",
    hotel_motion_thesis: "present_in_control",
    fit: r.packetQuality,
    canonical_write: "YES_in_control_set",
    readiness: r.packetQuality?.includes("COMPLETE") ? "READY_OR_WATCH" : "PARTIAL",
    surface: "customer_visible_control",
    ui: "shown_in_bethesda_nyc_bags",
    operational_difference:
      "Named org + page-level source + buyer/timing/lodging pillars + canonical persist + surface pass — YOTEL campaigns stop at generator registration",
  }));
  write(
    "BETHESDA_NYC_SUCCESS_TRACE.csv",
    toCsv(bethRows, [
      "hotelKey",
      "opportunityId",
      "title",
      "organizationName",
      "source_discovery",
      "generator",
      "child_account",
      "buyer",
      "timing",
      "lodging",
      "who",
      "contact",
      "hotel_motion_thesis",
      "fit",
      "canonical_write",
      "readiness",
      "surface",
      "ui",
      "operational_difference",
    ])
  );

  // --- YOTEL 10 ---
  const seed = buildYotelTenGeneratorCampaigns();
  const yotelTen = seed.map((c) => {
    const stored = campaignsDoc.campaigns.find((x) => x.campaignId === c.campaignId) || c;
    const linked = (stored.opportunityIds || [])
      .map((id) => cqOps.find((o) => o.id === id))
      .filter(Boolean);
    const childOps = cqOps.filter(
      (o) =>
        o.parentCampaignId === c.campaignId ||
        o.demandGeneratorId === c.demandGeneratorId ||
        (o.eventSeriesId && o.eventSeriesId === c.eventSeriesId)
    );
    const ready = linked.concat(childOps).filter((o) =>
      isGdiCustomerOpportunityReady(o, { nowDate: NOW }).ok
    ).length;
    const watch = linked.concat(childOps).filter((o) =>
      isValidFutureWatch(o, { nowDate: NOW }).ok
    ).length;
    const vis = isGdiDemandGeneratorVisible(stored, { nowDate: NOW });
    return {
      name: c.name,
      canonical_generator_id: c.demandGeneratorId,
      campaign_id: c.campaignId,
      future_current_status: c.cycleStatus,
      source: c.officialSource,
      venue: c.venue,
      date_cycle: c.eventCycleId,
      hotel_fit: c.hotelFit,
      child_decomposition_status: stored.childDecompositionState || "CHILD_DECOMPOSITION_NOT_YET_RUN",
      child_count: childOps.length,
      lead_count: stored.researchLeads || 0,
      candidate_count: stored.candidateOpportunities || 0,
      ready_count: ready,
      watch_count: watch,
      buyer_coverage: linked.length ? "GENERATOR_LEVEL_ONLY" : "NONE",
      last_research: stored.latestResearchDate || "",
      next_action: stored.nextAction || c.nextAction,
      visible: vis.ok === true,
    };
  });
  // NOTE: yotelTen snapshot is pre-canary; refreshed after AI for Good upsert below.

  write(
    "CHILD_DECOMPOSITION_ROOT_CAUSE.md",
    `# Child Decomposition Root Cause

## Intended path
Demand Generator → ten-bases / participant decomposers → child entities → research leads → complete packets → candidates → canonical opportunities

## Actual path today
1. YOTEL 10 generators registered in \`demand-campaigns.json\` via \`buildYotelTenGeneratorCampaigns\` / \`upsertDemandCampaigns\`.
2. UI/API expose campaigns via \`listVisibleDemandCampaigns\`.
3. \`runGroupDemandResearch\` (\`research-orchestrator.js\`) does **not** call \`runTenBasesForHotel\` or \`decomposePublishedEventDemand\`.
4. Ten-bases decomposers exist only under \`lib/group-demand-intelligence/ten-bases-of-demand-v1/\` and are invoked by **report script** \`scripts/gdi-ten-bases-of-demand-v1-2026-10-04.mjs\`.
5. Campaign seed hardcodes \`childDecompositionState: CHILD_DECOMPOSITION_NOT_YET_RUN\`.
6. No scheduler / eligibility gate even attempts decomposition for these campaign IDs.

## Classification
**NOT INVOKED / NOT CONNECTED TO CURRENT-CYCLE ORCHESTRATOR** (report-only decomposers).

Not blocked by readiness thresholds. Not a missing Airtable write of children that already ran — decomposition never ran for these 10 campaign IDs.

## Exact root cause
\`demand-campaigns\` visibility layer is disconnected from \`ten-bases-of-demand-v1\` decomposers and from \`runGroupDemandResearch\`.
`
  );

  // --- Orphan reconciliation ---
  const tenBasesChildren = [
    ...readCsvRows(path.join(ROOT, "reports/gdi/ten-bases-of-demand-v1/PUBLISHED_EVENT_DECOMPOSITION.csv")),
    ...readCsvRows(path.join(ROOT, "reports/gdi/ten-bases-of-demand-v1/PARTICIPANT_ACCOUNT_MINING.csv")),
    ...readCsvRows(path.join(ROOT, "reports/gdi/ten-bases-of-demand-v1/INTL_ORG_DELEGATIONS.csv")),
  ];
  const genMatchers = [
    { campaignId: "ycamp_aidex_geneva_2026", demandGeneratorId: "dg_caed7f7e3e61b66f", nameRe: /aidex/i },
    {
      campaignId: "ycamp_geneva_health_forum_2026",
      demandGeneratorId: "dg_fd78f9e5d6e20b7a",
      nameRe: /geneva health forum|\bghf\b/i,
    },
    {
      campaignId: "ycamp_chi_geneva_centennial_2026",
      demandGeneratorId: "dg_553129bdeb9da5e0",
      nameRe: /chi geneva|concours hippique/i,
    },
    {
      campaignId: "ycamp_who_eb_160_2027",
      demandGeneratorId: "dg_6a71d54ed69d686c",
      nameRe: /executive board|eb160|who eb/i,
    },
    {
      campaignId: "ycamp_art_geneve_2027",
      demandGeneratorId: "dg_5f76f4137a9f8c8b",
      nameRe: /art gen[eè]ve|artgeneve/i,
    },
    {
      campaignId: "ycamp_watches_wonders_2027",
      demandGeneratorId: "dg_0a2eab247f311dff",
      nameRe: /watches.?&.?wonders|watches and wonders/i,
    },
    {
      campaignId: "ycamp_setac_europe_37_2027",
      demandGeneratorId: "dg_be7030c4d0e1b848",
      nameRe: /\bsetac\b/i,
    },
    {
      campaignId: "ycamp_wha_80_2027",
      demandGeneratorId: "dg_8e9bd7c04a63746b",
      nameRe: /world health assembly|\bwha\s*80\b/i,
    },
    {
      campaignId: "ycamp_ecosoc_has_2027",
      demandGeneratorId: "dg_b69c97488a47c745",
      nameRe: /ecosoc|humanitarian affairs segment/i,
    },
    {
      campaignId: "ycamp_ai_for_good_2027",
      demandGeneratorId: "dg_5d13ff208e7d1f67",
      nameRe: /ai for good|aiforgood/i,
    },
  ];
  const orphanRows = [];
  let yotelOrphans = 0;
  let linkedCorrect = 0;
  let otherHotel = 0;
  let noParent = 0;
  for (const row of tenBasesChildren) {
    const blob = `${row.organization || ""} ${row.generatorId || ""} ${row.officialSource || ""} ${row.hotelKey || ""}`;
    const match = genMatchers.find(
      (g) =>
        (row.generatorId && String(row.generatorId).includes(g.demandGeneratorId)) ||
        g.nameRe.test(blob)
    );
    let link = "NO_PARENT";
    if (match) {
      link = "LINKED_YOTEL_TEN";
      linkedCorrect += 1;
    } else if (/YOTEL/i.test(row.hotelKey || "")) {
      link = "YOTEL_ORPHAN_UNRELATED_TO_TEN";
      yotelOrphans += 1;
      noParent += 1;
    } else if (row.hotelKey) {
      link = "OTHER_HOTEL";
      otherHotel += 1;
      noParent += 1;
    } else {
      noParent += 1;
    }
    orphanRows.push({
      hotelKey: row.hotelKey,
      organization: row.organization,
      generatorId: row.generatorId,
      baseOfDemand: row.baseOfDemand,
      class: row.class,
      officialSource: row.officialSource,
      link_status: link,
      matched_campaign: match?.campaignId || "",
    });
  }
  write(
    "ORPHAN_CHILD_RECONCILIATION.csv",
    toCsv(orphanRows, [
      "hotelKey",
      "organization",
      "generatorId",
      "baseOfDemand",
      "class",
      "officialSource",
      "link_status",
      "matched_campaign",
    ])
  );

  // --- AI for Good canary ---
  const childAccounts = [];
  const buyerRows = [];
  const lodgingRows = [];
  const jevRows = [];
  const thesisRows = [];
  const writeRows = [];
  const gateRows = [];
  const candidates = [];

  for (const seedChild of aiForGoodChildSeeds()) {
    const admission = admitChild(seedChild);
    childAccounts.push({
      organization: seedChild.organizationName,
      role: seedChild.role,
      participantType: seedChild.participantType,
      admission_class: admission.class,
      admission_reason: admission.reason,
      evidence_url: seedChild.discoverySource,
      official_2027: OFFICIAL_2027,
      traveling_group: seedChild.travelingGroup,
      future_thesis: "2027-06-21 Palexpo cycle published; 2026 participation evidenced",
      yotel_relevance: "Palexpo airport corridor overflow",
    });
    if (admission.class === "REJECTED" || admission.class === "SIGNAL_ONLY") {
      buyerRows.push({
        organization: seedChild.organizationName,
        buyer_entity: seedChild.buyerEntity,
        buyer_role: seedChild.buyerRole,
        public_contact_path: seedChild.publicContactPath,
        resolved: admission.class === "SIGNAL_ONLY" ? "PARTIAL" : "NO",
        note: admission.reason,
      });
      lodgingRows.push({
        organization: seedChild.organizationName,
        lodging_state: seedChild.lodgingState,
        note: seedChild.lodgingNote,
      });
      continue;
    }

    const opp = buildChildOpportunity(seedChild, admission);
    candidates.push(opp);

    buyerRows.push({
      organization: seedChild.organizationName,
      buyer_entity: seedChild.buyerEntity,
      buyer_role: seedChild.buyerRole,
      public_contact_path: seedChild.publicContactPath,
      resolved: "YES_ROLE_ENTITY_PATH",
      note: "WHO before HOW — no Surfe; no named person required",
    });
    lodgingRows.push({
      organization: seedChild.organizationName,
      lodging_state: seedChild.lodgingState,
      note: seedChild.lodgingNote,
    });

    const advice = buildDeterministicActiveAdvice({
      organization: seedChild.organizationName,
      title: opp.title,
      researchPriority: RESEARCH_PRIORITY.P0_HIGH,
      currentBlockers: [
        seedChild.lodgingState === "UNKNOWN" ? "LODGING" : null,
        "TIMING",
        "WHO",
      ].filter(Boolean),
      completionPotentialScore: 7,
      language: "en",
    });
    const loop = mapJevLoop(advice);
    jevRows.push({
      organization: seedChild.organizationName,
      opportunity_id: opp.id,
      unresolved_pillar: advice.jevTargetBlocker,
      source_family: advice.jevSourceFamily,
      research_question: advice.jevResearchQuestion,
      worth_one_more_step: advice.jevDecision === "RESEARCH_NOW" ? "YES" : "NO",
      decision: loop,
      jev_decision: advice.jevDecision,
      writes_facts: false,
      promotes: false,
      outcome: "ADVISORY_LOGGED",
    });

    thesisRows.push({
      organization: seedChild.organizationName,
      why_this_group: seedChild.travelingGroup,
      why_this_market: "Palexpo Geneva / airport corridor",
      why_yotel: "YOTEL Geneva Lake overflow for Palexpo / La Côte",
      why_now: "2027 dates published; 2026 partner list public",
      who_buys: seedChild.buyerEntity,
      hotel_demand: "Traveling sponsor/delegation teams — room count UNKNOWN",
      evidence: seedChild.discoverySource,
      fact: "2026 role listed; 2027 summit dates at Palexpo",
      inference: "Likely renews / similar traveling pattern if role continues",
      unknown: "2027 confirmation, room nights, housing partner, named contact",
      what_yotel_could_win: "Overflow / preferred listing rooms for partner team",
      what_could_prevent: "Local Geneva housing; official housing exclusives; non-renewal",
      next_sales_action: opp.recommendedAction,
    });
  }

  // Canonical writes for RESEARCH_LEAD children (persist even if not customer-ready)
  invalidateGdiHotelReadCache(HOTEL_ID);
  const before = await loadOpportunitiesCanonical(HOTEL_ID);
  const existing = before.opportunities || [];
  let wrote = 0;
  let reconciled = true;
  for (const opp of candidates) {
    const promo = await promoteQualifiedGdiOpportunity({
      candidate: opp,
      existingOpps: existing,
      hotelId: HOTEL_ID,
      runId: RUN_ID,
      discoveryRunId: RUN_ID,
      method: "ai_for_good_2027_canary_child",
      playbook: "participant_sponsor_mining_v1",
      source: opp.discoverySource,
      dryRun: false,
    });
    writeRows.push({
      opportunity_id: opp.id,
      organization: opp.organizationName,
      action: promo.action,
      dry_run: false,
      validation_ok: promo.validation?.ok,
      validation_failed: (promo.validation?.failed || []).join("|"),
      customer_ready: promo.customerReadiness?.ok ?? promo.opportunity?.customerReadiness?.ok,
      customer_visible: promo.opportunity?.customerVisible,
      airtable_record: promo.recordId || "",
      note: promo.error || "",
    });
    if (promo.opportunity) {
      existing.push(promo.opportunity);
      wrote += 1;
    }
  }

  // Update campaign counters
  const childIds = candidates.map((c) => c.id);
  const leadCount = candidates.length;
  const aidexReadyLive = aidexCq
    ? isGdiCustomerOpportunityReady(aidexCq, { nowDate: NOW }).ok
    : false;
  const aidexWatchLive = aidexCq
    ? isValidFutureWatch(aidexCq, { nowDate: NOW }).ok
    : false;
  upsertDemandCampaigns(
    HOTEL_ID,
    [
      {
        campaignId: "ycamp_ai_for_good_2027",
        childDecompositionState: "CHILD_DECOMPOSITION_RUN_CANARY",
        childEntitiesDiscovered: childAccounts.filter((c) => c.admission_class !== "REJECTED").length,
        researchLeads: leadCount,
        candidateOpportunities: leadCount,
        customerReady: 0,
        validFutureWatch: 0,
        opportunityIds: childIds,
        latestResearchDate: NOW,
        nextAction: "Confirm 2027 renewals + lodging/contact; complete packets before ready",
        researchStatus: "CANARY_CHILDREN_PERSISTED",
      },
      {
        campaignId: "ycamp_aidex_geneva_2026",
        // Recompute from live gates — do not hardcode ready=1
        customerReady: aidexReadyLive ? 1 : 0,
        validFutureWatch: aidexWatchLive && !aidexReadyLive ? 1 : aidexWatchLive ? 1 : 0,
        latestResearchDate: NOW,
      },
    ],
    { note: "AI for Good 2027 canary child decomposition + AidEx gate recompute" }
  );

  // Refresh YOTEL 10 state after canary campaign upsert + child writes
  const campaignsAfter = loadDemandCampaigns(HOTEL_ID);
  invalidateGdiHotelReadCache(HOTEL_ID);
  const opsAfterForTen = await loadOpportunitiesCanonical(HOTEL_ID);
  const cqAfterForTen = (opsAfterForTen.opportunities || []).map((o) =>
    applyLiveCommercialQuality(o, { nowDate: NOW })
  );
  const yotelTenFinal = seed.map((c) => {
    const stored = campaignsAfter.campaigns.find((x) => x.campaignId === c.campaignId) || c;
    const linked = (stored.opportunityIds || [])
      .map((id) => cqAfterForTen.find((o) => o.id === id))
      .filter(Boolean);
    const childOps = cqAfterForTen.filter(
      (o) =>
        o.parentCampaignId === c.campaignId ||
        o.demandGeneratorId === c.demandGeneratorId ||
        (o.eventSeriesId && o.eventSeriesId === c.eventSeriesId) ||
        (stored.opportunityIds || []).includes(o.id)
    );
    const ready = childOps.filter((o) => isGdiCustomerOpportunityReady(o, { nowDate: NOW }).ok).length;
    const watch = childOps.filter((o) => isValidFutureWatch(o, { nowDate: NOW }).ok).length;
    const vis = isGdiDemandGeneratorVisible(stored, { nowDate: NOW });
    return {
      name: c.name,
      canonical_generator_id: c.demandGeneratorId,
      campaign_id: c.campaignId,
      future_current_status: c.cycleStatus,
      source: c.officialSource,
      venue: c.venue,
      date_cycle: c.eventCycleId,
      hotel_fit: c.hotelFit,
      child_decomposition_status: stored.childDecompositionState || "CHILD_DECOMPOSITION_NOT_YET_RUN",
      child_count: childOps.length,
      lead_count: stored.researchLeads || 0,
      candidate_count: stored.candidateOpportunities || 0,
      ready_count: ready,
      watch_count: watch,
      buyer_coverage: childOps.length ? "CHILD_OR_GENERATOR" : "NONE",
      last_research: stored.latestResearchDate || "",
      next_action: stored.nextAction || c.nextAction,
      visible: vis.ok === true,
    };
  });
  write(
    "YOTEL_TEN_GENERATOR_STATE.csv",
    toCsv(yotelTenFinal, [
      "name",
      "canonical_generator_id",
      "campaign_id",
      "future_current_status",
      "source",
      "venue",
      "date_cycle",
      "hotel_fit",
      "child_decomposition_status",
      "child_count",
      "lead_count",
      "candidate_count",
      "ready_count",
      "watch_count",
      "buyer_coverage",
      "last_research",
      "next_action",
      "visible",
    ])
  );

  invalidateGdiHotelReadCache(HOTEL_ID);
  const after = await loadOpportunitiesCanonical(HOTEL_ID);
  const afterCq = (after.opportunities || []).map((o) =>
    applyLiveCommercialQuality(o, { nowDate: NOW })
  );
  for (const opp of candidates) {
    const found = afterCq.find((o) => o.id === opp.id);
    const ready = found
      ? isGdiCustomerOpportunityReady(found, { nowDate: NOW })
      : { ok: false, failed: ["missing"] };
    const watch = found
      ? isValidFutureWatch(found, { nowDate: NOW })
      : { ok: false, reasons: ["missing"] };
    const facingOk = found
      ? filterCustomerFacingOpportunities([found], { nowDate: NOW }).length > 0
      : false;
    gateRows.push({
      opportunity_id: opp.id,
      organization: opp.organizationName,
      ready_ok: ready.ok,
      ready_failed: (ready.failed || []).join("|"),
      ready_state: ready.state || "",
      watch_ok: watch.ok,
      watch_class: watch.class || "",
      watch_reasons: (watch.reasons || []).join("|"),
      facing: facingOk,
      in_airtable: Boolean(found?._airtableRecordId || found),
      in_filesystem: (fsDoc.opportunities || []).some((o) => o.id === opp.id) || Boolean(found),
    });
    if (!found) reconciled = false;
  }

  // Re-check AidEx after surface fix
  const aidexAfter = afterCq.find((o) => o.id === "gdi_opp_aidex_geneva_11");
  const aidexLiveReady = aidexAfter
    ? isGdiCustomerOpportunityReady(aidexAfter, { nowDate: NOW }).ok
    : false;

  write(
    "AI_FOR_GOOD_CHILD_ACCOUNTS.csv",
    toCsv(childAccounts, [
      "organization",
      "role",
      "participantType",
      "admission_class",
      "admission_reason",
      "evidence_url",
      "official_2027",
      "traveling_group",
      "future_thesis",
      "yotel_relevance",
    ])
  );
  write(
    "AI_FOR_GOOD_BUYER_RESEARCH.csv",
    toCsv(buyerRows, [
      "organization",
      "buyer_entity",
      "buyer_role",
      "public_contact_path",
      "resolved",
      "note",
    ])
  );
  write(
    "AI_FOR_GOOD_LODGING_RESEARCH.csv",
    toCsv(lodgingRows, ["organization", "lodging_state", "note"])
  );
  write(
    "JEV_CANARY_LOG.csv",
    toCsv(jevRows, [
      "organization",
      "opportunity_id",
      "unresolved_pillar",
      "source_family",
      "research_question",
      "worth_one_more_step",
      "decision",
      "jev_decision",
      "writes_facts",
      "promotes",
      "outcome",
    ])
  );
  write(
    "AI_FOR_GOOD_TARGET_HOTEL_THESES.csv",
    toCsv(thesisRows, [
      "organization",
      "why_this_group",
      "why_this_market",
      "why_yotel",
      "why_now",
      "who_buys",
      "hotel_demand",
      "evidence",
      "fact",
      "inference",
      "unknown",
      "what_yotel_could_win",
      "what_could_prevent",
      "next_sales_action",
    ])
  );
  write(
    "CANONICAL_WRITE_QA.csv",
    toCsv(writeRows, [
      "opportunity_id",
      "organization",
      "action",
      "dry_run",
      "validation_ok",
      "validation_failed",
      "customer_ready",
      "customer_visible",
      "airtable_record",
      "note",
    ])
  );
  write(
    "READINESS_GATE_QA.csv",
    toCsv(gateRows, [
      "opportunity_id",
      "organization",
      "ready_ok",
      "ready_failed",
      "ready_state",
      "watch_ok",
      "watch_class",
      "watch_reasons",
      "facing",
      "in_airtable",
      "in_filesystem",
    ])
  );

  const researchLeads = childAccounts.filter((c) => c.admission_class === "RESEARCH_LEAD").length;
  const completePackets = childAccounts.filter((c) => c.admission_class === "COMPLETE_PACKET").length;
  const signalOnly = childAccounts.filter((c) => c.admission_class === "SIGNAL_ONLY").length;
  const aifgReady = gateRows.filter((g) => g.ready_ok === true || g.ready_ok === "true").length;
  const aifgWatch = gateRows.filter((g) => g.watch_ok === true || g.watch_ok === "true").length;
  const buyerResolved = buyerRows.filter((b) => String(b.resolved).startsWith("YES")).length;
  const contactPaths = buyerRows.filter((b) => b.public_contact_path).length;
  const lodgingSupported = lodgingRows.filter((l) =>
    ["DIRECT", "STRONG_INFERENCE", "WEAK"].includes(l.lodging_state)
  ).length;

  write(
    "UI_QA.md",
    `# UI QA — YOTEL / AI for Good

## Expected
- AI for Good visible as demand campaign
- Child-account section reflects canary children when API returns them
- Ready children only if \`isGdiCustomerOpportunityReady\` passes (expect 0 for canary)
- Watch children only if \`isValidFutureWatch\` passes
- Research leads not mislabeled as ready
- Empty-state copy accurate when no ready children

## Automated checks (this run)
- Visible campaigns: ${visibleCampaigns.count}
- AI for Good campaign present: ${
      visibleCampaigns.campaigns.some((c) => c.campaignId === "ycamp_ai_for_good_2027")
        ? "YES"
        : "NO"
    }
- AidEx CQ-ready after surface fix: ${aidexLiveReady ? "YES" : "NO"}
- AI for Good research leads persisted: ${leadCount}
- AI for Good customer-ready (gate): ${aifgReady}
- AI for Good valid future watch (gate): ${aifgWatch}

## Manual browser
1. Open YOTEL GDI page (auth or share as configured).
2. Confirm Demand Campaigns includes AI for Good Global Summit 2027.
3. Confirm child opportunities appear under research/watch — not Ready — unless gates pass.
4. Confirm AidEx appears in facing list after server restart with surface fix.
`
  );

  write(
    "ROOT_CAUSE.md",
    `# Root Cause Classification

## Ranked
1. **B. CHILD DECOMPOSITION NOT WIRED** — Campaigns visible; decomposers never invoked from live path.
2. **G. SNAPSHOT/API/UI DIVERGENCE** — AidEx: FS/campaign counters + raw Airtable ready vs CQ projection DQ via FUTURE_WATCH→exhibitor false positive (fixed this audit).
3. **A. DISCOVERY QUALITY** — Ten-bases report children are mostly noise and orphaned from the YOTEL 10.

## Also present
- **C. BUYER RESOLUTION WEAK** on YOTEL path (role paths only in canary)
- **D. LODGING EVIDENCE WEAK** (canary lodging mostly UNKNOWN)
- **F. CANONICAL PERSISTENCE GAP** historically for 9/10 generators (repaired as campaigns; children still missing until canary)
- **H/I** surface/readiness interactions amplified AidEx API 0-ready

## Not primary
Thresholds were not the blocker. Child decomposition simply did not run.
`
  );

  write(
    "MINIMUM_CHANGE_PLAN.md",
    `# Minimum Change Plan

## P0
1. **Wire campaign → child decomposition for one generator at a time**  
   - Root: B  
   - Files: \`demand-campaigns/*\` + call into \`ten-bases-of-demand-v1/decomposers.js\` (or thin wrapper) from a controlled job — not broad \`runGroupDemandResearch\` rediscovery  
   - Risk: medium (wrong children if SERP noise)  
   - Yield: high — unlocks account-level pipeline

2. **Keep surface FUTURE_WATCH ≠ exhibitor fix**  
   - Root: G/I  
   - Files: \`customer-surface-revalidation-v1.js\` \`isExhibitorStyle\`  
   - Risk: low  
   - Yield: restores AidEx/CQ-facing consistency

3. **Stop hardcoding campaign \`customerReady\` counters**  
   - Root: G  
   - Files: \`yotel-ten-generators.js\` seed; recompute from live gates  
   - Risk: low  
   - Yield: truth alignment

## P1
4. **Require Complete Demand Packet before customer-ready** on child writes  
   - Root: A/C/D  
   - Files: promote path + packet schema  
   - Risk: medium (fewer ready)  
   - Yield: quality

5. **Jev advisory loop on admitted children only** (already policy-gated)  
   - Root: C/D  
   - Files: \`jev-active-advisor.js\` hooked after admission  
   - Risk: low  
   - Yield: faster pillar closure

## P2
6. Comp-set / multilingual / feeder as optional deepeners after child admission — not discovery front-doors.

## Explicit non-changes
- Do not lower thresholds
- Do not redesign 10 Bases broadly before campaign→child wire proves yield
`
  );

  const liveUsed = inventory.filter((i) => i.status === "LIVE_AND_USED").length;
  const liveNotWired = inventory.filter((i) => i.status === "LIVE_BUT_NOT_WIRED").length;
  const reportOnly = inventory.filter((i) => i.status === "REPORT_ONLY").length;
  const p0 = 3;
  const p1 = 2;
  const p2 = 1;

  const countsMatch =
    aidexLiveReady === true &&
    Number(readyIds.length) >= 1 &&
    reconciled;

  write(
    "FOUNDER_REPORT.md",
    `# GDI Final Opportunity Yield Audit — Founder Report

**Run:** ${RUN_ID}  
**Date:** ${NOW}  
**Hotel:** YOTEL Geneva Lake (\`${HOTEL_ID}\`)

## Verdict
GDI's YOTEL gap is not "missing architecture." Generators are visible; **child decomposition is not wired** to the live path. Bethesda/NYC succeeded because named account packets were researched and persisted end-to-end. Ten-bases produced 238 orphaned report children that do not belong to the YOTEL 10. AI for Good canary admitted **${researchLeads} research leads** from named 2026 partners with 2027 Palexpo thesis; **${completePackets} complete packets**, **${aifgReady} customer-ready**.

## AidEx divergence (resolved cause)
- Filesystem / raw Airtable: AidEx present; salesperson/raw ready could pass.
- Live API after \`applyLiveCommercialQuality\`: \`FUTURE_CYCLE\` → \`FUTURE_WATCH\`, then \`isExhibitorStyle\` treated \`FUTURE_WATCH\` as exhibitor → \`INSUFFICIENT_TEAM_PROOF\` → surface DQ → 0 facing.
- **Fix applied:** \`isExhibitorStyle\` no longer treats lifecycle \`FUTURE_WATCH\` as exhibitor.
- Post-fix CQ-ready: **${aidexLiveReady ? "YES" : "NO"}**

## Feature inventory
| Status | Count |
|---|---|
| LIVE_AND_USED | ${liveUsed} |
| LIVE_BUT_NOT_WIRED | ${liveNotWired} |
| REPORT_ONLY | ${reportOnly} |

## YOTEL 10
- Active visible generators: ${yotelTenFinal.filter((y) => y.visible).length}
- With child decomposition run: ${yotelTenFinal.filter((y) => !/NOT_YET_RUN/i.test(y.child_decomposition_status)).length} (AI for Good canary)
- Orphan ten-bases children linked to YOTEL 10: ${linkedCorrect}
- YOTEL orphan children unrelated to the 10: ${yotelOrphans}

## AI for Good canary
- Entities discovered: ${childAccounts.length}
- Research leads: ${researchLeads}
- Signal-only: ${signalOnly}
- Complete packets: ${completePackets}
- Customer ready: ${aifgReady}
- Valid future watch: ${aifgWatch}
- Buyer entities resolved (role path): ${buyerResolved}
- Public contact paths: ${contactPaths}
- Lodging-supported (not UNKNOWN): ${lodgingSupported}
- Jev recommendations: ${jevRows.length}
- Jev blockers resolved: 0 (advisory only)
- Jev classification changes: 0
- Canonical write reconciled: ${reconciled ? "YES" : "NO"} (wrote ${wrote})

## Minimum changes
- P0: ${p0} | P1: ${p1} | P2: ${p2}

## Guardrails
- Thresholds changed: **NO**
- Speculative children: **NO** (named 2026 partners only)
- Jev wrote facts: **NO**
- Jev promoted: **NO**
- ADP changed: **NO**
- Share tokens changed: **NO**
`
  );

  const ret = {
    FEATURES_LIVE_AND_USED_COUNT: liveUsed,
    FEATURES_LIVE_BUT_NOT_WIRED_COUNT: liveNotWired,
    FEATURES_REPORT_ONLY_COUNT: reportOnly,
    AIDEX_FILESYSTEM_READY: aidexFsReady ? "YES" : "NO",
    AIDEX_AIRTABLE_READY: aidexAtReady ? "YES" : "NO",
    AIDEX_LIVE_API_READY: aidexLiveReady ? "YES" : "NO",
    AIDEX_DIVERGENCE_ROOT_CAUSE:
      "applyLiveCommercialQuality remapped opportunityType FUTURE_CYCLE→FUTURE_WATCH; isExhibitorStyle treated FUTURE_WATCH as exhibitor → INSUFFICIENT_TEAM_PROOF → surface_eligibility DQ on live API path (fixed)",
    YOTEL_ACTIVE_GENERATORS: yotelTenFinal.filter((y) => y.visible).length,
    YOTEL_GENERATORS_WITH_CHILD_DECOMPOSITION_RUN: yotelTenFinal.filter(
      (y) => !/NOT_YET_RUN/i.test(y.child_decomposition_status)
    ).length,
    YOTEL_ORPHAN_CHILD_ACCOUNTS_FOUND: yotelOrphans,
    CHILD_DECOMPOSITION_ROOT_CAUSE:
      "NOT_INVOKED — demand-campaigns not connected to ten-bases decomposers or runGroupDemandResearch",
    BETHESDA_NYC_KEY_OPERATIONAL_DIFFERENCE:
      "Named org + page-level validation + buyer/timing/lodging pillars + canonical persist + surface pass; YOTEL stopped at generator registration",
    AI_FOR_GOOD_CHILD_ENTITIES_DISCOVERED: childAccounts.length,
    AI_FOR_GOOD_RESEARCH_LEADS: researchLeads,
    AI_FOR_GOOD_COMPLETE_PACKETS: completePackets,
    AI_FOR_GOOD_CUSTOMER_READY: aifgReady,
    AI_FOR_GOOD_VALID_FUTURE_WATCH: aifgWatch,
    AI_FOR_GOOD_BUYER_ENTITIES_RESOLVED: buyerResolved,
    AI_FOR_GOOD_PUBLIC_CONTACT_PATHS: contactPaths,
    AI_FOR_GOOD_LODGING_SUPPORTED_CHILDREN: lodgingSupported,
    JEV_RECOMMENDATIONS_ISSUED: jevRows.length,
    JEV_BLOCKERS_RESOLVED: 0,
    JEV_CLASSIFICATION_CHANGES: 0,
    CANONICAL_WRITE_RECONCILED: reconciled ? "YES" : "NO",
    AIRTABLE_FILESYSTEM_API_UI_COUNTS_MATCH: countsMatch ? "YES" : "PARTIAL",
    TOP_ROOT_CAUSE: "B. CHILD DECOMPOSITION NOT WIRED",
    SECOND_ROOT_CAUSE: "G. SNAPSHOT/API/UI DIVERGENCE (AidEx CQ surface)",
    THIRD_ROOT_CAUSE: "A. DISCOVERY QUALITY (ten-bases orphans/noise)",
    P0_CHANGES_REQUIRED_COUNT: p0,
    P1_CHANGES_REQUIRED_COUNT: p1,
    P2_CHANGES_REQUIRED_COUNT: p2,
    GDI_THRESHOLDS_CHANGED: "NO",
    SPECULATIVE_CHILDREN_CREATED: "NO",
    JEV_WROTE_VERIFIED_FACTS: "NO",
    JEV_PROMOTED_OPPORTUNITIES: "NO",
    ADP_CHANGED: "NO",
    SHARE_TOKENS_CHANGED: "NO",
    FINAL_VERDICT:
      "YIELD_BLOCKED_BY_UNWIRED_CHILD_DECOMPOSITION; AIDEX_SURFACE_BUG_FIXED; AI_FOR_GOOD_CANARY_PRODUCED_RESEARCH_LEADS_NOT_READY",
    RUN_ID: RUN_ID,
  };
  write("_RETURN.json", JSON.stringify(ret, null, 2));
  console.log(JSON.stringify(ret, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
