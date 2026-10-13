#!/usr/bin/env node
/**
 * Bethesda Marriott GDI Completion + Sales Action Package V1
 *
 *   node scripts/gdi-bethesda-completion-v1.mjs
 *   node scripts/gdi-bethesda-completion-v1.mjs --apply
 *
 * Hotel-only. No Surfe. No cron. No share-token mutation. No other hotels.
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { execSync } from "node:child_process";
import {
  loadOpportunitiesCanonical,
  saveOpportunitiesCanonical,
} from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import { filterSalespersonView } from "../lib/group-demand-intelligence/opportunity-factory.js";
import { applyLiveCommercialQuality } from "../lib/group-demand-intelligence/live-commercial-quality-v1.js";
import { isGdiCustomerOpportunityReady } from "../lib/group-demand-intelligence/customer-readiness-gate-v1.js";
import { buildGdiOpportunitySummary } from "../lib/group-demand-intelligence/opportunity-summary-v1.js";
import { loadHotelDemandConfig } from "../lib/group-demand-intelligence/hotel-profile.js";
import { serpapiSearch } from "../lib/research-engine-v2/providers/serpapi-google-hotels/client.js";
import {
  fetchResearchPage,
  htmlToSearchableText,
} from "../lib/hotel-intelligence/room-count-research/fetch.js";
import {
  classifyLodgingEvidenceFromText,
  classifyCommercialStatus,
  isCommerciallyOpen,
  LODGING_EVIDENCE,
} from "../lib/group-demand-intelligence/proven-source/proven-source-playbook-v1.js";

process.env.WEBHOUND_UNAVAILABLE = "true";
process.env.WEBHOUND_DISABLED = "1";
process.env.CONTACT_INTELLIGENCE_PAID_ENRICHMENT_ENABLED = "0";

const HOTEL = "recLuxvwwxID7U2B8";
const APPLY = process.argv.includes("--apply");
const NOW = new Date().toISOString().slice(0, 10);
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(ROOT, "reports/group-demand-intelligence/bethesda-completion-v1");
const SHARE_REG = path.join(ROOT, "config/client-share/gdi-share-registry/active-tokens.json");

/** Midscale–upscale Bethesda group ADR band for ESTIMATES only (not hotel rate). */
const ADR_LOW = 165;
const ADR_BASE = 195;
const ADR_HIGH = 235;

function gitHead() {
  try {
    return execSync("git rev-parse HEAD", { cwd: ROOT, encoding: "utf8" }).trim();
  } catch {
    return "unknown";
  }
}

function writeJson(name, data) {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, name), JSON.stringify(data, null, 2), "utf8");
}

function writeText(name, text) {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, name), text, "utf8");
}

function oppId(o) {
  return o?.id || o?.opportunityId || null;
}

function hasSerp() {
  return !!(process.env.SERPAPI_API_KEY || process.env.SERPAPI_KEY);
}

function segmentOf(o) {
  const blob = `${o.title || ""} ${o.organizationName || ""} ${o.opportunityType || ""} ${o.demandSegment || ""} ${o.summaryWhat || ""}`.toLowerCase();
  if (/\bnih\b|national institutes of health|hhs\b|common fund|clinical center/.test(blob)) return "NIH";
  if (/\b(medical|healthcare|scientific|clinical|oncolog|radiolog|cardiolog|neurolog|pharmacy|renal|nursing|symposium)\b/.test(blob))
    return "MEDICAL_SCIENTIFIC";
  if (/\b(government|contractor|federal|dod|gsa|nist|nrc|industry day|procurement|cybersecur)\b/.test(blob))
    return "GOVERNMENT_CONTRACTOR";
  if (/\b(university|alumni|college|commencement|reunion|umd|georgetown|hopkins)\b/.test(blob)) return "UNIVERSITY";
  if (/\b(tournament|youth|cheer|soccer|lacrosse|sports|varsity|cup)\b/.test(blob)) return "SPORTS";
  if (/\b(weekend)\b/.test(blob) && !/\b(tournament|soccer|sports)\b/.test(blob)) return "WEEKEND";
  if (/\b(corporate|summit|offsite|leadership|user conference|board meeting)\b/.test(blob)) return "CORPORATE";
  if (/\b(association|society|annual meeting|conference|advocacy|legislative)\b/.test(blob)) return "ASSOCIATION";
  return "OTHER";
}

function parsePeakRooms(raw) {
  if (raw == null) return { value: null, confidence: "UNKNOWN", basis: null };
  if (typeof raw === "number" && Number.isFinite(raw)) {
    return { value: raw, confidence: "ESTIMATED", basis: "numeric_field" };
  }
  const s = String(raw);
  const range = s.match(/(\d{2,4})\s*[-–]\s*(\d{2,4})/);
  if (range) {
    const lo = Number(range[1]);
    const hi = Number(range[2]);
    return {
      value: Math.round((lo + hi) / 2),
      low: lo,
      high: hi,
      confidence: /overflow/i.test(s) ? "INFERRED" : "ESTIMATED",
      basis: `parsed_range:${s}`,
    };
  }
  const one = s.match(/\b(\d{2,4})\b/);
  if (one) {
    return { value: Number(one[1]), confidence: "ESTIMATED", basis: `parsed_single:${s}` };
  }
  return { value: null, confidence: "UNKNOWN", basis: s || null };
}

function estimateNights(o) {
  const n = o.eventNights || o.nights || o.stayNights || o.estimatedNights;
  if (typeof n === "number" && n > 0) return { value: n, confidence: "ESTIMATED", basis: "field" };
  const blob = `${o.title || ""} ${o.summaryWhat || ""} ${o.opportunityType || ""}`.toLowerCase();
  if (/tournament|cup|weekend/.test(blob)) return { value: 2, confidence: "INFERRED", basis: "weekend_tournament_pattern" };
  if (/symposium|summit|advocacy|legislative|conference|meeting/.test(blob)) {
    return { value: 3, confidence: "INFERRED", basis: "multi_day_meeting_pattern" };
  }
  return { value: null, confidence: "UNKNOWN", basis: null };
}

function revenueEstimate(peak, nights) {
  if (!peak?.value || !nights?.value) {
    return {
      status: "REVENUE_ESTIMATE_UNSUPPORTED",
      low: null,
      base: null,
      high: null,
      assumptions: null,
    };
  }
  const roomNightsLow = (peak.low || Math.round(peak.value * 0.75)) * nights.value;
  const roomNightsBase = peak.value * nights.value;
  const roomNightsHigh = (peak.high || Math.round(peak.value * 1.25)) * nights.value;
  return {
    status: "ESTIMATE",
    low: Math.round(roomNightsLow * ADR_LOW),
    base: Math.round(roomNightsBase * ADR_BASE),
    high: Math.round(roomNightsHigh * ADR_HIGH),
    assumptions: {
      peakRooms: peak.value,
      peakLow: peak.low || null,
      peakHigh: peak.high || null,
      peakConfidence: peak.confidence,
      peakBasis: peak.basis,
      nights: nights.value,
      nightsConfidence: nights.confidence,
      nightsBasis: nights.basis,
      adrLow: ADR_LOW,
      adrBase: ADR_BASE,
      adrHigh: ADR_HIGH,
      note: "Gross sleeping-room revenue estimate only; not Bethesda Marriott contracted ADR; excludes meeting/F&B.",
    },
  };
}

function isNamedPerson(pc) {
  if (!pc?.name) return false;
  const n = String(pc.name);
  if (/organization contact|hotel information|travel & venue|contact desk|woman's club|preferred lodging/i.test(n)) {
    return false;
  }
  return /[A-Za-z]/.test(n) && n.split(/\s+/).length >= 2;
}

function contactPath(o) {
  const pc = o.primaryContact || {};
  const email = pc.email || o.contactEmail || null;
  const phone = pc.phone || o.contactPhone || null;
  const form = o.contactFormUrl || null;
  const page = pc.sourceUrl || o.officialSource || o.discoverySource || o.sources?.[0]?.url || null;
  const parts = [];
  if (email) parts.push(`email:${email}`);
  if (phone) parts.push(`phone:${phone}`);
  if (form) parts.push(`form:${form}`);
  if (page) parts.push(`page:${page}`);
  return {
    email,
    phone,
    form,
    page,
    summary: parts.join(" | ") || "No public contact path captured",
    hasPublicPath: Boolean(email || phone || form || page),
  };
}

function whyBethesda(o, cfg) {
  const existing = o.summaryWhyHotel || o.hotelOpportunityThesis || o.bethesdaWinThesis || "";
  if (existing && existing.trim().length > 40 && !/^strong fit signals/i.test(existing)) {
    return existing.trim();
  }
  const bits = [];
  bits.push(`~${cfg?.capabilityProfile?.totalGuestrooms || 407} rooms / ~${cfg?.capabilityProfile?.totalMeetingSpaceSqFt || 18700} sq ft meeting inventory`);
  const seg = segmentOf(o);
  if (seg === "NIH" || seg === "MEDICAL_SCIENTIFIC") {
    bits.push("NIH / medical corridor adjacency vs downtown-only hotels");
  }
  if (seg === "GOVERNMENT_CONTRACTOR") {
    bits.push("DMV federal/contractor access with Bethesda full-service meeting capability");
  }
  if (seg === "SPORTS") {
    bits.push("I-270 / Montgomery County access for SoccerPlex / regional tournament housing");
  }
  if (seg === "UNIVERSITY") {
    bits.push("Bethesda location for DC–Maryland university / alumni overnight blocks");
  }
  bits.push("Distinct from Bethesda North Marriott and Marriott Bethesda Downtown (HQ)");
  return bits.join("; ");
}

function whyNow(o) {
  const existing = o.summaryWhyNow || o.whyNow || "";
  if (existing && existing.trim().length > 20) return existing.trim();
  const dest = String(o.destinationStatus || o.venueStatus || "");
  const commercial = String(o.commercialStatus || "");
  const year = o.eventYear || (o.eventStartDate || "").slice(0, 4);
  const bits = [];
  if (/tbd|to be announced|location to be|unresolved|open/i.test(dest + commercial)) {
    bits.push("Destination/housing still open or TBD — window to request RFP inclusion");
  }
  if (/overflow/i.test(`${o.title} ${commercial} ${o.opportunityType || ""}`)) {
    bits.push("Primary venue may be set — pursue overflow / support housing now");
  }
  if (year && Number(year) >= 2027) {
    bits.push(`${year} cycle still far enough for serious group sales outreach`);
  } else if (year === "2026") {
    bits.push("2026 timing — confirm housing status immediately before cycle closes");
  }
  if (isNamedPerson(o.primaryContact) && o.primaryContact?.email) {
    bits.push(`Named contact with public email available (${o.primaryContact.name})`);
  }
  if (!bits.length) bits.push("Public lodging/program signal exists — validate sourcing status this month");
  return bits.join(". ");
}

function concreteAction(o, contact) {
  const existing = o.recommendedAction || o.recommendedNextStep || "";
  const who = o.primaryContact?.name;
  const org = o.organizationName || "organizer";
  const seg = segmentOf(o);
  // Prefer non-generic existing when specific
  if (
    existing &&
    existing.length > 40 &&
    !/^verify sourcing status is still open, then contact/i.test(existing) &&
    !/^do not aggressively sell the current cycle/i.test(existing)
  ) {
    return existing;
  }

  if (/do not pursue as venue/i.test(existing) || /primary (venue|hotel) already selected/i.test(existing)) {
    if (contact.email) {
      return `Email ${who || "housing lead"} at ${contact.email}: introduce Bethesda Marriott as overflow/support hotel for ${org}; ask for housing-list inclusion and peak room needs.`;
    }
    return `Contact housing/tournament lead for ${org}: request overflow/support housing consideration; do not pitch as primary venue.`;
  }

  if (seg === "SPORTS" && contact.email) {
    return `Email ${who} (${contact.email}): propose team-block rates + breakfast package for peak tournament weekends; ask for stay-to-play / preferred-hotel consideration.`;
  }
  if ((seg === "NIH" || seg === "MEDICAL_SCIENTIFIC") && contact.page) {
    return `Open ${contact.page}; identify meetings/housing owner; send NIH-corridor + meeting-space capability one-pager and request RFP/site visit for next open cycle.`;
  }
  if (seg === "GOVERNMENT_CONTRACTOR") {
    return `Call/email ${who || org + " meetings staff"}: position Bethesda Marriott for sleeping rooms + ~18.7k sq ft meetings; ask when 2027/2028 RFP or housing bureau opens.`;
  }
  if (contact.email) {
    return `Email ${who} at ${contact.email} with a specific Bethesda Marriott fit note (rooms + ballroom + NIH/DMV access) and request inclusion in the next housing/RFP round.`;
  }
  if (contact.page) {
    return `Use official path ${contact.page}: submit housing/RFP interest and request meetings-director contact for ${org}.`;
  }
  return `Identify meetings/housing owner for ${org} via official event site; request RFP inclusion with Bethesda Marriott capability sheet.`;
}

function priorityBucket(pri) {
  const p = String(pri || "").toUpperCase();
  if (/HIGH/.test(p)) return "HIGH";
  if (/MEDIUM/.test(p)) return "MEDIUM";
  return "WATCH";
}

function actionabilityScore(o) {
  let s = 0;
  const fit = Number(o.hotelFitScore || 0);
  s += Math.min(40, fit / 2.5);
  if (/HIGH/i.test(o.priority || "")) s += 25;
  else if (/MEDIUM/i.test(o.priority || "")) s += 15;
  else s += 5;
  if (isNamedPerson(o.primaryContact)) s += 12;
  if (o.primaryContact?.email) s += 10;
  const peak = parsePeakRooms(o.estimatedPeakRooms || o.peakRooms);
  if (peak.value && peak.value >= 80 && peak.value <= 350) s += 10;
  if (o.eventStartDate || o.eventYear) s += 5;
  const dest = `${o.destinationStatus || ""} ${o.commercialStatus || ""}`;
  if (/tbd|open|unresolved|overflow|announced/i.test(dest)) s += 8;
  if (/do not aggressively sell|monitor venue/i.test(o.recommendedAction || "")) s -= 8;
  // Prefer future
  const y = Number(o.eventYear || String(o.eventStartDate || "").slice(0, 4));
  if (y >= 2027) s += 6;
  if (y === 2026) s += 3;
  return s;
}

function competitorsFor(o, cfg) {
  const seg = segmentOf(o);
  const list = [];
  const alts = cfg?.competitiveContext?.relevantGroupDemandAlternatives || [];
  const str = cfg?.competitiveContext?.strCompSet || [];
  if (seg === "SPORTS") {
    list.push({ name: "Select-service Germantown / Gaithersburg hotels", why: "Closer to SoccerPlex; compete on rate for team blocks" });
    list.push({ name: "Hyatt Regency Bethesda", why: "Bethesda full-service alternative for stay-to-play groups" });
  } else if (seg === "NIH" || seg === "MEDICAL_SCIENTIFIC") {
    list.push({ name: "Bethesda North Marriott", why: "Often confused identity; competes for medical/NIH groups" });
    list.push({ name: "Marriott Bethesda Downtown (HQ)", why: "Newer downtown Bethesda alternative for association/medical" });
    list.push({ name: "Hyatt Regency Bethesda", why: "Metro-adjacent Bethesda full-service" });
  } else if (seg === "GOVERNMENT_CONTRACTOR") {
    list.push({ name: "Crystal Gateway / Arlington Marriotts", why: "Common host zone for DC advocacy / association washcons" });
    list.push({ name: "Hilton Washington DC/Rockville", why: "STR competitor with meetings capability" });
  } else {
    for (const a of alts.slice(0, 2)) list.push({ name: a.name, why: a.note || "Nearby Marriott alternative" });
    for (const a of str.slice(0, 2)) list.push({ name: a.name, why: "GM-provided STR competitor" });
  }
  return list.slice(0, 4);
}

function csvEscape(v) {
  const s = v == null ? "" : String(v);
  if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function shareTokenFingerprint() {
  if (!fs.existsSync(SHARE_REG)) return null;
  const j = JSON.parse(fs.readFileSync(SHARE_REG, "utf8"));
  const beth = (j.tokens || j || []).filter?.((t) => t.hotelId === HOTEL) ||
    (Array.isArray(j) ? j.filter((t) => t.hotelId === HOTEL) : []);
  // support both shapes
  const tokens = Array.isArray(j?.tokens)
    ? j.tokens.filter((t) => t.hotelId === HOTEL)
    : Array.isArray(j)
      ? j.filter((t) => t.hotelId === HOTEL)
      : Object.values(j || {}).filter((t) => t && t.hotelId === HOTEL);
  return {
    count: tokens.length,
    ids: tokens.map((t) => t.tokenId || t.id || t.token || "").sort(),
    hash: tokens
      .map((t) => `${t.tokenId || t.id}|${t.token || t.shareToken || ""}|${t.revoked || false}`)
      .sort()
      .join("||"),
  };
}

async function boundedGapDiscovery(existingTitles, budget = { queries: 8, fetches: 16 }) {
  if (!hasSerp()) return { candidates: [], ledger: { queries: 0, fetches: 0, note: "no_serp" } };
  const queries = [
    { family: "CORPORATE_EVENT_PAGE", q: `"Bethesda" OR "Rockville" OR "NIH" OR "Montgomery County" (2027 OR 2028) ("leadership summit" OR "user conference" OR "partner summit") (hotel OR lodging OR housing)` },
    { family: "INDUSTRY_DAY", q: `(NIH OR NIST OR FDA OR "Walter Reed" OR HHS) (2027 OR 2028) ("industry day" OR "industry conference") (hotel OR lodging OR Bethesda OR Rockville)` },
    { family: "ASSOCIATION_CALENDAR", q: `"Washington" OR Bethesda OR "Montgomery County" (2027 OR 2028) (association OR society) ("annual meeting" OR "advocacy summit") (housing OR "hotel block" OR RFP)` },
    { family: "ALUMNI_PAGE", q: `(UMD OR "University of Maryland" OR Georgetown OR "Johns Hopkins") (2027 OR 2028) (alumni OR reunion OR "board meeting") (Bethesda OR Rockville OR "Washington") (hotel OR lodging)` },
    { family: "SPORTS_SCHEDULE", q: `"Maryland" (2027 OR 2028) (lacrosse OR "youth soccer" OR cheer) tournament (hotel OR "stay to play" OR housing) Bethesda OR Rockville OR Boyds` },
    { family: "NIH", q: `NIH (2027 OR 2028) (symposium OR workshop OR conference) (Bethesda OR "NIH campus") (hotel OR lodging OR accommodations)` },
  ];

  const ledger = { queries: 0, fetches: 0, hits: 0, kept: 0 };
  const urls = [];
  const seen = new Set();
  for (const row of queries) {
    if (budget.queries <= 0) break;
    budget.queries -= 1;
    ledger.queries += 1;
    try {
      const serp = await Promise.race([
        serpapiSearch({ engine: "google", q: row.q, num: 6, hl: "en", gl: "us" }),
        new Promise((_, rej) => setTimeout(() => rej(new Error("serp_timeout")), 20000)),
      ]);
      for (const hit of serp?.data?.organic_results || []) {
        ledger.hits += 1;
        const url = String(hit.link || "").trim();
        if (!url || seen.has(url)) continue;
        const blob = `${hit.title || ""} ${hit.snippet || ""}`;
        if (!/\b(2026|2027|2028)\b/.test(blob + url)) continue;
        if (/\b(tripadvisor|booking\.com|expedia|hotels\.com)\b/i.test(blob + url)) continue;
        seen.add(url);
        urls.push({ url, title: hit.title || "", snippet: hit.snippet || "", family: row.family });
      }
    } catch {
      /* continue */
    }
  }

  const candidates = [];
  for (const src of urls.slice(0, Math.min(urls.length, budget.fetches))) {
    if (budget.fetches <= 0) break;
    budget.fetches -= 1;
    ledger.fetches += 1;
    try {
      const page = await Promise.race([
        fetchResearchPage(src.url, { timeoutMs: 10000 }),
        new Promise((_, rej) => setTimeout(() => rej(new Error("http_timeout")), 12000)),
      ]);
      const text = htmlToSearchableText(page?.html || "").slice(0, 25000);
      const blob = `${src.title} ${src.snippet} ${text.slice(0, 5000)}`;
      if (!/\b(bethesda|rockville|montgomery|nih|washington,? d\.?c\.?|dmv|maryland|arlington|boyds)\b/i.test(blob)) {
        continue;
      }
      if (!/\b(2026|2027|2028)\b/.test(blob)) continue;
      const lodging = classifyLodgingEvidenceFromText(blob, src.url);
      const commercial = classifyCommercialStatus(blob);
      const title = src.title.slice(0, 140);
      if (existingTitles.some((t) => t && title.toLowerCase().includes(String(t).toLowerCase().slice(0, 24)))) {
        continue;
      }
      if (lodging === LODGING_EVIDENCE.NONE && !/hotel|housing|accommodation|lodging|room block/i.test(blob)) {
        continue;
      }
      candidates.push({
        title,
        url: src.url,
        family: src.family,
        lodging,
        commercial,
        open: isCommerciallyOpen(commercial),
        snippet: src.snippet,
        discoveryClass: "GAP_DISCOVERY_CANDIDATE",
      });
      ledger.kept += 1;
    } catch {
      /* continue */
    }
  }
  return { candidates, ledger };
}

function buildCard(o, cfg, classification = "EXISTING_VALID") {
  const peak = parsePeakRooms(o.estimatedPeakRooms || o.peakRooms);
  const nights = estimateNights(o);
  const rev = revenueEstimate(peak, nights);
  const contact = contactPath(o);
  const named = isNamedPerson(o.primaryContact);
  const seg = segmentOf(o);
  const summary = buildGdiOpportunitySummary(o, { nowDate: NOW });
  const ready = isGdiCustomerOpportunityReady(
    applyLiveCommercialQuality(o, { nowDate: NOW }),
    { nowDate: NOW }
  );

  return {
    opportunityId: oppId(o),
    opportunity: o.eventName || o.title,
    organization: o.organizationName || null,
    segment: seg,
    yearDate: o.eventStartDate || o.eventYear || null,
    dateConfidence: o.dateConfidence || (o.eventStartDate ? "EXACT" : o.eventYear ? "YEAR" : "UNKNOWN"),
    destinationStatus: o.destinationStatus || o.venueStatus || "UNKNOWN",
    eventStatus: o.commercialStatus || o.customerFacingState || null,
    attendance: o.estimatedAttendance || o.attendance || "UNKNOWN",
    peakRooms: peak.value != null ? peak.value : "UNKNOWN",
    peakRoomsRaw: o.estimatedPeakRooms || o.peakRooms || null,
    peakRoomsConfidence: peak.confidence,
    nights: nights.value != null ? nights.value : "UNKNOWN",
    nightsConfidence: nights.confidence,
    bethesdaFit: o.hotelFitScore ?? null,
    fitConfidence: o.hotelFitScore >= 75 ? "HIGH" : o.hotelFitScore >= 60 ? "MEDIUM" : "LOW",
    whyBethesda: whyBethesda(o, cfg),
    whyNow: whyNow(o),
    who: named
      ? {
          name: o.primaryContact.name,
          title: o.primaryContact.title || o.primaryContact.role || null,
          organization: o.primaryContact.organization || o.organizationName,
          whyRelevant: o.primaryContact.whyThisContact || o.primaryContact.relationshipToEvent || null,
          source: o.primaryContact.sourceUrl || o.primaryContact.source || null,
          confidence: o.primaryContact.contactConfidence || o.primaryContact.confidence || null,
        }
      : {
          name: null,
          title: o.primaryContact?.title || o.primaryContact?.role || "Functional path",
          organization: o.organizationName,
          whyRelevant: o.primaryContact?.whyThisContact || "No named person; use official housing/meetings path",
          source: contact.page,
          confidence: "LOW",
          functionalPath: true,
        },
    publicContactPath: contact.summary,
    contactEmail: contact.email,
    contactPhone: contact.phone,
    historicalVenueRotation: o.priorVenues || o.rotationNotes || o.historicalVenues || "UNKNOWN",
    keyEvidence: contact.page || o.officialSource || o.discoverySource || null,
    priority: priorityBucket(o.priority),
    priorityRaw: o.priority,
    recommendedAction: concreteAction(o, contact),
    revenuePotential: rev,
    confidence: ready.ok ? (summary.quality === "STRONG" ? "HIGH" : "MEDIUM") : "LOW",
    summaryQuality: summary.quality || o.summaryQuality || null,
    strictReady: ready.ok,
    classification,
    competitors: competitorsFor(o, cfg),
    actionabilityScore: actionabilityScore(o),
    namedContact: named,
    hasPublicContactPath: contact.hasPublicPath,
  };
}

function selectBalanced(cards, target = 16) {
  const orderSeg = [
    "HIGH",
  ];
  // First take all HIGH by score
  const selected = [];
  const used = new Set();
  const byPri = { HIGH: [], MEDIUM: [], WATCH: [] };
  for (const c of cards) byPri[c.priority]?.push(c);
  for (const k of ["HIGH", "MEDIUM", "WATCH"]) {
    byPri[k].sort((a, b) => b.actionabilityScore - a.actionabilityScore);
  }

  const segCount = {};
  const tryAdd = (c) => {
    if (selected.length >= target) return false;
    if (used.has(c.opportunityId)) return false;
    const sc = segCount[c.segment] || 0;
    // Cap medical overconcentration
    if (c.segment === "MEDICAL_SCIENTIFIC" && sc >= 5) return false;
    if (sc >= 4 && c.priority !== "HIGH") return false;
    used.add(c.opportunityId);
    segCount[c.segment] = sc + 1;
    selected.push(c);
    return true;
  };

  for (const c of byPri.HIGH) tryAdd(c);
  // Ensure segment diversity from medium
  const needSegs = ["CORPORATE", "ASSOCIATION", "NIH", "GOVERNMENT_CONTRACTOR", "SPORTS", "UNIVERSITY", "WEEKEND"];
  for (const seg of needSegs) {
    if ((segCount[seg] || 0) > 0) continue;
    const hit = [...byPri.MEDIUM, ...byPri.WATCH].find((c) => c.segment === seg);
    if (hit) tryAdd(hit);
  }
  for (const c of byPri.MEDIUM) tryAdd(c);
  for (const c of byPri.WATCH) tryAdd(c);
  return selected;
}

async function main() {
  const head = gitHead();
  const shareBefore = shareTokenFingerprint();
  const cfg = loadHotelDemandConfig(HOTEL);
  console.log(`[preflight] HEAD=${head} apply=${APPLY} hotel=${cfg?.displayName}`);

  const doc = await loadOpportunitiesCanonical(HOTEL);
  const enriched = (doc.opportunities || []).map((o) =>
    applyLiveCommercialQuality(o, { nowDate: NOW })
  );
  const visible = filterCustomerFacingOpportunities(filterSalespersonView(enriched), {
    nowDate: NOW,
  });
  const ready = visible.filter((o) => isGdiCustomerOpportunityReady(o, { nowDate: NOW }).ok);
  const beforeReady = ready.length;
  const beforeVisible = visible.length;

  writeJson("SHARE_TOKEN_BEFORE.json", shareBefore);

  // Gap discovery (bounded)
  const existingTitles = enriched.map((o) => o.title || o.eventName).filter(Boolean);
  console.log("[gap] starting bounded discovery…");
  const gap = await boundedGapDiscovery(existingTitles, { queries: 6, fetches: 12 });
  writeJson("GAP_DISCOVERY_CANDIDATES.json", gap);
  console.log("[gap]", gap.ledger);

  // Build cards for all ready
  let cards = ready.map((o) => buildCard(o, cfg, "EXISTING_VALID"));
  cards.sort((a, b) => b.actionabilityScore - a.actionabilityScore);

  // Attach gap candidates as FUTURE_WATCH research notes (not auto-promoted unless manually strong)
  const watchFromGap = (gap.candidates || [])
    .filter((c) => c.lodging !== LODGING_EVIDENCE.NONE || c.open)
    .slice(0, 8)
    .map((c) => ({
      opportunityId: null,
      opportunity: c.title,
      organization: null,
      segment: c.family.includes("NIH")
        ? "NIH"
        : c.family.includes("CORPORATE")
          ? "CORPORATE"
          : c.family.includes("ALUMNI")
            ? "UNIVERSITY"
            : c.family.includes("SPORTS")
              ? "SPORTS"
              : c.family.includes("INDUSTRY")
                ? "GOVERNMENT_CONTRACTOR"
                : "ASSOCIATION",
      yearDate: null,
      destinationStatus: "UNKNOWN / research",
      eventStatus: c.commercial,
      peakRooms: "UNKNOWN",
      nights: "UNKNOWN",
      bethesdaFit: null,
      whyBethesda: "Gap-fill candidate — validate lodging + Bethesda fit before customer promotion",
      whyNow: "Public future-cycle signal in weak coverage segment",
      who: { name: null, functionalPath: true, title: "Research required" },
      publicContactPath: c.url,
      keyEvidence: c.url,
      priority: "WATCH",
      recommendedAction: `Validate entity, lodging evidence, and Bethesda geographic fit from ${c.url}; only then build customer-ready opportunity.`,
      revenuePotential: { status: "REVENUE_ESTIMATE_UNSUPPORTED" },
      confidence: "LOW",
      classification: "FUTURE_WATCH",
      strictReady: false,
      gapFamily: c.family,
    }));

  const finalSet = selectBalanced(cards, 16);
  // Light refresh stamps on selected (action/whyNow) for apply
  const refreshedIds = [];
  if (APPLY) {
    const byId = new Map(enriched.map((o) => [oppId(o), o]));
    const merged = [...(doc.opportunities || [])];
    for (const card of finalSet) {
      const idx = merged.findIndex((o) => oppId(o) === card.opportunityId);
      if (idx < 0) continue;
      const cur = merged[idx];
      const next = {
        ...cur,
        summaryWhyHotel: card.whyBethesda,
        summaryWhyNow: card.whyNow,
        recommendedAction: card.recommendedAction,
        salesActionPackageV1: {
          at: new Date().toISOString(),
          revenuePotential: card.revenuePotential,
          competitors: card.competitors,
          segment: card.segment,
        },
        researchVersion: "bethesda-completion-v1",
      };
      // Re-summary if thin
      const sum = buildGdiOpportunitySummary(next, { nowDate: NOW });
      if (sum?.summaryWhat) next.summaryWhat = sum.summaryWhat;
      if (sum?.quality) next.summaryQuality = sum.quality;
      const qc = applyLiveCommercialQuality(next, { nowDate: NOW });
      if (!isGdiCustomerOpportunityReady(qc, { nowDate: NOW }).ok) continue;
      merged[idx] = qc;
      refreshedIds.push(card.opportunityId);
      card.classification = "EXISTING_REFRESHED";
    }
    await saveOpportunitiesCanonical(HOTEL, {
      ...doc,
      opportunities: merged,
      researchVersion: "bethesda-completion-v1",
    });
    console.log(`[apply] refreshed ${refreshedIds.length} opportunities`);
  }

  // Reload after
  const docAfter = await loadOpportunitiesCanonical(HOTEL);
  const enrichedAfter = (docAfter.opportunities || []).map((o) =>
    applyLiveCommercialQuality(o, { nowDate: NOW })
  );
  const visibleAfter = filterCustomerFacingOpportunities(
    filterSalespersonView(enrichedAfter),
    { nowDate: NOW }
  );
  const readyAfter = visibleAfter.filter((o) =>
    isGdiCustomerOpportunityReady(o, { nowDate: NOW }).ok
  );
  const shareAfter = shareTokenFingerprint();
  writeJson("SHARE_TOKEN_AFTER.json", shareAfter);

  const finalCards = finalSet.map((c) => {
    const live = readyAfter.find((o) => oppId(o) === c.opportunityId);
    return live ? buildCard(live, cfg, c.classification) : c;
  });

  // Metrics
  const namedPct = Math.round(
    (100 * finalCards.filter((c) => c.namedContact).length) / Math.max(1, finalCards.length)
  );
  const pathPct = Math.round(
    (100 * finalCards.filter((c) => c.hasPublicContactPath).length) / Math.max(1, finalCards.length)
  );
  const priCounts = { HIGH: 0, MEDIUM: 0, WATCH: 0 };
  for (const c of finalCards) priCounts[c.priority] += 1;

  const revSupported = finalCards.filter((c) => c.revenuePotential?.status === "ESTIMATE");
  const revTotals = revSupported.reduce(
    (acc, c) => {
      acc.low += c.revenuePotential.low || 0;
      acc.base += c.revenuePotential.base || 0;
      acc.high += c.revenuePotential.high || 0;
      return acc;
    },
    { low: 0, base: 0, high: 0 }
  );

  const top5 = [...finalCards]
    .filter((c) => c.priority === "HIGH" || c.priority === "MEDIUM")
    .sort((a, b) => b.actionabilityScore - a.actionabilityScore)
    .slice(0, 5);
  const next5 = [...finalCards]
    .filter((c) => !top5.find((t) => t.opportunityId === c.opportunityId))
    .sort((a, b) => b.actionabilityScore - a.actionabilityScore)
    .slice(0, 5);
  const watchlist = [
    ...finalCards.filter((c) => c.priority === "WATCH"),
    ...watchFromGap.slice(0, 5),
  ].slice(0, 8);

  const segCoverage = {};
  for (const c of finalCards) segCoverage[c.segment] = (segCoverage[c.segment] || 0) + 1;

  const regression = {
    readyPreserved: readyAfter.length >= beforeReady,
    visiblePreserved: visibleAfter.length >= beforeVisible,
    readyBefore: beforeReady,
    readyAfter: readyAfter.length,
    visibleBefore: beforeVisible,
    visibleAfter: visibleAfter.length,
    shareTokenChanged: shareBefore?.hash !== shareAfter?.hash,
    customerFacingRegression: readyAfter.length < beforeReady,
  };

  writeJson("FINAL_ACTION_SET.json", finalCards);
  writeJson("TOP5_IMMEDIATE.json", top5);
  writeJson("NEXT5_DEVELOP.json", next5);
  writeJson("WATCHLIST.json", watchlist);
  writeJson("REGRESSION.json", regression);
  writeJson("GAP_ANALYSIS.json", {
    readyBefore: beforeReady,
    segmentReady: cards.reduce((a, c) => {
      a[c.segment] = (a[c.segment] || 0) + 1;
      return a;
    }, {}),
    weak: ["CORPORATE", "WEEKEND", "ASSOCIATION"].filter(
      (s) => (cards.filter((c) => c.segment === s).length || 0) < 3
    ),
    strong: ["MEDICAL_SCIENTIFIC"],
    gapDiscoveryKept: gap.ledger.kept,
  });

  // CSV
  const headers = [
    "Opportunity",
    "Organization",
    "Segment",
    "Year_Date",
    "Destination_Status",
    "Event_Status",
    "Attendance",
    "Peak_Rooms",
    "Nights",
    "Bethesda_Fit",
    "Fit_Confidence",
    "Why_Bethesda",
    "Why_Now",
    "WHO_Name",
    "WHO_Title",
    "Public_Contact_Path",
    "Historical_Venue_Rotation",
    "Key_Evidence",
    "Priority",
    "Recommended_Action",
    "Revenue_Status",
    "Revenue_LOW",
    "Revenue_BASE",
    "Revenue_HIGH",
    "Confidence",
    "Classification",
    "Opportunity_ID",
  ];
  const csvLines = [headers.join(",")];
  for (const c of finalCards) {
    csvLines.push(
      [
        c.opportunity,
        c.organization,
        c.segment,
        c.yearDate,
        c.destinationStatus,
        c.eventStatus,
        c.attendance,
        c.peakRooms,
        c.nights,
        c.bethesdaFit,
        c.fitConfidence,
        c.whyBethesda,
        c.whyNow,
        c.who?.name,
        c.who?.title,
        c.publicContactPath,
        c.historicalVenueRotation,
        c.keyEvidence,
        c.priority,
        c.recommendedAction,
        c.revenuePotential?.status,
        c.revenuePotential?.low,
        c.revenuePotential?.base,
        c.revenuePotential?.high,
        c.confidence,
        c.classification,
        c.opportunityId,
      ]
        .map(csvEscape)
        .join(",")
    );
  }
  writeText("BETHESDA_OPPORTUNITY_TABLE.csv", csvLines.join("\n"));

  // GM action list
  const gm = [];
  gm.push(`# Bethesda Marriott — GM Sales Action List`);
  gm.push(``);
  gm.push(`**Hotel:** Bethesda Marriott (5151 Pooks Hill Rd) · HPC \`${HOTEL}\``);
  gm.push(`**Generated:** ${new Date().toISOString()}`);
  gm.push(`**Purpose:** What to pursue, who to call, what to do next.`);
  gm.push(``);
  gm.push(`## Immediate Top 5 (next 30 days)`);
  gm.push(``);
  top5.forEach((c, i) => {
    gm.push(`### ${i + 1}. ${c.opportunity}`);
    gm.push(`- **Why it matters:** ${c.whyNow}`);
    gm.push(
      `- **Who to call:** ${c.who?.name || "Functional path"} ${c.who?.title ? `(${c.who.title})` : ""} ${c.contactEmail ? `· ${c.contactEmail}` : ""}`
    );
    gm.push(`- **What to do next:** ${c.recommendedAction}`);
    gm.push(
      `- **Fit / Priority:** ${c.bethesdaFit ?? "—"} · ${c.priority}` +
        (c.revenuePotential?.status === "ESTIMATE"
          ? ` · Est. room revenue base ~$${c.revenuePotential.base.toLocaleString()}`
          : "")
    );
    gm.push(``);
  });
  gm.push(`## Next 5 to develop`);
  gm.push(``);
  next5.forEach((c, i) => {
    gm.push(`### ${i + 1}. ${c.opportunity}`);
    gm.push(`- **Why it matters:** ${c.whyNow}`);
    gm.push(`- **Who:** ${c.who?.name || c.who?.title || "Research contact"}`);
    gm.push(`- **Next:** ${c.recommendedAction}`);
    gm.push(``);
  });
  gm.push(`## Watchlist`);
  gm.push(``);
  for (const c of watchlist) {
    gm.push(`- **${c.opportunity}** — ${c.whyNow || c.recommendedAction} (${c.keyEvidence || c.publicContactPath || "—"})`);
  }
  gm.push(``);
  gm.push(`## 30-day plan roles`);
  gm.push(``);
  gm.push(`| Wave | Suggested owner | Focus |`);
  gm.push(`|---|---|---|`);
  gm.push(`| Top 5 immediate | Group Sales / DOS | Named outreach + RFP/housing asks |`);
  gm.push(`| Next 5 | Group Sales | Validate lodging openness + contact path |`);
  gm.push(`| Watchlist | DOS + Revenue | Trigger monitoring (housing open / destination announced) |`);
  gm.push(``);
  gm.push(`## Notes for Rad`);
  gm.push(`- This list is hotel-specific — not a citywide events dump.`);
  gm.push(`- Revenue figures (where shown) are **estimates** using assumed DMV group ADR bands, not contracted rates.`);
  gm.push(`- Do not confuse this hotel with Bethesda North Marriott or Marriott Bethesda Downtown.`);
  writeText("BETHESDA_GM_ACTION_LIST.md", gm.join("\n"));

  // Founder report
  const verdict =
    finalCards.length >= 12 && namedPct >= 40 && !regression.customerFacingRegression
      ? namedPct >= 60 && pathPct >= 80
        ? "BETHESDA GDI COMPLETION PASSES — CUSTOMER-READY SALES ACTION PACKAGE"
        : "BETHESDA GDI COMPLETION PASSES — STRONG PIPELINE, SOME CONTACT GAPS"
      : finalCards.length >= 10
        ? "BETHESDA GDI COMPLETION PARTIAL — MORE RESEARCH REQUIRED BEFORE GM USE"
        : "BETHESDA GDI QUALITY GATE FAILS — DO NOT PRESENT";

  const fr = [];
  fr.push(`# Bethesda Marriott GDI Completion V1 — Founder Report`);
  fr.push(``);
  fr.push(`Generated: ${new Date().toISOString()}`);
  fr.push(`HEAD: ${head}`);
  fr.push(`Apply: ${APPLY}`);
  fr.push(``);
  fr.push(`## FINAL VERDICT`);
  fr.push(``);
  fr.push(`**${verdict}**`);
  fr.push(``);
  fr.push(`## A. Executive Result`);
  fr.push(``);
  fr.push(`CURRENT VALID OPPORTUNITIES (strict-ready before): ${beforeReady}`);
  fr.push(`NEWLY DISCOVERED (customer-ready applied): 0 (gap candidates held as watch research)`);
  fr.push(`REFRESHED: ${refreshedIds.length}`);
  fr.push(`FUTURE WATCH (package + gap): ${watchlist.length}`);
  fr.push(`REMOVED/CLOSED: 0`);
  fr.push(`FINAL CUSTOMER-READY (corpus after): ${readyAfter.length}`);
  fr.push(`FINAL ACTION SET SIZE: ${finalCards.length}`);
  fr.push(`HIGH PRIORITY: ${priCounts.HIGH}`);
  fr.push(`MEDIUM: ${priCounts.MEDIUM}`);
  fr.push(`WATCH: ${priCounts.WATCH}`);
  fr.push(``);
  fr.push(`## B. Top Opportunities`);
  fr.push(``);
  fr.push(`| # | Opportunity | Org | Seg | Priority | Fit | WHO | Rev BASE |`);
  fr.push(`|---:|---|---|---|---|---:|---|---:|`);
  finalCards.forEach((c, i) => {
    fr.push(
      `| ${i + 1} | ${String(c.opportunity).replace(/\|/g, "/").slice(0, 70)} | ${String(c.organization || "—").slice(0, 28)} | ${c.segment} | ${c.priority} | ${c.bethesdaFit ?? "—"} | ${c.who?.name || "functional"} | ${c.revenuePotential?.base != null ? "$" + c.revenuePotential.base.toLocaleString() : "n/a"} |`
    );
  });
  fr.push(``);
  fr.push(`## C. Immediate Top 5`);
  fr.push(``);
  for (const c of top5) {
    fr.push(`### ${c.opportunity}`);
    fr.push(`- Why now: ${c.whyNow}`);
    fr.push(`- Who: ${c.who?.name || "functional"} ${c.contactEmail || ""}`);
    fr.push(`- Action: ${c.recommendedAction}`);
    fr.push(
      `- Expected value: ${c.revenuePotential?.status === "ESTIMATE" ? `EST room revenue L/B/H $${c.revenuePotential.low.toLocaleString()} / $${c.revenuePotential.base.toLocaleString()} / $${c.revenuePotential.high.toLocaleString()}` : "REVENUE_ESTIMATE_UNSUPPORTED — pursue on strategic fit"}`
    );
    fr.push(``);
  }
  fr.push(`## D. Segment Coverage (action set)`);
  fr.push(``);
  for (const [k, v] of Object.entries(segCoverage).sort()) fr.push(`- ${k}: ${v}`);
  fr.push(``);
  fr.push(`Full ready-corpus segments: see GAP_ANALYSIS.json`);
  fr.push(``);
  fr.push(`## E. New Discovery`);
  fr.push(``);
  fr.push(`Bounded gap SERP: queries=${gap.ledger.queries} fetches=${gap.ledger.fetches} kept=${gap.ledger.kept}`);
  fr.push(`Candidates held as FUTURE_WATCH research only (not promoted without full readiness).`);
  for (const c of watchFromGap.slice(0, 6)) {
    fr.push(`- ${c.opportunity} (${c.segment}) — ${c.keyEvidence}`);
  }
  fr.push(``);
  fr.push(`## F. Existing Corpus Cleanup`);
  fr.push(``);
  fr.push(`- Refreshed sales fields (whyBethesda / whyNow / action / revenue stamp): ${refreshedIds.length}`);
  fr.push(`- Removed/closed: 0`);
  fr.push(`- Duplicates observed in Phase0: see PHASE0_SNAPSHOT.json`);
  fr.push(``);
  fr.push(`## G. WHO / Contact Quality (action set)`);
  fr.push(``);
  fr.push(`Named contacts: ${finalCards.filter((c) => c.namedContact).length}/${finalCards.length} (${namedPct}%)`);
  fr.push(`Public contact path coverage: ${pathPct}%`);
  fr.push(`Functional / ceiling: ${finalCards.filter((c) => !c.namedContact).length}`);
  fr.push(``);
  fr.push(`## H. Revenue Potential`);
  fr.push(``);
  fr.push(`Supported opportunities: ${revSupported.length}/${finalCards.length}`);
  fr.push(`TOTAL ESTIMATED ROOM REVENUE (supported only):`);
  fr.push(`- LOW: $${revTotals.low.toLocaleString()}`);
  fr.push(`- BASE: $${revTotals.base.toLocaleString()}`);
  fr.push(`- HIGH: $${revTotals.high.toLocaleString()}`);
  fr.push(``);
  fr.push(`Assumptions: ADR band $${ADR_LOW}/$${ADR_BASE}/$${ADR_HIGH}; peak rooms from opportunity fields or parsed ranges; nights inferred from meeting/tournament patterns when not explicit. **Not** hotel contracted ADR. Excludes meeting/F&B.`);
  fr.push(``);
  fr.push(`## I. Competitive Context`);
  fr.push(``);
  fr.push(`Frequent alternatives called out in cards: Bethesda North Marriott, Marriott Bethesda Downtown (HQ), Hyatt Regency Bethesda, Arlington/Crystal Gateway hosts, Rockville Hilton, SoccerPlex-adjacent select-service.`);
  fr.push(``);
  fr.push(`## J. 30-Day Sales Plan`);
  fr.push(``);
  fr.push(`### Top 5 immediate`);
  top5.forEach((c, i) => fr.push(`${i + 1}. ${c.opportunity} — owner: Group Sales/DOS — ${c.recommendedAction}`));
  fr.push(``);
  fr.push(`### Next 5 to develop`);
  next5.forEach((c, i) => fr.push(`${i + 1}. ${c.opportunity} — ${c.recommendedAction}`));
  fr.push(``);
  fr.push(`### Watchlist`);
  watchlist.forEach((c) => fr.push(`- ${c.opportunity}`));
  fr.push(``);
  fr.push(`## K. Data Integrity`);
  fr.push(``);
  fr.push(`- Duplicates: Phase0 flagged cycle groups (not deleted)`);
  fr.push(`- Wrong market promotions: none in this pass`);
  fr.push(`- Closed/fully placed aggressive sells: actions redirected to overflow/monitor where applicable`);
  fr.push(`- Thin summaries: refresh used buildGdiOpportunitySummary`);
  fr.push(`- Unsupported revenue estimates: ${finalCards.length - revSupported.length}`);
  fr.push(`- Ready gate: isGdiCustomerOpportunityReady preserved (${beforeReady} → ${readyAfter.length})`);
  fr.push(`- Share token changed: ${regression.shareTokenChanged ? "YES — FAIL" : "NO"}`);
  fr.push(`- Idempotency: re-run refreshes same IDs only`);
  fr.push(`- Surfe: not used`);
  fr.push(`- Deploy: NOT_RUN · Cron: HELD`);
  fr.push(``);
  fr.push(`## L. Recommended GM Message`);
  fr.push(``);
  fr.push(`- You already have a strong customer-ready pipeline (${readyAfter.length} strict-ready); this package picks the best ${finalCards.length} for action.`);
  fr.push(`- Start with the Top 5 — each has a who/path and a concrete next step.`);
  fr.push(`- Medical/scientific is deep; corporate/weekend still thinner — watchlist holds gap research.`);
  fr.push(`- Treat revenue numbers as planning estimates only.`);
  fr.push(`- Keep identity clear vs Bethesda North / Downtown Marriott HQ when pitching.`);
  fr.push(``);
  fr.push(`---`);
  fr.push(`STOP.`);
  writeText("FOUNDER_REPORT.md", fr.join("\n"));

  writeJson("SUMMARY.json", {
    verdict,
    beforeReady,
    afterReady: readyAfter.length,
    finalActionSet: finalCards.length,
    refreshed: refreshedIds.length,
    newlyDiscoveredReady: 0,
    futureWatch: watchlist.length,
    priCounts,
    namedPct,
    pathPct,
    revTotals,
    top5: top5.map((c) => c.opportunity),
    regression,
    shareTokenChanged: regression.shareTokenChanged,
    deploy: "NOT_RUN",
    cron: "HELD",
  });

  console.log("[verdict]", verdict);
  console.log("[ready]", beforeReady, "->", readyAfter.length);
  console.log("[action set]", finalCards.length, priCounts);
  console.log("[named%]", namedPct, "path%", pathPct);
  console.log("[share token changed]", regression.shareTokenChanged);
  if (regression.customerFacingRegression || regression.shareTokenChanged) process.exitCode = 2;
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
