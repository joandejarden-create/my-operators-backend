/**
 * GDI PDF Report — canonical data contract + loader V1.
 * Derives customer-safe report data from live GDI/HI. No Markdown parsing.
 */

import { loadHotelDemandConfig } from "../hotel-profile.js";
import { loadOpportunitiesCanonical } from "../opportunity-persistence.js";
import { filterCustomerFacingOpportunities } from "../customer-visibility.js";
import { filterSalespersonView } from "../opportunity-factory.js";
import { applyLiveCommercialQuality } from "../live-commercial-quality-v1.js";
import { isGdiCustomerOpportunityReady } from "../customer-readiness-gate-v1.js";

export const GDI_PDF_REPORT_TYPE = "GROUP_AND_DEMAND_INTELLIGENCE";
export const GDI_PDF_REPORT_VERSION = 1;

const ADR_LOW = 165;
const ADR_BASE = 195;
const ADR_HIGH = 235;

function oppId(o) {
  return o?.id || o?.opportunityId || null;
}

function isNamedPerson(pc) {
  if (!pc?.name) return false;
  const n = String(pc.name);
  if (
    /organization contact|hotel information|travel & venue|contact desk|preferred lodging|functional/i.test(
      n
    )
  ) {
    return false;
  }
  return /[A-Za-z]/.test(n) && n.split(/\s+/).length >= 2;
}

function priorityBucket(pri) {
  const p = String(pri || "").toUpperCase();
  if (/HIGH/.test(p)) return "HIGH";
  if (/MEDIUM/.test(p)) return "MEDIUM";
  return "WATCH";
}

function segmentOf(o) {
  const blob = `${o.title || ""} ${o.organizationName || ""} ${o.opportunityType || ""} ${o.summaryWhat || ""}`.toLowerCase();
  if (/\bnih\b|national institutes of health|hhs\b|common fund/.test(blob)) return "NIH";
  if (/\b(medical|healthcare|scientific|clinical|pharmacy|renal|nursing|translational)\b/.test(blob))
    return "Medical / Scientific";
  if (/\b(government|contractor|federal|nist|nrc|industry day|cybersecur)\b/.test(blob))
    return "Government / Contractor";
  // Association / legislative BEFORE university — "American College of Cardiology"
  // must not classify as University solely because of "college".
  if (
    /\b(association|society|advocacy|legislative|annual meeting)\b/.test(blob) ||
    /\bcollege of (cardiology|surgeons|physicians|radiology|pathologists)\b/.test(blob) ||
    /\bamerican college of\b/.test(blob)
  ) {
    return "Association";
  }
  if (
    /\b(university|alumni|reunion)\b/.test(blob) ||
    (/\bcollege\b/.test(blob) && !/\bcollege of\b/.test(blob))
  ) {
    return "University";
  }
  if (/\b(tournament|youth|soccer|lacrosse|sports|cup)\b/.test(blob)) return "Sports / Weekend";
  if (/\b(corporate|summit|leadership|user conference)\b/.test(blob)) return "Corporate";
  return "Other";
}

/** Customer-facing title — strip scraped nav chrome; preserve provenance on the source record. */
function customerDisplayTitle(o) {
  let t = String(o.eventName || o.title || "Untitled opportunity");
  t = t.replace(/\s*\|\s*NCIF Conferences[\s\S]*$/i, "");
  t = t.replace(/Skip to main content[\s\S]*$/i, "");
  t = t.replace(/\s*NCI at Frederick[\s\S]*$/i, "");
  t = t.replace(/\s{2,}/g, " ").trim();
  if (!t || t.length < 8) {
    t = String(o.eventName || o.seriesName || o.title || "Untitled opportunity")
      .replace(/Skip to main content[\s\S]*$/i, "")
      .trim();
  }
  if (/Skip to main|NCIF Conferences/i.test(o.title || "") && o.eventName) {
    t = String(o.eventName).trim();
  }
  if (/nci rna biology/i.test(t) && !/symposium/i.test(t)) {
    t = "NCI RNA Biology Symposium";
  }
  return t.slice(0, 180) || "Untitled opportunity";
}

function normalizeTitleKey(title) {
  return String(title || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\b(2026|2027|2028|2029)\b/g, " ")
    .replace(/\b(hotel travel open|hotel and travel open)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/** Collapse identical FUTURE_WATCH display titles (duplicate series rows) without touching store. */
function dedupeFutureWatchCards(cards) {
  const byKey = new Map();
  for (const c of cards || []) {
    const key = normalizeTitleKey(c.opportunity);
    if (!key) continue;
    const prev = byKey.get(key);
    if (!prev) {
      byKey.set(key, c);
      continue;
    }
    const prevDirty = /Skip to main|NCIF Conferences/i.test(prev.opportunity || "");
    const nextDirty = /Skip to main|NCIF Conferences/i.test(c.opportunity || "");
    if (prevDirty && !nextDirty) {
      byKey.set(key, c);
      continue;
    }
    // Prefer the more specific titled form when both are clean.
    if (
      !nextDirty &&
      (c.opportunity || "").length > (prev.opportunity || "").length &&
      !prevDirty
    ) {
      byKey.set(key, c);
    }
  }
  return [...byKey.values()];
}

function emailLikelyBelongsToNamedPerson(name, email) {
  if (!name || !email || !String(email).includes("@")) return true;
  const local = String(email)
    .split("@")[0]
    .toLowerCase()
    .replace(/[^a-z]/g, "");
  const parts = String(name)
    .toLowerCase()
    .split(/\s+/)
    .map((p) => p.replace(/[^a-z]/g, ""))
    .filter((p) => p.length >= 3);
  if (!parts.length || !local) return true;
  return parts.some((p) => local.includes(p.slice(0, Math.min(4, p.length))));
}

function formatHumanReportDate(isoDate) {
  const s = String(isoDate || "").slice(0, 10);
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(s);
  if (!m) return s || null;
  const months = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
  ];
  return `${months[Number(m[2]) - 1]} ${Number(m[3])}, ${m[1]}`;
}

function customerMarketLabel(cfg) {
  const raw = cfg?.demandTerritory?.label || cfg?.market || null;
  const city = cfg?.geo?.city || null;
  if (raw && /^dmv$/i.test(String(raw).trim())) {
    if (city) return `${city} / Washington metropolitan area`;
    return "Washington metropolitan area";
  }
  if (raw && city && !String(raw).toLowerCase().includes(String(city).toLowerCase())) {
    return `${city} · ${raw}`;
  }
  return city || raw || null;
}

function parsePeakRooms(raw) {
  if (raw == null) return null;
  if (typeof raw === "number" && Number.isFinite(raw)) return { value: raw, low: raw, high: raw };
  const s = String(raw);
  const range = s.match(/(\d{2,4})\s*[-–]\s*(\d{2,4})/);
  if (range) {
    const lo = Number(range[1]);
    const hi = Number(range[2]);
    return { value: Math.round((lo + hi) / 2), low: lo, high: hi };
  }
  const one = s.match(/\b(\d{2,4})\b/);
  if (one) {
    const v = Number(one[1]);
    return { value: v, low: v, high: v };
  }
  return null;
}

function estimateNights(o) {
  const n = o.eventNights || o.nights || o.stayNights || o.estimatedNights;
  if (typeof n === "number" && n > 0) return n;
  const blob = `${o.title || ""} ${o.summaryWhat || ""}`.toLowerCase();
  if (/tournament|cup|weekend/.test(blob)) return 2;
  if (/symposium|summit|advocacy|legislative|conference|meeting/.test(blob)) return 3;
  return null;
}

function revenueForOpp(o) {
  const peak = parsePeakRooms(o.estimatedPeakRooms || o.peakRooms);
  const nights = estimateNights(o);
  if (!peak?.value || !nights) return null;
  const roomNightsLow = (peak.low || Math.round(peak.value * 0.75)) * nights;
  const roomNightsBase = peak.value * nights;
  const roomNightsHigh = (peak.high || Math.round(peak.value * 1.25)) * nights;
  return {
    low: Math.round(roomNightsLow * ADR_LOW),
    base: Math.round(roomNightsBase * ADR_BASE),
    high: Math.round(roomNightsHigh * ADR_HIGH),
    peakRooms: peak.value,
    nights,
    assumptions: {
      adrLow: ADR_LOW,
      adrBase: ADR_BASE,
      adrHigh: ADR_HIGH,
      note: "Gross sleeping-room estimate only; not contracted ADR; excludes meeting/F&B.",
    },
  };
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
  if (peak?.value && peak.value >= 80 && peak.value <= 350) s += 10;
  if (o.eventStartDate || o.eventYear) s += 5;
  const dest = `${o.destinationStatus || ""} ${o.commercialStatus || ""}`;
  if (/tbd|open|unresolved|overflow|announced/i.test(dest)) s += 8;
  if (/do not aggressively sell|monitor venue/i.test(o.recommendedAction || "")) s -= 8;
  const y = Number(o.eventYear || String(o.eventStartDate || "").slice(0, 4));
  if (y >= 2027) s += 6;
  if (y === 2026) s += 3;
  return s;
}

function customerSafeBlocker(o) {
  const raw = String(
    o.holdReason || o.blocker || o.primaryBlocker || o.futureWatchTrigger || o.watchTrigger || ""
  ).toUpperCase();
  if (/HOUSING|LODGING|HOTEL.?BLOCK/.test(raw)) {
    return "Official hotel/housing information has not yet been released.";
  }
  if (/DESTINATION|LOCATION|HOST.?CITY|TBD/.test(raw)) {
    return "Host city or venue has not been finalized.";
  }
  if (/RFP|SOURCING/.test(raw)) {
    return "Hotel sourcing / RFP has not opened publicly.";
  }
  if (/CONTACT|WHO/.test(raw)) {
    return "A named decision-maker contact is not yet public.";
  }
  if (/CYCLE|FUTURE|DATE/.test(raw)) {
    return "Waiting on a future-cycle announcement or date confirmation.";
  }
  const why = o.summaryWhyNow || o.whyNow || "";
  if (why) return String(why).slice(0, 160);
  return "Not yet ready for active pursuit; monitoring for the next public trigger.";
}

function mapWho(o) {
  const pc = o.primaryContact || {};
  if (isNamedPerson(pc)) {
    return {
      name: pc.name,
      title: pc.title || pc.role || null,
      organization: pc.organization || o.organizationName || null,
      kind: "NAMED",
    };
  }
  return {
    name: null,
    title: "Conference / Meetings Team",
    organization: o.organizationName || null,
    kind: "FUNCTIONAL",
  };
}

function mapContactPath(o) {
  const pc = o.primaryContact || {};
  const email = pc.email || o.contactEmail || null;
  const phone = pc.phone || o.contactPhone || null;
  const page =
    pc.sourceUrl || o.officialSource || o.discoverySource || o.sources?.[0]?.url || null;
  const named = isNamedPerson(pc) ? pc.name : null;
  const belongsToNamedPerson = named
    ? emailLikelyBelongsToNamedPerson(named, email)
    : true;
  const parts = [];
  if (email) {
    if (named && !belongsToNamedPerson) {
      parts.push(`Organization contact path: ${email}`);
    } else {
      parts.push(email);
    }
  }
  if (phone) parts.push(phone);
  return {
    email,
    phone,
    page: page && !email ? "Official event / housing page available" : null,
    display: parts.length
      ? parts.join(" · ")
      : page
        ? "Official event / housing page"
        : "Not yet supported",
    hasPublicPath: Boolean(email || phone || page),
    belongsToNamedPerson,
    namedPerson: named,
    provenanceNote:
      named && email && !belongsToNamedPerson
        ? `Email is an organization contact path; it is not confirmed as ${named}'s personal address.`
        : null,
  };
}

function mapOpportunityCard(o, classification = "READY") {
  const peak = parsePeakRooms(o.estimatedPeakRooms || o.peakRooms);
  const nights = estimateNights(o);
  const rev = revenueForOpp(o);
  const who = mapWho(o);
  const contact = mapContactPath(o);
  return {
    opportunityId: oppId(o),
    priority: priorityBucket(o.priority),
    opportunity: customerDisplayTitle(o),
    organization: o.organizationName || null,
    segment: segmentOf(o),
    datesCycle: o.eventStartDate || o.eventYear || null,
    destinationStatus: o.destinationStatus || o.venueStatus || "Unknown",
    eventStatus: o.commercialStatus || o.customerFacingState || null,
    peakRooms: peak?.value ?? null,
    peakRoomsLabel: peak?.value != null ? String(peak.value) : "Unknown",
    nights: nights ?? null,
    nightsLabel: nights != null ? String(nights) : "Unknown",
    hotelFit: o.hotelFitScore ?? null,
    whyHotel: o.summaryWhyHotel || o.hotelOpportunityThesis || o.bethesdaWinThesis || "Hotel fit based on inventory and market position.",
    whyNow: o.summaryWhyNow || o.whyNow || "Public demand signal supports timely outreach.",
    who,
    contactPath: contact,
    recommendedAction:
      o.recommendedAction ||
      o.recommendedNextStep ||
      "Confirm sourcing status and introduce the hotel.",
    revenue: rev,
    confidence: o.summaryQuality === "STRONG" ? "High" : "Medium",
    classification,
    actionabilityScore: actionabilityScore(o),
    namedContact: who.kind === "NAMED",
    hasPublicContactPath: contact.hasPublicPath,
    currentBlocker:
      classification === "FUTURE_WATCH" ? customerSafeBlocker(o) : null,
    trigger:
      classification === "FUTURE_WATCH"
        ? o.futureWatchTrigger || o.watchTrigger || "Next public housing or destination update"
        : null,
  };
}

function selectActionSet(readyCards, { topN = 16, top5 = 5 } = {}) {
  const sorted = [...readyCards].sort((a, b) => {
    const pr = { HIGH: 0, MEDIUM: 1, WATCH: 2 };
    if (pr[a.priority] !== pr[b.priority]) return pr[a.priority] - pr[b.priority];
    return (b.actionabilityScore || 0) - (a.actionabilityScore || 0);
  });
  const actionSet = sorted.slice(0, topN);
  const immediate = [...actionSet]
    .sort((a, b) => (b.actionabilityScore || 0) - (a.actionabilityScore || 0))
    .slice(0, top5);
  return { actionSet, immediate };
}

function buildActionPlan(immediate, actionSet) {
  const immIds = new Set(immediate.map((c) => c.opportunityId));
  const develop = actionSet.filter((c) => !immIds.has(c.opportunityId)).slice(0, 5);
  const roleFor = (c) => {
    if (c.priority === "HIGH") return "DOS / Group Sales";
    if (/Sports/i.test(c.segment)) return "Group Sales";
    if (/Government|NIH|Medical/i.test(c.segment)) return "DOS / Group Sales";
    return "Group Sales";
  };
  return {
    actNow: immediate.map((c) => ({
      opportunity: c.opportunity,
      ownerRole: roleFor(c),
      action: c.recommendedAction,
      desiredOutcome: "Confirm interest / sourcing status / housing-list inclusion",
      timing: "Week 1",
    })),
    develop: develop.map((c) => ({
      opportunity: c.opportunity,
      ownerRole: roleFor(c),
      action: c.recommendedAction,
      desiredOutcome: "Qualify and advance or move to watch",
      timing: "Week 2–3",
    })),
  };
}

/**
 * @param {string} hotelId
 * @param {object} [opts]
 * @returns {Promise<object|null>} null if no meaningful GDI data
 */
export async function getGdiPdfReportData(hotelId, opts = {}) {
  const id = String(hotelId || "").trim();
  if (!id) throw new Error("hotelId_required");
  const nowDate = opts.nowDate || new Date().toISOString().slice(0, 10);
  const cfg = loadHotelDemandConfig(id);
  if (!cfg) {
    return {
      available: false,
      reason: "Hotel is not configured for Group & Demand Intelligence.",
      hotelId: id,
    };
  }

  const doc = await loadOpportunitiesCanonical(id);
  const enriched = (doc.opportunities || []).map((o) =>
    applyLiveCommercialQuality(o, { nowDate })
  );
  const visible = filterCustomerFacingOpportunities(filterSalespersonView(enriched), {
    nowDate,
  });
  const ready = visible.filter((o) => isGdiCustomerOpportunityReady(o, { nowDate }).ok);
  const futureWatchRaw = enriched.filter((o) =>
    /FUTURE_WATCH/i.test(String(o.customerFacingState || o.state || ""))
  );

  if (!ready.length && !futureWatchRaw.length) {
    return {
      available: false,
      reason: "No customer-ready GDI report is currently available for this hotel.",
      hotelId: id,
      hotelName: cfg.displayName || id,
    };
  }

  const readyCards = ready.map((o) => mapOpportunityCard(o, "READY"));
  const watchCards = dedupeFutureWatchCards(
    futureWatchRaw.slice(0, 24).map((o) => mapOpportunityCard(o, "FUTURE_WATCH"))
  ).slice(0, 12);
  const { actionSet, immediate } = selectActionSet(readyCards, {
    topN: opts.actionSetSize ?? 16,
    top5: opts.top5Size ?? 5,
  });

  const namedCount = actionSet.filter((c) => c.namedContact).length;
  const pathCount = actionSet.filter((c) => c.hasPublicContactPath).length;
  const priCounts = { HIGH: 0, MEDIUM: 0, WATCH: 0 };
  for (const c of actionSet) priCounts[c.priority] = (priCounts[c.priority] || 0) + 1;

  const revSupported = actionSet.map((c) => c.revenue).filter(Boolean);
  const revenueScenario =
    revSupported.length > 0
      ? {
          low: revSupported.reduce((s, r) => s + r.low, 0),
          base: revSupported.reduce((s, r) => s + r.base, 0),
          high: revSupported.reduce((s, r) => s + r.high, 0),
          opportunityCount: revSupported.length,
          disclaimer:
            "Estimated room-revenue scenarios based on current assumptions and available opportunity data. These are not forecasts.",
          scopeNote:
            "Scenario totals include only opportunities with sufficient supporting inputs.",
          assumptions: {
            adrLow: ADR_LOW,
            adrBase: ADR_BASE,
            adrHigh: ADR_HIGH,
          },
        }
      : null;

  const segmentMix = {};
  for (const c of actionSet) {
    segmentMix[c.segment] = (segmentMix[c.segment] || 0) + 1;
  }

  const market = customerMarketLabel(cfg);

  return {
    available: true,
    reportMetadata: {
      reportType: GDI_PDF_REPORT_TYPE,
      reportTypeLabel: "Group & Demand Intelligence",
      reportVersion: GDI_PDF_REPORT_VERSION,
      generatedAt: new Date().toISOString(),
      dataSnapshotAt: doc.updatedAt || new Date().toISOString(),
      reportDate: nowDate,
      reportDateLabel: formatHumanReportDate(nowDate),
      hotelId: id,
    },
    hotel: {
      name: cfg.displayName || id,
      market,
      marketRaw: cfg.demandTerritory?.label || null,
      city: cfg.geo?.city || null,
      address: cfg.geo?.address || null,
      rooms: cfg.capabilityProfile?.totalGuestrooms ?? null,
      meetingSqFt: cfg.capabilityProfile?.totalMeetingSpaceSqFt ?? null,
      largestMeetingSqFt: cfg.capabilityProfile?.largestMeetingRoomSqFt ?? null,
      demandAnchors: (cfg.commercialPriorities?.demandAnchorFocus || []).slice(0, 6),
    },
    executiveSummary: {
      customerReadyCount: ready.length,
      actionSetCount: actionSet.length,
      highPriority: priCounts.HIGH,
      mediumPriority: priCounts.MEDIUM,
      futureWatchCount: watchCards.length,
      namedContactCoveragePct: actionSet.length
        ? Math.round((100 * namedCount) / actionSet.length)
        : 0,
      publicContactPathCoveragePct: actionSet.length
        ? Math.round((100 * pathCount) / actionSet.length)
        : 0,
      revenueScenario,
    },
    opportunitySummary: {
      ready: ready.length,
      futureWatch: watchCards.length,
      priorityDistribution: priCounts,
      segmentMix,
    },
    topOpportunities: actionSet,
    immediatePursuits: immediate,
    actionPlan: buildActionPlan(immediate, actionSet),
    pipeline: {
      ready: ready.length,
      futureWatch: watchCards.length,
      actionSet: actionSet.length,
    },
    futureWatch: watchCards.slice(0, 8),
    supportingIntelligence: {
      rooms: cfg.capabilityProfile?.totalGuestrooms ?? null,
      meetingSqFt: cfg.capabilityProfile?.totalMeetingSpaceSqFt ?? null,
      largestMeetingSqFt: cfg.capabilityProfile?.largestMeetingRoomSqFt ?? null,
      demandAnchors: (cfg.commercialPriorities?.demandAnchorFocus || []).slice(0, 6),
      territoryLabel: market,
    },
    methodology: [
      "Opportunities are based on public, evidence-supported research.",
      "Confirmed and estimated fields are distinguished where possible.",
      "Room-demand and revenue values may be estimates.",
      "Future event status can change; hotel-team validation is expected.",
    ],
  };
}

export function buildGdiPdfFilename(hotelName, reportDate) {
  const slug = String(hotelName || "Hotel")
    .replace(/[^A-Za-z0-9]+/g, "_")
    .replace(/^_|_$/g, "")
    .slice(0, 48);
  const d = String(reportDate || new Date().toISOString().slice(0, 10)).slice(0, 10);
  return `Dealality_GDI_${slug}_${d}.pdf`;
}
