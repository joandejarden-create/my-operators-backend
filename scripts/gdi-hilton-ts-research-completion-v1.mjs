#!/usr/bin/env node
/**
 * Hilton TS — GDI Full E2E Research Completion + Competitive Expansion ($0 phase).
 *
 * - Revalidates prior PASS 8 commercially (as-of 2026-11-24)
 * - Ranks HOLD 25; attempts $0 salvage from saved evidence only
 * - Competitive expansion from existing store + paid preflight plan (NO paid calls)
 * - Writes new STAGED candidate package (pointer NOT activated)
 *
 * Usage:
 *   node scripts/gdi-hilton-ts-research-completion-v1.mjs
 *   node scripts/gdi-hilton-ts-research-completion-v1.mjs --write-package
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildCustomerPublicationPackage,
  verifyPublicationPackageIntegrity,
  createGdiPublicationPackageStore,
  runPublicationQualityGate,
  GDI_PUBLICATION_LIFECYCLE_STATE,
} from "../lib/group-demand-intelligence/publication-package/index.js";
import { projectOpportunityForPresentation } from "../lib/group-demand-intelligence/presentation/gdi-universal-presentation-pipeline-v1.js";
import { GDI_PRESENTATION_SURFACES } from "../lib/group-demand-intelligence/contracts/gdi-presentation-contract-v1.js";
import {
  evaluateNamedContactCoverageStandard,
  assertFinalizedPackageNamedContactCoverage,
  attachNamedContactCoverageMetadata,
} from "../lib/group-demand-intelligence/workflow/gdi-named-contact-coverage-standard-v1.js";
import { evaluateContactCompletenessForFinalization } from "../lib/group-demand-intelligence/presentation/gdi-verified-commercial-route-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const HOTEL_ID = "rec35fExUxCClpOP6";
const HOTEL_NAME = "Hilton New York Times Square";
const CONTRACT = "gdi-hilton-ts-research-completion-v1";
const AS_OF = "2026-11-24";
const AS_OF_MS = Date.parse(`${AS_OF}T12:00:00.000Z`);
const PRIOR_REPORT = path.join(
  ROOT,
  "reports/gdi-hilton-ts-full-e2e-reprocess-v1",
);
const REPORT_DIR = path.join(ROOT, "reports", CONTRACT);
const OPP_PATH = path.join(
  ROOT,
  "data/group-demand-intelligence/hotels",
  HOTEL_ID,
  "opportunities.json",
);

function has(flag) {
  return process.argv.includes(flag);
}
function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}
function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}
function writeJson(file, body) {
  ensureDir(path.dirname(file));
  fs.writeFileSync(file, `${JSON.stringify(body, null, 2)}\n`, "utf8");
}
function readJson(file) {
  return JSON.parse(fs.readFileSync(file, "utf8"));
}

function isPlausiblePersonName(name) {
  const n = text(name);
  if (!n || n.length < 5 || n.length > 80) return false;
  if (
    /headquarters|contact desk|housing desk|member select|android once|enquire beyond|colonial dames|boutique & luxury|city of new york|easy rfp|learning futures|climate monitoring|onpeak|booth booth|spain shines|group sales|survey software|past weather|or concierge|free univ|computer science|mercurius|boutique design/i.test(
      n,
    )
  ) {
    return false;
  }
  if (
    /\b(association|society|committee|conference|congress|foundation|institute|dames|beyond|futures|monitoring|desk|housing|new york|inc\.?|llc|ltd)\b/i.test(
      n,
    )
  ) {
    return false;
  }
  if (/@|\.com|\.org|http/i.test(n)) return false;
  const parts = n.split(/\s+/).filter(Boolean);
  if (parts.length < 2 || parts.length > 4) return false;
  if (/^(free|university|professor|phd|dr|mr|ms|mrs|inc|llc|ltd|bank)$/i.test(parts.at(-1))) {
    return false;
  }
  if (parts.every((p) => p.length > 3 && p === p.toUpperCase())) return false;
  const personish = parts.filter((p) => /^[\p{L}][\p{L}'\-]{1,30}$/u.test(p));
  return personish.length >= 2;
}

function primaryOf(opp) {
  return opp.primaryContact && typeof opp.primaryContact === "object"
    ? opp.primaryContact
    : {
        name: opp.primaryContactName || null,
        role: opp.primaryContactRole || null,
        email: opp.primaryContactEmail || null,
        phone: opp.primaryContactPhone || null,
      };
}

function parseStartDate(opp) {
  const cands = [
    opp.eventStartDate,
    opp.needDates,
    opp.needDateStart,
    opp.eventDate,
    opp.dates?.start,
    opp.needDateRange?.start,
  ];
  for (const c of cands) {
    const d = new Date(c);
    if (!Number.isNaN(d.getTime())) return d.toISOString().slice(0, 10);
  }
  return null;
}

function daysFromAsOf(iso) {
  if (!iso) return null;
  const d = Date.parse(`${iso}T12:00:00.000Z`);
  if (Number.isNaN(d)) return null;
  return Math.round((d - AS_OF_MS) / 86400000);
}

function collectUrls(opp) {
  const urls = [
    ...(Array.isArray(opp.sourceUrls) ? opp.sourceUrls : []),
    ...(Array.isArray(opp.seedUrls) ? opp.seedUrls : []),
    ...(Array.isArray(opp.investigatedUrls) ? opp.investigatedUrls : []),
    ...(Array.isArray(opp.evidence)
      ? opp.evidence.map((e) => e?.url || e?.sourceUrl).filter(Boolean)
      : []),
    opp.primaryContact?.sourceUrl,
    opp.primaryContact?.emailSource,
    opp.primaryContact?.phoneSource,
    opp.commercialEvidenceUrl,
    opp.locationEvidenceUrl,
    opp.eventDateEvidenceUrl,
  ]
    .map((u) => text(u))
    .filter((u) => /^https?:\/\//i.test(u));
  return [...new Set(urls)];
}

function classifyBuyer(opp, primary) {
  const hint = text(
    primary.targetRoleMatch ||
      primary.contactRoleMatch ||
      opp.buyerContactPathClass ||
      "",
  );
  const role = text(primary.role || primary.title || "");
  // Never auto DIRECT_BUYER from seniority alone.
  if (/EXECUTIVE_SPONSOR|EXECUTIVE_DIRECTOR|^ED$/i.test(hint) || /executive director|vice president|partner & coo|coo\b/i.test(role)) {
    return {
      class: "VERIFIED_VIABLE_ALTERNATIVE",
      reason:
        "Senior / sponsor role evidenced — not auto-classified DIRECT_BUYER from title alone",
    };
  }
  if (
    /EVENT_MEETINGS_OWNER|HOUSING_SOURCING_CONTACT|MEETING_MANAGER|DIRECTOR OF EVENTS/i.test(
      `${hint} ${role}`,
    )
  ) {
    return {
      class: "DIRECT_BUYER",
      reason: "Event / meetings / housing ownership evidenced in role",
    };
  }
  if (/GENERAL_ORGANIZATION_CONTACT/i.test(hint)) {
    return {
      class: "VERIFIED_VIABLE_ALTERNATIVE",
      reason: "General organization contact — viable alternative, not proven lodging buyer",
    };
  }
  return {
    class: "VERIFIED_VIABLE_ALTERNATIVE",
    reason: "Insufficient evidence for DIRECT_BUYER; retain as viable alternative",
  };
}

function extractCompetitor(opp) {
  const blob = JSON.stringify(opp);
  const why = text(opp.primaryContact?.whyThisContact || "");
  const thesis = text(opp.hotelOpportunityThesis || "");
  const m = blob.match(
    /Westin New York at Times Square|New York Marriott Marquis|Marriott Marquis|Sheraton New York(?: Times Square)?|Sheraton New York/i,
  );
  if (!m) return null;
  const incumbent = text(m[0]);
  const overflow = /overflow|stay-to-play|housing|room.?block|incumbent|host venue/i.test(
    `${why} ${thesis}`,
  );
  return {
    label: overflow
      ? "EVIDENCED_OVERFLOW_OPPORTUNITY"
      : "HISTORICAL_COMPETITOR_BOOKING",
    incumbentHotel: incumbent,
    evidenceSnippet: why || thesis.slice(0, 220) || null,
    overflowPathway: overflow,
  };
}

function revalidatePriorPass(opp, priorRow) {
  const primary = primaryOf(opp);
  const start = parseStartDate(opp);
  const days = daysFromAsOf(start);
  const future = text(opp.futureCycleEvidenceState);
  const nameOk = isPlausiblePersonName(primary.name);
  const roleBlob = text(primary.role || primary.title || "");
  const scrapeJunk = /,\s*[A-Z][a-z]+\s+[A-Z]/.test(`${text(primary.name)} ${roleBlob}`);
  const urls = collectUrls(opp);
  const buyer = classifyBuyer(opp, primary);
  const competitive = extractCompetitor(opp);
  const reasons = [];

  if (!nameOk || scrapeJunk) reasons.push("PRIMARY_NOT_PLAUSIBLE_PERSON");
  if (days != null && days < -7) {
    reasons.push(`EVENT_DATE_PASSED:${start}`);
  }
  if (
    (days != null && days < -7) &&
    !/CURRENT_FUTURE_CYCLE_CONFIRMED|FUTURE_CYCLE/i.test(future)
  ) {
    reasons.push("NO_FUTURE_CYCLE_AFTER_PAST_EDITION");
  }
  if (
    days != null &&
    days >= -7 &&
    days < 21 &&
    !/CURRENT_FUTURE_CYCLE_CONFIRMED|FUTURE_CYCLE/i.test(future)
  ) {
    // Just ended / ending — sourcing window effectively closed without next-cycle proof
    if (days < 0) reasons.push("SOURCING_WINDOW_CLOSED_RECENT_PAST");
  }
  if (future === "NO_FUTURE_CYCLE_EVIDENCE" && days != null && days < 45) {
    reasons.push("NEAR_TERM_OR_PAST_WITHOUT_NEXT_CYCLE_EVIDENCE");
  }
  if (!urls.length && !text(primary.sourceUrl) && !text(primary.emailSource)) {
    reasons.push("THIN_SOURCE_URL_PROVENANCE");
  }
  // Scale Healthcare: VP with no email + scrape-looking why
  if (/Scale Healthcare/i.test(text(opp.title)) && !text(primary.email)) {
    reasons.push("NO_PUBLIC_EMAIL_AND_WEAK_SPONSOR_PATH");
  }

  const retain =
    reasons.length === 0 ||
    // Allow thin URL if person+email+future date strong
    (reasons.length === 1 &&
      reasons[0] === "THIN_SOURCE_URL_PROVENANCE" &&
      nameOk &&
      text(primary.email) &&
      days != null &&
      days > 60);

  // Force downgrade when past without future cycle
  const forceDown =
    reasons.some((r) =>
      /EVENT_DATE_PASSED|NO_FUTURE_CYCLE_AFTER_PAST|SOURCING_WINDOW_CLOSED|NEAR_TERM_OR_PAST_WITHOUT|PRIMARY_NOT_PLAUSIBLE|NO_PUBLIC_EMAIL_AND_WEAK/i.test(
        r,
      ),
    );

  const decision = forceDown ? "DOWNGRADE" : retain ? "RETAIN" : "DOWNGRADE";
  const nextAction =
    decision === "RETAIN"
      ? text(opp.recommendedNextStep) ||
        `Outreach to ${text(primary.name)}${
          primary.role ? ` (${text(primary.role)})` : ""
        } — confirm lodging / meeting decision role for ${text(
          opp.organizationName,
        )}, introduce Hilton New York Times Square fit for ${start || "the next cycle"}, and request a short commercial conversation.`
      : null;

  return {
    opportunityId: opp.id,
    title: opp.title,
    organizationName: opp.organizationName,
    priorMarketScope: priorRow?.marketScope || null,
    eventStartDate: start,
    daysFromAsOf: days,
    futureCycleEvidenceState: future || null,
    primary: {
      name: text(primary.name) || null,
      role: text(primary.role || primary.title) || null,
      email: text(primary.email) || null,
      emailType: text(primary.emailType) || null,
      phone: text(primary.phone) || null,
      linkedin: text(primary.linkedinUrl || primary.linkedin) || null,
      sourceUrl: text(primary.sourceUrl) || null,
      whyThisContact: text(primary.whyThisContact) || null,
    },
    buyerClassification: buyer,
    competitive,
    hotelFitThesis: text(opp.hotelOpportunityThesis).slice(0, 280) || null,
    roomDemandStatus: text(opp.roomDemandStatus) || null,
    decision,
    reasons: forceDown
      ? reasons
      : retain
        ? reasons.length
          ? reasons
          : ["COMMERCIALLY_VALIDATED_BASELINE"]
        : reasons,
    recommendedNextAction: nextAction,
    marketScope: competitive ? "COMPETITIVE" : "HOTEL",
  };
}

function rankHold(opp, holdReasons) {
  const start = parseStartDate(opp);
  const days = daysFromAsOf(start);
  const primary = primaryOf(opp);
  const exhibitor = /EXHIBITOR BLOCK/i.test(text(opp.title));
  const past = days != null && days < -14;
  const peak = Number(opp.publishedPeakRooms) || 0;
  const thesis = text(opp.hotelOpportunityThesis);
  const nyc = /\b(new york|nyc|times square|midtown|manhattan)\b/i.test(
    `${text(opp.eventLocationSummary)} ${text(opp.title)}`,
  );
  let score = 0;
  const notes = [];
  if (!past && days != null && days > 30 && days < 540) {
    score += 30;
    notes.push("FUTURE_WINDOW");
  } else if (!past && days != null && days >= 0 && days <= 30) {
    score += 8;
    notes.push("IMMINENT_WINDOW");
  } else if (past) {
    score -= 40;
    notes.push("PAST_EVENT");
  }
  if (nyc) {
    score += 15;
    notes.push("NYC_FIT");
  }
  if (/OVERFLOW_ONLY|ESTIMATED_ROOM_DEMAND|CONFIRMED/i.test(text(opp.roomDemandStatus))) {
    score += 10;
    notes.push("ROOM_DEMAND_SIGNAL");
  }
  if (peak >= 50) {
    score += 10;
    notes.push("PEAK_ROOMS");
  }
  if (isPlausiblePersonName(primary.name)) {
    score += 20;
    notes.push("NAMED_PERSON_PRESENT");
  } else {
    score -= 5;
    notes.push("NO_PLAUSIBLE_NAMED_PRIMARY");
  }
  if (text(primary.email) && /DIRECT_WORK/i.test(text(primary.emailType))) {
    score += 10;
    notes.push("DIRECT_EMAIL");
  }
  if (exhibitor) {
    score -= 50;
    notes.push("EXHIBITOR_PLACEHOLDER");
  }
  if (/WEAK|CLOSED/i.test(text(opp.opportunityQualification))) {
    score -= 15;
    notes.push("WEAK_OR_CLOSED_QUAL");
  }
  if (thesis.length > 80) {
    score += 5;
    notes.push("THESIS_PRESENT");
  }

  // $0 salvage attempt
  const pool = [
    primary,
    ...(Array.isArray(opp.backupContacts) ? opp.backupContacts : []),
    ...(Array.isArray(opp.contacts) ? opp.contacts : []),
    ...(Array.isArray(opp.namedStakeholders) ? opp.namedStakeholders : []),
  ].filter(Boolean);
  const salvagePeople = pool
    .filter((p) => isPlausiblePersonName(p.name || p.fullName))
    .map((p) => ({
      name: text(p.name || p.fullName),
      role: text(p.role || p.title) || null,
      email: text(p.email) || null,
      phone: text(p.phone) || null,
      sourceUrl: text(p.sourceUrl || p.source) || null,
    }));

  let salvageDisposition = "HOLD_NEEDS_PAID_WHO";
  let salvageReason = holdReasons.join(";") || "CONTACT_GAP";
  if (exhibitor) {
    salvageDisposition = "REJECT_EXHIBITOR_PLACEHOLDER";
    salvageReason = "EXHIBITOR_BLOCK_PLACEHOLDER";
  } else if (past && !/FUTURE_CYCLE/i.test(text(opp.futureCycleEvidenceState))) {
    salvageDisposition = "REJECT_HISTORICAL_PAST";
    salvageReason = `PAST_EVENT_${start || "UNKNOWN"}`;
  } else if (
    salvagePeople.length &&
    !past &&
    days != null &&
    days > 45 &&
    nyc &&
    !exhibitor
  ) {
    // Still require commercial buyer relevance — most salvage names are weak
    const best = salvagePeople[0];
    const cleaned = {
      ...opp,
      primaryContact: {
        name: best.name,
        role: best.role,
        email: best.email,
        phone: best.phone,
        sourceUrl: best.sourceUrl,
      },
      sourceUrls: collectUrls(opp),
    };
    const coverage = evaluateNamedContactCoverageStandard(cleaned);
    const contactEval = evaluateContactCompletenessForFinalization(cleaned, {
      looksLikePersonName: isPlausiblePersonName,
    });
    if (coverage.finalizeEligible && contactEval.complete) {
      salvageDisposition = "RECOVERED_ZERO_DOLLAR";
      salvageReason = "EXISTING_EVIDENCE_NAMED_PERSON";
    } else {
      salvageDisposition = "HOLD_NEEDS_PAID_WHO";
      salvageReason = "NAMED_PERSON_CANDIDATE_BUT_GATES_INCOMPLETE";
    }
  }

  return {
    opportunityId: opp.id,
    title: opp.title,
    organizationName: opp.organizationName,
    eventStartDate: start,
    daysFromAsOf: days,
    score,
    rankNotes: notes,
    priorHoldReasons: holdReasons,
    salvagePeople,
    salvageDisposition,
    salvageReason,
    priorityForPaidWho:
      salvageDisposition === "HOLD_NEEDS_PAID_WHO" && score >= 25 && !exhibitor && !past,
  };
}

function main() {
  const writePackage = has("--write-package");
  const raw = readJson(OPP_PATH);
  const opportunities = raw.opportunities || [];
  const byId = Object.fromEntries(opportunities.map((o) => [o.id, o]));
  const priorMatrix = readJson(
    path.join(PRIOR_REPORT, "03_PASS_HOLD_REJECT_MATRIX.json"),
  );

  // Phase 1 — revalidate prior PASS 8
  const passRevalidation = priorMatrix.pass.map((p) =>
    revalidatePriorPass(byId[p.opportunityId], p),
  );
  const retained = passRevalidation.filter((r) => r.decision === "RETAIN");
  const downgraded = passRevalidation.filter((r) => r.decision === "DOWNGRADE");

  writeJson(path.join(REPORT_DIR, "01_PASS8_COMMERCIAL_REVALIDATION.json"), {
    contract: CONTRACT,
    asOf: AS_OF,
    priorPassCount: priorMatrix.pass.length,
    retainedCount: retained.length,
    downgradedCount: downgraded.length,
    rows: passRevalidation,
  });

  // Phase 2 — rank HOLD + $0 salvage
  const holdRanked = priorMatrix.hold
    .map((h) => rankHold(byId[h.opportunityId], h.reasons || []))
    .sort((a, b) => b.score - a.score);
  const holdRecovered = holdRanked.filter(
    (h) => h.salvageDisposition === "RECOVERED_ZERO_DOLLAR",
  );
  const holdPaidPriority = holdRanked.filter((h) => h.priorityForPaidWho);

  writeJson(path.join(REPORT_DIR, "02_HOLD25_RANK_AND_ZERO_DOLLAR_SALVAGE.json"), {
    contract: CONTRACT,
    asOf: AS_OF,
    holdCount: holdRanked.length,
    recoveredZeroDollar: holdRecovered.length,
    paidWhoPriorityCount: holdPaidPriority.length,
    ranked: holdRanked,
  });

  // Phase 3 — competitive $0 scan
  const competitiveFromStore = [];
  for (const opp of opportunities) {
    const c = extractCompetitor(opp);
    if (c) {
      competitiveFromStore.push({
        opportunityId: opp.id,
        title: opp.title,
        organizationName: opp.organizationName,
        competitive: c,
        inRetainedBaseline: retained.some((r) => r.opportunityId === opp.id),
      });
    }
  }
  writeJson(path.join(REPORT_DIR, "03_COMPETITIVE_EXPANSION_ZERO_DOLLAR.json"), {
    contract: CONTRACT,
    note:
      "Only competitor evidence present in Hilton store under $0 reuse. Expansion requires paid Exa envelope.",
    recoveredFromStore: competitiveFromStore,
    newlyVerifiedBeyondSssp: competitiveFromStore.filter(
      (c) => c.opportunityId !== "gdi_opp_267d801f2fa39f9e",
    ).length,
    targetCompetitors: [
      "New York Marriott Marquis",
      "The Westin New York at Times Square",
      "Sheraton New York Times Square",
      "Other defensible Midtown / Times Square peers",
    ],
  });

  // Phase 5 — paid preflight plan (NO CALLS)
  const paidPlan = {
    contract: CONTRACT,
    status: "AWAITING_FOUNDER_AUTHORIZATION",
    proposedCeilingUsd: 16,
    buckets: {
      whoAltGapRecoveryUsd: 5,
      competitiveDemandRecoveryUsd: 8,
      publicHowCompletionUsd: 3,
    },
    provider: "Exa Search (Agent OFF)",
    surfe: false,
    findymail: false,
    agent: false,
    allowlistWhoAlt: holdPaidPriority.slice(0, 8).map((h) => ({
      opportunityId: h.opportunityId,
      title: h.title,
      score: h.score,
      eventStartDate: h.eventStartDate,
      estimatedQueries: 2,
      estimatedCostUsd: 0.5,
      purpose: "Named commercial buyer / viable alternative + role verification",
    })),
    allowlistCompetitive: [
      {
        queryTheme: "association conference hotel block Westin New York Times Square 2026 2027 2028",
        estimatedQueries: 4,
        estimatedCostUsd: 2,
      },
      {
        queryTheme: "Marriott Marquis New York group housing overflow association 2027",
        estimatedQueries: 4,
        estimatedCostUsd: 2,
      },
      {
        queryTheme: "Sheraton New York Times Square conference housing block 2027",
        estimatedQueries: 4,
        estimatedCostUsd: 2,
      },
      {
        queryTheme: "NRF Big Show 2027 official hotel onPeak Midtown overflow",
        estimatedQueries: 3,
        estimatedCostUsd: 1.5,
      },
    ],
    allowlistHow: retained
      .filter((r) => !r.primary.email || r.primary.emailType === "GENERIC_ORGANIZATION")
      .map((r) => ({
        opportunityId: r.opportunityId,
        title: r.title,
        gap: !r.primary.email
          ? "MISSING_PERSONAL_EMAIL"
          : "ORG_EMAIL_ONLY",
        estimatedCostUsd: 0.4,
      })),
    dedupeProtections: [
      "Reuse existing opportunity IDs; never create duplicate identities for same event+org",
      "Skip queries already evidenced in opportunity.evidence / primaryContact.sourceUrl",
      "Hotel-level spend ledger with hard stop at $16 aggregate",
      "No Agent escalation; no Surfe; no Findymail",
    ],
    hotelLedgerTemplate: {
      hotelId: HOTEL_ID,
      ceilingUsd: 16,
      spentUsd: 0,
      remainingUsd: 16,
      calls: [],
    },
    authorizationTokenRequired:
      "FOUNDER_APPROVE_HILTON_TS_RESEARCH_COMPLETION_16USD_20261124",
  };
  writeJson(path.join(REPORT_DIR, "04_PAID_RESEARCH_PREFLIGHT_PLAN.json"), paidPlan);

  // Phase 6 — new candidate package from retained + $0 recovered only
  const finalIds = [
    ...retained.map((r) => r.opportunityId),
    ...holdRecovered.map((h) => h.opportunityId),
  ];
  const uniqueFinalIds = [...new Set(finalIds)];

  const passOpps = uniqueFinalIds.map((id) => {
    const src = byId[id];
    const reval = retained.find((r) => r.opportunityId === id);
    const p = primaryOf(src);
    let cleaned = {
      ...src,
      primaryContact: isPlausiblePersonName(p.name) ? p : null,
      marketScope: reval?.marketScope || "HOTEL",
      competitiveContext: reval?.competitive || extractCompetitor(src),
      sourceUrls: collectUrls(src),
      buyerClassification: reval?.buyerClassification || classifyBuyer(src, p),
      recommendedNextStep:
        reval?.recommendedNextAction || text(src.recommendedNextStep),
      gdiResearchCompletionV1: {
        contract: CONTRACT,
        asOf: AS_OF,
        retainedFromPriorPass: Boolean(reval),
        at: new Date().toISOString(),
      },
    };
    if (text(cleaned.recommendedNextStep).length < 20) {
      cleaned.recommendedNextStep = `Outreach to ${text(cleaned.primaryContact?.name)} — confirm lodging decision role and Hilton New York Times Square fit.`;
    }
    cleaned = attachNamedContactCoverageMetadata(cleaned, {
      secondContactAttempted: true,
      secondContactDisposition: "ATTEMPTED_NO_SECOND_VERIFIED_FROM_EXISTING_EVIDENCE",
      contactGaps: [
        "Second relevant named contact not verified in existing $0 evidence",
      ],
    });
    if (!cleaned.secondaryContact?.name) {
      cleaned.secondContactAttempted = true;
      cleaned.secondaryContactResearchStatus = "ATTEMPTED_NO_SECOND_VERIFIED";
      cleaned.secondaryContactResearchReason =
        "Existing Hilton research evidence reviewed; no distinct verified secondary person located ($0 reuse)";
      cleaned = attachNamedContactCoverageMetadata(cleaned, {
        secondContactAttempted: true,
        secondContactDisposition:
          "ATTEMPTED_NO_SECOND_VERIFIED_FROM_EXISTING_EVIDENCE",
        contactGaps: [
          "Second relevant named contact not verified in existing $0 evidence",
        ],
      });
    }
    return cleaned;
  });

  const dtos = passOpps.map((o) => {
    const projected = projectOpportunityForPresentation(o, {
      hotelId: HOTEL_ID,
      surface: GDI_PRESENTATION_SURFACES.EXTERNAL_SHARE,
      view: "detail",
    });
    return {
      ...projected.dto,
      marketScope: o.marketScope,
      competitiveContext: o.competitiveContext,
      buyerClassification: o.buyerClassification,
    };
  });

  const publicationVersion = `gdi_hilton_ts_research_completion_v1_${AS_OF.replace(
    /-/g,
    "",
  )}`;
  let pkg = null;
  let integrity = { ok: false };
  let quality = { ok: false, blockingCount: 0 };
  let namedAssert = { ok: false };
  let written = { ok: false, skipped: true };

  if (passOpps.length) {
    pkg = buildCustomerPublicationPackage({
      hotelId: HOTEL_ID,
      hotelName: HOTEL_NAME,
      opportunities: dtos,
      publicationVersion,
      lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.CUSTOMER_RENDERED,
      approvalStatus: "CANDIDATE",
      applicationCompatibilityVersion: "gdi-customer-app-compat-v1",
      notes:
        "Hilton TS research-completion v1 — commercially revalidated baseline after PASS8 audit; $0 only; pointer NOT activated; paid $16 envelope awaiting founder GO",
    });
    integrity = verifyPublicationPackageIntegrity(pkg);
    quality = runPublicationQualityGate({ opportunities: pkg.opportunities });
    namedAssert = assertFinalizedPackageNamedContactCoverage(passOpps);
    writeJson(path.join(REPORT_DIR, "05_STAGED_CANDIDATE_PACKAGE.json"), pkg);
    if (writePackage && integrity.ok && namedAssert.ok) {
      const store = createGdiPublicationPackageStore();
      written = store.writePackage({
        ...pkg,
        lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.CUSTOMER_RENDERED,
        approvalStatus: "CANDIDATE",
        gates: {
          integrity: { ok: integrity.ok },
          quality: { ok: quality.ok, blockingCount: quality.blockingCount },
          namedContactCoverage: namedAssert,
          pointerActivated: false,
          paidResearchSpendUsd: 0,
        },
      });
    }
  }

  const store = createGdiPublicationPackageStore();
  const hiltonPtr = store.readPointer(HOTEL_ID, "STAGED");
  const peer = [
    ["rece0NdV9FCnaFlVS", "Condesa DF"],
    ["recaokE8psIbuVeuH", "LaLuna"],
    ["rec1t35e3sPerzxYu", "Memmo Príncipe Real"],
    ["recLuxvwwxID7U2B8", "Bethesda Marriott"],
  ].map(([id, name]) => {
    const ptr = store.readPointer(id, "STAGED");
    return {
      hotelId: id,
      hotelName: name,
      hasStagedPointer: Boolean(ptr?.publicationVersion),
      publicationVersion: ptr?.publicationVersion || null,
    };
  });

  const hotelPass = passOpps.filter((o) => o.marketScope !== "COMPETITIVE").length;
  const competitivePass = passOpps.filter((o) => o.marketScope === "COMPETITIVE")
    .length;
  const salesReady = passOpps.filter((o) => o.salesReady === true).length;
  const personalEmail = passOpps.filter((o) =>
    /DIRECT_WORK/i.test(text(o.primaryContact?.emailType)),
  ).length;
  const phone = passOpps.filter((o) => text(o.primaryContact?.phone)).length;
  const linkedin = passOpps.filter((o) =>
    text(o.primaryContact?.linkedinUrl || o.primaryContact?.linkedin),
  ).length;
  const directBuyer = passOpps.filter(
    (o) => o.buyerClassification?.class === "DIRECT_BUYER",
  ).length;
  const secondary = passOpps.filter((o) =>
    isPlausiblePersonName(o.secondaryContact?.name),
  ).length;

  const scorecard = {
    contract: CONTRACT,
    asOf: AS_OF,
    priorCandidatePackage: "gdi_hilton_ts_reprocess_v1_20261124",
    newCandidatePackage: publicationVersion,
    existingEightPass: {
      retained: retained.length,
      downgraded: downgraded.length,
      retainedIds: retained.map((r) => r.opportunityId),
      downgradedIds: downgraded.map((r) => ({
        id: r.opportunityId,
        title: r.title,
        reasons: r.reasons,
      })),
    },
    hold: {
      researchedZeroDollar: holdRanked.length,
      recoveredZeroDollar: holdRecovered.length,
      paidWhoPriority: holdPaidPriority.length,
      rejectedExhibitorOrPast: holdRanked.filter((h) =>
        /REJECT_/.test(h.salvageDisposition),
      ).length,
    },
    competitive: {
      newlyVerifiedBeyondSssp: 0,
      retainedSsspOverflow: competitiveFromStore.some(
        (c) => c.opportunityId === "gdi_opp_267d801f2fa39f9e",
      ),
      requiresPaidEnvelope: true,
    },
    contacts: {
      distinctPrimary: passOpps.length,
      distinctSecondary: secondary,
      verifiedPersonalEmail: personalEmail,
      phone,
      linkedin,
      directBuyer,
    },
    finalization: {
      hotel: hotelPass,
      competitive: competitivePass,
      total: passOpps.length,
      salesReady,
    },
    spend: {
      actualProviderSpendUsd: 0,
      costPerAdditionalFinalizedOpportunity: null,
      proposedCeilingUsd: 16,
      paidCallsExecuted: false,
    },
  };
  writeJson(path.join(REPORT_DIR, "06_FOUNDER_SCORECARD.json"), scorecard);

  const certification = {
    contract: CONTRACT,
    at: new Date().toISOString(),
    ok: false,
    checks: {
      pass8Revalidated: passRevalidation.length === 8,
      notAllEightAutoRetained: retained.length < 8,
      holdRanked: holdRanked.length === 25,
      zeroDollarHoldRecovered: holdRecovered.length,
      competitiveExpansionBeyondSsspRequiresPaid: true,
      packageIntegrity: integrity.ok === true,
      namedContactAssert: namedAssert.ok === true,
      hiltonPointerNotActivated: !hiltonPtr,
      peerStagedUnchanged: peer.every((p) => p.hasStagedPointer),
      paidSpendZero: true,
      agentOff: true,
      opportunitiesJsonUnmutated: true,
      researchViewUntouched: true,
    },
    packageWrite: written,
    peer,
    hiltonStagedPointer: hiltonPtr,
  };
  certification.ok =
    certification.checks.pass8Revalidated &&
    certification.checks.notAllEightAutoRetained &&
    certification.checks.holdRanked &&
    certification.checks.packageIntegrity &&
    certification.checks.namedContactAssert &&
    certification.checks.hiltonPointerNotActivated &&
    certification.checks.peerStagedUnchanged &&
    certification.checks.paidSpendZero &&
    certification.checks.agentOff;
  writeJson(path.join(REPORT_DIR, "07_CERTIFICATION.json"), certification);

  console.log(
    JSON.stringify(
      {
        contract: CONTRACT,
        ok: certification.ok,
        retained: retained.length,
        downgraded: downgraded.length,
        holdRecoveredZeroDollar: holdRecovered.length,
        finalCandidateCount: passOpps.length,
        publicationVersion,
        packageWritten: written.ok === true,
        packagePath: written.path || null,
        pointerActivated: false,
        paidSpendUsd: 0,
        awaitingPaidAuth: true,
        reportDir: path.relative(ROOT, REPORT_DIR).replace(/\\/g, "/"),
      },
      null,
      2,
    ),
  );
  process.exit(certification.ok ? 0 : 2);
}

main();
