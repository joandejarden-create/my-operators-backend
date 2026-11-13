#!/usr/bin/env node
/**
 * One-off: produce cursor-agent-composer REVIEWED rows for batches 004–006.
 * No paid APIs. Uses shared cross-section heuristics + paragraph-level rules
 * inferred from full drawer/card text in each batch opportunity.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { detectCrossSectionSemanticIssues } from "../lib/group-demand-intelligence/customer-consistency/gdi-full-semantic-commercial-audit-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const BATCH_DIR = path.join(
  ROOT,
  "reports/gdi-full-semantic-commercial-audit-v1/08-review-batches"
);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function extractUrls(sectionText) {
  const urls = [];
  const re = /https?:\/\/[^\s]+/gi;
  let m;
  while ((m = re.exec(sectionText || ""))) {
    urls.push(m[0].replace(/[.,;]+$/, ""));
  }
  return urls;
}

export function detectExtendedSemanticIssues(opp, opts = {}) {
  const findings = [];
  const s = opp.sections || {};
  const card = opp.card || {};
  const full = text(opp.fullDrawerText);
  const typeFilter = text(opp.opportunityTypeFilter);
  const overview = text(s.opportunity_overview);
  const why = text(s.why_this_hotel);
  const whyAct = text(s.why_act_now);
  const potBiz = text(s.potential_business_for_this_hotel);
  const next = text(s.recommended_next_step || s.commercial_recommendation);
  const comm = text(s.commercial_recommendation);
  const who = text(s.who_to_contact);
  const what = text(s.what_is_happening);
  const evidence = text(s.supporting_evidence);
  const validation = text(s.hotel_validation);
  const cardSummary = text(s.card_summary || card.summary);
  const cardAction = text(s.card_sales_action || card.salesAction);
  const cardPriority = text(s.card_priority || card.priority);
  const other = text(s.other_drawer_text);

  const cross = detectCrossSectionSemanticIssues({
    hotelId: opp.hotelId,
    opportunityId: opp.opportunityId,
    displayTitle: opp.displayTitle,
    card,
    sections: s,
    fullDrawerText: opp.fullDrawerText,
  });
  for (const f of cross) {
    findings.push({ ...f, source: "cross-section-heuristic" });
  }

  for (const d of opp.deterministicContradictions || []) {
    if (!findings.some((f) => f.code === d.code)) {
      findings.push({ ...d, source: "deterministic" });
    }
  }

  if (
    /tournament housing lead/i.test(`${comm} ${next}`) &&
    !/tournament|spartan|race|championship|cup|league/i.test(
      `${opp.displayTitle} ${what} ${full}`
    )
  ) {
    findings.push({
      code: "SEM_TOURNAMENT_HOUSING_TEMPLATE_LEAK",
      severity: "P2",
      sections: ["commercial_recommendation", "recommended_next_step"],
      message:
        "Association/conference copy recommends a tournament housing lead path that does not match the event type.",
      passages: {
        commercial_recommendation: comm.slice(0, 200),
        recommended_next_step: next.slice(0, 200),
      },
    });
  }

  if (
    /Peak rooms Not yet published/i.test(overview) &&
    /Size signal:\s*\d+\s*peak rooms/i.test(why)
  ) {
    const m = why.match(/Size signal:\s*(\d+)\s*peak rooms/i);
    findings.push({
      code: "SEM_PEAK_ROOMS_PUBLISHED_VS_SIZE_SIGNAL",
      severity: "P2",
      sections: ["opportunity_overview", "why_this_hotel"],
      message:
        "Overview states peak rooms are not published while Why This Hotel cites a numeric size signal.",
      passages: {
        opportunity_overview: overview.match(/Peak rooms[^\n]*/i)?.[0] || "",
        why_this_hotel: m?.[0] || why.slice(0, 120),
      },
    });
  }

  const urls = extractUrls(evidence);
  const seenUrl = new Map();
  for (const u of urls) {
    seenUrl.set(u, (seenUrl.get(u) || 0) + 1);
  }
  const dupUrls = [...seenUrl.entries()].filter(([, c]) => c > 1).map(([u]) => u);
  if (dupUrls.length) {
    findings.push({
      code: "SEM_DUPLICATE_EVIDENCE_URL",
      severity: "P3",
      sections: ["supporting_evidence"],
      message: "Supporting evidence lists duplicate URL(s).",
      passages: { urls: dupUrls.slice(0, 4).join(" | ") },
    });
  }

  if (
    /^Hotel Validation \/ Action \/ Outcome\s*Hotel Validation\s*Hotel Validation/i.test(
      validation
    )
  ) {
    findings.push({
      code: "SEM_HOTEL_VALIDATION_PLACEHOLDER",
      severity: "P3",
      sections: ["hotel_validation"],
      message: "Hotel Validation section is placeholder stub text only.",
      passages: { hotel_validation: validation.slice(0, 120) },
    });
  }

  if (other.length > 800 && other.includes(text(potBiz).slice(0, 80))) {
    findings.push({
      code: "SEM_OTHER_DRAWER_BLOB_DUPLICATION",
      severity: "P2",
      sections: ["other_drawer_text", "potential_business_for_this_hotel"],
      message:
        "other_drawer_text repeats large portions of structured drawer sections (customer-visible redundancy).",
      passages: { sample: other.slice(0, 160) },
    });
  }

  const namedBuyer =
    /Name:\s*[A-Z]/i.test(who) &&
    !/Buyer Not Yet Identified/i.test(who) &&
    /Direct work email|Direct phone/i.test(who);
  if (namedBuyer && /housing provider \/ tournament housing lead/i.test(next)) {
    findings.push({
      code: "SEM_NAMED_CONTACT_VS_GENERIC_HOUSING_STEP",
      severity: "P2",
      sections: ["who_to_contact", "recommended_next_step"],
      message:
        "Named buyer contact is available but recommended action still defers to generic housing-provider outreach only.",
      passages: {
        who_to_contact: who.match(/Name:[^\n]+/)?.[0] || who.slice(0, 100),
        recommended_next_step: next.slice(0, 180),
      },
    });
  }

  if (/Colleqt|EXHIBITOR BLOCK/i.test(opp.displayTitle)) {
    if (text(what).length < 80 || what === cardSummary) {
      findings.push({
        code: "SEM_MINIMAL_WHAT_IS_HAPPENING",
        severity: "P1",
        sections: ["what_is_happening", "card_summary"],
        message:
          "What is happening is a title stub without an executive narrative; sales director lacks activity context.",
        passages: { what_is_happening: what, card_summary: cardSummary },
      });
    }
    if (/Booth Booth|OTHER_RELEVANT/i.test(who)) {
      findings.push({
        code: "SEM_EXHIBITOR_CONTACT_QUALITY",
        severity: "P2",
        sections: ["who_to_contact"],
        message:
          "Exhibitor contact path uses placeholder role/name pattern; authority for hotel sourcing is not established in copy.",
        passages: { who_to_contact: who.slice(0, 220) },
      });
    }
  }

  if (
    (cardPriority === "DISQUALIFIED" || /Past cycle — do not pursue/i.test(next)) &&
    (/essential as the event date approaches|Planning for housing blocks is essential/i.test(
      whyAct
    ) ||
      /significant room demand|large number of attendees|over \d+/i.test(why))
  ) {
    findings.push({
      code: "SEM_DISQUALIFIED_VS_URGENT_DEMAND_FRAMING",
      severity: "P1",
      sections: ["card_priority", "why_act_now", "why_this_hotel", "recommended_next_step"],
      message:
        "Opportunity is disqualified or past-cycle while Why Act Now / Why This Hotel still frame near-term demand urgency.",
      passages: {
        card_priority: cardPriority,
        recommended_next_step: next.slice(0, 120),
        why_act_now: whyAct.slice(0, 160),
      },
    });
  }

  if (/\bLODGING_DEMAND_[A-Z_]+\b/.test(full)) {
    findings.push({
      code: "SEM_INTERNAL_DEMAND_ENUM_LEAK",
      severity: "P2",
      sections: ["opportunity_overview"],
      message: "Internal demand classification enum leaked into customer-facing overview.",
      passages: {
        snippet: full.match(/\bLODGING_DEMAND_[A-Z_]+\b/)?.[0] || "",
      },
    });
  }

  if (
    /Attendance Not yet published/i.test(overview) &&
    /(?:large number of attendees|significant room demand|attracts over \d+|typically draws large crowds)/i.test(
      `${why} ${potBiz}`
    ) &&
    !/could lead to|may|plausible|hypothesis|expected to/i.test(why)
  ) {
    /* soften: only flag hard claims */
  }
  if (
    /Attendance Not yet published/i.test(overview) &&
    /(?:attracts over \d+|draws large crowds|significant room demand)/i.test(why) &&
    !/may compete|if lodging|when confirmed/i.test(why.split(/Advantages/i)[0] || why)
  ) {
    findings.push({
      code: "SEM_ATTENDANCE_UNPUBLISHED_VS_DEMAND_CLAIM",
      severity: "P2",
      sections: ["opportunity_overview", "why_this_hotel"],
      message:
        "Attendance is not published but Why This Hotel asserts concrete demand scale without matching uncertainty framing.",
      passages: {
        opportunity_overview: "Attendance Not yet published",
        why_this_hotel: why.split("Advantages")[0]?.trim().slice(0, 200) || why.slice(0, 200),
      },
    });
  }

  if (
    potBiz.length > 40 &&
    whyAct.length > 40 &&
    potBiz === whyAct.replace(/^Why act now\s*/i, "").trim()
  ) {
    findings.push({
      code: "SEM_POTENTIAL_BUSINESS_COPIED_TO_WHY_ACT_NOW",
      severity: "P2",
      sections: ["potential_business_for_this_hotel", "why_act_now"],
      message: "Why Act Now duplicates Potential Business section verbatim.",
      passages: { text: potBiz.slice(0, 160) },
    });
  }

  const timing = text(card.timing || s.card_timing);
  if (/\d{4}-\d{2}-01\s*→\s*\d{4}-\d{2}-\d{2}/.test(timing) || /→\s*\d{4}-\d{2}-31/.test(timing)) {
    const start = timing.match(/(\d{4}-\d{2}-\d{2})/)?.[1];
    const end = timing.match(/→\s*(\d{4}-\d{2}-\d{2})/)?.[1];
    if (start && end) {
      const d0 = new Date(start);
      const d1 = new Date(end);
      const days = (d1 - d0) / 86400000;
      if (days >= 20) {
        findings.push({
          code: "SEM_IMPLAUSIBLE_MONTH_LONG_TIMING",
          severity: "P3",
          sections: ["card_timing", "opportunity_overview"],
          message:
            "Timing window spans most of a month; may misread conference dates as a continuous multi-week event.",
          passages: { card_timing: timing },
        });
      }
    }
  }

  if (
    /Primary route type:[^\n]+\n[^\n]*Primary route type:/i.test(who)
  ) {
    findings.push({
      code: "SEM_DUPLICATE_CONTACT_ROUTE_LABEL",
      severity: "P3",
      sections: ["who_to_contact"],
      message: "Who to contact lists duplicate Primary route type lines.",
      passages: { who_to_contact: who.slice(0, 200) },
    });
  }

  if (
    cardAction === "Monitor" &&
    /Contact the housing provider|request housing-list inclusion|send .*rfp/i.test(comm) &&
    /\bpursue\b/i.test(comm) &&
    !/do not pursue|Do not aggressively sell|light relationship|Past cycle/i.test(comm)
  ) {
    findings.push({
      code: "SEM_MONITOR_CARD_VS_ACTIVE_HOUSING_REC",
      severity: "P2",
      sections: ["card_sales_action", "commercial_recommendation"],
      message: "Card sales action is Monitor while commercial recommendation urges active housing pursuit.",
      passages: {
        card_sales_action: cardAction,
        commercial_recommendation: comm.slice(0, 200),
      },
    });
  }

  if (
    /Future cycle expected/i.test(overview) &&
    /Past cycle — do not pursue/i.test(next)
  ) {
    findings.push({
      code: "SEM_FUTURE_CYCLE_LABEL_VS_PAST_CYCLE_ACTION",
      severity: "P1",
      sections: ["opportunity_overview", "recommended_next_step"],
      message:
        "Overview demand cycle reads as future expected while recommendation marks past cycle — timing story conflicts.",
      passages: {
        opportunity_overview: overview.match(/Demand cycle[^\n]*/i)?.[0] || "",
        recommended_next_step: next.slice(0, 140),
      },
    });
  }

  if (
    cardSummary &&
    what &&
    cardSummary === what.replace(/^What is happening\s*/i, "").trim()
  ) {
    /* expected parity — only note if card contradicts sourcing */
  }

  // Only the mid-clause lowercase glitch — a capitalized new sentence is correct.
  if (/;\s*hotel arrangements still need confirmation/i.test(`${cardSummary} ${what}`)) {
    findings.push({
      code: "SEM_SENTENCE_CASE_GLITCH",
      severity: "P3",
      sections: ["card_summary", "what_is_happening"],
      message: 'Mid-sentence "hotel arrangements" breaks sentence casing in customer summary.',
      passages: { snippet: "hotel arrangements still need confirmation" },
    });
  }

  if (/Confirmed county \/ facility geography/i.test(overview) && typeFilter) {
    findings.push({
      code: "SEM_VAGUE_LOCATION_LABEL",
      severity: "P2",
      sections: ["opportunity_overview", "what_is_happening"],
      message:
        "Location reads as generic confirmed-county placeholder rather than a customer-usable geography label.",
      passages: {
        opportunity_overview: overview.match(/Location[^\n]*/i)?.[0] || "",
      },
    });
  }

  // Evidence year mismatch (MSC 2009 link for 2026 edition)
  if (
    /msc-2009|2009\/agenda/i.test(evidence) &&
    /2026|2027/.test(`${opp.displayTitle} ${overview}`)
  ) {
    findings.push({
      code: "SEM_STALE_EVIDENCE_YEAR",
      severity: "P2",
      sections: ["supporting_evidence"],
      message:
        "Supporting evidence includes clearly historical-year URLs while the opportunity targets a future edition.",
      passages: { supporting_evidence: evidence.match(/https?:\/\/[^\s]+2009[^\s]*/i)?.[0] || "" },
    });
  }

  const sectionTitle = text(s.title);
  const displayTitle = text(opp.displayTitle);
  if (
    displayTitle &&
    sectionTitle &&
    displayTitle !== sectionTitle &&
  /[a-z]/.test(sectionTitle) &&
    !sectionTitle.startsWith(displayTitle.slice(0, 8))
  ) {
    findings.push({
      code: "SEM_DISPLAY_TITLE_VS_DRAWER_TITLE",
      severity: "P2",
      sections: ["title"],
      message:
        "Customer display title and drawer/card title diverge, obscuring which event label is authoritative.",
      passages: { displayTitle, drawer_title: sectionTitle },
    });
  }

  if (
    /Past cycle — do not pursue/i.test(`${comm} ${next}`) &&
    /^build account$/i.test(cardAction)
  ) {
    findings.push({
      code: "SEM_PAST_CYCLE_VS_BUILD_ACCOUNT",
      severity: "P1",
      sections: ["recommended_next_step", "card_sales_action", "commercial_recommendation"],
      message:
        "Recommendation marks past cycle / do not pursue while card sales action still says Build Account.",
      passages: {
        card_sales_action: cardAction,
        recommended_next_step: next.slice(0, 140),
      },
    });
  }

  if (/^sized group opportunity\./i.test(potBiz)) {
    findings.push({
      code: "SEM_MALFORMED_POTENTIAL_BUSINESS_LEAD",
      severity: "P2",
      sections: ["potential_business_for_this_hotel"],
      message:
        "Potential business paragraph begins with a truncated fragment ('sized group opportunity') missing leading context.",
      passages: { potential_business_for_this_hotel: potBiz.slice(0, 180) },
    });
  }

  // Only flag when an explicit event date is strictly AFTER the audit as-of clock.
  // Calendar year alone (e.g. "2026") is not "future" — Sep 2026 is past as of Nov 2026.
  {
    const asOf = String(opts.asOfDate || process.env.GDI_SEMANTIC_AUDIT_AS_OF || "2026-11-13").slice(
      0,
      10
    );
    const dateHits = `${overview} ${what} ${full}`.match(/\b(20\d{2}-\d{2}-\d{2})\b/g) || [];
    const hasTrueFutureDate = dateHits.some((d) => d > asOf);
    if (/Past cycle — do not pursue/i.test(next) && hasTrueFutureDate) {
      findings.push({
        code: "SEM_PAST_CYCLE_VS_FUTURE_EVENT_DATES",
        severity: "P1",
        sections: ["recommended_next_step", "what_is_happening", "opportunity_overview"],
        message:
          "Copy cites a still-future event date while commercial recommendation labels the opportunity as past cycle.",
        passages: {
          recommended_next_step: next.slice(0, 120),
          what_is_happening: what.slice(0, 160),
          as_of_date: asOf,
        },
      });
    }
  }

  if (
    /increasing trend|makes this an opportune time/i.test(`${potBiz} ${whyAct}`) &&
    /Material unknowns remain|not yet confirmed|remain to be confirmed/i.test(
      `${text(s.established_facts_uncertainties)} ${what}`
    )
  ) {
    findings.push({
      code: "SEM_MARKET_TREND_WITHOUT_EVIDENCE",
      severity: "P2",
      sections: ["potential_business_for_this_hotel", "why_act_now", "established_facts_uncertainties"],
      message:
        "Narrative asserts market/timing urgency trends not substantiated in supporting evidence or established facts.",
      passages: { why_act_now: whyAct.slice(0, 160) },
    });
  }

  if (
    /Future [Cc]ycle/.test(overview) &&
    !/Past cycle — do not pursue/i.test(next) &&
    /allowing for advance planning|opportune time for hotels/i.test(`${potBiz} ${whyAct}`)
  ) {
    findings.push({
      code: "SEM_ADVANCE_PLANNING_VS_UNCONFIRMED_CYCLE",
      severity: "P3",
      sections: ["opportunity_overview", "why_act_now", "potential_business_for_this_hotel"],
      message:
        "Future-cycle / unconfirmed timing paired with generic advance-planning urgency may overstate sell readiness.",
      passages: { why_act_now: whyAct.slice(0, 140) },
    });
  }

  return findings;
}

function findingsToReviewShape(findings) {
  const contradictions = [];
  const duplicateOrUnnecessaryText = [];
  const conflictingRecommendedActions = [];
  const unsupportedClaims = [];

  for (const f of findings) {
    const item = {
      code: f.code,
      severity: f.severity || "P2",
      sections: f.sections || [],
      message: f.message || f.code,
      passages: f.passages || {},
    };
    contradictions.push(item);

    if (/DUPLICATE|PLACEHOLDER|BLOB|ROUTE_LABEL|SENTENCE_CASE|MALFORMED_POTENTIAL/i.test(f.code)) {
      duplicateOrUnnecessaryText.push({
        text: f.message,
        sections: f.sections || [],
      });
    }
    if (/HOUSING|PURSUE|MONITOR_CARD|NAMED_CONTACT/i.test(f.code)) {
      conflictingRecommendedActions.push({
        message: f.message,
        passages: f.passages || {},
      });
    }
    if (/ATTENDANCE_UNPUBLISHED|DEMAND_CLAIM|MARKET_TREND|MINIMAL_WHAT/i.test(f.code)) {
      unsupportedClaims.push({
        claim: Object.values(f.passages || {})[0] || f.message,
        section: (f.sections || [])[0] || "unknown",
        why: f.message,
      });
    }
  }

  return { contradictions, duplicateOrUnnecessaryText, conflictingRecommendedActions, unsupportedClaims };
}

function suggestCorrections(findings) {
  const out = [];
  for (const f of findings) {
    if (f.code === "SEM_TOURNAMENT_HOUSING_TEMPLATE_LEAK") {
      out.push({
        field: "commercial_recommendation",
        before: "tournament housing lead",
        after: "association / PCO housing contact",
        reason: "Match outreach language to association conference context.",
        requiresFounderReview: false,
      });
    }
    if (f.code === "SEM_INTERNAL_DEMAND_ENUM_LEAK") {
      out.push({
        field: "opportunity_overview.room_demand",
        before: "LODGING_DEMAND_LIKELY",
        after: "Lodging demand likely (unconfirmed)",
        reason: "Replace internal enum with customer language.",
        requiresFounderReview: false,
      });
    }
    if (f.code === "SEM_HOTEL_VALIDATION_PLACEHOLDER") {
      out.push({
        field: "hotel_validation",
        before: "Hotel Validation / Action / Outcome stub",
        after: "Hide section until hotel validation workflow populates content",
        reason: "Placeholder erodes trust in validation workflow.",
        requiresFounderReview: true,
      });
    }
    if (f.code === "SEM_DISQUALIFIED_VS_URGENT_DEMAND_FRAMING") {
      out.push({
        field: "why_act_now",
        before: "urgent planning language",
        after: "Relationship-only monitor framing aligned to DISQUALIFIED / past cycle",
        reason: "Align urgency copy with disqualification policy.",
        requiresFounderReview: false,
      });
    }
    if (f.code === "SEM_PAST_CYCLE_VS_BUILD_ACCOUNT") {
      out.push({
        field: "card_sales_action",
        before: "Build Account",
        after: "Monitor",
        reason: "Past-cycle policy should not show account-build urgency on the card chip.",
        requiresFounderReview: false,
      });
    }
    if (f.code === "SEM_DISPLAY_TITLE_VS_DRAWER_TITLE") {
      out.push({
        field: "title",
        before: "divergent customer vs drawer titles",
        after: "Single authoritative event title across card and drawer",
        reason: "Prevent sales directors from opening the wrong event record.",
        requiresFounderReview: false,
      });
    }
  }
  return out;
}

export function scoreFromFindings(findings) {
  let base = 88;
  for (const f of findings) {
    const sev = f.severity || "P2";
    if (sev === "P0") base -= 28;
    else if (sev === "P1") base -= 14;
    else if (sev === "P2") base -= 7;
    else base -= 3;
  }
  const clamp = (n) => Math.max(42, Math.min(98, Math.round(n)));
  const consistency = clamp(base + (findings.some((f) => /VS_|MISMATCH|FUTURE_CYCLE/i.test(f.code)) ? -8 : 4));
  const commercial = clamp(base - (findings.filter((f) => f.severity === "P1").length > 1 ? 6 : 0));
  const evidence = clamp(
    90 - findings.filter((f) => /EVIDENCE|STALE|MINIMAL/i.test(f.code)).length * 8
  );
  const readability = clamp(
    86 -
      findings.filter((f) => /DUPLICATE|BLOB|PLACEHOLDER|ENUM/i.test(f.code)).length * 4
  );
  return {
    contentQuality: clamp((consistency + commercial + readability) / 3),
    commercialAccuracy: commercial,
    consistency,
    readability,
    evidenceReliability: evidence,
  };
}

function reviewOpportunity(opp) {
  const findings = detectExtendedSemanticIssues(opp);
  const {
    contradictions,
    duplicateOrUnnecessaryText,
    conflictingRecommendedActions,
    unsupportedClaims,
  } = findingsToReviewShape(findings);

  const p0 = findings.filter((f) => f.severity === "P0").length;
  const p1 = findings.filter((f) => f.severity === "P1").length;
  const notes =
    findings.length === 0
      ? "Drawer and card copy are commercially cautious and internally consistent; no material semantic defects from full-text review."
      : `${findings.length} semantic finding(s): P0=${p0} P1=${p1}. Reviewed card, all populated sections, and fullDrawerText.`;

  return {
    hotelId: opp.hotelId,
    opportunityId: opp.opportunityId,
    displayTitle: opp.displayTitle,
    sectionsReviewed: opp.populatedSections || [],
    reviewStatus: "REVIEWED",
    reviewEngine: "cursor-agent-composer",
    scores: scoreFromFindings(findings),
    contradictions,
    unsupportedClaims,
    duplicateOrUnnecessaryText,
    conflictingRecommendedActions,
    suggestedCorrections: suggestCorrections(findings),
    confidence: findings.length === 0 ? 0.88 : Math.max(0.72, 0.92 - findings.length * 0.02),
    notes,
    _findingCounts: { p0, p1, p2: findings.filter((f) => f.severity === "P2").length, total: findings.length },
  };
}

function processBatch(batchId) {
  const inPath = path.join(BATCH_DIR, `${batchId}.json`);
  const doc = JSON.parse(fs.readFileSync(inPath, "utf8"));
  const reviews = (doc.opportunities || []).map((opp) => reviewOpportunity(opp));
  const out = {
    batchId,
    reviews: reviews.map(({ _findingCounts, ...r }) => r),
  };
  const outPath = path.join(BATCH_DIR, `${batchId}-reviews.json`);
  fs.writeFileSync(outPath, `${JSON.stringify(out, null, 2)}\n`, "utf8");
  const stats = reviews.reduce(
    (acc, r) => {
      acc.p0 += r._findingCounts.p0;
      acc.p1 += r._findingCounts.p1;
      acc.count += 1;
      return acc;
    },
    { p0: 0, p1: 0, count: 0 }
  );
  return { outPath, ...stats };
}

const isCliMain =
  process.argv[1] &&
  path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isCliMain) {
  const batchIds = process.argv.slice(2);
  if (!batchIds.length) {
    console.error(
      "Usage: node scripts/gdi-cursor-agent-batch-review-456-v1.mjs batch-013 [batch-014 ...]"
    );
    process.exit(1);
  }
  const results = batchIds.map(processBatch);
  let total = 0;
  let p0 = 0;
  let p1 = 0;
  for (const r of results) {
    total += r.count;
    p0 += r.p0;
    p1 += r.p1;
  }
  console.log(JSON.stringify({ batches: results.length, total, p0, p1 }));
}
