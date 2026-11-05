/**
 * Internal recurring-meeting intelligence for the GDI Research Supervisor.
 * Organization identity stays separate from each event cycle.
 * A committed host is not an open opportunity. An inferred next cycle is not a meeting.
 * Does not publish, write Airtable, or call Parallel, Exa, or Firecrawl.
 */

import crypto from "node:crypto";
import { canonicalEntityKey } from "../workflow/gdi-global-weekly-feedback-v2.js";
import { namedCompetitorHost } from "./commercial-pathway-v1.js";

export const RECURRING_INTELLIGENCE_VERSION = "gdi-recurring-meeting-intelligence-v1";
export const RECURRING_AS_OF = "2026-10-24";

export const PROGRAM_TREATMENT = Object.freeze({
  OPEN_OR_UNKNOWN_SOURCING: "OPEN_OR_UNKNOWN_SOURCING",
  COMPETITIVE_OVERFLOW: "COMPETITIVE_OVERFLOW",
  FUTURE_CYCLE_COMPETITIVE: "FUTURE_CYCLE_COMPETITIVE",
  WATCH_RECURRENCE_UNKNOWN: "WATCH_RECURRENCE_UNKNOWN",
});

const EXPLICIT_OPEN = /hotel (?:is )?not (?:yet )?selected|accepting (?:hotel )?proposals|rfp (?:is )?open|site selection (?:is )?open/i;
const OVERFLOW_DEMAND = /overflow rooms|overflow room block|overflow hotel rooms|overflow lodging|rooms still needed/i;
const MULTI_YEAR = /multi-year|three-year agreement|3-year agreement/i;
const LARGE_ASSOCIATION = /national association|international association/i;
const STATED_ANNUAL = /\bannual\b|every year/i;
const STATED_BIENNIAL = /biennial|every (?:two|2) years/i;

function text(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function stripYear(value) {
  return text(value).replace(/\b20\d{2}\b/g, " ").replace(/\s+/g, " ").trim();
}

export function organizationKeyFor(name = "") {
  return canonicalEntityKey(text(name).replace(/\([^)]*\)/g, " "));
}

export function programKeyFor(cycle = {}) {
  const org = organizationKeyFor(cycle.organizationName);
  const program = canonicalEntityKey(stripYear(cycle.programName || cycle.title || ""));
  return `${org}::${program}`;
}

function hash(value) {
  return crypto.createHash("sha256").update(String(value)).digest("hex").slice(0, 16);
}

function yearsIn(quote) {
  return [...new Set([...text(quote).matchAll(/\b(20\d{2})\b/g)].map((match) => Number(match[1])))].sort();
}

function agreementYears(quote) {
  if (!MULTI_YEAR.test(text(quote))) return [];
  return yearsIn(quote);
}

function monthIndex(iso) {
  const [year, month] = String(iso || "").slice(0, 10).split("-").map(Number);
  if (!year || !month) return null;
  return year * 12 + (month - 1);
}

function shiftMonths(iso, delta) {
  const [year, month, day] = String(iso).slice(0, 10).split("-").map(Number);
  const total = year * 12 + (month - 1) + delta;
  const nextYear = Math.floor(total / 12);
  const nextMonth = total % 12;
  const lastDay = new Date(Date.UTC(nextYear, nextMonth + 1, 0)).getUTCDate();
  const nextDay = Math.min(day || 1, lastDay);
  return `${nextYear}-${String(nextMonth + 1).padStart(2, "0")}-${String(nextDay).padStart(2, "0")}`;
}

function fullMonthsBetween(fromIso, toIso) {
  const start = monthIndex(fromIso);
  const end = monthIndex(toIso);
  if (start == null || end == null) return null;
  const fromDay = Number(String(fromIso).slice(8, 10));
  const toDay = Number(String(toIso).slice(8, 10));
  let months = end - start;
  if (toDay < fromDay) months -= 1;
  return months;
}

function accommodationFrom(quote) {
  const blob = text(quote);
  const weekend = blob.match(/Weekend Rate\/Standard Room:\s*\$(\d+)/i);
  const weekday = blob.match(/Weekday Rate\/Standard Room:\s*\$(\d+)/i);
  if (!weekend && !weekday) return { known: false, rates: null };
  return {
    known: true,
    weekendUsd: weekend ? Number(weekend[1]) : null,
    weekdayUsd: weekday ? Number(weekday[1]) : null,
    appliesOnlyToThisCycle: true,
  };
}

function contactsFrom(quote) {
  const blob = text(quote);
  const emails = blob.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi) || [];
  const phones = blob.match(/\(\d{3}\)\s*\d{3}-\d{4}/g) || [];
  return [...new Set(emails.map((email) => email.toLowerCase()))].map((email) => ({
    email,
    phone: phones[0] || null,
    role: "published_organization_contact",
    hotelSourcingDecisionMaker: false,
  }));
}

function publicationPatterns(quote) {
  const blob = text(quote);
  const registration = blob.match(/Registration for the (20\d{2})[^.]{0,80}will open in ([^.]{0,40})/i);
  if (!registration) return [];
  return [{
    kind: "ATTENDEE_REGISTRATION",
    cycleYear: Number(registration[1]),
    statement: `Registration for ${registration[1]} opens ${text(registration[2]).replace(/\.$/, "")}.`,
    hotelRfp: false,
  }];
}

function cycleYear(cycle) {
  if (cycle.startDate) return Number(String(cycle.startDate).slice(0, 4));
  const years = yearsIn(cycle.evidenceQuote);
  return years.length === 1 ? years[0] : null;
}

export function classifyMeetingCycle(cycle = {}, options = {}) {
  const quote = text(cycle.evidenceQuote);
  const targetHotel = options.targetHotel || "";
  const host = text(cycle.host) || namedCompetitorHost(quote, targetHotel);
  const coveredYears = agreementYears(quote);
  const year = cycleYear(cycle);
  const multiYear = coveredYears.length > 0 && (year == null || coveredYears.includes(year));
  const overflow = OVERFLOW_DEMAND.test(quote);
  const explicitOpen = EXPLICIT_OPEN.test(quote) && !host && !multiYear;
  let treatment = PROGRAM_TREATMENT.OPEN_OR_UNKNOWN_SOURCING;
  let sourcingStatus = "UNKNOWN";
  if (multiYear) {
    treatment = "COMMITTED_NOT_OPEN";
    sourcingStatus = "MULTI_YEAR_AGREEMENT";
  } else if (host && overflow) {
    treatment = PROGRAM_TREATMENT.COMPETITIVE_OVERFLOW;
    sourcingStatus = "HOST_SELECTED";
  } else if (host) {
    treatment = "COMMITTED_NOT_OPEN";
    sourcingStatus = "HOST_SELECTED";
  }
  const rejectedInferences = [];
  if (/upcoming:\s*\d+\s+events/i.test(quote)) {
    rejectedInferences.push("An upcoming-event count is not a list of meetings.");
  }
  return {
    cycleId: cycle.cycleId || cycle.id || null,
    organizationName: text(cycle.organizationName),
    organizationKey: organizationKeyFor(cycle.organizationName),
    programKey: programKeyFor(cycle),
    programName: stripYear(cycle.programName || cycle.title || ""),
    startDate: cycle.startDate || null,
    endDate: cycle.endDate || null,
    year,
    location: cycle.location || null,
    host: host || null,
    accommodation: accommodationFrom(quote),
    sourcingStatus,
    treatment,
    confirmedOpen: explicitOpen,
    unknownIsNotConfirmedOpen: sourcingStatus === "UNKNOWN",
    overflowSupported: overflow,
    soldOutIsNotOverflow: /sold out|room block is full/i.test(quote) && !overflow,
    multiYearCovered: multiYear,
    coveredYears,
    contacts: contactsFrom(quote),
    evidence: {
      sourceUrl: cycle.sourceUrl || null,
      contentHash: cycle.contentHash || null,
      strength: cycle.contentHash ? "PAGE_READ" : cycle.excerptOnly ? "SEARCH_EXCERPT" : "PROVIDED_QUOTE",
      retrievedOn: cycle.retrievedOn || null,
    },
    rejectedInferences,
  };
}

function recurrenceOf(cycles) {
  const dated = cycles.filter((cycle) => cycle.startDate).sort((a, b) => String(a.startDate).localeCompare(String(b.startDate)));
  const stated = cycles.map((cycle) => text(cycle.evidenceQuote)).join(" ");
  const covered = [...new Set(cycles.flatMap((cycle) => agreementYears(cycle.evidenceQuote)))];
  if (covered.length >= 2) {
    return { pattern: "MULTI_YEAR_AGREEMENT", intervalMonths: 12, inference: "STATED", confidence: "OBSERVED" };
  }
  if (STATED_BIENNIAL.test(stated)) {
    return { pattern: "STATED_BIENNIAL", intervalMonths: 24, inference: "STATED", confidence: "OBSERVED" };
  }
  if (STATED_ANNUAL.test(stated)) {
    return { pattern: "STATED_ANNUAL", intervalMonths: 12, inference: "STATED", confidence: "OBSERVED" };
  }
  if (dated.length < 2) {
    return { pattern: "NOT_ESTABLISHED", intervalMonths: null, inference: "NONE", confidence: "NONE" };
  }
  const gaps = [];
  for (let i = 1; i < dated.length; i += 1) {
    gaps.push(monthIndex(dated[i].startDate) - monthIndex(dated[i - 1].startDate));
  }
  const annual = gaps.every((gap) => gap >= 11 && gap <= 13);
  const biennial = gaps.every((gap) => gap >= 23 && gap <= 25);
  const excerpt = dated.some((cycle) => cycle.evidence?.strength === "SEARCH_EXCERPT" || cycle.excerptOnly);
  if (annual) {
    return { pattern: "OBSERVED_ANNUAL", intervalMonths: 12, inference: "INFERRED", confidence: excerpt ? "LIMITED" : "OBSERVED" };
  }
  if (biennial) {
    return { pattern: "OBSERVED_BIENNIAL", intervalMonths: 24, inference: "INFERRED", confidence: excerpt ? "LIMITED" : "OBSERVED" };
  }
  return { pattern: "IRREGULAR", intervalMonths: null, inference: "NONE", confidence: "LOW" };
}

function expectedNextCycle(cycles, recurrence) {
  if (!recurrence.intervalMonths) return null;
  const dated = cycles.filter((cycle) => cycle.startDate).sort((a, b) => String(a.startDate).localeCompare(String(b.startDate)));
  const anchor = dated[dated.length - 1];
  if (anchor?.startDate) {
    const shifted = shiftMonths(anchor.startDate, recurrence.intervalMonths);
    return {
      inference: recurrence.inference === "STATED" ? "INFERRED_FROM_STATED_PATTERN" : "INFERRED",
      windowMonth: shifted.slice(0, 7),
      exactDate: null,
      venue: null,
      host: null,
      sourcingStatus: "UNKNOWN",
      rfpObserved: false,
      meetingAnnounced: false,
      availabilityAssumed: false,
      basis: `${recurrence.pattern} from ${dated.length} observed cycle${dated.length === 1 ? "" : "s"}. The month is a watch window, not an announced meeting.`,
    };
  }
  const covered = [...new Set(cycles.flatMap((cycle) => cycle.coveredYears || []))].sort();
  if (!covered.length) return null;
  const year = covered[covered.length - 1] + (recurrence.intervalMonths >= 24 ? 2 : 1);
  return {
    inference: "INFERRED",
    windowMonth: null,
    windowYear: String(year),
    exactDate: null,
    venue: null,
    host: null,
    sourcingStatus: "UNKNOWN",
    rfpObserved: false,
    meetingAnnounced: false,
    availabilityAssumed: false,
    basis: `The agreement names years through ${covered[covered.length - 1]}. ${year} is the following unlabeled year, not a meeting, a venue, or an RFP.`,
  };
}

function leadTimeFor(program, asOf) {
  const quote = program.cycles.map((cycle) => cycle.evidenceQuote || "").join(" ");
  const stated = text(quote).match(/(?:rfp|request for proposals?).{0,40}(\d{1,2})\s+months/i);
  const hosted = program.cycles
    .filter((cycle) => cycle.host && cycle.startDate && cycle.evidence?.strength === "PAGE_READ")
    .sort((a, b) => String(a.startDate).localeCompare(String(b.startDate)));
  const latestPublic = hosted[hosted.length - 1];
  const observed = latestPublic ? fullMonthsBetween(asOf, latestPublic.startDate) : null;
  const large = LARGE_ASSOCIATION.test(`${program.organizationName} ${quote}`);
  const relationship = stated
    ? { min: Number(stated[1]), max: Number(stated[1]), source: "STATED_LEAD_TIME" }
    : large
      ? { min: 12, max: 24, source: "LARGE_ASSOCIATION_HEURISTIC" }
      : { min: 6, max: 9, source: "POSSIBLE_OUTREACH_WINDOW" };
  return {
    hotelRfpLeadMonths: stated ? Number(stated[1]) : null,
    observedPublicHostLeadMonths: observed,
    observedPublicHostMeeting: latestPublic?.startDate || null,
    relationshipWindowMonths: relationship,
    windowIsUniversalRule: false,
    rfpOpen: false,
    basis: stated
      ? "A source states the RFP lead time. The RFP itself is not marked open."
      : observed != null
        ? `A host was already public ${observed} months before the meeting. That dates a public commitment, not an RFP.`
        : "Hotel sourcing lead time is not in the sources. The review date is a watch, not an open RFP.",
  };
}

function reviewFor(program, asOf) {
  const next = program.expectedNextCycle;
  const lead = program.leadTime;
  const months = lead.relationshipWindowMonths.max;
  let on = null;
  let placeholderMonth = false;
  if (next?.windowMonth) {
    on = shiftMonths(`${next.windowMonth}-01`, -months);
  } else if (next?.windowYear) {
    on = shiftMonths(`${next.windowYear}-01-01`, -months);
    placeholderMonth = true;
  } else {
    on = shiftMonths(asOf, 3);
  }
  const due = on <= asOf;
  return {
    on,
    status: due ? "DUE_FOR_REVIEW" : "SCHEDULED",
    rfpOpen: false,
    placeholderMonth,
    question: next
      ? "Has a later cycle been announced, and is its hotel still unselected? Do not create a meeting, a venue, or an RFP from the inferred window."
      : "Re-read the program history and learn whether another cycle exists. Do not invent the next meeting.",
    reason: next
      ? `${months} months before the inferred window is a research checkpoint. It is not evidence that sourcing is open.`
      : "Recurrence is not established. The checkpoint asks whether the program repeats.",
  };
}

export function buildRecurringProgram({ organizationName, programName, cycles = [], asOf = RECURRING_AS_OF, targetHotel = "", memory = [] } = {}) {
  const classified = cycles.map((cycle) => classifyMeetingCycle({
    ...cycle,
    organizationName: cycle.organizationName || organizationName,
    programName: cycle.programName || programName,
  }, { targetHotel }));
  const recurrence = recurrenceOf(classified.map((cycle, index) => ({ ...cycle, evidenceQuote: cycles[index].evidenceQuote, excerptOnly: cycles[index].excerptOnly })));
  const next = expectedNextCycle(classified, recurrence);
  const hosts = [...new Set(classified.map((cycle) => cycle.host).filter(Boolean))];
  const overflow = classified.some((cycle) => cycle.overflowSupported);
  let treatment = PROGRAM_TREATMENT.WATCH_RECURRENCE_UNKNOWN;
  if (overflow) treatment = PROGRAM_TREATMENT.COMPETITIVE_OVERFLOW;
  else if (!overflow && recurrence.pattern !== "NOT_ESTABLISHED" && hosts.length && next?.sourcingStatus === "UNKNOWN") {
    treatment = PROGRAM_TREATMENT.FUTURE_CYCLE_COMPETITIVE;
  } else if (classified.some((cycle) => cycle.sourcingStatus === "UNKNOWN")) {
    treatment = PROGRAM_TREATMENT.OPEN_OR_UNKNOWN_SOURCING;
  }
  const program = {
    version: RECURRING_INTELLIGENCE_VERSION,
    internalOnly: true,
    customerVisible: false,
    productionPublication: false,
    organizationName: text(organizationName || classified[0]?.organizationName),
    organizationKey: organizationKeyFor(organizationName || classified[0]?.organizationName),
    programName: stripYear(programName || classified[0]?.programName),
    programKey: classified[0]?.programKey || null,
    treatment,
    confirmedOpen: classified.some((cycle) => cycle.confirmedOpen),
    openHostClaimRejected: classified.some((cycle) => cycle.treatment === "COMMITTED_NOT_OPEN" || cycle.sourcingStatus === "HOST_SELECTED"),
    cycles: classified,
    sameOrganizationAcrossCycles: new Set(classified.map((cycle) => cycle.organizationKey)).size <= 1,
    recurrence,
    expectedNextCycle: next,
    hosts,
    committedYears: [...new Set(classified.flatMap((cycle) => cycle.coveredYears))].sort(),
    rotation: hosts.length <= 1 && classified.length > 1 ? "SAME_DESTINATION_OBSERVED" : "NOT_ESTABLISHED",
    contacts: classified.flatMap((cycle) => cycle.contacts),
    publicationPatterns: cycles.flatMap((cycle) => publicationPatterns(cycle.evidenceQuote)),
    leadTime: null,
    review: null,
  };
  program.leadTime = leadTimeFor({ ...program, cycles: cycles.map((cycle, index) => ({ ...cycle, ...classified[index] })) }, asOf);
  program.review = reviewFor(program, asOf);
  program.evidenceFingerprint = hash(classified.map((cycle) => [
    cycle.cycleId,
    cycle.startDate,
    cycle.host,
    cycle.sourcingStatus,
    cycle.evidence.contentHash,
  ].join("|")).join("\n"));
  program.weekly = planWeeklyRecurringRefresh({ programs: [program], memory, asOf })[0];
  return program;
}

export function buildRecurringPortfolio(cycles = [], options = {}) {
  const groups = new Map();
  for (const cycle of cycles) {
    const key = programKeyFor(cycle);
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(cycle);
  }
  const programs = [...groups.values()].map((group) => buildRecurringProgram({
    organizationName: group[0].organizationName,
    programName: group[0].programName || group[0].title,
    cycles: group,
    asOf: options.asOf,
    targetHotel: options.targetHotel,
    memory: options.memory,
  }));
  return {
    version: RECURRING_INTELLIGENCE_VERSION,
    internalOnly: true,
    customerVisible: false,
    programs,
  };
}

export function readRecurringFromOpportunity(opp = {}, options = {}) {
  if (Array.isArray(options.cycles) && options.cycles.length) {
    return buildRecurringPortfolio(options.cycles, options);
  }
  if (!text(opp.organizationName) && !text(opp.evidenceQuote)) {
    return {
      version: RECURRING_INTELLIGENCE_VERSION,
      status: "NO_ORGANIZATION",
      internalOnly: true,
      customerVisible: false,
      productionPublication: false,
    };
  }
  return buildRecurringProgram({
    organizationName: opp.organizationName,
    programName: opp.programName || opp.title || opp.organizationName,
    cycles: [{
      cycleId: opp.id || opp.opportunityId || null,
      organizationName: opp.organizationName,
      programName: opp.programName || opp.title,
      startDate: opp.eventStartDate || null,
      endDate: opp.eventEndDate || null,
      evidenceQuote: opp.evidenceQuote || "",
      sourceUrl: opp.officialSource || opp.sourceUrl || null,
      contentHash: opp.contentHash || null,
      retrievedOn: opp.retrievedOn || null,
    }],
    asOf: options.asOf || RECURRING_AS_OF,
    targetHotel: options.targetHotel || "",
    memory: options.memory || [],
  });
}

export function planWeeklyRecurringRefresh({ programs = [], memory = [], asOf = RECURRING_AS_OF } = {}) {
  const prior = new Map(memory.map((item) => [item.programKey, item]));
  return programs.map((program) => {
    const remembered = prior.get(program.programKey);
    const unchanged = remembered && remembered.evidenceFingerprint === program.evidenceFingerprint;
    const reviewDue = program.review?.on && program.review.on <= asOf;
    const currentSourcing = program.cycles.find((cycle) => cycle.sourcingStatus)?.sourcingStatus || "UNKNOWN";
    const newCycle = remembered && (program.cycles || []).some((cycle) => cycle.cycleId && !(remembered.cycleIds || []).includes(cycle.cycleId));
    let action = "RETAIN";
    if (remembered && remembered.sourcingStatus && remembered.sourcingStatus !== currentSourcing) action = "RECLASSIFY";
    else if (newCycle) action = "CREATE_CYCLE";
    else if (reviewDue) action = "REVISIT_DECISION_WINDOW";
    else if (unchanged) action = "SKIP_UNCHANGED";
    else if (!remembered && program.expectedNextCycle) action = "CREATE_FUTURE_CYCLE_WATCH";
    return {
      programKey: program.programKey,
      organizationKey: program.organizationKey,
      action,
      duplicateOrganization: false,
      futureCycleWatchId: program.expectedNextCycle ? `${program.programKey}:watch:${program.expectedNextCycle.windowMonth || program.expectedNextCycle.windowYear}` : null,
      meetingCreated: false,
      customerVisible: false,
      publication: "UNCHANGED",
    };
  });
}

export function selectParallelRecurringTool(program = {}) {
  if (program.cycles?.length) {
    return {
      tool: "structured_task",
      processor: "pro",
      estimatedUsd: 0.1,
      generator: null,
      reason: "The organization is already known. One structured Task follows its history and missing commercial facts.",
    };
  }
  return {
    tool: "findall",
    processor: null,
    generator: "preview",
    estimatedUsd: 0.1,
    reason: "Segment discovery uses FindAll preview. Core and pro generators stay unauthorized.",
  };
}

export function parallelRecurringTaskBrief(program = {}) {
  return {
    paidCalls: 0,
    outputIsEvidence: true,
    markdownReportIsNotQualification: true,
    questions: [
      "What meeting cycles are announced, and which claims are only inferred?",
      "Which hotels have hosted the program, and for which years?",
      "Does a source state overflow rooms, or only that a host hotel is selected?",
      "Is a later cycle announced? If not, return no meeting, no venue, and no RFP.",
      "Who decides accommodation, and which contact belongs to this organization?",
      "What sourcing window is source-backed? If unknown, return a review need rather than an open RFP.",
      "Which of those facts could matter to the target hotel, and which are competitor commitments?",
    ],
    supervisorRetains: ["budget", "evidence validation", "entity resolution", "publication"],
    programKey: program.programKey || null,
  };
}

export function proposeBoundedParallelComparison() {
  return {
    version: RECURRING_INTELLIGENCE_VERSION,
    authorized: false,
    paidCalls: 0,
    airtableWrites: 0,
    publication: false,
    aggregateCapUsd: 1,
    perHotelCapUsd: 0.5,
    transferable: false,
    ledgerPath: "reports/gdi-recurring-parallel-comparison-v1/budget-ledger.json",
    ledgerCreated: false,
    forbiddenLedgers: [
      "reports/gdi-eight-hotel-research-benchmark-v1/cohort-a-live/budget-ledger.json",
      "reports/gdi-segment-e2e-certification-v1/live/budget-ledger.json",
    ],
    refusedProcessors: ["ultra", "ultra2x", "ultra4x", "ultra8x", "pro-fast"],
    refusedGenerators: ["core", "pro"],
    arms: [
      {
        hotel: "Bethesda Marriott",
        segmentId: "associations_government",
        pathway: "FUTURE_CYCLE_COMPETITIVE",
        subject: "NADO & DDAA Washington Conference",
        tool: selectParallelRecurringTool({ cycles: [{}] }),
        brief: parallelRecurringTaskBrief(),
        excludesOpenHostClaimFor: "2028 Crystal Gateway Marriott",
      },
      {
        hotel: "Nobu Hotel London Portman Square",
        segmentId: "professional_planners_dmc",
        pathway: "ACCOUNT_LED",
        subject: "London DMC discovery",
        tool: selectParallelRecurringTool({ cycles: [] }),
        brief: parallelRecurringTaskBrief(),
        matchRequiresHotelRoomPlacement: true,
        officeAndEmailAreNotEnough: true,
      },
    ],
  };
}
