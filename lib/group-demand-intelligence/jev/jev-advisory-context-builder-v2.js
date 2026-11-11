/**
 * Build Jev V2 advisory context from persisted GDI artifacts.
 * Never fabricates missing commercial facts.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  ACTION_ATTEMPT_STATUS,
  DEMAND_TIMING_CLASS,
  normalizeAdvisoryContextV2,
} from "./jev-commercial-research-decision-v2.js";

const ROOT = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "../../..",
);

const HOTEL_META_BY_ID = Object.freeze({
  recCAGqkERSXgiku4: {
    hotelName: "Sheraton Guadalajara Expo",
    hotelCity: "Guadalajara",
    hotelCountry: "Mexico",
    hotelPositioning: "Large-box expo-adjacent full service",
    hotelCapabilities: ["meetings", "guestrooms", "banquet"],
  },
  recTk5I8VOzsPhxDz: {
    hotelName: "voco Cancún Zona Hotelera",
    hotelCity: "Cancún",
    hotelCountry: "Mexico",
    hotelPositioning: "Zona Hotelera lifestyle / meetings",
    hotelCapabilities: ["meetings", "guestrooms", "catering"],
  },
  recXh4NgF8sDbi9cQ: {
    hotelName: "Wyndham Alltra Punta Cana",
    hotelCity: "Punta Cana",
    hotelCountry: "Dominican Republic",
    hotelPositioning: "All-inclusive resort groups / incentives",
    hotelCapabilities: ["guestrooms", "incentive_groups", "events"],
  },
  recH9h3McgMNFQEla: {
    hotelName: "Radisson Blu Toronto Downtown",
    hotelCity: "Toronto",
    hotelCountry: "Canada",
    hotelPositioning: "Downtown full service / meetings",
    hotelCapabilities: ["meetings", "guestrooms", "banquet"],
  },
});

const HOTEL_ID_BY_NAME = Object.freeze({
  "Sheraton Guadalajara Expo": "recCAGqkERSXgiku4",
  "voco Cancún Zona Hotelera": "recTk5I8VOzsPhxDz",
  "Wyndham Alltra Punta Cana": "recXh4NgF8sDbi9cQ",
  "Radisson Blu Toronto Downtown": "recH9h3McgMNFQEla",
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function readJson(fp, fb = null) {
  if (!fs.existsSync(fp)) return fb;
  return JSON.parse(fs.readFileSync(fp, "utf8").replace(/^\uFEFF/, ""));
}

export function classifyDemandTimingFromEvidence({
  title = "",
  dateLabel = "",
  deepBlob = "",
  stagedMissing = [],
} = {}) {
  const blob = `${title} ${dateLabel} ${deepBlob} ${stagedMissing.join(" ")}`;
  if (
    /aniversario|anniversary|historical|past edition|already (held|occurred)|COMPLETED_OR_HISTORICAL/i.test(
      blob,
    ) &&
    !/2026|2027|recurring|annual series|next edition/i.test(blob)
  ) {
    return DEMAND_TIMING_CLASS.HISTORICAL;
  }
  if (
    /reward trip 2025|incentive.*2025|trip 2025/i.test(blob) &&
    !/2026|2027/i.test(blob)
  ) {
    return DEMAND_TIMING_CLASS.HISTORICAL;
  }
  if (/2026|2027|upcoming|future edition|next edition/i.test(blob)) {
    return DEMAND_TIMING_CLASS.FUTURE;
  }
  if (/annual|recurring|series|president.?s club|incentive program/i.test(blob)) {
    return DEMAND_TIMING_CLASS.RECURRING;
  }
  return DEMAND_TIMING_CLASS.UNDETERMINED;
}

export function assessRecurringAccountPotential({
  title = "",
  organizationName = "",
  segmentId = "",
  stagedMissing = [],
  demandTimingClass,
} = {}) {
  const blob = `${title} ${organizationName} ${segmentId} ${stagedMissing.join(" ")}`;
  if (
    /reward trip|incentive|president.?s club|annual congress|dmc|planner|intermediar/i.test(
      blob,
    )
  ) {
    return {
      recurringAccountPotential: true,
      recurringAccountEvidence:
        "Title/segment suggests programme or intermediary account that may recur; not proof of live demand.",
    };
  }
  if (demandTimingClass === DEMAND_TIMING_CLASS.RECURRING) {
    return {
      recurringAccountPotential: true,
      recurringAccountEvidence: "Timing class RECURRING from evidence labels.",
    };
  }
  return { recurringAccountPotential: false, recurringAccountEvidence: null };
}

export function mapAttemptStatus(status) {
  const s = String(status || "");
  if (Object.values(ACTION_ATTEMPT_STATUS).includes(s)) return s;
  return ACTION_ATTEMPT_STATUS.UNKNOWN;
}

export function resolveDemandTimingClassFromRow(row = {}, extras = {}) {
  if (extras.demandTimingClass) return extras.demandTimingClass;
  const t = row.timing?.timing;
  if (t === "FUTURE_OR_DATED_2026_PLUS") return DEMAND_TIMING_CLASS.FUTURE;
  if (
    t === "HISTORICAL_OR_ONE_OFF_CELEBRATION" ||
    t === "PAST_INCENTIVE_EDITION"
  ) {
    return DEMAND_TIMING_CLASS.HISTORICAL;
  }
  if (t === "AMBIGUOUS") return DEMAND_TIMING_CLASS.UNDETERMINED;
  return classifyDemandTimingFromEvidence({
    title: row.title,
    dateLabel: row.timing?.note,
    stagedMissing: row.stagedMissing || [],
  });
}

function publicResolveFromAttempt(attempt) {
  if (
    attempt === ACTION_ATTEMPT_STATUS.NEVER_INVESTIGATED ||
    attempt === ACTION_ATTEMPT_STATUS.ATTEMPTED_STILL_OPEN
  ) {
    return true;
  }
  if (
    attempt === ACTION_ATTEMPT_STATUS.ATTEMPTED_SOURCE_PRESENT ||
    attempt === ACTION_ATTEMPT_STATUS.EXHAUSTED
  ) {
    return false;
  }
  // ATTEMPTED_PARTIAL / UNKNOWN — do not invent resolvability
  return null;
}

/**
 * Build context from the five-candidate value-audit row + optional ledger/staged extras.
 */
export function buildAdvisoryContextFromValueAuditRow(row = {}, extras = {}) {
  const hotelId =
    extras.hotelId || HOTEL_ID_BY_NAME[row.hotel] || null;
  const hotelMeta = hotelId ? HOTEL_META_BY_ID[hotelId] || {} : {};
  const demandTimingClass = resolveDemandTimingClassFromRow(row, extras);
  const recurring = assessRecurringAccountPotential({
    title: row.title,
    organizationName: extras.organizationName || "",
    segmentId: row.segment,
    stagedMissing: row.stagedMissing || [],
    demandTimingClass,
  });
  const attempt = mapAttemptStatus(row.attemptStatus);

  return normalizeAdvisoryContextV2({
    candidateId: row.candidate_id,
    hotelId,
    hotelName: row.hotel || hotelMeta.hotelName || null,
    hotelCity: hotelMeta.hotelCity || null,
    hotelCountry: hotelMeta.hotelCountry || null,
    hotelPositioning: hotelMeta.hotelPositioning || null,
    hotelCapabilities: hotelMeta.hotelCapabilities || [],
    segmentId: row.segment,
    title: row.title,
    organizationName: extras.organizationName || null,
    opportunityDateLabel: row.timing?.note || null,
    demandTimingClass,
    ...recurring,
    sourcesAlreadyInvestigated: [
      ...(extras.sourceUrls || []),
      ...((row.freeFetches || []).map((f) => f.url).filter(Boolean)),
    ],
    priorStrategies: [
      { strategy: "exa_discovery", outcome: "COMPLETED" },
      {
        strategy: "deep_research",
        outcome: row.deepArtifactPath ? "COMPLETED" : "NOT_STARTED",
      },
      {
        strategy: row.jevRecommendation || "jev_recommended_action",
        outcome: attempt,
      },
    ],
    deepResearchCompleted: Boolean(row.deepArtifactPath),
    outstandingBlockers: row.stagedMissing || [],
    recommendedNextAction: row.jevRecommendation || null,
    recommendedActionAttemptStatus: attempt,
    lifetimeResearchCostUsd: extras.lifetimeResearchCostUsd ?? 0,
    remainingAuthorizedBudgetUsd:
      extras.remainingAuthorizedBudgetUsd === undefined
        ? null
        : extras.remainingAuthorizedBudgetUsd,
    publicResearchCanRealisticallyResolveBlockers:
      extras.publicResearchCanRealisticallyResolveBlockers !== undefined
        ? extras.publicResearchCanRealisticallyResolveBlockers
        : publicResolveFromAttempt(attempt),
    commercialFitInadequate: extras.commercialFitInadequate === true,
    nativeJevPrequal: row.jevPrequal || null,
    nativeJevStopContinue: row.jevStopContinue || null,
    provenanceGaps: extras.provenanceGaps || [],
  });
}

export function loadFiveCandidateValueAuditRows(
  auditPath = path.join(
    ROOT,
    "reports/gdi-jev-benchmark/five-candidate-value-audit/01-CANDIDATE_VALUE_ROWS.json",
  ),
) {
  return readJson(auditPath, []);
}

export function hotelMetaForId(hotelId) {
  return HOTEL_META_BY_ID[hotelId] || null;
}

export function hotelIdForName(hotelName) {
  return HOTEL_ID_BY_NAME[text(hotelName)] || null;
}

/**
 * Build V2 advisory context from benchmark sample row + discovery ledger row.
 * Used for Cohort B and live validation prep — no fabricated commercial facts.
 */
export function buildAdvisoryContextFromLedgerSample(
  sampleRow = {},
  ledgerRow = null,
  extras = {},
) {
  const hotelId =
    extras.hotelId ||
    ledgerRow?.hotelId ||
    HOTEL_ID_BY_NAME[sampleRow.hotel] ||
    null;
  const hotelMeta = hotelId ? HOTEL_META_BY_ID[hotelId] || {} : {};
  const urls = [
    ...(sampleRow.source_urls || []),
    ...(ledgerRow?.originalSourceUrls || []),
  ].filter(Boolean);
  const title = sampleRow.title || ledgerRow?.title || "";
  const stagedMissing = [];
  if (!urls.length) stagedMissing.push("source_url");
  if (!ledgerRow?.deepArtifactPath) stagedMissing.push("deep_research");
  const buyerRoles =
    ledgerRow?.structuredExaCandidateFields?.relevantBuyerRoles || [];
  if (!buyerRoles.length) {
    stagedMissing.push("Named commercially relevant contact not yet established");
  }
  if (
    !ledgerRow?.structuredExaCandidateFields?.guestroomDemandStatus ||
    ledgerRow?.structuredExaCandidateFields?.guestroomDemandStatus === "UNKNOWN"
  ) {
    stagedMissing.push(
      "Demand mechanism needs verification before sales-ready (speculative thesis)",
    );
  }

  const demandTimingClass =
    extras.demandTimingClass ||
    classifyDemandTimingFromEvidence({
      title,
      dateLabel: extras.opportunityDateLabel || "",
      deepBlob: text(
        ledgerRow?.structuredExaCandidateFields?.demandMechanismSummary || "",
      ),
      stagedMissing,
    });

  const recurring = assessRecurringAccountPotential({
    title,
    organizationName: ledgerRow?.organizationName || "",
    segmentId: sampleRow.segment,
    stagedMissing,
    demandTimingClass,
  });

  let attempt =
    extras.recommendedActionAttemptStatus || ACTION_ATTEMPT_STATUS.UNKNOWN;
  if (!extras.recommendedActionAttemptStatus) {
    if (urls.length && /housing|venue-travel|lodging/i.test(urls.join(" "))) {
      attempt = ACTION_ATTEMPT_STATUS.ATTEMPTED_SOURCE_PRESENT;
    } else if (ledgerRow?.deepArtifactPath && urls.length) {
      attempt = ACTION_ATTEMPT_STATUS.ATTEMPTED_PARTIAL;
    } else if (ledgerRow?.deepArtifactPath) {
      attempt = ACTION_ATTEMPT_STATUS.ATTEMPTED_STILL_OPEN;
    } else {
      attempt = ACTION_ATTEMPT_STATUS.NEVER_INVESTIGATED;
    }
  }

  return normalizeAdvisoryContextV2({
    candidateId: sampleRow.candidate_id,
    hotelId,
    hotelName: sampleRow.hotel || hotelMeta.hotelName || null,
    hotelCity: hotelMeta.hotelCity || null,
    hotelCountry: hotelMeta.hotelCountry || null,
    hotelPositioning: hotelMeta.hotelPositioning || null,
    hotelCapabilities: hotelMeta.hotelCapabilities || [],
    segmentId: sampleRow.segment,
    title,
    organizationName: ledgerRow?.organizationName || null,
    opportunityDateLabel: extras.opportunityDateLabel || null,
    demandTimingClass,
    ...recurring,
    sourcesAlreadyInvestigated: urls,
    priorStrategies: [
      { strategy: "exa_discovery", outcome: ledgerRow?.exaRunId ? "COMPLETED" : "UNKNOWN" },
      {
        strategy: "deep_research",
        outcome: ledgerRow?.deepArtifactPath ? "COMPLETED" : "NOT_STARTED",
      },
    ],
    deepResearchCompleted: Boolean(ledgerRow?.deepArtifactPath),
    outstandingBlockers: stagedMissing,
    recommendedNextAction: extras.recommendedNextAction || null,
    recommendedActionAttemptStatus: attempt,
    lifetimeResearchCostUsd: extras.lifetimeResearchCostUsd ?? 0,
    remainingAuthorizedBudgetUsd:
      extras.remainingAuthorizedBudgetUsd === undefined
        ? null
        : extras.remainingAuthorizedBudgetUsd,
    publicResearchCanRealisticallyResolveBlockers:
      extras.publicResearchCanRealisticallyResolveBlockers !== undefined
        ? extras.publicResearchCanRealisticallyResolveBlockers
        : publicResolveFromAttempt(attempt),
    commercialFitInadequate: extras.commercialFitInadequate === true,
    provenanceGaps: extras.provenanceGaps || [],
  });
}
