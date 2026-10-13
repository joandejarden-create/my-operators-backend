/**
 * Demand Generator + Program entity builders (hotel-agnostic identities).
 */

import { createHash } from "node:crypto";
import {
  GENERATOR_ID_PREFIX,
  PROGRAM_ID_PREFIX,
  SERIES_ID_PREFIX,
  ORGANIZATION_TYPE,
  GENERATOR_STATUS,
  PROGRAM_TYPE,
  RECURRENCE_STATUS,
  SOURCE_AUTHORITY,
  DG_SCHEMA_VERSION,
} from "./constants.js";

function clean(s) {
  return String(s || "").trim();
}

function normKey(s) {
  return clean(s)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim()
    .replace(/\s+/g, " ");
}

export function domainFromUrl(url) {
  try {
    const u = new URL(String(url || ""));
    return u.hostname.replace(/^www\./i, "").toLowerCase() || null;
  } catch {
    return null;
  }
}

export function computeDemandGeneratorId({
  organizationName,
  officialDomain,
  website,
} = {}) {
  const domain = clean(officialDomain) || domainFromUrl(website) || "";
  const name = normKey(organizationName);
  const seed = domain ? `d:${domain}|n:${name}` : `n:${name}`;
  return `${GENERATOR_ID_PREFIX}${createHash("sha256").update(seed).digest("hex").slice(0, 16)}`;
}

export function computeProgramId({
  demandGeneratorId,
  programName,
  programType,
} = {}) {
  const seed = [
    clean(demandGeneratorId),
    normKey(programName),
    clean(programType || "").toUpperCase(),
  ].join("|");
  return `${PROGRAM_ID_PREFIX}${createHash("sha256").update(seed).digest("hex").slice(0, 14)}`;
}

export function computeSeriesId({ demandGeneratorId, programName } = {}) {
  const seed = `${clean(demandGeneratorId)}|${normKey(programName)}`;
  return `${SERIES_ID_PREFIX}${createHash("sha256").update(seed).digest("hex").slice(0, 12)}`;
}

export function normalizeOrganizationType(raw) {
  const t = clean(raw).toUpperCase().replace(/[\s/-]+/g, "_");
  if (ORGANIZATION_TYPE[t]) return ORGANIZATION_TYPE[t];
  if (Object.values(ORGANIZATION_TYPE).includes(t)) return t;
  const blob = clean(raw).toLowerCase();
  if (/associat|society|chamber/.test(blob)) return ORGANIZATION_TYPE.ASSOCIATION;
  if (/nih|nist|fda|agency|federal|department of|dod|va\b/.test(blob)) {
    return ORGANIZATION_TYPE.GOVERNMENT_AGENCY;
  }
  if (/contractor|defense|aerospace|booz|leidos|saic|lockheed/.test(blob)) {
    return ORGANIZATION_TYPE.GOVERNMENT_CONTRACTOR;
  }
  if (/consult|advisory|deloitte|accenture|mckinsey/.test(blob)) {
    return ORGANIZATION_TYPE.CONSULTING_FIRM;
  }
  if (/universit|college|school of/.test(blob)) return ORGANIZATION_TYPE.UNIVERSITY;
  if (/hospital|health system|medical center|clinic/.test(blob)) {
    return ORGANIZATION_TYPE.HOSPITAL_HEALTH_SYSTEM;
  }
  if (/research|institute|laboratory|lab\b/.test(blob)) {
    return ORGANIZATION_TYPE.RESEARCH_INSTITUTION;
  }
  if (/soccer|tournament|athletic|sports/.test(blob)) {
    return ORGANIZATION_TYPE.SPORTS_ORGANIZATION;
  }
  if (/conference|expo|trade show|producer/.test(blob)) {
    return ORGANIZATION_TYPE.CONFERENCE_PRODUCER;
  }
  if (/nonprofit|foundation|ngo/.test(blob)) return ORGANIZATION_TYPE.NONPROFIT;
  if (/training|academy|certification|cpe|ceu/.test(blob)) {
    return ORGANIZATION_TYPE.TRAINING_PROVIDER;
  }
  if (/professional|institute of|board of/.test(blob)) {
    return ORGANIZATION_TYPE.PROFESSIONAL_BODY;
  }
  return ORGANIZATION_TYPE.OTHER;
}

export function normalizeProgramType(raw) {
  const t = clean(raw).toUpperCase().replace(/[\s/-]+/g, "_");
  if (PROGRAM_TYPE[t]) return PROGRAM_TYPE[t];
  if (Object.values(PROGRAM_TYPE).includes(t)) return t;
  const blob = clean(raw).toLowerCase();
  if (/annual.*(conference|meeting|summit)/.test(blob)) {
    return PROGRAM_TYPE.ANNUAL_CONFERENCE;
  }
  if (/training|academy|bootcamp/.test(blob)) return PROGRAM_TYPE.TRAINING_ACADEMY;
  if (/symposium/.test(blob)) return PROGRAM_TYPE.SYMPOSIUM;
  if (/tournament|cup|showcase/.test(blob)) {
    return PROGRAM_TYPE.SPORTS_TOURNAMENT_SERIES;
  }
  if (/board|committee/.test(blob)) return PROGRAM_TYPE.BOARD_CYCLE;
  if (/residential|dorm|orientation/.test(blob)) {
    return PROGRAM_TYPE.UNIVERSITY_RESIDENTIAL;
  }
  if (/certif/.test(blob)) return PROGRAM_TYPE.CERTIFICATION_PROGRAM;
  if (/government|federal program/.test(blob)) return PROGRAM_TYPE.GOVERNMENT_PROGRAM;
  return PROGRAM_TYPE.OTHER;
}

export function normalizeRecurrenceStatus(raw) {
  const t = clean(raw).toUpperCase().replace(/[\s-]+/g, "_");
  if (RECURRENCE_STATUS[t]) return RECURRENCE_STATUS[t];
  if (Object.values(RECURRENCE_STATUS).includes(t)) return t;
  return RECURRENCE_STATUS.UNKNOWN;
}

/**
 * Historical recurrence guides research — never invents a future cycle.
 */
export function assertNoInventedFuture({
  recurrenceStatus,
  confirmedFutureCycle,
  inventFromHistoryOnly,
} = {}) {
  if (inventFromHistoryOnly === true) {
    return {
      ok: false,
      code: "INVENTED_FUTURE_FROM_HISTORY",
      message:
        "Historical recurrence must not create a future opportunity without evidence",
    };
  }
  const status = normalizeRecurrenceStatus(recurrenceStatus);
  if (
    (status === RECURRENCE_STATUS.RECURRING_HISTORICAL ||
      status === RECURRENCE_STATUS.POSSIBLE_RECURRING) &&
    confirmedFutureCycle &&
    confirmedFutureCycle.invented === true
  ) {
    return {
      ok: false,
      code: "INVENTED_FUTURE_FROM_HISTORY",
      message: "confirmedFutureCycle.invented is forbidden",
    };
  }
  return { ok: true };
}

export function buildDemandGeneratorEntity(input = {}) {
  const organizationName = clean(input.organizationName);
  if (!organizationName) {
    const err = new Error("demand_generator_name_required");
    err.code = "demand_generator_name_required";
    throw err;
  }
  const website = clean(input.website) || null;
  const officialDomain =
    clean(input.officialDomain) || domainFromUrl(website) || null;
  const demandGeneratorId =
    clean(input.demandGeneratorId) ||
    computeDemandGeneratorId({ organizationName, officialDomain, website });
  const now = new Date().toISOString();
  const aliases = Array.isArray(input.organizationAliases)
    ? input.organizationAliases.map(clean).filter(Boolean)
    : clean(input.organizationAliases)
      ? [clean(input.organizationAliases)]
      : [];
  const primaryMarkets = Array.isArray(input.primaryMarkets)
    ? input.primaryMarkets.map(clean).filter(Boolean)
    : [];
  const sourceUrls = Array.isArray(input.sourceUrls)
    ? input.sourceUrls.map(clean).filter(Boolean)
    : [];

  return {
    demandGeneratorId,
    organizationName,
    organizationAliases: aliases,
    organizationType: normalizeOrganizationType(
      input.organizationType || organizationName
    ),
    officialDomain,
    website,
    headquartersLocation: clean(input.headquartersLocation) || null,
    primaryMarkets,
    industry: clean(input.industry || input.sector) || null,
    generatorStatus:
      GENERATOR_STATUS[clean(input.generatorStatus).toUpperCase()] ||
      Object.values(GENERATOR_STATUS).find(
        (v) => v === clean(input.generatorStatus).toUpperCase()
      ) ||
      GENERATOR_STATUS.DISCOVERED,
    sourceUrls,
    sourceAuthority:
      SOURCE_AUTHORITY[clean(input.sourceAuthority).toUpperCase()] ||
      SOURCE_AUTHORITY.OTHER,
    firstSeenAt: input.firstSeenAt || now,
    lastSeenAt: input.lastSeenAt || now,
    lastVerifiedAt: input.lastVerifiedAt || null,
    confidence:
      Number.isFinite(Number(input.confidence)) ? Number(input.confidence) : null,
    nextResearchAt: input.nextResearchAt || null,
    researchReason: clean(input.researchReason) || null,
    lastResearchResult: clean(input.lastResearchResult) || null,
    lastMaterialSignalAt: input.lastMaterialSignalAt || null,
    schemaVersion: DG_SCHEMA_VERSION,
    isCustomerFacing: false,
    payload: input.payload || null,
  };
}

export function buildProgramEntity(input = {}) {
  const demandGeneratorId = clean(input.demandGeneratorId);
  const programName = clean(input.programName);
  if (!demandGeneratorId || !programName) {
    const err = new Error("demand_program_requires_generator_and_name");
    err.code = "demand_program_requires_generator_and_name";
    throw err;
  }
  const programType = normalizeProgramType(input.programType || programName);
  const programId =
    clean(input.programId) ||
    computeProgramId({ demandGeneratorId, programName, programType });
  const seriesId =
    clean(input.seriesId) ||
    computeSeriesId({ demandGeneratorId, programName });
  const recurrenceStatus = normalizeRecurrenceStatus(input.recurrenceStatus);
  const inventGuard = assertNoInventedFuture({
    recurrenceStatus,
    confirmedFutureCycle: input.confirmedFutureCycle,
    inventFromHistoryOnly: input.inventFromHistoryOnly,
  });
  if (!inventGuard.ok) {
    const err = new Error(inventGuard.code);
    err.code = inventGuard.code;
    err.details = inventGuard;
    throw err;
  }
  const now = new Date().toISOString();
  return {
    programId,
    seriesId,
    demandGeneratorId,
    programName,
    programAliases: Array.isArray(input.programAliases)
      ? input.programAliases.map(clean).filter(Boolean)
      : [],
    programType,
    market: clean(input.market || input.geography) || null,
    typicalTiming: clean(input.typicalTiming) || null,
    recurrenceStatus,
    recurrenceFrequency: clean(input.recurrenceFrequency) || null,
    historicalCycles: Array.isArray(input.historicalCycles)
      ? input.historicalCycles
      : [],
    confirmedFutureCycle:
      input.confirmedFutureCycle && input.confirmedFutureCycle.invented !== true
        ? input.confirmedFutureCycle
        : null,
    sourceUrls: Array.isArray(input.sourceUrls)
      ? input.sourceUrls.map(clean).filter(Boolean)
      : [],
    firstSeenAt: input.firstSeenAt || now,
    lastSeenAt: input.lastSeenAt || now,
    lastVerifiedAt: input.lastVerifiedAt || null,
    schemaVersion: DG_SCHEMA_VERSION,
  };
}
