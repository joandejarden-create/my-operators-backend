/**
 * Demand Generator V1.1 — lodging / future-cycle evidence harvest helpers.
 * Does not lower TRUE bar. Historical recurrence alone is insufficient.
 */

import {
  ORGANIZATION_TYPE,
  PROGRAM_TYPE,
  SIGNAL_STATUS,
} from "./constants.js";

export const WATCH_FAIL_REASON = Object.freeze({
  NO_FUTURE_DATE: "NO_FUTURE_DATE",
  NO_LODGING_SIGNAL: "NO_LODGING_SIGNAL",
  NO_TRAVEL_SIGNAL: "NO_TRAVEL_SIGNAL",
  NO_EVENT_LOCATION: "NO_EVENT_LOCATION",
  NO_MARKET_CONFIRMATION: "NO_MARKET_CONFIRMATION",
  NO_HOUSING_SIGNAL: "NO_HOUSING_SIGNAL",
  NO_ROOM_DEMAND_THESIS: "NO_ROOM_DEMAND_THESIS",
  NO_OPEN_SOURCING: "NO_OPEN_SOURCING",
  NO_ACTION_PATH: "NO_ACTION_PATH",
  PAST_CYCLE_ONLY: "PAST_CYCLE_ONLY",
  PROGRAM_TOO_GENERIC: "PROGRAM_TOO_GENERIC",
  OTHER: "OTHER",
});

export const LODGING_EVIDENCE_STRENGTH = Object.freeze({
  STRONG: "STRONG",
  MODERATE: "MODERATE",
  WEAK: "WEAK",
  INSUFFICIENT: "INSUFFICIENT",
});

export const HARVEST_CLASSIFICATION = Object.freeze({
  GENERATOR_INCREMENTAL_NEW: "GENERATOR_INCREMENTAL_NEW",
  GENERIC_DISCOVERY_OVERLAP: "GENERIC_DISCOVERY_OVERLAP",
  EXISTING_UPDATE: "EXISTING_UPDATE",
  FUTURE_WATCH: "FUTURE_WATCH",
  REJECT: "REJECT",
});

function clean(s) {
  return String(s || "").trim();
}

function blob(s) {
  return clean(s).toLowerCase();
}

/**
 * Map prior qualify failReasons → V1.1 explicit taxonomy.
 */
export function classifyWatchFailureReasons(signal = {}) {
  const prior = Array.isArray(signal.failReasons) ? signal.failReasons : [];
  const out = new Set();
  for (const r of prior) {
    const u = String(r || "").toUpperCase();
    if (u === "NO_FUTURE_TIMING" || u === "NO_FUTURE_DATE") {
      out.add(WATCH_FAIL_REASON.NO_FUTURE_DATE);
    } else if (u === "NO_LODGING_THESIS" || u === "NO_LODGING_SIGNAL") {
      out.add(WATCH_FAIL_REASON.NO_LODGING_SIGNAL);
      out.add(WATCH_FAIL_REASON.NO_ROOM_DEMAND_THESIS);
    } else if (u === "NO_ACTION_PATH") {
      out.add(WATCH_FAIL_REASON.NO_ACTION_PATH);
    } else if (u === "WEAK_HOTEL_FIT" || u === "OUTSIDE_COMMERCIAL_GEOGRAPHY") {
      out.add(WATCH_FAIL_REASON.NO_MARKET_CONFIRMATION);
    } else if (u === "HISTORY_WITHOUT_FUTURE_EVIDENCE" || u === "INVENTED_FUTURE_FROM_HISTORY") {
      out.add(WATCH_FAIL_REASON.PAST_CYCLE_ONLY);
    } else if (u === "GENERATOR_ONLY_NOT_OPPORTUNITY") {
      out.add(WATCH_FAIL_REASON.PROGRAM_TOO_GENERIC);
    } else if (u) {
      out.add(WATCH_FAIL_REASON.OTHER);
    }
  }
  // Seed WATCH monitors without dates/lodging
  if (!signal.eventStartDate && !out.has(WATCH_FAIL_REASON.NO_FUTURE_DATE)) {
    out.add(WATCH_FAIL_REASON.NO_FUTURE_DATE);
  }
  if (!signal.lodgingDemandThesis && !out.has(WATCH_FAIL_REASON.NO_LODGING_SIGNAL)) {
    const thesis = blob(signal.hotelDemandThesis);
    if (!/hotel|lodging|housing|room block|stay-to-play|accommodat/.test(thesis)) {
      out.add(WATCH_FAIL_REASON.NO_LODGING_SIGNAL);
    }
  }
  if (!out.size) out.add(WATCH_FAIL_REASON.OTHER);
  return [...out];
}

/**
 * Build evidence-gap-driven queries for one generator/program.
 * Prefer official domain. No hotel-name hardcodes.
 */
export function buildLodgingEvidenceQueries({
  generator = {},
  program = {},
  missingReasons = [],
  year = new Date().getUTCFullYear() + 1,
} = {}) {
  const domain = clean(generator.officialDomain);
  const name = clean(generator.organizationName);
  const programName = clean(program.programName);
  const orgType = clean(generator.organizationType).toUpperCase();
  const programType = clean(program.programType).toUpperCase();
  const missing = new Set(missingReasons);
  const queries = [];
  const push = (q, lane) => {
    if (!q || queries.some((x) => x.query === q)) return;
    queries.push({ query: q, lane, domain: domain || null });
  };

  const needFuture =
    missing.has(WATCH_FAIL_REASON.NO_FUTURE_DATE) ||
    missing.has(WATCH_FAIL_REASON.PAST_CYCLE_ONLY);
  const needLodging =
    missing.has(WATCH_FAIL_REASON.NO_LODGING_SIGNAL) ||
    missing.has(WATCH_FAIL_REASON.NO_HOUSING_SIGNAL) ||
    missing.has(WATCH_FAIL_REASON.NO_ROOM_DEMAND_THESIS) ||
    missing.has(WATCH_FAIL_REASON.NO_TRAVEL_SIGNAL);

  const isSports =
    orgType === ORGANIZATION_TYPE.SPORTS_ORGANIZATION ||
    programType === PROGRAM_TYPE.SPORTS_TOURNAMENT_SERIES;
  const isTraining =
    orgType === ORGANIZATION_TYPE.TRAINING_PROVIDER ||
    programType === PROGRAM_TYPE.TRAINING_ACADEMY ||
    programType === PROGRAM_TYPE.CERTIFICATION_PROGRAM;
  const isGov =
    orgType === ORGANIZATION_TYPE.GOVERNMENT_AGENCY ||
    orgType === ORGANIZATION_TYPE.GOVERNMENT_CONTRACTOR ||
    programType === PROGRAM_TYPE.GOVERNMENT_PROGRAM ||
    programType === PROGRAM_TYPE.PROJECT_DEPLOYMENT;
  const isUniHealth =
    orgType === ORGANIZATION_TYPE.UNIVERSITY ||
    orgType === ORGANIZATION_TYPE.HOSPITAL_HEALTH_SYSTEM ||
    orgType === ORGANIZATION_TYPE.RESEARCH_INSTITUTION;

  if (domain && needFuture) {
    push(
      `site:${domain} ${programName || "conference OR meeting OR tournament"} ${year} OR ${year - 1}`,
      "official_future"
    );
    push(`site:${domain} registration OR register ${year}`, "official_registration");
  }
  if (domain && needLodging) {
    push(
      `site:${domain} hotel OR lodging OR housing OR accommodations OR "room block" OR "stay-to-play"`,
      "official_lodging"
    );
    push(`site:${domain} travel OR "host hotel" OR shuttle`, "official_travel");
  }
  if (name && programName) {
    push(`"${programName}" "${name}" ${year} hotel OR housing OR lodging`, "named_program_lodging");
  }

  if (isSports) {
    if (domain) {
      push(`site:${domain} tournament schedule hotel OR "stay-to-play" ${year}`, "sports_stay");
    }
    push(`"${programName || name}" stay-to-play OR "team hotel" ${year}`, "sports_housing");
  } else if (isTraining) {
    if (domain) {
      push(
        `site:${domain} training calendar OR academy OR "in-person" hotel OR residential ${year}`,
        "training_calendar"
      );
    }
  } else if (isGov) {
    if (domain) {
      push(
        `site:${domain} (workshop OR symposium OR training OR kickoff) (hotel OR travel OR lodging) ${year}`,
        "gov_program_travel"
      );
    }
    // Explicitly do NOT query award-only as lodging
  } else if (isUniHealth) {
    if (domain) {
      push(
        `site:${domain} (symposium OR conference OR orientation) (hotel OR accommodations OR travel) ${year}`,
        "uni_health_travel"
      );
    }
  } else {
    // Association / conference default
    if (domain) {
      push(
        `site:${domain} "annual meeting" OR conference (hotel OR housing OR "room block") ${year}`,
        "assoc_hotel_block"
      );
    }
  }

  return queries.slice(0, 6);
}

const STRONG_LODGING_RE =
  /hotel\s*block|room\s*block|housing\s*(page|portal|bureau)|stay[- ]to[- ]play|recommended\s+hotels?|host\s+hotel|accommodation\s+instructions?|book\s+your\s+hotel|official\s+hotel|overnight\s+(travel|stay)|shuttle\s+from\s+(the\s+)?hotel/i;

const MODERATE_LODGING_RE =
  /multi[- ]day|out[- ]of[- ](?:town|state|market)|nonlocal|travell?ing\s+(teams?|attendees|participants)|residential\s+program|no\s+on[- ]site\s+(lodging|hotel)|nearby\s+hotels?|lodging|accommodations?|housing/i;

const TRAVEL_RE =
  /travel\s+information|getting\s+there|visitor\s+travel|fly\s+in|airport\s+shuttle|out[- ]of[- ]town/i;

const FUTURE_DATE_RE =
  /\b(20(?:2[6-9]|3[0-5]))\b|\b(?:january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{1,2},?\s+20(?:2[6-9]|3[0-5])\b/i;

const PAST_ONLY_HINT_RE =
  /\b(20(?:1[0-9]|2[0-4]))\b.*\b(conference|tournament|meeting)\b/i;

const LOCAL_WEAK_RE =
  /\b(one[- ]day|single[- ]day|local\s+meeting|lunch\s+meeting|virtual\s+only|webinar\s+only)\b/i;

/**
 * Score lodging / future evidence from page title+snippet+body text.
 */
export function extractLodgingEvidenceFromText({
  title = "",
  snippet = "",
  text = "",
  url = "",
} = {}) {
  const combined = [title, snippet, text, url].map(blob).join(" \n ");
  const findings = {
    futureCycle: false,
    lodging: false,
    travel: false,
    housing: false,
    market: false,
    actionPath: false,
    strength: LODGING_EVIDENCE_STRENGTH.INSUFFICIENT,
    matchedYears: [],
    notes: [],
  };

  const yearMatches = combined.match(/\b20(?:2[6-9]|3[0-5])\b/g) || [];
  findings.matchedYears = [...new Set(yearMatches)];
  if (FUTURE_DATE_RE.test(combined) || findings.matchedYears.length) {
    findings.futureCycle = true;
    findings.notes.push("future_year_or_date_mentioned");
  }

  if (STRONG_LODGING_RE.test(combined)) {
    findings.lodging = true;
    findings.housing = /housing|stay[- ]to[- ]play|room\s*block|hotel\s*block/i.test(
      combined
    );
    findings.strength = LODGING_EVIDENCE_STRENGTH.STRONG;
    findings.notes.push("strong_lodging_language");
  } else if (MODERATE_LODGING_RE.test(combined)) {
    findings.lodging = true;
    findings.strength = LODGING_EVIDENCE_STRENGTH.MODERATE;
    findings.notes.push("moderate_lodging_language");
  }

  if (TRAVEL_RE.test(combined)) {
    findings.travel = true;
    findings.notes.push("travel_language");
    if (findings.strength === LODGING_EVIDENCE_STRENGTH.INSUFFICIENT) {
      findings.strength = LODGING_EVIDENCE_STRENGTH.WEAK;
    }
  }

  if (/washington|bethesda|arlington|maryland|dmv|virginia|college park/i.test(combined)) {
    findings.market = true;
  }

  if (
    /contact|register|registration|sales|housing\s*bureau|book\s+now/i.test(combined)
  ) {
    findings.actionPath = true;
  }

  if (LOCAL_WEAK_RE.test(combined) && !findings.lodging) {
    findings.strength = LODGING_EVIDENCE_STRENGTH.INSUFFICIENT;
    findings.notes.push("local_or_single_day_weak");
  }

  if (
    !findings.futureCycle &&
    PAST_ONLY_HINT_RE.test(combined) &&
    !FUTURE_DATE_RE.test(combined)
  ) {
    findings.notes.push("past_cycle_hint");
  }

  // Prefer official domains
  try {
    const host = new URL(url).hostname.replace(/^www\./i, "");
    if (/\.gov$|\.edu$|\.org$/i.test(host)) findings.notes.push("officialish_tld");
  } catch {
    /* ignore */
  }

  return findings;
}

export function mergeEvidenceFindings(list = []) {
  const merged = {
    futureCycle: false,
    lodging: false,
    travel: false,
    housing: false,
    market: false,
    actionPath: false,
    strength: LODGING_EVIDENCE_STRENGTH.INSUFFICIENT,
    matchedYears: [],
    notes: [],
    sources: [],
  };
  const strengthRank = {
    [LODGING_EVIDENCE_STRENGTH.INSUFFICIENT]: 0,
    [LODGING_EVIDENCE_STRENGTH.WEAK]: 1,
    [LODGING_EVIDENCE_STRENGTH.MODERATE]: 2,
    [LODGING_EVIDENCE_STRENGTH.STRONG]: 3,
  };
  for (const f of list) {
    if (!f) continue;
    merged.futureCycle = merged.futureCycle || f.futureCycle;
    merged.lodging = merged.lodging || f.lodging;
    merged.travel = merged.travel || f.travel;
    merged.housing = merged.housing || f.housing;
    merged.market = merged.market || f.market;
    merged.actionPath = merged.actionPath || f.actionPath;
    if (strengthRank[f.strength] > strengthRank[merged.strength]) {
      merged.strength = f.strength;
    }
    merged.matchedYears.push(...(f.matchedYears || []));
    merged.notes.push(...(f.notes || []));
    if (f.url) merged.sources.push(f.url);
  }
  merged.matchedYears = [...new Set(merged.matchedYears)];
  merged.notes = [...new Set(merged.notes)];
  merged.sources = [...new Set(merged.sources)];
  return merged;
}

/**
 * Decide if evidence is enough to attempt TRUE qualification inputs.
 */
export function evidenceToQualificationHints(evidence = {}, target = {}) {
  const strength = evidence.strength || LODGING_EVIDENCE_STRENGTH.INSUFFICIENT;
  const lodgingOk =
    strength === LODGING_EVIDENCE_STRENGTH.STRONG ||
    (strength === LODGING_EVIDENCE_STRENGTH.MODERATE &&
      evidence.futureCycle &&
      (evidence.travel || evidence.housing || evidence.market));

  const futureOk = evidence.futureCycle === true;
  let timingStatus = "UNKNOWN";
  let eventStartDate = null;
  if (futureOk && evidence.matchedYears?.length) {
    timingStatus = "FUTURE_CONFIRMED";
    // Conservative: year-only → use mid-year placeholder ONLY as timingStatus flag,
    // not as invented precise date for promotion display. Prefer null date + FUTURE_CONFIRMED.
    eventStartDate = null;
  }

  return {
    lodgingDemandThesis: lodgingOk,
    hasCredibleLodgingDemand: lodgingOk,
    lodgingEvidenceStrength: strength,
    timingStatus: futureOk ? timingStatus : "UNKNOWN",
    eventStartDate,
    hotelDemandThesis: lodgingOk
      ? `Guided harvest found ${strength.toLowerCase()} overnight-demand evidence for ${clean(
          target.programName || target.organizationName
        )}`
      : `Insufficient overnight-demand evidence (${strength}) for ${clean(
          target.programName || target.organizationName
        )}`,
    recommendedAction: lodgingOk
      ? evidence.actionPath
        ? `Contact organizer / housing channel via official program pages for ${clean(
            target.programName || target.organizationName
          )}`
        : `Validate overnight-demand details on official source and open hotel sales outreach for ${clean(
            target.programName || target.organizationName
          )}`
      : null,
    commercialGeographyOk: true,
    sourceUrls: evidence.sources || [],
  };
}

/**
 * Classify vs existing GDI opportunities.
 */
export function classifyAgainstExistingGdi({
  generator = {},
  program = {},
  evidence = {},
  trueActionable = false,
  existingOpps = [],
} = {}) {
  if (!trueActionable) {
    if (evidence.futureCycle && !evidence.lodging) {
      return HARVEST_CLASSIFICATION.FUTURE_WATCH;
    }
    if (
      !evidence.futureCycle &&
      (program.recurrenceStatus === "RECURRING_CONFIRMED" ||
        program.recurrenceStatus === "RECURRING_HISTORICAL")
    ) {
      return HARVEST_CLASSIFICATION.FUTURE_WATCH;
    }
    return HARVEST_CLASSIFICATION.REJECT;
  }

  const orgKeys = [
    generator.organizationName,
    ...(generator.organizationAliases || []),
  ]
    .map((x) => blob(x))
    .filter(Boolean);
  const programKey = blob(program.programName);
  const years = new Set((evidence.matchedYears || []).map(String));

  let best = null;
  for (const opp of existingOpps) {
    const oOrg = blob(opp.organizationName);
    const oTitle = blob(opp.title);
    const orgHit = orgKeys.some((k) => oOrg.includes(k) || k.includes(oOrg) || oTitle.includes(k));
    if (!orgHit) continue;
    const programHit =
      !programKey ||
      oTitle.includes(programKey) ||
      blob(opp.eventName).includes(programKey);
    const oppYear = String(opp.eventStartDate || opp.title || "").match(
      /\b(20(?:2[6-9]|3[0-5]))\b/
    )?.[1];
    const yearHit = oppYear && years.has(oppYear);
    if (programHit && yearHit) {
      best = { kind: HARVEST_CLASSIFICATION.EXISTING_UPDATE, opportunityId: opp.id || opp.opportunityId };
      break;
    }
    if (programHit || orgHit) {
      best = best || {
        kind: HARVEST_CLASSIFICATION.GENERIC_DISCOVERY_OVERLAP,
        opportunityId: opp.id || opp.opportunityId,
      };
    }
  }

  if (best?.kind === HARVEST_CLASSIFICATION.EXISTING_UPDATE) return best.kind;
  if (best?.kind === HARVEST_CLASSIFICATION.GENERIC_DISCOVERY_OVERLAP) {
    // Same org/program already in GDI from generic discovery — not incremental NEW
    return HARVEST_CLASSIFICATION.GENERIC_DISCOVERY_OVERLAP;
  }
  return HARVEST_CLASSIFICATION.GENERATOR_INCREMENTAL_NEW;
}

export function isOfficialishUrl(url, officialDomain) {
  try {
    const host = new URL(url).hostname.replace(/^www\./i, "").toLowerCase();
    const dom = clean(officialDomain).toLowerCase();
    if (dom && (host === dom || host.endsWith(`.${dom}`))) return true;
    if (/\.gov$|\.edu$/i.test(host)) return true;
    return false;
  } catch {
    return false;
  }
}

export function rankHarvestTargets({ generators, programs, fits } = {}) {
  const progByGen = new Map();
  for (const p of programs || []) {
    const id = p.demandGeneratorId;
    if (!progByGen.has(id)) progByGen.set(id, []);
    progByGen.get(id).push(p);
  }
  const scored = (fits || []).map((fit) => {
    const gen = (generators || []).find(
      (g) => g.demandGeneratorId === fit.demandGeneratorId
    );
    const progs = progByGen.get(fit.demandGeneratorId) || [];
    const bestProg =
      progs.find((p) => p.recurrenceStatus === "RECURRING_CONFIRMED") ||
      progs[0] ||
      null;
    let score = 0;
    if (fit.generatorPriority === "HIGH") score += 50;
    else if (fit.generatorPriority === "MEDIUM") score += 25;
    if (gen?.generatorStatus === "ACTIVE_GENERATOR") score += 20;
    else if (gen?.generatorStatus === "VERIFIED") score += 10;
    if (bestProg?.recurrenceStatus === "RECURRING_CONFIRMED") score += 20;
    else if (bestProg?.recurrenceStatus === "RECURRING_HISTORICAL") score += 8;
    if (["CORE", "HIGH"].includes(String(fit.marketRelevance || "").toUpperCase())) {
      score += 15;
    } else if (
      ["COMPETITIVE", "MEDIUM"].includes(String(fit.marketRelevance || "").toUpperCase())
    ) {
      score += 8;
    }
    if (["HIGH", "STRONG"].includes(String(fit.travelDemandPotential || "").toUpperCase())) {
      score += 10;
    }
    if (gen?.officialDomain) score += 5;
    return { score, generator: gen, program: bestProg, fit };
  });
  return scored
    .filter((x) => x.generator)
    .sort((a, b) => b.score - a.score);
}
