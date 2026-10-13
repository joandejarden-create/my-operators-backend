/**
 * Dealality-owned LinkedIn resolution for Market Alert stakeholders.
 * NEVER uses Surfe. Surfe LinkedIn URLs must not be persisted.
 *
 * Persistence policy:
 * - HIGH + CONFIRMED → persist URL with source DEALALITY_PUBLIC_RESEARCH
 * - MEDIUM → audit/report only
 * - LOW / AMBIGUOUS / NOT_FOUND → do not persist URL
 */

export const LINKEDIN_RESOLUTION_STATUS = Object.freeze({
  CONFIRMED: "CONFIRMED",
  AMBIGUOUS: "AMBIGUOUS",
  NOT_FOUND: "NOT_FOUND",
  NOT_ATTEMPTED: "NOT_ATTEMPTED",
});

export const LINKEDIN_RESOLUTION_CONFIDENCE = Object.freeze({
  HIGH: "HIGH",
  MEDIUM: "MEDIUM",
  LOW: "LOW",
});

export const LINKEDIN_RESOLUTION_SOURCE = Object.freeze({
  DEALALITY_PUBLIC_RESEARCH: "DEALALITY_PUBLIC_RESEARCH",
  SURFE: "SURFE", // never persist
});

/**
 * Score a candidate LinkedIn profile against known identity.
 * Does not fetch the network — caller supplies candidate evidence.
 *
 * @param {{ personName?: string, company?: string, role?: string, geography?: string, projectContext?: string }} identity
 * @param {{ url?: string, name?: string, company?: string, title?: string, geography?: string, snippet?: string, sourcePage?: string }} candidate
 */
export function scoreLinkedInCandidate(identity = {}, candidate = {}) {
  const url = String(candidate.url || "").trim();
  if (!/linkedin\.com\/in\//i.test(url)) {
    return {
      status: LINKEDIN_RESOLUTION_STATUS.NOT_FOUND,
      confidence: LINKEDIN_RESOLUTION_CONFIDENCE.LOW,
      matchCount: 0,
      matches: [],
      persistable: false,
    };
  }

  const matches = [];
  const nameOk = namesLikelyMatch(identity.personName, candidate.name || candidate.snippet);
  const companyOk = companyLikelyMatch(identity.company, candidate.company || candidate.snippet);
  const roleOk = roleLikelyMatch(identity.role, candidate.title || candidate.snippet);
  const geoOk = softIncludes(identity.geography, candidate.geography || candidate.snippet);
  const projectOk = softIncludes(identity.projectContext, candidate.snippet);
  const leadershipOk = /leadership|about|team|executive|founder|ceo/i.test(
    String(candidate.sourcePage || candidate.snippet || "")
  );

  if (nameOk) matches.push("exact_person_name");
  if (companyOk) matches.push("exact_company_match");
  if (roleOk) matches.push("role_title_match");
  if (geoOk) matches.push("geography_match");
  if (projectOk) matches.push("hotel_project_context_match");
  if (leadershipOk && companyOk) matches.push("company_leadership_corroboration");

  const matchCount = matches.length;
  let confidence = LINKEDIN_RESOLUTION_CONFIDENCE.LOW;
  let status = LINKEDIN_RESOLUTION_STATUS.NOT_FOUND;

  if (nameOk && companyOk && matchCount >= 2) {
    confidence = LINKEDIN_RESOLUTION_CONFIDENCE.HIGH;
    status = LINKEDIN_RESOLUTION_STATUS.CONFIRMED;
  } else if (nameOk && matchCount >= 2) {
    confidence = LINKEDIN_RESOLUTION_CONFIDENCE.MEDIUM;
    status = LINKEDIN_RESOLUTION_STATUS.AMBIGUOUS;
  } else if (nameOk || companyOk) {
    confidence = LINKEDIN_RESOLUTION_CONFIDENCE.LOW;
    status = LINKEDIN_RESOLUTION_STATUS.AMBIGUOUS;
  }

  return {
    status,
    confidence,
    matchCount,
    matches,
    persistable:
      status === LINKEDIN_RESOLUTION_STATUS.CONFIRMED &&
      confidence === LINKEDIN_RESOLUTION_CONFIDENCE.HIGH,
    url: status === LINKEDIN_RESOLUTION_STATUS.CONFIRMED ? url : null,
    source: LINKEDIN_RESOLUTION_SOURCE.DEALALITY_PUBLIC_RESEARCH,
  };
}

/**
 * Resolve LinkedIn for a stakeholder using caller-provided public candidates.
 * Network search is intentionally not wired here yet — audit/backfill may pass
 * candidates from public search. Default: NOT_ATTEMPTED.
 */
export async function resolveLinkedInProfile(identity = {}, opts = {}) {
  const candidates = Array.isArray(opts.candidates) ? opts.candidates : [];
  if (!candidates.length) {
    return {
      linkedinUrl: null,
      linkedinResolutionStatus: LINKEDIN_RESOLUTION_STATUS.NOT_ATTEMPTED,
      linkedinResolutionConfidence: null,
      linkedinResolutionSource: null,
      scored: [],
    };
  }

  const scored = candidates.map((c) => ({
    ...c,
    score: scoreLinkedInCandidate(identity, c),
  }));
  const persistable = scored.filter((s) => s.score.persistable);
  if (persistable.length === 1) {
    const best = persistable[0];
    return {
      linkedinUrl: best.score.url,
      linkedinResolutionStatus: LINKEDIN_RESOLUTION_STATUS.CONFIRMED,
      linkedinResolutionConfidence: LINKEDIN_RESOLUTION_CONFIDENCE.HIGH,
      linkedinResolutionSource: LINKEDIN_RESOLUTION_SOURCE.DEALALITY_PUBLIC_RESEARCH,
      scored,
    };
  }
  if (persistable.length > 1 || scored.some((s) => s.score.status === LINKEDIN_RESOLUTION_STATUS.AMBIGUOUS)) {
    return {
      linkedinUrl: null,
      linkedinResolutionStatus: LINKEDIN_RESOLUTION_STATUS.AMBIGUOUS,
      linkedinResolutionConfidence: LINKEDIN_RESOLUTION_CONFIDENCE.MEDIUM,
      linkedinResolutionSource: null,
      scored,
    };
  }
  return {
    linkedinUrl: null,
    linkedinResolutionStatus: LINKEDIN_RESOLUTION_STATUS.NOT_FOUND,
    linkedinResolutionConfidence: LINKEDIN_RESOLUTION_CONFIDENCE.LOW,
    linkedinResolutionSource: null,
    scored,
  };
}

/** Only persist LinkedIn when Dealality research confirmed HIGH. */
export function linkedInFieldsForPersistence(resolution = {}) {
  if (
    resolution.linkedinResolutionStatus === LINKEDIN_RESOLUTION_STATUS.CONFIRMED &&
    resolution.linkedinResolutionConfidence === LINKEDIN_RESOLUTION_CONFIDENCE.HIGH &&
    resolution.linkedinResolutionSource === LINKEDIN_RESOLUTION_SOURCE.DEALALITY_PUBLIC_RESEARCH &&
    resolution.linkedinUrl
  ) {
    return {
      linkedinUrl: resolution.linkedinUrl,
      linkedinResolutionStatus: resolution.linkedinResolutionStatus,
      linkedinResolutionConfidence: resolution.linkedinResolutionConfidence,
      linkedinResolutionSource: resolution.linkedinResolutionSource,
    };
  }
  return {
    linkedinUrl: null,
    linkedinResolutionStatus:
      resolution.linkedinResolutionStatus || LINKEDIN_RESOLUTION_STATUS.NOT_ATTEMPTED,
    linkedinResolutionConfidence: resolution.linkedinResolutionConfidence || null,
    linkedinResolutionSource: null,
  };
}

function norm(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function namesLikelyMatch(expected, hay) {
  const a = norm(expected);
  const b = norm(hay);
  if (!a || !b) return false;
  if (b.includes(a)) return true;
  const parts = a.split(" ").filter((p) => p.length > 1);
  if (parts.length < 2) return false;
  return parts.every((p) => b.includes(p));
}

function companyLikelyMatch(expected, hay) {
  const a = norm(expected);
  const b = norm(hay);
  if (!a || !b) return false;
  if (b.includes(a) || a.includes(b)) return true;
  const tokens = a.split(" ").filter((t) => t.length > 2 && !/^(llc|ltd|inc|the|and|of|group)$/.test(t));
  if (tokens.length < 1) return false;
  return tokens.filter((t) => b.includes(t)).length >= Math.min(2, tokens.length);
}

function roleLikelyMatch(expected, hay) {
  const a = norm(expected);
  const b = norm(hay);
  if (!a || !b) return false;
  if (b.includes(a)) return true;
  const aliases = [
    ["ceo", "chief executive"],
    ["founder", "co founder"],
    ["president", "president"],
    ["managing director", "md"],
  ];
  for (const [x, y] of aliases) {
    if ((a.includes(x) || a.includes(y)) && (b.includes(x) || b.includes(y))) return true;
  }
  return false;
}

function softIncludes(needle, hay) {
  const a = norm(needle);
  const b = norm(hay);
  if (!a || !b || a.length < 3) return false;
  return b.includes(a);
}
