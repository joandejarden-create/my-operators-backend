/**
 * Native WHO V9 — entity type gate (LOCATION + truncation + LATAM + positive personness).
 * Entity type runs BEFORE role scoring. No benchmark-name blacklists.
 */

export const ENTITY_TYPE = Object.freeze({
  PERSON: "PERSON",
  ORGANIZATION: "ORGANIZATION",
  DEPARTMENT: "DEPARTMENT",
  COMMITTEE: "COMMITTEE",
  PROGRAM: "PROGRAM",
  EVENT: "EVENT",
  LOCATION: "LOCATION",
  ROLE_TITLE: "ROLE_TITLE",
  FUNCTIONAL_CONTACT: "FUNCTIONAL_CONTACT",
  UNKNOWN: "UNKNOWN",
});

/** Applied to entity NAME tokens only — not job titles. */
const INSTITUTIONAL_TOKEN =
  /^(association|council|foundation|committee|department|program|conference|tournament|league|team|center|centre|office|division|services|events|meetings|registration|housing|media|camp|areas|advisory|officer|international|important|worth|natural|social|chief|operating|management|institute|society|bureau|agency|commission|board|club|alliance|network|group|corp|inc|llc|ltd|skills|learning|certificate|essential|online|course|webinar|training|curriculum|offers|account|ticketing|policy|mobile|expo|resources|works|college|force|task|water|new|staff|membership|festival|forum|retreat|pipeline|series|challenge|classic|nationals|championship|championships|study|affairs|trade|industry|global|world|campus|valley|beach|resort|city|region|headquarters|convention|member|latam|francophone|anglophone|hispanophone|asuntos|eventos|convenciones|ventas|relaciones|desarrollo|gerencia|experience|experiences)$/i;

const LOCATION_TOKEN =
  /^(campus|valley|beach|resort|city|region|headquarters|hq|office|center|centre|convention|plaza|park|avenue|street|boulevard|hotel|property|edificio|sede)$/i;

const LOCATION_PHRASE =
  /\b(campus|convention\s+center|headquarters|valley|beach\s+resort|santa\s+fe\s+campus|world\s+trade)\b/i;

const ROLE_TITLE_PHRASE =
  /^(chief\s+operating\s+officer|chief\s+executive\s+officer|executive\s+director|vice\s+president|co-?vice\s+president|president|treasurer|secretary|director|directora|gerente|manager|coordinator|coordinador|coordinadora|administrator|tournament\s+director|cups?\s+director|asuntos\s+industriales)$/i;

const DEPT_CHANNEL =
  /^(social\s+media|public\s+relations|human\s+resources|member\s+services|business\s+development|customer\s+service|information\s+technology|water\s+resources|new\s+works|college\s+expo|industry\s+affairs|global\s+study|world\s+trade)$/i;

const COMMITTEE_PHRASE =
  /\b(advisory|committee|commission|task\s*force|working\s*group|board\s+of\s+directors)\b/i;

const EVENT_PHRASE =
  /\b(annual\s+meeting|summit|expo|symposium|invitational|showcase|championships?|conference\s+and\s+expo|college\s+expo|winter\s+classic|nationals|cup\s+match)\b/i;

const PROGRAM_PHRASE =
  /\b(global\s+study|study\s+abroad|fellowship\s+program|certificate\s+program)\b/i;

const FUNCTIONAL_LOCAL =
  /^(info|events|contact|hello|office|admin|support|sales|meetings|housing|registrar|general|inquiries)@/i;

/** Particles allowed in Hispanic / Romance names — not institutional. */
const NAME_PARTICLE =
  /^(de|del|de\s+la|da|das|dos|van|von|la|le|di|du)$/i;

/** Known institutional stems — truncated prefixes of these are never surnames. */
const INSTITUTIONAL_STEMS = [
  "tournament",
  "tournaments",
  "africa",
  "african",
  "council",
  "international",
  "department",
  "association",
  "organization",
  "organisation",
  "conference",
  "championship",
  "championships",
  "registration",
  "affairs",
  "services",
  "development",
  "operations",
  "engagement",
  "francophone",
  "anglophone",
];

const SPANISH_ROLE_WORDS =
  /^(director|directora|gerente|coordinador|coordinadora|eventos|convenciones|ventas|relaciones|asuntos|desarrollo|presidente|presidenta)$/i;

/**
 * Truncation: final token is a proper prefix of an institutional stem.
 */
export function isTruncatedInstitutionalToken(token) {
  const t = String(token || "")
    .toLowerCase()
    .replace(/[^a-zà-ÿ]/gi, "");
  if (t.length < 4 || t.length > 12) return false;
  return INSTITUTIONAL_STEMS.some(
    (stem) => stem.startsWith(t) && t.length < stem.length && stem.length - t.length >= 2
  );
}

export function detectTruncation(name = "") {
  const tokens = String(name || "")
    .trim()
    .split(/\s+/)
    .filter(Boolean);
  if (!tokens.length) return { truncated: false, reasons: [] };
  const reasons = [];
  const last = tokens[tokens.length - 1];
  if (isTruncatedInstitutionalToken(last)) {
    reasons.push(`truncated_stem:${last}`);
  }
  // Mid-word cut: ends with incomplete consonant cluster typical of clipping
  if (/^[A-Z][a-z]{3,6}$/.test(last) && /(?:trn|frn|ntl|mnt|str|phr)$/i.test(last)) {
    reasons.push(`truncated_cluster:${last}`);
  }
  // Trailing hyphen / ellipsis / mid-word dash
  if (/[-–—…]$/.test(last) || /\.\.\.$/.test(last)) {
    reasons.push("truncated_punctuation");
  }
  return { truncated: reasons.length > 0, reasons };
}

/**
 * LATAM / multilingual human name shape (positive).
 */
export function looksLikeHumanNameV9(n) {
  const s = String(n || "").trim();
  if (!s || s.length < 5 || s.length > 70) return false;
  if (/[@<>&#\d]/.test(s)) return false;

  // Normalize particles for token checks
  const rawTokens = s.split(/\s+/).filter(Boolean);
  if (rawTokens.length < 2 || rawTokens.length > 5) return false;

  // Allow accented initials and particles
  const nameToken =
    /^[A-ZÀ-Ý](?:\.|[a-zà-ÿ''’\u00AD]+(?:-[A-ZÀ-Ý][a-zà-ÿ''’\u00AD]+)?)$/u;
  const particleOk = (t) => NAME_PARTICLE.test(t);

  let contentTokens = 0;
  for (const t of rawTokens) {
    if (particleOk(t)) continue;
    if (!nameToken.test(t) && !/^[A-ZÀ-Ý]\.$/u.test(t)) return false;
    if (INSTITUTIONAL_TOKEN.test(t) || LOCATION_TOKEN.test(t)) return false;
    if (SPANISH_ROLE_WORDS.test(t) && rawTokens.length <= 2) return false;
    if (isTruncatedInstitutionalToken(t)) return false;
    contentTokens += 1;
  }
  if (contentTokens < 2) return false;
  if (ROLE_TITLE_PHRASE.test(s)) return false;
  if (detectTruncation(s).truncated) return false;
  return true;
}

/**
 * Classify entity name (not its job title).
 */
export function classifyEntityTypeV9(name = "", opts = {}) {
  const raw = String(name || "").trim();
  const personEvidence = [];
  const nonPersonEvidence = [];

  const out = (type, extra = {}) => ({
    type,
    reasons: extra.reasons || [],
    okForNamedWho: type === ENTITY_TYPE.PERSON,
    okForFunctional: type === ENTITY_TYPE.FUNCTIONAL_CONTACT,
    entityTypeConfidence: extra.confidence ?? (type === ENTITY_TYPE.PERSON ? 0.7 : 0.85),
    personEvidence: extra.personEvidence || personEvidence,
    nonPersonEvidence: extra.nonPersonEvidence || nonPersonEvidence,
  });

  if (!raw) {
    return out(ENTITY_TYPE.UNKNOWN, { reasons: ["empty"], confidence: 1 });
  }

  if (/@/.test(raw) || FUNCTIONAL_LOCAL.test(raw)) {
    nonPersonEvidence.push("email_or_alias");
    return out(ENTITY_TYPE.FUNCTIONAL_CONTACT, {
      reasons: ["email_or_alias"],
      nonPersonEvidence,
      confidence: 0.95,
    });
  }

  const trunc = detectTruncation(raw);
  if (trunc.truncated) {
    nonPersonEvidence.push(...trunc.reasons);
    return out(ENTITY_TYPE.UNKNOWN, {
      reasons: ["truncated_label", ...trunc.reasons],
      nonPersonEvidence,
      confidence: 0.95,
    });
  }

  if (ROLE_TITLE_PHRASE.test(raw) || SPANISH_ROLE_WORDS.test(raw)) {
    nonPersonEvidence.push("role_title_phrase");
    return out(ENTITY_TYPE.ROLE_TITLE, {
      reasons: ["role_title_phrase"],
      nonPersonEvidence,
      confidence: 0.95,
    });
  }

  if (DEPT_CHANNEL.test(raw)) {
    nonPersonEvidence.push("department_channel");
    return out(ENTITY_TYPE.DEPARTMENT, {
      reasons: ["department_channel"],
      nonPersonEvidence,
      confidence: 0.95,
    });
  }

  if (COMMITTEE_PHRASE.test(raw)) {
    nonPersonEvidence.push("committee_phrase");
    return out(ENTITY_TYPE.COMMITTEE, {
      reasons: ["committee_phrase"],
      nonPersonEvidence,
      confidence: 0.95,
    });
  }

  if (PROGRAM_PHRASE.test(raw)) {
    nonPersonEvidence.push("program_phrase");
    return out(ENTITY_TYPE.PROGRAM, {
      reasons: ["program_phrase"],
      nonPersonEvidence,
      confidence: 0.9,
    });
  }

  if (EVENT_PHRASE.test(raw)) {
    nonPersonEvidence.push("event_phrase");
    return out(ENTITY_TYPE.EVENT, {
      reasons: ["event_phrase"],
      nonPersonEvidence,
      confidence: 0.9,
    });
  }

  if (LOCATION_PHRASE.test(raw)) {
    nonPersonEvidence.push("location_phrase");
    return out(ENTITY_TYPE.LOCATION, {
      reasons: ["location_phrase"],
      nonPersonEvidence,
      confidence: 0.9,
    });
  }

  const tokens = raw.split(/\s+/).filter(Boolean);
  const instHits = tokens.filter((t) => INSTITUTIONAL_TOKEN.test(t));
  const locHits = tokens.filter((t) => LOCATION_TOKEN.test(t));

  // All-caps institutional label
  if (tokens.length >= 2 && tokens.every((t) => /^[A-Z]{2,}$/.test(t))) {
    nonPersonEvidence.push("all_caps_label");
    return out(ENTITY_TYPE.ORGANIZATION, {
      reasons: ["all_caps_institutional"],
      nonPersonEvidence,
      confidence: 0.9,
    });
  }

  if (locHits.length >= 1 && tokens.length <= 4) {
    nonPersonEvidence.push(...locHits.map((t) => `location_token:${t}`));
    return out(ENTITY_TYPE.LOCATION, {
      reasons: ["location_token"],
      nonPersonEvidence,
      confidence: 0.9,
    });
  }

  if (instHits.length >= 1 && tokens.length <= 3) {
    // Strong rejection: any institutional token in short Title-Case phrase
    const type =
      /affairs|department|services|asuntos/i.test(raw)
        ? ENTITY_TYPE.DEPARTMENT
        : /study|program|curriculum/i.test(raw)
          ? ENTITY_TYPE.PROGRAM
          : /conference|summit|expo|tournament|championship/i.test(raw)
            ? ENTITY_TYPE.EVENT
            : ENTITY_TYPE.ORGANIZATION;
    nonPersonEvidence.push(...instHits.map((t) => `institutional_token:${t}`));
    return out(type, {
      reasons: ["institutional_token_in_name"],
      nonPersonEvidence,
      confidence: 0.92,
    });
  }

  if (instHits.length >= 2) {
    nonPersonEvidence.push("multi_institutional");
    return out(ENTITY_TYPE.ORGANIZATION, {
      reasons: ["multi_institutional"],
      nonPersonEvidence,
      confidence: 0.9,
    });
  }

  // Trailing title bleed
  if (
    /^(jr|sr|ii|iii|iv|vice|director|directora|manager|gerente|president|presidente)$/i.test(
      tokens[tokens.length - 1]
    )
  ) {
    nonPersonEvidence.push("trailing_title_token");
    return out(ENTITY_TYPE.UNKNOWN, {
      reasons: ["trailing_title_token"],
      nonPersonEvidence,
      confidence: 0.85,
    });
  }

  if (!looksLikeHumanNameV9(raw)) {
    nonPersonEvidence.push("not_human_name_shape");
    return out(ENTITY_TYPE.UNKNOWN, {
      reasons: ["not_human_name_shape", ...(opts.extraReasons || [])],
      nonPersonEvidence,
      confidence: 0.8,
    });
  }

  // Positive person evidence (required beyond capitalization)
  if (looksLikeHumanNameV9(raw)) personEvidence.push("human_name_shape");
  if (opts.schemaPerson) personEvidence.push("schema_person");
  if (opts.fromStaffCard) personEvidence.push("staff_card");
  if (opts.hasPersonMailto) personEvidence.push("person_mailto");
  if (opts.hasPersonPhone) personEvidence.push("person_phone");
  if (opts.hasTiedRole) personEvidence.push("tied_role");
  if (opts.profileUrl) personEvidence.push("profile_url");

  const hasShape = personEvidence.includes("human_name_shape");
  const hasCorroboration = personEvidence.some((e) =>
    [
      "schema_person",
      "staff_card",
      "person_mailto",
      "person_phone",
      "profile_url",
      "tied_role",
    ].includes(e)
  );

  if (!hasShape) {
    nonPersonEvidence.push("missing_human_name_shape");
    return out(ENTITY_TYPE.UNKNOWN, {
      reasons: ["insufficient_person_evidence"],
      personEvidence,
      nonPersonEvidence,
      confidence: 0.55,
    });
  }

  if (!hasCorroboration && !opts.allowShapeAlone) {
    return out(ENTITY_TYPE.UNKNOWN, {
      reasons: ["insufficient_person_evidence"],
      personEvidence,
      nonPersonEvidence,
      confidence: 0.55,
    });
  }

  const strong = personEvidence.some((e) =>
    ["schema_person", "staff_card", "person_mailto", "profile_url"].includes(e)
  );
  const confidence = strong ? 0.92 : hasCorroboration ? 0.82 : 0.7;
  return out(ENTITY_TYPE.PERSON, {
    reasons: personEvidence,
    personEvidence,
    confidence,
  });
}

/**
 * Gate a WHO candidate. Strong non-person evidence always wins.
 */
export function personTypeGateV9(candidate = {}) {
  const classified = classifyEntityTypeV9(candidate.name, {
    schemaPerson: Boolean(
      candidate.fromStructuredData && /person/i.test(candidate.schemaType || "")
    ),
    fromStaffCard: /STAFF|CONTACT|TOURNAMENT|LEADERSHIP|EVENT_TEAM|DIRECTORY/i.test(
      String(candidate.sectionKind || candidate.sectionHint || "")
    ),
    hasPersonMailto: Boolean(
      candidate.email && !FUNCTIONAL_LOCAL.test(String(candidate.email))
    ),
    hasPersonPhone: Boolean(candidate.phone),
    hasTiedRole: Boolean(candidate.role && String(candidate.role).trim().length >= 3),
    profileUrl: Boolean(
      candidate.profileUrl ||
        (/\/(people|staff|bio|profile|team)\//i.test(String(candidate.sourceUrl || "")))
    ),
    allowShapeAlone: Boolean(candidate.allowShapeAlone),
  });

  if (classified.type !== ENTITY_TYPE.PERSON || !classified.okForNamedWho) {
    return {
      reject: true,
      gate:
        classified.type === ENTITY_TYPE.UNKNOWN
          ? "ENTITY_TYPE_UNKNOWN"
          : "ENTITY_TYPE_NOT_PERSON",
      entityType: classified.type,
      entityTypeConfidence: classified.entityTypeConfidence,
      reasons: classified.reasons,
      personEvidence: classified.personEvidence,
      nonPersonEvidence: classified.nonPersonEvidence,
    };
  }

  return {
    reject: false,
    gate: null,
    entityType: ENTITY_TYPE.PERSON,
    entityTypeConfidence: classified.entityTypeConfidence,
    reasons: classified.reasons,
    personEvidence: classified.personEvidence,
    nonPersonEvidence: classified.nonPersonEvidence,
  };
}

export function isInstitutionalTokenV9(token) {
  return INSTITUTIONAL_TOKEN.test(String(token || ""));
}
