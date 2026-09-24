/**
 * Native WHO V8 — entity type validation before named WHO confirmation.
 * Only PERSON may enter named WHO. FUNCTIONAL_CONTACT → functional fallback.
 */

export const ENTITY_TYPE = Object.freeze({
  PERSON: "PERSON",
  ORGANIZATION: "ORGANIZATION",
  DEPARTMENT: "DEPARTMENT",
  COMMITTEE: "COMMITTEE",
  PROGRAM: "PROGRAM",
  EVENT: "EVENT",
  ROLE_TITLE: "ROLE_TITLE",
  FUNCTIONAL_CONTACT: "FUNCTIONAL_CONTACT",
  UNKNOWN: "UNKNOWN",
});

/** Applied to entity NAME only — not to job titles. */
const ORG_NOUN_TOKEN =
  /^(association|council|foundation|committee|department|program|conference|tournament|league|team|center|centre|office|division|services|events|meetings|registration|housing|media|camp|areas|advisory|officer|international|important|worth|natural|social|chief|operating|management|institute|society|bureau|agency|commission|board|club|alliance|network|group|corp|inc|llc|ltd|skills|learning|certificate|essential|online|course|webinar|training|curriculum|offers|account|ticketing|policy|mobile|expo|resources|works|college|force|task|water|new|staff|membership|festival|forum|retreat|pipeline|series|challenge|classic|nationals|championship|championships)$/i;

const ROLE_TITLE_PHRASE =
  /^(chief\s+operating\s+officer|chief\s+executive\s+officer|executive\s+director|vice\s+president|co-?vice\s+president|president|treasurer|secretary|director|manager|coordinator|administrator|tournament\s+director|cups?\s+director)$/i;

const DEPT_CHANNEL =
  /^(social\s+media|public\s+relations|human\s+resources|member\s+services|business\s+development|customer\s+service|information\s+technology|water\s+resources|new\s+works|college\s+expo)$/i;

const COMMITTEE_PHRASE =
  /\b(advisory|committee|commission|task\s*force|working\s*group|board\s+of\s+directors)\b/i;

const EVENT_PHRASE =
  /\b(annual\s+meeting|summit|expo|symposium|invitational|showcase|championships?|conference\s+and\s+expo|college\s+expo|winter\s+classic|nationals)\b/i;

const FUNCTIONAL_LOCAL =
  /^(info|events|contact|hello|office|admin|support|sales|meetings|housing|registrar|general|inquiries)@/i;

/**
 * Classify an extracted entity name (not its job title).
 * @returns {{ type: string, reasons: string[], okForNamedWho: boolean, okForFunctional: boolean }}
 */
export function classifyEntityTypeV8(name = "", opts = {}) {
  const raw = String(name || "").trim();
  const reasons = [];
  if (!raw) {
    return {
      type: ENTITY_TYPE.UNKNOWN,
      reasons: ["empty"],
      okForNamedWho: false,
      okForFunctional: false,
    };
  }

  // Functional email-as-name
  if (/@/.test(raw) || FUNCTIONAL_LOCAL.test(raw)) {
    return {
      type: ENTITY_TYPE.FUNCTIONAL_CONTACT,
      reasons: ["email_or_alias"],
      okForNamedWho: false,
      okForFunctional: true,
    };
  }

  if (ROLE_TITLE_PHRASE.test(raw)) {
    return {
      type: ENTITY_TYPE.ROLE_TITLE,
      reasons: ["role_title_phrase"],
      okForNamedWho: false,
      okForFunctional: false,
    };
  }

  if (DEPT_CHANNEL.test(raw)) {
    return {
      type: ENTITY_TYPE.DEPARTMENT,
      reasons: ["department_channel"],
      okForNamedWho: false,
      okForFunctional: false,
    };
  }

  // Committee / task-force phrases are never person names (incl. Title Case "Task Force").
  if (COMMITTEE_PHRASE.test(raw)) {
    return {
      type: ENTITY_TYPE.COMMITTEE,
      reasons: ["committee_phrase"],
      okForNamedWho: false,
      okForFunctional: false,
    };
  }

  if (EVENT_PHRASE.test(raw)) {
    return {
      type: ENTITY_TYPE.EVENT,
      reasons: ["event_phrase"],
      okForNamedWho: false,
      okForFunctional: false,
    };
  }

  // "Natural Areas" / "Management Advisory" — committee fragment tokens
  const tokens = raw.split(/\s+/).filter(Boolean);
  if (tokens.length >= 2 && tokens.every((t) => ORG_NOUN_TOKEN.test(t) || /^[A-Z][a-z]+$/.test(t))) {
    const orgHits = tokens.filter((t) => ORG_NOUN_TOKEN.test(t));
    if (orgHits.length >= 1 && tokens.length <= 3 && orgHits.length === tokens.length) {
      return {
        type: ENTITY_TYPE.ORGANIZATION,
        reasons: ["all_org_noun_tokens"],
        okForNamedWho: false,
        okForFunctional: false,
      };
    }
    if (orgHits.length >= 1 && !looksLikeHumanName(raw)) {
      const type = COMMITTEE_PHRASE.test(raw)
        ? ENTITY_TYPE.COMMITTEE
        : EVENT_PHRASE.test(raw)
          ? ENTITY_TYPE.EVENT
          : ENTITY_TYPE.ORGANIZATION;
      return {
        type,
        reasons: ["org_noun_in_name", ...orgHits.map((t) => `token:${t}`)],
        okForNamedWho: false,
        okForFunctional: false,
      };
    }
  }

  // Any token that is a pure org noun as last/first of 2-word "name"
  if (tokens.length === 2 && (ORG_NOUN_TOKEN.test(tokens[0]) || ORG_NOUN_TOKEN.test(tokens[1]))) {
    // Allow rare surnames that collide only if both look like given+family (e.g. not "Social Media")
    if (ORG_NOUN_TOKEN.test(tokens[0]) || ORG_NOUN_TOKEN.test(tokens[1])) {
      reasons.push("org_noun_token");
      return {
        type: ENTITY_TYPE.ORGANIZATION,
        reasons,
        okForNamedWho: false,
        okForFunctional: false,
      };
    }
  }

  if (tokens.length >= 3 && tokens.filter((t) => ORG_NOUN_TOKEN.test(t)).length >= 2) {
    return {
      type: ENTITY_TYPE.ORGANIZATION,
      reasons: ["multi_org_noun"],
      okForNamedWho: false,
      okForFunctional: false,
    };
  }

  if (EVENT_PHRASE.test(raw) && !looksLikeHumanName(raw)) {
    return {
      type: ENTITY_TYPE.EVENT,
      reasons: ["event_phrase"],
      okForNamedWho: false,
      okForFunctional: false,
    };
  }

  // Trailing title bleed ("Jennifer Roberts Vice")
  if (/^(jr|sr|ii|iii|iv|vice|director|manager|president)$/i.test(tokens[tokens.length - 1])) {
    return {
      type: ENTITY_TYPE.UNKNOWN,
      reasons: ["trailing_title_token"],
      okForNamedWho: false,
      okForFunctional: false,
    };
  }

  // Truncated / non-name leftovers
  if (tokens.some((t) => t.length <= 3 && !/^(ann|amy|bob|sam|lee|jay|kim|max|zoe)$/i.test(t))) {
    if (tokens.some((t) => /^(cit|blvd|ave|ste|apt|ext)$/i.test(t))) {
      return {
        type: ENTITY_TYPE.UNKNOWN,
        reasons: ["truncated_or_address_token"],
        okForNamedWho: false,
        okForFunctional: false,
      };
    }
  }

  if (/^(birth|past|current|guest|east|west|north|south|card|format|inside)\b/i.test(raw)) {
    return {
      type: ENTITY_TYPE.UNKNOWN,
      reasons: ["non_person_lead_token"],
      okForNamedWho: false,
      okForFunctional: false,
    };
  }

  if (!looksLikeHumanName(raw)) {
    return {
      type: ENTITY_TYPE.UNKNOWN,
      reasons: ["not_human_name_shape", ...(opts.extraReasons || [])],
      okForNamedWho: false,
      okForFunctional: false,
    };
  }

  // Positive person shape
  const positive = [];
  if (/^[A-Z][a-z]+(?:\s+[A-Z]\.)?\s+[A-Z][a-z'.-]+$/.test(raw)) positive.push("first_last");
  if (opts.schemaPerson) positive.push("schema_person");
  if (opts.fromStaffCard) positive.push("staff_card");
  if (opts.hasPersonMailto) positive.push("person_mailto");
  if (opts.hasTiedRole) positive.push("tied_role");

  return {
    type: ENTITY_TYPE.PERSON,
    reasons: positive.length ? positive : ["human_name_shape"],
    okForNamedWho: true,
    okForFunctional: false,
  };
}

function looksLikeHumanName(n) {
  const s = String(n || "").trim();
  if (!s || s.length < 5 || s.length > 60) return false;
  if (/[@<>&#\d]/.test(s)) return false;
  if (!/^[A-Z][a-z]+(?:\s+[A-Z](?:\.|[a-z'.-]+)){1,3}$/.test(s)) return false;
  const parts = s.split(/\s+/);
  if (parts.some((p) => ORG_NOUN_TOKEN.test(p))) return false;
  if (ROLE_TITLE_PHRASE.test(s)) return false;
  return true;
}

/**
 * Gate a WHO candidate. Returns reject reason or null.
 */
export function personTypeGateV8(candidate = {}) {
  const classified = classifyEntityTypeV8(candidate.name, {
    schemaPerson: Boolean(candidate.fromStructuredData && /person/i.test(candidate.schemaType || "")),
    fromStaffCard: /STAFF|CONTACT|TOURNAMENT|LEADERSHIP|EVENT_TEAM/i.test(
      String(candidate.sectionKind || candidate.sectionHint || "")
    ),
    hasPersonMailto: Boolean(
      candidate.email && !FUNCTIONAL_LOCAL.test(String(candidate.email))
    ),
    hasTiedRole: Boolean(candidate.role && String(candidate.role).length >= 4),
  });

  if (!classified.okForNamedWho) {
    return {
      reject: true,
      gate: "ENTITY_TYPE_NOT_PERSON",
      entityType: classified.type,
      reasons: classified.reasons,
    };
  }
  return {
    reject: false,
    gate: null,
    entityType: ENTITY_TYPE.PERSON,
    reasons: classified.reasons,
  };
}

export function isOrgNounTokenV8(token) {
  return ORG_NOUN_TOKEN.test(String(token || ""));
}
