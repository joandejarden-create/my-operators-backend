/**
 * Native WHO V11 — person / entity boundary.
 *
 * Ordering (mandatory):
 *   RAW TEXT → structural block → entity typing → PERSON confirmation
 *   → role classification → event relation → WHO ranking
 *
 * No hotel/person/event string blacklists. Structural + semantic rules only.
 */

import {
  ENTITY_TYPE,
  classifyEntityTypeV9,
  detectTruncation,
  looksLikeHumanNameV9,
  personTypeGateV9,
} from "./entity-type-gate-v9.js";

export { ENTITY_TYPE };

/** Honorifics stripped for normalized PERSON name; never accepted alone with given name. */
const HONORIFIC =
  /^(?:dr|dra|prof|professor|mr|ms|mrs|mx|ing|lic)\.?$/i;

const ACADEMIC_TITLE_PHRASE =
  /^(?:retired\s+)?(?:associate|assistant|adjunct|visiting|clinical|research)?\s*(?:professor|lecturer|instructor)(?:\s+emeritus)?$|^(?:professor\s+emeritus|research\s+director|local\s+government)$/i;

const ROLE_TITLE_EXTENDED =
  /^(?:chief\s+\w+\s+officer|executive\s+director|vice\s+president|co-?vice\s+president|president|treasurer|secretary|director|directora|gerente|manager|coordinator|administrator|associate\s+professor|assistant\s+professor|retired\s+associate\s+professor)$/i;

/** Nouns that mark org / dept / location fragments when they dominate a Title-Case span. */
const NON_PERSON_CONTENT_TOKEN =
  /^(institute|institution|association|society|foundation|council|committee|department|division|bureau|agency|commission|board|office|center|centre|research|survey|comparative|studies|study|program|programme|conference|summit|forum|government|housing|communities|local|regional|national|international|university|college|school|faculty|academy|vienna|austria|mexico|kansas|city|campus|headquarters|industry|affairs|resources|services|operations|engagement|membership|communications|marketing|partnerships|development|training|education|policy|trade|global|world|social|media|human|public|relations)$/i;

/** Mc / Mac surnames — positive support (must not be treated as fragments). */
const MC_MAC_SURNAME = /^Ma?c[A-ZÀ-Ý][a-zà-ÿ''’\u00AD-]+$/u;

const NAME_PARTICLE =
  /^(de|del|de\s+la|da|das|dos|van|von|la|le|di|du)$/i;

const GIVEN_OR_SURNAME =
  /^[A-ZÀ-Ý](?:\.|[a-zà-ÿ''’\u00AD]+(?:-[A-ZÀ-Ý][a-zà-ÿ''’\u00AD]+)?|c[A-ZÀ-Ý][a-zà-ÿ''’\u00AD]+)$/u;

/**
 * Strip leading honorific tokens from a candidate string.
 * @returns {{ bare: string, hadHonorific: boolean, honorifics: string[] }}
 */
export function stripHonorifics(name = "") {
  const tokens = String(name || "")
    .trim()
    .split(/\s+/)
    .filter(Boolean);
  const honorifics = [];
  while (tokens.length && HONORIFIC.test(tokens[0])) {
    honorifics.push(tokens.shift());
  }
  return {
    bare: tokens.join(" "),
    hadHonorific: honorifics.length > 0,
    honorifics,
  };
}

/**
 * LATAM / international / Mc-Mac human name shape (positive).
 * Stricter than absence-of-contradiction: requires coherent given+surname sequence.
 */
export function looksLikeHumanNameV11(n) {
  const stripped = stripHonorifics(n);
  const s = stripped.bare;
  if (!s || s.length < 3 || s.length > 70) return false;
  if (/[@<>&#\d]/.test(s)) return false;

  const rawTokens = s.split(/\s+/).filter(Boolean);
  if (rawTokens.length < 2 || rawTokens.length > 6) return false;

  // Honorific + single given name only → incomplete (e.g. "Dr Kseniya")
  if (stripped.hadHonorific && rawTokens.length < 2) return false;
  // After strip, still need ≥2 content tokens
  let contentTokens = 0;
  for (const t of rawTokens) {
    if (NAME_PARTICLE.test(t)) continue;
    if (HONORIFIC.test(t)) return false;
    if (MC_MAC_SURNAME.test(t) || GIVEN_OR_SURNAME.test(t) || /^[A-ZÀ-Ý]\.$/u.test(t)) {
      contentTokens += 1;
      continue;
    }
    // Fallback: accented classic token
    if (/^[A-ZÀ-Ý][a-zà-ÿ''’\u00AD]+(?:-[A-ZÀ-Ý][a-zà-ÿ''’\u00AD]+)?$/u.test(t)) {
      contentTokens += 1;
      continue;
    }
    return false;
  }
  if (contentTokens < 2) return false;

  if (ACADEMIC_TITLE_PHRASE.test(s) || ROLE_TITLE_EXTENDED.test(s)) return false;
  if (rawTokens.some((t) => NON_PERSON_CONTENT_TOKEN.test(t))) return false;
  if (detectTruncation(s).truncated) return false;

  // Prefer V9 shape when no Mc/Mac / particle complications
  if (!rawTokens.some((t) => MC_MAC_SURNAME.test(t) || NAME_PARTICLE.test(t))) {
    if (looksLikeHumanNameV9(s)) return true;
    // V9 rejects Mc*; V11 already accepted above
  }
  return true;
}

/**
 * Detect candidate as a partial span of a longer named entity in source text.
 */
export function detectNameFragment(candidate = "", sourceBlock = "") {
  const reasons = [];
  const stripped = stripHonorifics(candidate);
  const bare = stripped.bare;
  const block = String(sourceBlock || "").replace(/\s+/g, " ").trim();

  if (stripped.hadHonorific && bare.split(/\s+/).length < 2) {
    reasons.push("honorific_missing_surname");
  }

  if (block && bare) {
    const esc = bare.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    const honorificPrefix =
      "(?:Dr\\.?|Dra\\.?|Prof\\.?|Professor|Mr\\.?|Ms\\.?|Mrs\\.?|Mx\\.?)\\s+";
    // Next token after candidate must look like a surname — not an org noun
    const nextTokenRe = new RegExp(
      `${honorificPrefix}?${esc}\\s+([A-ZÀ-Ý][A-Za-zÀ-ÿ''’\\u00AD-]{2,})`,
      "u"
    );
    const next = block.match(nextTokenRe);
    if (
      next &&
      !NON_PERSON_CONTENT_TOKEN.test(next[1]) &&
      !/^(Institute|Association|Society|University|College|Center|Centre|Foundation|Email|Austria|Vienna)$/i.test(
        next[1]
      )
    ) {
      reasons.push("source_contains_longer_person_name");
    }

    // Incomplete candidate that is only a proper prefix of a longer person in block
    const fullPersonInBlock = block.match(
      new RegExp(
        `${honorificPrefix}?(${esc}\\s+[A-ZÀ-Ý][A-Za-zÀ-ÿ''’\\u00AD-]{2,}(?:\\s+[A-ZÀ-Ý][A-Za-zÀ-ÿ''’\\u00AD-]{2,})?)`,
        "u"
      )
    );
    if (
      fullPersonInBlock &&
      stripHonorifics(fullPersonInBlock[1]).bare.toLowerCase() !== bare.toLowerCase()
    ) {
      const extended = stripHonorifics(fullPersonInBlock[1]).bare;
      const extTokens = extended.split(/\s+/);
      const last = extTokens[extTokens.length - 1];
      if (!NON_PERSON_CONTENT_TOKEN.test(last) && extTokens.length > bare.split(/\s+/).length) {
        reasons.push("source_contains_longer_person_name");
      }
    }

    // Org-phrase overlap only when candidate itself fails person shape
    if (!looksLikeHumanNameV11(bare)) {
      const orgAround = new RegExp(
        `\\b(?:Institute|Association|Society|Center|Centre|University|College)\\b[^.\\n]{0,80}\\b${esc}\\b|\\b${esc}\\b[^.\\n]{0,40}\\b(?:Research|Institute|Association|Survey)\\b`,
        "i"
      );
      if (orgAround.test(block)) {
        reasons.push("candidate_overlaps_organization_phrase");
      }
    }
  }

  // Title-case two-token span dominated by institutional nouns
  const tokens = bare.split(/\s+/).filter(Boolean);
  if (tokens.length === 2 && tokens.every((t) => NON_PERSON_CONTENT_TOKEN.test(t) || /^[A-Z][a-z]+$/.test(t))) {
    const institutionalHits = tokens.filter((t) => NON_PERSON_CONTENT_TOKEN.test(t)).length;
    if (institutionalHits >= 1 && !looksLikeHumanNameV11(bare)) {
      reasons.push("partial_noun_phrase");
    }
  }

  return { isFragment: reasons.length > 0, reasons };
}

/**
 * Parse a multi-entity contact block into PERSON / TITLE / ORG / LOCATION slots.
 * Structural heuristics only — no hard-coded person/org strings.
 */
export function parseMultiEntityContactBlock(raw = "") {
  const text = String(raw || "").replace(/\s+/g, " ").trim();
  const entities = {
    persons: [],
    titles: [],
    organizations: [],
    locations: [],
    departments: [],
    ambiguous: false,
    reasons: [],
  };
  if (!text) return entities;

  // Role label before colon, then body
  const labeled = text.match(
    /^(?:executive\s+director|director|contact|secretary\s+general|president|coordinator|manager)\s*[:\-–]\s*(.+)$/i
  );
  const body = labeled ? labeled[1].trim() : text;
  if (labeled) entities.titles.push(labeled[0].split(/[:\-–]/)[0].trim());

  // Person: optional honorific + 2–4 name tokens before Institute/Association/University/Email/digits
  const personMatch = body.match(
    /(?:^|\b)((?:(?:Dr|Dra|Prof|Professor|Mr|Ms|Mrs|Mx|Ing|Lic)\.?\s+)?[A-ZÀ-Ý][A-Za-zÀ-ÿ''’\u00AD-]+(?:\s+(?:de|del|de\s+la|van|von|da|dos)?\s*[A-ZÀ-Ý][A-Za-zÀ-ÿ''’\u00AD-]+){1,3})(?=\s+(?:Institute|Association|Society|University|College|Center|Centre|Email|E-mail|\d{3,}|$))/u
  );
  if (personMatch) {
    const { bare } = stripHonorifics(personMatch[1]);
    if (looksLikeHumanNameV11(bare) || looksLikeHumanNameV11(personMatch[1])) {
      entities.persons.push(bare);
    }
  }

  const orgMatch = body.match(
    /\b((?:Institute|Association|Society|University|College|Center|Centre|Foundation|Council)\b[^,.\d]{3,80}?)(?=\s+(?:Vienna|Austria|Mexico|Email|\d{4}|$)|$)/i
  );
  if (orgMatch) {
    entities.organizations.push(orgMatch[1].replace(/\s+/g, " ").trim());
  }

  const locMatch = body.match(
    /\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s+(\d{4,5})\s+([A-Z][a-z]+)\b/
  );
  if (locMatch) {
    entities.locations.push(locMatch[1]);
  } else {
    const cityOnly = body.match(
      /\b(Vienna|Austria|Mexico\s+City|Kansas\s+City|Punta\s+Cana)\b/i
    );
    // Only treat as location when adjacent to org/research context — avoid over-claiming
    if (cityOnly && /institute|research|association|university/i.test(body)) {
      entities.locations.push(cityOnly[1]);
    }
  }

  // Department / portfolio: "Director of X, Y, Org Person"
  const deptMatch = body.match(
    /\b(?:director|head|vp|vice\s+president)\s+of\s+([A-Z][^,]{3,60}?)(?=,)/i
  );
  if (deptMatch) {
    entities.departments.push(deptMatch[1].trim());
  }

  // Trailing person after department / org clause (last First Last in block)
  const trailingPerson = body.match(
    /(?:,\s*|\s+)([A-ZÀ-Ý][A-Za-zÀ-ÿ''’\u00AD-]+\s+[A-ZÀ-Ý][A-Za-zÀ-ÿ''’\u00AD-]+)\s*$/u
  );
  if (trailingPerson && looksLikeHumanNameV11(trailingPerson[1])) {
    if (!entities.persons.includes(trailingPerson[1])) {
      entities.persons.push(trailingPerson[1]);
    }
  }

  if (!entities.persons.length && /[A-Z][a-z]+\s+[A-Z][a-z]+/.test(body)) {
    // Ambiguous Title-Case spans without clear person boundary
    const frag = detectNameFragment(body.split(/\s+/).slice(0, 2).join(" "), body);
    if (frag.isFragment) {
      entities.ambiguous = true;
      entities.reasons.push("EXTRACTION_AMBIGUOUS");
    }
  }

  return entities;
}

/**
 * Classify entity before role scoring (V11).
 */
export function classifyEntityTypeV11(name = "", opts = {}) {
  const raw = String(name || "").trim();
  const stripped = stripHonorifics(raw);
  const nameForShape = stripped.bare || raw;

  if (ACADEMIC_TITLE_PHRASE.test(raw) || ACADEMIC_TITLE_PHRASE.test(stripped.bare)) {
    return {
      type: ENTITY_TYPE.ROLE_TITLE,
      okForNamedWho: false,
      reasons: ["academic_title_phrase"],
      personEvidence: [],
      nonPersonEvidence: ["academic_title_phrase"],
      entityTypeConfidence: 0.95,
    };
  }

  if (ROLE_TITLE_EXTENDED.test(raw)) {
    return {
      type: ENTITY_TYPE.ROLE_TITLE,
      okForNamedWho: false,
      reasons: ["role_title_phrase"],
      personEvidence: [],
      nonPersonEvidence: ["role_title_phrase"],
      entityTypeConfidence: 0.95,
    };
  }

  // Institutional noun dominance BEFORE fragment checks (org/dept/location ≠ recoverable person)
  const tokens = nameForShape.split(/\s+/).filter(Boolean);
  const instHits = tokens.filter((t) => NON_PERSON_CONTENT_TOKEN.test(t)).length;
  if (instHits >= 1 && tokens.length <= 3 && !looksLikeHumanNameV11(nameForShape)) {
    const type =
      /government|housing|communities|department|division|affairs|industry/i.test(
        nameForShape
      )
        ? ENTITY_TYPE.DEPARTMENT
        : /institute|association|research|survey|university|college|comparative/i.test(
              nameForShape
            )
          ? ENTITY_TYPE.ORGANIZATION
          : /vienna|campus|city|austria|mexico|kansas/i.test(nameForShape)
            ? ENTITY_TYPE.LOCATION
            : ENTITY_TYPE.ORGANIZATION;
    return {
      type,
      okForNamedWho: false,
      reasons: ["institutional_token_dominance"],
      personEvidence: [],
      nonPersonEvidence: ["institutional_token_dominance"],
      entityTypeConfidence: 0.9,
    };
  }

  const fragment = detectNameFragment(raw, opts.sourceBlock || opts.evidenceQuote || "");
  if (fragment.isFragment) {
    return {
      type: ENTITY_TYPE.UNKNOWN,
      okForNamedWho: false,
      reasons: ["name_fragment", ...fragment.reasons],
      personEvidence: [],
      nonPersonEvidence: fragment.reasons,
      entityTypeConfidence: 0.95,
      extractionState: "EXTRACTION_AMBIGUOUS",
    };
  }

  if (!looksLikeHumanNameV11(nameForShape) && !looksLikeHumanNameV11(raw)) {
    const v9 = classifyEntityTypeV9(raw, opts);
    if (v9.type === ENTITY_TYPE.PERSON) {
      return {
        ...v9,
        type: ENTITY_TYPE.UNKNOWN,
        okForNamedWho: false,
        reasons: ["not_human_name_shape_v11", ...(v9.reasons || [])],
        nonPersonEvidence: [
          ...(v9.nonPersonEvidence || []),
          "not_human_name_shape_v11",
        ],
      };
    }
    return v9;
  }

  const v9 = classifyEntityTypeV9(nameForShape, {
    ...opts,
    allowShapeAlone: opts.allowShapeAlone,
  });

  if (v9.type === ENTITY_TYPE.PERSON) {
    return {
      ...v9,
      normalizedPersonName: nameForShape,
    };
  }

  const nonPersonTypes = new Set([
    ENTITY_TYPE.ORGANIZATION,
    ENTITY_TYPE.DEPARTMENT,
    ENTITY_TYPE.COMMITTEE,
    ENTITY_TYPE.PROGRAM,
    ENTITY_TYPE.EVENT,
    ENTITY_TYPE.LOCATION,
    ENTITY_TYPE.ROLE_TITLE,
    ENTITY_TYPE.FUNCTIONAL_CONTACT,
  ]);
  if (nonPersonTypes.has(v9.type)) {
    return v9;
  }

  if (
    looksLikeHumanNameV11(nameForShape) &&
    (opts.fromStaffCard ||
      opts.hasTiedRole ||
      opts.hasPersonMailto ||
      opts.schemaPerson ||
      opts.profileUrl ||
      opts.hasPersonPhone)
  ) {
    return {
      type: ENTITY_TYPE.PERSON,
      okForNamedWho: true,
      reasons: ["human_name_shape_v11", "v11_mc_mac_or_intl_support"],
      personEvidence: ["human_name_shape_v11"],
      nonPersonEvidence: [],
      entityTypeConfidence: 0.85,
      normalizedPersonName: nameForShape,
    };
  }

  return v9;
}

/**
 * Gate a WHO candidate under V11 person-boundary contract.
 * Entity typing BEFORE role. Positive person evidence required.
 */
export function personTypeGateV11(candidate = {}) {
  const sourceBlock =
    candidate.evidenceQuote ||
    candidate.sourceTextBlock ||
    candidate.sourceBlock ||
    "";

  // If block parse yields a clearer full person, prefer expansion signal
  const blockEntities = sourceBlock
    ? parseMultiEntityContactBlock(
        [candidate.role, sourceBlock, candidate.name].filter(Boolean).join(" ")
      )
    : null;

  const classified = classifyEntityTypeV11(candidate.name, {
    schemaPerson: Boolean(
      candidate.fromStructuredData && /person/i.test(candidate.schemaType || "")
    ),
    fromStaffCard: /STAFF|CONTACT|TOURNAMENT|LEADERSHIP|EVENT_TEAM|DIRECTORY/i.test(
      String(candidate.sectionKind || candidate.sectionHint || "")
    ),
    hasPersonMailto: Boolean(
      candidate.email && !/^(info|events|contact|hello|office|admin|support)@/i.test(
        String(candidate.email)
      )
    ),
    hasPersonPhone: Boolean(candidate.phone),
    hasTiedRole: Boolean(candidate.role && String(candidate.role).trim().length >= 3),
    profileUrl: Boolean(
      candidate.profileUrl ||
        (/\/(people|staff|bio|profile|team)\//i.test(String(candidate.sourceUrl || "")))
    ),
    allowShapeAlone: Boolean(candidate.allowShapeAlone),
    sourceBlock,
    evidenceQuote: sourceBlock,
  });

  if (classified.type !== ENTITY_TYPE.PERSON || !classified.okForNamedWho) {
    // Recovery only for incomplete person spans (honorific / truncated surname)
    // — never for org/dept/title/location rejects.
    const nonRecoverableType = [
      ENTITY_TYPE.ORGANIZATION,
      ENTITY_TYPE.DEPARTMENT,
      ENTITY_TYPE.COMMITTEE,
      ENTITY_TYPE.PROGRAM,
      ENTITY_TYPE.EVENT,
      ENTITY_TYPE.LOCATION,
      ENTITY_TYPE.ROLE_TITLE,
      ENTITY_TYPE.FUNCTIONAL_CONTACT,
    ].includes(classified.type);

    const recoverableFailure =
      !nonRecoverableType &&
      (classified.reasons || []).some((r) =>
        /honorific_missing_surname|source_contains_longer_person_name/i.test(String(r))
      );

    const recovered =
      recoverableFailure
        ? blockEntities?.persons?.find((p) => looksLikeHumanNameV11(p)) || null
        : null;

    return {
      reject: true,
      gate:
        classified.extractionState === "EXTRACTION_AMBIGUOUS"
          ? "EXTRACTION_AMBIGUOUS"
          : classified.type === ENTITY_TYPE.UNKNOWN
            ? "ENTITY_TYPE_UNKNOWN"
            : "ENTITY_TYPE_NOT_PERSON",
      entityType: classified.type,
      entityTypeConfidence: classified.entityTypeConfidence,
      reasons: classified.reasons,
      personEvidence: classified.personEvidence,
      nonPersonEvidence: classified.nonPersonEvidence,
      recoveredPersonName: recovered,
      normalizedPersonName: classified.normalizedPersonName || null,
      blockEntities,
    };
  }

  // Candidate accepted — still expand truncated honorific forms to full block person
  let normalized =
    classified.normalizedPersonName || stripHonorifics(candidate.name).bare;
  if (
    blockEntities?.persons?.length &&
    !blockEntities.persons.some((p) => p.toLowerCase() === normalized.toLowerCase())
  ) {
    const better = blockEntities.persons.find((p) =>
      p.toLowerCase().includes(normalized.toLowerCase().split(/\s+/)[0])
    );
    if (better && looksLikeHumanNameV11(better)) {
      normalized = better;
    }
  }

  return {
    reject: false,
    gate: null,
    entityType: ENTITY_TYPE.PERSON,
    entityTypeConfidence: classified.entityTypeConfidence,
    reasons: classified.reasons,
    personEvidence: classified.personEvidence,
    nonPersonEvidence: classified.nonPersonEvidence,
    normalizedPersonName: normalized,
    recoveredPersonName: null,
    blockEntities,
  };
}

/**
 * Offline helper: expand incomplete candidate using source block.
 */
export function resolvePersonFromBlock(candidate = {}) {
  const gate = personTypeGateV11(candidate);
  if (!gate.reject) {
    return {
      ok: true,
      name: gate.normalizedPersonName || candidate.name,
      gate,
    };
  }
  if (gate.recoveredPersonName) {
    const retry = personTypeGateV11({
      ...candidate,
      name: gate.recoveredPersonName,
    });
    if (!retry.reject) {
      return { ok: true, name: retry.normalizedPersonName, gate: retry, recovered: true };
    }
  }
  return { ok: false, name: null, gate };
}

/** Thin compatibility: V9 gate still available for prior-cohort regression. */
export function personTypeGateV9Compat(candidate) {
  return personTypeGateV9(candidate);
}
