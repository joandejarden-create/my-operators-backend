/**
 * Person → selected owner affiliation gate.
 *
 * Conservative supported-assertion parser:
 *   Bound each assertion first, then extract person / employer / role
 *   from that assertion only (same sentence, same subject).
 *
 * Structured titles, labels, co-occurrence, and cross-sentence role
 * extraction are not independent proof.
 */

import { classifyLeadershipCategory, LEADERSHIP_CATEGORY } from "./leadership-extraction.js";

const ROLE_PRIORITY_RE =
  /\b(ceo|chief executive|founder|co-?founder|managing director|president|owner|principal|partner|chairman|chairwoman|desarrollo|development|asset\s*manager|acquisitions?|investment)\b/i;

const IRRELEVANT_SOURCE_ROLE_RE =
  /\b(receptionist|intern|summer\s+intern|assistant|secretary|clerk|concierge|front\s*desk|reporter|journalist|correspondent|blogger|podcaster|interviewer|host|moderator|guest)\b/i;

const NEGATION_RE =
  /\b(does\s+not|doesn't|do\s+not|don't|never|no\s+longer|not\s+(?:an?\s+)?(?:employee|executive|officer|member|affiliated)|unrelated\s+to|competitor\s+of|rivals?\s+|opposed\s+to)\b/i;

const HISTORICAL_RE =
  /\b(formerly|former|ex-|left|departed|resigned|retired\s+from|until\s+\d{4}|in\s+\d{4}\s+(?:left|departed|resigned)|alumni\s+of|was\s+(?:the\s+)?(?:ceo|president|founder|director)\s+(?:of|at)\b[^.]*\b(?:until|through|to)\s+\d{4}|no\s+longer\s+(?:with|at|works?))\b/i;

const THIRD_PARTY_SPEECH_RE =
  /\b(interviewed|interviews|interviewing|spoke\s+with|speaks\s+with|quoted|quotes|asked|reporting\s+on|wrote\s+about|covers?)\b/i;

/**
 * True legal-form suffixes only. Meaningful entity-name words (Capital, Holdings,
 * Management, Partners, Group, Properties, …) are identity components — never
 * stripped to manufacture equivalence between distinct entities.
 */
const LEGAL_FORM_SUFFIX_TOKENS = new Set([
  "llc",
  "llp",
  "inc",
  "ltd",
  "corp",
  "co",
  "company",
  "limited",
  "incorporated",
  "corporation",
  "plc",
  "lp",
  "sa",
  "sas",
  "bv",
  "gmbh",
  "nv",
  "ag",
  "pty",
]);

/**
 * After "and", only treat the next segment as the same org name when it is
 * clearly an org-name continuation:
 *   "& Co" / "& Name" / "Name Holdings|Capital|…"
 * Multi-token predicates before a suffix (e.g. "advises harbor holdings") are cut.
 * Ambiguous continuations are not absorbed.
 */
const ORG_NAME_CONTINUATION_RE =
  /^(?:&\s*[a-z][a-z0-9.'-]*|[a-z][a-z0-9.'-]*\s+(?:holdings|capital|partners|group|company|associates|llc|llp|inc|corp|ltd|fund|management|properties|realty|hotels|hospitality|trust|ventures|equity))\b/i;

const ROLE_WORDS =
  "ceo|chief executive(?:\\s+officer)?|founder|co-?founder|managing director|president|owner|principal|partner|chairman|chairwoman|director|officer|executive";

function normalizeTokens(name) {
  return String(name || "")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .split(/\s+/)
    .map((t) => t.replace(/[^a-z0-9]/g, ""))
    .filter((t) => t.length > 2);
}

/**
 * Ordered entity tokens for identity comparison.
 * Preserve every alphanumeric name component regardless of length (initials,
 * short codes, numeric segments). Length-based dropping collapses distinct
 * entities (e.g. "Harbor AB Holdings" vs "Harbor CD Holdings").
 * Trailing legal-form tokens are stripped later via stripTrailingLegalForms —
 * they are not erased here as identity components mid-name.
 */
function normalizeEntityTokenSequence(name) {
  return String(name || "")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/&/g, " and ")
    .split(/\s+/)
    .map((t) => t.replace(/[^a-z0-9]/g, ""))
    .filter((t) => t.length > 0);
}

function stripTrailingLegalForms(tokens) {
  const out = [...(tokens || [])];
  while (out.length && LEGAL_FORM_SUFFIX_TOKENS.has(out[out.length - 1])) {
    out.pop();
  }
  return out;
}

function entitySequencesEqual(a, b) {
  if (!a?.length || !b?.length || a.length !== b.length) return false;
  return a.every((t, i) => t === b[i]);
}

function escapeRe(s) {
  return String(s || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function personNamePattern(personTokens) {
  if (!personTokens.length) return null;
  if (personTokens.length === 1) {
    return new RegExp(`\\b${escapeRe(personTokens[0])}\\b`, "i");
  }
  return new RegExp(`\\b${personTokens.map(escapeRe).join("\\s+")}\\b`, "i");
}

/**
 * Strict entity match: normalized full-name equality (word order preserved),
 * or equality to an independently evidenced alias. Token containment / soft
 * org-word stripping must not merge parent, subsidiary, fund, or near-names.
 *
 * @param {string} employerPhrase
 * @param {string[]|string} ownerTokensOrName - preferred: pass opts.ownerName
 * @param {{ aliases?: string[], ownerName?: string }} [opts]
 */
export function employerMatches(employerPhrase, ownerTokensOrName, opts = {}) {
  if (!employerPhrase) return false;

  const ownerName =
    opts.ownerName ||
    (typeof ownerTokensOrName === "string"
      ? ownerTokensOrName
      : Array.isArray(ownerTokensOrName)
        ? ownerTokensOrName.join(" ")
        : "");
  if (!String(ownerName || "").trim() && !(Array.isArray(ownerTokensOrName) && ownerTokensOrName.length)) {
    return false;
  }

  const empSeq = stripTrailingLegalForms(normalizeEntityTokenSequence(employerPhrase));
  if (!empSeq.length) return false;

  const ownerSeq = stripTrailingLegalForms(
    normalizeEntityTokenSequence(ownerName || (ownerTokensOrName || []).join(" "))
  );
  if (ownerSeq.length && entitySequencesEqual(empSeq, ownerSeq)) {
    return true;
  }

  // Evidenced aliases only — full-sequence equality under the same rules.
  // Do not invent aliases or accept free-text claims without structured evidence.
  for (const alias of opts.aliases || []) {
    const aliasSeq = stripTrailingLegalForms(normalizeEntityTokenSequence(alias));
    if (aliasSeq.length && entitySequencesEqual(empSeq, aliasSeq)) {
      return true;
    }
  }

  return false;
}

/**
 * Bound employer object: same-sentence, stop at non-org "and" continuations.
 */
export function parseEmployerPhrase(raw) {
  let s = String(raw || "").trim();
  if (!s) return "";
  s = s.replace(/^(?:the|a|an)\s+/i, "");

  // Same sentence only
  const punct = s.search(/[.;!?\n]/);
  if (punct >= 0) s = s.slice(0, punct);

  // Cut relative clauses
  const rel = s.search(/\s+(?:who|which|where|when|while|that)\b/i);
  if (rel >= 0) s = s.slice(0, rel);

  // "and" / "&" — keep only evidenced org-name continuations
  const parts = s.split(/\s+and\s+/i);
  if (parts.length > 1) {
    const kept = [parts[0]];
    for (let i = 1; i < parts.length; i++) {
      const seg = parts[i].trim();
      if (ORG_NAME_CONTINUATION_RE.test(seg)) {
        kept.push(seg);
      } else {
        // New predicate / ambiguous clause — do not absorb
        break;
      }
    }
    s = kept.join(" and ");
  }

  s = s.trim().slice(0, 80);
  return s;
}

/** First sentence remainder after a cue (no cross-sentence field extraction). */
function sameSentenceRemainder(textFromCue) {
  const s = String(textFromCue || "");
  const punct = s.search(/[.;!?\n]/);
  return punct >= 0 ? s.slice(0, punct) : s;
}

function subjectBefore(text, matchIndex, personTokens) {
  const before = text.slice(Math.max(0, matchIndex - 120), matchIndex);
  const pat = personNamePattern(personTokens);
  if (!pat) return false;

  if (/,\s*who\s+$/i.test(before)) {
    const ante = before.match(/\b([a-z][a-z.'-]*(?:\s+[a-z][a-z.'-]*){0,3})\s*,\s*who\s+$/i);
    if (ante) return pat.test(ante[1]);
    return false;
  }

  if (THIRD_PARTY_SPEECH_RE.test(before)) {
    const idx = before.search(THIRD_PARTY_SPEECH_RE);
    const beforeVerb = before.slice(0, idx);
    const afterVerb = before.slice(idx).replace(THIRD_PARTY_SPEECH_RE, "");
    const personBefore = pat.test(beforeVerb);
    const personAfter = pat.test(afterVerb);
    if (personBefore && !personAfter) return false;
  }

  const tail = before
    .replace(/[^a-z0-9\s.'-]/g, " ")
    .trim()
    .split(/\s+/)
    .filter(Boolean)
    .slice(-4);
  const tailBlob = tail.join(" ");
  if (!pat.test(tailBlob)) return false;

  const endAnchored = new RegExp(`(?:${pat.source})\\s*$`, pat.flags || "i");
  return endAnchored.test(tailBlob);
}

function roleIsObjectiveRelevant(roleText) {
  const title = String(roleText || "").trim();
  if (!title) return false;
  if (IRRELEVANT_SOURCE_ROLE_RE.test(title)) return false;
  const cat = classifyLeadershipCategory(title);
  return (
    ROLE_PRIORITY_RE.test(title) ||
    cat === LEADERSHIP_CATEGORY.DEVELOPMENT_ASSET ||
    cat === LEADERSHIP_CATEGORY.BOARD_OWNERSHIP ||
    cat === LEADERSHIP_CATEGORY.OPERATING_EXECUTIVE
  );
}

/**
 * Extract optional "as <role>" from the SAME sentence remainder after employer.
 * Never crosses sentence or subject boundaries.
 */
function roleAsInSameSentence(sameSentenceAfterCue) {
  const m = String(sameSentenceAfterCue || "").match(
    /^([a-z0-9][a-z0-9 .,&'-]{0,60}?)\s+as\s+(?:a\s+|an\s+)?([a-z][a-z\s-]{2,40})\s*$/i
  );
  if (m) return { employerRaw: m[1].trim(), role: m[2].trim() };
  // "joined as CEO" with no employer after joined — employer missing
  const asOnly = String(sameSentenceAfterCue || "").match(
    /^as\s+(?:a\s+|an\s+)?([a-z][a-z\s-]{2,40})\s*$/i
  );
  if (asOnly) return { employerRaw: "", role: asOnly[1].trim() };
  return { employerRaw: sameSentenceAfterCue, role: null };
}

/**
 * Parse explicit supported assertions from evidence text.
 */
export function parseAffiliationAssertions(text, personTokens) {
  const blob = String(text || "").toLowerCase();
  const out = [];
  if (!blob.trim() || !personTokens.length) return out;

  const patterns = [
    {
      kind: "ROLE_EMPLOYER",
      re: new RegExp(
        `\\b(?:is|as)\\s+(?:the\\s+)?(${ROLE_WORDS})\\s+(?:of|at|for)\\s+`,
        "gi"
      ),
      parse(blob, m) {
        const rem = sameSentenceRemainder(blob.slice(m.index + m[0].length));
        return {
          role: m[1],
          employer: parseEmployerPhrase(rem),
          excerpt_end: m.index + m[0].length + rem.length,
        };
      },
    },
    {
      kind: "WORKS_AT",
      re: /\b(?:works?|serving|serves)\s+(?:at|for|with)\s+/gi,
      parse(blob, m) {
        const rem = sameSentenceRemainder(blob.slice(m.index + m[0].length));
        const { employerRaw, role } = roleAsInSameSentence(rem);
        return {
          role,
          employer: parseEmployerPhrase(employerRaw || rem),
          excerpt_end: m.index + m[0].length + rem.length,
        };
      },
    },
    {
      kind: "JOINED",
      re: /\bjoined\s+/gi,
      parse(blob, m) {
        const rem = sameSentenceRemainder(blob.slice(m.index + m[0].length));
        const { employerRaw, role } = roleAsInSameSentence(rem);
        return {
          role,
          employer: parseEmployerPhrase(employerRaw || (role ? "" : rem)),
          excerpt_end: m.index + m[0].length + rem.length,
        };
      },
    },
    {
      kind: "LEADS",
      re: /\b(?:leads?|heads?)\s+/gi,
      parse(blob, m) {
        const rem = sameSentenceRemainder(blob.slice(m.index + m[0].length));
        return {
          role: "lead",
          employer: parseEmployerPhrase(rem),
          excerpt_end: m.index + m[0].length + rem.length,
        };
      },
    },
  ];

  for (const p of patterns) {
    const re = new RegExp(p.re.source, "gi");
    let m;
    while ((m = re.exec(blob))) {
      if (!subjectBefore(blob, m.index, personTokens)) continue;
      const parsed = p.parse(blob, m);
      // JOINED/WORKS without employer object → not a supported affiliation assertion
      if (!parsed.employer && p.kind !== "ROLE_EMPLOYER") {
        // "Bob joined as CEO" — role without employer; skip as owner affiliation
        continue;
      }
      if (!parsed.employer) continue;

      const start = Math.max(0, m.index - 40);
      const end = Math.min(blob.length, parsed.excerpt_end + 8);
      const excerpt = blob.slice(start, end);
      const win = blob.slice(Math.max(0, m.index - 40), Math.min(blob.length, m.index + 100));
      out.push({
        kind: p.kind,
        person_matched: true,
        employer: parsed.employer,
        role: parsed.role ? String(parsed.role).trim() : null,
        excerpt,
        negated: NEGATION_RE.test(win),
        historical: HISTORICAL_RE.test(win),
        cue_index: m.index,
      });
    }
  }
  return out;
}

/**
 * @param {object} person
 * @param {object} selectedOwner — { owner_display_name, owner_entity_id, aliases? }
 * @param {object} [opts]
 */
export function qualifyPersonOwnerAffiliation(person, selectedOwner, opts = {}) {
  const ownerName = String(selectedOwner?.owner_display_name || "").trim();
  if (!person?.display_name) {
    return { ok: false, reason: "MISSING_PERSON_NAME" };
  }
  if (!ownerName) {
    return { ok: false, reason: "MISSING_SELECTED_OWNER" };
  }

  const evidenceBlobs = [];
  const evidenceRefs = [];
  for (const e of person.evidence || []) {
    if (e?.excerpt) {
      evidenceBlobs.push(String(e.excerpt));
      evidenceRefs.push({
        source_url: e.source_url || e.url || null,
        excerpt: String(e.excerpt),
      });
    }
  }
  if (opts.sourceDocumentText) evidenceBlobs.push(String(opts.sourceDocumentText));
  if (opts.allowProvenanceNotes) {
    for (const n of person.provenance?.affiliation_notes || []) {
      evidenceBlobs.push(String(n));
    }
  }

  const blob = evidenceBlobs.join("\n").toLowerCase();
  const ownerTokens = normalizeTokens(ownerName);
  const personTokens = normalizeTokens(person.display_name);
  const structuredTitle = String(person.title || "").trim();
  const matchOpts = {
    ownerName,
    aliases: [
      ...(selectedOwner?.aliases || []),
      ...(opts.owner_aliases || []),
    ],
  };

  if (String(person.display_name).trim().split(/\s+/).filter(Boolean).length >= 2 && personTokens.length < 2) {
    return {
      ok: false,
      reason: "PERSON_NAME_TOO_WEAK",
      affiliation_independent: false,
      role_relevant: false,
    };
  }

  if (!blob.trim()) {
    return {
      ok: false,
      reason: "AFFILIATION_NEEDS_SOURCE_SUPPORT",
      affiliation_independent: false,
      role_relevant: false,
      structured_title_ignored: Boolean(structuredTitle),
    };
  }

  const assertions = parseAffiliationAssertions(blob, personTokens);

  const toOwner = assertions.filter((a) => employerMatches(a.employer, ownerTokens, matchOpts));
  const toOther = assertions.filter(
    (a) => a.employer && !employerMatches(a.employer, ownerTokens, matchOpts)
  );

  const rivalRole = toOther.find((a) => a.kind === "ROLE_EMPLOYER" && !a.negated && !a.historical);
  if (rivalRole && (toOwner.length === 0 || tokensPresentOwnerElsewhere(blob, ownerTokens))) {
    const ownerRole = toOwner.find(
      (a) => a.kind === "ROLE_EMPLOYER" && !a.negated && !a.historical && roleIsObjectiveRelevant(a.role)
    );
    if (!ownerRole) {
      return {
        ok: false,
        reason: "WRONG_EMPLOYER",
        affiliation_independent: false,
        role_relevant: false,
        rival_employer: rivalRole.employer,
        source_role: rivalRole.role,
        binding_decision: {
          matched_assertions: assertions,
          rejected: "role_or_affiliation_bound_to_other_employer",
        },
        evidence_refs: evidenceRefs,
      };
    }
  }

  const liveOwnerAff = toOwner.filter((a) => !a.negated && !a.historical);
  const negatedOwner = toOwner.filter((a) => a.negated);
  const historicalOwner = toOwner.filter((a) => a.historical && !a.negated);

  if (!liveOwnerAff.length) {
    if (negatedOwner.length) {
      return {
        ok: false,
        reason: "AFFILIATION_NEGATED",
        affiliation_independent: false,
        role_relevant: false,
        evidence_refs: evidenceRefs,
      };
    }
    if (historicalOwner.length) {
      return {
        ok: false,
        reason: "AFFILIATION_HISTORICAL",
        affiliation_independent: false,
        role_relevant: false,
        evidence_refs: evidenceRefs,
      };
    }
    const personOrg = String(person.organization_name || "").toLowerCase();
    if (personOrg && !employerMatches(personOrg, ownerTokens, matchOpts) && toOther.length) {
      return {
        ok: false,
        reason: "WRONG_EMPLOYER",
        affiliation_independent: false,
        role_relevant: false,
        evidence_refs: evidenceRefs,
      };
    }
    return {
      ok: false,
      reason: "AFFILIATION_NOT_INDEPENDENTLY_SUPPORTED",
      affiliation_independent: false,
      role_relevant: false,
      structured_title_ignored: Boolean(structuredTitle),
      binding_decision: { matched_assertions: assertions },
      evidence_refs: evidenceRefs,
    };
  }

  const affiliationAssertion = liveOwnerAff[0];

  const roleAssertions = liveOwnerAff.filter(
    (a) => a.role && roleIsObjectiveRelevant(a.role) && !IRRELEVANT_SOURCE_ROLE_RE.test(a.role)
  );
  const irrelevantRole = liveOwnerAff.find((a) => a.role && IRRELEVANT_SOURCE_ROLE_RE.test(a.role));
  if (irrelevantRole) {
    return {
      ok: false,
      reason: "IRRELEVANT_ROLE",
      role_relevant: false,
      affiliation_independent: true,
      source_role: irrelevantRole.role,
      structured_title_ignored: Boolean(structuredTitle),
      binding_window: irrelevantRole.excerpt?.slice(0, 180) || null,
      binding_decision: {
        affiliation_assertion: affiliationAssertion,
        role_assertion: irrelevantRole,
      },
      evidence_refs: evidenceRefs,
    };
  }

  if (!roleAssertions.length) {
    return {
      ok: false,
      reason: "ROLE_NOT_SOURCE_SUPPORTED",
      affiliation_independent: true,
      role_relevant: false,
      source_role: null,
      structured_title_ignored: Boolean(structuredTitle),
      binding_window: affiliationAssertion.excerpt?.slice(0, 180) || null,
      binding_decision: {
        affiliation_assertion: affiliationAssertion,
        role_assertion: null,
        note: "affiliation_to_owner_without_source_role_for_same_person",
      },
      evidence_refs: evidenceRefs,
    };
  }

  const roleAssertion = roleAssertions[0];
  return {
    ok: true,
    affiliation_independent: true,
    role_relevant: true,
    source_role: roleAssertion.role,
    binding_window: roleAssertion.excerpt?.slice(0, 180) || affiliationAssertion.excerpt?.slice(0, 180) || null,
    binding_decision: {
      affiliation_assertion: affiliationAssertion,
      role_assertion: roleAssertion,
      employer: roleAssertion.employer || affiliationAssertion.employer,
    },
    evidence_refs: evidenceRefs,
  };
}

function tokensPresentOwnerElsewhere(blob, ownerTokens) {
  return ownerTokens.every((t) => blob.includes(t));
}

export { ROLE_PRIORITY_RE, NEGATION_RE, HISTORICAL_RE, IRRELEVANT_SOURCE_ROLE_RE };
