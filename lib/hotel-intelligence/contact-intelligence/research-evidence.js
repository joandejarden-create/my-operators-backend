/** Pure, offline checks over captured passages. Not a truth/accuracy verifier.
 * URL-only references, provider claims and caller status flags are insufficient.
 * Keep original captures outside this module; do not turn retrieval dates into verification dates.
 */
export const normalizeEvidenceText = (v) => String(v || '').normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
const esc = (v) => String(v).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

/** Strip brand/collection suffixes so "Cap Juluca A Belmond Hotel Anguilla" can match "Cap Juluca". */
export function hotelCoreName(hotelName = '') {
  return String(hotelName || '')
    .replace(/,?\s+(?:an? |by )?(?:autograph collection|tapestry collection|lxr hotels|auberge resort|belmond hotel|all[- ]inclusive|adults only).*$/i, '')
    .replace(/\s+an\s+auberge\s+resort.*$/i, '')
    .replace(/\s+a\s+belmond\s+hotel.*$/i, '')
    .trim() || String(hotelName || '').trim();
}

export function mentions(text, name) {
  const n = normalizeEvidenceText(name);
  return n.length >= 3 && (` ${normalizeEvidenceText(text)} `).includes(` ${n} `);
}

/** True if text mentions the hotel under full or core name. */
export function mentionsHotel(text, hotelName) {
  return mentions(text, hotelName) || mentions(text, hotelCoreName(hotelName));
}

export function evidenceRecords(refs = []) {
  return (Array.isArray(refs) ? refs : [refs]).filter((r) => {
    if (!r || typeof r !== 'object') return false;
    try { if (!['http:', 'https:'].includes(new URL(r.source_url || r.url).protocol)) return false; } catch { return false; }
    const text = r.excerpt || r.passage;
    if (typeof text !== 'string' || text.trim().length < 20) return false;
    if (/PROVIDER|DIRECTORY|SNIPPET|HYPOTHESIS/i.test(r.source_type || r.source_class || '')) return false;
    if (/SURFE|CONTACTOUT|APOLLO|ZOOMINFO/i.test(new URL(r.source_url || r.url).hostname)) return false;
    if (r.source_text != null && !String(r.source_text).includes(text)) return false;
    return true;
  });
}

/**
 * Classify a retrieved passage lead. Does not invent owners.
 * Negative controls: operator boilerplate, décor metaphors, proposed deals, former owners.
 */
export function classifyOwnershipLead(excerpt, hotelName = '') {
  const text = String(excerpt || '');
  const s = normalizeEvidenceText(text);
  const hotelOk = mentionsHotel(text, hotelName);
  const reasons = [];

  if (/\b(?:plans? to|agreed to|will acquire|subject to|proposed|letter of intent|loi|in talks|negociando)\b/i.test(text)) {
    reasons.push('PROPOSED_OR_CONTINGENT_TRANSACTION');
  }
  if (/\b(?:formerly|previously|once owned|no longer|ex-owner|sold the|divested)\b/i.test(text) &&
      !/\b(?:still|remains|currently)\b/i.test(text)) {
    reasons.push('FORMER_OR_HISTORICAL_OWNER_LANGUAGE');
  }
  // Décor / marketing metaphor: "feel of a … residence owned by a glamorous traveler"
  if (/\b(?:feel of|atmosphere of|style of|like a|as if)\b[^.!]{0,80}\bowned by\b/i.test(text)) {
    reasons.push('DECOR_OR_MARKETING_METAPHOR');
  }
  // Operator / brand self-description without asset transaction
  if (/\b(?:renowned |internationally )?owner and operator\b/i.test(text) &&
      !/\b(?:acquired|purchased|bought|acquisition by|adquiriu|adquirió)\b/i.test(text)) {
    reasons.push('OPERATOR_SELF_DESCRIPTION');
  }
  if (/\bfamily[- ]owned\b/i.test(text) && hotelOk) {
    reasons.push('FAMILY_OWNED_UNNAMED');
  }
  if (/\b(?:acquired|purchased|bought|acquisition by|adquiriu|adquirió)\b/i.test(text) && hotelOk) {
    reasons.push('TRANSACTION_ACQUISITION_LANGUAGE');
  }
  if (/\breached a deal to acquire\b/i.test(text) || /\bagreed to acquire\b/i.test(text)) {
    reasons.push('CORPORATE_PARENT_TRANSACTION_LANGUAGE');
  }
  // Present-tense named owner / founder-owner statements (hotel-bound).
  // These are research leads — NOT transaction-bound OWNER_CANDIDATE and NOT current ownership proof.
  if (
    hotelOk &&
    (/\bis the owner of\b/i.test(text) ||
      /\bfounder and owner of\b/i.test(text) ||
      /\bmain shareholder of\b/i.test(text) ||
      /\bProprietor\b/i.test(text))
  ) {
    reasons.push('NAMED_CURRENT_OWNER_STATEMENT');
  }
  if (!hotelOk && /\b(?:owner|acquir|purchas)\b/i.test(text)) {
    reasons.push('HOTEL_NAME_NOT_BOUND_IN_PASSAGE');
  }

  const hasTxn = reasons.includes('TRANSACTION_ACQUISITION_LANGUAGE');
  const blocked = reasons.some((r) =>
    r === 'DECOR_OR_MARKETING_METAPHOR' ||
    r === 'PROPOSED_OR_CONTINGENT_TRANSACTION' ||
    (r === 'OPERATOR_SELF_DESCRIPTION' && !hasTxn)
  );
  return {
    hotel_bound: hotelOk,
    reasons,
    allow_owner_candidate: !blocked && hasTxn,
    named_owner_statement_lead:
      !blocked && reasons.includes('NAMED_CURRENT_OWNER_STATEMENT') && !hasTxn,
    note: blocked
      ? 'Negative control — do not promote to OWNER_CANDIDATE from this passage alone'
      : hasTxn
        ? 'Transaction language present — still requires currentness adjudication'
        : reasons.includes('NAMED_CURRENT_OWNER_STATEMENT')
          ? 'Named owner statement lead — requires currency corroboration; not transaction-bound OWNER_CANDIDATE'
          : 'No transaction-bound owner candidate',
  };
}

/**
 * Split evidence text into sentence/clause/list units while protecting contact tokens
 * and single-letter initials (J. Rivera) so initials do not create false boundaries.
 */
export function evidenceUnits(text) {
  const raw = String(text || "");
  const { text: protectedContacts } = protectContactTokens(raw);
  // Preserve initials like "J. Rivera" / "A.B. Smith" across sentence splits
  const protectedInitials = protectedContacts.replace(/\b([A-Za-z])\.(?=\s*[A-Za-z])/g, "$1·");
  return protectedInitials
    .split(/(?:(?<=[.!?])\s+|\n+|;\s+|\|\s*)/)
    .map((u) => u.replace(/·/g, ".").trim())
    .filter(Boolean);
}

/**
 * Explicit denial that the named party owns the hotel.
 */
export function ownershipNegationForParty(text, hotel, owner) {
  const hotelNames = [hotel, hotelCoreName(hotel)].filter(Boolean);
  if (!hotelNames.some((h) => mentions(text, h)) || !mentions(text, owner)) return false;
  for (const unit of evidenceUnits(text)) {
    const s = normalizeEvidenceText(unit);
    if (!mentions(unit, owner)) continue;
    if (!hotelNames.some((h) => mentions(unit, h)) && !/\b(?:the property|the hotel|it)\b/i.test(unit)) {
      // Allow negation units that name owner + own without repeating hotel if hotel is in full text
    }
    const o = esc(normalizeEvidenceText(owner));
    if (new RegExp(`${o}.{0,60}\\b(?:does not|do not|doesn't|don't|never|not|no longer)\\s+(?:own|owns|owned)\\b`).test(s)) {
      return true;
    }
    if (/\b(?:does not|do not|doesn't|don't|never|not|no longer)\s+(?:own|owns|owned)\b/.test(s) && mentions(unit, owner)) {
      return true;
    }
  }
  // Full-text fallback for single-clause denials
  const s = normalizeEvidenceText(text);
  const o = esc(normalizeEvidenceText(owner));
  if (new RegExp(`${o}.{0,60}\\b(?:does not|do not|doesn't|don't|never|not|no longer)\\s+(?:own|owns|owned)\\b`).test(s)) {
    return true;
  }
  return false;
}

/**
 * Named party is bound only as operator/manager (not owner) toward the hotel.
 * Evaluated per evidence unit so a later "X owns hotel" does not credit Y who only manages.
 */
export function operatorOnlyBinding(text, hotel, owner) {
  const hotelNames = [hotel, hotelCoreName(hotel)].filter(Boolean);
  if (!hotelNames.some((h) => mentions(text, h)) || !mentions(text, owner)) return false;
  let operatorHit = false;
  let ownershipVerbForOwner = false;
  for (const unit of evidenceUnits(text)) {
    if (!mentions(unit, owner)) continue;
    const s = normalizeEvidenceText(unit);
    const o = esc(normalizeEvidenceText(owner));
    for (const hotelName of hotelNames) {
      const h = esc(normalizeEvidenceText(hotelName));
      const hotelInUnit = mentions(unit, hotelName);
      if (
        hotelInUnit &&
        (new RegExp(`${o}.{0,40}\\b(?:manages|managed|operates|operated|is the operator of|operator of)\\b.{0,80}${h}`).test(s) ||
          new RegExp(`${h}.{0,40}\\b(?:managed by|operated by|operated under)\\b.{0,40}${o}`).test(s))
      ) {
        operatorHit = true;
      }
      if (
        hotelInUnit &&
        (new RegExp(`${o}.{0,40}\\b(?:owns|owned|acquired|purchased|is the (?:current )?owner of|proprietor of)\\b.{0,80}${h}`).test(s) ||
          new RegExp(`${h}.{0,60}\\b(?:owned by|property of|is the property of)\\b.{0,40}${o}`).test(s))
      ) {
        ownershipVerbForOwner = true;
      }
    }
  }
  return operatorHit && !ownershipVerbForOwner;
}

/**
 * Acquisition/ownership language for this party is superseded by a sale/divestiture.
 */
export function formerOwnerAfterSaleBinding(text, hotel, owner) {
  const hotelNames = [hotel, hotelCoreName(hotel)].filter(Boolean);
  if (!hotelNames.some((h) => mentions(text, h)) || !mentions(text, owner)) return false;
  // Sale language often spans one sentence — evaluate full text with owner as seller subject
  const s = normalizeEvidenceText(text);
  const o = esc(normalizeEvidenceText(owner));
  for (const hotelName of hotelNames) {
    const h = esc(normalizeEvidenceText(hotelName));
    if (
      new RegExp(
        `${o}.{0,160}\\b(?:acquired|purchased|bought)\\b.{0,160}${h}.{0,200}\\b(?:sold|divested|disposed of)\\b`
      ).test(s)
    ) {
      return true;
    }
    if (
      new RegExp(
        `${o}.{0,160}\\b(?:acquired|purchased|bought)\\b.{0,200}\\bsold (?:it|the (?:hotel|property|resort)|${h})\\b`
      ).test(s)
    ) {
      return true;
    }
    if (new RegExp(`${o}.{0,80}\\b(?:sold|divested)\\b.{0,100}${h}`).test(s)) {
      return true;
    }
    if (
      new RegExp(
        `${o}.{0,80}\\b(?:formerly|previously|once|no longer)\\b.{0,40}\\b(?:owned|owner)\\b.{0,80}${h}`
      ).test(s) ||
      new RegExp(`${h}.{0,80}\\b(?:formerly|previously)\\s+owned by\\b.{0,60}${o}`).test(s)
    ) {
      return true;
    }
  }
  return false;
}

/**
 * Passage binds hotel + named party via acquisition/ownership transaction language.
 * Does NOT mean current ownership is proven. Prefer currentOwnershipPassageSupport.
 */
export function ownershipPassageSupport(text, hotel, owner) {
  const hotelNames = [hotel, hotelCoreName(hotel)].filter(Boolean);
  if (!hotelNames.some((h) => mentions(text, h)) || !mentions(text, owner)) return false;
  if (ownershipNegationForParty(text, hotel, owner)) return false;
  if (operatorOnlyBinding(text, hotel, owner)) return false;
  if (formerOwnerAfterSaleBinding(text, hotel, owner)) return false;
  if (classifyOwnershipLead(text, hotel).reasons.includes('DECOR_OR_MARKETING_METAPHOR')) return false;
  for (const unit of evidenceUnits(text)) {
    if (!mentions(unit, owner)) continue;
    if (isOwnershipReporterFrame(unit, owner)) continue;
    if (!hotelNames.some((h) => mentions(unit, h))) continue;
    const softNegated =
      /\b(proposed|plans to|agreed to|will acquire|subject to|nao|anteriormente|pretende)\b/i.test(unit);
    if (softNegated) continue;
    const s = normalizeEvidenceText(unit);
    const o = esc(normalizeEvidenceText(owner));
    for (const hotelName of hotelNames) {
      if (!mentions(unit, hotelName)) continue;
      const h = esc(normalizeEvidenceText(hotelName));
      const passive = new RegExp(
        `${h}.{0,120}\\b(?:owned by|acquired by|purchased by|acquisition by|propiedad de|adquirido por|adquirida por|pertence a) ${o}\\b`
      );
      // Active: owner is verb subject — no intervening reporter/org gap
      const active = new RegExp(
        `(?:^|\\s)${o} (?:has |have |today |recently |today announced it has |announced it has )?(?:acquired|purchased|owns|adquiriu|adquirio|compro) (?:the |el |o |a |hotel |resort |property ){0,2}${h}\\b`
      );
      const activeProperty = new RegExp(
        `(?:^|\\s)${o} (?:has |have )?(?:acquired|purchased|owns) (?:the )?(?:property|hotel|resort)\\b`
      );
      if (passive.test(s) || active.test(` ${s} `) || activeProperty.test(` ${s} `)) return true;
    }
  }
  return false;
}

/**
 * True when the named party appears only as a reporter/announcer of someone
 * else's ownership — not as the ownership-relationship subject.
 */
export function isOwnershipReporterFrame(text, owner) {
  const s = normalizeEvidenceText(text);
  const o = esc(normalizeEvidenceText(owner));
  if (!o || !mentions(text, owner)) return false;
  return new RegExp(
    `${o}.{0,80}\\b(?:confirms?|announces?|says|said|reports?|stated?|states|discloses?|notes?|according to|per)\\b.{0,120}\\b(?:owns|owned|acquired|purchased|is the (?:current )?owner|proprietor)\\b`
  ).test(s);
}

/**
 * Active ownership: proposed owner must be the grammatical subject of the
 * ownership verb (optional short auxiliaries only). No intervening org.
 */
function activeCurrentOwnershipInUnit(s, o, h) {
  // "Beacon Capital owns Seaside Hotel" / "Beacon Capital is the owner of Seaside Hotel"
  const owns = new RegExp(
    `(?:^|\\s)${o} (?:has |have |today |recently |currently )?(?:owns|is the (?:current )?(?:property )?owner of|is owner of|proprietor of) (?:the |el |o |a )?${h}\\b`
  );
  return owns.test(` ${s} `);
}

function passiveCurrentOwnershipInUnit(s, o, h) {
  return new RegExp(
    `${h}.{0,80}\\b(?:is |are )?(?:owned by|the property of|property of) ${o}\\b`
  ).test(s);
}

function activeAcquisitionWithCurrentnessInUnit(s, o, h) {
  const acquired = new RegExp(
    `(?:^|\\s)${o} (?:has |have )?(?:acquired|purchased|bought) (?:the |el |o |a )?${h}\\b`
  );
  const remains = /\b(?:remains|still is|continues as|currently is|is currently|current (?:property )?owner)\b/;
  return acquired.test(` ${s} `) && remains.test(s);
}

/**
 * Subject → ownership relation → object with present/current temporal status.
 * Binding is per evidence unit. Reporter/adviser orgs do not inherit another
 * party's ownership verb. Ambiguous co-occurrence is not support.
 */
export function currentOwnershipPassageSupport(text, hotel, owner) {
  if (!text || !hotel || !owner) return false;
  if (ownershipNegationForParty(text, hotel, owner)) return false;
  if (operatorOnlyBinding(text, hotel, owner)) return false;
  if (formerOwnerAfterSaleBinding(text, hotel, owner)) return false;
  if (classifyOwnershipLead(text, hotel).reasons.includes('DECOR_OR_MARKETING_METAPHOR')) return false;

  const hotelNames = [hotel, hotelCoreName(hotel)].filter(Boolean);
  if (!hotelNames.some((h) => mentions(text, h)) || !mentions(text, owner)) return false;

  for (const unit of evidenceUnits(text)) {
    if (!mentions(unit, owner)) continue;
    if (isOwnershipReporterFrame(unit, owner)) continue;
    const softNegated =
      /\b(proposed|plans to|agreed to|will acquire|subject to|nao|anteriormente|pretende)\b/i.test(unit);
    if (softNegated) continue;
    const s = normalizeEvidenceText(unit);
    const o = esc(normalizeEvidenceText(owner));
    for (const hotelName of hotelNames) {
      if (!mentions(unit, hotelName)) continue;
      const h = esc(normalizeEvidenceText(hotelName));
      if (activeCurrentOwnershipInUnit(s, o, h) || passiveCurrentOwnershipInUnit(s, o, h)) {
        return true;
      }
      if (activeAcquisitionWithCurrentnessInUnit(s, o, h)) return true;
    }
  }

  // Multi-sentence: "X acquired Hotel." + "X remains the current owner."
  // Still require X to be the acquisition subject (no reporter gap).
  if (isOwnershipReporterFrame(text, owner)) return false;
  const full = normalizeEvidenceText(text);
  const oFull = esc(normalizeEvidenceText(owner));
  for (const hotelName of hotelNames) {
    const h = esc(normalizeEvidenceText(hotelName));
    const acquired = new RegExp(
      `(?:^|\\s)${oFull} (?:has |have )?(?:acquired|purchased|bought) (?:the |el |o |a )?${h}\\b`
    );
    const remainsOwner = new RegExp(
      `(?:^|\\s)${oFull}.{0,40}\\b(?:remains|still is|continues as)\\b.{0,40}\\b(?:current )?(?:property )?owner\\b`
    );
    if (
      acquired.test(` ${full} `) &&
      remainsOwner.test(` ${full} `) &&
      !formerOwnerAfterSaleBinding(text, hotel, owner)
    ) {
      return true;
    }
  }
  return false;
}

/**
 * Protect emails, URLs, and phones before sentence/clause segmentation so dots
 * inside those tokens do not create false unit boundaries.
 */
export function protectContactTokens(text) {
  const tokens = [];
  let out = String(text || '');
  const stash = (m) => {
    const id = tokens.length;
    tokens.push(m);
    return ` ⟦C${id}⟧ `;
  };
  out = out.replace(/[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}/gi, stash);
  out = out.replace(/https?:\/\/[^\s<>"']+/gi, stash);
  out = out.replace(/\+?\d[\d\s().-]{6,}\d/g, stash);
  return { text: out, tokens };
}

/** Contact route classification for holder vs reach-target. */
export const CONTACT_ROUTE = Object.freeze({
  DIRECT: "DIRECT",
  ASSISTANT_MEDIATED: "ASSISTANT_MEDIATED",
  ORGANIZATIONAL: "ORGANIZATIONAL",
  UNKNOWN: "UNKNOWN",
});

function namePartsMatch(unitOrName, personName) {
  const parts = String(personName || "")
    .toLowerCase()
    .split(/\s+/)
    .filter((p) => p.length > 1);
  if (!parts.length) return false;
  const u = String(unitOrName || "").toLowerCase();
  if (parts.length >= 2) return parts.every((p) => u.includes(p));
  return parts.some((p) => u.includes(p));
}

function normalizePersonLabel(label) {
  return String(label || "")
    .toLowerCase()
    .replace(/[^a-z0-9\s.'-]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Identify whose contact value this is (holder) vs whom it reaches (target),
 * and whether the route is direct, assistant-mediated, organizational, or unknown.
 */
export function analyzeContactAttribution(sourceBody, value, personName = null) {
  const empty = {
    holder: null,
    reach_target: null,
    route: CONTACT_ROUTE.UNKNOWN,
    person_is_holder: false,
    person_is_reach_target: false,
    personally_attributable: false,
  };
  const raw = String(sourceBody || "").toLowerCase();
  const v = String(value || "").trim().toLowerCase();
  if (!v || v.length < 3 || !raw.includes(v)) return empty;

  const { text: protectedAll, tokens } = protectContactTokens(raw);
  let protectedText = protectedAll;
  if (!protectedText.includes(v)) {
    const idxTok = tokens.findIndex((t) => String(t).toLowerCase() === v);
    if (idxTok >= 0) protectedText = protectedAll.replace(`⟦C${idxTok}⟧`, "⟦VALUE⟧");
    else protectedText = protectContactTokens(raw.replace(v, "⟦VALUE⟧")).text;
  } else {
    protectedText = protectedText.replace(v, "⟦VALUE⟧");
  }
  protectedText = protectedText.replace(/\b([a-z])\.(?=\s*[a-z])/g, "$1·");

  const units = protectedText
    .split(/(?:(?<=[.!?])\s+|\n+|;\s+|\|\s*)/)
    .map((u) => u.replace(/·/g, ".").trim())
    .filter(Boolean);
  const idx = units.findIndex((u) => u.includes("⟦VALUE⟧"));
  if (idx < 0) return empty;

  const unit = units[idx];
  const before = unit.split("⟦VALUE⟧")[0];
  const fullWindow = [units[idx - 1], unit, units[idx + 1]].filter(Boolean).join(" ");

  let holder = null;
  let reachTarget = null;
  let route = CONTACT_ROUTE.UNKNOWN;

  // Assistant-mediated: "contact his assistant Bob Jones at EMAIL" / "assistant Bob: EMAIL"
  const asst =
    before.match(
      /\b(?:his |her |their )?(?:assistant|ea|executive assistant)\s+([a-z][a-z.'-]*(?:\s+[a-z][a-z.']*){0,3})\s*(?:at|:)?\s*$/i
    ) ||
    before.match(
      /\b(?:contact|email|reach|call)\s+(?:his |her |their )?(?:assistant|ea)\s+([a-z][a-z.'-]*(?:\s+[a-z][a-z.']*){0,3})\s+(?:at|:)\s*$/i
    );
  if (asst) {
    holder = normalizePersonLabel(asst[1])
      .replace(/\b(?:at|to|for)$/g, "")
      .trim();
    route = CONTACT_ROUTE.ASSISTANT_MEDIATED;
    const forMatch =
      unit.match(/\bfor\s+([a-z][a-z.'-]*(?:\s+[a-z][a-z.']*){0,3})\s*[,:]/i) ||
      fullWindow.match(/\bfor\s+([a-z][a-z.'-]*(?:\s+[a-z][a-z.']*){0,3})\s*[,:]/i);
    if (forMatch) reachTarget = normalizePersonLabel(forMatch[1]);
  }

  // Structural holder: "Name: value" / "Name, Title: value" / "Name at value"
  if (!holder) {
    const contactName = before.match(
      /\b(?:contact|email|reach|call)\s+([a-z][a-z.'-]*(?:\s+[a-z][a-z.']*){0,3})\s+(?:at|:)\s*$/i
    );
    // "J. Rivera, CEO:" / "Alex Rivera:" — capture person before optional title clause
    const nameTitleColon = before.match(
      /([a-z][a-z.'-]*(?:\s+[a-z][a-z.']+){0,3})\s*,\s*[a-z][^:]*[:\-–—]\s*$/i
    );
    const colon = before.match(
      /([a-z][a-z.'-]*(?:\s+[a-z][a-z.']+){0,3})\s*[:\-–—]\s*$/i
    );
    const atName = before.match(
      /([a-z][a-z.'-]*(?:\s+[a-z][a-z.']+){0,3})\s+at\s*$/i
    );
    const forColon = before.match(
      /\bfor\s+([a-z][a-z.'-]*(?:\s+[a-z][a-z.']*){0,3})\s*[:\-–—]\s*$/i
    );
    const picked = contactName || forColon || nameTitleColon || colon || atName;
    if (picked) {
      holder = normalizePersonLabel(picked[1])
        .replace(/\b(?:at|to|for)$/g, "")
        .trim();
      route = CONTACT_ROUTE.DIRECT;
    }
  }

  // Generic org inboxes — only when the holder label itself is a mailbox word
  if (
    holder &&
    /^(info|contact|sales|press|media|hello|office|reception|support|admin|team)$/i.test(holder)
  ) {
    route = CONTACT_ROUTE.ORGANIZATIONAL;
  }

  // Adjacent name-only prior row: only when contact row has NO other person as holder
  if (!holder && idx > 0) {
    const prior = units[idx - 1];
    const stripped = prior
      .replace(/⟦[^⟧]+⟧/g, " ")
      .replace(/[^a-z0-9\s.'/]/gi, " ")
      .replace(/\s+/g, " ")
      .trim();
    const looksNameOnly =
      stripped.length > 0 &&
      stripped.length <= 60 &&
      !/[.!?]$/.test(prior.trim()) &&
      !/@/.test(prior) &&
      !/\b(?:is|was|are|were|ceo|chief|director|president|contact|email|assistant|for)\b/i.test(
        prior
      );
    // Contact row must not already name a different person before the value
    const rowNamesOther = /[a-z]{2,}\s+[a-z]{2,}/i.test(
      before.replace(/⟦[^⟧]+⟧/g, " ").trim()
    );
    if (looksNameOnly && !rowNamesOther) {
      holder = normalizePersonLabel(stripped);
      route = CONTACT_ROUTE.DIRECT;
    }
  }

  const personIsHolder = personName ? namePartsMatch(holder || "", personName) : false;
  const personIsReachTarget = personName
    ? namePartsMatch(reachTarget || "", personName)
    : false;

  // Personal attribution: only the contact holder on a direct route, or the
  // assistant as holder of an assistant-mediated route. Reach-targets and
  // organizational inboxes are not personally attributable.
  const personallyAttributable =
    personIsHolder &&
    (route === CONTACT_ROUTE.DIRECT || route === CONTACT_ROUTE.ASSISTANT_MEDIATED);

  // Same-sentence co-occurrence alone is never enough when a different holder is identified
  if (personName && holder && !personIsHolder) {
    return {
      holder,
      reach_target: reachTarget,
      route,
      person_is_holder: false,
      person_is_reach_target: personIsReachTarget,
      personally_attributable: false,
    };
  }

  // No structural holder: do not fall back to "any name in the unit"
  if (!holder) {
    return {
      holder: null,
      reach_target: reachTarget,
      route: CONTACT_ROUTE.UNKNOWN,
      person_is_holder: false,
      person_is_reach_target: personIsReachTarget,
      personally_attributable: false,
    };
  }

  return {
    holder,
    reach_target: reachTarget,
    route,
    person_is_holder: personIsHolder,
    person_is_reach_target: personIsReachTarget,
    personally_attributable: personallyAttributable,
  };
}

/**
 * Contact value is personally attributable to a person only when they are the
 * identified contact holder (not merely a reach-target or co-mentioned name).
 */
export function contactValuePersonBound(sourceBody, value, personName) {
  if (!personName) {
    const a = analyzeContactAttribution(sourceBody, value, null);
    return Boolean(a.holder);
  }
  return analyzeContactAttribution(sourceBody, value, personName).personally_attributable;
}

/** @deprecated Use ownershipPassageSupport / supported_transaction_claim — not current ownership. */
export function supportedOwnershipEvidence(refs, hotel, owner) {
  return evidenceRecords(refs).filter((r) => ownershipPassageSupport(r.excerpt || r.passage, hotel, owner));
}
export function supportedAffiliationEvidence(refs, person, owner, title) {
  return evidenceRecords(refs).filter((r) => {
    const text = r.excerpt || r.passage;
    return mentions(text, person) && mentions(text, owner) && mentions(text, title) &&
      !/\b(former|formerly|retired|deceased|left the company|ex director|fallecid|falecido)\b/i.test(text);
  });
}
export function domainEvidenceSupported(refs, domain, owner) {
  const host = (v) => { try { return new URL(v.includes('://') ? v : `https://${v}`).hostname.replace(/^www\./,'').toLowerCase(); } catch { return ''; } };
  const target = host(domain || '');
  return Boolean(target) && evidenceRecords(refs).some((r) =>
    host(r.source_url || r.url) === target && mentions(r.excerpt || r.passage, owner));
}
export function classifyContactPurpose(email = '') {
  const local = String(email).split('@')[0];
  if (/media|press|prensa|imprensa|presse/i.test(local)) return 'MEDIA_PURPOSE';
  if (/privacy|derechosarco|sostenibilidad|ouvidoria/i.test(local)) return 'PURPOSE_RESTRICTED';
  return 'UNDETERMINED';
}

