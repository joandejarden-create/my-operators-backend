/**
 * Adaptive V2.1 — candidate quality gate + entity-name extraction.
 * HIGH RECALL without obvious garbage. Speculative OK; garbage not.
 */

const LEGAL_SUFFIX_RE =
  /\b(ltda\.?|l\.?t\.?d\.?a\.?|s\.?\s*a\.?|sa|s\/a|llc|l\.?p\.?|inc\.?|corp\.?|spe|spv|eireli|mei)\b/i;

const FUND_RE = /\b(fii|fundo(?:\s+imobili[aá]rio)?|reit|cvm)\b/i;

const ROLE_HINT_RE =
  /\b(raz[aã]o\s+social|empresa|administradora|propriet[aá]rio|investidor|sponsor|owner|developer|desenvolvedor|holding|controladora|s[oó]cio|qsa)\b/i;

const FIELD_LABEL_RE =
  /^(c[oó]digo(\s+hotel)?|endere[cç]o|estado|cidade|munic[ií]pio|cep|nome\s+fantasia|data\s+da\s+abertura|cnpj|hotel\s*code|status|situa[cç][aã]o)\b/i;

const METADATA_NOISE_RE =
  /\*|^\s*[-–—|:;/]+\s*$|field\s*label|navigation|cookie|privacy\s*policy|table\s*header|nome\s*fantasia\s*:/i;

const SENTENCE_VERB_RE =
  /\b(foi|tem|est[aá]|possu[ei]|pertencente|pertence|antes|depois|cadastrada|fundada|localizada)\b/i;

const GENERIC_SINGLE =
  /^(hotel|hotels|pousada|resort|empresa|owner|owners|company|grupo|group|the|a|an|da|do|de|e|ou|and|or)\b$/i;

const PERSON_NAME_RE =
  /^[A-ZÁÉÍÓÚÂÊÔÃÕÇ][a-záéíóúâêôãõç]+(?:\s+(?:de|da|do|dos|das|e)\s+[A-ZÁÉÍÓÚÂÊÔÃÕÇ][a-záéíóúâêôãõç]+|\s+[A-ZÁÉÍÓÚÂÊÔÃÕÇ][a-záéíóúâêôãõç]+){1,4}$/;

/**
 * Extract a clean entity/legal name from registry or claim prose.
 * @returns {string|null}
 */
export function extractEntityNameFromProse(raw = "") {
  const text = String(raw || "").replace(/\s+/g, " ").trim();
  if (!text) return null;

  // "Pousada X LTDA ; Nome Fantasia: ..." or "Pousada X Ltda, tem o nome..."
  const beforeDelimiter = text.split(/\s*[;|]\s*|\s*,\s*(?=tem\b|possui\b|nome\s+fantasia)/i)[0];
  let candidate = beforeDelimiter.trim();

  // Prefer segment containing legal suffix
  const ltdaMatch = text.match(
    /([A-ZÁÉÍÓÚÂÊÔÃÕÇ0-9][\wÁÉÍÓÚÂÊÔÃÕÇáéíóúâêôãõç0-9 .&'’-]{2,80}?\b(?:LTDA\.?|Ltda\.?|S\.?A\.?|EIRELI|SPE|SPV)\b)/i
  );
  if (ltdaMatch) {
    candidate = ltdaMatch[1].trim();
  }

  // Strip trailing registry prose after legal suffix
  candidate = candidate
    .replace(/\b(foi|tem|est[aá]|possu[ei]|cadastrad[oa]|fundad[oa]|localizad[oa]).*$/i, "")
    .replace(/\s*[;,:)]+$/, "")
    .replace(/^[(\[]+/, "")
    .replace(/\s+/g, " ")
    .trim();

  // "Nome Fantasia: X" alone
  const fantasia = text.match(/nome\s+fantasia\s*:\s*([^;|]+)/i);
  if ((!candidate || candidate.length < 4) && fantasia) {
    candidate = fantasia[1].trim();
  }

  if (candidate.length > 90) {
    // Too long — try first clause only
    candidate = candidate.split(/\s+[-–—]\s+/)[0].slice(0, 90).trim();
  }

  return candidate.length >= 3 ? candidate : null;
}

/**
 * Qualify whether a string may enter the durable candidate store.
 * @returns {{ ok: boolean, reason?: string, cleaned_name?: string, signals?: object }}
 */
export function qualifyOwnerCandidateName(rawName = {}, ctx = {}) {
  const raw = typeof rawName === "string" ? rawName : rawName?.entity_name || "";
  const extracted = extractEntityNameFromProse(raw) || String(raw || "").trim();
  const name = extracted.replace(/\s+/g, " ").trim();

  if (!name || name.length < 3) {
    return { ok: false, reason: "TOO_SHORT", cleaned_name: null };
  }
  if (name.length > 100) {
    return { ok: false, reason: "TOO_LONG_SENTENCE", cleaned_name: null };
  }
  if (GENERIC_SINGLE.test(name)) {
    return { ok: false, reason: "GENERIC_NOUN", cleaned_name: null };
  }
  if (FIELD_LABEL_RE.test(name) || /c[oó]digo\s+hotel/i.test(name)) {
    return { ok: false, reason: "FIELD_LABEL_OR_METADATA", cleaned_name: null };
  }
  if (METADATA_NOISE_RE.test(name) || /\*\s*,\s*\*/.test(name)) {
    return { ok: false, reason: "SCRAPE_BOILERPLATE_OR_PLACEHOLDER", cleaned_name: null };
  }
  // Address-only without entity
  if (
    /\b(rua|av\.|avenida|cep|munic[ií]pio)\b/i.test(name) &&
    !LEGAL_SUFFIX_RE.test(name) &&
    !ROLE_HINT_RE.test(name)
  ) {
    return { ok: false, reason: "ADDRESS_WITHOUT_ENTITY", cleaned_name: null };
  }
  // Partial CNPJ prose without name
  if (/^da\s+empresa\s+de\s+cnpj/i.test(name) || /^cnpj\s*\d/i.test(name)) {
    return { ok: false, reason: "PARTIAL_REGISTRY_PROSE", cleaned_name: null };
  }
  // Sentence fragment with verbs and no legal/fund/person structure
  const wordCount = name.split(/\s+/).length;
  if (SENTENCE_VERB_RE.test(name) && !LEGAL_SUFFIX_RE.test(name) && !FUND_RE.test(name)) {
    if (!PERSON_NAME_RE.test(name) || wordCount > 6) {
      return { ok: false, reason: "SENTENCE_FRAGMENT", cleaned_name: null };
    }
  }
  // Excessive punctuation / noise ratio
  const alnum = (name.match(/[A-Za-zÁÉÍÓÚÂÊÔÃÕÇáéíóúâêôãõç0-9]/g) || []).length;
  if (alnum / Math.max(name.length, 1) < 0.55) {
    return { ok: false, reason: "EXCESSIVE_NOISE_CHARS", cleaned_name: null };
  }
  // Initials-heavy fragments (e.g. "L R de Camargo") when not a clean person name
  if (/^[A-Z]\s+[A-Z]\b/.test(name) && !LEGAL_SUFFIX_RE.test(name) && !PERSON_NAME_RE.test(name)) {
    return { ok: false, reason: "INITIALS_FRAGMENT", cleaned_name: null };
  }

  const signals = {
    legal_suffix: LEGAL_SUFFIX_RE.test(name),
    fund_terminology: FUND_RE.test(name),
    role_hint: ROLE_HINT_RE.test(name),
    person_shape: PERSON_NAME_RE.test(name),
    has_cnpj: Boolean(ctx.cnpj) || /\b\d{2}\.?\d{3}\.?\d{3}\/?\d{4}-?\d{2}\b/.test(raw),
    multi_token: wordCount >= 2,
  };

  // Accept: legal/fund, multi-token org shape, or plausible person with role context
  if (signals.legal_suffix || signals.fund_terminology || signals.has_cnpj) {
    return { ok: true, reason: "LEGAL_OR_FUND_OR_CNPJ", cleaned_name: name, signals };
  }
  if (signals.multi_token && /^[A-ZÁÉÍÓÚ0-9]/.test(name) && wordCount <= 8) {
    return { ok: true, reason: "ENTITY_NAME_SHAPE", cleaned_name: name, signals };
  }
  if (signals.person_shape && (ctx.allow_person === true || ROLE_HINT_RE.test(String(ctx.role_evidence || "")))) {
    return { ok: true, reason: "PLAUSIBLE_PERSON", cleaned_name: name, signals };
  }
  if (signals.role_hint && signals.multi_token) {
    return { ok: true, reason: "ROLE_HINT_ENTITY", cleaned_name: name, signals };
  }

  // Single-token non-generic brand-like (e.g. Accor) — allow only with strong ctx
  if (wordCount === 1 && name.length >= 4 && ctx.strong_source === true) {
    return { ok: true, reason: "STRONG_SOURCE_SINGLE_TOKEN", cleaned_name: name, signals };
  }

  return { ok: false, reason: "INSUFFICIENT_ENTITY_SIGNALS", cleaned_name: name, signals };
}

export {
  LEGAL_SUFFIX_RE,
  FUND_RE,
  PERSON_NAME_RE,
};
