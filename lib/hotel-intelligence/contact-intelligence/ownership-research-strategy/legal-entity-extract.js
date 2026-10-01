/**
 * Deterministic extraction of Brazil (and shared) legal-entity signals from text.
 * Offline / fixture-safe — no network.
 */
import { ENTITY_CANDIDATE_ROLE } from "./constants.js";

/** Placeholder / non-authoritative CNPJ patterns (all zeros, etc.). */
const NON_AUTHORITATIVE_CNPJ_RE =
  /^0{2}\.?0{3}\.?0{3}\/?0{4}-?0{2}$|^00\.000\.000\/0001-00$/i;

const CNPJ_RE =
  /\b(\d{2}\.?\d{3}\.?\d{3}\/?\d{4}-?\d{2})\b/g;

const RAZAO_RE =
  /(?:raz[aã]o\s+social|denomina[cç][aã]o\s+social)\s*[:\-]?\s*([A-ZÁÉÍÓÚÂÊÔÃÕÇ][^.\n|]{3,120})/i;

const NOME_FANTASIA_RE =
  /(?:nome\s+fantasia|nome\s+comercial)\s*[:\-]?\s*([A-ZÁÉÍÓÚÂÊÔÃÕÇ][^.\n|]{3,80})/i;

const STATUS_INACTIVE_RE =
  /\b(baixada|inativa|incorporada|dissolvida|baixado|inativo)\b/i;

const ADMINISTRADORA_RE =
  /\badministradora\s+hoteleira\b|\badministra[cç][aã]o\s+hoteleira\b/i;

const FII_RE = /\bFII\b|fundo\s+de\s+investimento\s+imobili[aá]rio|CVM\b/i;

const CNAE_HOTEL_RE =
  /hoteis|hot[eé]is|hotelaria|pousadas|albergues|CNAE\s*55/i;

/**
 * Normalize a Brazilian CNPJ to digits-only (14) or null.
 * @param {string} raw
 */
export function normalizeCnpj(raw) {
  const digits = String(raw || "").replace(/\D/g, "");
  if (digits.length !== 14) return null;
  return digits;
}

/**
 * Format 14-digit CNPJ as XX.XXX.XXX/XXXX-XX.
 * @param {string} digits
 */
export function formatCnpj(digits) {
  const d = normalizeCnpj(digits);
  if (!d) return null;
  return `${d.slice(0, 2)}.${d.slice(2, 5)}.${d.slice(5, 8)}/${d.slice(8, 12)}-${d.slice(12)}`;
}

export function isNonAuthoritativeCnpj(raw) {
  const formatted = formatCnpj(raw) || String(raw || "").trim();
  if (!formatted) return true;
  if (NON_AUTHORITATIVE_CNPJ_RE.test(formatted)) return true;
  const d = normalizeCnpj(raw);
  return Boolean(d && /^0+$/.test(d));
}

/**
 * Extract CNPJ candidates from free text.
 * @param {string} text
 */
export function extractCnpjCandidates(text) {
  const out = [];
  const seen = new Set();
  const s = String(text || "");
  let m;
  const re = new RegExp(CNPJ_RE.source, "g");
  while ((m = re.exec(s)) !== null) {
    const digits = normalizeCnpj(m[1]);
    if (!digits || seen.has(digits)) continue;
    seen.add(digits);
    out.push({
      cnpj_digits: digits,
      cnpj_formatted: formatCnpj(digits),
      non_authoritative: isNonAuthoritativeCnpj(digits),
      offset: m.index,
    });
  }
  return out;
}

/**
 * Extract legal company name / trade name / status cues.
 * @param {string} text
 */
export function extractLegalEntityHints(text) {
  const s = String(text || "");
  let razao = s.match(RAZAO_RE)?.[1]?.trim() || null;
  const nomeFantasia = s.match(NOME_FANTASIA_RE)?.[1]?.trim() || null;
  // Privacy-policy / bare company-line style: "PARAISO ADMINISTRADORA HOTELEIRA LTDA"
  // without an explicit "Razão Social:" label.
  if (!razao) {
    const bare = s.match(
      /^\s*([A-ZÁÉÍÓÚÂÊÔÃÕÇ0-9][A-Za-zÁÉÍÓÚÂÊÔÃÕÇáéíóúâêôãõç0-9 .,&'\-]{4,100}\b(?:LTDA|Ltda|S\.?\s?A\.?|EIRELI))\s*$/m
    );
    if (bare) razao = bare[1].trim();
  }
  if (!razao) {
    const ctrl = s.match(
      /(?:controladora|denominada|empresa)\s*[:\-]?\s*([A-ZÁÉÍÓÚÂÊÔÃÕÇ0-9][A-Za-zÁÉÍÓÚÂÊÔÃÕÇáéíóúâêôãõç0-9 .,&'\-]{4,100}\b(?:LTDA|Ltda|S\.?\s?A\.?|EIRELI))/i
    );
    if (ctrl) razao = ctrl[1].trim();
  }
  const inactive = STATUS_INACTIVE_RE.test(s);
  const status = inactive
    ? (s.match(STATUS_INACTIVE_RE)?.[1] || "INATIVA").toUpperCase()
    : null;
  return {
    razao_social: razao,
    nome_fantasia: nomeFantasia,
    company_status: status,
    inactive: inactive,
    hotel_cnae_signal: CNAE_HOTEL_RE.test(s),
    administradora_hoteleira: ADMINISTRADORA_RE.test(s),
    fii_or_cvm_signal: FII_RE.test(s),
  };
}

/**
 * Extract QSA / principal name candidates near Brazilian role labels.
 * Names are staged principals — NOT automatic property owners.
 * @param {string} text
 */
export function extractPrincipalCandidates(text) {
  const s = String(text || "");
  const principals = [];
  const seen = new Set();

  function add(nameRaw, role) {
    let name = String(nameRaw || "")
      .replace(/\s+/g, " ")
      .replace(/\s*&.*$/g, "")
      .replace(/\s+\be\b\s+.*$/i, "")
      .replace(/\s*[•·;].*$/i, "")
      .trim();
    // Keep multi-word person names; drop truncated single tokens under 8 chars
    if (name.split(/\s+/).length < 2 && name.length < 10) return;
    if (name.length < 8) return;
    if (!/^[A-ZÁÉÍÓÚÂÊÔÃÕÇ]/.test(name)) return;
    // Reject company-ish tails
    if (/\b(ltda|s\.?a\.?|eireli|cia)\b/i.test(name)) return;
    const key = `${role}|${name.toLowerCase()}`;
    if (seen.has(key)) return;
    seen.add(key);
    principals.push({
      person_name: name,
      corporate_role: role,
      relationship: "PERSON_TO_LEGAL_ENTITY",
      not_automatic_property_owner: true,
    });
  }

  // Line-oriented: Role: Person Name
  const lineRe =
    /^\s*(s[oó]cio[- ]administrador(?:a)?|s[oó]cia[- ]administradora|s[oó]cios?(?:\s+e\s+administradores)?|administrador(?:a|es)?|diretor(?:a|es)?|presidente)\s*[:\-]\s*(.+)$/gim;
  let lm;
  while ((lm = lineRe.exec(s)) !== null) {
    const roleRaw = lm[1]
      .toUpperCase()
      .normalize("NFD")
      .replace(/\p{M}/gu, "")
      .replace(/\s+/g, "_");
    const role = /SOCIO.*ADMINISTRADOR/.test(roleRaw)
      ? "SOCIO_ADMINISTRADOR"
      : /^SOCIOS?$/.test(roleRaw) || roleRaw.startsWith("SOCIO")
        ? "SOCIO"
        : /ADMINISTRADOR/.test(roleRaw)
          ? "ADMINISTRADOR"
          : /DIRETOR/.test(roleRaw)
            ? "DIRETOR"
            : "PRESIDENTE";
    const rhs = lm[2];
    for (const part of rhs.split(/[;|/]/).map((x) => x.trim())) {
      add(part.replace(/\s*[-–—]\s*(s[oó]cio|administrador).*$/i, "").trim(), role);
    }
  }

  // Inline "Sócia-Administradora Name" without newline
  const inlineRe =
    /(s[oó]cio[- ]administrador(?:a)?|s[oó]cia[- ]administradora)\s*[:\-]?\s*([A-ZÁÉÍÓÚÂÊÔÃÕÇ][A-Za-zÁÉÍÓÚÂÊÔÃÕÇáéíóúâêôãõç .']{8,80})/gi;
  let im;
  while ((im = inlineRe.exec(s)) !== null) {
    add(im[2], "SOCIO_ADMINISTRADOR");
  }

  return principals;
}

/**
 * Classify candidate entity role from legal-name / text cues.
 * Never promotes operator/administradora/brand to PROPERTY_OWNER.
 * @param {{ legal_name?: string, text?: string, inactive?: boolean, independent_ownership_evidence?: boolean }} opts
 */
export function classifyEntityCandidateRole(opts = {}) {
  const name = String(opts.legal_name || "");
  const text = `${name} ${opts.text || ""}`;
  const roles = [];

  if (FII_RE.test(text)) roles.push(ENTITY_CANDIDATE_ROLE.REAL_ESTATE_FUND);
  if (ADMINISTRADORA_RE.test(text) || /\bopera[cç][aã]o\s+hoteleira\b/i.test(text)) {
    roles.push(ENTITY_CANDIDATE_ROLE.OPERATING_ENTITY);
    roles.push(ENTITY_CANDIDATE_ROLE.MANAGEMENT_COMPANY);
  }
  if (/\b(marriott|hilton|ihg|wyndham|accor|meli[aá]|ibis|tryp|caesar)\b/i.test(text) &&
      /\b(brand|franchise|bandeira|marca)\b/i.test(text)) {
    roles.push(ENTITY_CANDIDATE_ROLE.BRAND_FRANCHISOR);
  }
  if (/\b(incorporadora|incorporaç|desenvolvedor|developer)\b/i.test(text)) {
    roles.push(ENTITY_CANDIDATE_ROLE.DEVELOPER);
  }
  if (/\binvestidor|investimento\b/i.test(text) && !FII_RE.test(text)) {
    roles.push(ENTITY_CANDIDATE_ROLE.INVESTOR);
  }
  if (opts.inactive) {
    if (roles.includes(ENTITY_CANDIDATE_ROLE.OPERATING_ENTITY)) {
      roles.push(ENTITY_CANDIDATE_ROLE.HISTORICAL_OPERATOR);
    } else {
      roles.push(ENTITY_CANDIDATE_ROLE.HISTORICAL_OWNER);
    }
  }

  if (
    opts.independent_ownership_evidence === true &&
    !roles.includes(ENTITY_CANDIDATE_ROLE.OPERATING_ENTITY) &&
    !roles.includes(ENTITY_CANDIDATE_ROLE.MANAGEMENT_COMPANY) &&
    !roles.includes(ENTITY_CANDIDATE_ROLE.BRAND_FRANCHISOR)
  ) {
    roles.push(ENTITY_CANDIDATE_ROLE.PROPERTY_OWNER);
  }

  if (!roles.length) {
    // Generic Ltda / SA with hotel CNAE → operating/legal entity candidate, not owner.
    if (/\b(ltda|s\.?a\.?|eireli)\b/i.test(text) || CNAE_HOTEL_RE.test(text)) {
      roles.push(ENTITY_CANDIDATE_ROLE.OPERATING_ENTITY);
    } else {
      roles.push(ENTITY_CANDIDATE_ROLE.UNKNOWN_ROLE);
    }
  }

  return [...new Set(roles)];
}

/**
 * Full extraction pass over a document or SERP snippet.
 * @param {string} text
 * @param {{ source_url?: string, observed_at?: string }} meta
 */
export function extractLegalEntityBundle(text, meta = {}) {
  const cnpjs = extractCnpjCandidates(text).filter((c) => !c.non_authoritative);
  const hints = extractLegalEntityHints(text);
  const principals = extractPrincipalCandidates(text);
  const primaryCnpj = cnpjs[0] || null;
  const legalName = hints.razao_social || null;
  const roles = classifyEntityCandidateRole({
    legal_name: legalName || hints.nome_fantasia || "",
    text,
    inactive: hints.inactive,
    independent_ownership_evidence: false,
  });

  return {
    cnpjs,
    primary_cnpj: primaryCnpj,
    legal_name: legalName,
    trade_name: hints.nome_fantasia,
    company_status: hints.company_status,
    inactive: hints.inactive,
    hotel_cnae_signal: hints.hotel_cnae_signal,
    roles,
    principals,
    source_url: meta.source_url || null,
    observed_at: meta.observed_at || null,
    successor_research_required: hints.inactive === true,
    titled_property_ownership_claimed: false,
  };
}
