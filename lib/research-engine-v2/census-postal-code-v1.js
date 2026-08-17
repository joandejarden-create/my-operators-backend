/**
 * Postal Code — schema ensure + country-aware extract/normalize for Hotel Property Census.
 * Field: singleLineText "Postal Code". NULL_FILL only. Never invent / never placeholders.
 */
import {
  resolvePat,
  resolveTargetBase,
} from "./production-census-schema-create.js";
import {
  assertProductionCensusWriteTarget,
  PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
} from "./production-census-source-of-truth.js";

export const POSTAL_CODE_FIELD = "Postal Code";
export const POSTAL_CODE_MODULE_VERSION = "census-postal-code-v1";

/** Alias names that mean the same production field — reuse if present. */
export const POSTAL_CODE_ALIASES = Object.freeze([
  "Postal Code",
  "ZIP Code",
  "Zip Code",
  "Zip",
  "ZIP",
  "Postal",
  "Postcode",
  "Post Code",
  "Código Postal",
  "Codigo Postal",
  "Código ZIP",
  "Codigo ZIP",
]);

/** Placeholder / fabricated values — never write. */
const PLACEHOLDER_RE =
  /^(n\/?a|na|none|unknown|null|undefined|0+|x+|-+|\.+)$/i;

/**
 * Countries where postal systems are commonly used in hotel addresses.
 * Others: extract only when a strong country-specific pattern matches; else leave blank.
 */
export const POSTAL_COMMONLY_USED = Object.freeze(
  new Set([
    "Mexico",
    "Brazil",
    "Colombia",
    "Argentina",
    "Chile",
    "Peru",
    "Puerto Rico",
    "Costa Rica",
    "Dominican Republic",
    "Uruguay",
    "Ecuador",
    "Paraguay",
    "Guatemala",
    "Honduras",
    "El Salvador",
    "Panama",
    "Cuba",
    "United States",
    "Canada",
    "Cayman Islands",
    "Barbados",
  ])
);

/** Territories where postal is often absent / not applicable — do not force completeness. */
export const POSTAL_LOW_OR_NA_COVERAGE = Object.freeze(
  new Set([
    "Jamaica",
    "Bahamas",
    "Aruba",
    "Curaçao",
    "Curacao",
    "Bonaire",
    "Sint Maarten",
    "Saint Martin",
    "Anguilla",
    "Antigua and Barbuda",
    "Dominica",
    "Grenada",
    "Saint Lucia",
    "Saint Kitts and Nevis",
    "Saint Vincent and the Grenadines",
    "Trinidad and Tobago",
    "Turks and Caicos Islands",
    "British Virgin Islands",
    "US Virgin Islands",
    "Bermuda",
    "Belize",
    "Suriname",
    "Guyana",
    "Haiti",
    "Nicaragua",
  ])
);

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function isBlank(v) {
  return v == null || String(v).trim() === "";
}

/**
 * @param {string} country
 */
export function normalizeCountryKey(country) {
  return String(country || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .trim();
}

/**
 * Reject fabricated / empty placeholders.
 * @param {string} raw
 */
export function isPostalPlaceholder(raw) {
  const s = String(raw || "").trim();
  if (!s) return true;
  if (PLACEHOLDER_RE.test(s)) return true;
  // all zeros with optional hyphen
  if (/^0+(-0+)?$/.test(s)) return true;
  return false;
}

/**
 * Normalize presentation only — preserve leading zeros, letters, hyphens.
 * @param {string} raw
 * @param {string} [country]
 */
export function normalizePostalCode(raw, country = "") {
  if (isBlank(raw) || isPostalPlaceholder(raw)) return null;
  let s = String(raw).trim().replace(/\s+/g, " ");
  const ck = normalizeCountryKey(country);

  if (ck === "brazil") {
    const digits = s.replace(/\D/g, "");
    if (digits.length === 8) return `${digits.slice(0, 5)}-${digits.slice(5)}`;
    if (/^\d{5}-\d{3}$/.test(s)) return s;
  }

  if (ck === "mexico" || ck === "peru" || ck === "costa rica" || ck === "uruguay") {
    const digits = s.replace(/\D/g, "");
    if (digits.length === 5) return digits; // keep leading zeros via digit string
  }

  if (ck === "puerto rico" || ck === "united states" || ck === "us virgin islands") {
    const m = s.match(/^(\d{5})(?:[-\s]?(\d{4}))?$/);
    if (m) return m[2] ? `${m[1]}-${m[2]}` : m[1];
  }

  if (ck === "argentina") {
    // CPA: letter + 4 digits + optional 3 letters
    const m = s.toUpperCase().match(/^([A-Z])\s?(\d{4})\s?([A-Z]{0,3})$/);
    if (m) return m[3] ? `${m[1]}${m[2]}${m[3]}` : `${m[1]}${m[2]}`;
    if (/^\d{4}$/.test(s)) return s;
  }

  if (ck === "cayman islands") {
    const m = s.toUpperCase().match(/^(KY\d)\s*[-–]?\s*(\d{4})$/);
    if (m) return `${m[1]}-${m[2]}`;
  }

  if (ck === "barbados") {
    const m = s.toUpperCase().match(/^(BB)?\s?(\d{5})$/);
    if (m) return m[1] ? `BB${m[2]}` : m[2];
  }

  // Generic: collapse spaces, keep hyphens/letters/digits
  s = s.replace(/\s*-\s*/g, "-").trim();
  if (isPostalPlaceholder(s)) return null;
  return s;
}

/**
 * Validate candidate against country rules.
 * @param {string} value
 * @param {string} country
 */
export function isValidPostalForCountry(value, country) {
  const n = normalizePostalCode(value, country);
  if (!n) return false;
  const ck = normalizeCountryKey(country);

  const rules = {
    mexico: /^\d{5}$/,
    brazil: /^\d{5}-\d{3}$/,
    colombia: /^\d{6}$/,
    argentina: /^([A-Z]\d{4}[A-Z]{0,3}|\d{4})$/,
    chile: /^\d{7}$/,
    peru: /^\d{5}$/,
    "puerto rico": /^\d{5}(-\d{4})?$/,
    "united states": /^\d{5}(-\d{4})?$/,
    "costa rica": /^\d{5}$/,
    "dominican republic": /^\d{5}$/,
    uruguay: /^\d{5}$/,
    ecuador: /^\d{6}$/,
    paraguay: /^\d{4}$/,
    guatemala: /^\d{5}$/,
    honduras: /^\d{5}$/,
    "el salvador": /^\d{4}$/,
    panama: /^\d{4,6}$/,
    cuba: /^\d{5}$/,
    venezuela: /^\d{4}$/,
    bolivia: /^\d{4}$/,
    "cayman islands": /^KY\d-\d{4}$/,
    barbados: /^(BB)?\d{5}$/,
    canada: /^[A-Z]\d[A-Z][ -]?\d[A-Z]\d$/i,
  };

  const re = rules[ck];
  if (re) return re.test(n);
  // Unknown country: accept conservative alphanumeric 3–12 chars with optional hyphen
  return /^[A-Z0-9][A-Z0-9 -]{1,11}$/i.test(n) && /\d/.test(n);
}

/**
 * Extract postal from a free-text address using country-specific patterns.
 * @param {string} address
 * @param {string} country
 * @returns {{ ok: boolean, postal_code: string|null, method?: string, reason?: string }}
 */
export function extractPostalFromAddress(address, country) {
  const addr = String(address || "").trim();
  if (!addr) return { ok: false, postal_code: null, reason: "blank_address" };
  const ck = normalizeCountryKey(country);

  /** @type {{ re: RegExp, group?: number, method: string }[]} */
  let patterns = [];

  if (ck === "mexico") {
    patterns = [
      {
        re: /,\s*(\d{5})\s+[A-Za-zÀ-ÿ][^,]{0,40},\s*(?:[A-Z]\.?[A-Z]\.?S\.?|[A-Za-zÀ-ÿ]{3,})/u,
        method: "mx_cp_before_locality_state",
      },
      {
        re: /\b(\d{5})\s+(?:San José|San Jose|Ciudad |Tijuana|Cancún|Cancun|México|Mexico|Guadalajara|Monterrey|[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+)/u,
        method: "mx_cp_before_place",
      },
      {
        re: /,\s*(\d{5})\s+[A-Za-zÀ-ÿ]/u,
        method: "mx_cp_comma_locality",
      },
    ];
  } else if (ck === "brazil") {
    patterns = [
      { re: /\b(\d{5}-\d{3})\b/, method: "br_cep_hyphen" },
      { re: /\b(\d{5})\s*(\d{3})\b/, method: "br_cep_split", group: -1 },
      { re: /\b(\d{8})\b/, method: "br_cep_digits" },
    ];
  } else if (ck === "colombia") {
    patterns = [{ re: /\b(\d{6})\b/, method: "co_6digit" }];
  } else if (ck === "argentina") {
    patterns = [
      { re: /\b([A-Z]\d{4}[A-Z]{3})\b/i, method: "ar_cpa_full" },
      { re: /\b([A-Z]\d{4})\b(?=\s|,|$)/i, method: "ar_cpa_short" },
    ];
  } else if (ck === "chile") {
    patterns = [{ re: /\b(\d{7})\b/, method: "cl_7digit" }];
  } else if (ck === "peru") {
    patterns = [{ re: /\b(\d{5})\b(?=\s*(?:,|Peru|Perú|$))/i, method: "pe_5digit" }];
  } else if (ck === "puerto rico") {
    patterns = [
      { re: /\b(00\d{3})(?:-(\d{4}))?\b/, method: "pr_zip_leading_zeros" },
      { re: /\b(\d{5})(?:-(\d{4}))?\b/, method: "pr_zip" },
    ];
  } else if (ck === "costa rica") {
    patterns = [{ re: /\b(\d{5})\b/, method: "cr_5digit" }];
  } else if (ck === "dominican republic") {
    patterns = [{ re: /\b(\d{5})\b/, method: "do_5digit" }];
  } else if (ck === "uruguay") {
    patterns = [{ re: /\b(\d{5})\b/, method: "uy_5digit" }];
  } else if (ck === "ecuador") {
    patterns = [{ re: /\b(\d{6})\b/, method: "ec_6digit" }];
  } else if (ck === "paraguay") {
    patterns = [{ re: /\b(\d{4})\b(?=\s|$)/, method: "py_4digit" }];
  } else if (ck === "guatemala" || ck === "honduras") {
    patterns = [{ re: /\b(\d{5})\b/, method: "gt_hn_5digit" }];
  } else if (ck === "el salvador" || ck === "venezuela" || ck === "bolivia") {
    patterns = [{ re: /\b(\d{4})\b(?=\s|$|,)/, method: "sv_ve_bo_4digit" }];
  } else if (ck === "panama") {
    // Prefer 4–6 digit tokens that are not all zeros; avoid house numbers mid-street when possible
    patterns = [
      { re: /,\s*(\d{4,6})\s+Panama\b/i, method: "pa_before_country" },
      { re: /\bPanama City\s+(\d{4,6})\b/i, method: "pa_after_city" },
    ];
  } else if (ck === "cuba") {
    patterns = [{ re: /\b(\d{5})\b/, method: "cu_5digit" }];
  } else if (ck === "cayman islands") {
    patterns = [{ re: /\b(KY\d)\s*[-–]?\s*(\d{4})\b/i, method: "ky_postal" }];
  } else if (ck === "barbados") {
    patterns = [{ re: /\b(?:BB)?\s?(\d{5})\b/i, method: "bb_postal" }];
  } else if (ck === "canada") {
    patterns = [
      {
        re: /\b([A-Z]\d[A-Z])\s?(\d[A-Z]\d)\b/i,
        method: "ca_fsa_ldu",
      },
    ];
  } else {
    // Conservative generic: 4–8 digit token near end of address
    patterns = [
      { re: /,\s*(\d{4,8})\s*[A-Za-zÀ-ÿ]*\s*$/, method: "generic_trailing_digits" },
    ];
  }

  for (const p of patterns) {
    const matches = [...addr.matchAll(new RegExp(p.re.source, p.re.flags.includes("g") ? p.re.flags : `${p.re.flags}g`))];
    // Prefer the last match — postal codes usually appear near the end of an address
    const m = matches.length ? matches[matches.length - 1] : null;
    if (!m) continue;
    let raw;
    if (p.method === "br_cep_split" && m[1] && m[2]) raw = `${m[1]}${m[2]}`;
    else if (p.method === "ky_postal" && m[1] && m[2]) raw = `${m[1]}-${m[2]}`;
    else if (p.method === "ca_fsa_ldu" && m[1] && m[2]) raw = `${m[1].toUpperCase()} ${m[2].toUpperCase()}`;
    else if (p.method === "pr_zip" || p.method === "pr_zip_leading_zeros") {
      raw = m[2] ? `${m[1]}-${m[2]}` : m[1];
    } else if (p.method === "bb_postal") {
      raw = m[1];
    } else {
      raw = m[1];
    }
    const normalized = normalizePostalCode(raw, country);
    if (!normalized || !isValidPostalForCountry(normalized, country)) continue;
    // Extra: reject street numbers mistaken as postal when alone early in address
    if (/^(calle|av\.?|avenida|rua|r\.|carrera|cl\.|#)\b/i.test(addr) && addr.indexOf(raw) < 12) {
      // still ok if pattern was country-anchored
      if (!/before_country|after_city|cpa|cep|zip|cp_before/i.test(p.method)) continue;
    }
    return {
      ok: true,
      postal_code: normalized,
      method: p.method,
      source: "address_parse",
    };
  }

  return { ok: false, postal_code: null, reason: "no_country_pattern_match" };
}

/**
 * Compare postal codes for identity / dedupe.
 * @returns {'match'|'mismatch'|'insufficient'}
 */
export function comparePostalIdentitySignal(a, b, country = "") {
  const na = normalizePostalCode(a, country);
  const nb = normalizePostalCode(b, country);
  if (!na || !nb) return "insufficient";
  const ca = na.replace(/\s+/g, "").toUpperCase();
  const cb = nb.replace(/\s+/g, "").toUpperCase();
  if (ca === cb) return "match";
  // ZIP+4 vs ZIP5: treat base match as match
  const base = (s) => s.split("-")[0];
  if (base(ca) === base(cb) && base(ca).length >= 4) return "match";
  return "mismatch";
}

/**
 * Adjust a 0–100 linkage score using postal signal (never sole match).
 * @param {number} score
 * @param {'match'|'mismatch'|'insufficient'} signal
 */
export function applyPostalToIdentityScore(score, signal) {
  let s = Number(score) || 0;
  if (signal === "match") s = Math.min(100, s + 12);
  if (signal === "mismatch") s = Math.max(0, s - 18);
  return s;
}

/**
 * Inspect live table for Postal Code or alias.
 * @param {{ fields?: { name: string, type: string, id: string }[] }} table
 */
export function resolvePostalFieldFromSchema(table) {
  const fields = table?.fields || [];
  for (const alias of POSTAL_CODE_ALIASES) {
    const hit = fields.find((f) => f.name === alias);
    if (hit) {
      return {
        exists: true,
        field_name: hit.name,
        field_id: hit.id,
        field_type: hit.type,
        reused_alias: hit.name !== POSTAL_CODE_FIELD,
      };
    }
  }
  return {
    exists: false,
    field_name: POSTAL_CODE_FIELD,
    field_id: null,
    field_type: null,
    reused_alias: false,
  };
}

/**
 * Ensure Postal Code singleLineText exists on Hotel Property Census only.
 * @param {{ apply?: boolean, token?: string, baseId?: string, log?: Function }} opts
 */
export async function ensurePostalCodeField(opts = {}) {
  const log = opts.log || console.log;
  const token = opts.token || resolvePat();
  const base = resolveTargetBase();
  const baseId = opts.baseId || base?.target_base_id || base?.baseId;
  assertProductionCensusWriteTarget({
    baseId,
    tableId: PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
  });

  const metaUrl = `https://api.airtable.com/v0/meta/bases/${encodeURIComponent(baseId)}/tables`;
  const res = await fetch(metaUrl, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const json = await res.json();
  if (!res.ok) {
    throw new Error(`meta_tables_failed:${res.status}:${json?.error?.message || ""}`);
  }
  const table = (json.tables || []).find(
    (t) => t.id === PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID
  );
  if (!table) throw new Error("hotel_property_census_not_found");

  const resolved = resolvePostalFieldFromSchema(table);
  if (resolved.exists) {
    return {
      ok: true,
      created: false,
      POSTAL_CODE_FIELD_STATUS: resolved.reused_alias
        ? "reused_existing_alias"
        : "already_exists",
      POSTAL_CODE_FIELD_TYPE: resolved.field_type,
      field_name: resolved.field_name,
      field_id: resolved.field_id,
      WRONG_TABLE_WRITES: 0,
    };
  }

  if (!opts.apply) {
    return {
      ok: true,
      created: false,
      dry_run: true,
      POSTAL_CODE_FIELD_STATUS: "missing_would_create",
      POSTAL_CODE_FIELD_TYPE: "singleLineText",
      field_name: POSTAL_CODE_FIELD,
      WRONG_TABLE_WRITES: 0,
    };
  }

  const createUrl = `https://api.airtable.com/v0/meta/bases/${encodeURIComponent(baseId)}/tables/${encodeURIComponent(PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID)}/fields`;
  let created = null;
  for (let attempt = 1; attempt <= 4; attempt += 1) {
    const cres = await fetch(createUrl, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        name: POSTAL_CODE_FIELD,
        type: "singleLineText",
        description:
          "Property postal / ZIP code (text). Preserve leading zeros; country-specific formats. NULL_FILL Property Fundamentals.",
      }),
    });
    const cjson = await cres.json();
    if (cres.status === 429) {
      await sleep(1000 * attempt);
      continue;
    }
    if (!cres.ok) {
      throw new Error(
        `postal_field_create_failed:${cres.status}:${cjson?.error?.message || JSON.stringify(cjson)}`
      );
    }
    created = cjson;
    break;
  }

  log(`[postal] created field ${POSTAL_CODE_FIELD} on Hotel Property Census`);
  return {
    ok: true,
    created: true,
    POSTAL_CODE_FIELD_STATUS: "created",
    POSTAL_CODE_FIELD_TYPE: "singleLineText",
    field_name: POSTAL_CODE_FIELD,
    field_id: created?.id || null,
    WRONG_TABLE_WRITES: 0,
  };
}
