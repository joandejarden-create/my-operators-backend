/**
 * Peru MINCETUR — Establecimientos de Hospedaje Calificados open-data adapter (dry-run first).
 *
 * Source CSV: https://www.mincetur.gob.pe/Datos_abiertos/DGPDT/Establecimientos_hospedajes_calificados.csv
 * Catalog: https://www.datosabiertos.gob.pe/dataset/directorio-nacional-de-prestadores-de-servicios-turisticos-calificados
 *
 * Ownership: RUC is captured as ownership_signal only — never Owner Name.
 */

import { createHash } from "node:crypto";
import { AUTOPILOT_FORBIDDEN_FIELDS } from "./census-autopilot-field-allowlist.js";

export const PERU_MINCETUR_ADAPTER_VERSION = "peru-mincetur-open-data-adapter-v1";

export const MAP_PERU_MINCETUR = Object.freeze({
  sourceDatasetUrl:
    "https://www.datosabiertos.gob.pe/dataset/directorio-nacional-de-prestadores-de-servicios-turisticos-calificados",
  sourceCsvUrl:
    "https://www.mincetur.gob.pe/Datos_abiertos/DGPDT/Establecimientos_hospedajes_calificados.csv",
  country: "Peru",
  familySourceFamily: "Government — Peru MINCETUR",
  sourceType: "government_open_data_mincetur",
  // CSV columns (semicolon-delimited)
  fechaCorte: "FECHA_CORTE",
  camas: "CAMA",
  habitaciones: "HABI",
  ruc: "RUC",
  razonSocial: "RAZON_SOCIAL",
  nombreComercial: "NOMBRE_COMERCIAL",
  departamento: "DEPARTAMENTO",
  provincia: "PROVINCIA",
  distrito: "DISTRITO",
  email: "E_MAIL",
  paginaWeb: "PAGINA_WEB",
  clase: "CLASE",
  categoria: "CATEGORIA",
  nroCertificado: "NRO_CERTIFICADO",
  repLegal: "REP_LEGAL",
  via: "VIA",
  desVia: "DES_VIA",
  numero: "NUMERO",
  // Census targets
  propertyName: "Property Name",
  city: "City",
  stateRegion: "State / Region",
  countryField: "Country",
  address: "Address",
  roomsKeys: "Rooms / Keys",
  roomsConfidence: "Rooms Confidence",
  roomsSourceUrl: "Rooms Source URL",
  roomsSourceType: "Rooms Source Type",
  sourceUrl: "Source URL",
  officialPropertyUrl: "Official Property URL",
  familySource: "Family / Source Family",
  sourceTypeField: "Source Type",
  sourceConfidence: "Source Confidence",
  identityConfidence: "Identity Confidence",
  propertyIdentityKey: "Property Identity Key",
  propertyType: "Property Type",
  productionUseStatus: "Production Use Status",
  enrichmentStatus: "Enrichment Status",
  humanReviewRequired: "Human Review Required",
  phone: "Phone",
});

export const PERU_MINCETUR_DEFAULT_CLASSES = Object.freeze([
  "HOTEL",
  "APART HOTEL",
  "RESORT",
]);

export const PERU_MINCETUR_HOSTEL_LIKE = Object.freeze([
  "HOSTAL",
  "ALBERGUE",
]);

export const PERU_MINCETUR_ROOMS_SANITY_MAX = 1200;

/**
 * @param {string|number|null|undefined} value
 */
export function normalizePeruText(value) {
  return String(value || "")
    .normalize("NFKC")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * @param {string} value
 */
export function titleCasePeruPlace(value) {
  const s = normalizePeruText(value);
  if (!s) return "";
  return s
    .toLowerCase()
    .split(" ")
    .map((w) => (w ? w[0].toUpperCase() + w.slice(1) : w))
    .join(" ");
}

/**
 * @param {string} raw
 */
export function normalizePeruWebsite(raw) {
  let s = normalizePeruText(raw);
  if (!s) return null;
  s = s.replace(/^\/+:/, "");
  if (!/^https?:\/\//i.test(s)) s = `https://${s}`;
  try {
    const u = new URL(s);
    if (!/^https?:$/i.test(u.protocol)) return null;
    if (!u.hostname || u.hostname.length < 3) return null;
    // Reject datos.gov / mincetur catalog hosts as "official property"
    if (/datosabiertos\.gob\.pe|mincetur\.gob\.pe|gob\.pe$/i.test(u.hostname) && !/hotel/i.test(u.hostname)) {
      // allow only if clearly a hotel subdomain — otherwise skip
      if (/mincetur|datosabiertos/i.test(u.hostname)) return null;
    }
    return u.toString().replace(/\/$/, "");
  } catch {
    return null;
  }
}

/**
 * @param {string} cert
 * @param {string} ruc
 * @param {string} name
 * @param {string} district
 */
export function buildPeruMinceturIdentityKey(cert, ruc, name, district) {
  const c = normalizePeruText(cert).replace(/[^\dA-Za-z_-]/g, "");
  if (c) return `gov_pe_mincetur_${c}`;
  const r = normalizePeruText(ruc).replace(/\D/g, "");
  const slug = createHash("sha1")
    .update(`${normalizePeruText(name).toLowerCase()}|${normalizePeruText(district).toLowerCase()}`)
    .digest("hex")
    .slice(0, 10);
  if (r) return `gov_pe_mincetur_${r}_${slug}`;
  return `gov_pe_mincetur_${slug}`;
}

/**
 * Simple semicolon CSV parse (MINCETUR export has no quoted fields in practice).
 * @param {string} text
 */
export function parsePeruMinceturCsv(text) {
  const lines = String(text || "")
    .split(/\r?\n/)
    .map((l) => l.trimEnd())
    .filter((l) => l.length > 0);
  if (!lines.length) return { header: [], rows: [] };
  const header = lines[0].split(";").map((h) => h.trim());
  const rows = [];
  for (const line of lines.slice(1)) {
    const cols = line.split(";");
    /** @type {Record<string, string>} */
    const obj = {};
    for (let i = 0; i < header.length; i += 1) {
      obj[header[i]] = cols[i] != null ? String(cols[i]).trim() : "";
    }
    // Repair shifted rows where CLASE looks like a URL
    const clase = obj[MAP_PERU_MINCETUR.clase] || "";
    if (/^https?:|^www\.|\/\//i.test(clase)) {
      if (!obj[MAP_PERU_MINCETUR.paginaWeb]) obj[MAP_PERU_MINCETUR.paginaWeb] = clase;
      obj[MAP_PERU_MINCETUR.clase] = "";
    }
    rows.push(obj);
  }
  return { header, rows };
}

/**
 * @param {Record<string, string>} row
 * @param {{ classes?: string[], includeHostelLike?: boolean }} [opts]
 */
export function isPeruMinceturHotelRow(row, opts = {}) {
  const clase = normalizePeruText(row?.[MAP_PERU_MINCETUR.clase]).toUpperCase();
  if (!clase) return false;
  if (opts.includeHostelLike) return true;
  const allowed = (opts.classes || PERU_MINCETUR_DEFAULT_CLASSES).map((c) =>
    String(c).toUpperCase()
  );
  return allowed.includes(clase);
}

/**
 * @param {unknown} raw
 */
export function parsePeruMinceturRooms(raw) {
  const n = Number(String(raw ?? "").replace(/[^\d.-]/g, ""));
  if (!Number.isFinite(n) || n <= 0) {
    return { ok: false, rooms: null, reason: "missing_or_non_positive" };
  }
  if (n > PERU_MINCETUR_ROOMS_SANITY_MAX) {
    return { ok: false, rooms: n, reason: "rooms_above_sanity_max", hold: true };
  }
  return { ok: true, rooms: Math.round(n), reason: null };
}

/**
 * @param {Record<string, string>} row
 */
export function buildPeruAddress(row) {
  const via = normalizePeruText(row?.[MAP_PERU_MINCETUR.via]);
  const des = normalizePeruText(row?.[MAP_PERU_MINCETUR.desVia]);
  const num = normalizePeruText(row?.[MAP_PERU_MINCETUR.numero]);
  const parts = [via, des, num].filter(Boolean);
  return parts.join(" ").trim();
}

/**
 * @param {Record<string, unknown>} patch
 * @param {{ ownershipSignal?: object|null }} [meta]
 */
export function validatePeruMinceturCensusPatch(patch = {}, meta = {}) {
  const failed = [];
  const name = normalizePeruText(patch[MAP_PERU_MINCETUR.propertyName]);
  const country = normalizePeruText(patch[MAP_PERU_MINCETUR.countryField]);
  const identity = normalizePeruText(patch[MAP_PERU_MINCETUR.propertyIdentityKey]);
  const city = normalizePeruText(patch[MAP_PERU_MINCETUR.city]);

  if (!name) failed.push("required_property_name");
  if (country !== "Peru") failed.push("required_country_peru");
  if (!identity || !identity.startsWith("gov_pe_mincetur_")) failed.push("required_identity_key");
  if (!city || /^unknown$/i.test(city)) failed.push("required_city");

  for (const field of AUTOPILOT_FORBIDDEN_FIELDS) {
    if (Object.prototype.hasOwnProperty.call(patch, field) && patch[field] != null && patch[field] !== "") {
      failed.push(`forbidden_field:${field}`);
    }
  }
  if (meta.ownershipSignal && patch["Owner Name"]) {
    failed.push("owner_name_must_not_be_set_from_mincetur");
  }

  return {
    ok: failed.length === 0,
    failed,
    sanitized_payload_preview: { ...patch },
    field_mapping: MAP_PERU_MINCETUR,
  };
}

/**
 * @param {Record<string, string>} row
 * @param {{ dryRun?: boolean }} [opts]
 */
export function mapPeruMinceturRowToCensusCandidate(row, opts = {}) {
  const commercial = normalizePeruText(row?.[MAP_PERU_MINCETUR.nombreComercial]);
  const razon = normalizePeruText(row?.[MAP_PERU_MINCETUR.razonSocial]);
  const name = commercial || razon;
  const city = titleCasePeruPlace(row?.[MAP_PERU_MINCETUR.distrito]);
  const state = titleCasePeruPlace(row?.[MAP_PERU_MINCETUR.departamento]);
  const clase = normalizePeruText(row?.[MAP_PERU_MINCETUR.clase]);
  const ruc = normalizePeruText(row?.[MAP_PERU_MINCETUR.ruc]);
  const cert = normalizePeruText(row?.[MAP_PERU_MINCETUR.nroCertificado]);
  const identityKey = buildPeruMinceturIdentityKey(cert, ruc, name, city);
  const sourceUrl = MAP_PERU_MINCETUR.sourceDatasetUrl;
  const officialUrl = normalizePeruWebsite(row?.[MAP_PERU_MINCETUR.paginaWeb]);
  const roomsParsed = parsePeruMinceturRooms(row?.[MAP_PERU_MINCETUR.habitaciones]);
  const address = buildPeruAddress(row);
  const phone = normalizePeruText(row?.TELEF1 || row?.TELEF2 || "");

  /** @type {Record<string, unknown>} */
  const fields = {
    [MAP_PERU_MINCETUR.propertyName]: name,
    [MAP_PERU_MINCETUR.city]: city || "Unknown",
    [MAP_PERU_MINCETUR.stateRegion]: state || "",
    [MAP_PERU_MINCETUR.countryField]: MAP_PERU_MINCETUR.country,
    [MAP_PERU_MINCETUR.propertyIdentityKey]: identityKey,
    [MAP_PERU_MINCETUR.sourceUrl]: sourceUrl,
    [MAP_PERU_MINCETUR.familySource]: MAP_PERU_MINCETUR.familySourceFamily,
    [MAP_PERU_MINCETUR.sourceTypeField]: MAP_PERU_MINCETUR.sourceType,
    [MAP_PERU_MINCETUR.sourceConfidence]: "High",
    [MAP_PERU_MINCETUR.identityConfidence]: city && name ? "High" : "Medium",
    [MAP_PERU_MINCETUR.propertyType]: clase || "Hotel",
    [MAP_PERU_MINCETUR.productionUseStatus]: "Candidate",
    [MAP_PERU_MINCETUR.enrichmentStatus]: "Not Started",
    [MAP_PERU_MINCETUR.humanReviewRequired]: Boolean(roomsParsed.hold),
  };

  if (address) fields[MAP_PERU_MINCETUR.address] = address;
  if (phone) fields[MAP_PERU_MINCETUR.phone] = phone;
  if (officialUrl) fields[MAP_PERU_MINCETUR.officialPropertyUrl] = officialUrl;

  if (roomsParsed.ok && roomsParsed.rooms != null) {
    fields[MAP_PERU_MINCETUR.roomsKeys] = roomsParsed.rooms;
    fields[MAP_PERU_MINCETUR.roomsConfidence] = "Medium";
    fields[MAP_PERU_MINCETUR.roomsSourceUrl] = sourceUrl;
    fields[MAP_PERU_MINCETUR.roomsSourceType] = "government_mincetur";
  }

  const ownershipSignal = {
    country: "Peru",
    tax_id_type: "RUC",
    tax_id: ruc || null,
    legal_name_from_registry: razon || null,
    commercial_name: commercial || null,
    legal_representative: normalizePeruText(row?.[MAP_PERU_MINCETUR.repLegal]) || null,
    nro_certificado: cert || null,
    source_url: sourceUrl,
    lane: "ownership_enrichment_blocked",
    note: "Do not write to Owner Name until ownership enrichment lane is approved",
  };

  const validation = validatePeruMinceturCensusPatch(fields, { ownershipSignal });

  return {
    adapter_version: PERU_MINCETUR_ADAPTER_VERSION,
    dry_run: opts.dryRun !== false,
    identity_key: identityKey,
    nro_certificado: cert || null,
    ruc: ruc || null,
    raw: {
      fecha_corte: row?.[MAP_PERU_MINCETUR.fechaCorte] || null,
      clase,
      categoria: row?.[MAP_PERU_MINCETUR.categoria] || null,
      habitaciones_raw: row?.[MAP_PERU_MINCETUR.habitaciones] || null,
      rooms_parse: roomsParsed,
      pagina_web_raw: row?.[MAP_PERU_MINCETUR.paginaWeb] || null,
    },
    fields,
    ownership_signal: ownershipSignal,
    validation,
    error_handling: {
      validation_error: !validation.ok,
      api_error: false,
      network_error: false,
      user_facing_message: validation.ok
        ? null
        : `Peru MINCETUR candidate failed validation: ${validation.failed.join(", ")}`,
    },
  };
}

/**
 * @param {{ fetchImpl?: typeof fetch, csvUrl?: string }} [opts]
 */
export async function fetchPeruMinceturCsv(opts = {}) {
  const fetchImpl = opts.fetchImpl || fetch;
  const url = opts.csvUrl || MAP_PERU_MINCETUR.sourceCsvUrl;
  let response;
  try {
    response = await fetchImpl(url, {
      headers: {
        Accept: "text/csv,*/*",
        "User-Agent": "DealalityCensusBot/1.0",
      },
    });
  } catch (err) {
    return {
      ok: false,
      error_kind: "network",
      message: err?.message || String(err),
      rows: [],
      request_url: url,
    };
  }
  const text = await response.text();
  if (!response.ok) {
    return {
      ok: false,
      error_kind: "api",
      message: `HTTP ${response.status}: ${text.slice(0, 300)}`,
      rows: [],
      request_url: url,
    };
  }
  const parsed = parsePeruMinceturCsv(text);
  return {
    ok: true,
    rows: parsed.rows,
    header: parsed.header,
    request_url: url,
    byte_length: text.length,
  };
}

/**
 * @param {{
 *   maxRows?: number,
 *   hotelsOnly?: boolean,
 *   includeApartHotel?: boolean,
 *   includeHostelLike?: boolean,
 *   fetchImpl?: typeof fetch,
 * }} [opts]
 */
export async function runPeruMinceturDryRun(opts = {}) {
  const fetched = await fetchPeruMinceturCsv(opts);
  if (!fetched.ok) {
    return {
      ok: false,
      adapter_version: PERU_MINCETUR_ADAPTER_VERSION,
      dry_run: true,
      error_kind: fetched.error_kind,
      message: fetched.message,
      summary: null,
      candidates: [],
      field_mapping: MAP_PERU_MINCETUR,
    };
  }

  let classes = [...PERU_MINCETUR_DEFAULT_CLASSES];
  if (opts.hotelsOnly) classes = ["HOTEL"];
  if (opts.includeApartHotel === false) {
    classes = classes.filter((c) => c !== "APART HOTEL");
  }

  const filtered = fetched.rows.filter((row) =>
    isPeruMinceturHotelRow(row, {
      classes,
      includeHostelLike: opts.includeHostelLike,
    })
  );
  const limited = filtered.slice(0, Math.max(Number(opts.maxRows) || filtered.length, 1));
  const candidates = limited.map((row) =>
    mapPeruMinceturRowToCensusCandidate(row, { dryRun: true })
  );
  const valid = candidates.filter((c) => c.validation.ok);
  const invalid = candidates.filter((c) => !c.validation.ok);
  const withWeb = candidates.filter((c) => c.fields?.[MAP_PERU_MINCETUR.officialPropertyUrl]).length;
  const withRuc = candidates.filter((c) => c.ownership_signal?.tax_id).length;

  return {
    ok: true,
    adapter_version: PERU_MINCETUR_ADAPTER_VERSION,
    dry_run: true,
    airtable_writes: false,
    source: {
      csv_url: MAP_PERU_MINCETUR.sourceCsvUrl,
      dataset_url: MAP_PERU_MINCETUR.sourceDatasetUrl,
      classes,
      fecha_corte_sample: fetched.rows[0]?.[MAP_PERU_MINCETUR.fechaCorte] || null,
    },
    summary: {
      raw_rows_fetched: fetched.rows.length,
      lodging_filtered: filtered.length,
      candidates: candidates.length,
      validation_pass: valid.length,
      validation_fail: invalid.length,
      rooms_sanity_hold: candidates.filter((c) => c.raw?.rooms_parse?.hold).length,
      official_property_url_present: withWeb,
      ownership_ruc_present: withRuc,
      ownership_lane: "blocked_do_not_write_owner_name",
    },
    candidates: valid,
    invalid_sample: invalid.slice(0, 25),
    field_mapping: MAP_PERU_MINCETUR,
    error_handling: {
      validation_error: "Candidate excluded from writable preview",
      api_error: "CSV fetch failed; no candidates",
      network_error: "Retry later; no Airtable writes attempted",
      user_facing_message:
        "Peru MINCETUR dry-run complete. No Hotel Property Census writes were performed.",
    },
  };
}
