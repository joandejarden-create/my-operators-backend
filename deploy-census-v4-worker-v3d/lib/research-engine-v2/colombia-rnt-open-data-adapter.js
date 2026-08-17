/**
 * Colombia Registro Nacional de Turismo (RNT) — open-data adapter (dry-run first).
 *
 * Source: MinCIT via datos.gov.co Socrata dataset `thwd-ivmp`.
 * Role: government lodging inventory seed for Hotel Property Census.
 *
 * Ownership: NIT is captured as an ownership *signal* for a future enrichment lane.
 * Never map NIT / razón social into Autopilot-forbidden Owner Name fields.
 */

import { AUTOPILOT_FORBIDDEN_FIELDS } from "./census-autopilot-field-allowlist.js";

export const COLOMBIA_RNT_ADAPTER_VERSION = "colombia-rnt-open-data-adapter-v1";

/** Central Airtable / census field mapping (source → product). */
export const MAP_COLOMBIA_RNT = Object.freeze({
  sourceDatasetId: "thwd-ivmp",
  sourceDatasetUrl:
    "https://www.datos.gov.co/Comercio-Industria-y-Turismo/Registro-Nacional-de-Turismo-RNT/thwd-ivmp",
  sourceApiBase: "https://www.datos.gov.co/resource/thwd-ivmp.json",
  country: "Colombia",
  familySourceFamily: "Government — Colombia RNT",
  sourceType: "government_open_data_rnt",
  // Raw Socrata columns
  codigoRnt: "codigo_rnt",
  estadoRnt: "estado_rnt",
  razonSocial: "razon_social_establecimiento",
  departamento: "departamento",
  municipio: "municipio",
  nit: "nit",
  categoria: "categoria",
  subCategoria: "sub_categoria",
  habitaciones: "habitaciones",
  camas: "camas",
  ano: "ano",
  // Hotel Property Census targets (inventory lane only)
  propertyName: "Property Name",
  city: "City",
  stateRegion: "State / Region",
  countryField: "Country",
  roomsKeys: "Rooms / Keys",
  roomsConfidence: "Rooms Confidence",
  roomsSourceUrl: "Rooms Source URL",
  roomsSourceType: "Rooms Source Type",
  sourceUrl: "Source URL",
  familySource: "Family / Source Family",
  sourceTypeField: "Source Type",
  sourceConfidence: "Source Confidence",
  identityConfidence: "Identity Confidence",
  propertyIdentityKey: "Property Identity Key",
  propertyType: "Property Type",
  productionUseStatus: "Production Use Status",
  enrichmentStatus: "Enrichment Status",
  humanReviewRequired: "Human Review Required",
});

export const COLOMBIA_RNT_LODGING_CATEGORIA =
  "ESTABLECIMIENTOS DE ALOJAMIENTO TURÍSTICO";

/** Default hotel-census subcategories (hostels/glamping excluded unless opted in). */
export const COLOMBIA_RNT_DEFAULT_SUBCATEGORIES = Object.freeze(["HOTEL", "APARTAHOTEL"]);

export const COLOMBIA_RNT_HOSTEL_LIKE = Object.freeze([
  "HOSTAL",
  "ALBERGUE",
  "CAMPAMENTO",
  "GLAMPING",
  "REFUGIO",
]);

/** Rooms above this are treated as suspect (data quality Hold). */
export const COLOMBIA_RNT_ROOMS_SANITY_MAX = 1200;

/**
 * @param {string|number|null|undefined} value
 */
export function normalizeColombiaText(value) {
  return String(value || "")
    .normalize("NFKC")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * @param {string} value
 */
export function titleCaseColombiaPlace(value) {
  const s = normalizeColombiaText(value);
  if (!s) return "";
  return s
    .toLowerCase()
    .split(" ")
    .map((w) => (w ? w[0].toUpperCase() + w.slice(1) : w))
    .join(" ");
}

/**
 * Stable identity key for Hotel Property Census matching.
 * @param {string|number} codigoRnt
 */
export function buildColombiaRntIdentityKey(codigoRnt) {
  const id = normalizeColombiaText(codigoRnt).replace(/[^\dA-Za-z_-]/g, "");
  if (!id) return null;
  return `gov_co_rnt_${id}`;
}

/**
 * Record-level source URL (dataset + codigo_rnt filter).
 * @param {string|number} codigoRnt
 */
export function buildColombiaRntSourceUrl(codigoRnt) {
  const id = encodeURIComponent(normalizeColombiaText(codigoRnt));
  return `${MAP_COLOMBIA_RNT.sourceDatasetUrl}/explore?filter=codigo_rnt=${id}`;
}

/**
 * @param {Record<string, unknown>} row
 * @param {{ includeHostelLike?: boolean, subcategories?: string[] }} [opts]
 */
export function isColombiaRntLodgingHotelRow(row, opts = {}) {
  const categoria = normalizeColombiaText(row?.[MAP_COLOMBIA_RNT.categoria]).toUpperCase();
  const sub = normalizeColombiaText(row?.[MAP_COLOMBIA_RNT.subCategoria]).toUpperCase();
  const estado = normalizeColombiaText(row?.[MAP_COLOMBIA_RNT.estadoRnt]).toUpperCase();
  if (!categoria.includes("ALOJAMIENTO")) return false;
  if (estado && estado !== "ACTIVO") return false;

  const allowed = (opts.subcategories || COLOMBIA_RNT_DEFAULT_SUBCATEGORIES).map((s) =>
    String(s).toUpperCase()
  );
  if (opts.includeHostelLike) {
    return Boolean(sub);
  }
  return allowed.includes(sub);
}

/**
 * Keep newest `ano` per codigo_rnt.
 * @param {Record<string, unknown>[]} rows
 */
export function dedupeColombiaRntByCodigo(rows = []) {
  /** @type {Map<string, Record<string, unknown>>} */
  const byId = new Map();
  for (const row of rows) {
    const id = normalizeColombiaText(row?.[MAP_COLOMBIA_RNT.codigoRnt]);
    if (!id) continue;
    const year = Number(row?.[MAP_COLOMBIA_RNT.ano]) || 0;
    const prev = byId.get(id);
    const prevYear = Number(prev?.[MAP_COLOMBIA_RNT.ano]) || 0;
    if (!prev || year >= prevYear) byId.set(id, row);
  }
  return [...byId.values()];
}

/**
 * @param {unknown} raw
 */
export function parseColombiaRntRooms(raw) {
  const n = Number(String(raw ?? "").replace(/[^\d.-]/g, ""));
  if (!Number.isFinite(n) || n <= 0) {
    return { ok: false, rooms: null, reason: "missing_or_non_positive" };
  }
  if (n > COLOMBIA_RNT_ROOMS_SANITY_MAX) {
    return { ok: false, rooms: n, reason: "rooms_above_sanity_max", hold: true };
  }
  return { ok: true, rooms: Math.round(n), reason: null };
}

/**
 * Validation before any future write path.
 * @param {Record<string, unknown>} patch
 * @param {{ ownershipSignal?: Record<string, unknown>|null }} [meta]
 */
export function validateColombiaRntCensusPatch(patch = {}, meta = {}) {
  const failed = [];
  const name = normalizeColombiaText(patch[MAP_COLOMBIA_RNT.propertyName]);
  const country = normalizeColombiaText(patch[MAP_COLOMBIA_RNT.countryField]);
  const identity = normalizeColombiaText(patch[MAP_COLOMBIA_RNT.propertyIdentityKey]);
  const city = normalizeColombiaText(patch[MAP_COLOMBIA_RNT.city]);

  if (!name) failed.push("required_property_name");
  if (country !== "Colombia") failed.push("required_country_colombia");
  if (!identity || !identity.startsWith("gov_co_rnt_")) failed.push("required_identity_key");
  if (!city || /^unknown$/i.test(city)) failed.push("required_city");

  for (const field of AUTOPILOT_FORBIDDEN_FIELDS) {
    if (Object.prototype.hasOwnProperty.call(patch, field) && patch[field] != null && patch[field] !== "") {
      failed.push(`forbidden_field:${field}`);
    }
  }

  // Guard: ownership signal must not leak into Owner Name
  if (meta.ownershipSignal && patch["Owner Name"]) {
    failed.push("owner_name_must_not_be_set_from_rnt");
  }

  return {
    ok: failed.length === 0,
    failed,
    sanitized_payload_preview: { ...patch },
    field_mapping: MAP_COLOMBIA_RNT,
  };
}

/**
 * Map one RNT row → census candidate (inventory lane) + ownership sidecar signal.
 * @param {Record<string, unknown>} row
 * @param {{ dryRun?: boolean }} [opts]
 */
export function mapColombiaRntRowToCensusCandidate(row, opts = {}) {
  const codigo = normalizeColombiaText(row?.[MAP_COLOMBIA_RNT.codigoRnt]);
  const identityKey = buildColombiaRntIdentityKey(codigo);
  const sourceUrl = buildColombiaRntSourceUrl(codigo);
  const name = normalizeColombiaText(row?.[MAP_COLOMBIA_RNT.razonSocial]);
  const city = titleCaseColombiaPlace(row?.[MAP_COLOMBIA_RNT.municipio]);
  const state = titleCaseColombiaPlace(row?.[MAP_COLOMBIA_RNT.departamento]);
  const sub = normalizeColombiaText(row?.[MAP_COLOMBIA_RNT.subCategoria]);
  const nit = normalizeColombiaText(row?.[MAP_COLOMBIA_RNT.nit]);
  const roomsParsed = parseColombiaRntRooms(row?.[MAP_COLOMBIA_RNT.habitaciones]);

  /** @type {Record<string, unknown>} */
  const fields = {
    [MAP_COLOMBIA_RNT.propertyName]: name,
    [MAP_COLOMBIA_RNT.city]: city || "Unknown",
    [MAP_COLOMBIA_RNT.stateRegion]: state || "",
    [MAP_COLOMBIA_RNT.countryField]: MAP_COLOMBIA_RNT.country,
    [MAP_COLOMBIA_RNT.propertyIdentityKey]: identityKey,
    [MAP_COLOMBIA_RNT.sourceUrl]: sourceUrl,
    [MAP_COLOMBIA_RNT.familySource]: MAP_COLOMBIA_RNT.familySourceFamily,
    [MAP_COLOMBIA_RNT.sourceTypeField]: MAP_COLOMBIA_RNT.sourceType,
    [MAP_COLOMBIA_RNT.sourceConfidence]: "High",
    [MAP_COLOMBIA_RNT.identityConfidence]: city ? "High" : "Medium",
    [MAP_COLOMBIA_RNT.propertyType]: sub || "Hotel",
    [MAP_COLOMBIA_RNT.productionUseStatus]: "Candidate",
    [MAP_COLOMBIA_RNT.enrichmentStatus]: "Not Started",
    [MAP_COLOMBIA_RNT.humanReviewRequired]: roomsParsed.hold ? true : false,
  };

  if (roomsParsed.ok && roomsParsed.rooms != null) {
    fields[MAP_COLOMBIA_RNT.roomsKeys] = roomsParsed.rooms;
    fields[MAP_COLOMBIA_RNT.roomsConfidence] = "Medium";
    fields[MAP_COLOMBIA_RNT.roomsSourceUrl] = sourceUrl;
    fields[MAP_COLOMBIA_RNT.roomsSourceType] = "government_rnt";
  }

  const ownershipSignal = {
    country: "Colombia",
    tax_id_type: "NIT",
    tax_id: nit || null,
    legal_name_from_registry: name || null,
    codigo_rnt: codigo || null,
    source_url: sourceUrl,
    lane: "ownership_enrichment_blocked",
    note: "Do not write to Owner Name until ownership enrichment lane is approved",
  };

  const validation = validateColombiaRntCensusPatch(fields, { ownershipSignal });

  return {
    adapter_version: COLOMBIA_RNT_ADAPTER_VERSION,
    dry_run: opts.dryRun !== false,
    identity_key: identityKey,
    codigo_rnt: codigo,
    raw: {
      ano: row?.[MAP_COLOMBIA_RNT.ano] ?? null,
      estado_rnt: row?.[MAP_COLOMBIA_RNT.estadoRnt] ?? null,
      sub_categoria: sub,
      habitaciones_raw: row?.[MAP_COLOMBIA_RNT.habitaciones] ?? null,
      rooms_parse: roomsParsed,
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
        : `Colombia RNT candidate failed validation: ${validation.failed.join(", ")}`,
    },
  };
}

/**
 * Build Socrata SoQL where clause.
 * @param {{
 *   year?: number|string|null,
 *   subcategories?: string[],
 *   includeHostelLike?: boolean,
 *   activeOnly?: boolean,
 * }} [opts]
 */
export function buildColombiaRntWhereClause(opts = {}) {
  const parts = [`upper(categoria) like '%ALOJAMIENTO%'`];
  if (opts.activeOnly !== false) parts.push(`upper(estado_rnt)='ACTIVO'`);
  if (opts.year != null && opts.year !== "") {
    parts.push(`ano='${String(opts.year).replace(/'/g, "")}'`);
  }
  if (!opts.includeHostelLike) {
    const subs = (opts.subcategories || COLOMBIA_RNT_DEFAULT_SUBCATEGORIES)
      .map((s) => `'${String(s).replace(/'/g, "").toUpperCase()}'`)
      .join(", ");
    parts.push(`upper(sub_categoria) in (${subs})`);
  }
  return parts.join(" AND ");
}

/**
 * @param {{
 *   limit?: number,
 *   offset?: number,
 *   year?: number|string|null,
 *   subcategories?: string[],
 *   includeHostelLike?: boolean,
 *   fetchImpl?: typeof fetch,
 * }} [opts]
 */
export async function fetchColombiaRntPage(opts = {}) {
  const fetchImpl = opts.fetchImpl || fetch;
  const limit = Math.min(Math.max(Number(opts.limit) || 1000, 1), 50000);
  const offset = Math.max(Number(opts.offset) || 0, 0);
  const u = new URL(MAP_COLOMBIA_RNT.sourceApiBase);
  u.searchParams.set("$limit", String(limit));
  u.searchParams.set("$offset", String(offset));
  u.searchParams.set("$order", "codigo_rnt,ano");
  u.searchParams.set("$where", buildColombiaRntWhereClause(opts));

  let response;
  try {
    response = await fetchImpl(String(u), {
      headers: { Accept: "application/json", "User-Agent": "DealalityCensusBot/1.0" },
    });
  } catch (err) {
    return {
      ok: false,
      error_kind: "network",
      message: err?.message || String(err),
      rows: [],
      request_url: String(u),
    };
  }

  const text = await response.text();
  if (!response.ok) {
    return {
      ok: false,
      error_kind: "api",
      message: `HTTP ${response.status}: ${text.slice(0, 400)}`,
      rows: [],
      request_url: String(u),
    };
  }

  let rows;
  try {
    rows = JSON.parse(text);
  } catch (err) {
    return {
      ok: false,
      error_kind: "api",
      message: `JSON parse failed: ${err?.message || err}`,
      rows: [],
      request_url: String(u),
    };
  }

  if (!Array.isArray(rows)) {
    return {
      ok: false,
      error_kind: "api",
      message: "Unexpected non-array payload",
      rows: [],
      request_url: String(u),
    };
  }

  return { ok: true, rows, request_url: String(u), limit, offset };
}

/**
 * Paginate until exhausted or maxRows reached. Dry-run friendly.
 * @param {{
 *   maxRows?: number,
 *   pageSize?: number,
 *   year?: number|string|null,
 *   subcategories?: string[],
 *   includeHostelLike?: boolean,
 *   fetchImpl?: typeof fetch,
 *   onPage?: (info: object) => void,
 * }} [opts]
 */
export async function fetchColombiaRntLodgingRows(opts = {}) {
  const pageSize = Math.min(Math.max(Number(opts.pageSize) || 5000, 1), 50000);
  const maxRows = Math.max(Number(opts.maxRows) || 25000, 1);
  /** @type {Record<string, unknown>[]} */
  const all = [];
  let offset = 0;
  let pages = 0;

  while (all.length < maxRows) {
    const page = await fetchColombiaRntPage({
      ...opts,
      limit: Math.min(pageSize, maxRows - all.length),
      offset,
    });
    pages += 1;
    if (!page.ok) {
      return {
        ok: false,
        error_kind: page.error_kind,
        message: page.message,
        rows: all,
        pages,
        request_url: page.request_url,
      };
    }
    opts.onPage?.({ page: pages, fetched: page.rows.length, offset, total: all.length + page.rows.length });
    if (!page.rows.length) break;
    all.push(...page.rows);
    if (page.rows.length < pageSize) break;
    offset += page.rows.length;
  }

  return {
    ok: true,
    rows: all.slice(0, maxRows),
    pages,
    truncated: all.length >= maxRows,
  };
}

/**
 * End-to-end dry-run pipeline (no Airtable writes).
 * @param {{
 *   maxRows?: number,
 *   pageSize?: number,
 *   year?: number|string|null,
 *   includeApartahotel?: boolean,
 *   hotelsOnly?: boolean,
 *   includeHostelLike?: boolean,
 *   fetchImpl?: typeof fetch,
 * }} [opts]
 */
export async function runColombiaRntDryRun(opts = {}) {
  let subcategories = [...COLOMBIA_RNT_DEFAULT_SUBCATEGORIES];
  if (opts.hotelsOnly) subcategories = ["HOTEL"];
  if (opts.includeApartahotel === false) {
    subcategories = subcategories.filter((s) => s !== "APARTAHOTEL");
  }

  const fetched = await fetchColombiaRntLodgingRows({
    ...opts,
    subcategories,
  });

  if (!fetched.ok) {
    return {
      ok: false,
      adapter_version: COLOMBIA_RNT_ADAPTER_VERSION,
      dry_run: true,
      error_kind: fetched.error_kind,
      message: fetched.message,
      summary: null,
      candidates: [],
      field_mapping: MAP_COLOMBIA_RNT,
    };
  }

  const deduped = dedupeColombiaRntByCodigo(fetched.rows);
  const candidates = deduped.map((row) => mapColombiaRntRowToCensusCandidate(row, { dryRun: true }));
  const valid = candidates.filter((c) => c.validation.ok);
  const invalid = candidates.filter((c) => !c.validation.ok);
  const roomsHeld = candidates.filter((c) => c.raw?.rooms_parse?.hold).length;
  const withNit = candidates.filter((c) => c.ownership_signal?.tax_id).length;

  return {
    ok: true,
    adapter_version: COLOMBIA_RNT_ADAPTER_VERSION,
    dry_run: true,
    airtable_writes: false,
    source: {
      dataset_id: MAP_COLOMBIA_RNT.sourceDatasetId,
      dataset_url: MAP_COLOMBIA_RNT.sourceDatasetUrl,
      year_filter: opts.year ?? null,
      subcategories,
    },
    summary: {
      raw_rows_fetched: fetched.rows.length,
      pages: fetched.pages,
      truncated: Boolean(fetched.truncated),
      unique_codigo_rnt: deduped.length,
      candidates: candidates.length,
      validation_pass: valid.length,
      validation_fail: invalid.length,
      rooms_sanity_hold: roomsHeld,
      ownership_nit_present: withNit,
      ownership_lane: "blocked_do_not_write_owner_name",
    },
    candidates: valid,
    invalid_sample: invalid.slice(0, 25),
    field_mapping: MAP_COLOMBIA_RNT,
    error_handling: {
      validation_error: "Candidate excluded from writable preview; see validation.failed",
      api_error: "Fetch aborted; no candidates produced",
      network_error: "Retry later; no Airtable writes attempted",
      user_facing_message:
        "Colombia RNT dry-run complete. No Hotel Property Census writes were performed.",
    },
  };
}
