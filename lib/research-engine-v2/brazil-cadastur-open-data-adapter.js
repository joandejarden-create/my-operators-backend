/**
 * Brazil CADASTUR — Meios de Hospedagem open-data adapter.
 *
 * Source: Ministério do Turismo quarterly XLSX on dados.turismo.gov.br
 * Rooms field: Unidade Habitacionais (UH) — NEVER Leitos (beds).
 *
 * Ownership: CNPJ / Nome do Responsável are ownership signals only — never Owner Name.
 */
import fs from "node:fs";
import path from "node:path";
import XLSX from "xlsx";
import { AUTOPILOT_FORBIDDEN_FIELDS } from "./census-autopilot-field-allowlist.js";
import {
  extractPostalFromAddress,
  normalizePostalCode,
} from "./census-postal-code-v1.js";

export const BRAZIL_CADASTUR_ADAPTER_VERSION =
  "brazil-cadastur-open-data-adapter-v1";

export const MAP_BRAZIL_CADASTUR = Object.freeze({
  sourceDatasetUrl: "https://dados.turismo.gov.br/dataset/meios-de-hospedagem",
  sourceXlsxUrl:
    "https://dados.turismo.gov.br/dataset/d2333d1b-db1e-438b-955a-028db80a031e/resource/938cb620-7252-4cd0-9def-443dd2fe3f3b/download/meio-de-hospedagem-1-trimestre-2026.xlsx",
  country: "Brazil",
  familySourceFamily: "Government — Brazil CADASTUR",
  sourceType: "government_open_data_cadastur",
  // XLSX columns
  cnpj: "Número de Inscrição do CNPJ",
  razaoSocial: "Nome da Pessoa Jurídica",
  nomeFantasia: "Nome Fantasia",
  situacaoCadastral: "Situação Cadastral",
  situacaoAtividade: "Situação da Atividade",
  uf: "UF",
  municipio: "Município",
  enderecoComercial: "Endereço Completo Comercial",
  telefoneComercial: "Telefone Comercial",
  website: "Website",
  certificado: "Número do Certificado",
  tipoHospedagem: "Tipo de Hospedagem",
  unidadeHabitacionais: "Unidade Habitacionais",
  leitos: "Leitos",
  // Census
  propertyName: "Property Name",
  city: "City",
  stateRegion: "State / Region",
  countryField: "Country",
  address: "Address",
  postalCode: "Postal Code",
  phone: "Phone",
  officialPropertyUrl: "Official Property URL",
  roomsKeys: "Rooms / Keys",
  roomsConfidence: "Rooms Confidence",
  roomsSourceUrl: "Rooms Source URL",
  roomsSourceType: "Rooms Source Type",
  sourceUrl: "Source URL",
  familySource: "Family / Source Family",
  propertyIdentityKey: "Property Identity Key",
});

/** Types eligible for hotel Rooms / Keys HIGH matching. */
export const BRAZIL_CADASTUR_HOTEL_TYPES = Object.freeze([
  "Hotel",
  "Resort",
  "Flat/Apart-Hotel",
  "Hotel Histórico",
  "Hotel Fazenda",
  "Pousada",
]);

export const BRAZIL_CADASTUR_EXCLUDE_TYPES = Object.freeze([
  "Albergue/Hostel",
  "Cama e Café",
  "Outros",
]);

export const BRAZIL_CADASTUR_ROOMS_SANITY_MAX = 2500;

export const BRAZIL_UF_TO_STATE = Object.freeze({
  AC: "Acre",
  AL: "Alagoas",
  AP: "Amapá",
  AM: "Amazonas",
  BA: "Bahia",
  CE: "Ceará",
  DF: "Distrito Federal",
  ES: "Espírito Santo",
  GO: "Goiás",
  MA: "Maranhão",
  MT: "Mato Grosso",
  MS: "Mato Grosso do Sul",
  MG: "Minas Gerais",
  PA: "Pará",
  PB: "Paraíba",
  PR: "Paraná",
  PE: "Pernambuco",
  PI: "Piauí",
  RJ: "Rio de Janeiro",
  RN: "Rio Grande do Norte",
  RS: "Rio Grande do Sul",
  RO: "Rondônia",
  RR: "Roraima",
  SC: "Santa Catarina",
  SP: "São Paulo",
  SE: "Sergipe",
  TO: "Tocantins",
});

export function normalizeBrazilText(value) {
  return String(value || "")
    .normalize("NFKC")
    .replace(/\s+/g, " ")
    .trim();
}

export function normalizeBrazilWebsite(raw) {
  const s = normalizeBrazilText(raw);
  if (!s || s === "-" || /^n\/?a$/i.test(s)) return null;
  let url = s;
  if (!/^https?:\/\//i.test(url)) url = `https://${url}`;
  try {
    const u = new URL(url);
    if (/turismo\.gov\.br|cadastur|dados\.turismo/i.test(u.hostname)) return null;
    return u.toString().replace(/\/$/, "");
  } catch {
    return null;
  }
}

export function parseBrazilCadasturRooms(rawUh) {
  const n = Number(String(rawUh ?? "").replace(/[^\d.]/g, ""));
  if (!Number.isFinite(n) || n <= 0) {
    return { ok: false, rooms: null, hold: false };
  }
  if (n > BRAZIL_CADASTUR_ROOMS_SANITY_MAX) {
    return { ok: false, rooms: Math.round(n), hold: true };
  }
  return { ok: true, rooms: Math.round(n), hold: false };
}

export function isBrazilCadasturHotelRow(row, opts = {}) {
  const tipo = normalizeBrazilText(row?.[MAP_BRAZIL_CADASTUR.tipoHospedagem]);
  const sit = normalizeBrazilText(row?.[MAP_BRAZIL_CADASTUR.situacaoAtividade]);
  if (sit && !/^opera/i.test(sit)) return false;
  const allowed = opts.types || BRAZIL_CADASTUR_HOTEL_TYPES;
  if (BRAZIL_CADASTUR_EXCLUDE_TYPES.includes(tipo)) return false;
  return allowed.some((t) => t.toLowerCase() === tipo.toLowerCase());
}

export function buildBrazilCadasturIdentityKey(cnpj, certificado, name, city) {
  const c = String(cnpj || certificado || "").replace(/\D/g, "");
  if (c) return `gov_br_cadastur_${c}`;
  const slug = normalizeBrazilText(name)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .slice(0, 40);
  const citySlug = normalizeBrazilText(city)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .slice(0, 20);
  return `gov_br_cadastur_${slug}_${citySlug}`;
}

export function extractBrazilPostalFromAddress(address) {
  const from = extractPostalFromAddress(address, "Brazil");
  if (from.ok) return from.postal_code;
  const m = String(address || "").match(/CEP[:\s]*(\d{5}-?\d{3}|\d{8})/i);
  if (!m) return null;
  return normalizePostalCode(m[1], "Brazil");
}

export function mapBrazilCadasturRowToNormalized(row) {
  const nome =
    normalizeBrazilText(row?.[MAP_BRAZIL_CADASTUR.nomeFantasia]) ||
    normalizeBrazilText(row?.[MAP_BRAZIL_CADASTUR.razaoSocial]);
  const city = normalizeBrazilText(row?.[MAP_BRAZIL_CADASTUR.municipio]);
  const uf = normalizeBrazilText(row?.[MAP_BRAZIL_CADASTUR.uf]).toUpperCase();
  const state =
    BRAZIL_UF_TO_STATE[uf] || uf;
  const address = normalizeBrazilText(
    row?.[MAP_BRAZIL_CADASTUR.enderecoComercial]
  );
  const phone = normalizeBrazilText(
    row?.[MAP_BRAZIL_CADASTUR.telefoneComercial]
  );
  const website = normalizeBrazilWebsite(row?.[MAP_BRAZIL_CADASTUR.website]);
  const cnpj = normalizeBrazilText(row?.[MAP_BRAZIL_CADASTUR.cnpj]);
  const cert = normalizeBrazilText(row?.[MAP_BRAZIL_CADASTUR.certificado]);
  const roomsParse = parseBrazilCadasturRooms(
    row?.[MAP_BRAZIL_CADASTUR.unidadeHabitacionais]
  );
  const postal = extractBrazilPostalFromAddress(address);
  const identity = buildBrazilCadasturIdentityKey(cnpj, cert, nome, city);
  const leitos = Number(
    String(row?.[MAP_BRAZIL_CADASTUR.leitos] ?? "").replace(/[^\d.]/g, "")
  );

  return {
    adapter: "brazil_cadastur",
    identity_key: identity,
    property_name: nome,
    commercial_name: normalizeBrazilText(
      row?.[MAP_BRAZIL_CADASTUR.nomeFantasia]
    ),
    legal_name: normalizeBrazilText(row?.[MAP_BRAZIL_CADASTUR.razaoSocial]),
    city,
    state_region: state,
    state_uf: uf,
    country: "Brazil",
    address: address || null,
    postal_code: postal || null,
    phone: phone || null,
    website,
    rooms: roomsParse.ok ? roomsParse.rooms : null,
    rooms_hold: roomsParse.hold,
    rooms_parse: roomsParse,
    leitos: Number.isFinite(leitos) ? leitos : null,
    tipo: normalizeBrazilText(row?.[MAP_BRAZIL_CADASTUR.tipoHospedagem]),
    cnpj: cnpj || null,
    certificado: cert || null,
    source_url: MAP_BRAZIL_CADASTUR.sourceDatasetUrl,
    // Explicit: never expose leitos as rooms
    rooms_field_used: "Unidade Habitacionais",
    rooms_field_rejected: "Leitos",
  };
}

/**
 * @param {{
 *   xlsxPath?: string,
 *   xlsxUrl?: string,
 *   maxRows?: number,
 *   hotelsOnly?: boolean,
 *   cacheDir?: string,
 * }} [opts]
 */
export async function fetchBrazilCadasturLodgingRows(opts = {}) {
  const url = opts.xlsxUrl || MAP_BRAZIL_CADASTUR.sourceXlsxUrl;
  const cacheDir =
    opts.cacheDir ||
    path.join(
      process.cwd(),
      "data/research-engine-v2/official-rooms-sources"
    );
  fs.mkdirSync(cacheDir, { recursive: true });
  const cachePath =
    opts.xlsxPath || path.join(cacheDir, "brazil-cadastur-q1-2026.xlsx");

  let buf;
  if (fs.existsSync(cachePath) && !opts.forceDownload) {
    buf = fs.readFileSync(cachePath);
  } else {
    const res = await fetch(url, {
      redirect: "follow",
      headers: { "User-Agent": "DealalityCensusBot/1.0 (+property-fundamentals)" },
    });
    if (!res.ok) {
      return {
        ok: false,
        error_kind: "http_error",
        message: `CADASTUR download HTTP ${res.status}`,
        rows: [],
      };
    }
    buf = Buffer.from(await res.arrayBuffer());
    fs.writeFileSync(cachePath, buf);
  }

  const wb = XLSX.read(buf, { type: "buffer" });
  const sheet = wb.Sheets[wb.SheetNames[0]];
  let rows = XLSX.utils.sheet_to_json(sheet, { defval: "" });
  if (opts.hotelsOnly !== false) {
    rows = rows.filter((r) => isBrazilCadasturHotelRow(r, opts));
  }
  if (opts.maxRows) rows = rows.slice(0, opts.maxRows);

  const normalized = rows
    .map(mapBrazilCadasturRowToNormalized)
    .filter((r) => r.property_name && r.city);

  // Safety: ensure no accidental Leitos→rooms mapping in adapter
  for (const r of normalized) {
    if (r.rooms != null && r.leitos != null && r.rooms === r.leitos && r.leitos > 0) {
      // UH can equal beds by coincidence; only flag when UH missing in raw — already used UH
    }
  }

  return {
    ok: true,
    adapter_version: BRAZIL_CADASTUR_ADAPTER_VERSION,
    source_url: url,
    cache_path: cachePath,
    raw_count: rows.length,
    rows: normalized,
    field_mapping: MAP_BRAZIL_CADASTUR,
    forbidden_fields_guard: AUTOPILOT_FORBIDDEN_FIELDS,
    rooms_policy: {
      write_field: "Unidade Habitacionais",
      never_write: ["Leitos", "Leitos Acessíveis"],
    },
  };
}
