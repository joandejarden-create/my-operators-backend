/**
 * INEGI DENUE client — official API + optional state CSV extract fallback.
 *
 * Docs: https://www.inegi.org.mx/servicios/api_denue.html
 * Token: https://www.inegi.org.mx/app/api/denue/v1/tokenVerify.aspx
 *
 * DENUE razón social = associated registered business identity for an establishment.
 * It does NOT automatically establish economic owner or property owner.
 * CLEE / Id establecimiento must NOT be treated as RFC.
 */

import fs from "node:fs";
import path from "node:path";
import { createReadStream } from "node:fs";
import { createInterface } from "node:readline";
import { execFileSync } from "node:child_process";

export const DENUE_CLIENT_VERSION = "inegi-denue-client-v1";
export const DENUE_API_BASE = "https://www.inegi.org.mx/app/api/denue/v1/consulta";
export const DENUE_TOKEN_SETUP_URL = "https://www.inegi.org.mx/app/api/denue/v1/tokenVerify.aspx";
export const DENUE_DOCS_URL = "https://www.inegi.org.mx/servicios/api_denue.html";
export const DENUE_STATE_CSV_URL = (cveEnt) =>
  `https://www.inegi.org.mx/contenidos/masiva/denue/denue_${String(cveEnt).padStart(2, "0")}_csv.zip`;

/** Federal entity codes used by this pilot. */
export const DENUE_ENTIDAD = Object.freeze({
  BCS: "03",
  CDMX: "09",
  JALISCO: "14",
  QUINTANA_ROO: "23",
});

/** SCIAN lodging / hotel activity filters (prefer codigo 7211*; name fallback is secondary). */
export const DENUE_HOTEL_ACTIVITY_RE =
  /\b(hoteles?|moteles?|hostales?|hospedaje|alojamiento|resorts?|posadas?)\b/i;

/** True when row is lodging-class or clear hotel establishment (not "resortes", not construction-named hotel). */
export function isDenueHotelEstablishment({ codigo_act, nombre_act, nom_estab } = {}) {
  const code = String(codigo_act || "");
  if (/^7211/.test(code)) return true;
  const act = String(nombre_act || "");
  const nombre = String(nom_estab || "");
  // SCIAN 72* lodging sector without subclass
  if (/^72/.test(code) && DENUE_HOTEL_ACTIVITY_RE.test(act)) return true;
  if (DENUE_HOTEL_ACTIVITY_RE.test(act)) return true;
  // Name-only: require hotel/motel token AND exclude obvious non-lodging activities
  if (/\b(hoteles?|moteles?|hostales?|resorts?)\b/i.test(nombre)) {
    if (/edificaci[oó]n|fabricaci[oó]n|construcci[oó]n|alambre|comercio al por|servicios de apoyo/i.test(act)) {
      return false;
    }
    // Prefer when activity also looks lodging-adjacent or blank
    if (!act || DENUE_HOTEL_ACTIVITY_RE.test(act) || /turismo|hospitalidad/i.test(act)) return true;
  }
  return false;
}

function getToken() {
  return String(
    process.env.DENUE_TOKEN ||
      process.env.INEGI_DENUE_TOKEN ||
      process.env.INEGI_API_TOKEN ||
      ""
  ).trim();
}

export function isDenueApiConfigured() {
  return Boolean(getToken());
}

export function describeDenueAccess() {
  const tokenPresent = isDenueApiConfigured();
  return {
    api_configured: tokenPresent,
    token_env_keys_checked: ["DENUE_TOKEN", "INEGI_DENUE_TOKEN", "INEGI_API_TOKEN"],
    token_setup_url: DENUE_TOKEN_SETUP_URL,
    docs_url: DENUE_DOCS_URL,
    csv_fallback_urls: {
      "03": DENUE_STATE_CSV_URL("03"),
      "09": DENUE_STATE_CSV_URL("09"),
      "14": DENUE_STATE_CSV_URL("14"),
      "23": DENUE_STATE_CSV_URL("23"),
    },
  };
}

function encodePathSegment(s) {
  return encodeURIComponent(String(s || "").trim()).replace(/%2C/gi, ",");
}

/**
 * Normalize a DENUE API JSON object (Spanish keys) into a stable record.
 * retrieval_timestamp is when WE fetched — not DENUE's update date.
 */
export function normalizeDenueRecord(raw, { retrievedAt = null, sourceMode = "api" } = {}) {
  const r = raw || {};
  const clee = r.CLEE || r.Clee || r.clee || null;
  const id =
    r.Id ||
    r.ID ||
    r.id ||
    r["Id del Establecimiento"] ||
    r.IdEstablecimiento ||
    null;
  return {
    clee: clee != null ? String(clee) : null,
    establecimiento_id: id != null ? String(id) : null,
    nombre: r.Nombre || r.nombre || r["Nombre de la Unidad Económica"] || null,
    razon_social: r.Razon_social || r.Razón_social || r.razon_social || r["Razón social"] || null,
    actividad: r.Clase_actividad || r.Clase_act || r.actividad || r["Nombre de clase de la actividad"] || null,
    actividad_id: r.Codigo_Actividad || r.codigo_actividad || r["Código de la clase de actividad SCIAN"] || null,
    estrato: r.Estrato || r.estrato || r["Personal ocupado (estrato)"] || null,
    telefono: r.Telefono || r.Teléfono || r.telefono || null,
    correo: r.Correo_e || r.Correo || r.correo_e || r["Correo electrónico"] || null,
    website: r.Sitio_internet || r.www || r.Website || r["Sitio en Internet"] || null,
    calle: r.Calle || r.calle || null,
    num_ext: r.Num_Exterior || r.Numero_Exterior || r["Número Exterior"] || null,
    num_int: r.Num_Interior || r["Número Interior"] || null,
    colonia: r.Colonia || r.colonia || null,
    cp: r.CP || r.Codigo_Postal || r["Código Postal"] || null,
    ubicacion: r.Ubicacion || r.ubicacion || r["Localidad, municipio y entidad federativa"] || null,
    latitud: numOrNull(r.Latitud ?? r.latitud),
    longitud: numOrNull(r.Longitud ?? r.longitud),
    tipo_establecimiento: r.Tipo_establecimiento || r.tipo || null,
    fecha_alta: r.Fecha_Alta || r["Fecha de alta"] || null,
    raw: r,
    provenance: {
      source: "INEGI_DENUE",
      source_mode: sourceMode,
      retrieved_at: retrievedAt || new Date().toISOString(),
      // Explicit: retrieval time ≠ DENUE update/verification date
      retrieval_is_not_source_update_date: true,
      denue_update_date_supplied: Boolean(r.Fecha_Alta || r["Fecha de alta"]),
      denue_update_date_value: r.Fecha_Alta || r["Fecha de alta"] || null,
      note: "Razón social is registered business association — not automatic economic owner. CLEE/Id ≠ RFC.",
    },
  };
}

function numOrNull(v) {
  if (v == null || v === "") return null;
  const n = Number(String(v).replace(",", "."));
  return Number.isFinite(n) ? n : null;
}

async function apiGet(urlPath) {
  const token = getToken();
  if (!token) {
    return {
      ok: false,
      error: {
        class: "MISSING_TOKEN",
        message: "DENUE_TOKEN missing",
        setup_url: DENUE_TOKEN_SETUP_URL,
      },
      data: null,
      http_status: null,
    };
  }
  const url = `${DENUE_API_BASE}${urlPath}/${encodeURIComponent(token)}`;
  const retrievedAt = new Date().toISOString();
  const res = await fetch(url, { method: "GET", headers: { Accept: "application/json" } });
  const text = await res.text();
  if (/No Autorizado|clave valida|no autorizado/i.test(text)) {
    return {
      ok: false,
      error: { class: "UNAUTHORIZED", message: text.slice(0, 200), setup_url: DENUE_TOKEN_SETUP_URL },
      data: null,
      http_status: res.status,
      retrieved_at: retrievedAt,
    };
  }
  let data;
  try {
    data = JSON.parse(text);
  } catch {
    return {
      ok: false,
      error: { class: "PARSE_ERROR", message: text.slice(0, 300) },
      data: null,
      http_status: res.status,
      retrieved_at: retrievedAt,
    };
  }
  const rows = Array.isArray(data) ? data : data ? [data] : [];
  return {
    ok: true,
    data: rows.map((r) => normalizeDenueRecord(r, { retrievedAt, sourceMode: "api" })),
    http_status: res.status,
    retrieved_at: retrievedAt,
    url_template: urlPath,
  };
}

/** Buscar around lat/lng (max 5000m per docs). */
export async function denueBuscar({ condicion, lat, lng, radioMeters = 500 }) {
  const cond = encodePathSegment(condicion || "hotel");
  const coords = `${lat},${lng}`;
  const dist = Math.min(5000, Math.max(1, Number(radioMeters) || 500));
  return apiGet(`/Buscar/${cond}/${coords}/${dist}`);
}

/** Nombre search by establishment name or razón social. */
export async function denueNombre({ nombre, entidad = "00", inicio = 1, fin = 20 }) {
  const n = encodePathSegment(nombre);
  const ent = String(ent).padStart(2, "0");
  return apiGet(`/Nombre/${n}/${ent}/${inicio}/${fin}`);
}

/** Ficha by establishment id. */
export async function denueFicha({ id }) {
  return apiGet(`/Ficha/${encodePathSegment(id)}`);
}

/**
 * Ensure state CSV zip is present under dataDir; download if missing.
 * Returns { ok, path, bytes, downloaded }.
 */
export async function ensureDenueStateCsvZip(cveEnt, dataDir) {
  const code = String(cveEnt).padStart(2, "0");
  fs.mkdirSync(dataDir, { recursive: true });
  const zipPath = path.join(dataDir, `denue_${code}_csv.zip`);
  if (fs.existsSync(zipPath) && fs.statSync(zipPath).size > 1000) {
    return { ok: true, path: zipPath, bytes: fs.statSync(zipPath).size, downloaded: false };
  }
  const url = DENUE_STATE_CSV_URL(code);
  const res = await fetch(url);
  if (!res.ok) {
    return { ok: false, path: zipPath, bytes: 0, downloaded: false, error: `HTTP ${res.status} for ${url}` };
  }
  const buf = Buffer.from(await res.arrayBuffer());
  fs.writeFileSync(zipPath, buf);
  return { ok: true, path: zipPath, bytes: buf.length, downloaded: true, url };
}

function unzipToDir(zipPath, outDir) {
  fs.mkdirSync(outDir, { recursive: true });
  // Windows PowerShell Expand-Archive; also try tar
  try {
    execFileSync(
      "powershell.exe",
      ["-NoProfile", "-Command", `Expand-Archive -Force -LiteralPath '${zipPath}' -DestinationPath '${outDir}'`],
      { stdio: "pipe" }
    );
  } catch {
    execFileSync("tar", ["-xf", zipPath, "-C", outDir], { stdio: "pipe" });
  }
}

function findCsvFiles(dir) {
  const out = [];
  if (!fs.existsSync(dir)) return out;
  for (const ent of fs.readdirSync(dir, { withFileTypes: true })) {
    const p = path.join(dir, ent.name);
    if (ent.isDirectory()) out.push(...findCsvFiles(p));
    else if (/\.csv$/i.test(ent.name)) out.push(p);
  }
  return out;
}

function parseCsvLine(line) {
  const cols = [];
  let cur = "";
  let inQ = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (inQ && line[i + 1] === '"') {
        cur += '"';
        i++;
      } else inQ = !inQ;
    } else if (ch === "," && !inQ) {
      cols.push(cur);
      cur = "";
    } else cur += ch;
  }
  cols.push(cur);
  return cols;
}

/**
 * Stream hotel-like rows from an extracted DENUE state CSV directory.
 * Official mass CSV headers (latin1): id, clee, nom_estab, raz_social, codigo_act, nombre_act, …
 * Filters in-stream to avoid loading full state into memory.
 */
export async function loadDenueHotelRowsFromCsvDir(csvDir, { retrievedAt = null } = {}) {
  const files = findCsvFiles(csvDir);
  const rows = [];
  const ts = retrievedAt || new Date().toISOString();
  for (const file of files) {
    // INEGI DENUE mass CSV is Latin-1 / Windows-1252 in practice
    const rl = createInterface({
      input: createReadStream(file, { encoding: "latin1" }),
      crlfDelay: Infinity,
    });
    let headers = null;
    for await (const line of rl) {
      if (!line || !line.trim()) continue;
      const cols = parseCsvLine(line);
      if (!headers) {
        headers = cols.map((h) => h.replace(/^\uFEFF/, "").replace(/^"|"$/g, "").trim());
        continue;
      }
      const obj = {};
      headers.forEach((h, i) => {
        const raw = cols[i] != null ? cols[i] : "";
        obj[h] = String(raw).replace(/^"|"$/g, "").trim();
      });
      const act =
        obj.nombre_act ||
        obj["Nombre de clase de la actividad"] ||
        obj["Nombre de la clase de actividad"] ||
        obj.Clase_actividad ||
        obj.actividad ||
        "";
      const code =
        obj.codigo_act ||
        obj["Código de la clase de actividad SCIAN"] ||
        obj["Codigo de la clase de actividad SCIAN"] ||
        obj.Codigo_Actividad ||
        "";
      const nombre =
        obj.nom_estab || obj["Nombre de la Unidad Económica"] || obj.Nombre || "";
      const isHotel = isDenueHotelEstablishment({
        codigo_act: code,
        nombre_act: act,
        nom_estab: nombre,
      });
      if (!isHotel) continue;
      const calle = [obj.tipo_vial, obj.nom_vial].filter(Boolean).join(" ") || obj.Calle || "";
      rows.push(
        normalizeDenueRecord(
          {
            CLEE: obj.clee || obj.CLEE,
            Id: obj.id || obj.Id || obj["Id del Establecimiento"],
            Nombre: nombre,
            Razon_social:
              obj.raz_social ||
              obj["Razón social"] ||
              obj["Razon social"] ||
              obj.Razon_social,
            Clase_actividad: act,
            Codigo_Actividad: code,
            Estrato: obj.per_ocu || obj["Personal ocupado (estrato)"] || obj.Estrato,
            Telefono: obj.telefono || obj.Telefono || obj.Teléfono,
            Correo_e: obj.correoelec || obj["Correo electrónico"] || obj.Correo_e,
            Sitio_internet: obj.www || obj["Sitio en Internet"],
            Calle: calle,
            Num_Exterior: obj.numero_ext || obj["Número exterior"] || obj["Número Exterior"],
            Num_Interior: obj.numero_int || obj["Número interior"],
            Colonia: obj.nomb_asent || obj["Nombre del asentamiento"] || obj.Colonia,
            CP: obj.cod_postal || obj["Código Postal"] || obj.CP,
            Ubicacion: [obj.localidad, obj.municipio, obj.entidad].filter(Boolean).join(", "),
            Latitud: obj.latitud || obj.Latitud,
            Longitud: obj.longitud || obj.Longitud,
            Fecha_Alta: obj.fecha_alta || obj["Fecha de alta"] || obj.Fecha_Alta,
          },
          { retrievedAt: ts, sourceMode: "state_csv_extract" }
        )
      );
    }
  }
  return rows;
}

/**
 * Prepare hotel subset index for a state (download+extract+filter once).
 */
export async function prepareDenueHotelIndexForEntidad(cveEnt, dataDir) {
  const code = String(cveEnt).padStart(2, "0");
  const indexPath = path.join(dataDir, `denue_${code}_hotels.json`);
  if (fs.existsSync(indexPath)) {
    const cached = JSON.parse(fs.readFileSync(indexPath, "utf8"));
    return { ok: true, from_cache: true, path: indexPath, count: cached.rows?.length || 0, meta: cached.meta };
  }
  const zip = await ensureDenueStateCsvZip(code, dataDir);
  if (!zip.ok) return { ok: false, error: zip.error };
  const extractDir = path.join(dataDir, `denue_${code}_extract`);
  unzipToDir(zip.path, extractDir);
  const retrievedAt = new Date().toISOString();
  const rows = await loadDenueHotelRowsFromCsvDir(extractDir, { retrievedAt });
  const payload = {
    meta: {
      cve_ent: code,
      source_zip: zip.path,
      source_url: DENUE_STATE_CSV_URL(code),
      zip_bytes: zip.bytes,
      retrieved_at: retrievedAt,
      hotel_filter: "SCIAN 7211* OR hotel/alojamiento activity name",
      note: "State extract — not national DENUE dump",
    },
    rows,
  };
  fs.writeFileSync(indexPath, `${JSON.stringify(payload)}\n`);
  return { ok: true, from_cache: false, path: indexPath, count: rows.length, meta: payload.meta };
}

export {
  getToken as _getDenueTokenPresenceOnly,
};
