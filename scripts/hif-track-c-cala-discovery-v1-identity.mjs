#!/usr/bin/env node
/**
 * Track C — DealSignal Global Source / CALA-First Discovery V1 → HPC identity.
 * Mexico first. Parents + unresolved identities = inventory only (no HPC).
 * CREATE_READY: address + brand-directory Official URL + geo collision clear.
 * CUSTOMER_BLOCKED. --apply / --apply-hpc-create
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { execSync } from "node:child_process";
import dotenv from "dotenv";
import {
  GRAPH_BASE_ID_CANONICAL,
  HPC_TABLE_ID,
  TABLE_HI_HOTEL_DISCOVERY_INVENTORY,
  MAP_DISCOVERY_INVENTORY,
} from "../lib/hotel-intelligence/sec-edgar/discovery-inventory-airtable-contract-v23.js";
import { buildDiscoveryInventoryFields } from "../lib/hotel-intelligence/sec-edgar/hif-foundation-persistence-v1.js";
import {
  resolveAuthenticatedFounderIdentity,
  persistHpcCreateAuthorization,
  executeAuthorizedHpcCreate,
} from "../lib/hotel-census/hpc-authorized-create-reuse-v1.js";
import { performLiveHpcIdentityCollisionCheck } from "../lib/hotel-census/hpc-live-identity-collision-v1.js";
import {
  TRACK_C_STANDING_AUTH,
  inferFamilySelect,
  inferBrandFamilyFromLabel,
} from "../lib/hotel-intelligence/sec-edgar/hif-track-c-universe-expansion-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) {
    dotenv.config({ path: p, override: false });
    break;
  }
}
const APPLY = process.argv.includes("--apply");
const APPLY_HPC = process.argv.includes("--apply-hpc-create");
const OUT = path.join(ROOT, "reports/sec-edgar-hif-track-c-cala-discovery-v1-identity");
fs.mkdirSync(OUT, { recursive: true });
const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
if (!token) {
  console.error("NO_AIRTABLE_TOKEN");
  process.exit(2);
}
const ids = JSON.parse(
  fs.readFileSync(path.join(ROOT, "lib/hotel-intelligence/sec-edgar/discovery-inventory-table-ids.generated.json"), "utf8"),
);
const inventoryTableId = ids.tableIds[TABLE_HI_HOTEL_DISCOVERY_INVENTORY];

const BRAND_RE =
  /marriott\.com|hilton\.com|ihg\.com|hyatt\.com|resortsbyhyatt\.com|wyndham|choicehotels|accor|kimptonhotels|tapestry/i;

/**
 * action:
 *  - HOTEL: match/create under CREATE_READY
 *  - PARENT_ONLY: destination/portfolio — inventory notes only, never HPC
 *  - UNRESOLVED: no hotel proof — inventory notes only, never HPC
 */
const QUEUE = [
  {
    action: "HOTEL",
    priority: "MX_P0",
    discoveryPropertyId: "disc_ds_cala_v1_finn_courtyard_pdc_2026",
    findingId: "find_ds_cala_v1_finn_courtyard_pdc_committed_2026",
    hotelName: "Courtyard by Marriott Playa del Carmen",
    aliases: [
      "Courtyard Playa del Carmen",
      "Wyndham Garden Playa del Carmen",
      "Wyndham Garden Playa Del Carmen",
    ],
    city: "Playa del Carmen",
    stateRegion: "Quintana Roo",
    country: "Mexico",
    address: "Avenida Aviación Lote 5 y 6 Manzana 29, Fraccionamiento Playacar Fase II",
    brand: "Courtyard",
    brandUrl: "https://www.marriott.com/en-us/hotels/cunpy-courtyard-playa-del-carmen/overview/",
    rooms: null,
    owner: "Fibra Inn (FINN13 / Trust DB/1616)",
    operator: "Servicios Hoteleros FINN",
    operatorNote: "Internalized ops; franchise/license Marriott Courtyard — brand≠operator; Aimbridge Latam exited",
    relationshipNote: "Franchise (Marriott) + owner-operator (FINN); prior brand Wyndham Garden",
    sources: [
      "https://www.marriott.com/en-us/hotels/cunpy-courtyard-playa-del-carmen/overview/",
      "https://www.bmv.com.mx/docs-pub/eventfid/eventfid_1593193_1616_1.pdf",
      "https://fibrainn.mx/storage/docs/fibra-inn-announces-the-conversion-of-one-of-its-hotels-to-courtyard-by-marriott-playa-del-carmen.pdf",
    ],
    handoffFrom: "DEALSIGNAL_CALA_DISCOVERY_V1",
    // Pre-conversion HPC (same Playacar address) — steward merge candidate, not a second hotel
    knownSamePropertyHpc: ["recf65FZ1FuVVqIrz"],
  },
  {
    action: "HOTEL",
    priority: "MX_P0",
    discoveryPropertyId: "disc_ds_cala_v1_fiho_yucatan_resort_2026",
    findingId: "find_ds_cala_v1_fiho_yucatan_resort_lease_2026",
    hotelName: "The Yucatán Resort Playa del Carmen",
    aliases: [
      "The Yucatan Resort Playa del Carmen",
      "The Yucatan Playa del Carmen, Tapestry Collection by Hilton",
      "The Yucatán Playa del Carmen",
    ],
    city: "Playa del Carmen",
    stateRegion: "Quintana Roo",
    country: "Mexico",
    address: "Avenida 1 Norte, entre 30 y 32, Gonzalo Guerrero",
    brand: "Tapestry Collection by Hilton",
    brandUrl: "https://www.hilton.com/en/hotels/czmpdup-the-yucatan-playa-del-carmen/",
    rooms: 60,
    owner: "FibraHotel (FIHO 12 / Trust F/1596)",
    operator: null,
    operatorNote:
      "Ops suspended 2026-04-30 for fixed-rent lease portfolio; prior Playa Hotels & Resorts AI op — current lease operator TBD; do not invent",
    relationshipNote: "Owner FibraHotel; brand Hilton Tapestry historically; lease-model transition — operator≠owner",
    sources: [
      "https://www.hilton.com/en/hotels/czmpdup-the-yucatan-playa-del-carmen/",
      "https://bmv.com.mx/docs-pub/eventfid/eventfid_1574632_1596_1.pdf",
      "https://www.hrratings.com/pdf/Reporte_FIHO_NRSRO__2026.pdf",
    ],
    handoffFrom: "DEALSIGNAL_CALA_DISCOVERY_V1",
  },
  {
    action: "HOTEL",
    priority: "MX_P0",
    discoveryPropertyId: "disc_ds_cala_v1_finn_hi_monterrey_valle_2026",
    findingId: "find_ds_cala_v1_finn_hi_monterrey_valle_2026",
    hotelName: "Holiday Inn Monterrey Valle",
    aliases: ["Holiday Inn Monterrey Valle by IHG"],
    city: "San Pedro Garza García",
    stateRegion: "Nuevo León",
    country: "Mexico",
    address: "Avenida Lázaro Cárdenas 2305, Colonia Residencial San Agustín",
    brand: "Holiday Inn",
    brandUrl: "https://www.ihg.com/holidayinn/hotels/us/en/monterrey/mtyzv/hoteldetail",
    rooms: 198,
    owner: "Fibra Inn (FINN13 / Trust DB/1616)",
    operator: "Servicios Hoteleros FINN",
    operatorNote: "Internalized; IHG franchise renewal after prior expiry — franchise≠HMA",
    relationshipNote: "Owner-operator FINN + IHG franchise",
    sources: [
      "https://www.ihg.com/holidayinn/hotels/us/en/monterrey/mtyzv/hoteldetail",
      "https://fibrainn.mx/assets/docs/2025-corporate-governance-letters-updated_69d5ae218a163.pdf",
      "https://fibrainn.mx/en/portfolio.html",
    ],
    handoffFrom: "DEALSIGNAL_CALA_DISCOVERY_V1",
  },
  {
    action: "HOTEL",
    priority: "MX_P0",
    discoveryPropertyId: "disc_ds_cala_v1_finn_hi_puebla_la_noria_2026",
    findingId: "find_ds_cala_v1_finn_hi_puebla_la_noria_2026",
    hotelName: "Holiday Inn Puebla La Noria",
    aliases: ["Holiday Inn Puebla La Noria Hotel", "Holiday Inn La Noria"],
    city: "Puebla",
    stateRegion: "Puebla",
    country: "Mexico",
    address: "Circuito Interior Juan Pablo II 1936, Col. Ex-Hacienda La Noria",
    brand: "Holiday Inn",
    brandUrl: "https://www.ihg.com/holidayinn/hotels/us/en/puebla/pueln/hoteldetail",
    rooms: 150,
    owner: "Fibra Inn (FINN13 / Trust DB/1616)",
    operator: "Servicios Hoteleros FINN",
    operatorNote: "Non-renewal notice Jul 2025 context in FINN YE notes; brand may change — verify live franchise; operator internalized",
    relationshipNote: "Owner-operator FINN + IHG franchise (non-renewal signal monitored)",
    sources: [
      "https://www.ihg.com/holidayinn/hotels/us/en/puebla/pueln/hoteldetail",
      "https://fibrainn.mx/assets/docs/f1616-fibra-inn-notas-ingles-4t2025_6a875905dc66b.pdf",
      "https://fibrainn.mx/assets/docs/esg-report-2025-fibra-inn-hg_69f93dd0a2b90.pdf",
    ],
    handoffFrom: "DEALSIGNAL_CALA_DISCOVERY_V1",
  },
  {
    action: "PARENT_ONLY",
    priority: "DO_PARENT",
    discoveryPropertyId: "disc_ds_cala_v1_cabo_rojo_puntacana_2026",
    findingId: "find_ds_cala_v1_cabo_rojo_puntacana_committed_2026",
    hotelName: "Cabo Rojo / Pedernales Tourism Destination (parent — not a hotel)",
    city: null,
    stateRegion: "Pedernales",
    country: "Dominican Republic",
    address: null,
    brand: null,
    brandUrl: null,
    owner: "Estado Dominicano via Fideicomiso Pro-Pedernales; Consorcio Cabo Rojo (Grupo Puntacana lead)",
    operator: "Consorcio Cabo Rojo",
    operatorNote: "Destination PPP partner COMMITTED — individual hotels TBD; no portfolio HPC",
    sources: [
      "https://propedernales.do/licitacion-adjudica-a-consorcio-cabo-rojo-encabezado-por-grupo-puntacana-como-socio-minoritario-del-gobierno-para-el-desarrollo-turistico-de-pedernales/",
      "https://grupopuntacana.com.do/es/notas-de-prensa/item/consorcio-liderado-por-el-grupo-puntacana-afi-reservas-y-afi-popular-asumen-la-gestion-del-destino-cabo-rojo",
    ],
    handoffFrom: "DEALSIGNAL_CALA_DISCOVERY_V1",
    skipHpcReason: "DESTINATION_PARENT_NOT_HOTEL",
  },
  {
    action: "UNRESOLVED",
    priority: "CO_WATCH",
    discoveryPropertyId: "disc_ds_cala_v1_santander_concession_watch_2026",
    findingId: "find_ds_cala_v1_santander_concession_watch_2026",
    hotelName: "Departamento de Santander — concesión inmueble (identity TBD)",
    city: "Bucaramanga",
    stateRegion: "Santander",
    country: "Colombia",
    address: null,
    brand: null,
    brandUrl: null,
    owner: "Departamento de Santander",
    operator: null,
    operatorNote: "Hotel identity unverified in SECOP aggregator — do not create HPC until pliego proves hotel",
    sources: [
      {
        url: "https://procesosecop.com/proceso/CO1.REQ.11369786",
        limitation: "Confirm SECOP II; hotel use unproven",
      },
    ],
    handoffFrom: "DEALSIGNAL_CALA_DISCOVERY_V1",
    skipHpcReason: "HOTEL_IDENTITY_UNPROVEN",
  },
  {
    action: "PARENT_ONLY",
    priority: "MX_PARENT",
    discoveryPropertyId: "disc_ds_cala_v1_finn_internalization_parent_2026",
    findingId: "find_ds_cala_v1_finn_internalization_franchise_2026",
    hotelName: "Fibra Inn — Internalization + Franchise Flexibility Portfolio (parent)",
    city: null,
    stateRegion: null,
    country: "Mexico",
    address: null,
    brand: null,
    brandUrl: null,
    owner: "Fibra Inn (FINN13 / Trust DB/1616)",
    operator: "Servicios Hoteleros FINN",
    operatorNote: "Portfolio parent — links Courtyard PDC, HI Monterrey Valle, HI Puebla La Noria; not a hotel HPC",
    sources: [
      "https://fibrainn.mx/storage/docs/fibra-inn-finaliza-proceso-de-internalizacion-de-la-operacion-en-28-hoteles.pdf",
      "https://fibrainn.mx/assets/docs/2025-corporate-governance-letters-updated_69d5ae218a163.pdf",
    ],
    handoffFrom: "DEALSIGNAL_CALA_DISCOVERY_V1",
    skipHpcReason: "PORTFOLIO_PARENT_NOT_HOTEL",
  },
];

function writeJson(name, obj) {
  fs.writeFileSync(path.join(OUT, name), `${JSON.stringify(obj, null, 2)}\n`);
}
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
function sourceUrls(h) {
  return (h.sources || [])
    .map((s) => (typeof s === "string" ? s : s?.url))
    .filter(Boolean);
}
async function getRecord(tableId, recordId) {
  const res = await fetch(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${tableId}/${recordId}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  return { ok: res.ok, json: await res.json() };
}
async function patchRecord(tableId, recordId, fields) {
  const res = await fetch(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${tableId}/${recordId}`, {
    method: "PATCH",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify({ fields, typecast: true }),
  });
  return { ok: res.ok, json: await res.json() };
}
async function createRecord(tableId, fields) {
  const res = await fetch(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${tableId}`, {
    method: "POST",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify({ fields, typecast: true }),
  });
  return { ok: res.ok, json: await res.json() };
}
async function findByDiscId(discId) {
  const formula = `{${MAP_DISCOVERY_INVENTORY.discoveryPropertyId}}="${discId}"`;
  const url =
    `https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${inventoryTableId}` +
    `?filterByFormula=${encodeURIComponent(formula)}&maxRecords=1`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
  const json = await res.json();
  return json.records?.[0] || null;
}

function countryMatches(expected, actual) {
  const e = String(expected || "").toLowerCase();
  const a = String(actual || "").toLowerCase();
  if (!e || !a) return false;
  if (e.includes("mexico") || e === "mx") return /mexico|méxico|\bmx\b/.test(a);
  if (e.includes("dominican") || e === "do") return /dominican|dominicana|\bdo\b/.test(a);
  if (e.includes("colombia") || e === "co") return /colombia|\bco\b/.test(a);
  return a.includes(e.slice(0, 5)) || e.includes(a.slice(0, 5));
}

function addressCompatible(expectedAddr, hpcAddr) {
  const toks = String(expectedAddr || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .split(" ")
    .filter((t) => t.length > 2 && !["colonia", "avenida", "fraccionamiento", "entre"].includes(t));
  if (!toks.length) return true;
  const h = String(hpcAddr || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
  const hits = toks.filter((t) => h.includes(t)).length;
  return hits >= Math.min(2, toks.length);
}

function cityCompatible(expectedCity, hpcCity) {
  const cityA = String(hpcCity || "").toLowerCase();
  const cityE = String(expectedCity || "").toLowerCase();
  if (!cityE) return true;
  return (
    cityA.includes(cityE.slice(0, 5)) ||
    cityE.includes(cityA.slice(0, 5)) ||
    (cityE.includes("san pedro") && /monterrey|garza/.test(cityA)) ||
    (cityA.includes("san pedro") && /monterrey|garza/.test(cityE))
  );
}

function brandUrlCompatible(expectedUrl, hpcUrl) {
  const e = String(expectedUrl || "").toLowerCase();
  const a = String(hpcUrl || "").toLowerCase();
  if (!e || !a) return false;
  // Marriott/IHG/Hilton property codes in path
  const code = e.match(/\/([a-z]{5})[-/]/)?.[1] || e.match(/\/hotels\/[a-z]{2}\/en\/[^/]+\/([a-z0-9]+)\/?/i)?.[1];
  if (code && a.includes(code)) return true;
  if (e.includes("cunpy") && a.includes("cunpy")) return true;
  if (e.includes("pueln") && a.includes("pueln")) return true;
  if (e.includes("mtyzv") && a.includes("mtyzv")) return true;
  if (e.includes("czmpdup") && a.includes("czmpdup")) return true;
  return false;
}

function candidatePassesGeo(h, cand) {
  if (!countryMatches(h.country, cand.country)) return false;
  const cityOk = cityCompatible(h.city, cand.city);
  const addrOk = h.address ? addressCompatible(h.address, cand.address) : false;
  const urlOk = brandUrlCompatible(h.brandUrl, cand.officialUrl);
  // Accept if country + (address OR (city + brand URL code))
  if (addrOk) return true;
  if (cityOk && urlOk) return true;
  // Exact property-name + city + country with no competing stronger address elsewhere — weak; require url or addr
  return false;
}

/**
 * Resolve single-id collision OR multi-candidate REQUIRES_REVIEW via geo/URL disambiguation.
 * Prefer current-brand URL match over historical alias when both are same property.
 */
async function resolveCollision(h, collision) {
  const candidates = [];
  if (collision?.hpcRecordId) {
    candidates.push({
      id: collision.hpcRecordId,
      name: collision.candidates?.[0]?.name,
      city: collision.candidates?.[0]?.city,
      country: collision.candidates?.[0]?.country,
      address: collision.candidates?.[0]?.address,
      officialUrl: collision.candidates?.[0]?.officialUrl,
      brand: collision.candidates?.[0]?.brand,
    });
  }
  for (const c of collision?.candidates || []) {
    if (!candidates.some((x) => x.id === c.id)) {
      candidates.push({
        id: c.id,
        name: c.name || c.canonicalName,
        city: c.city,
        country: c.country,
        address: c.address,
        officialUrl: c.officialUrl,
        brand: c.brand,
      });
    }
  }
  if (!candidates.length) return { accept: false, reason: "NO_CANDIDATES" };

  const scored = [];
  for (const cand of candidates) {
    // Always re-read for authoritative fields
    const rb = await getRecord(HPC_TABLE_ID, cand.id);
    if (!rb.ok) continue;
    const f = rb.json.fields || {};
    const mapped = {
      id: rb.json.id,
      name: f["Property Name"],
      address: f.Address,
      city: f.City,
      country: f.Country,
      brand: f["Current Brand"],
      officialUrl: f["Official Property URL"],
    };
    if (!candidatePassesGeo(h, mapped)) continue;
    let score = 0;
    if (addressCompatible(h.address, mapped.address)) score += 3;
    if (brandUrlCompatible(h.brandUrl, mapped.officialUrl)) score += 4;
    if (
      String(mapped.brand || "")
        .toLowerCase()
        .includes(String(h.brand || "").toLowerCase().slice(0, 6))
    ) {
      score += 2;
    }
    // Prefer current Courtyard over historical Wyndham when converting
    if (/wyndham/i.test(mapped.brand || "") && /courtyard/i.test(h.brand || "")) score -= 1;
    scored.push({ ...mapped, score });
  }

  if (!scored.length) {
    return {
      accept: false,
      reason: "NO_GEO_SAFE_CANDIDATE",
      rejected: candidates.map((c) => ({ id: c.id, name: c.name, city: c.city })),
    };
  }

  scored.sort((a, b) => b.score - a.score);
  const best = scored[0];
  const siblings = scored.slice(1).filter((s) => s.score >= best.score - 2);
  // If two same-score different hotels (e.g. La Noria vs Finsa), require address win
  if (scored.length > 1 && scored[1].score === best.score) {
    const addrWinners = scored.filter((s) => addressCompatible(h.address, s.address));
    if (addrWinners.length === 1) {
      return {
        accept: true,
        hpcRead: addrWinners[0],
        collisionDisambiguation: "ADDRESS_UNIQUE",
        samePropertySiblings: scored.filter((s) => s.id !== addrWinners[0].id).map((s) => s.id),
      };
    }
    return {
      accept: false,
      reason: "AMBIGUOUS_EQUAL_SCORE",
      rejected: scored.map((s) => ({ id: s.id, name: s.name, score: s.score })),
    };
  }

  return {
    accept: true,
    hpcRead: best,
    collisionDisambiguation: siblings.length ? "MULTI_SAME_PROPERTY_OR_NEAR" : "SINGLE_GEO_SAFE",
    samePropertySiblings: siblings.map((s) => s.id),
  };
}

function buildNotes(h, hpcId) {
  return [
    "CUSTOMER_BLOCKED",
    "Track C CALA Discovery V1 identity",
    `handoffFrom=${h.handoffFrom}`,
    h.findingId ? `finding=${h.findingId}` : null,
    `action=${h.action}`,
    h.skipHpcReason ? `skipHpc=${h.skipHpcReason}` : null,
    "do_not_requalify_commercial",
    "geography=HOTEL_PROPERTY_NOT_OWNER_HQ",
    h.owner ? `owner_verified=${h.owner}` : "owner=UNVERIFIED",
    h.operator ? `operator_verified=${h.operator}` : null,
    h.operatorNote ? `operatorNote=${h.operatorNote}` : null,
    h.relationshipNote ? `rel=${h.relationshipNote}` : null,
    "brand_neq_operator",
    h.rooms != null ? `rooms_sourced=${h.rooms}` : null,
    hpcId ? `hpc=${hpcId}` : null,
    ...sourceUrls(h).slice(0, 4).map((u, i) => `src${i + 1}=${u}`),
  ]
    .filter(Boolean)
    .join(" | ");
}

async function upsertInventoryOnly(h, ledger) {
  const notes = buildNotes(h, null);
  const existing = await findByDiscId(h.discoveryPropertyId);
  if (existing) {
    if (APPLY) {
      const patch = await patchRecord(inventoryTableId, existing.id, {
        [MAP_DISCOVERY_INVENTORY.hpcMatchStatus]: "DISCOVERED_NOT_IN_HPC",
        [MAP_DISCOVERY_INVENTORY.notesQuarantine]: notes,
        [MAP_DISCOVERY_INVENTORY.identityClass]:
          h.action === "UNRESOLVED" ? "AMBIGUOUS_PROPERTY" : "GENERIC_BRAND_OR_COMPANY",
      });
      ledger.summary.inventoryLinked += patch.ok ? 1 : 0;
      if (!patch.ok) ledger.summary.failures += 1;
      return { inventoryId: existing.id, inventoryAction: patch.ok ? "NOTES_UPDATED" : "PATCH_FAILED" };
    }
    return { inventoryId: existing.id, inventoryAction: "WOULD_UPDATE_NOTES" };
  }
  if (APPLY) {
    const payload = {
      discoveryPropertyId: h.discoveryPropertyId,
      hotelName: h.hotelName,
      hotelNameNormalized: h.hotelName.toLowerCase(),
      hotelNameRaw: h.hotelName,
      city: h.city,
      stateRegion: h.stateRegion,
      country: h.country,
      address: h.address,
      currentBrand: h.brand,
      identityClass: h.action === "UNRESOLVED" ? "AMBIGUOUS_PROPERTY" : "GENERIC_BRAND_OR_COMPANY",
      evidenceConfidence: h.action === "UNRESOLVED" ? "LOW" : "HIGH",
      sourceUrl: sourceUrls(h)[0] || null,
      discoveryMethod: "TRACK_C_CALA_DISCOVERY_V1_IDENTITY",
      hpcMatchStatus: "DISCOVERED_NOT_IN_HPC",
      notes,
      verified: h.action !== "UNRESOLVED",
      evidenceSupported: true,
    };
    const fields = buildDiscoveryInventoryFields(payload);
    const created = await createRecord(inventoryTableId, fields);
    if (created.ok) {
      ledger.summary.inventoryCreated += 1;
      return { inventoryId: created.json.id, inventoryAction: "CREATED_PARENT_OR_WATCH" };
    }
    ledger.summary.failures += 1;
    return { inventoryId: null, inventoryAction: "CREATE_FAILED", error: created.json };
  }
  return { inventoryId: null, inventoryAction: "WOULD_CREATE_PARENT_OR_WATCH" };
}

async function main() {
  let founderAuth = null;
  if (APPLY_HPC) {
    const gitName = execSync("git config user.name", { encoding: "utf8" }).trim();
    const gitEmail = execSync("git config user.email", { encoding: "utf8" }).trim();
    founderAuth = resolveAuthenticatedFounderIdentity({
      envValue: process.env.HPC_CREATE_APPROVER_IDENTITY || "",
      gitName,
      gitEmail,
    });
    if (!founderAuth.ok) {
      console.error("FOUNDER_AUTH_FAILED", founderAuth);
      process.exit(3);
    }
  }

  const ledger = {
    asOf: new Date().toISOString(),
    mode: APPLY ? (APPLY_HPC ? "APPLY_WITH_HPC_CREATE" : "APPLY_LINKS") : "DRY_RUN",
    publicationState: "CUSTOMER_BLOCKED",
    paidResearchUsd: 0,
    operations: [],
    summary: {
      examined: 0,
      mexicoHotels: 0,
      matches: 0,
      creates: 0,
      parentOnly: 0,
      unresolvedSkipped: 0,
      researchRequired: 0,
      ambiguousRejected: 0,
      inventoryLinked: 0,
      inventoryCreated: 0,
      failures: 0,
      readBackOk: 0,
    },
  };

  const rows = [];
  const heItems = [];

  for (const h of QUEUE) {
    ledger.summary.examined += 1;

    if (h.action === "PARENT_ONLY" || h.action === "UNRESOLVED") {
      if (h.action === "PARENT_ONLY") ledger.summary.parentOnly += 1;
      else ledger.summary.unresolvedSkipped += 1;
      const inv = await upsertInventoryOnly(h, ledger);
      rows.push({
        priority: h.priority,
        action: h.action,
        discoveryPropertyId: h.discoveryPropertyId,
        findingId: h.findingId,
        hotelName: h.hotelName,
        country: h.country,
        city: h.city,
        owner: h.owner || null,
        operator: h.operator || null,
        canonicalHpcRecordId: null,
        matchOrCreate: "SKIPPED_NO_HPC",
        skipHpcReason: h.skipHpcReason,
        inventoryAirtableId: inv.inventoryId,
        inventoryAction: inv.inventoryAction,
        gaps: [h.skipHpcReason],
        sources: sourceUrls(h),
        heDeepLinkTarget: null,
        hotelExplorerHandoffStatus: "NOT_APPLICABLE",
      });
      continue;
    }

    ledger.summary.mexicoHotels += 1;
    const brandUrl = h.brandUrl && BRAND_RE.test(h.brandUrl) ? h.brandUrl : null;
    const gaps = [];
    if (!h.address) gaps.push("MISSING_PHYSICAL_ADDRESS");
    if (!brandUrl) gaps.push("MISSING_BRAND_DIRECTORY_OFFICIAL_URL");

    let hpcId = null;
    let matchOrCreate = "NONE";
    let hpcRead = null;
    let collisionMeta = null;

    // Alias-aware collision + multi-candidate geo/URL disambiguation (no duplicate creates)
    let samePropertySiblings = [];
    try {
      const collision = await performLiveHpcIdentityCollisionCheck({
        hotelName: h.hotelName,
        aliases: h.aliases || [h.hotelName],
        city: h.city,
        brand: h.brand,
        address: h.address,
        country: h.country,
        officialUrl: brandUrl,
        adpPropertyId: h.discoveryPropertyId,
      });
      collisionMeta = {
        outcome: collision?.outcome || null,
        rawId: collision?.hpcRecordId || null,
        candidateCount: (collision?.candidates || []).length,
      };
      if (
        collision?.hpcRecordId ||
        collision?.outcome === "HPC_IDENTITY_COLLISION_REQUIRES_REVIEW" ||
        (collision?.candidates || []).length
      ) {
        const verdict = await resolveCollision(h, collision);
        if (verdict.accept) {
          hpcId = verdict.hpcRead.id;
          matchOrCreate = "MATCHED_EXISTING";
          hpcRead = verdict.hpcRead;
          samePropertySiblings = [
            ...new Set([...(verdict.samePropertySiblings || []), ...(h.knownSamePropertyHpc || [])]),
          ].filter((id) => id !== hpcId);
          collisionMeta.disambiguation = verdict.collisionDisambiguation;
          collisionMeta.samePropertySiblings = samePropertySiblings;
          ledger.summary.matches += 1;
          ledger.summary.readBackOk += 1;
          // Non-destructive address fill when matched Courtyard lacks street address
          if (
            APPLY &&
            h.address &&
            !String(hpcRead.address || "").trim() &&
            brandUrlCompatible(brandUrl, hpcRead.officialUrl)
          ) {
            const patchAddr = await patchRecord(HPC_TABLE_ID, hpcId, { Address: h.address });
            ledger.operations.push({
              hotel: h.hotelName,
              action: "FILL_ADDRESS_NONDESTRUCTIVE",
              ok: patchAddr.ok,
              hpcRecordId: hpcId,
            });
            if (patchAddr.ok) hpcRead.address = h.address;
          }
        } else {
          ledger.summary.ambiguousRejected += 1;
          collisionMeta.rejected = verdict;
          ledger.operations.push({
            hotel: h.hotelName,
            action: "REJECT_COLLISION",
            reason: verdict.reason,
            rejected: verdict.rejected || null,
          });
        }
      }
    } catch (e) {
      gaps.push(`COLLISION_ERROR:${e.message}`);
      collisionMeta = { outcome: "LOOKUP_ERROR", error: e.message };
    }

    const createReady = !hpcId && Boolean(h.address && brandUrl);
    if (!hpcId && !createReady) ledger.summary.researchRequired += 1;

    if (createReady && APPLY_HPC && founderAuth?.ok) {
      const fields = {
        "Property Name": h.hotelName,
        "Canonical Property Name": h.hotelName,
        Country: "Mexico",
        City: h.city,
        "State / Region": h.stateRegion || null,
        Address: h.address,
        "Current Brand": h.brand,
        "Affiliation Status": "Branded",
        "Family / Source Family": inferFamilySelect(h.brand),
        "Official Property URL": brandUrl,
      };
      for (const k of Object.keys(fields)) {
        if (fields[k] == null || fields[k] === "") delete fields[k];
      }
      const auth = persistHpcCreateAuthorization({
        runId: `track_c_cala_disc_v1_${new Date().toISOString().slice(0, 10)}`,
        draftPropertyId: h.discoveryPropertyId,
        hotelName: h.hotelName,
        fields,
        approverIdentity: founderAuth.approverIdentity,
        authorizationSource: `${TRACK_C_STANDING_AUTH.source}_CALA_DISCOVERY_V1_IDENTITY`,
      });
      if (!auth.ok) {
        gaps.push(`AUTH:${auth.error}`);
        ledger.summary.failures += 1;
      } else {
        const exec = await executeAuthorizedHpcCreate({
          runId: `track_c_cala_disc_v1_${new Date().toISOString().slice(0, 10)}`,
          authorization: auth.record,
          dryRun: false,
          apply: true,
          simulation: false,
        });
        ledger.operations.push({
          hotel: h.hotelName,
          action: "HPC_CREATE",
          ok: exec.ok,
          hpcRecordId: exec.hpcRecordId || null,
          state: exec.state,
        });
        if (exec.ok && exec.hpcRecordId) {
          hpcId = exec.hpcRecordId;
          matchOrCreate = exec.reusedExisting ? "REUSED_EXISTING" : "CREATED";
          ledger.summary.creates += 1;
          const rb = await getRecord(HPC_TABLE_ID, hpcId);
          if (rb.ok) {
            const f = rb.json.fields || {};
            hpcRead = {
              id: rb.json.id,
              name: f["Property Name"],
              address: f.Address,
              city: f.City,
              country: f.Country,
              brand: f["Current Brand"],
              officialUrl: f["Official Property URL"],
            };
            ledger.summary.readBackOk += 1;
          }
        } else {
          gaps.push(`CREATE_FAIL:${exec.error || exec.state}`);
          ledger.summary.failures += 1;
        }
        await sleep(200);
      }
    } else if (createReady && !APPLY_HPC) {
      matchOrCreate = "WOULD_CREATE";
      ledger.operations.push({ hotel: h.hotelName, action: "WOULD_HPC_CREATE", brandUrl });
    }

    const notes = [
      buildNotes(h, hpcId),
      samePropertySiblings.length
        ? `samePropertySiblingHpc=${samePropertySiblings.join(",")}`
        : null,
    ]
      .filter(Boolean)
      .join(" | ");
    let inventoryId = null;
    let invAction = "NONE";
    const existing = await findByDiscId(h.discoveryPropertyId);
    if (existing) {
      inventoryId = existing.id;
      invAction = "EXISTING";
      if (APPLY && hpcId) {
        const patch = await patchRecord(inventoryTableId, inventoryId, {
          [MAP_DISCOVERY_INVENTORY.hpcMatchStatus]: "MATCHED",
          [MAP_DISCOVERY_INVENTORY.hpcHotel]: [hpcId],
          [MAP_DISCOVERY_INVENTORY.notesQuarantine]: notes,
          ...(brandUrl ? { [MAP_DISCOVERY_INVENTORY.sourceFilingUrl]: brandUrl } : {}),
          ...(h.address ? { [MAP_DISCOVERY_INVENTORY.address]: h.address } : {}),
          ...(h.brand ? { [MAP_DISCOVERY_INVENTORY.currentBrand]: h.brand } : {}),
        });
        invAction = patch.ok ? "LINKED" : "LINK_FAILED";
        if (patch.ok) ledger.summary.inventoryLinked += 1;
        else ledger.summary.failures += 1;
      } else if (!APPLY && hpcId) {
        invAction = "WOULD_LINK";
      }
    } else if (APPLY && (hpcId || createReady)) {
      const payload = {
        discoveryPropertyId: h.discoveryPropertyId,
        hotelName: h.hotelName,
        hotelNameNormalized: h.hotelName.toLowerCase(),
        hotelNameRaw: h.hotelName,
        city: h.city,
        stateRegion: h.stateRegion,
        country: "Mexico",
        address: h.address,
        currentBrand: h.brand,
        brandFamily: inferBrandFamilyFromLabel(h.brand),
        identityClass: "VERIFIED_PROPERTY",
        evidenceConfidence: "HIGH",
        sourceUrl: brandUrl || sourceUrls(h)[0] || null,
        discoveryMethod: "TRACK_C_CALA_DISCOVERY_V1_IDENTITY",
        hpcMatchStatus: hpcId ? "MATCHED" : "DISCOVERED_NOT_IN_HPC",
        notes,
        verified: true,
        evidenceSupported: true,
      };
      const fields = buildDiscoveryInventoryFields(payload);
      if (hpcId) fields[MAP_DISCOVERY_INVENTORY.hpcHotel] = [hpcId];
      if (inferBrandFamilyFromLabel(h.brand)) {
        fields[MAP_DISCOVERY_INVENTORY.brandFamily] = inferBrandFamilyFromLabel(h.brand);
      }
      if (h.aliases?.length) {
        fields[MAP_DISCOVERY_INVENTORY.historicalAliases] = h.aliases.join("; ");
      }
      const created = await createRecord(inventoryTableId, fields);
      if (created.ok) {
        inventoryId = created.json.id;
        invAction = "CREATED";
        ledger.summary.inventoryCreated += 1;
        if (hpcId) ledger.summary.inventoryLinked += 1;
      } else {
        invAction = "CREATE_FAILED";
        gaps.push(`INV:${JSON.stringify(created.json?.error || created.json)}`);
        ledger.summary.failures += 1;
      }
      await sleep(100);
    } else if (!existing) {
      invAction = createReady || hpcId ? "WOULD_CREATE_OR_LINK_INVENTORY" : "NO_INVENTORY_WITHOUT_GATE";
    }

    let idempotencyOk = null;
    if (APPLY && inventoryId && hpcId) {
      const inv2 = await getRecord(inventoryTableId, inventoryId);
      const linked = inv2.ok ? inv2.json.fields?.[MAP_DISCOVERY_INVENTORY.hpcHotel] || [] : [];
      const status = inv2.ok ? inv2.json.fields?.[MAP_DISCOVERY_INVENTORY.hpcMatchStatus] : null;
      idempotencyOk = linked[0] === hpcId && status === "MATCHED";
    }

    const row = {
      priority: h.priority,
      action: h.action,
      discoveryPropertyId: h.discoveryPropertyId,
      findingId: h.findingId,
      hotelName: h.hotelName,
      aliases: h.aliases || [],
      country: h.country,
      city: h.city,
      address: h.address,
      brand: h.brand,
      brandUrl,
      rooms: h.rooms,
      owner: h.owner || null,
      operator: h.operator || null,
      operatorNote: h.operatorNote || null,
      relationshipNote: h.relationshipNote || null,
      sources: sourceUrls(h),
      handoffFrom: h.handoffFrom,
      canonicalHpcRecordId: hpcId,
      matchOrCreate,
      collision: collisionMeta,
      samePropertySiblings,
      inventoryAirtableId: inventoryId,
      inventoryAction: invAction,
      gaps,
      idempotencyOk,
      heDeepLinkTarget: hpcId
        ? `http://localhost:8080/app#/opportunity-radar (open HE by HPC ${hpcId})`
        : null,
      hotelExplorerHandoffStatus: hpcId ? "READY" : gaps.length ? "RESEARCH_REQUIRED" : "PENDING",
      hpcReadBack: hpcRead,
    };
    rows.push(row);

    if (hpcId) {
      heItems.push({
        hpcRecordId: hpcId,
        verifiedHotelName: hpcRead?.name || h.hotelName,
        heDeepLinkTarget: row.heDeepLinkTarget,
        brand: hpcRead?.brand || h.brand,
        operator: h.operator,
        operatorNote: h.operatorNote,
        owner: h.owner,
        physicalAddress: hpcRead?.address || h.address,
        city: hpcRead?.city || h.city,
        country: hpcRead?.country || h.country,
        roomsSourced: h.rooms,
        officialHotelUrl: hpcRead?.officialUrl || brandUrl,
        aliases: h.aliases,
        discoveryPropertyId: h.discoveryPropertyId,
        findingId: h.findingId,
        inventoryAirtableId: inventoryId,
        sourceEvidence: sourceUrls(h),
        ownershipIntelligence: {
          ownerVerified: Boolean(h.owner),
          operatorVerified: Boolean(h.operator),
          differentiateOwnerVsManager: true,
          relationshipNote: h.relationshipNote,
          portfolioOwner: h.owner?.includes("Fibra Inn")
            ? "Fibra Inn"
            : h.owner?.includes("FibraHotel")
              ? "FibraHotel"
              : null,
        },
        researchGaps: gaps,
        recommendedResearchPriority: "IMMEDIATE_CALA_MEXICO",
      });
    }
  }

  const report = {
    asOf: new Date().toISOString(),
    publicationState: "CUSTOMER_BLOCKED",
    paidResearchUsd: 0,
    wave: "DEALSIGNAL_GLOBAL_SOURCE_CALA_DISCOVERY_V1",
    geographyRule: "HOTEL_PROPERTY_NOT_OWNER_HQ",
    priorDealSignalIdentityPreserved: true,
    summary: ledger.summary,
    rows,
  };

  writeJson("CALA_DISCOVERY_V1_IDENTITY_TABLE.json", report);
  writeJson(`PERSISTENCE_LEDGER_${ledger.mode}.json`, ledger);
  writeJson("HOTEL_EXPLORER_HANDOFF.json", {
    asOf: new Date().toISOString(),
    publicationState: "CUSTOMER_BLOCKED",
    items: heItems,
  });
  writeJson("IC_HANDOFF.json", {
    asOf: new Date().toISOString(),
    from: "TRACK_C",
    to: ["INTEGRATION_CONTROLLER", "TRACK_A"],
    publicationState: "CUSTOMER_BLOCKED",
    wave: "DEALSIGNAL_GLOBAL_SOURCE_CALA_DISCOVERY_V1",
    summary: ledger.summary,
    canonicalIds: rows
      .filter((r) => r.canonicalHpcRecordId)
      .map((r) => ({
        discoveryPropertyId: r.discoveryPropertyId,
        findingId: r.findingId,
        hpcRecordId: r.canonicalHpcRecordId,
        matchOrCreate: r.matchOrCreate,
        hotelName: r.hotelName,
        country: r.country,
        owner: r.owner,
        operator: r.operator,
      })),
    deferredNoHpc: rows
      .filter((r) => !r.canonicalHpcRecordId)
      .map((r) => ({
        discoveryPropertyId: r.discoveryPropertyId,
        hotelName: r.hotelName,
        reason: r.skipHpcReason || r.gaps?.join("; ") || "NO_HPC",
        action: r.action,
      })),
  });

  const md = `# Track C — CALA Discovery V1 Identity & Ownership

**Examined:** ${ledger.summary.examined} · **Mexico hotels:** ${ledger.summary.mexicoHotels}  
**HPC matches:** ${ledger.summary.matches} · **Creates:** ${ledger.summary.creates}  
**Parents (no HPC):** ${ledger.summary.parentOnly} · **Unresolved skipped:** ${ledger.summary.unresolvedSkipped}  
**Collision rejects:** ${ledger.summary.ambiguousRejected} · **Paid:** $0 · **CUSTOMER_BLOCKED**

| Priority | Name | Action | HPC | Owner | Operator |
|----------|------|--------|-----|-------|----------|
${rows
  .map(
    (r) =>
      `| ${r.priority} | ${r.hotelName} | ${r.matchOrCreate}${r.skipHpcReason ? ` (${r.skipHpcReason})` : ""} | ${r.canonicalHpcRecordId || "—"} | ${r.owner || "—"} | ${r.operator || "—"} |`,
  )
  .join("\n")}
`;
  fs.writeFileSync(path.join(OUT, "FOUNDER_CALA_DISCOVERY_V1_IDENTITY_REPORT.md"), md);

  console.log(JSON.stringify({ done: true, ...ledger.summary, heHandoffs: heItems.length }, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
