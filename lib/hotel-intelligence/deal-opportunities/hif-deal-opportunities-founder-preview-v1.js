/**
 * DealSignal — founder/admin internal preview for Critical-Mass permanent-rule Qualified.
 *
 * Does NOT modify the permanent qualification classifier.
 * Does NOT change CUSTOMER_BLOCKED publication status.
 * Display overlay only for authorized founder/admin sessions.
 *
 * Root cause context: CM V3–V4 records were often persisted under adjacent /
 * Track B field types that the live commercial-eligibility gate keeps on watch,
 * even when research notes mark QUALIFIED. Founder preview surfaces the verified
 * baseline nine for internal review without customer publication.
 */

import { isAuthorizedForRadarInternalPreviewHotels } from "../radar-internal-preview-hotels-v1.js";

export const DEAL_OPPORTUNITIES_FOUNDER_PREVIEW_V1 =
  "hif-deal-opportunities-founder-preview-v1";

/**
 * Verified permanent-rule Qualified finding IDs (CM V2–V4 + Chat B strengtheners).
 * Cumulative count remains 9 hotels; multiple finding IDs may map to one hotel.
 */
export const CM_PERMANENT_QUALIFIED_FINDING_IDS = Object.freeze([
  // Voir Beach (CALA) — already passes live gate as BCT
  "find_ds_cm_v2_voir_beach_brand_open_2025",
  // Baha Mar #4 (CALA)
  "find_ds_cm_v2_baha_mar_brand_open_2026",
  // Europe V3
  "find_ds_cm_v3_oromana_mct_open_2026",
  "find_ds_cm_v3_agueda_operator_open_2026",
  "find_ds_cm_v3_areia_branca_concession_open_2026",
  // Chat B strengtheners (PR #569)
  "find_tb_cb_cb_gap_areia_branca_vortal",
  "find_tb_cb_cb_gap_oromana_pcap_ppt",
  "find_tb_cb_cb_gap_agueda_entity_hma",
  // Europe / CALA V4
  "find_cb2_hotel_do_mel_gois_operator_2026",
  "find_ds_cm_v4_gois_operator_open_2026",
  "find_ds_cm_v4_leaburu_mct_open_2026",
  "find_ds_cm_v4_santa_eulalia_mct_open_2026",
  "find_ds_cm_v4_pau_dalho_concession_open_2026",
]);

/** Canonical hotel keys for the nine permanent-rule Qualified opportunities. */
export const CM_PERMANENT_QUALIFIED_HOTELS = Object.freeze([
  {
    key: "voir-beach",
    label: "Voir Beach",
    region: "CALA",
    country: "Bahamas",
    nameRe: /voir\s*beach/i,
    findingIds: ["find_ds_cm_v2_voir_beach_brand_open_2025"],
    kindHint: "BCT",
  },
  {
    key: "baha-mar-4",
    label: "Baha Mar #4",
    region: "CALA",
    country: "Bahamas",
    nameRe: /baha\s*mar/i,
    findingIds: ["find_ds_cm_v2_baha_mar_brand_open_2026"],
    kindHint: "BCT",
  },
  {
    key: "oromana",
    label: "Hotel Oromana",
    region: "Europe",
    country: "Spain",
    nameRe: /oromana/i,
    findingIds: [
      "find_ds_cm_v3_oromana_mct_open_2026",
      "find_tb_cb_cb_gap_oromana_pcap_ppt",
    ],
    kindHint: "MCT",
  },
  {
    key: "agueda",
    label: "Águeda Passivhaus",
    region: "Europe",
    country: "Portugal",
    nameRe: /[áa]gueda|passivhaus/i,
    findingIds: [
      "find_ds_cm_v3_agueda_operator_open_2026",
      "find_tb_cb_cb_gap_agueda_entity_hma",
    ],
    kindHint: "MCT",
  },
  {
    key: "areia-branca",
    label: "Areia Branca REVIVE",
    region: "Europe",
    country: "Portugal",
    nameRe: /areia\s*branca/i,
    findingIds: [
      "find_ds_cm_v3_areia_branca_concession_open_2026",
      "find_tb_cb_cb_gap_areia_branca_vortal",
    ],
    kindHint: "MCT",
  },
  {
    key: "hotel-do-mel",
    label: "Hotel do Mel",
    region: "Europe",
    country: "Portugal",
    nameRe: /hotel\s*do\s*mel|casa\s*da\s*costeira|g[oó]is/i,
    findingIds: [
      "find_cb2_hotel_do_mel_gois_operator_2026",
      "find_ds_cm_v4_gois_operator_open_2026",
    ],
    kindHint: "MCT",
  },
  {
    key: "ostatu-leaburu",
    label: "Ostatu de Leaburu",
    region: "Europe",
    country: "Spain",
    nameRe: /ostatu|leaburu/i,
    findingIds: ["find_ds_cm_v4_leaburu_mct_open_2026"],
    kindHint: "MCT",
  },
  {
    key: "santa-eulalia",
    label: "Santa Eulalia del Campo",
    region: "Europe",
    country: "Spain",
    nameRe: /santa\s*eulalia/i,
    findingIds: ["find_ds_cm_v4_santa_eulalia_mct_open_2026"],
    kindHint: "MCT",
  },
  {
    key: "pau-dalho",
    label: "Fazenda Pau D'Alho",
    region: "CALA",
    country: "Brazil",
    nameRe: /pau\s*d['’]?alho|fazenda\s*pau/i,
    findingIds: ["find_ds_cm_v4_pau_dalho_concession_open_2026"],
    kindHint: "MCT",
  },
]);

const FINDING_SET = new Set(CM_PERMANENT_QUALIFIED_FINDING_IDS);

export function isDealOpportunitiesFounderPreviewRequested(query = {}) {
  const q = query || {};
  return (
    q.founderPreview === "1" ||
    q.founderPreview === "true" ||
    q.dealSignalFounderPreview === "1" ||
    q.internalPreview === "1"
  );
}

export function isAuthorizedForDealOpportunitiesFounderPreview(
  dealalityUser,
  req = {},
) {
  return isAuthorizedForRadarInternalPreviewHotels(dealalityUser, req);
}

function matchHotelSpec(opportunity) {
  const id = String(opportunity?.id || "");
  const name = String(opportunity?.name || "");
  for (const hotel of CM_PERMANENT_QUALIFIED_HOTELS) {
    if (hotel.findingIds.includes(id)) return hotel;
    if (FINDING_SET.has(id) && hotel.nameRe.test(name)) return hotel;
    if (hotel.nameRe.test(name)) return hotel;
  }
  return null;
}

function kindRank(kind) {
  if (kind === "MCT" || kind === "BCT") return 2;
  return 1;
}

/**
 * Among duplicates for the same hotel, prefer principal BCT/MCT rows.
 */
export function pickPreferredOpportunity(rows) {
  return [...rows].sort((a, b) => {
    const kr = kindRank(b.kind) - kindRank(a.kind);
    if (kr) return kr;
    const g =
      (b.group === "qualified" ? 1 : 0) - (a.group === "qualified" ? 1 : 0);
    if (g) return g;
    return String(a.id).localeCompare(String(b.id));
  })[0];
}

/**
 * Apply founder-preview promotion to a loaded opportunities list.
 * @returns {{ opportunities: object[], preview: object }}
 */
export function applyDealOpportunitiesFounderPreview(opportunities, opts = {}) {
  const list = Array.isArray(opportunities) ? opportunities : [];
  const authorized = Boolean(opts.authorized);
  const requested = Boolean(opts.requested);
  const enabled = authorized && requested;

  const meta = {
    version: DEAL_OPPORTUNITIES_FOUNDER_PREVIEW_V1,
    requested,
    authorized,
    enabled,
    publicationState: "CUSTOMER_BLOCKED",
    classifierUnchanged: true,
    expectedHotels: CM_PERMANENT_QUALIFIED_HOTELS.length,
    promotedFindingIds: [],
    matchedHotels: [],
    missingHotels: [],
  };

  if (!enabled) {
    meta.missingHotels = CM_PERMANENT_QUALIFIED_HOTELS.map((h) => h.key);
    return { opportunities: list, preview: meta };
  }

  const byHotel = new Map();
  for (const o of list) {
    const hotel = matchHotelSpec(o);
    if (!hotel) continue;
    if (!byHotel.has(hotel.key)) byHotel.set(hotel.key, []);
    byHotel.get(hotel.key).push(o);
  }

  const promoteIds = new Set();
  for (const hotel of CM_PERMANENT_QUALIFIED_HOTELS) {
    const rows = byHotel.get(hotel.key) || [];
    if (!rows.length) {
      meta.missingHotels.push(hotel.key);
      continue;
    }
    const preferred = pickPreferredOpportunity(rows);
    promoteIds.add(preferred.id);
    meta.matchedHotels.push({
      key: hotel.key,
      label: hotel.label,
      findingId: preferred.id,
      priorGroup: preferred.group,
    });
    meta.promotedFindingIds.push(preferred.id);
  }

  const out = list.map((o) => {
    if (!promoteIds.has(o.id)) return o;
    const hotel = matchHotelSpec(o);
    const kind =
      o.kind === "BCT" || o.kind === "MCT"
        ? o.kind
        : hotel?.kindHint || o.kind || "MCT";
    return {
      ...o,
      group: "qualified",
      kind,
      type:
        kind === "BCT"
          ? "Brand conversion"
          : kind === "MCT"
            ? "Management opportunity"
            : o.type,
      status:
        o.status && !/^Historical|Emerging/i.test(String(o.status))
          ? o.status
          : kind === "BCT"
            ? "Private selection"
            : "Operator selection",
      founderPreview: true,
      founderPreviewHotelKey: hotel?.key || null,
      publicationState: "CUSTOMER_BLOCKED",
      commercialDisposition: "founder_preview_qualified",
    };
  });

  return { opportunities: out, preview: meta };
}
