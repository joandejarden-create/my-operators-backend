/**
 * Marriott gap clusters from Census gap ledger — Level 2 prioritization.
 * Webhound may use these samples for pattern discovery; never Census SoT.
 */

import { isStreetLevelAddress } from "./production-census-geocoding-providers.js";
import { extractMarshaCode } from "./marriott-hqv-coordinate-client.js";
import { MAP_FIRST_PASS } from "./production-census-first-pass-enrichment.js";

export const MARRIOTT_GAP_CLUSTER = Object.freeze({
  BLOCKED_PAGE_WITH_URL: "marriott_official_url_page_blocked",
  MARSHA_MISSING_LEVEL_2: "marriott_marsha_missing_level_2",
  CLEAN_IDENTITY_MISSING_ROOMS: "marriott_clean_identity_missing_rooms",
  RESORT_MICROSITE_CANDIDATE: "marriott_resort_microsite_candidate",
  FACTSHEET_POTENTIAL: "marriott_factsheet_potential",
  MISSING_ADDRESS_PHONE_ONLY: "marriott_missing_address_phone_only",
  MISSING_ROOMS_ONLY: "marriott_missing_rooms_only",
});

function isBlank(v) {
  return v == null || !String(v).trim();
}

function isHttpUrl(v) {
  return /^https?:\/\//i.test(String(v || "").trim());
}

function isMarriottFamily(fields = {}) {
  const fam = String(
    fields["Brand Family"] || fields["Family / Source Family"] || fields[MAP_FIRST_PASS.family] || ""
  ).toLowerCase();
  const brand = String(fields["Current Brand"] || fields[MAP_FIRST_PASS.currentBrand] || "").toLowerCase();
  const url = String(fields["Official Property URL"] || fields["Source URL"] || "").toLowerCase();
  return (
    /marriott/.test(fam) ||
    /marriott|sheraton|westin|w hotel|st\.?\s*regis|ritz|autograph|tribute|design hotel|aloft|element|moxy|ac hotel|courtyard|residence inn|springhill|fairfield|towneplace|protea|four points|le meridien|delta hotel|edition|jw marriott|marriott vacation/i.test(
      brand
    ) ||
    /marriott\.com/.test(url)
  );
}

function hasMarsha(fields = {}, record = {}) {
  const url = fields["Official Property URL"] || fields["Source URL"] || "";
  const id = fields["Property Identity Key"] || record.id || "";
  return Boolean(extractMarshaCode(url) || extractMarshaCode(id));
}

function looksResortOrLuxury(fields = {}) {
  const hay = [
    fields["Property Name"],
    fields["Current Brand"],
    fields["Official Property URL"],
  ]
    .map((x) => String(x || ""))
    .join(" ")
    .toLowerCase();
  return /resort|ritz|st\.?\s*regis|w hotel|edition|jw marriott|autograph|tribute|luxury|spa/i.test(
    hay
  );
}

/**
 * Classify one Marriott Census record into gap clusters.
 * @param {object} record
 * @param {{ pageBlocked?: boolean }} [opts]
 */
export function classifyMarriottGapClusters(record, opts = {}) {
  const f = record?.fields || {};
  if (!isMarriottFamily(f)) return { is_marriott: false, clusters: [] };

  const officialUrl = String(f["Official Property URL"] || "").trim();
  const sourceUrl = String(f["Source URL"] || "").trim();
  const hasUrl = isHttpUrl(officialUrl) || isHttpUrl(sourceUrl);
  const addressOk = isStreetLevelAddress(f.Address || "");
  const phoneOk = !isBlank(f.Phone);
  const roomsOk = !isBlank(f["Rooms / Keys"]);
  const coordsOk = f.Latitude != null && f.Longitude != null;
  const marsha = hasMarsha(f, record);
  const clusters = [];

  if (hasUrl && opts.pageBlocked === true && (!addressOk || !phoneOk || !roomsOk)) {
    clusters.push(MARRIOTT_GAP_CLUSTER.BLOCKED_PAGE_WITH_URL);
  }
  if (marsha && (!addressOk || !phoneOk || !roomsOk || !coordsOk)) {
    clusters.push(MARRIOTT_GAP_CLUSTER.MARSHA_MISSING_LEVEL_2);
  }
  if (!roomsOk && !isBlank(f.City) && !isBlank(f.Country) && !isBlank(f["Current Brand"])) {
    clusters.push(MARRIOTT_GAP_CLUSTER.CLEAN_IDENTITY_MISSING_ROOMS);
  }
  if (looksResortOrLuxury(f) && (!addressOk || !phoneOk || !roomsOk)) {
    clusters.push(MARRIOTT_GAP_CLUSTER.RESORT_MICROSITE_CANDIDATE);
    clusters.push(MARRIOTT_GAP_CLUSTER.FACTSHEET_POTENTIAL);
  }
  if (!addressOk && !phoneOk && roomsOk) {
    clusters.push(MARRIOTT_GAP_CLUSTER.MISSING_ADDRESS_PHONE_ONLY);
  }
  if (roomsOk === false && addressOk && phoneOk) {
    clusters.push(MARRIOTT_GAP_CLUSTER.MISSING_ROOMS_ONLY);
  }

  return {
    is_marriott: true,
    record_id: record.id,
    marsha: extractMarshaCode(officialUrl || sourceUrl) || extractMarshaCode(f["Property Identity Key"]),
    official_property_url: officialUrl || null,
    source_url: sourceUrl || null,
    missing: {
      hotel_url: !hasUrl,
      address: !addressOk,
      phone: !phoneOk,
      rooms: !roomsOk,
      coords: !coordsOk,
      state: isBlank(f["State / Region"]),
    },
    clusters: [...new Set(clusters)],
  };
}

/**
 * Build prioritized Marriott samples for Webhound / adapter testing.
 * @param {object[]} records
 * @param {{ perCluster?: number, pageBlockedIds?: Set<string> }} [opts]
 */
export function buildMarriottGapClusterSamples(records = [], opts = {}) {
  const perCluster = opts.perCluster || 8;
  const blockedIds = opts.pageBlockedIds || new Set();
  /** @type {Record<string, object[]>} */
  const byCluster = {};
  let marriottTotal = 0;
  let withUrlBlocked = 0;
  let withMarsha = 0;
  let missingRooms = 0;
  let missingAddress = 0;
  let missingPhone = 0;

  for (const rec of records) {
    const classified = classifyMarriottGapClusters(rec, {
      pageBlocked: blockedIds.has(rec.id),
    });
    if (!classified.is_marriott) continue;
    marriottTotal += 1;
    if (classified.marsha) withMarsha += 1;
    if (classified.missing.rooms) missingRooms += 1;
    if (classified.missing.address) missingAddress += 1;
    if (classified.missing.phone) missingPhone += 1;
    if (classified.clusters.includes(MARRIOTT_GAP_CLUSTER.BLOCKED_PAGE_WITH_URL)) {
      withUrlBlocked += 1;
    }
    for (const c of classified.clusters) {
      byCluster[c] = byCluster[c] || [];
      if (byCluster[c].length < perCluster) {
        byCluster[c].push({
          ...classified,
          property_name: rec.fields?.["Property Name"] || null,
          brand: rec.fields?.["Current Brand"] || null,
          city: rec.fields?.City || null,
          country: rec.fields?.Country || null,
        });
      }
    }
  }

  const priorityOrder = [
    MARRIOTT_GAP_CLUSTER.BLOCKED_PAGE_WITH_URL,
    MARRIOTT_GAP_CLUSTER.MARSHA_MISSING_LEVEL_2,
    MARRIOTT_GAP_CLUSTER.CLEAN_IDENTITY_MISSING_ROOMS,
    MARRIOTT_GAP_CLUSTER.RESORT_MICROSITE_CANDIDATE,
    MARRIOTT_GAP_CLUSTER.FACTSHEET_POTENTIAL,
    MARRIOTT_GAP_CLUSTER.MISSING_ADDRESS_PHONE_ONLY,
    MARRIOTT_GAP_CLUSTER.MISSING_ROOMS_ONLY,
  ];

  return {
    marriott_total: marriottTotal,
    with_marsha: withMarsha,
    with_url_blocked: withUrlBlocked,
    missing_rooms: missingRooms,
    missing_address: missingAddress,
    missing_phone: missingPhone,
    priority_order: priorityOrder,
    samples_by_cluster: byCluster,
    webhound_seed_records: priorityOrder.flatMap((c) => byCluster[c] || []).slice(0, 40),
  };
}
