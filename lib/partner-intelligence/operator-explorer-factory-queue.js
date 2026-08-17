/**
 * Operator Explorer factory queue — next operators after Arbor/HE quality freeze.
 * Not the protected quality baseline. Used by factory-init + OS.
 */
export const OPERATOR_FACTORY_QUEUE_VERSION = "operator-factory-queue-v7";

/**
 * @typedef {object} OperatorFactoryQueueEntry
 * @property {string} slug
 * @property {string|null} recordId
 * @property {string} companyName
 * @property {string} domain
 * @property {string} region
 * @property {string} explorerUrl
 * @property {string} referenceFolder
 * @property {'queued'|'in_progress'|'blocked'|'factory_ready'} status
 * @property {string} notes
 */

function entry( partial ) {
  return Object.freeze({
    explorerUrl: partial.recordId
      ? `/operator-explorer-gold-mock.html?id=${partial.recordId}`
      : "",
    referenceFolder: partial.companyName,
    ...partial,
  });
}

/** Priority queue for Tab Factory builds after golden baselines. */
export const OPERATOR_FACTORY_QUEUE = Object.freeze([
  // Wave A — fixture auditPass / founder review
  entry({
    slug: "ghl-hoteles",
    recordId: "reciI2tYQBfMoMK9G",
    companyName: "GHL Hoteles (GHL Holding)",
    domain: "ghlhoteles.com",
    region: "CALA",
    referenceFolder: "GHL Hoteles",
    status: "factory_ready",
    notes: "Fixture Tab Factory auditPass=true — founder visual review next.",
  }),
  entry({
    slug: "aimbridge-latam",
    recordId: "recGWxIJqnYHkJZFD",
    companyName: "Aimbridge Hospitality (LATAM)",
    domain: "aimbridgelatam.com",
    region: "CALA",
    referenceFolder: "Aimbridge LATAM",
    status: "factory_ready",
    notes: "Fixture Tab Factory auditPass=true — founder visual review next.",
  }),
  // Wave B — Masters created 2026-07-24; scaffold next
  entry({
    slug: "tafer-hotels-resorts",
    recordId: "recJ6NPSYveCTo3At",
    companyName: "Tafer Hotels & Resorts",
    domain: "taferresorts.com",
    region: "CALA",
    status: "queued",
    notes: "Master created 2026-07-24. Next: factory-init + Tab Factory content from taferresorts.com.",
  }),
  entry({
    slug: "grupo-presidente",
    recordId: "recJtFkhjaO57rSDC",
    companyName: "Grupo Presidente",
    domain: "grupopresidente.com.mx",
    region: "CALA",
    status: "queued",
    notes: "Master created 2026-07-24. Next: factory-init + Tab Factory from grupopresidente.com.mx.",
  }),
  entry({
    slug: "highgate",
    recordId: "recLjxtxIIVJaGbXK",
    companyName: "Highgate",
    domain: "highgate.com",
    region: "CALA",
    status: "queued",
    notes: "Master created 2026-07-24. Global platform — label CALA vs enterprise in every tab.",
  }),
  entry({
    slug: "grupo-hotelero-santa-fe",
    recordId: "reckyv9O0Y3auYpJJ",
    companyName: "Grupo Hotelero Santa Fe",
    domain: "gsf-hotels.com",
    region: "CALA",
    status: "queued",
    notes: "Master created 2026-07-24. Multi-brand (Krystal/Hyatt/Hilton/Secrets) — Tab Factory next.",
  }),
  entry({
    slug: "arriva-hospitality-group",
    recordId: "reck6gjQd3wdeugmZ",
    companyName: "Arriva Hospitality Group (AHG)",
    domain: "arrivahotels.mx",
    region: "CALA",
    status: "queued",
    notes: "Master created 2026-07-24. Next: factory-init + Tab Factory from arrivahotels.mx.",
  }),
  entry({
    slug: "brittain-resorts-hotels",
    recordId: "receHCdI6CEsJqdG4",
    companyName: "Brittain Resorts & Hotels (BRH)",
    domain: "brittainresorts.com",
    region: "US",
    status: "queued",
    notes: "Master created 2026-07-24. US Southeast core — confirm CALA relevance before Active release.",
  }),
  entry({
    slug: "atlantica-hotels-international",
    recordId: "recfwDdU5t9h4uFnZ",
    companyName: "Atlantica Hotels International (AHI)",
    domain: "atlanticahotels.com.br",
    region: "CALA",
    status: "queued",
    notes: "Master created 2026-07-24. Brasil AHI — Tab Factory from atlanticahotels.com.br.",
  }),
  entry({
    slug: "viento-sur-gestion-hotelera",
    recordId: "recZPHT2zqc8K6itx",
    companyName: "Viento Sur Gestión Hotelera (CALA)",
    domain: "",
    region: "CALA",
    status: "queued",
    notes: "Confirm official domain before provenance gate. PI not started.",
  }),
  // Wave C — brand-managed parent companies (Operator lens). Masters created 2026-07-24.
  entry({
    slug: "marriott-international-managed",
    recordId: "recGmiPhRt6hiayd9",
    companyName: "Marriott International (Managed)",
    domain: "marriott.com",
    region: "GLOBAL",
    status: "in_progress",
    notes: "Brand-managed Core 5. Master Active. Thin factory fixtures + Setup Profile filled. Phase 2: Arbor/HE parity.",
  }),
  entry({
    slug: "ihg-managed",
    recordId: "rec7IXYQYpKMYsrDl",
    companyName: "IHG Hotels & Resorts (Managed)",
    domain: "ihg.com",
    region: "GLOBAL",
    status: "in_progress",
    notes: "Brand-managed Core 5. Master Active. Thin factory fixtures + Setup Profile filled. Phase 2: Arbor/HE parity.",
  }),
  entry({
    slug: "hilton-managed",
    recordId: "rec3Uwxe6ovpiokuN",
    companyName: "Hilton (Managed)",
    domain: "hilton.com",
    region: "GLOBAL",
    status: "in_progress",
    notes: "Brand-managed Core 5. Master Active. Thin factory fixtures + Setup Profile filled. Phase 2: Arbor/HE parity.",
  }),
  entry({
    slug: "accor-managed",
    recordId: "recF2WqLqNVyKGz9E",
    companyName: "Accor (Managed)",
    domain: "group.accor.com",
    region: "GLOBAL",
    status: "in_progress",
    notes: "Brand-managed Core 5. Master Active. Thin factory fixtures + Setup Profile filled. Phase 2: Arbor/HE parity.",
  }),
  entry({
    slug: "minor-hotels-managed",
    recordId: "rec8SrT3VjRkkYTxm",
    companyName: "Minor Hotels (Managed)",
    domain: "minorhotels.com",
    region: "GLOBAL",
    status: "in_progress",
    notes: "Brand-managed Core 5. Master Active. Thin factory fixtures + Setup Profile filled. Phase 2: Arbor/HE parity.",
  }),
  // Wave B add — Playa Hotels & Resorts (owner/operator all-inclusive; not brand-managed parent)
  entry({
    slug: "playa-hotels-resorts",
    recordId: "rec3TUHT9Z4AnFp5P",
    companyName: "Playa Hotels & Resorts",
    domain: "playaresorts.com",
    region: "CALA",
    status: "in_progress",
    notes: "Wave B. Master rec3TUHT9Z4AnFp5P. All-inclusive owner/operator Mexico/Caribbean. Note Hyatt acquisition (2025) in diligence.",
  }),
  // Wave D — founder add 2026-07-24 (recordIds filled after Master create)
  entry({
    slug: "royalton-hotels-resorts",
    recordId: "recOc5kpsg4Muip9Y",
    companyName: "Royalton Hotels & Resorts",
    domain: "royaltonresorts.com",
    region: "CALA",
    status: "in_progress",
    notes: "Wave D. Formerly Blue Diamond Resorts (2025). All-inclusive CALA. Master recOc5kpsg4Muip9Y.",
  }),
  entry({
    slug: "driftwood-hospitality-management",
    recordId: "recKVILWcRLqrQlWs",
    companyName: "Driftwood Hospitality Management",
    domain: "driftwoodhospitality.com",
    region: "US",
    status: "in_progress",
    notes: "Wave D. U.S. third-party full-service. Confirm CALA relevance. Master recKVILWcRLqrQlWs.",
  }),
  entry({
    slug: "remington-hospitality",
    recordId: "rec6UB6RpMKSs2tAo",
    companyName: "Remington Hospitality",
    domain: "remingtonhospitality.com",
    region: "CALA",
    status: "in_progress",
    notes: "Wave D. U.S. + CALA (Miami). Label enterprise vs CALA. Master rec6UB6RpMKSs2tAo.",
  }),
  // Wave E — founder add 2026-07-24 (Masters created same day)
  entry({
    slug: "oxohotel",
    recordId: "rectsHzacZDFTH1Ze",
    companyName: "OxoHotel",
    domain: "oxohotel.com",
    region: "CALA",
    status: "queued",
    notes: "Wave E. Colombia multi-brand operator (Marriott/Hilton/IHG + proprietary). Master rectsHzacZDFTH1Ze. Founder label: Oxohotels.",
  }),
  entry({
    slug: "grupo-marta-hospitality",
    recordId: "recuEDrp6oeJIEuRX",
    companyName: "Grupo Marta Hospitality",
    domain: "grupomarta.com",
    region: "CALA",
    status: "queued",
    notes: "Wave E. Costa Rica operator (IHG / Best Western / F&B). Master recuEDrp6oeJIEuRX.",
  }),
  entry({
    slug: "grupo-iberostar",
    recordId: "recwEHUotSGpfkZEJ",
    companyName: "Grupo Iberostar",
    domain: "grupoiberostar.com",
    region: "CALA",
    status: "queued",
    notes: "Wave E. Iberostar Group corporate — label group vs Iberostar Beachfront brand lens. Master recwEHUotSGpfkZEJ.",
  }),
]);

/**
 * @param {string} slugOrRecordId
 * @returns {OperatorFactoryQueueEntry | null}
 */
export function getOperatorFactoryQueueEntry(slugOrRecordId) {
  const key = String(slugOrRecordId || "").trim();
  if (!key) return null;
  return (
    OPERATOR_FACTORY_QUEUE.find((o) => o.slug === key || o.recordId === key) || null
  );
}

export function listOperatorFactoryQueue({ status = null } = {}) {
  if (!status) return [...OPERATOR_FACTORY_QUEUE];
  return OPERATOR_FACTORY_QUEUE.filter((o) => o.status === status);
}
