/**
 * Operator Explorer Phase 1 — Record Purpose + universe isolation.
 * Test Fixture Masters stay in Airtable but are excluded from production universes.
 */

export const RECORD_PURPOSE = Object.freeze({
  PRODUCTION: "Production",
  RESEARCH: "Research",
  TEST_FIXTURE: "Test Fixture",
});

/** Nine beta/dummy Masters — must remain Test Fixture and excluded from prod universes. */
export const TEST_FIXTURE_MASTER_IDS = Object.freeze([
  "recTUjuDxL96yWcQA", // Antillano Norte Hospitality Group
  "recBReJUmxdOUvQzp", // Cordillera One Gestión
  "recZPHT2zqc8K6itx", // Viento Sur Gestión Hotelera
  "recZgNR85WZKDItLF", // Mangle Azul Hospitalidad
  "recbT3q8ApRIBu4j5", // Panamerican Lodging Partners S.A.
  "reckO98E46sKTn3F3", // Río Plata Hotel Partners
  "recq3NiRxOerg4kZU", // Barrio Hotelero CDMX
  "recwbyY4qfNP1bV3r", // Metro Lodging São Paulo
  "recxAa86Qoc0nFRSt", // Oro Verde Lodge & Hotel Operators
]);

export const RESEARCH_STAGE_MASTER_IDS = Object.freeze([
  "recjgHXqTJktijFUR", // Álvarez Argüelles Hoteles
  "recHj56wpRLUnJ5Wx", // Tremun Hoteles
  "rec9JSyGQjvodsPSJ", // AADESA
]);

/** Production Real + Real Research Required from universe audit (excludes fixtures + research stage). */
export const PRODUCTION_PURPOSE_MASTER_IDS = Object.freeze([
  "recF5Z87OAqFgndoq", // Arbor
  "recWPKu5laVZxsvpn", // Hotel Equities
  "reciI2tYQBfMoMK9G", // GHL
  "recGWxIJqnYHkJZFD", // Aimbridge LATAM
  "rec3TUHT9Z4AnFp5P", // Playa
  "reckyv9O0Y3auYpJJ", // Santa Fe
  "recLjxtxIIVJaGbXK", // Highgate
  "recKVILWcRLqrQlWs", // Driftwood
  "recfwDdU5t9h4uFnZ", // Atlantica
  "recQ6Cf8O2z0tiqBz", // Cenote Azul
  "recwEHUotSGpfkZEJ", // Grupo Iberostar
  "recGmiPhRt6hiayd9", // Marriott Managed
  "rec3Uwxe6ovpiokuN", // Hilton Managed
  "recF2WqLqNVyKGz9E", // Accor Managed
  "rec7IXYQYpKMYsrDl", // IHG Managed
  "rec8SrT3VjRkkYTxm", // Minor Managed
  // Real — Research Required
  "reck6gjQd3wdeugmZ", // Arriva
  "receHCdI6CEsJqdG4", // Brittain
  "recuEDrp6oeJIEuRX", // Grupo Marta
  "recJtFkhjaO57rSDC", // Grupo Presidente
  "rectsHzacZDFTH1Ze", // OxoHotel
  "rec6UB6RpMKSs2tAo", // Remington
  "recOc5kpsg4Muip9Y", // Royalton
  "recJ6NPSYveCTo3At", // Tafer
]);

export function recordPurposeForMasterId(masterId) {
  if (TEST_FIXTURE_MASTER_IDS.includes(masterId)) return RECORD_PURPOSE.TEST_FIXTURE;
  if (RESEARCH_STAGE_MASTER_IDS.includes(masterId)) return RECORD_PURPOSE.RESEARCH;
  if (PRODUCTION_PURPOSE_MASTER_IDS.includes(masterId)) return RECORD_PURPOSE.PRODUCTION;
  return null;
}

export function isTestFixtureMaster(masterId, fields = {}) {
  if (TEST_FIXTURE_MASTER_IDS.includes(masterId)) return true;
  return String(fields["Record Purpose"] || "") === RECORD_PURPOSE.TEST_FIXTURE;
}

/**
 * Central production universe filter for Explorer / Fit / research waves.
 * Explicit test harnesses may pass { allowTestFixtures: true }.
 */
export function filterProductionUniverse(masters, options = {}) {
  const allowTestFixtures = options.allowTestFixtures === true;
  return (masters || []).filter((m) => {
    const id = m.id || m.masterId;
    const fields = m.fields || m;
    if (!allowTestFixtures && isTestFixtureMaster(id, fields)) return false;
    return true;
  });
}

export function assertNoTestFixturesInProductionList(masters, context = "production universe") {
  const leaked = (masters || []).filter((m) => isTestFixtureMaster(m.id || m.masterId, m.fields || m));
  if (leaked.length) {
    const names = leaked.map((m) => m.fields?.company_name || m.id).join(", ");
    throw new Error(`Test Fixture leak in ${context}: ${names}`);
  }
  return true;
}

/** Alias strings that must NOT become separate Masters. */
export const FORBIDDEN_DUPLICATE_MASTER_ALIASES = Object.freeze([
  "mxm",
  "marriott management",
  "hilton management services",
  "hms",
  "accorhotels",
  "nh hotels",
  "nh hotel group",
  "iberostar managed",
]);

export function normalizeEntityKey(name) {
  return String(name || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}
