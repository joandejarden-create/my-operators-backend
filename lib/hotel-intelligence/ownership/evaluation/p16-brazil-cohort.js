/**
 * P1.6 Brazil ownership eval cohort (20 hotels + Webhound A′ regression set).
 */

import { createHash } from "node:crypto";
import { MAP_CENSUS_FIELDS } from "../../map_hotel_intelligence_fields.js";

export const OWNERSHIP_P16_BRAZIL_VERSION = "ownership-p16-brazil-eval-v1";
export const OWNERSHIP_P16_BRAZIL_SEED = "ownership-intelligence-p16-brazil-20-v1";

/** Webhound A′ regression — method reproduction targets (not truth import). */
export const WEBHOUND_APRIME_REGRESSION = Object.freeze([
  {
    slot: 1,
    hotel_id: "dhl_06G68BZ46WA56J3PZ1HPND70VB",
    name: "ibis Santos Valongo",
    city: "Santos",
    country: "Brazil",
    brand: "ibis",
    seed_cnpj: "80732928003891",
    seed_company: "ATRIO HOTEIS S.A.",
    expect: {
      cnpj_chain: true,
      seed_validation: "VALIDATED_OR_PROBABLE",
      franchise_disclosure_lane: true,
      structure: ["condo_hotel", "lease_lessor_structure", "propco_holdco", "mixed_structure"],
    },
  },
  {
    slot: 2,
    hotel_id: "dhl_06G68CAMN88WZP4KQ6GS20JXDE",
    name: "Sleep Inn Vitoria",
    city: "Vitoria",
    country: "Brazil",
    brand: "Sleep Inn",
    seed_cnpj: "14777686000217",
    seed_company: "FOUR TOWERS HOTELS LTDA",
    expect: {
      qsa_path: true,
      family_structure: true,
      related_propco_candidate: true,
    },
  },
  {
    slot: 3,
    hotel_id: "dhl_06G68CPJX862477HAPQSSCYZ4V",
    name: "Portal Do Atlantico",
    city: "Porto Seguro",
    country: "Brazil",
    seed_cnpj: "47583518000169",
    seed_company: "HOTEL PORTAL DE OUROESTE LTDA",
    expect: {
      false_seed_rejected: true,
      distributed_structure: true,
    },
  },
  {
    slot: 4,
    hotel_id: "dhl_06G687TVM41PA7RYW0NQVHSJ50",
    name: "Peten Esplendido Hotel and Conference Center",
    city: "Flores",
    country: "Guatemala",
    expect: { guatemala_separate: true },
  },
  {
    slot: 5,
    hotel_id: "dhl_06G68C3AD4JBRF6G9GGPKC1KSR",
    name: "Terrazza Hotel",
    city: "Campos do Jordao",
    country: "Brazil",
    expect: {
      hotel_name_cnpj_discovery: true,
      individual_owner: true,
    },
  },
]);

function hash(id) {
  return createHash("sha256")
    .update(`${OWNERSHIP_P16_BRAZIL_SEED}:${id}`)
    .digest("hex");
}

function isBrazil(country) {
  return /brazil|brasil/i.test(String(country || ""));
}

/**
 * @param {object[]} censusRecords
 * @param {object[]} priorHotels
 */
export function buildP16BrazilEvalCohort(censusRecords, priorHotels = []) {
  const pool = [];

  for (const h of priorHotels || []) {
    if (!isBrazil(h.country)) continue;
    pool.push({
      source: "prior_eval",
      hotel_id: h.hotel_id,
      census_record_id: h.census_record_id,
      name: h.name,
      country: h.country,
      city: h.city,
      brand: h.brand,
      website_present: h.website_present,
      _sort: hash(h.census_record_id || h.hotel_id),
    });
  }

  for (const rec of censusRecords || []) {
    const f = rec.fields || {};
    const country = f[MAP_CENSUS_FIELDS.country] || f.Country;
    if (!isBrazil(country)) continue;
    const name =
      f[MAP_CENSUS_FIELDS.officialName] ||
      f[MAP_CENSUS_FIELDS.propertyName] ||
      f["Property Name"];
    if (!name) continue;
    const id = rec.id;
    if (pool.some((x) => x.census_record_id === id)) continue;
    pool.push({
      source: "census_fill",
      hotel_id: null,
      census_record_id: id,
      name,
      country,
      city: f[MAP_CENSUS_FIELDS.city] || f.City || null,
      brand: f[MAP_CENSUS_FIELDS.brandName] || null,
      website_present: Boolean(f[MAP_CENSUS_FIELDS.website]),
      _sort: hash(id),
    });
  }

  pool.sort((a, b) => a._sort.localeCompare(b._sort));

  // Deterministic 20: prefer mix from prior + census; tag segments by name heuristics
  const selected = pool.slice(0, 20).map((h, idx) => {
    const n = String(h.name || "").toLowerCase();
    let segment = "independent";
    if (/ibis|novotel|mercure|accor|marriott|hilton|hyatt|ihg|wyndham|choice|sleep inn|comfort|radisson|intercontinental/i.test(n)) {
      segment = /sleep|ibis|hampton|fairfield|courtyard|aloft/i.test(n)
        ? "select_service"
        : "branded_full_service";
    }
    if (/portal|flat|condo|residence|apart|flat/i.test(n)) segment = "condo_apart";
    return { ...h, eval_segment: segment, eval_index: idx + 1 };
  });

  return {
    version: OWNERSHIP_P16_BRAZIL_VERSION,
    seed: OWNERSHIP_P16_BRAZIL_SEED,
    total: selected.length,
    hotels: selected,
    webhound_aprime_regression: WEBHOUND_APRIME_REGRESSION,
  };
}

/**
 * Score Webhound A′ regression reproduction from native research output.
 * @param {object} hotelResult
 */
export function scoreAprimeRegression(hotelResult) {
  const spec = WEBHOUND_APRIME_REGRESSION.find(
    (w) => w.hotel_id === hotelResult.hotel_id || w.name === hotelResult.name
  );
  if (!spec) return null;

  const checks = [];
  const m = hotelResult.metrics || {};
  const bc = m.brazil_corporate || {};
  const structure = m.ownership_structure?.structure || hotelResult.ownership_structure?.structure;

  if (spec.expect.false_seed_rejected) {
    const rejected =
      (bc.seeds_rejected || 0) > 0 ||
      (hotelResult.review_items || []).some((r) =>
        /seed_rejected|explicit_seed_rejected|cadastur_seed_rejected/i.test(r.summary || "")
      );
    checks.push({ id: "false_seed_rejected", pass: rejected });
  }
  if (spec.expect.qsa_path) {
    checks.push({ id: "qsa_path", pass: (bc.qsa_partners || 0) > 0 });
  }
  if (spec.expect.related_propco_candidate) {
    checks.push({ id: "related_propco_candidate", pass: (bc.propco_candidates || 0) > 0 });
  }
  if (spec.expect.hotel_name_cnpj_discovery) {
    const methodHit = (m.method_attempts || []).some(
      (a) => a.method === "hotel_name_to_company" && a.success
    );
    checks.push({ id: "hotel_name_cnpj_discovery", pass: methodHit || (bc.seeds_validated || 0) > 0 });
  }
  if (spec.expect.individual_owner) {
    checks.push({
      id: "individual_owner",
      pass: structure === "individual_owner" || bc.sole_individual_partner,
    });
  }
  if (spec.expect.family_structure) {
    checks.push({
      id: "family_structure",
      pass: structure === "family_controlled" || (bc.individual_controllers || 0) >= 2,
    });
  }
  if (spec.expect.franchise_disclosure_lane) {
    checks.push({
      id: "franchise_disclosure_lane",
      pass: (m.franchise_disclosure?.searches || 0) > 0,
    });
  }
  if (spec.expect.distributed_structure) {
    checks.push({
      id: "distributed_structure",
      pass: ["distributed_unit_ownership", "condo_hotel", "unknown"].includes(structure),
    });
  }
  if (spec.expect.guatemala_separate) {
    checks.push({
      id: "guatemala_lane_deferred",
      pass: !m.brazil_corporate && (m.country_adapter?.adapter || "").includes("guatemala"),
    });
  }
  if (spec.expect.cnpj_chain) {
    checks.push({
      id: "cnpj_chain",
      pass: (bc.cnpj_lookups || 0) > 0 && (bc.seeds_validated || 0) > 0,
    });
  }

  const passed = checks.filter((c) => c.pass).length;
  const total = checks.length;
  let status = "NOT_REPRODUCED";
  if (passed === total && total > 0) status = "FULLY_REPRODUCED";
  else if (passed > 0) status = "PARTIALLY_REPRODUCED";

  return { slot: spec.slot, name: spec.name, status, checks, passed, total };
}
