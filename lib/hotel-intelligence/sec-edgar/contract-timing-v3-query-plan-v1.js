/**
 * V3 expanded SEC-API.io query plan — franchise vs management vs license,
 * partitioned by year/form, with adaptive priority weights.
 */

import {
  PERIOD_PARTITIONS,
  BASELINE_JPMCC_ACCESSION,
} from "./contract-timing-v2-query-families-v1.js";

export const CONTRACT_TIMING_V3_QUERY_PLAN_V1 = "contract-timing-v3-query-plan-v1";

/** Prior wave accessions excluded from "new vs V2" incremental accounting. */
export const V2_KNOWN_ACCESSIONS = Object.freeze([
  BASELINE_JPMCC_ACCESSION,
  "0001539497-18-000207",
  "0001539497-18-000531",
  "0001539497-18-000821",
  "0001539497-19-002159",
]);

export const AGREEMENT_DISCOVERY_FAMILY = Object.freeze({
  FRANCHISE: "FRANCHISE",
  MANAGEMENT: "MANAGEMENT",
  LICENSE: "LICENSE",
  LEASE: "LEASE",
  AMENDMENT: "AMENDMENT",
  RENEWAL_EXTENSION: "RENEWAL_EXTENSION",
  TERMINATION_RIGHT: "TERMINATION_RIGHT",
});

export const V3_SEC_API_QUERY_FAMILIES = Object.freeze([
  {
    id: "franchise_expiration_date",
    agreementFamily: AGREEMENT_DISCOVERY_FAMILY.FRANCHISE,
    query: '"Franchise Expiration Date"',
    priority: 1,
    formTypesPreferred: ["424B2", "424B5", "424B3", "424B4"],
  },
  {
    id: "franchise_agreement_expiration_date",
    agreementFamily: AGREEMENT_DISCOVERY_FAMILY.FRANCHISE,
    query: '"Franchise Agreement Expiration"',
    priority: 1,
    formTypesPreferred: ["424B2", "424B5", "10-K", "S-11"],
  },
  {
    id: "franchise_term_and_renewal",
    agreementFamily: AGREEMENT_DISCOVERY_FAMILY.RENEWAL_EXTENSION,
    query: '"Franchise" "Term" "Renewal" Hotel',
    priority: 2,
    formTypesPreferred: ["424B2", "424B5", "10-K", "EX-10"],
  },
  {
    id: "management_agreement_expiration",
    agreementFamily: AGREEMENT_DISCOVERY_FAMILY.MANAGEMENT,
    query: '"Management Agreement Expiration"',
    priority: 1,
    formTypesPreferred: ["424B2", "424B5", "10-K", "EX-10"],
  },
  {
    id: "hotel_management_agreement_ex10",
    agreementFamily: AGREEMENT_DISCOVERY_FAMILY.MANAGEMENT,
    query: '"Hotel Management Agreement" "Initial Term" OR "Management Agreement Expiration"',
    priority: 2,
    // EX-10 is not a SEC form type — search parent forms that attach exhibits
    formTypesPreferred: ["10-K", "8-K", "S-11"],
  },
  {
    id: "hma_initial_term",
    agreementFamily: AGREEMENT_DISCOVERY_FAMILY.MANAGEMENT,
    query: '"Hotel Management Agreement" "expires" OR "expiration date" "Manager"',
    priority: 2,
    formTypesPreferred: ["10-K", "S-11", "8-K"],
  },
  {
    id: "license_agreement_expiration",
    agreementFamily: AGREEMENT_DISCOVERY_FAMILY.LICENSE,
    query: '"License Agreement Expiration"',
    priority: 2,
    formTypesPreferred: ["424B2", "424B5", "10-K"],
  },
  {
    id: "hotel_franchise_agreement_ex10",
    agreementFamily: AGREEMENT_DISCOVERY_FAMILY.FRANCHISE,
    query: '"Hotel Franchise Agreement"',
    priority: 2,
    formTypesPreferred: ["EX-10", "10-K", "8-K"],
  },
  {
    id: "hotel_collateral_schedule",
    agreementFamily: AGREEMENT_DISCOVERY_FAMILY.FRANCHISE,
    query: '"Hotel" "Collateral" "Franchise Expiration"',
    priority: 2,
    formTypesPreferred: ["424B2", "424B5"],
  },
  {
    id: "mortgaged_property_franchise_agreement",
    agreementFamily: AGREEMENT_DISCOVERY_FAMILY.FRANCHISE,
    query: '"Mortgaged Property" "Franchise Agreement"',
    priority: 2,
    formTypesPreferred: ["424B2", "424B5"],
  },
  {
    id: "termination_right_franchise",
    agreementFamily: AGREEMENT_DISCOVERY_FAMILY.TERMINATION_RIGHT,
    query: '"Termination Right" Franchise Hotel',
    priority: 3,
    formTypesPreferred: ["424B2", "424B5", "10-K", "EX-10"],
  },
  {
    id: "lease_expiration_hotel",
    agreementFamily: AGREEMENT_DISCOVERY_FAMILY.LEASE,
    query: '"Ground Lease Expiration" Hotel OR "Hotel Lease" Expiration',
    priority: 3,
    formTypesPreferred: ["424B2", "424B5", "10-K"],
  },
  {
    id: "amendment_franchise_management",
    agreementFamily: AGREEMENT_DISCOVERY_FAMILY.AMENDMENT,
    query: '"Amendment" "Franchise Agreement" Hotel OR "Amended and Restated" "Management Agreement" Hotel',
    priority: 3,
    formTypesPreferred: ["8-K", "EX-10", "10-K"],
  },
]);

/** Extra CMBS / financing CIKs beyond V2 registry (independent discovery). */
export const V3_CMBS_ISSUER_CIKS = Object.freeze([
  { slug: "ubs-commercial-mortgage-securitization", cik: "0001532799" },
  { slug: "wells-fargo-commercial-mortgage", cik: "0000850779" },
  { slug: "jpmorgan-chase-commercial-mortgage", cik: "0001013611" },
  { slug: "morgan-stanley-capital-i", cik: "0001005150" },
  { slug: "gs-mortgage-securities", cik: "0001541502" },
  { slug: "citigroup-commercial-mortgage", cik: "0001539497" },
  { slug: "deutsche-bank-securities", cik: "0000830718" },
  { slug: "bank-of-america-merrill", cik: "0001102116" },
]);

export function listV3SecApiQueryPlan(opts = {}) {
  const periods = opts.periods || PERIOD_PARTITIONS;
  const families = [...V3_SEC_API_QUERY_FAMILIES].sort((a, b) => a.priority - b.priority);
  const weights = opts.weights || {};
  const plan = [];
  for (const fam of families) {
    const w = weights[fam.id] != null ? Number(weights[fam.id]) : 1 / fam.priority;
    for (const period of periods) {
      plan.push({
        id: `${fam.id}__${period.id}`,
        queryFamily: fam.id,
        agreementFamily: fam.agreementFamily,
        query: fam.query,
        formTypes: fam.formTypesPreferred,
        startDate: period.startDate,
        endDate: period.endDate,
        periodId: period.id,
        priority: fam.priority,
        weight: w,
      });
    }
  }
  plan.sort((a, b) => b.weight - a.weight || a.priority - b.priority);
  return plan;
}

export function isPriorWaveAccession(accession) {
  const a = String(accession || "");
  return V2_KNOWN_ACCESSIONS.some(
    (k) => k.replace(/-/g, "") === a.replace(/-/g, ""),
  );
}

/**
 * After a batch, boost query families / periods that produced verified dates;
 * keep exploration weight for low-yield families.
 */
export function adaptWeightsFromBatch(prevWeights = {}, batchStats = {}) {
  const next = { ...prevWeights };
  const byFamily = batchStats.yieldByQueryFamily || {};
  for (const [fam, count] of Object.entries(byFamily)) {
    const n = Number(count) || 0;
    const prev = next[fam] != null ? Number(next[fam]) : 1;
    // Boost productive families; floor exploration at 0.35
    next[fam] = Math.max(0.35, Number((prev * 0.6 + (1 + Math.log1p(n)) * 0.4).toFixed(3)));
  }
  // Gentle decay for families not seen this batch (still explored)
  for (const fam of V3_SEC_API_QUERY_FAMILIES) {
    if (byFamily[fam.id] == null) {
      const prev = next[fam.id] != null ? Number(next[fam.id]) : 1 / fam.priority;
      next[fam.id] = Math.max(0.35, Number((prev * 0.92).toFixed(3)));
    }
  }
  return next;
}

export default {
  listV3SecApiQueryPlan,
  adaptWeightsFromBatch,
  isPriorWaveAccession,
  V3_SEC_API_QUERY_FAMILIES,
  V2_KNOWN_ACCESSIONS,
  AGREEMENT_DISCOVERY_FAMILY,
};
