/**
 * Query families for Contract Timing V2 multi-provider discovery.
 */

export const CONTRACT_TIMING_V2_QUERY_FAMILIES_V1 = "contract-timing-v2-query-families-v1";

/** Baseline accession excluded from incremental yield. */
export const BASELINE_JPMCC_ACCESSION = "0001539497-20-000579";
export const BASELINE_JPMCC_DATE_COUNT = 57;

export const SEC_API_QUERY_FAMILIES = Object.freeze([
  {
    id: "franchise_expiration_date",
    query: '"Franchise Expiration Date"',
    priority: 1,
    formTypesPreferred: ["424B2", "424B5", "424B3", "424B4"],
  },
  {
    id: "franchise_agreement_expiration",
    query: '"Franchise Agreement Expiration"',
    priority: 1,
    formTypesPreferred: ["424B2", "424B5", "10-K", "S-11"],
  },
  {
    id: "franchise_termination_date",
    query: '"Franchise Termination Date"',
    priority: 2,
    formTypesPreferred: ["424B2", "424B5", "8-K", "10-K"],
  },
  {
    id: "management_agreement_expiration",
    query: '"Management Agreement Expiration"',
    priority: 1,
    formTypesPreferred: ["424B2", "424B5", "10-K", "EX-10"],
  },
  {
    id: "hotel_management_agreement",
    query: '"Hotel Management Agreement"',
    priority: 2,
    formTypesPreferred: ["EX-10", "10-K", "8-K", "S-11"],
  },
  {
    id: "hotel_franchise_agreement",
    query: '"Hotel Franchise Agreement"',
    priority: 2,
    formTypesPreferred: ["EX-10", "10-K", "8-K"],
  },
  {
    id: "license_agreement_expiration",
    query: '"License Agreement Expiration"',
    priority: 2,
    formTypesPreferred: ["424B2", "424B5", "10-K"],
  },
  {
    id: "expiration_or_termination_franchise",
    query: '"Expiration or Termination of Franchise"',
    priority: 2,
    formTypesPreferred: ["424B2", "424B5", "10-K"],
  },
  {
    id: "mortgaged_property_hotel",
    query: '"Mortgaged Property" Hotel Franchise',
    priority: 3,
    formTypesPreferred: ["424B2", "424B5"],
  },
  {
    id: "hotel_property_schedule",
    query: '"Hotel Property Schedule" Franchise',
    priority: 3,
    formTypesPreferred: ["10-K", "S-11", "424B5"],
  },
  {
    id: "initial_term_renewal_option_hma",
    query: '"Initial Term" "Renewal Option" "Hotel Management"',
    priority: 3,
    formTypesPreferred: ["EX-10", "10-K", "S-11"],
  },
]);

export const EXA_QUERY_FAMILIES = Object.freeze([
  {
    id: "hotel_franchise_renewal_announce",
    query: "hotel franchise agreement renewal expiration date announcement",
    priority: 1,
  },
  {
    id: "management_agreement_extension",
    query: "hotel management agreement extension renewal term years",
    priority: 1,
  },
  {
    id: "hotel_reflag_conversion",
    query: "hotel conversion reflag franchise agreement termination Marriott Hilton Hyatt",
    priority: 2,
  },
  {
    id: "cmbs_franchise_expiration_disclosure",
    query: "CMBS prospectus Franchise Expiration Date hotel collateral schedule",
    priority: 1,
  },
  {
    id: "owner_operator_change_contract",
    query: "hotel owner operator change management agreement signed expires",
    priority: 2,
  },
  {
    id: "historical_hotel_name_franchise",
    query: "formerly known as hotel franchise agreement expiration",
    priority: 3,
  },
]);

/** Period partitions to overcome result caps / diversify coverage. */
export const PERIOD_PARTITIONS = Object.freeze([
  { id: "2012_2015", startDate: "2012-01-01", endDate: "2015-12-31" },
  { id: "2016_2018", startDate: "2016-01-01", endDate: "2018-12-31" },
  { id: "2019_2020", startDate: "2019-01-01", endDate: "2020-12-31" },
  { id: "2021_2022", startDate: "2021-01-01", endDate: "2022-12-31" },
  { id: "2023_2024", startDate: "2023-01-01", endDate: "2024-12-31" },
  { id: "2025_2026", startDate: "2025-01-01", endDate: "2026-12-31" },
]);

export function listSecApiQueryPlan(opts = {}) {
  const families = [...SEC_API_QUERY_FAMILIES].sort((a, b) => a.priority - b.priority);
  const periods = opts.periods || PERIOD_PARTITIONS;
  const plan = [];
  for (const fam of families) {
    for (const period of periods) {
      plan.push({
        id: `${fam.id}__${period.id}`,
        queryFamily: fam.id,
        query: fam.query,
        formTypes: fam.formTypesPreferred,
        startDate: period.startDate,
        endDate: period.endDate,
        periodId: period.id,
        priority: fam.priority,
      });
    }
  }
  return plan;
}

export function isBaselineAccession(accession) {
  return String(accession || "").replace(/-/g, "") === BASELINE_JPMCC_ACCESSION.replace(/-/g, "");
}

export default {
  SEC_API_QUERY_FAMILIES,
  EXA_QUERY_FAMILIES,
  PERIOD_PARTITIONS,
  listSecApiQueryPlan,
  isBaselineAccession,
  BASELINE_JPMCC_ACCESSION,
};
