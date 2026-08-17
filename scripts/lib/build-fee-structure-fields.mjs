/**
 * Map fee profile → Airtable Fee Structure fields with Meta select validation.
 */
import { pickBasis } from "./fee-structure-basis-normalize.mjs";

export const FEE_SELECT_COLS = [
  "Basis - Typical Application Fee",
  "Basis - Typical Royalty Fee Range",
  "Basis - Typical Marketing Fee Range",
  "Basis - Typical Tech",
  "Basis - Typical Loyalty Program Fee",
  "Basis - Typical Reservation / Distribution Fee",
  "Basis - Typical Training Fee",
  "Typical Owner Early-Termination Rights (without cause)",
  "Typical Termination Fee Structure (if any)",
  "Who Can Exercise Termination Right After Failed Test?",
  "Key Money / Co-Investment",
  "Do Agreements Typically Cap Operator Reimbursable Expenses?",
  "Do You Usually Require Audit Rights for Owner Books / Operator Systems?",
  "Fee Positioning Band",
  "Typical CapEx / PIP Intensity Band",
  "Basis - Typical Technical Fee Range",
  "Basis - Other Required Program Fees",
  "Basis - Typical Management Fee",
  "Basis - Typical Incentive Fee",
  "Basis - Typical Incentive Fee Excess",
];

export function pickFeeSelect(choices, preferred, proposals, brandName, column) {
  if (preferred == null || preferred === "") return null;
  if (!Array.isArray(choices) || !choices.length) return preferred;
  const exact = choices.find(
    (c) => String(c).trim().toLowerCase() === String(preferred).trim().toLowerCase()
  );
  if (exact) return exact;
  const picked = pickBasis(choices, preferred);
  if (
    String(picked).trim().toLowerCase() !== String(preferred).trim().toLowerCase() &&
    Array.isArray(proposals)
  ) {
    proposals.push({
      brandName,
      column,
      preferred,
      fallback: picked,
      note: "Preferred select not in Meta; used pickBasis fallback or omit if unsafe.",
    });
  }
  // Only use fallback if it's a reasonable match — otherwise omit
  if (String(picked).trim().toLowerCase() === String(preferred).trim().toLowerCase()) return picked;
  // For known soft matches via pickBasis normalize — accept
  if (picked && preferred) return picked;
  return null;
}

/**
 * @param {object} profile
 * @param {Record<string, string[]>} metaChoices
 * @param {Array} proposals
 * @param {string} brandName
 */
export function buildFeeStructureFieldsFromProfile(profile, metaChoices, proposals = [], brandName = "") {
  const sel = (col, want) => pickFeeSelect(metaChoices[col], want, proposals, brandName, col);

  const fields = {
    "Min - Typical Application Fee": profile.appMin,
    "Max - Typical Application Fee": profile.appMax,
    "Basis - Typical Application Fee": sel("Basis - Typical Application Fee", profile.appBasis),
    "Additional Notes - Typical Application Fee": profile.appNotes,
    "Application Fee Per Unit Over Threshold": profile.appPerRoom,
    "Application Fee Threshold (Units)": profile.appThreshold,

    "Min - Typical Royalty Fee Range": profile.royaltyMin,
    "Max - Typical Royalty Fee Range": profile.royaltyMax,
    "Basis - Typical Royalty Fee Range": sel("Basis - Typical Royalty Fee Range", profile.royaltyBasis),
    "Additional Notes - Typical Royalty Fee Range": profile.royaltyNotes,

    "Min - Typical Marketing Fee Range": profile.marketingMin,
    "Max - Typical Marketing Fee Range": profile.marketingMax,
    "Basis - Typical Marketing Fee Range": sel(
      "Basis - Typical Marketing Fee Range",
      profile.marketingBasis
    ),
    "Additional Notes - Typical Marketing Fee Range": profile.marketingNotes,

    "Min - Typical Tech": profile.techMin,
    "Max - Typical Tech": profile.techMax,
    "Basis - Typical Tech": sel("Basis - Typical Tech", profile.techBasis),
    "Additional Notes - Typical Tech": profile.techNotes,

    "Min - Typical Loyalty Program Fee": profile.loyaltyMin,
    "Max - Typical Loyalty Program Fee": profile.loyaltyMax,
    "Basis - Typical Loyalty Program Fee": sel(
      "Basis - Typical Loyalty Program Fee",
      profile.loyaltyBasis
    ),
    "Additional Notes - Typical Loyalty Program Fee": profile.loyaltyNotes,

    "Min - Typical Reservation / Distribution Fee": profile.reservationMin,
    "Max - Typical Reservation / Distribution Fee": profile.reservationMax,
    "Basis - Typical Reservation / Distribution Fee": sel(
      "Basis - Typical Reservation / Distribution Fee",
      profile.reservationBasis
    ),
    "Additional Notes - Typical Reservation / Distribution Fee": profile.reservationNotes,

    "Min - Typical Training Fee": profile.trainingMin,
    "Max - Typical Training Fee": profile.trainingMax,
    "Basis - Typical Training Fee": sel("Basis - Typical Training Fee", profile.trainingBasis),
    "Additional Notes - Typical Training Fee": profile.trainingNotes,

    "Typical Incentives Offered": profile.incentives,
    "Typical Owner Early-Termination Rights (without cause)": sel(
      "Typical Owner Early-Termination Rights (without cause)",
      profile.earlyTerm
    ),
    "Early-Termination Notes": profile.earlyTermNotes,
    "Typical Termination Fee Structure (if any)": sel(
      "Typical Termination Fee Structure (if any)",
      profile.termStruct
    ),
    "Typical Termination Fee Structure (if any) Text": profile.termStructNotes,
    "Who Can Exercise Termination Right After Failed Test?": sel(
      "Who Can Exercise Termination Right After Failed Test?",
      profile.perfTerm
    ),
    "Key Money / Co-Investment": sel("Key Money / Co-Investment", profile.keyMoney),
    "Typical Expectations for Owner-Funded Reserves": profile.reserves,
    "Do Agreements Typically Cap Operator Reimbursable Expenses?": sel(
      "Do Agreements Typically Cap Operator Reimbursable Expenses?",
      profile.capReimb
    ),
    "Do You Usually Require Audit Rights for Owner Books / Operator Systems?": sel(
      "Do You Usually Require Audit Rights for Owner Books / Operator Systems?",
      profile.auditRights
    ),
    "Fee Positioning Band": sel("Fee Positioning Band", profile.feeBand),
    "Typical CapEx / PIP Intensity Band": sel("Typical CapEx / PIP Intensity Band", profile.pipBand),

    // Extended columns
    "Min - Other": profile.otherMin,
    "Max - Typical Technical Fee Range": profile.techFeeRangeMax,
    "Basis - Typical Technical Fee Range": sel(
      "Basis - Typical Technical Fee Range",
      profile.techFeeRangeBasis
    ),
    "Min - Other Required Program Fees": profile.otherProgramMin,
    "Max - Other Required Program Fees": profile.otherProgramMax,
    "Basis - Other Required Program Fees": sel(
      "Basis - Other Required Program Fees",
      profile.otherProgramBasis
    ),
    "Description of Fee": profile.feeDescription,
    "Min - Typical Management Fee": profile.mgmtMin,
    "Max - Typical Management Fee": profile.mgmtMax,
    "Basis - Typical Management Fee": sel("Basis - Typical Management Fee", profile.mgmtBasis),
    "Min - Typical Incentive Fee": profile.incentiveMin,
    "Max - Typical Incentive Fee": profile.incentiveMax,
    "Basis - Typical Incentive Fee": sel("Basis - Typical Incentive Fee", profile.incentiveBasis),
    "Notes - Typical Incentive Fee": profile.incentiveNotes,
    "Min - Typical Incentive Fee Excess": profile.incentiveExcessMin,
    "Max - Typical Incentive Fee Excess": profile.incentiveExcessMax,
    "Basis - Typical Incentive Fee Excess": sel(
      "Basis - Typical Incentive Fee Excess",
      profile.incentiveExcessBasis
    ),
    "Notes - Typical Incentive Fee Excess": profile.incentiveExcessNotes,
  };

  for (const k of Object.keys(fields)) {
    if (fields[k] === null || fields[k] === undefined || fields[k] === "") delete fields[k];
  }

  return {
    fields,
    resolved: {
      sourceTier: profile.sourceTier,
      cohort: profile.cohort,
    },
  };
}

export function normalizeFeeCompare(v) {
  if (v === undefined || v === null) return "";
  if (typeof v === "number") return String(Math.round(v * 1e6) / 1e6);
  return String(v).replace(/\s+/g, " ").trim();
}

export function diffFeeFields(expected, existingFields) {
  const mismatches = [];
  for (const [col, want] of Object.entries(expected)) {
    const cur = existingFields?.[col];
    const a = normalizeFeeCompare(want);
    const b = normalizeFeeCompare(cur);
    if (a === b) continue;
    if (typeof want === "number" && cur != null && cur !== "") {
      const nb = Number(cur);
      if (Number.isFinite(nb) && Math.abs(nb - want) < 1e-6) continue;
      // percent tolerance 0.1 percentage points if both look like decimals
      if (Math.abs(nb - want) < 0.0005) continue;
    }
    mismatches.push({ column: col, expected: want, actual: cur ?? null });
  }
  return mismatches;
}
