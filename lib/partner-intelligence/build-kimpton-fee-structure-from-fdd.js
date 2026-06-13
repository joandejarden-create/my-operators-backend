/**
 * Map parsed Kimpton FDD economics → Brand Setup child table fields.
 * Owner-facing notes avoid per-line FDD filename / Item citations.
 */

const CONFIRM =
  "Confirm in your countersigned franchise disclosure document—not a property-specific quote.";

function pct(n) {
  if (n == null || !Number.isFinite(n)) return null;
  return n;
}

function fmtUsd(n) {
  if (n == null || !Number.isFinite(n)) return "";
  return n.toLocaleString("en-US", { maximumFractionDigits: 0 });
}

/**
 * @param {ReturnType<import('./parse-kimpton-fdd-economics.mjs').parseKimptonFddEconomics>} econ
 */
export function buildKimptonFeeStructurePatch(econ) {
  const i5 = econ.item5 || {};
  const i6 = econ.item6 || {};
  const i7 = econ.item7 || {};

  const royalty = pct(i6.royaltyRoomPct);
  const fb = pct(i6.royaltyFoodBeveragePct);
  const marketing = pct(i6.servicesContributionPct);
  const loyaltyHigh = pct(i6.loyaltyFullFolioPct);
  const loyaltyLow = pct(i6.loyaltyRoomMeetingPct);
  const tech = i6.technologyPerRoomMonthly;
  const gds = i6.gdsPerReservation;

  const appMin = i5.applicationFeeMinimum ?? 100000;
  const appPerRoom = i5.applicationFeePerRoom ?? 500;
  const appMax = Math.max(appMin, appPerRoom * 300);

  const trainingMin = i5.preopeningSupportMin ?? 20000;
  const trainingMax = Math.max(i5.deptHeadCertMax ?? 60000, i5.preopeningSupportMax ?? 35000);

  const patch = {
    "Min - Typical Application Fee": appMin,
    "Max - Typical Application Fee": appMax,
    "Basis - Typical Application Fee": "Per Application",
    "Additional Notes - Typical Application Fee": `$${appPerRoom}/guest room, minimum $${fmtUsd(appMin)}. Partial refund if application withdrawn before approval (less $15,000). ${CONFIRM}`,

    "Min - Typical Royalty Fee Range": royalty,
    "Max - Typical Royalty Fee Range": royalty,
    "Basis - Typical Royalty Fee Range": "% of Gross Revenue",
    "Additional Notes - Typical Royalty Fee Range":
      royalty != null && fb != null
        ? `${(royalty * 100).toFixed(0)}% on gross room revenue plus ${(fb * 100).toFixed(0)}% on gross food and beverage sales. ${CONFIRM}`
        : CONFIRM,

    "Min - Typical Marketing Fee Range": marketing,
    "Max - Typical Marketing Fee Range": marketing,
    "Basis - Typical Marketing Fee Range": "% of Gross Revenue",
    "Additional Notes - Typical Marketing Fee Range":
      marketing != null
        ? `Services contribution ${(marketing * 100).toFixed(0)}% of gross room revenue in aggregate (marketing and reservations-related fees). ${CONFIRM}`
        : CONFIRM,

    "Min - Typical Loyalty Program Fee": loyaltyLow ?? loyaltyHigh,
    "Max - Typical Loyalty Program Fee": loyaltyHigh ?? loyaltyLow,
    "Basis - Typical Loyalty Program Fee": "% of Gross Revenue",
    "Additional Notes - Typical Loyalty Program Fee":
      loyaltyHigh != null && loyaltyLow != null
        ? `IHG One Rewards assessments: ${(loyaltyHigh * 100).toFixed(2)}% qualifying full-folio revenue and ${(loyaltyLow * 100).toFixed(3)}% qualifying room and meeting revenue. Model net after chargebacks. ${CONFIRM}`
        : CONFIRM,

    "Min - Typical Tech": tech,
    "Max - Typical Tech": tech,
    "Basis - Typical Tech": "Per Room / Month",
    "Additional Notes - Typical Tech":
      tech != null
        ? `Technology services fee about $${tech.toFixed(2)}/room/month plus other mandatory IHG systems (PMS, CRS, digital, Wi‑Fi). ${CONFIRM}`
        : CONFIRM,

    "Min - Typical Reservation / Distribution Fee": gds,
    "Max - Typical Reservation / Distribution Fee": gds,
    "Basis - Typical Reservation / Distribution Fee": "Per Reservation / Per Booking",
    "Additional Notes - Typical Reservation / Distribution Fee":
      gds != null
        ? `GDS fee about $${gds.toFixed(2)}/reservation; IHG Ignite digital marketing commission about 2.25% on qualifying consumed revenue. ${CONFIRM}`
        : CONFIRM,

    "Min - Typical Training Fee": trainingMin,
    "Max - Typical Training Fee": trainingMax,
    "Basis - Typical Training Fee": "One-Time",
    "Additional Notes - Typical Training Fee": `Pre-opening support about $${fmtUsd(trainingMin)}–$${fmtUsd(i5.preopeningSupportMax ?? 35000)}; department-head certification about $${fmtUsd(i5.deptHeadCertMin ?? 40000)}–$${fmtUsd(i5.deptHeadCertMax ?? 60000)}; IHG Learning Program about $${fmtUsd(i5.ihglLearningAnnual ?? 5500)}/year. ${CONFIRM}`,

    "Typical Incentives Offered":
      "Key money, fee ramps, and conversion co-investment may be available in competitive gateway markets—deal-specific.",
    "Typical Termination Fee Structure (if any)": "Liquidated damages / lost future fees",
    "Typical Termination Fee Structure (if any) Text":
      "Termination with cause may trigger liquidated damages—review cure periods and exit framework with counsel.",
    "Who Can Exercise Termination Right After Failed Test?": "Either party (with cure periods)",
    "Key Money / Co-Investment": "Sometimes offered for strategic conversions",
    "Typical Expectations for Owner-Funded Reserves":
      "FF&E, capital reserve (up to 5% of gross revenue may apply), and public-space reserves typical for upper-upscale lifestyle conversions.",
  };

  if (i7.perRoomMin != null && i7.perRoomMax != null) {
    patch["Additional Notes - Typical Application Fee"] +=
      ` Illustrative 200-room total investment about $${fmtUsd(i7.perRoomMin)}–$${fmtUsd(i7.perRoomMax)}/key excluding land.`;
  }

  return Object.fromEntries(
    Object.entries(patch).filter(([, v]) => v != null && v !== "")
  );
}

/**
 * @param {ReturnType<import('./parse-kimpton-fdd-economics.mjs').parseKimptonFddEconomics>} econ
 */
export function buildKimptonDealTermsPatch(econ) {
  const i17 = econ.item17 || {};
  const newDev = i17.termNewDevelopmentYears ?? 20;
  const conversion = i17.termConversionYears ?? 10;

  return {
    "Quantity - Typical Minimum Initial Term": "1",
    "Length - Typical Minimum Initial Term": String(newDev),
    "Duration - Typical Minimum Initial Term": "Year(s)",
    "Quantity - Typical Renewal Option": "0",
    "Length - Typical Renewal Option": "",
    "Duration - Typical Renewal Option": "",
    "Renewal Structure": "No automatic renewal — re-licensing may be offered",
    "Renewal Notice Responsibility": "Mutual",
    "Typical Renewal Conditions": `Initial license term is ${newDev} years for new development and ${conversion} years for conversions; change-of-ownership/re-licensing typically ${conversion} years. License does not provide automatic renewal—re-licensing may carry materially different terms. ${CONFIRM}`,
    "Performance Test Requirement": "Yes",
    "Typical Cure Period for Performance Test Failure":
      "Cure periods apply to QA, fee, and reporting defaults—confirm with counsel.",
    "Duration - Typical Cure Period for Performance Test Failure": "Month(s)",
    "Typical QA": "Yes",
    "Mandatory PIP at Renewal": "Yes",
    "Mandatory PIP for Conversions": "Yes",
    "Typical Mandatory PIP for Conversions ($/room)": 25000,
    "Conversion - Typical max time allowed for completion": "24",
    "Conversion - Typical max time allowed for completion -Duration": "Month(s)",
    "Renewal - Typical max time allowed for completion": "24",
    "Renewal - Typical max time allowed for completion -Duration": "Month(s)",
  };
}
