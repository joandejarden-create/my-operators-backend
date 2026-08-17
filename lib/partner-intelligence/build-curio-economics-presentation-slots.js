/**
 * Build Brand Explorer economics.* presentation rows from parsed Curio FDD economics.
 * Copy is owner-facing — no per-line FDD filename or Item citations.
 */

function fmtUsd(n) {
  if (n == null || !Number.isFinite(n)) return "";
  if (n >= 1_000_000) return `$${(n / 1_000_000).toFixed(1)}M`;
  return `$${n.toLocaleString("en-US", { maximumFractionDigits: 0 })}`;
}

function pctLabel(n) {
  if (n == null || !Number.isFinite(n)) return "";
  const p = n * 100;
  return `${p % 1 === 0 ? p.toFixed(0) : p.toFixed(2)}%`;
}

/**
 * @param {object[]} templateRows
 * @param {ReturnType<import('./parse-curio-fdd-economics.mjs').parseCurioFddEconomics>} econ
 * @param {{ brandName?: string }} [opts]
 */
export function applyCurioEconomicsPresentationSlots(templateRows, econ, opts = {}) {
  const brandName = opts.brandName || "Curio Collection by Hilton";
  const i5 = econ.item5 || {};
  const i6 = econ.item6 || {};
  const i7 = econ.item7 || {};
  const i17 = econ.item17 || {};
  const i19 = econ.item19 || {};

  const royaltyLine =
    i6.royaltyRoomPct != null
      ? `${pctLabel(i6.royaltyRoomPct)} monthly royalty on gross rooms revenue (GRR)`
      : "";
  const programLine =
    i6.programFeePct != null
      ? `${pctLabel(i6.programFeePct)} monthly program fee on GRR`
      : "";
  const honorsLine =
    i6.honorsEligibleFolioPct != null
      ? `${pctLabel(i6.honorsEligibleFolioPct)} Hilton Honors on eligible guest folio (waived for on-property enrollment stays)`
      : "";
  const appLine =
    i5.applicationFeeNewDevBase != null
      ? `$${fmtUsd(i5.applicationFeeNewDevBase).replace("$", "")} new-build/conversion (+ $${i5.applicationFeePerRoomOver250 ?? 400}/key over 250)`
      : "";
  const termLine = `${i17.termNewDevelopmentYears ?? 23} yr new build · ${i17.termConversionYearsMin ?? 10}–${i17.termConversionYearsMax ?? 20} yr conversion · no automatic renewal`;
  const capitalLine =
    i7.perRoomMin != null && i7.perRoomMax != null
      ? `${fmtUsd(i7.perRoomMin)}–${fmtUsd(i7.perRoomMax)}/key (${i7.typicalRoomCount ?? 200}-room illustrative, ex-land)`
      : "";
  const honorsSampleLine =
    i19.honorsOccupancyContributionAvgPct != null
      ? `Item 19 sample (${i19.sampleYear ?? 2025}): ~${pctLabel(i19.honorsOccupancyContributionAvgPct)} avg Hilton Honors occupancy contribution (${i19.comparableHotelCount ?? 63} comparable U.S. hotels)`
      : "";

  const programRampNote =
    i6.programFeeRampY1Pct != null
      ? `Program fee may ramp (${pctLabel(i6.programFeeRampY1Pct)} yrs 1–2, ${pctLabel(i6.programFeeRampY3Pct ?? 0.035)} yr 3, ${pctLabel(i6.programFeePct)} steady-state)—not guaranteed.`
      : "";

  const economicsRows = [
    {
      slotKey: "economics.intro",
      title: "",
      body: `${brandName} (upper-upscale soft collection, Hilton) economics below reflect typical disclosed ranges from the franchise disclosure document—not a quote or substitute for your countersigned agreement, LOI, or advisors.`,
      sort: 0,
    },
    {
      slotKey: "economics.checklist",
      title: "",
      body: [
        royaltyLine && `Monthly royalty (${royaltyLine})`,
        programLine && `Monthly program fee (${programLine})`,
        programRampNote,
        honorsLine && `Hilton Honors (${honorsLine})`,
        i6.digitalAdvancePct != null &&
          `Digital direct marketing (${pctLabel(i6.digitalAdvancePct)} of digital direct revenue, capped per stay)`,
        i6.thirdPartyReservationPerStay != null &&
          `Third-party reservation charges (up to $${i6.thirdPartyReservationPerStay.toFixed(2)}/stay)`,
        i6.onqMaintMin != null &&
          `OnQ stack (initial ${fmtUsd(i5.onqInitialMin)}–${fmtUsd(i5.onqInitialMax)}; maintenance ${fmtUsd(i6.onqMaintMin)}–${fmtUsd(i6.onqMaintMax)}/mo)`,
        "Initial term length by deal type (new build vs conversion vs change of ownership)",
        "PIP at opening, conversion, re-licensing, and cycled renovation",
        "QA, brand non-compliance, and performance cure paths",
        "Transfer, change-of-control, and re-licensing (no automatic renewal)",
        "Royalty/program fee modifications offered in competitive deals",
        "Mandatory Hilton systems beyond headline fee lines",
      ]
        .filter(Boolean)
        .join("\n"),
      sort: 0,
    },
    {
      slotKey: "economics.cash.preopening",
      title: "",
      body: `Owner typically funds: Conversion or new-build PIP, FF&E, OnQ and connected-room cutover (${fmtUsd(i5.onqInitialMin)}–${fmtUsd(i5.onqInitialMax)} illustrative), working capital, and franchise application/training cash outlays.\n\nBrand typically provides: Design review, opening playbooks, pre-opening support ($${fmtUsd(i5.openingProcessServicesFee ?? 20000).replace("$", "")} opening process services fee), and milestone QA—not operating payroll.`,
      sort: 1,
    },
    {
      slotKey: "economics.cash.ramp",
      title: "",
      body: `Owner typically funds: Ramp marketing and Hilton Honors enrollment while occupancy builds; monthly royalty and program fees scale with gross rooms revenue.\n\nBrand typically provides: Negotiated program-fee ramp (${pctLabel(i6.programFeeRampY1Pct ?? 0.03)} early years when offered), royalty relief in competitive deals, and channel guidance—not guaranteed.`,
      sort: 2,
    },
    {
      slotKey: "economics.cash.steadystate",
      title: "",
      body: `Owner funds: Steady-state fee stack—${pctLabel(i6.royaltyRoomPct)} royalty + ${pctLabel(i6.programFeePct)} program fee on GRR, plus Hilton Honors, OnQ maintenance, and distribution charges.\n\nBrand provides: Hilton distribution, QA cadence, and benchmarks—not property payroll or routine FF&E.`,
      sort: 3,
    },
    {
      slotKey: "economics.cash.renewal",
      title: "",
      body: "Owner typically funds: Re-licensing application, renewal or conversion PIP, reserves, and potential new franchise agreement terms—there is no automatic renewal right.\n\nBrand typically provides: Re-licensing standards; phased PIP timing may be negotiable when re-licensing is offered in Hilton's discretion.",
      sort: 4,
    },
    {
      slotKey: "economics.opening.step.1",
      title: "Application & Feasibility",
      body: `Submit franchise application (${appLine || "per disclosure"}); pay PIP fee (${fmtUsd(i5.pipFee ?? 10000)} when applicable). Qualify conversion, new-build, or change-of-ownership path—market tier, F&B scope, and experiential PIP before term sheet.`,
      sort: 1,
    },
    {
      slotKey: "economics.opening.step.2",
      title: "Design & Standards",
      body: "Curio design narrative and culinary-forward F&B plan—adaptive reuse, destination character, and Hilton design approval before major FF&E commit.",
      sort: 2,
    },
    {
      slotKey: "economics.opening.step.3",
      title: "Pre-Opening Planning",
      body: `PIP sequencing, OS&E, OnQ PMS/CRS cutover (${fmtUsd(i5.onqInitialMin)}–${fmtUsd(i5.onqInitialMax)}), required training ($${fmtUsd(i5.trainingMin ?? 5000).replace("$", "")}–$${fmtUsd(i5.trainingMax ?? 15000).replace("$", "")}), and F&B onboarding.`,
      sort: 3,
    },
    {
      slotKey: "economics.opening.step.4",
      title: "Opening Support",
      body: `Hilton opening training, Curio service execution, design and F&B QA, soft opening, and Hilton Honors launch—budget opening process services (${fmtUsd(i5.openingProcessServicesFee ?? 20000)}).`,
      sort: 4,
    },
    {
      slotKey: "economics.opening.step.5",
      title: "Stabilization",
      body: "Heightened QA on design, culinary execution, and guest experience during ramp; third-party operators run day-to-day while Hilton development tracks milestone remediation.",
      sort: 5,
    },
    {
      slotKey: "economics.opening.process",
      title: "",
      body: "Typical Curio path: application and PIP scoping → design and F&B narrative approval → OnQ and Hilton systems cutover → opening QA on experiential standards → stabilization. Third-party management is common; Hilton approves brand milestones while the operator runs opening.",
      sort: 0,
    },
    {
      slotKey: "economics.opening.financials",
      title: "",
      body:
        i7.totalInvestmentMin != null
          ? `Illustrative ${i7.typicalRoomCount ?? 200}-room total investment ${fmtUsd(i7.totalInvestmentMin)}–${fmtUsd(i7.totalInvestmentMax)} (${capitalLine}, excluding real property). Amounts paid to Hilton/affiliates before opening up to ${fmtUsd(i7.paidToHiltonMax)} in the same illustration.\n\nFront-loaded conversion/new-build capex, FF&E, and technology\n\nWorking capital through ramp (3-month additional funds often $1.2M–$1.7M in disclosure table)\n\nFee stack stepping from ramped program fee to steady-state royalty + program fee\n\nRe-licensing PIP reserves—no automatic renewal`
          : "Front-loaded standards, FF&E, and technology; working capital through ramp; fee stack from opening to stabilized.",
      sort: 0,
    },
    {
      slotKey: "economics.fee.join",
      title: "To Join",
      body: [
        appLine && `Application ${appLine}`,
        i5.applicationFeeChangeOfOwnership != null &&
          `Change of ownership ${fmtUsd(i5.applicationFeeChangeOfOwnership)}`,
        i5.pipFee != null && `PIP fee ${fmtUsd(i5.pipFee)}`,
        i5.openingProcessServicesFee != null && `Opening process services ${fmtUsd(i5.openingProcessServicesFee)}`,
        i5.onqInitialMin != null && `OnQ hardware/software/install ${fmtUsd(i5.onqInitialMin)}–${fmtUsd(i5.onqInitialMax)}`,
        i5.trainingMin != null && `Required training $${fmtUsd(i5.trainingMin).replace("$", "")}–$${fmtUsd(i5.trainingMax ?? 15000).replace("$", "")}`,
        "Design, market study, and F&B concept costs vary by conversion vs new-build",
      ]
        .filter(Boolean)
        .join("; "),
      sort: 1,
    },
    {
      slotKey: "economics.fee.operate",
      title: "To Operate",
      body: [
        royaltyLine && `Steady-state ${royaltyLine}`,
        programLine,
        honorsLine,
        i6.digitalAdvancePct != null && `Hilton Advance ${pctLabel(i6.digitalAdvancePct)} digital direct (max $30/stay)`,
        i6.onqMaintMin != null && `OnQ maintenance ${fmtUsd(i6.onqMaintMin)}–${fmtUsd(i6.onqMaintMax)}/month`,
        i6.spaRoyaltyPct != null && `Eforea spa royalty ${pctLabel(i6.spaRoyaltyPct)} gross spa revenue (if spa amendment)`,
        "Model net contribution after loyalty chargebacks and distribution—not headline RevPAR",
      ]
        .filter(Boolean)
        .join("; "),
      sort: 2,
    },
    {
      slotKey: "economics.fee.change",
      title: "When Things Change",
      body: "Re-licensing (no auto-renewal), conversion PIP, room-addition fees ($400/key), termination liquidated damages, and reserves—triggered at exit, flag change, or ownership transfer.",
      sort: 3,
    },
    {
      slotKey: "economics.fee_variability",
      title: "",
      body: "Room count, market tier, new build vs conversion vs change of ownership, incentive package, spa/restaurant brand amendments, and operator-led opening change how fees and capital land on your deal.",
      sort: 0,
    },
    {
      slotKey: "economics.risk",
      title: "Term & renewal",
      body: `${termLine}. Re-licensing may require materially different terms than the original franchise agreement.`,
      sort: 1,
    },
    {
      slotKey: "economics.risk",
      title: "Performance & exit",
      body: "QA evaluations, brand non-compliance fees, service improvement programs, and cure paths—understand liquidated damages on early termination with counsel.",
      sort: 2,
    },
    {
      slotKey: "economics.risk",
      title: "Transfer & sale",
      body: "Change-of-ownership application ($150,000 in disclosure table), brand approval, and potential re-licensing terms affect liquidity—plan before marketing the asset.",
      sort: 3,
    },
    {
      slotKey: "economics.risk",
      title: "Area of protection",
      body: "Area-of-protection limits same-brand competition—confirm geography in franchise agreement and state addenda.",
      sort: 4,
    },
    {
      slotKey: "economics.negotiability",
      title: "",
      body: "Hilton modified monthly royalty fee in 10 instances and monthly program fee in 1 instance during 2025 (per disclosure)—key money, ramps, and conversion assistance may be available in competitive markets. Confirm what was offered for your asset.",
      sort: 0,
    },
    {
      slotKey: "economics.negotiable_items",
      title: "",
      body: "Monthly Royalty Fee Relief\nMonthly Program Fee Ramp\nFranchise Application Fee\nPIP Scope or Timing\nMarketing Co-Op or Opening Support",
      sort: 0,
    },
    {
      slotKey: "economics.rarely_negotiable",
      title: "",
      body: "Core Hilton Brand Standards Framework\nMandatory OnQ / HITS Technology Stack\nHilton Honors Program Participation\nFundamental QA and Reporting Obligations",
      sort: 0,
    },
    {
      slotKey: "economics.model",
      title: "",
      body: `${brandName} trades recurring Hilton fees and program participation for distribution, Hilton Honors, flexible upper-upscale standards, and revenue support. Owners fund conversion or new-build capex, culinary-forward F&B, and working capital through ramp.${honorsSampleLine ? ` ${honorsSampleLine}.` : ""}`,
      sort: 0,
    },
    {
      slotKey: "economics.kpi.fee_stack",
      title: "",
      body: "Application · Royalty · Program fee · Hilton Honors · OnQ · Digital direct · Training · QA",
      sort: 0,
    },
    {
      slotKey: "economics.kpi.agreement",
      title: "",
      body: termLine,
      sort: 0,
    },
    {
      slotKey: "economics.kpi.performance",
      title: "",
      body: "QA, brand non-compliance, and service improvement program fees with cure periods—liquidated damages on certain terminations",
      sort: 0,
    },
    {
      slotKey: "economics.kpi.capital",
      title: "",
      body: capitalLine || "Conversion/new-build PIP · owner reserves · prototype compliance",
      sort: 0,
    },
    {
      slotKey: "economics.kpi.incentives",
      title: "",
      body: "Royalty/program fee relief, application fee reduction, PIP timing—deal-specific (10 royalty modifications in 2025 per disclosure)",
      sort: 0,
    },
    {
      slotKey: "economics.kpi.negotiability",
      title: "",
      body: "Often negotiated in competitive conversions",
      sort: 0,
    },
    {
      slotKey: "economics.fee",
      title: "Royalty / brand fee",
      body:
        royaltyLine
          ? `${royaltyLine}—upper-upscale soft collection standards, QA, and Hilton portfolio access.${i6.spaRoyaltyPct ? ` Optional Eforea spa: +${pctLabel(i6.spaRoyaltyPct)} gross spa revenue.` : ""}`
          : "Ongoing monthly royalty on gross rooms revenue per disclosure.",
      sort: 1,
    },
    {
      slotKey: "economics.fee",
      title: "Marketing / brand fund",
      body:
        programLine
          ? `${programLine} funds system and network programs (not a segregated advertising fund). ${programRampNote} Rate capped at current plus 1% of GRR over franchise term.`
          : "Monthly program fee on gross rooms revenue—model net after fund charges.",
      sort: 2,
    },
    {
      slotKey: "economics.fee",
      title: "Technology / systems",
      body:
        i5.onqInitialMin != null
          ? `OnQ initial ${fmtUsd(i5.onqInitialMin)}–${fmtUsd(i5.onqInitialMax)}; connectivity ${fmtUsd(i6.onqConnMin)}–${fmtUsd(i6.onqConnMax)}/mo; maintenance ${fmtUsd(i6.onqMaintMin)}–${fmtUsd(i6.onqMaintMax)}/mo—plus connected room, GRO, and digital key stack per HITS agreement.`
          : "Mandatory OnQ PMS/CRS and HITS stack—budget cutover, interfaces, and training.",
      sort: 3,
    },
    {
      slotKey: "economics.fee",
      title: "Loyalty / program participation",
      body:
        honorsLine
          ? `${honorsLine}. Model net contribution after member benefits, reward redemptions, and guest assistance handling fees.`
          : "Hilton Honors participation with folio assessments—model net P&L impact.",
      sort: 4,
    },
    {
      slotKey: "economics.fee",
      title: "Reservation / distribution",
      body:
        i6.digitalAdvancePct != null
          ? `Hilton Advance ${pctLabel(i6.digitalAdvancePct)} on digital direct revenue (max $30/stay); third-party reservation charges up to $${(i6.thirdPartyReservationPerStay ?? 6.05).toFixed(2)}/stay. Stress-test OTA vs Hilton direct mix.${honorsSampleLine ? ` ${honorsSampleLine}.` : ""}`
          : "Hilton-mediated reservation and digital marketing fees—stress-test channel mix.",
      sort: 5,
    },
    {
      slotKey: "economics.fee",
      title: "Training / opening support",
      body: `Application ${appLine || "per disclosure"}; PIP ${fmtUsd(i5.pipFee ?? 10000)}; opening process services ${fmtUsd(i5.openingProcessServicesFee ?? 20000)}; training $${fmtUsd(i5.trainingMin ?? 5000).replace("$", "")}–$${fmtUsd(i5.trainingMax ?? 15000).replace("$", "")} plus attendee travel and wages.`,
      sort: 6,
    },
    {
      slotKey: "economics.lifecycle.preopening",
      title: "",
      body: "Heavy",
      sort: 0,
    },
    {
      slotKey: "economics.lifecycle.ramp",
      title: "",
      body: "Moderate–heavy",
      sort: 0,
    },
    {
      slotKey: "economics.lifecycle.steadystate",
      title: "",
      body: "Moderate",
      sort: 0,
    },
    {
      slotKey: "economics.lifecycle.renewal",
      title: "",
      body: "Moderate–heavy",
      sort: 0,
    },
    {
      slotKey: "economics.incentives",
      title: "",
      body: "Competitive markets may offer royalty relief, program-fee ramps, application fee reduction, or conversion co-investment—confirm actual package from Hilton development, not marketing typicals.",
      sort: 0,
    },
    {
      slotKey: "economics.term_renewal",
      title: "",
      body: `${termLine}. If Hilton offers re-licensing in its discretion, expect PIP conditions and potentially materially different fee terms.`,
      sort: 0,
    },
    {
      slotKey: "economics.performance_exit",
      title: "",
      body: "QA failures trigger re-evaluation fees and brand non-compliance charges—model termination exposure and liquidated damages with counsel.\n\nEarly exit without cause is not authorized.\n\nConfirm audit rights, guest assistance handling fees, and service improvement program costs.",
      sort: 0,
    },
    {
      slotKey: "economics.legal",
      title: "Area of protection",
      body: "Confirm area-of-protection geography in franchise agreement and state addenda.",
      sort: 1,
    },
    {
      slotKey: "economics.legal",
      title: "Transfer & sale",
      body: "Change-of-ownership and permitted transfers require Hilton approval—plan before marketing the asset.",
      sort: 2,
    },
    {
      slotKey: "economics.legal",
      title: "LOI & process",
      body: "Clarify binding vs exploratory LOI terms, design approval gates, and 14-day disclosure waiting period before application fees.",
      sort: 3,
    },
    {
      slotKey: "economics.support_burden",
      title: "",
      body: "Owners carry design review, culinary-forward F&B onboarding, OnQ cutover, training, and QA for upper-upscale soft collection—budget management time for Hilton brand milestones before signing.",
      sort: 0,
    },
    {
      slotKey: "economics.diligence",
      title: "",
      body: [
        "Which fee types apply to this asset (conversion vs new build vs change of ownership)?",
        `Renewal/re-licensing PIP scope for ${brandName}?`,
        "QA, brand non-compliance, and service improvement cure paths?",
        "Transfer rules with operator track record?",
        "Actual royalty/program fee relief or ramp offered?",
        "Who funds design review, OnQ implementation, and opening training?",
        honorsSampleLine && `Does Item 19 honors mix (${pctLabel(i19.honorsOccupancyContributionAvgPct)} avg) match your comp set?`,
      ]
        .filter(Boolean)
        .join("\n"),
      sort: 0,
    },
  ];

  const nonEconomics = (templateRows || []).filter((r) => !String(r.slotKey || "").startsWith("economics."));
  return [...nonEconomics, ...economicsRows];
}
