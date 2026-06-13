/**
 * Build Brand Explorer economics.* presentation rows from parsed Kimpton FDD economics.
 * Copy is owner-facing — no per-line FDD filename or Item 5/6/7 citations.
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
 * @param {ReturnType<import('./parse-kimpton-fdd-economics.mjs').parseKimptonFddEconomics>} econ
 * @param {{ brandName?: string }} [opts]
 */
export function applyKimptonEconomicsPresentationSlots(templateRows, econ, opts = {}) {
  const brandName = opts.brandName || "Kimpton Hotels";
  const i5 = econ.item5 || {};
  const i6 = econ.item6 || {};
  const i7 = econ.item7 || {};
  const i17 = econ.item17 || {};

  const royaltyLine =
    i6.royaltyRoomPct != null
      ? `${pctLabel(i6.royaltyRoomPct)} GRR${i6.royaltyFoodBeveragePct ? ` + ${pctLabel(i6.royaltyFoodBeveragePct)} F&B` : ""}`
      : "";
  const marketingLine =
    i6.servicesContributionPct != null
      ? `${pctLabel(i6.servicesContributionPct)} services contribution (GRR aggregate)`
      : "";
  const appLine =
    i5.applicationFeeMinimum != null
      ? `$${fmtUsd(i5.applicationFeeMinimum).replace("$", "")} min ($${i5.applicationFeePerRoom ?? 500}/key)`
      : "";
  const techLine =
    i6.technologyPerRoomMonthly != null ? `$${i6.technologyPerRoomMonthly.toFixed(2)}/room/month` : "";
  const loyaltyLine =
    i6.loyaltyFullFolioPct != null && i6.loyaltyRoomMeetingPct != null
      ? `${pctLabel(i6.loyaltyFullFolioPct)} full folio · ${pctLabel(i6.loyaltyRoomMeetingPct)} room/meeting`
      : "";
  const termLine = `${i17.termNewDevelopmentYears ?? 20} yr new build · ${i17.termConversionYears ?? 10} yr conversion · no auto-renewal`;
  const capitalLine =
    i7.perRoomMin != null && i7.perRoomMax != null
      ? `${fmtUsd(i7.perRoomMin)}–${fmtUsd(i7.perRoomMax)}/key (200-room illustrative, ex-land)`
      : "";

  const economicsRows = [
    {
      slotKey: "economics.intro",
      title: "",
      body: `${brandName} (upper-upscale lifestyle, IHG) economics below reflect typical disclosed ranges for diligence—not a quote or substitute for your franchise disclosure document, LOI, or advisors.`,
      sort: 0,
    },
    {
      slotKey: "economics.checklist",
      title: "",
      body: [
        royaltyLine && `Royalty stack (${royaltyLine}) and services contribution`,
        marketingLine && `Marketing & reservations (${marketingLine})`,
        loyaltyLine && `IHG One Rewards assessments (${loyaltyLine})`,
        techLine && `Technology (${techLine}) and mandatory IHG systems`,
        i6.gdsPerReservation != null && `GDS (~$${i6.gdsPerReservation.toFixed(2)}/reservation) and digital marketing (~2.25%)`,
        "Initial term length by deal type (new build vs conversion)",
        "PIP at opening, conversion, and renewal",
        "Performance tests, QA, and termination rights",
        "Transfer and change-of-control",
        "Incentives actually offered for this asset",
      ]
        .filter(Boolean)
        .join("\n"),
      sort: 0,
    },
    {
      slotKey: "economics.kpi.royalty",
      title: "",
      body: royaltyLine,
      sort: 0,
    },
    {
      slotKey: "economics.kpi.marketing",
      title: "",
      body: marketingLine,
      sort: 0,
    },
    {
      slotKey: "economics.kpi.application",
      title: "",
      body: appLine,
      sort: 0,
    },
    {
      slotKey: "economics.kpi.technology",
      title: "",
      body: techLine,
      sort: 0,
    },
    {
      slotKey: "economics.kpi.loyalty",
      title: "",
      body: loyaltyLine,
      sort: 0,
    },
    {
      slotKey: "economics.kpi.term",
      title: "",
      body: termLine,
      sort: 0,
    },
    {
      slotKey: "economics.kpi.training",
      title: "",
      body:
        i5.preopeningSupportMin != null
          ? `Pre-opening $${fmtUsd(i5.preopeningSupportMin).replace("$", "")}–${fmtUsd(i5.preopeningSupportMax ?? 35000).replace("$", "")} · cert $${fmtUsd(i5.deptHeadCertMin ?? 40000).replace("$", "")}–${fmtUsd(i5.deptHeadCertMax ?? 60000).replace("$", "")}`
          : "",
      sort: 0,
    },
    {
      slotKey: "economics.kpi.capital",
      title: "",
      body: capitalLine,
      sort: 0,
    },
    {
      slotKey: "economics.kpi.fee_stack",
      title: "",
      body: "Application · Royalty · Services contribution · Technology · Loyalty · GDS/digital · Training",
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
      body: "Performance and QA programs with cure periods—liquidated damages on certain terminations",
      sort: 0,
    },
    {
      slotKey: "economics.kpi.incentives",
      title: "",
      body: "Key money, ramp relief, marketing co-op—deal-specific in gateway markets",
      sort: 0,
    },
    {
      slotKey: "economics.kpi.negotiability",
      title: "",
      body: "Often negotiated",
      sort: 0,
    },
    {
      slotKey: "economics.fee",
      title: "Royalty / brand fee",
      body:
        royaltyLine
          ? `Ongoing ${royaltyLine} on prescribed revenue bases—upper-upscale lifestyle standards, QA, and IHG portfolio access.`
          : "Ongoing royalty on gross room and F&B revenues per disclosure.",
      sort: 1,
    },
    {
      slotKey: "economics.fee",
      title: "Marketing / brand fund",
      body:
        marketingLine
          ? `${marketingLine} covers marketing and reservations-related services in aggregate.`
          : "Marketing and reservations contribution per disclosure—model net after fund charges.",
      sort: 2,
    },
    {
      slotKey: "economics.fee",
      title: "Technology / systems",
      body:
        techLine
          ? `${techLine} technology services fee plus PMS (Opera), CRS, IHG Concerto, Wi‑Fi, and related mandatory stack.`
          : "Mandatory IHG PMS/CRS and technology schedule—budget cutover, interfaces, and training.",
      sort: 3,
    },
    {
      slotKey: "economics.fee",
      title: "Loyalty / program participation",
      body:
        loyaltyLine
          ? `IHG One Rewards: ${loyaltyLine}. Model net contribution after member benefits and chargebacks.`
          : "IHG One Rewards participation with folio-type assessments—model net P&L impact.",
      sort: 4,
    },
    {
      slotKey: "economics.fee",
      title: "Reservation / distribution",
      body:
        i6.gdsPerReservation != null
          ? `GDS about $${i6.gdsPerReservation.toFixed(2)}/reservation; IHG Ignite digital marketing about 2.25% on qualifying consumed revenue. Stress-test OTA vs direct mix.`
          : "IHG-mediated reservation and digital marketing fees—stress-test channel mix.",
      sort: 5,
    },
    {
      slotKey: "economics.fee",
      title: "Training / opening support",
      body: `Application ${appLine || "per disclosure"}; PIP inspection about $${fmtUsd(i5.pipInspectionFee ?? 12000).replace("$", "")}; pre-opening support $${fmtUsd(i5.preopeningSupportMin ?? 20000).replace("$", "")}–${fmtUsd(i5.preopeningSupportMax ?? 35000).replace("$", "")}; department-head certification $${fmtUsd(i5.deptHeadCertMin ?? 40000).replace("$", "")}–${fmtUsd(i5.deptHeadCertMax ?? 60000).replace("$", "")}.`,
      sort: 6,
    },
    {
      slotKey: "economics.fee.join",
      title: "To Join",
      body: [
        appLine && `Application ${appLine}`,
        i5.pipInspectionFee != null && `PIP inspection about ${fmtUsd(i5.pipInspectionFee)}`,
        `Pre-opening support $${fmtUsd(i5.preopeningSupportMin ?? 20000).replace("$", "")}–${fmtUsd(i5.preopeningSupportMax ?? 35000).replace("$", "")}`,
        `Department-head certification $${fmtUsd(i5.deptHeadCertMin ?? 40000).replace("$", "")}–${fmtUsd(i5.deptHeadCertMax ?? 60000).replace("$", "")}`,
        "PMS/CRS implementation often $97K–$130K for ~176–200 keys (premise-based illustrative)",
        "Design and F&B concept development fees vary by asset",
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
        marketingLine,
        loyaltyLine && `Loyalty ${loyaltyLine}`,
        techLine && `Technology ${techLine}`,
        i6.gdsPerReservation != null && `GDS $${i6.gdsPerReservation.toFixed(2)}/reservation`,
        "Capital reserve up to 5% of gross revenue may apply",
      ]
        .filter(Boolean)
        .join("; "),
      sort: 2,
    },
    {
      slotKey: "economics.fee.change",
      title: "When Things Change",
      body: "Renewal PIP, conversion PIP, re-licensing fees, termination liquidated damages, and reserves—triggered at renewal, flag change, or exit.",
      sort: 3,
    },
    {
      slotKey: "economics.opening.step.1",
      title: "Application & Feasibility",
      body:
        "Qualify urban gateway, resort, or conversion asset with IHG development—market tier, lifestyle F&B capability, and conversion PIP scope before term sheet.",
      sort: 1,
    },
    {
      slotKey: "economics.opening.step.2",
      title: "Design & Standards",
      body:
        "Kimpton design narrative and F&B program review—adaptive reuse, neighborhood character, wine hour and restaurant-forward spaces, and IHG design approval before major FF&E commit.",
      sort: 2,
    },
    {
      slotKey: "economics.opening.step.3",
      title: "Pre-Opening Planning",
      body:
        "PIP sequencing, OS&E, Opera PMS and IHG Concerto CRS cutover, department-head certification budget, and F&B onboarding aligned to lifestyle service standards.",
      sort: 3,
    },
    {
      slotKey: "economics.opening.step.4",
      title: "Opening Support",
      body:
        "IHG opening training, Kimpton service and wine hour execution, design and F&B QA, soft opening, and IHG One Rewards launch coordination.",
      sort: 4,
    },
    {
      slotKey: "economics.opening.step.5",
      title: "Stabilization",
      body:
        "Heightened lifestyle QA on design, F&B, and guest experience during ramp; third-party operators run day-to-day while IHG development tracks milestone remediation.",
      sort: 5,
    },
    {
      slotKey: "economics.opening.process",
      title: "",
      body:
        "Typical Kimpton path: feasibility on conversion or new-build fit, design and F&B narrative approval with realistic lifestyle PIP, pre-opening IHG systems and certification spend, opening QA on restaurant and guestroom standards, then stabilization. Third-party management is common; IHG development approves design and brand milestones while the operator runs opening.",
      sort: 0,
    },
    {
      slotKey: "economics.opening.financials",
      title: "",
      body:
        i7.totalInvestmentMin != null
          ? `Illustrative 200-room total investment ${fmtUsd(i7.totalInvestmentMin)}–${fmtUsd(i7.totalInvestmentMax)} (${fmtUsd(i7.perRoomMin)}–${fmtUsd(i7.perRoomMax)}/key, excluding land). Amounts paid to IHG/affiliates before opening often ${fmtUsd(i7.paidToIhgflMin)}–${fmtUsd(i7.paidToIhgflMax)} in the same illustration.\n\nFront-loaded standards, FF&E, F&B, and technology\n\nWorking capital through ramp (3-month initial phase often $1.5M+ hotel operating funds)\n\nFee stack stepping from opening-weighted to stabilized\n\nRenewal PIP reserves even if opening PIP is lighter`
          : "Front-loaded standards, FF&E, and technology; working capital through ramp; fee stack from opening to stabilized.",
      sort: 0,
    },
    {
      slotKey: "economics.model",
      title: "",
      body: `${brandName} trades recurring IHG fees and program participation for distribution, IHG One Rewards, lifestyle standards, and revenue support. Owners fund conversion or new-build capex, F&B complexity, and working capital through ramp.`,
      sort: 0,
    },
    {
      slotKey: "economics.term_renewal",
      title: "",
      body: `${termLine}. Re-licensing may require materially different terms than the original license.`,
      sort: 0,
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
  ];

  const nonEconomics = (templateRows || []).filter((r) => !String(r.slotKey || "").startsWith("economics."));
  return [...nonEconomics, ...economicsRows];
}
