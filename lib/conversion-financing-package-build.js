/**
 * Conversion Financing Package — deterministic narrative builder.
 * Platform-derived interpretation; not financing advice or lender recommendation.
 */

function toText(value) {
  if (value == null) return "";
  if (Array.isArray(value)) return value.map((x) => toText(x)).filter(Boolean).join(", ");
  return String(value).trim();
}

function hasValue(value) {
  return toText(value) !== "";
}

function listOrDash(items) {
  const arr = Array.isArray(items) ? items.filter((x) => hasValue(x)) : [];
  return arr.length ? arr : ["—"];
}

function summarizeDealContext(dealFields) {
  const f = dealFields || {};
  const keys =
    f["Total Number of Rooms/Keys"] ??
    f["Total Number of Rooms"] ??
    f["Number of Keys"] ??
    f["Room Count"];
  return {
    propertyName: toText(f["Property Name"] || f["Project Name"] || f.Name),
    projectType: toText(f["Project Type"]),
    dealStage: toText(f["Stage of Development"] || f["Deal Status"]),
    city: toText(f.City),
    country: toText(f.Country),
    marketRegion: toText(f["Primary Market Region"] || f["Hotel Submarket & Location"]),
    hotelType: toText(f["Hotel Type"] || f["Hotel Chain Scale"]),
    keys: keys != null ? String(keys) : "",
    keysRange: keys != null ? String(keys) : "",
    currentBrand: toText(f["Current Brand Affiliation"] || f["Is the hotel currently branded?"]),
    currentOperator: toText(f["Operator Name Current"] || f["Is the hotel currently managed by a third-party operator?"]),
    pipStatus: toText(f["PIP / CapEx Status"]),
  };
}

function inferProviderCategories(inputs) {
  const need = toText(inputs.capitalNeedType).toLowerCase();
  const unlock = (inputs.capitalUnlock || []).map((x) => toText(x).toLowerCase());
  const categories = [];

  const add = (label, rationale) => {
    if (!categories.some((c) => c.label === label)) categories.push({ label, rationale });
  };

  if (need.includes("development") || need.includes("construction")) {
    add("Development Lender", "May be relevant for ground-up or major construction capital needs.");
  }
  if (need.includes("bridge") || unlock.some((u) => u.includes("stabilization"))) {
    add("Bridge Lender", "Could be a fit depending on mandate for transitional or stabilization capital.");
  }
  if (need.includes("refinance") || toText(inputs.existingDebtStatus).toLowerCase().includes("maturity")) {
    add("Hospitality Lender", "Typically reviewed by lenders active in hotel refinance and maturity solutions.");
  }
  if (need.includes("pip") || need.includes("renovation") || need.includes("conversion") || need.includes("repositioning")) {
    add("Private Credit", "May be relevant for renovation, PIP, or conversion-oriented capex.");
    add("Hospitality Lender", "Could be a fit depending on mandate for branded renovation or conversion paths.");
  }
  if (need.includes("working capital")) {
    add("Local Bank", "May be relevant for smaller working-capital or stabilization facilities, depending on mandate.");
  }
  if (need.includes("acquisition")) {
    add("PE / JV Capital", "Could be a fit depending on mandate for acquisition-plus-conversion structures.");
  }
  if (unlock.some((u) => u.includes("operator"))) {
    add("Mezzanine / Preferred Equity", "May be relevant when operator transition affects near-term cash flow or equity needs.");
  }
  add("Family Office", "Could be a fit depending on mandate for owner-controlled hospitality opportunities.");
  add("Local Bank", "May be relevant for relationship-based or smaller-ticket hotel lending, depending on geography and size.");

  return categories.slice(0, 8);
}

function collectRiskFlags(inputs, dealCtx) {
  const flags = [];
  const add = (item, question) => flags.push({ item, question });

  if (!hasValue(inputs.capitalNeedType)) add("Financing need type", "What type of capital is being requested?");
  if (!hasValue(inputs.capitalAmount) && !hasValue(inputs.capitalAmountRange)) {
    add("Capital amount", "What amount or range is being requested?");
  }
  if (!hasValue(inputs.capitalUnlock) || !(inputs.capitalUnlock || []).length) {
    add("Capital unlock", "What strategic outcome should this capital enable?");
  }
  if (!hasValue(inputs.useOfProceeds) || !(inputs.useOfProceeds || []).length) {
    add("Use of proceeds", "How will proceeds be allocated across PIP, renovation, debt, or other uses?");
  }
  if (!hasValue(inputs.pipCapexEstimate) && !hasValue(inputs.pipCapexEstimateStatus)) {
    add("PIP / capex scope", "Is there an estimated renovation or PIP budget?");
  }
  if (toText(inputs.capitalFinancialsAvailability).includes("Not Available") || toText(inputs.capitalFinancialsAvailability).includes("Not Yet")) {
    add("Historical financials", "What operating history can be shared with a capital provider?");
  }
  if (toText(inputs.existingDebtStatus).includes("Unknown") || toText(inputs.existingDebtStatus).includes("Not Disclosed")) {
    add("Existing debt", "What is the current debt position and maturity profile?");
  }
  if (toText(inputs.ownerEquityContributionStatus).includes("Not Yet") || toText(inputs.ownerEquityContributionStatus).includes("Not Disclosed")) {
    add("Owner equity", "How much sponsor equity is available or planned?");
  }
  if (!hasValue(inputs.targetBrandPath) && toText(inputs.capitalBrandStatus).toLowerCase().includes("conversion")) {
    add("Target brand path", "Which brand path is under consideration?");
  }
  if (!hasValue(dealCtx.keys)) add("Room count", "How many keys does the asset have?");
  if ((inputs.supportingDocumentsAvailable || []).includes("None Yet")) {
    add("Supporting documents", "Which financial or project documents can be prepared for review?");
  }
  return flags;
}

function collectExecutionDependencies(inputs) {
  const deps = [];
  const brand = toText(inputs.capitalBrandStatus).toLowerCase();
  const operator = toText(inputs.capitalOperatorStatus).toLowerCase();
  const pip = toText(inputs.pipCapexEstimateStatus).toLowerCase();

  if (brand.includes("loi") || brand.includes("application") || brand.includes("conversion")) {
    deps.push("Brand approval or franchise agreement path");
  }
  if (pip.includes("not estimated") || pip === "") deps.push("PIP or renovation budget finalization");
  if (operator.includes("not selected") || operator.includes("being evaluated")) {
    deps.push("Operator selection or management agreement");
  }
  if (toText(inputs.existingDebtStatus).toLowerCase().includes("maturity")) {
    deps.push("Existing debt maturity or refinance coordination");
  }
  if (toText(inputs.ownerEquityContributionStatus).toLowerCase().includes("not yet")) {
    deps.push("Owner equity commitment");
  }
  if (toText(inputs.capitalNeedType).toLowerCase().includes("acquisition")) {
    deps.push("Acquisition closing timeline and purchase agreement");
  }
  if ((inputs.useOfProceeds || []).some((u) => toText(u).toLowerCase().includes("permit"))) {
    deps.push("Permits and regulatory approvals");
  }
  return [...new Set(deps)];
}

/**
 * @param {object} inputs Owner financing request inputs
 * @param {object} [dealFields] Merged deal fields for opportunity context
 */
export function buildConversionFinancingPackage(inputs, dealFields) {
  const i = inputs || {};
  const dealCtx = summarizeDealContext(dealFields);
  const amountLabel = hasValue(i.capitalAmountRange)
    ? i.capitalAmountRange
    : hasValue(i.capitalAmount)
      ? i.capitalAmount
      : "Not specified";

  const snapshot = {
    opportunitySummary: {
      headline: "Opportunity Summary",
      summary: [
        dealCtx.propertyName
          ? `${dealCtx.propertyName} — ${dealCtx.projectType || "hotel opportunity"}`
          : "Hotel opportunity",
        [dealCtx.city, dealCtx.country].filter(Boolean).join(", ") || dealCtx.marketRegion || "Location pending",
        dealCtx.keys ? `${dealCtx.keys} keys` : "Keys not specified",
        dealCtx.dealStage ? `Stage: ${dealCtx.dealStage}` : null,
        dealCtx.currentBrand ? `Current brand context: ${dealCtx.currentBrand}` : null,
      ]
        .filter(Boolean)
        .join(" · "),
      marketRegion: dealCtx.marketRegion,
      country: dealCtx.country,
      hotelType: dealCtx.hotelType,
      keysRange: dealCtx.keysRange,
      assetStatus: i.capitalAssetStatus,
      financingContext: i.capitalNeedType,
    },
    capitalRequest: {
      headline: "Capital Request",
      needType: i.capitalNeedType,
      amount: amountLabel,
      currency: i.capitalCurrency,
      timing: i.capitalTiming,
      useOfProceeds: listOrDash(i.useOfProceeds),
      summary: `Requesting ${amountLabel}${i.capitalCurrency ? ` (${i.capitalCurrency})` : ""} for ${i.capitalNeedType || "unspecified need type"} with timing ${i.capitalTiming || "not specified"}.`,
    },
    conversionRepositioningThesis: {
      headline: "Conversion / Repositioning Thesis",
      capitalUnlock: listOrDash(i.capitalUnlock),
      summary: hasValue(i.capitalUnlock)
        ? `Capital is intended to help unlock: ${listOrDash(i.capitalUnlock).join("; ")}. This should be read alongside the hotel's brand, operator, and renovation path.`
        : "Capital unlock purpose not yet specified — a capital provider will likely ask what strategic outcome this financing enables.",
    },
    brandOperatorContext: {
      headline: "Brand / Operator Context",
      currentBrandStatus: i.capitalBrandStatus,
      targetBrandPath: i.targetBrandPath,
      operatorStatus: i.capitalOperatorStatus,
      summary: [
        i.capitalBrandStatus ? `Brand: ${i.capitalBrandStatus}` : null,
        i.targetBrandPath ? `Target path: ${i.targetBrandPath}` : null,
        i.capitalOperatorStatus ? `Operator: ${i.capitalOperatorStatus}` : null,
        dealCtx.currentBrand ? `Deal setup brand: ${dealCtx.currentBrand}` : null,
        dealCtx.currentOperator ? `Deal setup operator: ${dealCtx.currentOperator}` : null,
      ]
        .filter(Boolean)
        .join(" · "),
    },
    pipCapexScope: {
      headline: "PIP / Capex Scope",
      estimate: i.pipCapexEstimate,
      estimateStatus: i.pipCapexEstimateStatus,
      dealPipStatus: dealCtx.pipStatus,
      summary: [
        i.pipCapexEstimate ? `Estimate: ${i.pipCapexEstimate}` : null,
        i.pipCapexEstimateStatus ? `Status: ${i.pipCapexEstimateStatus}` : null,
        dealCtx.pipStatus ? `Deal setup PIP status: ${dealCtx.pipStatus}` : null,
      ]
        .filter(Boolean)
        .join(" · ") || "PIP / capex scope not yet documented.",
    },
    financialBaseline: {
      headline: "Financial Baseline",
      availability: i.capitalFinancialsAvailability,
      documents: listOrDash(i.supportingDocumentsAvailable),
      summary: i.capitalFinancialsAvailability
        ? `Financials: ${i.capitalFinancialsAvailability}. Documents noted: ${listOrDash(i.supportingDocumentsAvailable).join(", ")}.`
        : "Financial baseline not yet specified.",
    },
    capitalStackContext: {
      headline: "Capital Stack Context",
      existingDebtStatus: i.existingDebtStatus,
      ownerEquityStatus: i.ownerEquityContributionStatus,
      summary: [
        i.existingDebtStatus ? `Debt: ${i.existingDebtStatus}` : null,
        i.ownerEquityContributionStatus ? `Equity: ${i.ownerEquityContributionStatus}` : null,
      ]
        .filter(Boolean)
        .join(" · ") || "Capital stack details not yet specified.",
    },
    executionDependencies: {
      headline: "Execution Dependencies",
      items: collectExecutionDependencies(i),
    },
    capitalProviderReviewLens: {
      headline: "Capital Provider Review Lens",
      disclaimer:
        "Indicative categories only — not a lender recommendation, approval, or guarantee of financing.",
      categories: inferProviderCategories(i),
    },
    riskFlagsAndOpenQuestions: {
      headline: "Risk Flags & Open Questions",
      items: collectRiskFlags(i, dealCtx),
    },
    ownerSharingControls: {
      headline: "Owner Sharing Controls",
      sharingPreference: i.capitalSharingPreference,
      sharingStatus: i.capitalSharingStatus,
      summary:
        "This financing request is private by default. Owner controls whether an anonymized summary or named opportunity is shared with capital providers.",
    },
  };

  const narrativeSections = [
    snapshot.opportunitySummary,
    snapshot.capitalRequest,
    snapshot.conversionRepositioningThesis,
    snapshot.brandOperatorContext,
    snapshot.pipCapexScope,
    snapshot.financialBaseline,
    snapshot.capitalStackContext,
    {
      headline: "Execution Dependencies",
      summary: snapshot.executionDependencies.items.length
        ? snapshot.executionDependencies.items.join("; ")
        : "No major execution dependencies identified from current inputs.",
    },
    {
      headline: "Capital Provider Review Lens",
      summary: snapshot.capitalProviderReviewLens.categories
        .map((c) => `${c.label}: ${c.rationale}`)
        .join(" "),
    },
    {
      headline: "Risk Flags & Open Questions",
      summary: snapshot.riskFlagsAndOpenQuestions.items.length
        ? snapshot.riskFlagsAndOpenQuestions.items.map((r) => r.question).join(" ")
        : "No major open questions identified from current inputs.",
    },
  ];

  const narrative = narrativeSections
    .map((sec) => `## ${sec.headline}\n${sec.summary}`)
    .join("\n\n");

  const riskFlagsText = snapshot.riskFlagsAndOpenQuestions.items
    .map((r) => `${r.item}: ${r.question}`)
    .join("\n");
  const depsText = snapshot.executionDependencies.items.join("\n");
  const providerFitText = snapshot.capitalProviderReviewLens.categories
    .map((c) => `${c.label} — ${c.rationale}`)
    .join("\n");

  return {
    snapshot,
    narrative,
    derived: {
      riskFlagsText,
      executionDependenciesText: depsText,
      providerCategoryFitText: providerFitText,
    },
    labels: {
      status: "Platform-Derived",
      packageType: "Conversion Financing Package",
    },
  };
}

export function sanitizeConversionFinancingInputs(raw) {
  const p = raw && typeof raw === "object" ? raw : {};
  const arr = (v) =>
    Array.isArray(v) ? v.map((x) => toText(x)).filter(Boolean) : toText(v) ? [toText(v)] : [];
  return {
    capitalNeedType: toText(p.capitalNeedType),
    capitalAmount: toText(p.capitalAmount),
    capitalAmountRange: toText(p.capitalAmountRange),
    capitalCurrency: toText(p.capitalCurrency),
    useOfProceeds: arr(p.useOfProceeds),
    capitalUnlock: arr(p.capitalUnlock),
    capitalAssetStatus: toText(p.capitalAssetStatus),
    capitalBrandStatus: toText(p.capitalBrandStatus),
    targetBrandPath: toText(p.targetBrandPath),
    pipCapexEstimate: toText(p.pipCapexEstimate),
    pipCapexEstimateStatus: toText(p.pipCapexEstimateStatus),
    capitalOperatorStatus: toText(p.capitalOperatorStatus),
    capitalFinancialsAvailability: toText(p.capitalFinancialsAvailability),
    existingDebtStatus: toText(p.existingDebtStatus),
    ownerEquityContributionStatus: toText(p.ownerEquityContributionStatus),
    capitalTiming: toText(p.capitalTiming),
    capitalSharingPreference: toText(p.capitalSharingPreference),
    capitalSharingStatus: toText(p.capitalSharingStatus) || "Draft",
    supportingDocumentsAvailable: arr(p.supportingDocumentsAvailable),
  };
}
