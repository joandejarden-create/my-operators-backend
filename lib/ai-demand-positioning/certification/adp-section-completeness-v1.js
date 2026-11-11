/**
 * ADP Section Completeness Certification V1
 *
 * Independent per-section assessment of customer-facing ADP report product.
 * Used by Full E2E OS and portfolio product-completeness audits.
 *
 * Status vocabulary (binding):
 *   POPULATED_AND_CERTIFIED | POPULATED_NOT_CERTIFIED | DATA_EXISTS_NOT_CONNECTED
 *   RESEARCH_REQUIRED | COMPARABLE_HISTORY_REQUIRED | BENCHMARK_REQUIRED
 *   NOT_APPLICABLE | BLOCKED_TECHNICAL | BLOCKED_AUTHORIZATION
 */

export const ADP_SECTION_COMPLETENESS_V1 = "ADP_SECTION_COMPLETENESS_V1";

export const SECTION_STATUS = Object.freeze({
  POPULATED_AND_CERTIFIED: "POPULATED_AND_CERTIFIED",
  POPULATED_NOT_CERTIFIED: "POPULATED_NOT_CERTIFIED",
  DATA_EXISTS_NOT_CONNECTED: "DATA_EXISTS_NOT_CONNECTED",
  RESEARCH_REQUIRED: "RESEARCH_REQUIRED",
  COMPARABLE_HISTORY_REQUIRED: "COMPARABLE_HISTORY_REQUIRED",
  BENCHMARK_REQUIRED: "BENCHMARK_REQUIRED",
  NOT_APPLICABLE: "NOT_APPLICABLE",
  BLOCKED_TECHNICAL: "BLOCKED_TECHNICAL",
  BLOCKED_AUTHORIZATION: "BLOCKED_AUTHORIZATION",
});

/**
 * Canonical ADP product sections (aligned to owner-ai-demand.html + owner payload).
 */
export const ADP_PRODUCT_SECTIONS = Object.freeze([
  {
    id: "executive_summary",
    label: "Executive Summary",
    purpose: "Owner-facing narrative of strength, constraint, focus",
    payloadPaths: ["executiveRead", "brief"],
    required: true,
  },
  {
    id: "biggest_strength",
    label: "Biggest Strength",
    purpose: "Primary positive demand signal",
    payloadPaths: ["executiveRead.primaryStrength", "executiveRead.current.primaryStrength"],
    required: true,
  },
  {
    id: "biggest_constraint",
    label: "Biggest Constraint",
    purpose: "Primary binding weakness",
    payloadPaths: ["executiveRead.primaryConstraint", "executiveRead.current.primaryConstraint"],
    required: true,
  },
  {
    id: "changes_since_prior",
    label: "Changes Since Last Comparable Run",
    purpose: "Certified delta vs comparable prior period",
    payloadPaths: ["executiveRead.trend", "executiveRead.COMPARABLE_PRIOR_AVAILABLE"],
    required: true,
  },
  {
    id: "property_snapshot",
    label: "Property Snapshot",
    purpose: "Headline KPI strip for the period",
    payloadPaths: ["executiveMetrics", "demandCapture"],
    required: true,
  },
  {
    id: "ai_consideration_rate",
    label: "AI Consideration Rate",
    purpose: "Answer-level presence rate",
    payloadPaths: ["executiveMetrics.considerationRate"],
    required: true,
  },
  {
    id: "ai_scenario_presence",
    label: "AI Scenario Presence",
    purpose: "Scenario-level presence rate",
    payloadPaths: ["executiveMetrics.scenarioPresence", "demandCapture"],
    required: true,
  },
  {
    id: "rank_appearance",
    label: "#1 / Top-3 Appearance Rate",
    purpose: "Ranked appearance rates when measurable",
    payloadPaths: ["executiveMetrics.rankMetrics"],
    required: true,
  },
  {
    id: "competitor_present_scenarios",
    label: "Competitor-Present Scenarios",
    purpose: "Scenarios where competitors appear",
    payloadPaths: ["executiveMetrics.competitorPresentScenarios"],
    required: true,
  },
  {
    id: "demand_territories",
    label: "AI Presence by Demand Territory",
    purpose: "Intent / territory presence matrix",
    payloadPaths: ["intentPresenceIndex", "competitiveRankingByTerritory"],
    required: true,
  },
  {
    id: "provider_presence",
    label: "Provider Presence",
    purpose: "Per-provider presence and citations",
    payloadPaths: ["evidence.providers", "evidence.providerCitations"],
    required: true,
  },
  {
    id: "historical_trends",
    label: "Historical Trends",
    purpose: "Multi-period certified trend charts",
    payloadPaths: ["trends"],
    required: true,
  },
  {
    id: "brand_portfolio_position",
    label: "Brand & Portfolio Position",
    purpose: "Governed peer cohort ranking",
    payloadPaths: ["brandPortfolioPosition"],
    required: true,
  },
  {
    id: "ai_reality_gaps",
    label: "AI Reality Gaps",
    purpose: "Attribute recognition vs governed facts",
    payloadPaths: ["realityGap"],
    required: true,
  },
  {
    id: "property_reality_coverage",
    label: "Property Reality Coverage",
    purpose: "Share of governed attributes recognized",
    payloadPaths: ["realityGap"],
    required: true,
  },
  {
    id: "ai_recognizes_misses",
    label: "AI Recognizes / AI Misses",
    purpose: "Attribute-level recognition lists",
    payloadPaths: ["realityGap.recognized", "realityGap.gaps"],
    required: true,
  },
  {
    id: "ai_competitive_set",
    label: "AI Competitive Set",
    purpose: "Observed competitive set vs declared",
    payloadPaths: ["competitiveSet", "competitiveRankingByTerritory"],
    required: true,
  },
  {
    id: "competitive_displacement",
    label: "Competitive Displacement",
    purpose: "Who fills absences",
    payloadPaths: ["lostDemand.displacement"],
    required: true,
  },
  {
    id: "priority_actions",
    label: "Priority Actions",
    purpose: "Evidence-grounded recommendations",
    payloadPaths: ["actions"],
    required: true,
  },
  {
    id: "citation_intelligence",
    label: "Citation Intelligence / Source Landscape",
    purpose: "Source mix and citation coverage",
    payloadPaths: ["evidence.topSources", "evidence.citationRate", "evidence.ownedSourceShare"],
    required: true,
  },
  {
    id: "evidence_drawers",
    label: "Evidence and Source Drawers",
    purpose: "Traceable observation/source evidence",
    payloadPaths: ["evidence", "lostDemand.scenarios"],
    required: true,
  },
]);

function getPath(obj, path) {
  if (!path) return obj;
  const parts = String(path).split(".");
  let cur = obj;
  for (const p of parts) {
    if (cur == null) return undefined;
    cur = cur[p];
  }
  return cur;
}

function hasMeaningful(v) {
  if (v == null) return false;
  if (typeof v === "string") return v.trim().length > 0 && !/being established|not yet available|not available/i.test(v);
  if (typeof v === "number") return Number.isFinite(v);
  if (typeof v === "boolean") return true;
  if (Array.isArray(v)) return v.length > 0;
  if (typeof v === "object") return Object.keys(v).length > 0;
  return false;
}

function rateOk(rateObj) {
  if (rateObj == null) return false;
  if (typeof rateObj === "number") return Number.isFinite(rateObj);
  return Number.isFinite(Number(rateObj.rate)) || Number.isFinite(Number(rateObj.overallRate));
}

/**
 * @param {object} opts
 * @param {string} opts.propertyId
 * @param {object} opts.payload - owner report payload
 * @param {object} [opts.context]
 * @param {boolean} [opts.context.hasComparablePriorPeriod]
 * @param {number} [opts.context.periodArtifactCount]
 * @param {boolean} [opts.context.airtableAttributesPresent]
 * @param {boolean} [opts.context.commercialProfilePresent]
 * @param {boolean} [opts.context.bppReady]
 * @param {boolean} [opts.context.marketRegistryPresent]
 * @param {boolean} [opts.context.competitiveCertified]
 */
export function assessAdpSectionCompleteness({
  propertyId,
  payload,
  context = {},
} = {}) {
  const p = payload?.payload && typeof payload.payload === "object" ? payload.payload : payload || {};
  const sections = {};
  const blockers = [];

  const em = p.executiveMetrics || {};
  const er = p.executiveRead || {};
  const rg = p.realityGap || {};
  const evidence = p.evidence || {};
  const trends = Array.isArray(p.trends) ? p.trends : [];
  const trendPriorRoles = trends.filter((t) => t && t.role && t.role !== "current");
  const comparablePrior =
    context.hasComparablePriorPeriod === true ||
    er.COMPARABLE_PRIOR_AVAILABLE === true ||
    trendPriorRoles.length > 0;
  const bpp = p.brandPortfolioPosition || null;
  const bppReady =
    context.bppReady === true ||
    (bpp &&
      bpp.customerPublished === true &&
      bpp.status === "READY" &&
      Array.isArray(bpp.kpis) &&
      bpp.kpis.length > 0);

  function set(id, status, detail, nextAction = null) {
    sections[id] = { status, detail: detail || null, nextAction };
    if (
      status === SECTION_STATUS.BLOCKED_TECHNICAL ||
      status === SECTION_STATUS.RESEARCH_REQUIRED ||
      status === SECTION_STATUS.DATA_EXISTS_NOT_CONNECTED ||
      status === SECTION_STATUS.COMPARABLE_HISTORY_REQUIRED ||
      status === SECTION_STATUS.BENCHMARK_REQUIRED
    ) {
      blockers.push({ sectionId: id, status, detail, nextAction });
    }
  }

  // Executive
  if (er.available === false && !hasMeaningful(er.narrative) && !hasMeaningful(er.current)) {
    set("executive_summary", SECTION_STATUS.RESEARCH_REQUIRED, "executiveRead unavailable");
  } else if (hasMeaningful(er.narrative) || hasMeaningful(er.current) || hasMeaningful(p.brief?.items)) {
    // Executive section status is independent of whole-report atomic gate
    // (atomic failures still block canApprove via assessAtomicReportSnapshot).
    set("executive_summary", SECTION_STATUS.POPULATED_AND_CERTIFIED, "executiveRead/brief present");
  } else {
    set("executive_summary", SECTION_STATUS.BLOCKED_TECHNICAL, "executive payload missing");
  }

  const strength =
    getPath(er, "primaryStrength") ||
    getPath(er, "current.primaryStrength") ||
    getPath(er, "compositionV3.sections.biggestStrength");
  set(
    "biggest_strength",
    hasMeaningful(strength)
      ? SECTION_STATUS.POPULATED_AND_CERTIFIED
      : SECTION_STATUS.POPULATED_NOT_CERTIFIED,
    hasMeaningful(strength) ? "strength present" : "strength missing/generic"
  );

  const constraint =
    getPath(er, "primaryConstraint") ||
    getPath(er, "current.primaryConstraint") ||
    getPath(er, "compositionV3.sections.biggestConstraint");
  set(
    "biggest_constraint",
    hasMeaningful(constraint)
      ? SECTION_STATUS.POPULATED_AND_CERTIFIED
      : SECTION_STATUS.POPULATED_NOT_CERTIFIED,
    hasMeaningful(constraint) ? "constraint present" : "constraint missing"
  );

  if (comparablePrior) {
    set(
      "changes_since_prior",
      hasMeaningful(er.trend) || trendPriorRoles.length > 0
        ? SECTION_STATUS.POPULATED_AND_CERTIFIED
        : SECTION_STATUS.DATA_EXISTS_NOT_CONNECTED,
      comparablePrior
        ? "comparable prior flagged — verify trend attachment"
        : null,
      "ATTACH_CERTIFIED_TREND_FROM_COMPARABLE_PRIOR"
    );
  } else if ((context.periodArtifactCount || 0) >= 2) {
    set(
      "changes_since_prior",
      SECTION_STATUS.DATA_EXISTS_NOT_CONNECTED,
      `${context.periodArtifactCount} period artifacts on disk but not comparable/linked`,
      "RECONCILE_PERIOD_COMPARABILITY"
    );
  } else {
    set(
      "changes_since_prior",
      SECTION_STATUS.COMPARABLE_HISTORY_REQUIRED,
      "Single monitoring period baseline — no second comparable period",
      "AUTHORIZE_NEXT_MONITORING_PERIOD"
    );
  }

  set(
    "property_snapshot",
    rateOk(em.considerationRate) || rateOk(p.demandCapture)
      ? SECTION_STATUS.POPULATED_AND_CERTIFIED
      : SECTION_STATUS.BLOCKED_TECHNICAL,
    "headline KPIs"
  );
  set(
    "ai_consideration_rate",
    rateOk(em.considerationRate)
      ? SECTION_STATUS.POPULATED_AND_CERTIFIED
      : SECTION_STATUS.BLOCKED_TECHNICAL,
    em.considerationRate?.rate ?? null
  );
  set(
    "ai_scenario_presence",
    rateOk(em.scenarioPresence) || rateOk(p.demandCapture)
      ? SECTION_STATUS.POPULATED_AND_CERTIFIED
      : SECTION_STATUS.BLOCKED_TECHNICAL,
    em.scenarioPresence?.rate ?? p.demandCapture?.overallRate ?? null
  );

  const rm = em.rankMetrics || {};
  if (rm.rankEligibleN != null && Number(rm.rankEligibleN) === 0) {
    set(
      "rank_appearance",
      SECTION_STATUS.NOT_APPLICABLE,
      "No rank-eligible observations in period"
    );
  } else if (rateOk(rm.numberOneAppearanceRate) || rateOk(rm.topThreeAppearanceRate)) {
    set("rank_appearance", SECTION_STATUS.POPULATED_AND_CERTIFIED, "rank metrics present");
  } else if (rm.rankEligibleN == null && !rateOk(rm.numberOneAppearanceRate)) {
    // Rank metrics omitted when sample below MIN_RANK_SAMPLE — governed N/A, not a pipeline break
    set(
      "rank_appearance",
      SECTION_STATUS.NOT_APPLICABLE,
      "Rank metrics omitted — insufficient rank-eligible sample (not fabricated)"
    );
  } else {
    set("rank_appearance", SECTION_STATUS.POPULATED_NOT_CERTIFIED, "rank metrics incomplete");
  }

  const cps = em.competitorPresentScenarios;
  if (cps && typeof cps === "object" && Number.isFinite(Number(cps.scenarioCount))) {
    set(
      "competitor_present_scenarios",
      SECTION_STATUS.POPULATED_AND_CERTIFIED,
      `scenarioCount=${cps.scenarioCount}`
    );
  } else if (cps == null) {
    // Missing block after enrich — treat as zero when competitive analysis otherwise present
    set(
      "competitor_present_scenarios",
      hasMeaningful(p.competitiveSet) || hasMeaningful(p.lostDemand)
        ? SECTION_STATUS.POPULATED_AND_CERTIFIED
        : SECTION_STATUS.POPULATED_NOT_CERTIFIED,
      hasMeaningful(p.competitiveSet)
        ? "competitorPresentScenarios omitted; competitive set present — count treated as zero"
        : "competitorPresentScenarios missing"
    );
  } else {
    set("competitor_present_scenarios", SECTION_STATUS.POPULATED_NOT_CERTIFIED, null);
  }

  const territories =
    hasMeaningful(p.intentPresenceIndex) ||
    hasMeaningful(p.competitiveRankingByTerritory?.byTerritory);
  set(
    "demand_territories",
    territories
      ? SECTION_STATUS.POPULATED_AND_CERTIFIED
      : SECTION_STATUS.BLOCKED_TECHNICAL,
    null
  );

  const providers = evidence.providers || evidence.providerCitations;
  set(
    "provider_presence",
    hasMeaningful(providers)
      ? SECTION_STATUS.POPULATED_AND_CERTIFIED
      : SECTION_STATUS.BLOCKED_TECHNICAL,
    Array.isArray(providers) ? `${providers.length} providers` : typeof providers
  );

  // Trends
  if (trendPriorRoles.length >= 1 && trends.length >= 2) {
    set("historical_trends", SECTION_STATUS.POPULATED_AND_CERTIFIED, `${trends.length} trend points`);
  } else if ((context.periodArtifactCount || 0) >= 2) {
    set(
      "historical_trends",
      SECTION_STATUS.DATA_EXISTS_NOT_CONNECTED,
      "Multiple period files exist; staged snapshot missing multi-point trends",
      "ATTACH_TRENDS_ON_STAGED_READ_PATH"
    );
  } else if (trends.length === 1 && trends[0]?.role === "current") {
    set(
      "historical_trends",
      SECTION_STATUS.COMPARABLE_HISTORY_REQUIRED,
      "Baseline-only trends array (current role)",
      "AUTHORIZE_NEXT_MONITORING_PERIOD"
    );
  } else {
    set(
      "historical_trends",
      SECTION_STATUS.COMPARABLE_HISTORY_REQUIRED,
      "No multi-period trends in payload",
      "AUTHORIZE_NEXT_MONITORING_PERIOD"
    );
  }

  // Brand portfolio
  const bppStatus = String(bpp?.status || "");
  const bppAwaiting =
    !bppReady &&
    (/PENDING|AWAITING|PARTIAL|NOT_CONFIGURED|METHODOLOGY|ASSURANCE|EXCEPTION_SUPPRESSED/i.test(
      bppStatus
    ) ||
      /being established|in progress|not yet available|under review/i.test(
        String(bpp?.statusLabel || bpp?.headline || "")
      ));
  if (bppReady) {
    set("brand_portfolio_position", SECTION_STATUS.POPULATED_AND_CERTIFIED, "BPP READY");
  } else if (!bpp || bppAwaiting) {
    set(
      "brand_portfolio_position",
      SECTION_STATUS.BENCHMARK_REQUIRED,
      bpp
        ? bpp.status || bpp.statusLabel || "Portfolio measurement being established"
        : "brandPortfolioPosition absent — no customer-published BPP pack",
      bppStatus === "EXCEPTION_SUPPRESSED"
        ? "REVIEW_BPP_EXCEPTION_OR_BUILD_COHORT"
        : "BUILD_GOVERNED_PEER_COHORT"
    );
  } else {
    set(
      "brand_portfolio_position",
      SECTION_STATUS.POPULATED_NOT_CERTIFIED,
      `bpp status=${bpp.status || "unknown"}`,
      "COMPLETE_BPP_PUBLICATION_PACK"
    );
  }

  // Reality gap / attributes
  if (rg.measured === true || rg.measurementStatus === "MEASURED") {
    set("ai_reality_gaps", SECTION_STATUS.POPULATED_AND_CERTIFIED, rg.display || "MEASURED");
    set("property_reality_coverage", SECTION_STATUS.POPULATED_AND_CERTIFIED, rg.display);
    set(
      "ai_recognizes_misses",
      (rg.recognized?.length || 0) + (rg.gaps?.length || 0) > 0
        ? SECTION_STATUS.POPULATED_AND_CERTIFIED
        : SECTION_STATUS.POPULATED_NOT_CERTIFIED,
      `recognized=${rg.recognizedCount ?? rg.recognized?.length} gaps=${rg.gapCount ?? rg.gaps?.length}`
    );
  } else if (
    rg.measurementStatus === "NOT_MEASURED_NO_GOVERNED_ATTRIBUTES" ||
    rg.measured === false
  ) {
    if (context.airtableAttributesPresent || context.commercialProfilePresent) {
      set(
        "ai_reality_gaps",
        SECTION_STATUS.DATA_EXISTS_NOT_CONNECTED,
        "Airtable attribute/commercial facts exist but Reality Gap NOT_MEASURED",
        "MAP_AIRTABLE_ATTRS_TO_GOVERNED_SET"
      );
      set(
        "property_reality_coverage",
        SECTION_STATUS.DATA_EXISTS_NOT_CONNECTED,
        "Unmapped attributes",
        "MAP_AIRTABLE_ATTRS_TO_GOVERNED_SET"
      );
      set(
        "ai_recognizes_misses",
        SECTION_STATUS.DATA_EXISTS_NOT_CONNECTED,
        "Cannot list recognizes/misses without governed attributes"
      );
    } else {
      set(
        "ai_reality_gaps",
        SECTION_STATUS.RESEARCH_REQUIRED,
        rg.display || "NOT_MEASURED",
        "GOVERN_PROPERTY_ATTRIBUTES"
      );
      set(
        "property_reality_coverage",
        SECTION_STATUS.RESEARCH_REQUIRED,
        "No governed attributes",
        "GOVERN_PROPERTY_ATTRIBUTES"
      );
      set("ai_recognizes_misses", SECTION_STATUS.RESEARCH_REQUIRED, "No attribute set");
    }
  } else if (hasMeaningful(rg.gaps) || hasMeaningful(rg.recognized)) {
    // Legacy lists without MEASURED certification — treat as unconnected to governed contract
    const legacyStatus =
      context.airtableAttributesPresent || context.commercialProfilePresent
        ? SECTION_STATUS.DATA_EXISTS_NOT_CONNECTED
        : SECTION_STATUS.POPULATED_NOT_CERTIFIED;
    set(
      "ai_reality_gaps",
      legacyStatus,
      "legacy realityGap lists without measured flag — recertify under governed attribute set",
      "RECERTIFY_REALITY_GAP_UNDER_GOVERNED_ATTRS"
    );
    set("property_reality_coverage", legacyStatus, rg.display, "RECERTIFY_REALITY_GAP_UNDER_GOVERNED_ATTRS");
    set("ai_recognizes_misses", legacyStatus, null, "RECERTIFY_REALITY_GAP_UNDER_GOVERNED_ATTRS");
  } else if (context.airtableAttributesPresent || context.commercialProfilePresent) {
    // Missing realityGap object but Airtable facts exist → connect, don't treat as hard technical fail
    set(
      "ai_reality_gaps",
      SECTION_STATUS.DATA_EXISTS_NOT_CONNECTED,
      "realityGap absent/empty; Airtable commercial/ADP attributes present",
      "MAP_AIRTABLE_ATTRS_TO_GOVERNED_SET"
    );
    set(
      "property_reality_coverage",
      SECTION_STATUS.DATA_EXISTS_NOT_CONNECTED,
      "Unmapped attributes",
      "MAP_AIRTABLE_ATTRS_TO_GOVERNED_SET"
    );
    set(
      "ai_recognizes_misses",
      SECTION_STATUS.DATA_EXISTS_NOT_CONNECTED,
      "Cannot list recognizes/misses without governed attributes"
    );
  } else {
    set(
      "ai_reality_gaps",
      SECTION_STATUS.RESEARCH_REQUIRED,
      "realityGap missing and no Airtable attribute facts",
      "GOVERN_PROPERTY_ATTRIBUTES"
    );
    set(
      "property_reality_coverage",
      SECTION_STATUS.RESEARCH_REQUIRED,
      null,
      "GOVERN_PROPERTY_ATTRIBUTES"
    );
    set("ai_recognizes_misses", SECTION_STATUS.RESEARCH_REQUIRED, null);
  }

  // Competitive
  const marketOk = context.marketRegistryPresent !== false;
  const compCert = context.competitiveCertified !== false;
  if (!marketOk || context.competitiveCertified === false) {
    set(
      "ai_competitive_set",
      SECTION_STATUS.RESEARCH_REQUIRED,
      "Market registry / competitive certification missing",
      "BUILD_GOVERNED_MARKET_REGISTRY"
    );
    set(
      "competitive_displacement",
      SECTION_STATUS.RESEARCH_REQUIRED,
      "Cannot certify displacement without market registry",
      "BUILD_GOVERNED_MARKET_REGISTRY"
    );
  } else if (!p.competitiveSet || typeof p.competitiveSet !== "object") {
    // Missing parent object is a hard contract failure (SPA Property Snapshot binds
    // topObservedAlternative off competitiveSet — undefined parent previously threw).
    set(
      "ai_competitive_set",
      SECTION_STATUS.BLOCKED_TECHNICAL,
      "competitiveSet parent object missing — cannot bind topObservedAlternative"
    );
    set(
      "competitive_displacement",
      hasMeaningful(p.lostDemand?.displacement)
        ? SECTION_STATUS.POPULATED_AND_CERTIFIED
        : SECTION_STATUS.BLOCKED_TECHNICAL,
      "competitiveSet parent missing"
    );
  } else if (hasMeaningful(p.competitiveSet?.observed) || hasMeaningful(p.lostDemand?.displacement)) {
    set(
      "ai_competitive_set",
      SECTION_STATUS.POPULATED_AND_CERTIFIED,
      `observed=${p.competitiveSet?.observedCount ?? p.competitiveSet?.observed?.length}`
    );
    // Zero displacement is a valid measured outcome when competitive set was evaluated
    set(
      "competitive_displacement",
      SECTION_STATUS.POPULATED_AND_CERTIFIED,
      `displacement=${(p.lostDemand?.displacement || []).length}`
    );
  } else if (hasMeaningful(p.competitiveSet) || hasMeaningful(p.lostDemand)) {
    set(
      "ai_competitive_set",
      SECTION_STATUS.POPULATED_AND_CERTIFIED,
      `observed=${p.competitiveSet?.observedCount ?? 0}`
    );
    set(
      "competitive_displacement",
      SECTION_STATUS.POPULATED_AND_CERTIFIED,
      `displacement=${(p.lostDemand?.displacement || []).length}`
    );
  } else {
    set("ai_competitive_set", SECTION_STATUS.BLOCKED_TECHNICAL, "competitive set empty");
    set("competitive_displacement", SECTION_STATUS.BLOCKED_TECHNICAL, "displacement empty");
  }

  if (Array.isArray(p.actions) && p.actions.length > 0) {
    set("priority_actions", SECTION_STATUS.POPULATED_AND_CERTIFIED, `actions=${p.actions.length}`);
  } else if (hasMeaningful(er) || hasMeaningful(p.brief)) {
    // Executive present but empty actions — still a product gap, not silent success
    set(
      "priority_actions",
      SECTION_STATUS.POPULATED_NOT_CERTIFIED,
      "actions=0 — regenerate executive actions from evidence",
      "REBUILD_PRIORITY_ACTIONS_FROM_EVIDENCE"
    );
  } else {
    set("priority_actions", SECTION_STATUS.BLOCKED_TECHNICAL, "actions missing");
  }

  const citeOk =
    Number.isFinite(Number(evidence.citationRate)) ||
    hasMeaningful(evidence.topSources) ||
    hasMeaningful(evidence.ownedSourceShare);
  set(
    "citation_intelligence",
    citeOk ? SECTION_STATUS.POPULATED_AND_CERTIFIED : SECTION_STATUS.POPULATED_NOT_CERTIFIED,
    `citationRate=${evidence.citationRate ?? "n/a"} sources=${(evidence.topSources || []).length}`
  );

  set(
    "evidence_drawers",
    hasMeaningful(evidence) || hasMeaningful(p.lostDemand?.scenarios)
      ? SECTION_STATUS.POPULATED_AND_CERTIFIED
      : SECTION_STATUS.BLOCKED_TECHNICAL,
    null
  );

  const statuses = Object.values(sections).map((s) => s.status);
  const certifiedCount = statuses.filter((s) => s === SECTION_STATUS.POPULATED_AND_CERTIFIED).length;
  const materialIncomplete = blockers.filter(
    (b) =>
      b.status !== SECTION_STATUS.COMPARABLE_HISTORY_REQUIRED &&
      b.status !== SECTION_STATUS.BENCHMARK_REQUIRED &&
      b.status !== SECTION_STATUS.NOT_APPLICABLE
  );

  return {
    contractVersion: ADP_SECTION_COMPLETENESS_V1,
    propertyId: propertyId || p.property?.propertyId || null,
    sectionCount: ADP_PRODUCT_SECTIONS.length,
    certifiedCount,
    incompleteCount: ADP_PRODUCT_SECTIONS.length - certifiedCount,
    sections,
    blockers,
    materialIncompleteCount: materialIncomplete.length,
    publicationReadyCandidate:
      materialIncomplete.length === 0 &&
      certifiedCount >= ADP_PRODUCT_SECTIONS.length - 2 /* allow history+bpp deferred */,
    assessedAt: new Date().toISOString(),
  };
}

export function summarizeSectionPortfolio(hotelAssessments) {
  const bySection = {};
  for (const sec of ADP_PRODUCT_SECTIONS) {
    bySection[sec.id] = {
      label: sec.label,
      POPULATED_AND_CERTIFIED: 0,
      DATA_EXISTS_NOT_CONNECTED: 0,
      RESEARCH_REQUIRED: 0,
      COMPARABLE_HISTORY_REQUIRED: 0,
      BENCHMARK_REQUIRED: 0,
      NOT_APPLICABLE: 0,
      BLOCKED_TECHNICAL: 0,
      POPULATED_NOT_CERTIFIED: 0,
      BLOCKED_AUTHORIZATION: 0,
    };
  }
  for (const hotel of hotelAssessments || []) {
    for (const [id, row] of Object.entries(hotel.sections || {})) {
      if (!bySection[id]) continue;
      const st = row.status;
      if (bySection[id][st] != null) bySection[id][st] += 1;
    }
  }
  return bySection;
}
