/**
 * Active Brand Explorer profiles — governance upgrade audit (dry-run default).
 * Does not rebuild Explorer content or write Airtable.
 * @see docs/data-intelligence/active-brand-governance-upgrade-v1.md
 */
import { isBrandStatusActive } from "../brand-status-active.js";
import {
  buildIntelligenceProfileWorkflowPlan,
  countSources,
  isStrongerLiveGovernancePreserved,
} from "./intelligence-profile-workflow.js";
import { isPlatformReady } from "./intelligence-production-queue.js";
import { isStableGovernanceChangeClass } from "./profile-governance-publish-readiness.js";
import {
  buildPackageFromRecords,
  factStatusCounts,
} from "./stewardship-package.js";

export const UPGRADE_VERSION = "1";
export const REPORT_JSON_NAME = "active-brand-governance-upgrade.json";
export const REPORT_MD_NAME = "active-brand-governance-upgrade.md";

/**
 * v1 batch — Explorer-active legacy brands pending PI governance upgrade.
 * knownRecId is a hint only; resolution must match Airtable Brand Name unless unmatched.
 */
export const ACTIVE_BRAND_BATCH = [
  {
    key: "ascend-hotel-collection",
    displayName: "Ascend Hotel Collection",
    aliases: ["Ascend Hotel Collection", "Ascend"],
  },
  {
    key: "comfort-inn-suites",
    displayName: "Comfort Inn & Suites",
    aliases: ["Comfort Inn & Suites", "Comfort Inn and Suites"],
  },
  {
    key: "country-inn-suites-choice",
    displayName: "Country Inn & Suites by Choice",
    aliases: ["Country Inn & Suites by Choice", "Country Inn & Suites"],
  },
  {
    key: "curio-collection-hilton",
    displayName: "Curio Collection by Hilton",
    knownRecId: "receQkxgjlezsc1xg",
    aliases: ["Curio Collection by Hilton", "Curio Collection"],
  },
  {
    key: "everhome-suites",
    displayName: "Everhome Suites",
    aliases: ["Everhome Suites"],
  },
  {
    key: "kimpton-hotels",
    displayName: "Kimpton Hotels",
    knownRecId: "recCKuXCmGvxHPfb3",
    aliases: ["Kimpton Hotels", "Kimpton Hotel & Restaurants", "Kimpton"],
  },
  {
    key: "quality-inn",
    displayName: "Quality Inn",
    aliases: ["Quality Inn"],
  },
  {
    key: "radisson-blu-choice",
    displayName: "Radisson Blu by Choice",
    knownRecId: "recWPEvxBQxVVzSq3",
    aliases: ["Radisson Blu by Choice", "Radisson Blu (Choice)", "Radisson Blu"],
  },
  {
    key: "radisson-choice",
    displayName: "Radisson by Choice",
    aliases: ["Radisson by Choice", "Radisson (Choice)"],
  },
  {
    key: "radisson-individuals-choice",
    displayName: "Radisson Individuals by Choice",
    aliases: ["Radisson Individuals by Choice", "Radisson Individuals (Choice)"],
  },
  {
    key: "radisson-red-choice",
    displayName: "Radisson RED by Choice",
    aliases: ["Radisson RED by Choice", "Radisson RED (Choice)", "Radisson Red by Choice"],
  },
];

export const PROFILE_COMPLETENESS = {
  STRONG: "Strong Existing Profile",
  ADEQUATE: "Adequate Existing Profile",
  THIN: "Thin Existing Profile",
  MISSING: "Missing / Not Active",
  UNKNOWN: "Unable To Determine",
};

export const UPGRADE_PROFILE_STATUS = {
  PLATFORM_READY: "Platform Ready",
  ACTIVE_LEGACY: "Active Legacy Profile",
  GOVERNANCE_UPGRADE: "Active — Governance Upgrade Needed",
  EVIDENCE_PACKAGE: "Active — Evidence Package Needed",
  FACT_APPROVAL: "Active — Fact Approval Needed",
  EXTRACTION: "Extraction Needed",
  LIGHT_ENRICHMENT: "Active — Light Enrichment Needed",
  LEVEL2_CANDIDATE: "Level 2+ Candidate",
  FULL_PRODUCTION: "New / Full Production Needed",
  UNRESOLVED: "Unresolved Record",
};

export const NEXT_ACTION = {
  NO_ACTION: "No Action",
  MONITOR: "Monitor / optional enrichment",
  GOVERNANCE_PUBLISH: "Governance Publish Needed",
  SOURCE_PACKAGE: "Source Package Needed",
  FACT_APPROVAL: "Fact Approval Needed",
  EXTRACTION: "Extraction Needed",
  LIGHT_ENRICHMENT: "Light Enrichment Needed",
  FULL_PRODUCTION: "Full Production Needed",
  RESOLVE_RECORD: "Resolve Brand Record ID",
};

/** Ideal evidence package items (report-only in v1). */
export const EVIDENCE_PACKAGE_ITEMS = [
  "official brand page",
  "official development page",
  "official fact sheet / one-pager",
  "official brochure or pitch deck",
  "press kit / media kit",
  "recent openings / PR links",
  "image/logo source",
  "FDD (if useful and available)",
];

const SUBSTANTIVE_FIELD_CANDIDATES = [
  "Parent Company",
  "Hotel Chain Scale",
  "Brand Chain Scale",
  "Chain Scale",
  "Brand Positioning",
  "Brand Tagline",
  "Brand Customer Promise",
  "Brand Architecture",
  "Brand Model",
  "Service Model",
  "Region Offered",
  "Brand Description",
  "Typical Use Case",
];

const LOGO_FIELD_CANDIDATES = ["Logo", "Brand Logo", "Logo Image", "Brand Logo Image"];

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

export function normalizeBrandName(name) {
  return nz(name)
    .toLowerCase()
    .replace(/\s+/g, " ")
    .replace(/\s*&\s*/g, " & ")
    .replace(/[’']/g, "'");
}

function fieldPopulated(fields, key) {
  const v = fields?.[key];
  if (v == null || v === "") return false;
  if (Array.isArray(v)) return v.length > 0;
  if (typeof v === "object" && v.name) return Boolean(nz(v.name));
  return Boolean(nz(v));
}

function hasAttachment(fields, keys) {
  for (const key of keys) {
    const v = fields?.[key];
    if (Array.isArray(v) && v.length > 0) return true;
    if (typeof v === "string" && v.startsWith("http")) return true;
  }
  for (const [k, v] of Object.entries(fields || {})) {
    if (!/logo|image|icon|hero|photo/i.test(k)) continue;
    if (Array.isArray(v) && v.length > 0) return true;
  }
  return false;
}

export function assessExplorerActiveStatus(fields) {
  if (!fields) return { active: false, status: null, reason: "no_fields" };
  const brandStatus = nz(fields["Brand Status"] ?? fields["brand_status"]);
  const activeCheckbox = fields.Active;
  const activeByStatus = brandStatus ? isBrandStatusActive(brandStatus) : false;
  const activeByCheckbox =
    activeCheckbox === true ||
    String(activeCheckbox).toLowerCase() === "yes" ||
    activeCheckbox === 1;
  const active = activeByStatus || activeByCheckbox;
  return {
    active,
    brandStatus: brandStatus || null,
    activeCheckbox: activeCheckbox ?? null,
    reason: active ? "brand_status_or_active_flag" : "not_active_in_setup",
  };
}

export function assessExistingProfileCompleteness(fields, explorerActive) {
  if (!fields) return { category: PROFILE_COMPLETENESS.UNKNOWN, score: 0, signals: [] };
  if (!explorerActive) {
    return { category: PROFILE_COMPLETENESS.MISSING, score: 0, signals: ["explorer_not_active"] };
  }

  const signals = [];
  let score = 0;
  if (fieldPopulated(fields, "Brand Name")) {
    score += 1;
    signals.push("brand_name");
  }
  for (const key of SUBSTANTIVE_FIELD_CANDIDATES) {
    if (fieldPopulated(fields, key)) {
      score += 1;
      signals.push(key);
    }
  }
  const hasLogo = hasAttachment(fields, LOGO_FIELD_CANDIDATES);
  if (hasLogo) {
    score += 1;
    signals.push("logo");
  }
  const hasHeroSignal =
    fieldPopulated(fields, "Explorer Hero Verification") ||
    fieldPopulated(fields, "Explorer Hero Data Source");
  if (hasHeroSignal) {
    score += 1;
    signals.push("hero_signal");
  }

  let category = PROFILE_COMPLETENESS.THIN;
  if (score >= 8) category = PROFILE_COMPLETENESS.STRONG;
  else if (score >= 5) category = PROFILE_COMPLETENESS.ADEQUATE;

  return { category, score, signals, hasLogo, hasHeroSignal };
}

export function assessAssetSignals(fields) {
  if (!fields) {
    return {
      logo: "unable to determine",
      heroImage: "unable to determine",
      propertyDesignImages: "unable to determine",
      pdfBrochures: "unable to determine",
      recentOpeningsPrLinks: "unable to determine",
      developmentMaterials: "unable to determine",
      note: "Brand Setup row not loaded.",
    };
  }

  const logo = hasAttachment(fields, LOGO_FIELD_CANDIDATES) ? "present" : "missing_or_unknown";
  const heroImage =
    fieldPopulated(fields, "Explorer Hero Verification") ||
    fieldPopulated(fields, "Explorer Hero Data Source")
      ? "signal_present"
      : "missing_or_unknown";

  let imageFieldCount = 0;
  for (const [k, v] of Object.entries(fields)) {
    if (!/image|photo|gallery|hero/i.test(k)) continue;
    if (/logo|icon|favicon/i.test(k)) continue;
    if (Array.isArray(v) && v.length > 0) imageFieldCount += 1;
  }

  return {
    logo,
    heroImage,
    propertyDesignImages: imageFieldCount > 0 ? "likely_present" : "unable to determine",
    pdfBrochures:
      "unable to determine — recommend future asset-governance audit (presentation slots / attachments)",
    recentOpeningsPrLinks:
      "unable to determine — check footprint/openings tables in future audit",
    developmentMaterials:
      "unable to determine — use PI Source Library inventory for PDFs/brochures",
    note: "v1 reads Brand Setup - Brand Basics only; presentation slot tables not scanned.",
  };
}

export function recommendMissingEvidencePackage({ sources, assetSignals, profileCompleteness }) {
  const missing = [];
  const sourceTypes = new Set((sources || []).map((s) => nz(s.sourceType)));
  const hasType = (t) => sourceTypes.has(t);

  if (!hasType("Brand Page") && !hasType("Website Capture")) {
    missing.push("official brand page");
  }
  if (!hasType("Development Page") && !hasType("Website Capture")) {
    missing.push("official development page");
  }
  if (!hasType("Development Brochure") && !hasType("Owner Presentation")) {
    missing.push("official fact sheet / one-pager");
    missing.push("official brochure or pitch deck");
  }
  if (!hasType("Press Release") && !hasType("Website Capture")) {
    missing.push("press kit / media kit");
  }
  if (!hasType("FDD")) {
    missing.push("FDD (if useful and available)");
  }
  if (assetSignals.logo === "missing_or_unknown") missing.push("image/logo source");
  if (profileCompleteness.category === PROFILE_COMPLETENESS.THIN) {
    missing.push("recent openings / PR links");
  }
  return [...new Set(missing)];
}

export function resolveBrandCatalogEntry(batchEntry, catalogRows) {
  const aliases = new Set(
    [batchEntry.displayName, ...(batchEntry.aliases || [])].map(normalizeBrandName).filter(Boolean)
  );

  const matches = (catalogRows || []).filter((row) => {
    const name = normalizeBrandName(row.name);
    return aliases.has(name);
  });

  if (batchEntry.knownRecId) {
    const byId = (catalogRows || []).find((r) => r.id === batchEntry.knownRecId);
    if (byId) {
      const nameMatch = aliases.has(normalizeBrandName(byId.name));
      return {
        resolved: true,
        recordId: byId.id,
        airtableName: byId.name,
        fields: byId.fields,
        matchMethod: nameMatch ? "known_rec_id_and_name" : "known_rec_id_name_mismatch",
        nameMismatch: nameMatch ? null : `Expected aliases for "${batchEntry.displayName}"; found "${byId.name}"`,
        ambiguous: false,
      };
    }
  }

  if (matches.length === 1) {
    return {
      resolved: true,
      recordId: matches[0].id,
      airtableName: matches[0].name,
      fields: matches[0].fields,
      matchMethod: "name_match",
      nameMismatch: null,
      ambiguous: false,
    };
  }

  if (matches.length > 1) {
    return {
      resolved: false,
      recordId: null,
      airtableName: null,
      fields: null,
      matchMethod: "ambiguous_name",
      ambiguous: true,
      candidates: matches.map((m) => ({ id: m.id, name: m.name })),
      unresolvedReason: `Multiple Brand Basics rows match: ${matches.map((m) => m.name).join("; ")}`,
    };
  }

  return {
    resolved: false,
    recordId: null,
    airtableName: null,
    fields: null,
    matchMethod: "not_found",
    ambiguous: false,
    unresolvedReason: batchEntry.knownRecId
      ? `No Brand Basics row for knownRecId ${batchEntry.knownRecId} and no name match`
      : "No Brand Basics row matched display name or aliases",
  };
}

export function classifyRecommendedNextAction(profileStatus, plan, options = {}) {
  switch (profileStatus) {
    case UPGRADE_PROFILE_STATUS.PLATFORM_READY:
      return options.optionalEnrichmentRecommended
        ? NEXT_ACTION.MONITOR
        : NEXT_ACTION.NO_ACTION;
    case UPGRADE_PROFILE_STATUS.ACTIVE_LEGACY:
      return NEXT_ACTION.NO_ACTION;
    case UPGRADE_PROFILE_STATUS.GOVERNANCE_UPGRADE:
      return NEXT_ACTION.GOVERNANCE_PUBLISH;
    case UPGRADE_PROFILE_STATUS.EVIDENCE_PACKAGE:
      return NEXT_ACTION.SOURCE_PACKAGE;
    case UPGRADE_PROFILE_STATUS.FACT_APPROVAL:
      return NEXT_ACTION.FACT_APPROVAL;
    case UPGRADE_PROFILE_STATUS.EXTRACTION:
      return NEXT_ACTION.EXTRACTION;
    case UPGRADE_PROFILE_STATUS.LIGHT_ENRICHMENT:
      return NEXT_ACTION.LIGHT_ENRICHMENT;
    case UPGRADE_PROFILE_STATUS.FULL_PRODUCTION:
    case UPGRADE_PROFILE_STATUS.LEVEL2_CANDIDATE:
      return NEXT_ACTION.FULL_PRODUCTION;
    case UPGRADE_PROFILE_STATUS.UNRESOLVED:
      return NEXT_ACTION.RESOLVE_RECORD;
    default:
      return NEXT_ACTION.LIGHT_ENRICHMENT;
  }
}

export function classifyUpgradeProfileStatus({
  explorerActive,
  profileCompletenessCategory,
  plan,
  resolved,
}) {
  if (!resolved) return UPGRADE_PROFILE_STATUS.UNRESOLVED;
  if (!explorerActive) {
    return profileCompletenessCategory === PROFILE_COMPLETENESS.MISSING
      ? UPGRADE_PROFILE_STATUS.FULL_PRODUCTION
      : UPGRADE_PROFILE_STATUS.LEVEL2_CANDIDATE;
  }

  const stageId = plan?.currentStage?.stageId ?? 0;
  const changeClass = plan?.governance?.changeClass;
  const stableGovernance = isStableGovernanceChangeClass(changeClass);
  const governanceNorm = plan?.governance?.normalized;
  const hasLiveGovernance = Boolean(governanceNorm?.validationStatus && governanceNorm?.displayLabel);
  const downgradeProtected =
    changeClass === "downgrade" &&
    isStrongerLiveGovernancePreserved(
      changeClass,
      governanceNorm,
      plan?.blockers?.publishScope || []
    );

  if (stageId === 8 && (stableGovernance || downgradeProtected)) {
    if (profileCompletenessCategory === PROFILE_COMPLETENESS.THIN) {
      return UPGRADE_PROFILE_STATUS.LIGHT_ENRICHMENT;
    }
    return UPGRADE_PROFILE_STATUS.PLATFORM_READY;
  }

  if (hasLiveGovernance && stableGovernance) {
    return UPGRADE_PROFILE_STATUS.PLATFORM_READY;
  }

  if (stageId === 7 || (plan?.governance?.eligible && changeClass === "new")) {
    return UPGRADE_PROFILE_STATUS.GOVERNANCE_UPGRADE;
  }

  if (stageId <= 2 || (plan?.sourceCounts?.total || 0) === 0) {
    return UPGRADE_PROFILE_STATUS.EVIDENCE_PACKAGE;
  }

  if (stageId === 3) return UPGRADE_PROFILE_STATUS.EVIDENCE_PACKAGE;
  if (stageId === 4) return UPGRADE_PROFILE_STATUS.EXTRACTION;
  if (stageId === 5) return UPGRADE_PROFILE_STATUS.FACT_APPROVAL;
  if (stageId === 6) return UPGRADE_PROFILE_STATUS.GOVERNANCE_UPGRADE;

  if (profileCompletenessCategory === PROFILE_COMPLETENESS.THIN) {
    return UPGRADE_PROFILE_STATUS.LIGHT_ENRICHMENT;
  }

  return UPGRADE_PROFILE_STATUS.GOVERNANCE_UPGRADE;
}

export function computePriority(profileStatus, plan) {
  if (profileStatus === UPGRADE_PROFILE_STATUS.UNRESOLVED) return "P0";
  if (
    profileStatus === UPGRADE_PROFILE_STATUS.GOVERNANCE_UPGRADE ||
    profileStatus === UPGRADE_PROFILE_STATUS.EVIDENCE_PACKAGE
  ) {
    return "P1";
  }
  if (
    profileStatus === UPGRADE_PROFILE_STATUS.FACT_APPROVAL ||
    profileStatus === UPGRADE_PROFILE_STATUS.EXTRACTION
  ) {
    return "P2";
  }
  if (profileStatus === UPGRADE_PROFILE_STATUS.PLATFORM_READY) return "P4";
  return "P3";
}

export function inspectActiveBrand({
  batchEntry,
  resolution,
  sources,
  facts,
  readinessReport,
}) {
  if (!resolution.resolved) {
    return {
      key: batchEntry.key,
      brandName: batchEntry.displayName,
      recordId: null,
      resolved: false,
      unresolvedReason: resolution.unresolvedReason,
      ambiguous: resolution.ambiguous,
      candidates: resolution.candidates || [],
      profileStatus: UPGRADE_PROFILE_STATUS.UNRESOLVED,
      recommendedNextAction: NEXT_ACTION.RESOLVE_RECORD,
      priority: "P0",
      rebuildNeeded: false,
      governanceUpgradeNeeded: false,
      optionalEnrichmentRecommended: false,
    };
  }

  const targetProfile = {
    id: resolution.recordId,
    entityType: "brand",
    name: resolution.airtableName,
    fields: resolution.fields,
  };

  const explorer = assessExplorerActiveStatus(resolution.fields);
  const profileCompleteness = assessExistingProfileCompleteness(resolution.fields, explorer.active);
  const assetSignals = assessAssetSignals(resolution.fields);

  const plan = buildIntelligenceProfileWorkflowPlan({
    entityType: "brand",
    targetRecId: resolution.recordId,
    targetProfile,
    sources: sources || [],
    facts: facts || [],
    published: [],
    readinessReport,
  });

  const profileStatus = classifyUpgradeProfileStatus({
    explorerActive: explorer.active,
    profileCompletenessCategory: profileCompleteness.category,
    plan,
    resolved: true,
  });

  const optionalEnrichmentRecommended =
    profileStatus === UPGRADE_PROFILE_STATUS.LIGHT_ENRICHMENT ||
    profileStatus === UPGRADE_PROFILE_STATUS.PLATFORM_READY;

  const recommendedNextAction = classifyRecommendedNextAction(profileStatus, plan, {
    optionalEnrichmentRecommended,
  });
  const missingEvidence = recommendMissingEvidencePackage({
    sources: sources || [],
    assetSignals,
    profileCompleteness,
  });

  const sc = countSources(buildPackageFromRecords({
    sources: sources || [],
    facts: facts || [],
    published: [],
    entityType: "brand",
    targetRecId: resolution.recordId,
  }));

  const fc = plan.factCounts || factStatusCounts(facts || []);
  const approvedSources = sc.approvedExplorer || 0;
  const platformReady =
    isPlatformReady({
      resolved: true,
      currentStage: plan.currentStage,
      readyForPlatformUsage: plan.currentStage?.stageId === 8,
      changeClass: plan.governance?.changeClass,
    }) || profileStatus === UPGRADE_PROFILE_STATUS.PLATFORM_READY;

  return {
    key: batchEntry.key,
    brandName: resolution.airtableName || batchEntry.displayName,
    displayName: batchEntry.displayName,
    recordId: resolution.recordId,
    resolved: true,
    matchMethod: resolution.matchMethod,
    nameMismatch: resolution.nameMismatch,
    explorerActive: explorer.active,
    explorerActiveDetail: explorer,
    profileCompleteness: profileCompleteness.category,
    profileCompletenessDetail: profileCompleteness,
    assetSignals,
    piSourceCount: sc.total,
    approvedSourceCount: approvedSources,
    pendingFactCount: fc.pendingCandidates ?? fc.pending ?? 0,
    approvedFactCount: fc.approved || 0,
    totalFactCount: fc.total || 0,
    governance: {
      liveValidationStatus: plan.governance?.live?.validationStatus || null,
      liveUsagePermission: plan.governance?.live?.usagePermission || null,
      companyValidated: plan.governance?.live?.companyValidated ?? null,
      changeClass: plan.governance?.changeClass,
      eligible: plan.governance?.eligible,
      expectedChip: plan.governance?.normalized?.displayLabel || null,
      sourceBasis: plan.governance?.normalized?.sourceBasis || null,
      displaySubtitle: plan.governance?.normalized?.displaySubtitle || null,
    },
    workflowStage: plan.currentStage,
    profileStatus,
    recommendedNextAction,
    priority: computePriority(profileStatus, plan),
    rebuildNeeded: profileStatus === UPGRADE_PROFILE_STATUS.FULL_PRODUCTION,
    governanceUpgradeNeeded:
      profileStatus === UPGRADE_PROFILE_STATUS.GOVERNANCE_UPGRADE ||
      recommendedNextAction === NEXT_ACTION.GOVERNANCE_PUBLISH,
    optionalEnrichmentRecommended,
    missingEvidencePackage: missingEvidence,
    platformReady,
    nextCommands: plan.nextCommands,
    notes: [
      resolution.nameMismatch,
      profileStatus === UPGRADE_PROFILE_STATUS.PLATFORM_READY
        ? "Explorer Active — Platform Ready (governance stable)"
        : explorer.active
          ? "Explorer Active — Pending Governance Upgrade (legacy content preserved)"
          : "Brand Setup not Active/Live — treat as production candidate",
      plan.blockers?.labels?.length ? `PI blockers: ${plan.blockers.labels.join("; ")}` : null,
    ].filter(Boolean),
  };
}

export function buildActiveBrandGovernanceUpgradeReport(rows) {
  const resolvedRows = rows.filter((r) => r.resolved);
  const summary = {
    totalReviewed: rows.length,
    resolved: resolvedRows.length,
    unresolved: rows.filter((r) => !r.resolved).length,
    platformReady: rows.filter((r) => r.profileStatus === UPGRADE_PROFILE_STATUS.PLATFORM_READY).length,
    governanceUpgradeNeeded: rows.filter((r) => r.governanceUpgradeNeeded).length,
    sourcePackageNeeded: rows.filter(
      (r) => r.recommendedNextAction === NEXT_ACTION.SOURCE_PACKAGE
    ).length,
    extractionNeeded: rows.filter((r) => r.recommendedNextAction === NEXT_ACTION.EXTRACTION).length,
    factApprovalNeeded: rows.filter(
      (r) => r.recommendedNextAction === NEXT_ACTION.FACT_APPROVAL
    ).length,
    lightEnrichmentNeeded: rows.filter(
      (r) => r.profileStatus === UPGRADE_PROFILE_STATUS.LIGHT_ENRICHMENT
    ).length,
    fullProductionNeeded: rows.filter(
      (r) =>
        r.profileStatus === UPGRADE_PROFILE_STATUS.FULL_PRODUCTION ||
        r.profileStatus === UPGRADE_PROFILE_STATUS.LEVEL2_CANDIDATE
    ).length,
    explorerActive: resolvedRows.filter((r) => r.explorerActive).length,
  };

  const actionRank = {
    [NEXT_ACTION.RESOLVE_RECORD]: 0,
    [NEXT_ACTION.GOVERNANCE_PUBLISH]: 1,
    [NEXT_ACTION.SOURCE_PACKAGE]: 2,
    [NEXT_ACTION.EXTRACTION]: 3,
    [NEXT_ACTION.FACT_APPROVAL]: 4,
    [NEXT_ACTION.LIGHT_ENRICHMENT]: 5,
    [NEXT_ACTION.MONITOR]: 8,
    [NEXT_ACTION.FULL_PRODUCTION]: 6,
    [NEXT_ACTION.NO_ACTION]: 9,
  };

  const recommendedNextActions = [...rows]
    .filter((r) => r.recommendedNextAction !== NEXT_ACTION.NO_ACTION)
    .sort((a, b) => (actionRank[a.recommendedNextAction] ?? 99) - (actionRank[b.recommendedNextAction] ?? 99))
    .slice(0, 5)
    .map((r) => ({
      brandName: r.brandName,
      recordId: r.recordId,
      action: r.recommendedNextAction,
      profileStatus: r.profileStatus,
      command: r.nextCommands?.[0] || null,
    }));

  return {
    upgradeVersion: UPGRADE_VERSION,
    generatedAt: new Date().toISOString(),
    mode: "dry_run",
    airtableModified: false,
    summary,
    brands: rows,
    recommendedNextActions,
    doesNotDo: [
      "Rebuild Brand Explorer presentation content",
      "Overwrite populated Brand Setup fields",
      "Download or register PI sources",
      "Approve facts or publish governance",
      "Set Company Validated or Company Validation Date",
      "Downgrade Company Reviewed / Company Validated profiles",
    ],
  };
}

export function buildActiveBrandGovernanceUpgradeMarkdown(report) {
  const s = report.summary;
  const lines = [
    "# Active Brand Profile Governance Upgrade v1",
    "",
    `Generated: ${report.generatedAt}`,
    `Mode: **${report.mode}**`,
    `Airtable modified: **${report.airtableModified ? "yes" : "no"}**`,
    "",
    "## Executive summary",
    "",
    "| Metric | Count |",
    "|--------|------:|",
    `| Active brands reviewed | ${s.totalReviewed} |`,
    `| Resolved records | ${s.resolved} |`,
    `| Unresolved records | ${s.unresolved} |`,
    `| Explorer active (resolved) | ${s.explorerActive} |`,
    `| Platform-ready | ${s.platformReady} |`,
    `| Governance upgrade needed | ${s.governanceUpgradeNeeded} |`,
    `| Source package needed | ${s.sourcePackageNeeded} |`,
    `| Extraction needed | ${s.extractionNeeded} |`,
    `| Fact approval needed | ${s.factApprovalNeeded} |`,
    `| Light enrichment needed | ${s.lightEnrichmentNeeded} |`,
    `| Full production needed | ${s.fullProductionNeeded} |`,
    "",
    "## Recommended next 5 actions",
    "",
  ];

  if (!report.recommendedNextActions.length) {
    lines.push("_No urgent actions — monitor platform-ready brands._", "");
  } else {
    for (const item of report.recommendedNextActions) {
      lines.push(
        `1. **${item.brandName}** (\`${item.recordId || "unresolved"}\`) — ${item.action} · ${item.profileStatus}`
      );
      if (item.command) lines.push(`   - \`${item.command}\``);
    }
    lines.push("");
  }

  lines.push("## Brand inventory", "");
  lines.push(
    "| Brand | Record | Explorer active | Profile completeness | PI sources (appr/total) | Facts (appr/pend/total) | Status | Next action | Chip |",
    "|-------|--------|-----------------|----------------------|-------------------------|-------------------------|--------|-------------|------|"
  );

  for (const row of report.brands) {
    lines.push(
      `| ${row.brandName} | \`${row.recordId || "—"}\` | ${row.explorerActive ? "yes" : row.resolved ? "no" : "—"} | ${row.profileCompleteness || "—"} | ${row.approvedSourceCount ?? "—"}/${row.piSourceCount ?? "—"} | ${row.approvedFactCount ?? "—"}/${row.pendingFactCount ?? "—"}/${row.totalFactCount ?? "—"} | ${row.profileStatus} | ${row.recommendedNextAction} | ${row.governance?.expectedChip || "—"} |`
    );
  }

  lines.push("", "## Per-brand detail", "");

  for (const row of report.brands) {
    lines.push(`### ${row.brandName}`, "");
    if (!row.resolved) {
      lines.push(`- **Unresolved:** ${row.unresolvedReason}`);
      if (row.candidates?.length) {
        lines.push(`- Candidates: ${row.candidates.map((c) => `\`${c.id}\` ${c.name}`).join("; ")}`);
      }
      lines.push("");
      continue;
    }
    lines.push(
      `- Record: \`${row.recordId}\``,
      `- Explorer active: **${row.explorerActive ? "yes" : "no"}**`,
      `- Profile completeness: **${row.profileCompleteness}**`,
      `- PI sources: **${row.approvedSourceCount}/${row.piSourceCount}** approved`,
      `- Facts: **${row.approvedFactCount}** approved · **${row.pendingFactCount}** pending · **${row.totalFactCount}** total`,
      `- Governance: ${row.governance?.liveValidationStatus || "—"} · change class **${row.governance?.changeClass || "—"}**`,
      `- Trust chip: **${row.governance?.expectedChip || "—"}** · Source basis: **${row.governance?.sourceBasis || "—"}**`,
      `- Profile status: **${row.profileStatus}**`,
      `- Rebuild needed: **${row.rebuildNeeded ? "Yes" : "No"}**`,
      `- Governance upgrade needed: **${row.governanceUpgradeNeeded ? "Yes" : "No"}**`,
      `- Optional enrichment: **${row.optionalEnrichmentRecommended ? "Yes" : "No"}**`,
      `- Workflow stage: **${row.workflowStage?.stageId}** ${row.workflowStage?.stageLabel}`,
      `- Priority: **${row.priority}**`
    );
    if (row.missingEvidencePackage?.length) {
      lines.push(`- Missing evidence package: ${row.missingEvidencePackage.join("; ")}`);
    }
    if (row.assetSignals) {
      lines.push(
        `- Assets: logo=${row.assetSignals.logo}; hero=${row.assetSignals.heroImage}; images=${row.assetSignals.propertyDesignImages}; PDFs=${row.assetSignals.pdfBrochures}`
      );
    }
    if (row.notes?.length) {
      lines.push("- Notes:");
      for (const n of row.notes) lines.push(`  - ${n}`);
    }
    lines.push("");
  }

  lines.push("## Does not do", "");
  for (const item of report.doesNotDo) lines.push(`- ${item}`);
  lines.push("");

  return lines.join("\n");
}
