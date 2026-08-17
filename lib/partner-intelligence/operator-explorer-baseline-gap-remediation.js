/**
 * Operator Explorer baseline gap remediation — intentional suppress + content overlays.
 *
 * Default: dry-run only. Does not write Airtable.
 * Optional --apply writes local fixture overlay JSON for review (not live Setup).
 *
 * Goldens: Arbor Lodging + Hotel Equities.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  OPERATOR_QUALITY_BASELINE_OPERATORS,
  getOperatorQualityBaselineEntry,
} from "./operator-explorer-quality-baseline.js";
import {
  loadOperatorFixturePayload,
  mergeLiveAndFixturePrefill,
} from "./operator-explorer-fixture-payload.js";
import { loadLiveOperatorPrefill } from "./operator-explorer-tab-factory-audit.js";
import { evaluateOperatorTabFactoryFromPayload } from "./operator-explorer-tab-factory-evaluate.js";

export const BASELINE_GAP_REMEDIATION_VERSION = "operator-baseline-gap-remediation-v1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");
export const OVERLAY_DIR = path.join(ROOT, "fixtures", "operator-explorer-baseline-overlays");

/**
 * Fields that may be intentionally empty on CALA division profiles without failing Tab Factory.
 * Values are human reasons (not written to Airtable).
 */
export const INTENTIONAL_SUPPRESS_BY_SLUG = Object.freeze({
  "arbor-lodging-cala": Object.freeze({
    "op.snapshot.parentCompany":
      "CALA division profile — enterprise Arbor platform labeled in narrative, not a separate parentCompany scalar",
    "op.markets.activeCountries":
      "Honest zero CALA managed footprint — no active countries under management; team experience markets carry the pattern",
    "op.markets.activeMarkets":
      "Honest zero CALA managed footprint — no active markets under management; Mexico City hub + team experience labeled",
    "op.proof.ownerReferences": "Owner references not published on Explorer (confidential)",
    "op.proof.lenderReferences": "Lender references not published on Explorer (confidential)",
  }),
  "hotel-equities-cala": Object.freeze({
    "op.snapshot.parentCompany":
      "CALA division profile — Hotel Equities enterprise platform labeled in leadership/org copy",
    "op.proof.ownerReferences": "Owner references not published on Explorer (confidential)",
    "op.proof.lenderReferences": "Lender references not published on Explorer (confidential)",
  }),
});

/**
 * Source-backed / fixture-aligned fills for remaining golden gaps.
 * Keys = prefill keys.
 */
export const BASELINE_GAP_CONTENT_BY_SLUG = Object.freeze({
  "arbor-lodging-cala": Object.freeze({
    industryRecognition:
      "Public company materials and industry coverage highlight Arbor Lodging's vertically integrated owner-operator platform and Mexico City CALA hub positioning for third-party growth.",
    notableAchievements:
      "Approximately $1.75B transaction volume cited in leadership bios; approved operator relationships with Marriott, Hilton, Hyatt, and IHG for LATAM & Caribbean growth pursuit; Mexico City office integrated with U.S. operations for 3+ years.",
    revenueManagementCapability: "Strong",
    preOpeningSupportCapability: "Strong",
    conversionReflagExperience: "Proven",
    fbCapabilityLevel: "Proven",
    offeredServices:
      "Third-party hotel management; pre-opening and transition support; revenue management; sales & marketing; owner reporting; brand compliance and PIP execution support",
    priorityMarkets: "Mexico (Mexico City hub, Baja and coastal leisure corridors); broader CALA relationship-led expansion",
    targetGrowthMarkets: "Mexico-first CALA; Caribbean and Latin America gateway and leisure markets where brand-approved third-party mandates fit Arbor's model",
    regions: "Caribbean, Latin America",
    markets_regional_portfolio_json: [
      {
        title: "Current CALA Managed Footprint",
        description:
          "Arbor Lodging does not currently manage hotels in CALA. This profile reflects team experience and growth positioning from the Mexico City hub—not an active managed portfolio in the region.",
      },
      {
        title: "Mexico City Regional Hub",
        description:
          "Fully operational Mexico City office with BD, operations, commercial, finance, and communications integrated to the U.S. platform.",
      },
    ],
    ownerReportingLevel: "Structured owner reporting",
    ownerReportingCadence: "Monthly owner operating reviews; quarterly owner strategy sessions",
    ownerResponseTime: "Business-day responsive owner cadence (confirm in executed agreement)",
    reportTypes: "Owner P&L, forecast, labor, commercial pacing, open issues, quarterly strategy pack",
    ownerPortalFeatures: "Structured owner review packs and action tracking (portal features per agreement)",
    operatingCollaborationMode:
      "Regional owner accountability with enterprise platform depth; milestone-based transitions",
    leadership_executives_json: [
      {
        name: "Vamsi Bonthala",
        title: "Chief Executive Officer, Arbor Lodging Partners",
        bio: "Co-founded Arbor Lodging in 2006 and has overseen approximately $1.75B of transaction volume across the vertically integrated platform.",
      },
      {
        name: "Sheenal Patel",
        title: "Co-Founder and CEO, Arbor Lodging Management",
        bio: "Leads management-company strategy, property management, capital improvements, and human capital for the operating platform.",
      },
      {
        name: "Jorge Calderon",
        title: "Senior Vice President of Operations",
        bio: "Mexico operations leader with experience across multiple CALA destinations supporting openings, quality, and efficiency.",
      },
    ],
    owner_diligence_json: [
      {
        question: "How do you summarize operating experience for CALA owners?",
        answer:
          "Vertically integrated owner-operator platform with Mexico City CALA hub, brand approvals for Marriott, Hilton, Hyatt, and IHG, and diligence packs spanning overview, leadership, case studies, and NDA-gated references.",
      },
      {
        question: "What reporting cadence should owners expect?",
        answer:
          "Customized monthly owner reports plus quarterly strategy sessions; pack format finalized in the management agreement.",
      },
    ],
    infra_technology_maturity_level: "Structured",
    infra_services_offered_json: [
      {
        title: "Systems Cutover Support",
        description: "IT and openings readiness for takeovers and pre-openings coordinated with regional leadership.",
      },
      {
        title: "Owner Reporting Stack",
        description: "Operating and financial reporting rhythm supported by shared services and regional finance.",
      },
    ],
    infra_data_governance_json: [
      {
        title: "Owner Data Discipline",
        description: "Financial and operating data handled through structured owner reviews and agreement-defined access.",
      },
    ],
    infra_analytics_support_json: [
      {
        title: "Commercial Pacing Analytics",
        description: "Comp-set and pacing context in owner commercial reviews for assets under management.",
      },
    ],
    bestFitOwnerTypes:
      "Developers, family offices, institutional holders, and owners seeking hands-on third-party management with regional accountability",
    bestFitGeographies: "Mexico-first CALA; Baja and coastal leisure; major urban gateways",
    lessIdealSituations:
      "Passive owner mandates without governance appetite; unfunded PIP/brand obligations; ultra-luxury standalone outside core strengths; exploratory inquiries without decision authority",
    minPropertySize: 80,
    maxPropertySize: 350,
  }),
  "hotel-equities-cala": Object.freeze({
    companyHistory:
      "Hotel Equities built a multi-decade U.S. operating platform and expanded a dedicated CALA division with in-market hubs in Miami, Mexico City, Cancún, and the Dominican Republic. The CALA profile focuses on regional hotel management for resort, all-inclusive, urban, and lifestyle assets—backed by the Hotel Equities enterprise platform for openings, IT, revenue, shared services, and owner reporting.",
    missionStatement:
      "Hotel Equities (CALA) delivers in-market hotel management for Caribbean and Latin America owners—combining regional operating hubs with the Hotel Equities enterprise platform for brand stewardship, commercial discipline, and owner reporting.",
    differentiators:
      "In-market CALA leadership (not remote U.S.-only), resort and all-inclusive operating depth, urban/lifestyle capability, and enterprise platform backing for owner reporting and brand compliance.",
    managementPhilosophy:
      "Regional accountability with enterprise standards—owner-aligned reporting, brand stewardship, and commercial discipline across CALA hubs.",
    industryRecognition:
      "Hotel Equities public materials and CALA division positioning emphasize multi-decade operating pedigree, resort and urban capability, and regional leadership anchored in Miami, Mexico City, Cancún, and Dominican Republic markets.",
    notableAchievements:
      "CALA operating hubs and shared services supporting resort, all-inclusive, and urban/lifestyle assets; enterprise platform backing openings, IT, revenue, and owner reporting.",
    brandFamiliesOperated:
      "Major branded and independent portfolios across Hotel Equities enterprise and CALA assignments (confirm live flag list by market)",
    priorityMarkets: "Caribbean and Latin America resort and urban gateways; Mexico and Dominican Republic operating focus",
    targetGrowthMarkets: "CALA destinations where Hotel Equities regional hubs and brand relationships create owner value",
    regions: "Caribbean, Latin America",
    markets_regional_portfolio_json: [
      {
        title: "CALA Operating Hubs",
        description:
          "Leadership and support anchored in Miami, Mexico City, Cancún, and Dominican Republic markets—not a remote U.S.-only model.",
      },
      {
        title: "Resort & Urban Verticals",
        description:
          "Operating model covering sprawling resort/all-inclusive assets and gateway lifestyle hotels with brand compliance and commercial mix discipline.",
      },
    ],
    ownerReportingLevel: "Structured owner reporting",
    ownerReportingCadence: "Monthly asset owner reporting with regional owner escalation paths",
    ownerResponseTime: "Regional leadership escalation with enterprise owner support",
    reportTypes: "Owner asset performance, forecasting, owner packs, commercial pacing",
    ownerPortalFeatures: "Owner reporting packs via shared services and ops finance support",
    operatingCollaborationMode:
      "In-market CALA leadership with Hotel Equities enterprise platform backing for owner governance",
    leadership_executives_json: [
      {
        name: "Juan Corvinos",
        title: "CALA Division Leadership",
        bio: "In-market executive oversight for Caribbean and Latin America growth, owner alignment, and coordination with the Hotel Equities enterprise platform.",
      },
      {
        name: "Michael Register",
        title: "CALA Development & Owner Growth",
        bio: "Sourcing, management agreements, owner introductions, and market-entry support across CALA destinations.",
      },
      {
        name: "Marilia Pergola",
        title: "CALA Operations",
        bio: "Portfolio operating standards, brand stewardship, pre-opening execution, and owner satisfaction across resort and urban assets.",
      },
    ],
    bestFitOwnerTypes:
      "Owners of resort, all-inclusive, urban, and lifestyle hotels seeking regional CALA management with enterprise depth",
    bestFitGeographies: "Caribbean and Latin America; Mexico and Dominican Republic hubs",
    lessIdealSituations:
      "Mandates requiring no regional presence; unfunded openings; assets outside HE CALA resort/urban operating strengths",
    minPropertySize: 100,
    maxPropertySize: 500,
    bf_fit_criteria_json: [
      {
        fitCriteria: "Market Fit",
        operatorLooksFor: "CALA destinations where Miami/Mexico City/Cancún/DR hubs can staff and support the asset.",
        importance: "High",
      },
      {
        fitCriteria: "Asset Type Fit",
        operatorLooksFor: "Resort, all-inclusive, urban, and lifestyle hotels aligned to HE CALA verticals.",
        importance: "High",
      },
      {
        fitCriteria: "Ownership Fit",
        operatorLooksFor: "Owners wanting in-market accountability with enterprise reporting and brand stewardship.",
        importance: "High",
      },
      {
        fitCriteria: "Brand / Flag Fit",
        operatorLooksFor: "Branded or independent assets where HE platform and regional teams add compliance and commercial value.",
        importance: "High",
      },
    ],
    bf_best_fit_project_types_json: [
      {
        fitLevel: "Best Fit",
        projectType: "Resort / All-Inclusive",
        ownerContext: "Sprawling resort assets needing labor, F&B, and seasonal operating discipline.",
      },
      {
        fitLevel: "Best Fit",
        projectType: "Urban / Lifestyle",
        ownerContext: "Gateway hotels needing commercial mix, brand compliance, and conversion support.",
      },
      {
        fitLevel: "Selective Fit",
        projectType: "Pre-Opening / Transition",
        ownerContext: "When openings IT and commercial ramp can be staffed from CALA hubs.",
      },
    ],
    bf_preferred_deal_profile_json: [
      {
        label: "Preferred Owner Type",
        value: "Institutional and private owners seeking regional CALA operators with enterprise depth.",
      },
      {
        label: "Ideal Situation",
        value: "Clear brand path, funded opening/PIP plan, and owner governance appetite.",
      },
    ],
    bf_evaluation_path_json: [
      { title: "Initial Screen", description: "Market, asset type, keys, brand status, ownership profile." },
      { title: "CALA Hub Fit", description: "Confirm staffing path from Miami/Mexico City/Cancún/DR hubs." },
      { title: "Owner Discussion", description: "Align reporting, fees, brand path, and transition milestones." },
    ],
    bf_red_flags_json: [
      {
        title: "No Regional Resourcing Path",
        description: "Asset cannot be supported from HE CALA hubs or enterprise platform.",
      },
      {
        title: "Unfunded Openings / PIP",
        description: "Opening or brand obligations lack realistic capital and schedule.",
      },
    ],
    owner_diligence_json: [
      {
        question: "How is CALA leadership structured?",
        answer:
          "In-market executive oversight with regional hubs and Hotel Equities enterprise platform support for openings, IT, revenue, and shared services.",
      },
      {
        question: "What asset types are strongest?",
        answer:
          "Resort and all-inclusive verticals plus urban/lifestyle hotels with brand compliance and commercial complexity.",
      },
    ],
    case_studies_json: [
      {
        title: "CALA Resort Operating Depth",
        description:
          "Regional resort and all-inclusive operating model spanning labor, F&B intensity, seasonality, and owner reporting—supported by HE CALA hubs.",
      },
      {
        title: "Urban & Lifestyle Brand Stewardship",
        description:
          "Gateway and lifestyle hotels with commercial mix discipline, brand compliance, and conversion/repositioning support from in-market teams.",
      },
    ],
  }),
});

function nz(v) {
  return v == null ? "" : String(v).trim();
}

/**
 * Apply intentional suppress + content overlay onto prefill for evaluation.
 */
export function applyBaselineGapOverlay(prefill, slug, { applyContent = true } = {}) {
  const out = { ...(prefill || {}) };
  const suppress = INTENTIONAL_SUPPRESS_BY_SLUG[slug] || {};
  const content = applyContent ? BASELINE_GAP_CONTENT_BY_SLUG[slug] || {} : {};

  for (const [k, v] of Object.entries(content)) {
    const cur = out[k];
    const empty =
      cur == null ||
      cur === "" ||
      (Array.isArray(cur) && cur.length === 0) ||
      (typeof cur === "string" && wordsThin(cur)) ||
      // Force brand family scalar when still a non-useful placeholder
      (k === "brandFamiliesOperated" && !/\b(marriott|hilton|hyatt|ihg|choice|brand)\b/i.test(String(cur || "")));
    if (empty) out[k] = v;
  }

  out.__intentionalSuppressFieldKeys = Object.keys(suppress);
  out.__intentionalSuppressReasons = suppress;
  return out;
}

function wordsThin(s) {
  return String(s || "")
    .trim()
    .split(/\s+/)
    .filter(Boolean).length < 6;
}

export function listIntentionalSuppressFieldKeys(slug) {
  return Object.keys(INTENTIONAL_SUPPRESS_BY_SLUG[slug] || {});
}

/**
 * @param {{
 *   operators?: string[],
 *   source?: 'fixtures'|'live'|'merged',
 *   apply?: boolean,
 *   approveRemediation?: boolean,
 *   fixtureOverlayOnly?: boolean
 * }} opts
 */
export async function runOperatorBaselineGapRemediation(opts = {}) {
  const source = opts.source || "merged";
  const operators =
    opts.operators?.length > 0
      ? opts.operators
      : OPERATOR_QUALITY_BASELINE_OPERATORS.map((o) => o.slug);
  const apply = opts.apply === true;
  const approve = opts.approveRemediation === true;
  const fixtureOverlayOnly = opts.fixtureOverlayOnly !== false;

  if (apply && !approve) {
    throw new Error("Apply requires --approve-operator-baseline-gap-remediation");
  }
  if (apply && !fixtureOverlayOnly) {
    throw new Error(
      "Airtable apply is not enabled in v1. Use --confirm-fixture-overlay-only (default) for local overlay writes."
    );
  }

  const results = [];
  for (const id of operators) {
    const entry = getOperatorQualityBaselineEntry(id);
    if (!entry) throw new Error(`Not a quality baseline operator: ${id}`);

    const fixture = loadOperatorFixturePayload(entry.slug);
    let prefill = { ...fixture.prefill };
    if (source === "live" || source === "merged") {
      const live = await loadLiveOperatorPrefill(entry.recordId);
      prefill =
        source === "live"
          ? { ...(live.prefill || {}) }
          : mergeLiveAndFixturePrefill(live.prefill || {}, prefill);
    }

    const before = evaluateOperatorTabFactoryFromPayload({
      operatorSlug: entry.slug,
      operatorName: entry.companyName,
      recordId: entry.recordId,
      prefill,
      source,
      fixtureFiles: fixture.fixtureFiles,
    });

    const overlaid = applyBaselineGapOverlay(prefill, entry.slug, { applyContent: true });
    const after = evaluateOperatorTabFactoryFromPayload({
      operatorSlug: entry.slug,
      operatorName: entry.companyName,
      recordId: entry.recordId,
      prefill: overlaid,
      source: `${source}+overlay`,
      fixtureFiles: fixture.fixtureFiles,
    });

    const contentKeys = Object.keys(BASELINE_GAP_CONTENT_BY_SLUG[entry.slug] || {});
    const suppressKeys = listIntentionalSuppressFieldKeys(entry.slug);

    let overlayPath = null;
    if (apply) {
      fs.mkdirSync(OVERLAY_DIR, { recursive: true });
      overlayPath = path.join(OVERLAY_DIR, `${entry.slug}.json`);
      const payload = {
        _meta: {
          version: BASELINE_GAP_REMEDIATION_VERSION,
          slug: entry.slug,
          recordId: entry.recordId,
          companyName: entry.companyName,
          generatedAt: new Date().toISOString(),
          note: "Local evaluation overlay only — not an Airtable write. Review before any Setup apply.",
          intentionalSuppress: INTENTIONAL_SUPPRESS_BY_SLUG[entry.slug] || {},
        },
        prefillOverlay: BASELINE_GAP_CONTENT_BY_SLUG[entry.slug] || {},
      };
      fs.writeFileSync(overlayPath, JSON.stringify(payload, null, 2));
    }

    results.push({
      operatorSlug: entry.slug,
      recordId: entry.recordId,
      companyName: entry.companyName,
      before: {
        auditPass: before.auditPass,
        fieldAuditPass: before.fieldAuditPass,
        failFindings: before.failFindings,
        sectionPatternPass: before.sectionPatternParity?.pass === true,
        failingSections: before.sectionPatternParity?.failingSectionIds || [],
      },
      after: {
        auditPass: after.auditPass,
        fieldAuditPass: after.fieldAuditPass,
        failFindings: after.failFindings,
        sectionPatternPass: after.sectionPatternParity?.pass === true,
        failingSections: after.sectionPatternParity?.failingSectionIds || [],
        remainingFails: (after.failFindingDetails || []).map((f) => f.fieldKey),
      },
      contentKeysProposed: contentKeys,
      intentionalSuppressKeys: suppressKeys,
      overlayPath: overlayPath ? path.relative(ROOT, overlayPath).replace(/\\/g, "/") : null,
      deltaFailFindings: before.failFindings - after.failFindings,
    });
  }

  return {
    version: BASELINE_GAP_REMEDIATION_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: !apply,
    source,
    applyPerformed: apply,
    writeKind: apply ? "fixture_overlay_json" : "none",
    results,
    summary: {
      operators: results.length,
      totalFailBefore: results.reduce((n, r) => n + r.before.failFindings, 0),
      totalFailAfter: results.reduce((n, r) => n + r.after.failFindings, 0),
      allFieldAuditPassAfter: results.every((r) => r.after.fieldAuditPass),
      allSectionPatternPassAfter: results.every((r) => r.after.sectionPatternPass),
      allAuditPassAfter: results.every((r) => r.after.auditPass),
    },
  };
}

export function writeBaselineGapRemediationReports(report, reportsDir = path.join(ROOT, "reports")) {
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, "operator-explorer-baseline-gap-remediation.json");
  const mdPath = path.join(reportsDir, "operator-explorer-baseline-gap-remediation.md");
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));

  const md = [
    "# Operator Explorer Baseline Gap Remediation",
    "",
    `Version: \`${report.version}\``,
    `Generated: ${report.generatedAt}`,
    `dryRun: **${report.dryRun}** · source: **${report.source}** · writeKind: **${report.writeKind}**`,
    "",
    "## Summary",
    "",
    `- Fail findings before → after: **${report.summary.totalFailBefore} → ${report.summary.totalFailAfter}**`,
    `- fieldAuditPass after: **${report.summary.allFieldAuditPassAfter}**`,
    `- sectionPatternPass after: **${report.summary.allSectionPatternPassAfter}**`,
    `- full auditPass after: **${report.summary.allAuditPassAfter}**`,
    "",
  ];
  for (const r of report.results) {
    md.push(
      `## ${r.companyName}`,
      "",
      `- Before: fails=${r.before.failFindings} fieldPass=${r.before.fieldAuditPass} sectionPass=${r.before.sectionPatternPass}`,
      `- After: fails=${r.after.failFindings} fieldPass=${r.after.fieldAuditPass} sectionPass=${r.after.sectionPatternPass}`,
      `- Delta fails: **${r.deltaFailFindings}**`,
      `- Intentional suppress: ${r.intentionalSuppressKeys.join(", ") || "(none)"}`,
      `- Overlay path: ${r.overlayPath || "(dry-run)"}`,
      ""
    );
    if (r.after.remainingFails?.length) {
      md.push("Remaining fails:", ...r.after.remainingFails.map((k) => `- \`${k}\``), "");
    }
  }
  fs.writeFileSync(mdPath, md.join("\n"));
  return { jsonPath, mdPath };
}
