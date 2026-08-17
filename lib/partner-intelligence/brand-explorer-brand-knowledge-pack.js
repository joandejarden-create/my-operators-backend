/**
 * Brand Explorer v36B — Brand Knowledge Pack (read-only internal object).
 */
import { getActiveProfileBrandConfig } from "./brand-explorer-active-profile-brand-config.js";
import { auditPresentationRowsAgainstContract } from "./brand-explorer-full-tab-content-contract.js";
import { extendAssetPackWithRenderReadiness } from "./brand-explorer-render-readiness-contract.js";
import { evaluateExternalOwnerReadinessRule } from "./brand-explorer-external-owner-readiness-rules.js";
import { ATELIER_PROOF_FALLBACK_HEADS } from "./brand-explorer-active-profile-factory-rules.js";

export const KNOWLEDGE_PACK_VERSION = "v36B";

const TAB_PREFIXES = [
  { tab: "Overview", prefixes: ["overview.", "hero."] },
  { tab: "Value to Owners", prefixes: ["valueOwners."] },
  { tab: "Operating Model", prefixes: ["operations."] },
  { tab: "Owner Considerations", prefixes: ["standards."] },
  { tab: "Commercial Engine", prefixes: ["commercial."] },
  { tab: "Economics & Obligations", prefixes: ["economics."] },
  { tab: "Loyalty Program", prefixes: ["loyalty."] },
  { tab: "Footprint & Growth", prefixes: ["footprint."] },
  { tab: "Brand Materials", prefixes: ["materials."] },
  { tab: "Dealality Insight", prefixes: ["insight.", "dealalityInsight."] },
];

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function tabCoverageFromRows(presentationRows = []) {
  const visible = presentationRows.filter((r) => r.visible !== false);
  return TAB_PREFIXES.map(({ tab, prefixes }) => {
    const rows = visible.filter((r) => prefixes.some((p) => r.slotKey.startsWith(p)));
    return {
      tab,
      rowCount: rows.length,
      populated: rows.filter((r) => nz(r.title) || nz(r.body)).length,
      hasImageSlots: rows.filter((r) => nz(r.imageUrl)).length,
      status: rows.length === 0 ? "empty" : rows.some((r) => nz(r.title) || nz(r.body)) ? "populated" : "shell",
    };
  });
}

function buildContextFields(brandConfig) {
  const model = brandConfig?.brandModelType || brandConfig?.copyGovernanceMode || "unknown";
  return {
    brandModelType: model,
    copyGovernanceMode: brandConfig?.copyGovernanceMode || model,
    positioning:
      brandConfig?.overviewScenarioCopy?.["overview.scenario.1"]?.body?.slice(0, 200) ||
      "See scenario copy packages and approved sources",
    ownerFit: "Independent/lifestyle owner fit per brand model — affiliation without franchise flag where applicable",
    assetFit: `Gallery min ${brandConfig?.galleryMinimum || 6}; property examples min ${brandConfig?.propertyExampleMinimum || 3}`,
    operatingModel:
      model.includes("consortium") || model.includes("affiliation")
        ? "Participation standards + owner control; not franchise prototype"
        : "Franchise/collection operating model per brand family",
    commercialModel:
      model.includes("affiliation") || model.includes("consortium")
        ? "Distribution/recognition via affiliation; no published fee stack"
        : "Commercial engine via parent platform systems",
    distributionOrAffiliationContext: brandConfig?.allowedSiblingMentions?.join(", ") || "",
    loyaltyOrRecognitionContext: brandConfig?.parentCompany || "",
    standardsOrParticipationContext: brandConfig?.standardDetailGovernanceRequired
      ? "Standard detail governance review required"
      : "Standard franchise/collection standards",
    economicsAndObligationsContext:
      brandConfig?.franchiseLanguageBlocked
        ? "Affiliation economics — no FDD/fee-stack language in external copy"
        : "Economics per franchise disclosure patterns where applicable",
    propertyExampleStrategy: brandConfig?.propertyExampleStrategy || "",
    galleryStrategy: brandConfig?.galleryPoolStrategy || brandConfig?.galleryPoolFixture || "fixture_pool",
    scenarioStrategy: brandConfig?.overviewScenarioCopy ? "brand_config_packages" : "factory_generated",
    footprintContext: brandConfig?.geographicFallbackRule || "",
  };
}

export function buildBrandKnowledgePack(ctx = {}) {
  const {
    brandSlug,
    brandConfig: inputConfig,
    presentationRows = [],
    brandApi = null,
    assetPack = null,
    registryAssets = [],
    approvedSources = [],
    copyGovernancePlan = null,
    factoryRules = null,
    completeBuildReport = null,
    finalQa = null,
    visualReport = null,
    draftPlan = null,
  } = ctx;

  const brandConfig = inputConfig || getActiveProfileBrandConfig(brandSlug);
  if (!brandConfig) {
    throw new Error(`No active profile brand config for: ${brandSlug}`);
  }

  const renderContract = assetPack
    ? extendAssetPackWithRenderReadiness(assetPack, {
        presentationRows,
        brandApi,
        registryAssets,
      })
    : null;

  const tabContractAudit = auditPresentationRowsAgainstContract(
    presentationRows,
    brandApi?.brandExplorer?.blocks || []
  );

  const externalOwnerRule = evaluateExternalOwnerReadinessRule(presentationRows);

  const sourceCoverage = {
    approvedCount: approvedSources.length,
    approvedForExplorer: approvedSources.filter((s) => s.approvedForExplorerUse === "Yes").length,
    sourceIds: approvedSources.map((s) => s.id || s.sourceId).filter(Boolean),
    readableByFactory: approvedSources.length > 0,
  };

  const visualAssetCoverage = renderContract
    ? {
        galleryRenderReady: renderContract.summary.galleryRenderReady,
        galleryMinimum: renderContract.summary.galleryMinimum,
        propertyExamplesRenderReady: renderContract.summary.propertyExamplesRenderReady,
        scenariosRenderReady: renderContract.summary.scenariosRenderReady,
        registryOnlyCount: renderContract.summary.registryOnlyCount,
        pass: renderContract.pass,
      }
    : { pass: false, note: "asset pack not built" };

  const knownUnknowns = [];
  if (!brandConfig.overviewScenarioCopy) knownUnknowns.push("overview scenario copy not in brand config");
  if (visualAssetCoverage.registryOnlyCount > 0) {
    knownUnknowns.push(`${visualAssetCoverage.registryOnlyCount} assets registry-ready but not render-ready`);
  }
  if (tabContractAudit.documentedButNotRenderedRows?.length) {
    knownUnknowns.push("materials.caseStudy rows may exist without UI render");
  }

  const founderReviewItems = [
    ...(copyGovernancePlan?.founderReviewQueue || []).map((q) => q.reason || q.slotKey),
    ...(factoryRules?.blockers || []),
    ...(draftPlan?.pendingGovernanceGates || []),
  ].filter(Boolean);

  const externalOwnerReadinessRisks = [
    ...(externalOwnerRule.blockers || []),
    ...(factoryRules?.rules?.uiFallback?.risks?.map((r) => r.issue) || []),
    ...(visualReport?.defects?.slice(0, 5).map((d) => d.message || d.type) || []),
  ];

  return {
    knowledgePackVersion: KNOWLEDGE_PACK_VERSION,
    generatedAt: new Date().toISOString(),
    brandName: brandConfig.name,
    slug: brandConfig.slug,
    recordId: brandConfig.recordId,
    parentCompany: brandConfig.parentCompany,
    ...buildContextFields(brandConfig),
    sourceCoverage,
    visualAssetCoverage,
    tabCoverage: tabCoverageFromRows(presentationRows),
    tabContractAudit: {
      visibleRowCount: tabContractAudit.visibleRowCount,
      documentedMismatchCount: tabContractAudit.contract?.summary?.documentedMismatchCount,
      hardcodedFallbackSurfaces: tabContractAudit.contract?.hardcodedFallbackSurfaces?.map((f) => f.id),
      proofFallbackHeads: ATELIER_PROOF_FALLBACK_HEADS,
    },
    knownUnknowns,
    founderReviewItems: [...new Set(founderReviewItems)].slice(0, 20),
    externalOwnerReadinessRisks: [...new Set(externalOwnerReadinessRisks)].slice(0, 25),
    completeBuildStatus: completeBuildReport
      ? {
          halted: completeBuildReport.halted,
          readyForActiveProfile: completeBuildReport.readyForActiveProfile,
          haltReason: completeBuildReport.haltReason,
        }
      : null,
    finalQaStatus: finalQa
      ? {
          readiness: finalQa.readiness || finalQa.scores?.overallActiveProfileReadiness,
          blockers: finalQa.blockers?.slice(0, 10),
        }
      : null,
    draftPlanSummary: draftPlan?.summary || null,
    copyGovernanceSummary: copyGovernancePlan
      ? {
          rowsAudited: copyGovernancePlan.rowsAudited,
          repairsProposed: copyGovernancePlan.repairs?.length,
          founderQueue: copyGovernancePlan.founderReviewQueue?.length,
        }
      : null,
  };
}
