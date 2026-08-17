/**
 * Brand Explorer v36C — contract enforcement with calibrated external-owner score.
 */
import { evaluateExternalOwnerReadinessRule } from "./brand-explorer-external-owner-readiness-rules.js";
import {
  auditPresentationRowExternalOwner,
  auditExternalOwnerPhrase,
} from "./brand-explorer-external-owner-content-governance.js";
import {
  ATELIER_SCENARIO_FALLBACK_TITLES,
  ATELIER_PROOF_FALLBACK_HEADS,
} from "./brand-explorer-active-profile-factory-rules.js";
import { HARDCODED_FALLBACK_SURFACES } from "./brand-explorer-full-tab-content-contract.js";

export const CONTRACT_ENFORCEMENT_VERSION = "v36C";

const TAB_GAP_PREFIXES = {
  "Owner Considerations": ["standards."],
  "Loyalty Program": ["loyalty."],
  "Economics & Obligations": ["economics."],
};

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function isEmptyPlaceholder(v) {
  const t = nz(v);
  return !t || t === "—" || t === "-" || t === "&nbsp;";
}

function tabFromSlot(slotKey) {
  const key = nz(slotKey);
  if (key.startsWith("overview.")) return "Overview";
  if (key.startsWith("valueOwners.")) return "Value to Owners";
  if (key.startsWith("operations.")) return "Operating Model";
  if (key.startsWith("standards.")) return "Owner Considerations";
  if (key.startsWith("commercial.")) return "Commercial Engine";
  if (key.startsWith("economics.")) return "Economics & Obligations";
  if (key.startsWith("loyalty.")) return "Loyalty Program";
  if (key.startsWith("footprint.")) return "Footprint & Growth";
  if (key.startsWith("materials.")) return "Brand Materials";
  if (key.startsWith("insight.")) return "Dealality Insight";
  return "Unknown";
}

function detectFallbackRisk(presentationRows, brandApi, factoryRules) {
  const blocks = brandApi?.brandExplorer?.blocks || [];
  const risks = [];

  for (let i = 1; i <= 3; i += 1) {
    const slotKey = `overview.scenario.${i}`;
    const block = blocks.find((b) => b.slotKey === slotKey);
    if (!block?.imageUrl) {
      risks.push({
        id: "scenario_fallback_risk",
        slotKey,
        detail: `No API block with imageUrl — atelier may render ${ATELIER_SCENARIO_FALLBACK_TITLES[i - 1] || "default"}`,
        ownerVisible: true,
      });
    }
  }

  const proofRows = (presentationRows || []).filter(
    (r) => /^overview\.proof\.\d+$/.test(r.slotKey) && r.visible !== false && nz(r.body)
  );
  if (proofRows.length < 2) {
    for (const head of ATELIER_PROOF_FALLBACK_HEADS.slice(0, 2)) {
      risks.push({
        id: "proof_fallback_risk",
        slotKey: "overview.proof.*",
        detail: `Proof grid may show fallback: ${head}`,
        ownerVisible: true,
      });
    }
  }

  const uiFallback = factoryRules?.rules?.uiFallback;
  if (uiFallback?.risks?.length) {
    for (const r of uiFallback.risks) {
      risks.push({
        id: "ui_fallback_risk",
        slotKey: r.surface || r.issue,
        detail: r.detail || r.issue,
        ownerVisible: true,
      });
    }
  }

  for (const surface of HARDCODED_FALLBACK_SURFACES) {
    if (surface.id === "commercial_static_demand") {
      const hasDemand = blocks.some((b) => b.slotKey === "commercial.demand" && nz(b.body));
      if (!hasDemand) {
        risks.push({
          id: surface.id,
          slotKey: "commercial.demand",
          detail: surface.detail,
          ownerVisible: true,
        });
      }
    }
    if (surface.id === "loyalty_demand_matrix") {
      const hasLoyalty = blocks.filter((b) => b.slotKey.startsWith("loyalty.") && nz(b.body)).length >= 3;
      if (!hasLoyalty) {
        risks.push({
          id: surface.id,
          slotKey: "loyalty.*",
          detail: surface.detail,
          ownerVisible: true,
        });
      }
    }
  }

  return risks;
}

function tabGapIssues(presentationRows, tabName, prefixes, minimum = 2) {
  const rows = (presentationRows || []).filter(
    (r) => r.visible !== false && prefixes.some((p) => r.slotKey.startsWith(p))
  );
  const populated = rows.filter((r) => nz(r.title) && nz(r.body)).length;
  if (populated < minimum) {
    return {
      tab: tabName,
      populated,
      minimum,
      pass: false,
      ownerVisible: true,
    };
  }
  return { tab: tabName, populated, minimum, pass: true, ownerVisible: false };
}

/**
 * Calibrated external owner readiness — prevents high scores when founder-visible gaps remain.
 */
export function enforceExternalOwnerReadiness(ctx = {}) {
  const {
    brandSlug,
    brandConfig,
    presentationRows = [],
    brandApi,
    renderContract,
    factoryRules,
    knowledgePack,
    approvedSourcesCount = 0,
  } = ctx;

  const violations = [];
  const categories = [];
  let score = 100;
  let scoreCap = 100;

  const rule = evaluateExternalOwnerReadinessRule(presentationRows);

  for (const row of presentationRows.filter((r) => r.visible !== false)) {
    const audit = auditPresentationRowExternalOwner(row);
    const combined = `${row.title}\n${row.body}`;

    if (
      audit.hits.some(
        (h) =>
          h.patternId === "http_url" &&
          row.slotKey !== "footprint.openings" &&
          row.slotKey !== "footprint.momentum"
      )
    ) {
      violations.push({
        id: "visible_source_url",
        severity: "critical",
        slotKey: row.slotKey,
        recordId: row.recordId,
        ownerVisible: true,
        penalty: 35,
      });
      scoreCap = Math.min(scoreCap, 40);
      categories.push("blocked_by_internal_language");
    }

    if (audit.hits.some((h) => ["sources_block", "source_line"].includes(h.patternId))) {
      violations.push({
        id: "sources_block_visible",
        severity: "critical",
        slotKey: row.slotKey,
        recordId: row.recordId,
        ownerVisible: true,
        penalty: 30,
      });
      scoreCap = Math.min(scoreCap, 45);
      categories.push("blocked_by_copy");
    }

    if (audit.hits.some((h) => ["franchise_disclosure", "item_19", "loi", "brand_verified"].includes(h.patternId))) {
      violations.push({
        id: "governance_language",
        severity: "high",
        slotKey: row.slotKey,
        recordId: row.recordId,
        ownerVisible: true,
        penalty: 25,
      });
      scoreCap = Math.min(scoreCap, 50);
      categories.push("blocked_by_copy");
    }

    if (nz(row.title) && isEmptyPlaceholder(row.body) && !row.slotKey.startsWith("materials.gallery.")) {
      violations.push({
        id: "empty_card_body",
        severity: "medium",
        slotKey: row.slotKey,
        recordId: row.recordId,
        ownerVisible: true,
        penalty: 8,
      });
      categories.push("content_shell_only");
    }

    if (row.slotKey === "footprint.openings") {
      const modalFields = [
        row.caseSummaryOverview,
        row.caseSummaryBrandRelevance,
        row.caseSummaryOwnerObjective,
        row.caseSummaryInterpretation,
      ];
      const emptyModal = modalFields.filter(isEmptyPlaceholder).length;
      if (emptyModal >= 2) {
        violations.push({
          id: "modal_placeholder",
          severity: "high",
          slotKey: row.slotKey,
          recordId: row.recordId,
          ownerVisible: true,
          penalty: 18,
        });
        scoreCap = Math.min(scoreCap, 65);
        categories.push("founder_review_required");
      }
    }
  }

  const galleryMin = brandConfig?.galleryMinimum || 6;
  const galleryReady = renderContract?.summary?.galleryRenderReady ?? 0;
  const propertyReady = renderContract?.summary?.propertyExamplesRenderReady ?? 0;
  const propertyMin = renderContract?.summary?.propertyExampleMinimum || 3;

  if (galleryReady < galleryMin) {
    violations.push({
      id: "gallery_render_not_ready",
      severity: "high",
      slotKey: "materials.gallery.*",
      ownerVisible: true,
      penalty: 20,
      detail: `${galleryReady}/${galleryMin}`,
    });
    categories.push("blocked_by_images");
    scoreCap = Math.min(scoreCap, 55);
  }

  if (propertyReady < propertyMin) {
    violations.push({
      id: "property_example_render_not_ready",
      severity: "high",
      slotKey: "footprint.openings",
      ownerVisible: true,
      penalty: 22,
      detail: `${propertyReady}/${propertyMin} row-level image match`,
    });
    categories.push("blocked_by_images");
    scoreCap = Math.min(scoreCap, 60);
  }

  if ((renderContract?.summary?.registryOnlyCount || 0) > 0) {
    violations.push({
      id: "registry_only_images",
      severity: "high",
      ownerVisible: false,
      penalty: 15,
      detail: `${renderContract.summary.registryOnlyCount} assets`,
    });
    categories.push("blocked_by_images");
  }

  const underpopulated = (knowledgePack?.tabCoverage || []).filter((t) => t.status === "empty");
  if (underpopulated.length > 1) {
    violations.push({
      id: "underpopulated_tabs",
      severity: "high",
      ownerVisible: true,
      penalty: underpopulated.length * 10,
      detail: underpopulated.map((t) => t.tab).join(", "),
    });
    categories.push("content_shell_only");
    scoreCap = Math.min(scoreCap, 50);
  }

  for (const [tab, prefixes] of Object.entries(TAB_GAP_PREFIXES)) {
    const min = tab === "Owner Considerations" ? 3 : 2;
    const gap = tabGapIssues(presentationRows, tab, prefixes, min);
    if (!gap.pass) {
      violations.push({
        id: "tab_coverage_gap",
        severity: "high",
        tab,
        ownerVisible: true,
        penalty: 12,
        detail: `${gap.populated}/${gap.minimum} populated slots`,
      });
      categories.push("founder_review_required");
      scoreCap = Math.min(scoreCap, 65);
    }
  }

  if (brandConfig?.franchiseLanguageBlocked) {
    const franchiseRows = presentationRows.filter((r) =>
      /\b(franchise flag|franchise conversion|standard prototype|fdd|item\s*19)\b/i.test(`${r.title}\n${r.body}`)
    );
    if (franchiseRows.length) {
      violations.push({
        id: "wrong_model_language",
        severity: "high",
        ownerVisible: true,
        penalty: 20,
        detail: `${franchiseRows.length} rows`,
      });
      categories.push("blocked_by_model_fit");
      scoreCap = Math.min(scoreCap, 55);
    }
  }

  const fallbackRisks = detectFallbackRisk(presentationRows, brandApi, factoryRules);
  for (const risk of fallbackRisks) {
    violations.push({
      id: risk.id,
      severity: "medium",
      slotKey: risk.slotKey,
      ownerVisible: risk.ownerVisible,
      penalty: 15,
      detail: risk.detail,
    });
    categories.push("blocked_by_renderer_mismatch");
    scoreCap = Math.min(scoreCap, 70);
  }

  if (approvedSourcesCount < 3) {
    violations.push({
      id: "insufficient_sources",
      severity: "high",
      ownerVisible: false,
      penalty: 25,
    });
    categories.push("blocked_by_sources");
    scoreCap = Math.min(scoreCap, 40);
  }

  for (const v of violations) {
    score -= v.penalty || 0;
  }
  score = Math.max(0, Math.min(scoreCap, score));

  const uniqueCategories = [...new Set(categories)];
  const founderVisibleViolations = violations.filter((v) => v.ownerVisible);
  const pass =
    score >= 85 &&
    founderVisibleViolations.filter((v) => v.severity === "critical" || v.severity === "high").length === 0 &&
    rule.pass;

  let band = "blocked";
  if (pass) band = "external_owner_ready";
  else if (scoreCap <= 65 || uniqueCategories.includes("founder_review_required")) band = "founder_review_required";
  else if (score >= 70) band = "content_shell_only";
  else band = "blocked";

  if (brandSlug === "design-hotels") {
    const dhBlockers = violations.filter((v) =>
      [
        "modal_placeholder",
        "property_example_render_not_ready",
        "tab_coverage_gap",
        "visible_source_url",
        "sources_block_visible",
        "wrong_model_language",
      ].includes(v.id)
    );
    if (dhBlockers.length) {
      score = Math.min(score, 65);
      band = "founder_review_required";
    }
  }

  return {
    version: CONTRACT_ENFORCEMENT_VERSION,
    brandSlug,
    numericScore: score,
    scoreCap,
    band,
    pass,
    categories: uniqueCategories.length ? uniqueCategories : ["content_shell_only"],
    violations,
    founderVisibleViolationCount: founderVisibleViolations.length,
    blockers: violations.map((v) => `${v.id}:${v.slotKey || v.tab || ""}`),
    ruleDetail: rule,
  };
}

export function enforcePresentationPlan(planValidation) {
  if (!planValidation) return { pass: false, blockers: ["no_plan"] };
  return {
    pass: planValidation.pass,
    externalOwnerReady: planValidation.summary?.externalOwnerReady ?? 0,
    total: planValidation.summary?.total ?? 0,
    renderBlocked: planValidation.summary?.renderBlocked ?? 0,
    blockers: planValidation.blockers || [],
  };
}

export { tabFromSlot };
