/**
 * Source provenance by tab for Brand Explorer.
 * Classifies evidence as brand-specific vs parent vs third-party per tab/section.
 */
import { CANONICAL_BRAND_SOURCE_RULES, evaluateBrandSpecificSourceValidation } from "./brand-explorer-brand-specific-source-validation.js";
import { TAB_CONTRACTS } from "./brand-explorer-tab-contracts.js";
import { getActiveProfileBrandConfig } from "./brand-explorer-active-profile-brand-config.js";

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function hostnameOf(url) {
  try {
    return new URL(String(url)).hostname.replace(/^www\./i, "").toLowerCase();
  } catch {
    return "";
  }
}

function hostMatches(host, domain) {
  const h = nz(host).toLowerCase();
  const d = nz(domain).toLowerCase().replace(/^www\./, "");
  if (!h || !d) return false;
  return h === d || h.endsWith(`.${d}`);
}

function classifyHost(host, rule) {
  if (!host) return "unknown";
  const required = rule?.requiredBrandDomains || [];
  const parent = rule?.allowedParentDomains || [];
  if (required.some((d) => hostMatches(host, d))) return "brand_specific";
  if (parent.some((d) => hostMatches(host, d))) return "parent";
  return "third_party";
}

/** Tabs/sections that must not be parent-dominated. */
const BRAND_SPECIFIC_TAB_SECTIONS = Object.freeze([
  "Brand Positioning",
  "Value Creation & Proof",
  "Support Across the Lifecycle",
  "Standards Philosophy",
  "Third-Party Operator Compatibility",
  "Opening & Conversion Path",
  "Geographic Footprint & Growth",
  "Similar Brands",
]);

/**
 * @returns {{ pass: boolean, brandSlug: string, tabs: object[], failures: string[], overall: object }}
 */
export function evaluateSourceProvenanceByTab({
  brandSlug,
  brandConfig = null,
  registryAssets = [],
  presentationRows = [],
  brandApi = null,
} = {}) {
  const config = brandConfig || getActiveProfileBrandConfig(brandSlug) || null;
  const rule = CANONICAL_BRAND_SOURCE_RULES[brandSlug] || {
    requiredBrandDomains: config?.consumerUrl ? [hostnameOf(config.consumerUrl)].filter(Boolean) : [],
    allowedParentDomains: [],
  };

  const base = evaluateBrandSpecificSourceValidation({
    brandSlug,
    brandConfig: config,
    registryAssets,
    presentationRows,
    brandApi,
  });

  const sources = base.sources || [];
  const classCounts = base.classificationCounts || {};
  const brandSpecific =
    (classCounts.brand_specific || 0) + (classCounts.brand_development || 0);
  const parent = classCounts.parent || 0;
  const total = brandSpecific + parent + (classCounts.third_party || 0);

  const failures = [...(base.failures || [])];
  const tabs = TAB_CONTRACTS.map((tab) => {
    const brandSpecificSections = tab.sections.filter((s) =>
      BRAND_SPECIFIC_TAB_SECTIONS.includes(s.sectionName)
    );
    const requiresBrandSource = brandSpecificSections.length > 0;
    let status = "accepted";
    let note = "Shared brand source pool acceptable for this tab.";
    if (requiresBrandSource) {
      if ((rule.requiredBrandDomains || []).length && (base.missingRequiredBrandDomains || []).length) {
        status = "rejected";
        note = `Missing canonical brand domains: ${base.missingRequiredBrandDomains.join(", ")}`;
      } else if (total >= 3 && brandSpecific > 0 && brandSpecific < 3 && parent / total >= 0.7) {
        status = "rejected";
        note = "Parent-company umbrella pages dominate brand-specific evidence.";
        if (!failures.includes("sources_mostly_parent_company_umbrella")) {
          failures.push("sources_mostly_parent_company_umbrella");
        }
      } else if (total > 0 && brandSpecific === 0 && parent > 0) {
        status = "rejected";
        note = "Brand-specific tab has parent-only sources.";
      }
    }
    return {
      tabId: tab.tabId,
      tabName: tab.tabName,
      primarySourceDomains: [...new Set(sources.map((s) => s.host).filter(Boolean))].slice(0, 8),
      classificationCounts: classCounts,
      sourceRole: requiresBrandSource ? "brand_specific_content" : "mixed_or_context",
      status,
      note,
      sections: tab.sections.map((s) => ({
        sectionName: s.sectionName,
        brandSpecificRequired: BRAND_SPECIFIC_TAB_SECTIONS.includes(s.sectionName),
        fieldCount: s.fields.length,
      })),
    };
  });

  if ((rule.requiredBrandDomains || []).length && (base.missingRequiredBrandDomains || []).length) {
    if (!failures.some((f) => String(f).startsWith("missing_canonical"))) {
      failures.push(`missing_canonical_brand_domains:${base.missingRequiredBrandDomains.join(",")}`);
    }
  }

  const rejectedTabs = tabs.filter((t) => t.status === "rejected");
  return {
    pass: failures.length === 0 && rejectedTabs.length === 0,
    brandSlug,
    requiredBrandDomains: rule.requiredBrandDomains || [],
    missingRequiredBrandDomains: base.missingRequiredBrandDomains || [],
    classificationCounts: classCounts,
    sources: sources.slice(0, 40),
    tabs,
    failures: [...new Set(failures)],
    overall: {
      brandSpecific,
      parent,
      thirdParty: classCounts.third_party || 0,
      parentShare: total ? parent / total : 0,
    },
  };
}

export function formatSourceProvenanceMarkdown(report) {
  const lines = [
    `# Source Provenance by Tab — ${report.brandSlug}`,
    "",
    `Pass: **${report.pass}**`,
    `Required domains: ${(report.requiredBrandDomains || []).join(", ") || "—"}`,
    `Missing: ${(report.missingRequiredBrandDomains || []).join(", ") || "—"}`,
    `Counts: brand=${report.overall?.brandSpecific || 0} parent=${report.overall?.parent || 0} third=${report.overall?.thirdParty || 0}`,
    "",
    "| Tab | Status | Role | Note |",
    "| --- | --- | --- | --- |",
  ];
  for (const t of report.tabs || []) {
    lines.push(`| ${t.tabName} | ${t.status} | ${t.sourceRole} | ${t.note} |`);
  }
  if (report.failures?.length) {
    lines.push("", "## Failures", "");
    for (const f of report.failures) lines.push(`- ${f}`);
  }
  lines.push("");
  return lines.join("\n");
}
