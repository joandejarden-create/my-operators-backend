/**
 * Operator Explorer — Source provenance by tab.
 *
 * Validates that publishable tabs are backed by an acceptable source mix:
 * operator-specific official domains first; parent/enterprise context labeled;
 * third-party supplementary only.
 *
 * Benchmarks: Arbor Lodging + Hotel Equities quality baselines.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  OPERATOR_QUALITY_BASELINE_OPERATORS,
  getOperatorQualityBaselineEntry,
} from "./operator-explorer-quality-baseline.js";
import { getOperatorFactoryQueueEntry } from "./operator-explorer-factory-queue.js";
import { OPERATOR_PUBLISHABLE_TABS } from "./operator-explorer-tab-contracts.js";
import { PILOT_OPERATOR_SOURCE_CANDIDATES } from "../../api/lib/partner-intelligence-explorer-field-registry.js";

export const OPERATOR_SOURCE_PROVENANCE_VERSION = "operator-source-provenance-by-tab-v1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

/** Canonical domain rules for quality baselines + factory-queue operators. */
export const CANONICAL_OPERATOR_SOURCE_RULES = Object.freeze({
  "arbor-lodging-cala": Object.freeze({
    requiredOperatorDomains: Object.freeze(["arborlodging.com"]),
    allowedParentDomains: Object.freeze([]), // CALA uses same official domain; enterprise labeled in copy
    allowedThirdPartyDomains: Object.freeze(["hotelinvestmenttoday.com"]),
  }),
  "hotel-equities-cala": Object.freeze({
    requiredOperatorDomains: Object.freeze(["hotelequities.com"]),
    allowedParentDomains: Object.freeze([]),
    allowedThirdPartyDomains: Object.freeze([]),
  }),
  "ghl-hoteles": Object.freeze({
    requiredOperatorDomains: Object.freeze(["ghlhoteles.com"]),
    allowedParentDomains: Object.freeze([]),
    allowedThirdPartyDomains: Object.freeze([]),
  }),
  "aimbridge-latam": Object.freeze({
    requiredOperatorDomains: Object.freeze(["aimbridgelatam.com"]),
    allowedParentDomains: Object.freeze(["aimbridgehospitality.com"]),
    allowedThirdPartyDomains: Object.freeze(["businesswire.com"]),
  }),
  "tafer-hotels-resorts": Object.freeze({
    requiredOperatorDomains: Object.freeze(["taferresorts.com"]),
    allowedParentDomains: Object.freeze(["taferresorts.com.mx"]),
    allowedThirdPartyDomains: Object.freeze([]),
  }),
  "grupo-presidente": Object.freeze({
    requiredOperatorDomains: Object.freeze(["grupopresidente.com.mx"]),
    allowedParentDomains: Object.freeze([]),
    allowedThirdPartyDomains: Object.freeze([]),
  }),
  highgate: Object.freeze({
    requiredOperatorDomains: Object.freeze(["highgate.com"]),
    allowedParentDomains: Object.freeze([]),
    allowedThirdPartyDomains: Object.freeze([]),
  }),
  "grupo-hotelero-santa-fe": Object.freeze({
    requiredOperatorDomains: Object.freeze(["gsf-hotels.com"]),
    allowedParentDomains: Object.freeze([]),
    allowedThirdPartyDomains: Object.freeze([]),
  }),
  "arriva-hospitality-group": Object.freeze({
    requiredOperatorDomains: Object.freeze(["arrivahotels.mx"]),
    allowedParentDomains: Object.freeze([]),
    allowedThirdPartyDomains: Object.freeze([]),
  }),
  "brittain-resorts-hotels": Object.freeze({
    requiredOperatorDomains: Object.freeze(["brittainresorts.com"]),
    allowedParentDomains: Object.freeze([]),
    allowedThirdPartyDomains: Object.freeze([]),
  }),
  "atlantica-hotels-international": Object.freeze({
    requiredOperatorDomains: Object.freeze(["atlanticahotels.com.br"]),
    allowedParentDomains: Object.freeze([]),
    allowedThirdPartyDomains: Object.freeze([]),
  }),
  oxohotel: Object.freeze({
    requiredOperatorDomains: Object.freeze(["oxohotel.com"]),
    allowedParentDomains: Object.freeze([]),
    allowedThirdPartyDomains: Object.freeze([]),
  }),
  "grupo-marta-hospitality": Object.freeze({
    requiredOperatorDomains: Object.freeze(["grupomarta.com"]),
    allowedParentDomains: Object.freeze([]),
    allowedThirdPartyDomains: Object.freeze([]),
  }),
  "grupo-iberostar": Object.freeze({
    requiredOperatorDomains: Object.freeze(["grupoiberostar.com"]),
    allowedParentDomains: Object.freeze(["iberostar.com"]),
    allowedThirdPartyDomains: Object.freeze([]),
  }),
});

/** Tabs that must not be third-party-only / missing operator-official evidence. */
export const OPERATOR_SPECIFIC_TABS = Object.freeze([
  "Profile & Positioning",
  "Operating Platform",
  "Brand & Relationships",
  "Markets & Footprint",
  "Leadership",
  "Project Fit & Deal Profile",
  "Proof & Track Record",
]);

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
  if ((rule.requiredOperatorDomains || []).some((d) => hostMatches(host, d))) {
    return "operator_specific";
  }
  if ((rule.allowedParentDomains || []).some((d) => hostMatches(host, d))) {
    return "parent_enterprise";
  }
  if ((rule.allowedThirdPartyDomains || []).some((d) => hostMatches(host, d))) {
    return "third_party";
  }
  return "third_party";
}

function extractUrls(text) {
  const s = nz(text);
  if (!s) return [];
  const re = /https?:\/\/[^\s)"']+/gi;
  return [...s.matchAll(re)].map((m) => m[0].replace(/[.,;]+$/, ""));
}

function fixtureSuffix(slug) {
  if (slug === "arbor-lodging-cala") return "arbor-cala";
  if (slug === "hotel-equities-cala") return "he-cala";
  if (slug === "ghl-hoteles") return "ghl-hoteles";
  if (slug === "aimbridge-latam") return "aimbridge-latam";
  if (slug === "viento-sur-gestion-hotelera") return "viento-sur";
  // Factory queue slugs use themselves as fixture suffix
  if (getOperatorFactoryQueueEntry(slug)) return slug;
  return null;
}

/**
 * Collect URLs from baseline fixture _meta.source / notes and known pilot candidates.
 */
export function collectFixtureProvenanceSources(slug) {
  const entry =
    getOperatorQualityBaselineEntry(slug) || getOperatorFactoryQueueEntry(slug);
  if (!entry) return [];
  const suffix = fixtureSuffix(entry.slug);
  const sources = [];
  const seen = new Set();

  const push = (raw) => {
    const url = nz(raw?.sourceUrl || raw?.url || raw);
    if (!url || seen.has(url)) return;
    seen.add(url);
    sources.push({
      sourceTitle: nz(raw?.sourceTitle || raw?.title) || url,
      sourceUrl: url,
      host: hostnameOf(url),
      origin: raw?.origin || "fixture_meta",
      approvedForExplorerUse: raw?.approvedForExplorerUse || null,
    });
  };

  const dir = path.join(ROOT, "fixtures");
  if (suffix && fs.existsSync(dir)) {
    for (const name of fs.readdirSync(dir)) {
      if (!name.startsWith("operator-") || !name.endsWith(`-${suffix}.json`)) continue;
      try {
        const raw = JSON.parse(fs.readFileSync(path.join(dir, name), "utf8"));
        const meta = raw?._meta || {};
        for (const u of extractUrls(meta.source || "")) push({ sourceUrl: u, origin: "fixture_meta" });
        for (const u of extractUrls(meta.note || "")) push({ sourceUrl: u, origin: "fixture_meta" });
        if (meta.sourceUrl) push({ sourceUrl: meta.sourceUrl, sourceTitle: meta.sourceTitle, origin: "fixture_meta" });
      } catch {
        /* skip bad fixture */
      }
    }
  }

  // Pilot seed candidates (not necessarily in Source Library yet)
  const pilotKey = entry.slug === "arbor-lodging-cala" ? "arborLodging" : entry.slug === "hotel-equities-cala" ? "hotelEquities" : null;
  const candidates =
    (pilotKey && PILOT_OPERATOR_SOURCE_CANDIDATES[pilotKey]) ||
    [];
  for (const c of candidates) {
    push({ ...c, origin: "pilot_candidate" });
  }

  // Always include canonical homepage as required-domain anchor
  if (entry.domain) {
    push({
      sourceTitle: `${entry.companyName} official site`,
      sourceUrl: `https://www.${entry.domain}/`,
      origin: "canonical_domain",
    });
  }

  if (entry.slug === "aimbridge-latam") {
    push({
      sourceTitle: "Aimbridge Hospitality — Alex Fiz LATAM appointment",
      sourceUrl:
        "https://www.aimbridgehospitality.com/news/aimbridge-selects-alex-fiz-to-lead-its-latam-and-all-inclusive-divisions-/",
      origin: "parent_enterprise_labeled",
    });
  }

  return sources;
}

/**
 * Optional live PI Source Library rows for an operator Master id.
 */
export async function loadLiveOperatorPartnerSources(recordId) {
  try {
    const { listPartnerSources } = await import("./airtable-source.js");
    const rows = await listPartnerSources({
      operatorId: recordId,
      profileType: "Operator",
    });
    return (rows || []).map((s) => ({
      sourceTitle: s.sourceTitle,
      sourceUrl: s.sourceUrl,
      host: hostnameOf(s.sourceUrl),
      origin: "partner_intelligence",
      approvedForExplorerUse: s.approvedForExplorerUse,
      status: s.status,
      sourceType: s.sourceType,
      id: s.id,
    }));
  } catch (err) {
    return {
      error: err?.message || String(err),
      sources: [],
    };
  }
}

function countClasses(classified) {
  const counts = {
    operator_specific: 0,
    parent_enterprise: 0,
    third_party: 0,
    unknown: 0,
  };
  for (const s of classified) {
    counts[s.classification] = (counts[s.classification] || 0) + 1;
  }
  return counts;
}

/**
 * Sync evaluation from a pre-collected source list.
 */
export function evaluateOperatorSourceProvenanceByTab({
  operatorSlug,
  operatorName = null,
  recordId = null,
  sources = [],
} = {}) {
  const entry =
    getOperatorQualityBaselineEntry(operatorSlug) ||
    getOperatorQualityBaselineEntry(recordId) ||
    getOperatorFactoryQueueEntry(operatorSlug) ||
    getOperatorFactoryQueueEntry(recordId);
  const slug = entry?.slug || String(operatorSlug || "");
  const rule =
    CANONICAL_OPERATOR_SOURCE_RULES[slug] ||
    (entry?.domain
      ? {
          requiredOperatorDomains: [entry.domain],
          allowedParentDomains: [],
          allowedThirdPartyDomains: [],
        }
      : { requiredOperatorDomains: [], allowedParentDomains: [], allowedThirdPartyDomains: [] });

  const classified = (sources || [])
    .map((s) => {
      const host = s.host || hostnameOf(s.sourceUrl);
      return {
        ...s,
        host,
        classification: classifyHost(host, rule),
      };
    })
    .filter((s) => s.sourceUrl || s.host);

  const counts = countClasses(classified);
  const operatorSpecific = counts.operator_specific || 0;
  const parent = counts.parent_enterprise || 0;
  const third = counts.third_party || 0;
  const total = operatorSpecific + parent + third + (counts.unknown || 0);

  const missingRequired = (rule.requiredOperatorDomains || []).filter(
    (d) => !classified.some((s) => s.classification === "operator_specific" && hostMatches(s.host, d))
  );

  const failures = [];
  if ((rule.requiredOperatorDomains || []).length && missingRequired.length) {
    failures.push(`missing_canonical_operator_domains:${missingRequired.join(",")}`);
  }
  if (total === 0) {
    failures.push("no_sources_collected");
  } else if (operatorSpecific === 0) {
    failures.push("no_operator_specific_sources");
  }

  const approvedExplorer = classified.filter((s) =>
    /^(yes|true|1)$/i.test(nz(s.approvedForExplorerUse))
  );

  const tabs = OPERATOR_PUBLISHABLE_TABS.map((tab) => {
    const requiresOperatorSource = OPERATOR_SPECIFIC_TABS.includes(tab.tab);
    let status = "accepted";
    let note = "Shared operator source pool acceptable for this tab.";

    if (requiresOperatorSource) {
      if (missingRequired.length) {
        status = "rejected";
        note = `Missing canonical operator domains: ${missingRequired.join(", ")}`;
      } else if (operatorSpecific === 0 && third > 0) {
        status = "rejected";
        note = "Operator-specific tab has third-party-only evidence.";
        if (!failures.includes("operator_tab_third_party_only")) {
          failures.push("operator_tab_third_party_only");
        }
      } else if (total >= 3 && operatorSpecific > 0 && operatorSpecific < 2 && third / total >= 0.7) {
        status = "rejected";
        note = "Third-party sources dominate operator-specific evidence.";
        if (!failures.includes("sources_mostly_third_party")) {
          failures.push("sources_mostly_third_party");
        }
      } else if (operatorSpecific === 0) {
        status = "rejected";
        note = "No operator-specific official sources for this tab.";
      } else {
        note = "Operator-specific official source present.";
      }
    }

    return {
      tabName: tab.tab,
      tabIndex: tab.tabIndex,
      requiresOperatorSource,
      primarySourceDomains: [...new Set(classified.map((s) => s.host).filter(Boolean))].slice(0, 8),
      classificationCounts: counts,
      status,
      note,
    };
  });

  const pass = failures.length === 0 && tabs.every((t) => t.status === "accepted");

  return {
    version: OPERATOR_SOURCE_PROVENANCE_VERSION,
    operatorSlug: slug,
    operatorName: operatorName || entry?.companyName || slug,
    recordId: recordId || entry?.recordId || null,
    pass,
    failures,
    rule,
    sources: classified,
    classificationCounts: counts,
    approvedExplorerUseCount: approvedExplorer.length,
    missingRequiredOperatorDomains: missingRequired,
    tabs,
    gates: {
      source_provenance_by_tab: pass,
      operator_specific_source_validation: missingRequired.length === 0 && operatorSpecific > 0,
    },
  };
}

export function formatOperatorSourceProvenanceMarkdown(result) {
  const lines = [
    `## ${result.operatorName} (\`${result.operatorSlug}\`)`,
    "",
    `pass: **${result.pass}**`,
    `failures: ${(result.failures || []).join(", ") || "(none)"}`,
    `counts: operator=${result.classificationCounts?.operator_specific || 0} parent=${result.classificationCounts?.parent_enterprise || 0} third=${result.classificationCounts?.third_party || 0}`,
    `approvedExplorerUse: **${result.approvedExplorerUseCount || 0}**`,
    "",
    "### Tabs",
    "",
  ];
  for (const t of result.tabs || []) {
    lines.push(
      `- **${t.tabName}**: ${t.status === "accepted" ? "PASS" : "FAIL"} — ${t.note}`
    );
  }
  lines.push("", "### Sources", "");
  for (const s of (result.sources || []).slice(0, 20)) {
    lines.push(`- \`${s.classification}\` ${s.host || "—"} — ${s.sourceTitle || s.sourceUrl}`);
  }
  lines.push("");
  return lines.join("\n");
}

export { OPERATOR_QUALITY_BASELINE_OPERATORS };
