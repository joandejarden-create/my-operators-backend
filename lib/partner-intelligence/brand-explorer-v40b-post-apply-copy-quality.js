/**
 * v40B — Post-apply copy quality audit + founder review packets.
 *
 * Read-only. Audits full profile via internal preview + Presentation rows + Brand Library API.
 * External quality lock only confirms Profile in Preparation remains locked.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getActiveProfileBrandConfig } from "./brand-explorer-active-profile-brand-config.js";
import { getDiscoveryBrandConfig } from "./brand-explorer-brand-asset-image-governance.js";
import { loadBrandFactoryContext } from "./brand-explorer-active-profile-factory.js";
import { evaluateBrandExternalQualityLock } from "./brand-explorer-display-quality-lock.js";
import { renderBrandExplorerHtmlForTest } from "./brand-explorer-atelier-render-test-loader.js";
import { evaluateExternalOwnerReadinessRule } from "./brand-explorer-external-owner-readiness-rules.js";
import {
  V40B_VERSION,
  V40B_DEFAULT_BRANDS,
  V40B_BRAND_COPY_PROFILES,
  scanForbiddenLanguage,
  scanMechanicalCopy,
  evaluateBrandCopySignals,
  detectRepeatedBoilerplate,
  isVagueAfterScrub,
} from "./brand-explorer-v40b-copy-quality-patterns.js";

export { V40B_VERSION, V40B_DEFAULT_BRANDS };

export const REPORT_JSON = "brand-explorer-v40b-post-apply-copy-quality.json";
export const REPORT_MD = "brand-explorer-v40b-post-apply-copy-quality.md";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

const GALLERY_MIN = 6;
const PROPERTY_EXAMPLE_MIN = 3;

const MAP_PRESENTATION_FIELDS = Object.freeze({
  title: "Title",
  body: "Body",
  caseSummaryOverview: "Case Summary Overview",
  caseSummaryBrandRelevance: "Case Summary Brand Relevance",
  caseSummaryOwnerObjective: "Case Summary Owner Objective",
  caseSummaryInterpretation: "Case Summary Interpretation",
  caseSummaryTags: "Case Summary Tags",
});

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function resolveConfig(slug) {
  return getActiveProfileBrandConfig(slug) || getDiscoveryBrandConfig(slug) || null;
}

async function fetchBrandApiShape(slug) {
  const { getBrandLibraryBrandById } = await import("../../api/brand-library.js");
  const res = {
    statusCode: 200,
    payload: null,
    setHeader() {},
    status(c) {
      this.statusCode = c;
      return this;
    },
    json(p) {
      this.payload = p;
      return this;
    },
  };
  await getBrandLibraryBrandById({ query: { brandId: slug }, headers: {} }, res);
  if (res.statusCode !== 200 || !res.payload?.brand) {
    throw new Error(`brand API fetch failed for ${slug}: HTTP ${res.statusCode}`);
  }
  return res.payload.brand;
}

function loadV40ApplyReport() {
  const p = path.join(ROOT, "reports", "brand-explorer-v40-active-release-remediation.json");
  if (!fs.existsSync(p)) return null;
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return null;
  }
}

function collectRowTexts(row) {
  const texts = [];
  const fields = [];
  for (const [apiKey] of Object.entries(MAP_PRESENTATION_FIELDS)) {
    const val = nz(row[apiKey]);
    if (!val) continue;
    texts.push(val);
    fields.push({ field: apiKey, airtableField: MAP_PRESENTATION_FIELDS[apiKey], text: val });
  }
  return { texts, fields };
}

function countGallery(blocks = []) {
  return blocks.filter((b) => /^materials\.gallery\.\d+$/.test(nz(b.slotKey)) && nz(b.imageUrl)).length;
}

function countPropertyExamples(blocks = []) {
  return blocks.filter((b) => nz(b.slotKey) === "footprint.openings" && nz(b.imageUrl)).length;
}

function stripHtml(html) {
  return nz(html)
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    // Drop URL-bearing attributes so brand/site/image hrefs are not treated as owner-copy “raw URLs”
    .replace(/\s(?:href|src|srcset|data-src)=["'][^"']*["']/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/\s+/g, " ")
    .trim();
}

function dedupeHits(hits = []) {
  const map = new Map();
  for (const h of hits) {
    const key = `${h.id}|${h.source || ""}|${h.slotKey || ""}|${h.snippet || ""}`;
    if (!map.has(h.id)) map.set(h.id, h);
    else if (!map.get(h.id).sources) {
      const prev = map.get(h.id);
      map.set(h.id, {
        ...prev,
        sources: [...new Set([prev.source, h.source].filter(Boolean))],
        layers: [...new Set([prev.source, h.source].filter(Boolean))],
      });
    } else {
      const prev = map.get(h.id);
      prev.sources = [...new Set([...(prev.sources || []), h.source].filter(Boolean))];
      prev.layers = prev.sources;
    }
  }
  return [...map.values()];
}

function recommendFounderDecision({
  forbiddenHits,
  mechanicalHigh,
  brandSignals,
  galleryCount,
  propertyExampleCount,
  vagueRowCount,
}) {
  if (forbiddenHits.length > 0) {
    return {
      decision: "not_owner_ready",
      rationale: `Forbidden owner language still visible (${forbiddenHits.map((h) => h.label).join(", ")}).`,
    };
  }
  if (galleryCount < GALLERY_MIN || propertyExampleCount < PROPERTY_EXAMPLE_MIN) {
    return {
      decision: "not_owner_ready",
      rationale: `Visual readiness incomplete (gallery ${galleryCount}/${GALLERY_MIN}, property examples ${propertyExampleCount}/${PROPERTY_EXAMPLE_MIN}).`,
    };
  }
  if (mechanicalHigh > 0 || (brandSignals.avoidHits || []).length > 0 || vagueRowCount >= 5) {
    return {
      decision: "more_remediation_required",
      rationale:
        mechanicalHigh > 0
          ? "High-severity mechanical scrub artifacts remain; rewrite before founder visual pass."
          : vagueRowCount >= 5
            ? "Multiple rows read vague after scrub; need targeted copy remediation."
            : "Brand-specific avoid signals still present.",
    };
  }
  if ((brandSignals.missingExpected || []).length >= 2) {
    return {
      decision: "more_remediation_required",
      rationale: `Brand positioning cues thin (${brandSignals.missingExpected.join(", ")}).`,
    };
  }
  return {
    decision: "founder_visual_review_ready",
    rationale:
      "Forbidden language clear; visuals meet minimums; remaining items are founder judgment (positioning tone, image quality, active approval later).",
  };
}

function auditPresentationRows(presentationRows, brandSlug) {
  const rowAudits = [];
  const allTexts = [];
  const forbiddenHits = [];
  const mechanicalHits = [];
  let vagueRowCount = 0;

  for (const row of presentationRows || []) {
    if (/do not display|internal only/i.test(nz(row.externalDisplayStatus))) continue;
    const { texts, fields } = collectRowTexts(row);
    if (!texts.length) continue;
    allTexts.push(...texts);
    const joined = texts.join("\n\n");
    const forbidden = scanForbiddenLanguage(joined);
    const mechanical = scanMechanicalCopy(joined);
    const vagueFields = fields.filter((f) => f.field === "body" && isVagueAfterScrub(f.text));
    if (vagueFields.length) vagueRowCount += 1;
    for (const h of forbidden) {
      forbiddenHits.push({ ...h, slotKey: row.slotKey, recordId: row.recordId });
    }
    for (const h of mechanical) {
      mechanicalHits.push({ ...h, slotKey: row.slotKey, recordId: row.recordId });
    }
    rowAudits.push({
      recordId: row.recordId,
      slotKey: row.slotKey,
      forbidden,
      mechanical,
      vagueFields: vagueFields.map((f) => f.field),
      fieldCount: fields.length,
    });
  }

  const repeatedBoilerplate = detectRepeatedBoilerplate(allTexts);
  return {
    rowAudits,
    forbiddenHits,
    mechanicalHits,
    vagueRowCount,
    repeatedBoilerplate,
    corpusText: allTexts.join("\n\n"),
    visibleRowCount: rowAudits.length,
  };
}

/**
 * Audit one brand — Presentation + API + internal preview DOM + external lock.
 */
export async function auditBrandV40B(brandSlug) {
  const config = resolveConfig(brandSlug);
  const ctx = await loadBrandFactoryContext(brandSlug).catch(() => null);
  const brandApi = await fetchBrandApiShape(brandSlug);
  const presentationRows = ctx?.presentationRows || brandApi?.brandExplorer?.blocks || [];
  const blocks = brandApi?.brandExplorer?.blocks || presentationRows;

  const presentationAudit = auditPresentationRows(presentationRows, brandSlug);
  const apiCorpus = (blocks || [])
    .map((b) =>
      [b.title, b.body, b.caseSummaryOverview, b.caseSummaryBrandRelevance, b.caseSummaryOwnerObjective, b.caseSummaryInterpretation, b.caseSummaryTags]
        .map(nz)
        .filter(Boolean)
        .join("\n")
    )
    .filter(Boolean)
    .join("\n\n");

  const apiForbidden = scanForbiddenLanguage(apiCorpus);
  const apiMechanical = scanMechanicalCopy(apiCorpus);

  // External lock (locked profile) — expect Profile in Preparation only
  const externalHtml = renderBrandExplorerHtmlForTest(brandApi, { allPanels: true, internalPreview: false });
  const externalQl = evaluateBrandExternalQualityLock(brandApi, externalHtml, {
    brandSlug,
    brandBasics: ctx?.brandBasics,
  });

  // Internal preview — full profile for founder review content
  const internalHtml = renderBrandExplorerHtmlForTest(brandApi, { allPanels: true, internalPreview: true });
  const internalText = stripHtml(internalHtml);
  const internalForbidden = scanForbiddenLanguage(internalText);
  const internalMechanical = scanMechanicalCopy(internalText);
  // For full-profile DOM: ignore "Internal preview" / "Not owner-ready" banner strings
  const internalQl = evaluateBrandExternalQualityLock(
    { ...brandApi, shouldRenderFullProfile: true, brandExplorerDisplayState: "external_owner_ready" },
    internalHtml,
    { brandSlug, brandBasics: ctx?.brandBasics }
  );

  const combinedForbidden = [
    ...presentationAudit.forbiddenHits.map((h) => ({ ...h, source: "presentation" })),
    ...apiForbidden.map((h) => ({ ...h, source: "api_blocks" })),
    ...internalForbidden.map((h) => ({ ...h, source: "internal_preview_dom" })),
  ];
  const forbiddenUnique = dedupeHits(combinedForbidden);

  // Classify whether Presentation-only is clean (v40 scrub target) vs renderer/Brand Setup chrome
  const presentationForbiddenUnique = dedupeHits(
    presentationAudit.forbiddenHits.map((h) => ({ ...h, source: "presentation" }))
  );
  const rendererChromeHints = internalForbidden
    .filter((h) => !presentationForbiddenUnique.some((p) => p.id === h.id))
    .map((h) => h.label);

  const combinedMechanical = [
    ...presentationAudit.mechanicalHits,
    ...apiMechanical.map((h) => ({ ...h, source: "api_blocks" })),
    ...internalMechanical.map((h) => ({ ...h, source: "internal_preview_dom" })),
  ];
  const mechanicalHigh = combinedMechanical.filter((h) => h.severity === "high").length;

  const brandCorpus = [presentationAudit.corpusText, apiCorpus, internalText].join("\n\n");
  const brandSignals = evaluateBrandCopySignals(brandSlug, brandCorpus);

  const galleryCount = countGallery(blocks);
  const propertyExampleCount = countPropertyExamples(blocks);

  const ownerRule = evaluateExternalOwnerReadinessRule(presentationRows);

  const v40Apply = loadV40ApplyReport();
  const v40Brand = (v40Apply?.brandResults || []).find((b) => b.brandSlug === brandSlug);

  const founderDecision = recommendFounderDecision({
    forbiddenHits: forbiddenUnique,
    mechanicalHigh,
    brandSignals,
    galleryCount,
    propertyExampleCount,
    vagueRowCount: presentationAudit.vagueRowCount,
  });

  const remainingJudgment = [];
  if ((brandSignals.missingExpected || []).length) {
    remainingJudgment.push(`Confirm brand positioning: missing ${brandSignals.missingExpected.join(", ")}`);
  }
  if (combinedMechanical.length) {
    remainingJudgment.push(
      `Review mechanical scrub phrasing (${combinedMechanical.length} hits; ${mechanicalHigh} high)`
    );
  }
  if (presentationAudit.repeatedBoilerplate.length) {
    remainingJudgment.push("Diligence boilerplate repeats across multiple rows");
  }
  remainingJudgment.push("Founder visual review of gallery + property example image quality");
  remainingJudgment.push("Active profile approval is a separate later gate (not in v40B)");

  return {
    brandSlug,
    brandName: brandApi.name || V40B_BRAND_COPY_PROFILES[brandSlug]?.brandName || brandSlug,
    recordId: brandApi.id || config?.recordId || null,
    displayState: brandApi.brandExplorerDisplayState,
    shouldRenderFullProfile: brandApi.shouldRenderFullProfile === true,
    whatChanged: {
      v40ApplyExecuted: v40Apply?.applyExecuted === true,
      patchesPlanned: v40Brand?.patchPlan?.summary?.patchCount ?? null,
      recordsPatched: v40Brand?.applyResult?.recordsTouched ?? null,
      note: "v40 scrubbed Presentation Title/Body/Case Summary (+ External Display Status hides for duplicates only).",
    },
    forbiddenLanguage: {
      pass: forbiddenUnique.length === 0,
      presentationLayerPass: presentationForbiddenUnique.length === 0,
      hits: forbiddenUnique,
      presentationHits: presentationForbiddenUnique,
      presentationHitCount: presentationAudit.forbiddenHits.length,
      apiHitCount: apiForbidden.length,
      internalPreviewHitCount: internalForbidden.length,
      rendererOrBasicsOnlyLabels: rendererChromeHints,
      note:
        "Full internal-preview pass requires Presentation + renderer chrome clean. v40 scrubbed Presentation only; Brand Setup economics chrome may still inject FDD/LOI.",
    },
    mechanicalCopy: {
      hitCount: combinedMechanical.length,
      highSeverityCount: mechanicalHigh,
      hits: combinedMechanical.slice(0, 40),
      repeatedBoilerplate: presentationAudit.repeatedBoilerplate,
      vagueRowCount: presentationAudit.vagueRowCount,
    },
    brandCopyQuality: brandSignals,
    visuals: {
      galleryCount,
      galleryReady: galleryCount >= GALLERY_MIN,
      propertyExampleCount,
      propertyExamplesReady: propertyExampleCount >= PROPERTY_EXAMPLE_MIN,
    },
    externalDomLock: {
      note: "External render remains Profile in Preparation until active release — lock PASS only proves hidden, not owner-ready copy.",
      profileInPreparationRendered: externalQl.profileInPreparationRendered === true,
      forbiddenStringsFound: externalQl.forbiddenStringsFound,
      tabsRendered: (externalQl.tabsRenderedExternally || []).length,
      pass: externalQl.externalQualityLockPass === true || externalQl.profileInPreparationRendered === true,
    },
    internalPreview: {
      enabled: true,
      query: "?beInternalPreview=1",
      htmlLength: internalHtml.length,
      tabHint: (internalHtml.match(/data-atelier-panel="/g) || []).length,
      forbiddenPass: internalForbidden.length === 0,
      qualityLockNote:
        "Internal preview intentionally shows full profile; founder packet is based on this path, not external lock.",
      internalNotesFound: internalQl.internalNotesFound === true,
    },
    externalOwnerRule: {
      pass: ownerRule?.pass === true,
      blockers: ownerRule?.blockers || [],
    },
    presentation: {
      visibleRowCount: presentationAudit.visibleRowCount,
      vagueRowCount: presentationAudit.vagueRowCount,
      sampleRows: presentationAudit.rowAudits
        .filter((r) => r.forbidden.length || r.mechanical.length || r.vagueFields.length)
        .slice(0, 25),
    },
    remainingJudgmentItems: remainingJudgment,
    copyRisks: [
      ...forbiddenUnique.map((h) => `Forbidden: ${h.label}`),
      ...combinedMechanical
        .filter((h) => h.severity === "high" || h.severity === "medium")
        .slice(0, 8)
        .map((h) => `Mechanical (${h.severity}): ${h.note || h.id}`),
      ...(presentationAudit.vagueRowCount
        ? [`${presentationAudit.vagueRowCount} rows flagged vague/short after scrub`]
        : []),
    ],
    founderDecision,
    guardrails: {
      airtableWrites: false,
      activeProfileApproval: false,
      companyValidatedChanges: false,
      unlock: false,
    },
  };
}

export async function runV40BPostApplyCopyQuality({ brands = V40B_DEFAULT_BRANDS, dryRun = true } = {}) {
  if (!dryRun) {
    throw new Error("v40B is read-only. Use --dry-run only (no apply path).");
  }
  const brandResults = [];
  for (const brandSlug of brands) {
    brandResults.push(await auditBrandV40B(brandSlug));
  }

  const summary = {
    brandsAudited: brandResults.length,
    forbiddenCleanCount: brandResults.filter((b) => b.forbiddenLanguage.pass).length,
    founderVisualReviewReadyCount: brandResults.filter(
      (b) => b.founderDecision.decision === "founder_visual_review_ready"
    ).length,
    moreRemediationRequiredCount: brandResults.filter(
      (b) => b.founderDecision.decision === "more_remediation_required"
    ).length,
    notOwnerReadyCount: brandResults.filter((b) => b.founderDecision.decision === "not_owner_ready")
      .length,
    externalLockStillHidingCount: brandResults.filter((b) => b.externalDomLock.profileInPreparationRendered)
      .length,
  };

  return {
    version: V40B_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: true,
    brands,
    brandResults,
    summary,
    guardrails: {
      airtableWrites: false,
      activeProfileApproval: false,
      companyValidatedChanges: false,
      sourceLibraryChanges: false,
      registryChanges: false,
      imageFieldChanges: false,
      unlock: false,
    },
  };
}

function renderFounderPacketMarkdown(brand) {
  const lines = [
    `# Founder Review Packet — ${brand.brandName}`,
    "",
    `Slug: \`${brand.brandSlug}\` · Record: \`${brand.recordId || "n/a"}\``,
    `Generated: ${new Date().toISOString()} · v40B post-apply copy quality`,
    "",
    "> Inspect via internal preview (`?beInternalPreview=1`). External owners still see **Profile in Preparation** only.",
    "",
    "## Recommended decision",
    "",
    `**${brand.founderDecision.decision}**`,
    "",
    brand.founderDecision.rationale,
    "",
    "## What changed (v40 apply)",
    "",
    `- Apply executed (prior): ${brand.whatChanged.v40ApplyExecuted ? "yes" : "unknown / report missing"}`,
    `- Patches planned: ${brand.whatChanged.patchesPlanned ?? "n/a"}`,
    `- Records patched: ${brand.whatChanged.recordsPatched ?? "n/a"}`,
    `- ${brand.whatChanged.note}`,
    "",
    "## Visual readiness",
    "",
    `- Gallery imageUrl: **${brand.visuals.galleryCount}/${GALLERY_MIN}** (${brand.visuals.galleryReady ? "ready" : "short"})`,
    `- Property examples (footprint.openings): **${brand.visuals.propertyExampleCount}/${PROPERTY_EXAMPLE_MIN}** (${brand.visuals.propertyExamplesReady ? "ready" : "short"})`,
    "",
    "## Forbidden language",
    "",
    brand.forbiddenLanguage.pass
      ? "- **Pass** — no LOI / FDD / Item 19 / fee stack / net contribution / Sources / ADR / RevPAR / raw URLs detected in Presentation + API + internal preview text."
      : [
          `- **Fail** — hits: ${brand.forbiddenLanguage.hits.map((h) => h.label).join(", ")}`,
          `- Presentation layer clean: **${brand.forbiddenLanguage.presentationLayerPass ? "yes" : "no"}**`,
          brand.forbiddenLanguage.rendererOrBasicsOnlyLabels?.length
            ? `- Also in renderer/Brand Setup chrome (not Presentation-only): ${brand.forbiddenLanguage.rendererOrBasicsOnlyLabels.join(", ")}`
            : null,
          `- ${brand.forbiddenLanguage.note || ""}`,
        ]
          .filter(Boolean)
          .join("\n"),
    "",
    "## Mechanical copy risks",
    "",
  ];

  if (!brand.mechanicalCopy.hitCount) {
    lines.push("- No mechanical scrub patterns flagged.");
  } else {
    lines.push(`- Hits: ${brand.mechanicalCopy.hitCount} (high: ${brand.mechanicalCopy.highSeverityCount})`);
    lines.push(`- Vague rows: ${brand.mechanicalCopy.vagueRowCount}`);
    for (const h of brand.mechanicalCopy.hits.slice(0, 12)) {
      lines.push(`- \`${h.slotKey || h.source || "corpus"}\`: ${h.note || h.id}`);
    }
  }

  lines.push("", "## Brand-specific copy quality", "");
  for (const e of brand.brandCopyQuality.expected || []) {
    lines.push(`- Expected · ${e.label}: ${e.present ? "present" : "**missing**"}`);
  }
  for (const a of brand.brandCopyQuality.avoid || []) {
    lines.push(`- Avoid · ${a.label}: ${a.present ? "**still present**" : "clear"}`);
  }
  for (const o of brand.brandCopyQuality.optional || []) {
    lines.push(`- Optional · ${o.label}: ${o.present ? "present" : "absent"}`);
  }

  lines.push("", "## Remaining judgment items", "");
  for (const item of brand.remainingJudgmentItems) {
    lines.push(`- ${item}`);
  }

  lines.push("", "## Copy risks (summary)", "");
  if (!brand.copyRisks.length) {
    lines.push("- None flagged beyond normal founder taste pass.");
  } else {
    for (const r of brand.copyRisks) lines.push(`- ${r}`);
  }

  lines.push(
    "",
    "## External vs internal",
    "",
    `- External lock still hides full profile: **${brand.externalDomLock.profileInPreparationRendered ? "yes" : "no"}**`,
    `- Internal preview used for this packet: \`${brand.internalPreview.query}\``,
    "",
    "## Guardrails",
    "",
    "- No active-profile approval",
    "- No Company Validated changes",
    "- No unlock",
    ""
  );

  return lines.join("\n");
}

export function writeV40BReports(report, reportsDir = path.join(ROOT, "reports")) {
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, REPORT_JSON);
  const mdPath = path.join(reportsDir, REPORT_MD);
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2), "utf8");

  const md = [
    "# v40B Post-Apply Copy Quality Audit",
    "",
    `Generated: ${report.generatedAt}`,
    "",
    "Read-only. Founder packets use **internal preview** full profile. External DOM lock only confirms profiles remain hidden.",
    "",
    "## Summary",
    "",
    `- Brands: ${report.summary.brandsAudited}`,
    `- Forbidden language clean: ${report.summary.forbiddenCleanCount}/${report.summary.brandsAudited}`,
    `- founder_visual_review_ready: ${report.summary.founderVisualReviewReadyCount}`,
    `- more_remediation_required: ${report.summary.moreRemediationRequiredCount}`,
    `- not_owner_ready: ${report.summary.notOwnerReadyCount}`,
    `- Still externally locked (Profile in Preparation): ${report.summary.externalLockStillHidingCount}`,
    "",
  ];

  for (const b of report.brandResults) {
    md.push(`## ${b.brandSlug}`);
    md.push(`- decision: **${b.founderDecision.decision}**`);
    md.push(`- forbidden: ${b.forbiddenLanguage.pass ? "pass" : "fail"}`);
    md.push(
      `- gallery ${b.visuals.galleryCount}/6 · property examples ${b.visuals.propertyExampleCount}/3`
    );
    md.push(`- mechanical hits: ${b.mechanicalCopy.hitCount}`);
    md.push(`- rationale: ${b.founderDecision.rationale}`);
    md.push("");
  }

  md.push("## Guardrails");
  md.push("- No Airtable writes · no active approval · no Company Validated · no unlock");
  md.push("");

  fs.writeFileSync(mdPath, md.join("\n"), "utf8");

  const founderPaths = {};
  for (const b of report.brandResults) {
    const fname = `brand-explorer-v40b-founder-review-${b.brandSlug}.md`;
    const fpath = path.join(reportsDir, fname);
    fs.writeFileSync(fpath, renderFounderPacketMarkdown(b), "utf8");
    founderPaths[b.brandSlug] = fpath;
  }

  return { jsonPath, mdPath, founderPaths };
}
