/**
 * Final Public Restore Readiness Audit — read-only.
 *
 * Audits all remaining restore candidates (Lane 1 + Lane 2) before any
 * public restore is applied. Does not write Airtable, release fields,
 * Company Validated, Source Library, Registry, or intentional restore list.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { BUILT_BLOCKED_TARGETS, BUILT_BLOCKED_PROTECTED_PUBLIC_FULL, BUILT_BLOCKED_IDENTITIES } from "./brand-explorer-built-blocked-content.js";
import {
  FULL_BUILD_TRUE_INCOMPLETE_SLUGS,
  resolveFullBuildSlug,
  FULL_BUILD_IDENTITIES,
} from "./brand-explorer-full-build-content.js";
import { getActiveProfileBrandConfig } from "./brand-explorer-active-profile-brand-config.js";
import { evaluateTabFactoryFromPayload } from "./brand-explorer-tab-factory-evaluate.js";
import { renderBrandExplorerHtmlForTest } from "./brand-explorer-atelier-render-test-loader.js";
import { shouldRenderFullBrandExplorerProfile } from "./brand-explorer-display-state.js";
import {
  ACCIDENTAL_LEGACY_UNLOCK_HOLD_SLUGS,
  isLegacyVisibilityUnlockHeld,
  isIntentionalPublicRestoreSlug,
  readIntentionalPublicRestoreSlugs,
} from "./brand-explorer-public-restore-registry.js";
import { isOwnerFacingPresentationRow } from "./brand-explorer-public-visibility-quality-lock.js";
import {
  scanForbiddenLanguage,
  scanMechanicalCopy,
} from "./brand-explorer-v40b-copy-quality-patterns.js";
import { evaluateBrandExternalQualityLock } from "./brand-explorer-display-quality-lock.js";

export const READINESS_VERSION = "final-public-restore-readiness-v1";
export const REPORT_JSON = "brand-explorer-final-public-restore-readiness.json";
export const REPORT_MD = "brand-explorer-final-public-restore-readiness.md";
export const REPORT_LANE1_MD = "brand-explorer-final-public-restore-lane1.md";
export const REPORT_LANE2_MD = "brand-explorer-final-public-restore-lane2.md";

export const LANE1_RESTORE_CANDIDATES = Object.freeze([...BUILT_BLOCKED_TARGETS]);
export const LANE2_RESTORE_CANDIDATES = Object.freeze([...FULL_BUILD_TRUE_INCOMPLETE_SLUGS]);
export const ALL_RESTORE_CANDIDATES = Object.freeze([
  ...LANE1_RESTORE_CANDIDATES,
  ...LANE2_RESTORE_CANDIDATES,
]);

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const URL_ALLOWED_SLOTS = new Set(["footprint.momentum", "footprint.momentum_label", "materials.file"]);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function escapeFormulaValue(s) {
  return nz(s).replace(/'/g, "\\'");
}

export function resolveRestoreCandidateSlug(raw) {
  const key = nz(raw).toLowerCase();
  const aliases = {
    country: "country-inn-suites",
    "country-inn": "country-inn-suites",
    quality: "quality-inn",
    blu: "radisson-blu",
    red: "radisson-red",
    suburban: "suburban-studios",
    woodspring: "woodspring-suites",
    autograph: "autograph-collection",
    handwritten: "handwritten-collection",
    tapestry: "tapestry-collection-by-hilton",
    vignette: "vignette-collection",
    ascend: "ascend",
    comfort: "comfort-inn-suites",
    curio: "curio-collection",
    indigo: "hotel-indigo",
    kimpton: "kimpton",
    mgallery: "mgallery-collection",
    tribute: "tribute-portfolio",
    slh: "small-luxury-hotels-of-the-world",
  };
  if (aliases[key]) return aliases[key];
  return resolveFullBuildSlug(key);
}

export function resolveRestoreCandidateList(rawList) {
  if (!rawList?.length) return [...ALL_RESTORE_CANDIDATES];
  return [...new Set(rawList.map(resolveRestoreCandidateSlug))];
}

function laneFor(slug) {
  if (LANE1_RESTORE_CANDIDATES.includes(slug)) return "lane1";
  if (LANE2_RESTORE_CANDIDATES.includes(slug)) return "lane2";
  if (BUILT_BLOCKED_PROTECTED_PUBLIC_FULL.includes(slug)) return "protected_public_full";
  return "other";
}

async function fetchBrandApi(slug) {
  const { getBrandLibraryBrandById } = await import("../../api/brand-library.js");
  const identity =
    getActiveProfileBrandConfig(slug) ||
    BUILT_BLOCKED_IDENTITIES[slug] ||
    FULL_BUILD_IDENTITIES[slug] ||
    null;
  const lookupId = identity?.recordId || slug;
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
    },
  };
  await getBrandLibraryBrandById({ query: { brandId: lookupId }, headers: {} }, res);
  if (!res.payload?.brand) {
    // Fallback: try slug if recordId path failed
    if (lookupId !== slug) {
      await getBrandLibraryBrandById({ query: { brandId: slug }, headers: {} }, res);
    }
  }
  if (!res.payload?.brand) throw new Error(`Brand fetch failed for ${slug} (${lookupId})`);
  return res.payload.brand;
}

async function listPresentationRows(brandName) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY required");
  const formula = `{Brand Name}='${escapeFormulaValue(brandName)}'`;
  const rows = [];
  let offset = "";
  do {
    const params = new URLSearchParams({ pageSize: "100", filterByFormula: formula });
    if (offset) params.set("offset", offset);
    const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}?${params}`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(json.error?.message || `Presentation list failed: ${res.status}`);
    for (const rec of json.records || []) {
      const f = rec.fields || {};
      const image = f.Image;
      rows.push({
        recordId: rec.id,
        slotKey: nz(f["Slot Key"]),
        title: nz(f.Title),
        body: nz(f.Body),
        caseSummaryOverview: nz(f["Case Summary Overview"]),
        caseSummaryBrandRelevance: nz(f["Case Summary Brand Relevance"]),
        caseSummaryOwnerObjective: nz(f["Case Summary Owner Objective"]),
        caseSummaryInterpretation: nz(f["Case Summary Interpretation"]),
        caseSummaryTags: nz(f["Case Summary Tags"]),
        externalDisplayStatus: nz(f["External Display Status"]),
        active: f.Active !== false,
        visible: f.Visible !== false,
        imageUrl: Array.isArray(image) && image[0]?.url ? nz(image[0].url) : "",
        sortOrder: f["Sort Order"] || 0,
      });
    }
    offset = json.offset || "";
  } while (offset);
  return rows;
}

function stripRenderedOwnerText(html = "") {
  return String(html || "")
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    // Linked announcement / property URLs in markup are allowed; strip before raw_url scan.
    .replace(/https?:\/\/\S+/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function ownerFacingCorpus(rows = []) {
  return (rows || [])
    .filter(isOwnerFacingPresentationRow)
    .filter((r) => !URL_ALLOWED_SLOTS.has(nz(r.slotKey)))
    .filter((r) => !/^footprint\.openings$/i.test(nz(r.slotKey)))
    .map((r) =>
      [r.title, r.body, r.caseSummaryOverview, r.caseSummaryBrandRelevance, r.caseSummaryOwnerObjective]
        .map(nz)
        .filter(Boolean)
        .join("\n")
    )
    .filter(Boolean)
    .join("\n\n");
}

function basicsCorpus(brand = {}) {
  return [
    brand.brandPositioning,
    brand.guestPsychographics,
    brand.brandValueProposition,
    brand.keyBrandDifferentiators,
    brand.brandCustomerPromise,
    brand.brandTaglineMotto,
  ]
    .map(nz)
    .filter(Boolean)
    .join("\n");
}

function deriveFounderRecommendation({
  gateSuitePass,
  visibilityOk,
  accidentalUnlock,
  copyOk,
}) {
  if (accidentalUnlock) return "remediation_required";
  if (!gateSuitePass || !visibilityOk || !copyOk) return "approve_after_minor_cleanup";
  return "approve_for_active_release";
}

export async function auditBrandFinalPublicRestoreReadiness(brandSlug, { intentionalSlugs = null } = {}) {
  const slug = resolveRestoreCandidateSlug(brandSlug);
  const lane = laneFor(slug);

  if (lane === "protected_public_full") {
    return {
      brandSlug: slug,
      lane,
      refused: true,
      reason: "protected_public_full_baseline_untouched",
      recommendation: "n/a_protected_baseline",
      readyForPublicRestore: false,
      gates: {},
    };
  }

  const intentional = intentionalSlugs || readIntentionalPublicRestoreSlugs();
  const identity =
    getActiveProfileBrandConfig(slug) ||
    FULL_BUILD_IDENTITIES[slug] ||
    null;
  const brandName = identity?.name || slug;

  const brand = await fetchBrandApi(slug);
  const rows = await listPresentationRows(brand.name || brandName);
  const ownerRows = rows.filter(isOwnerFacingPresentationRow);
  const brandConfig = getActiveProfileBrandConfig(slug);

  // Match official gate suite render mode (internalPreview=false).
  const html = renderBrandExplorerHtmlForTest(brand, {
    allPanels: true,
    internalPreview: false,
  });

  const tabFactory = evaluateTabFactoryFromPayload({
    brand,
    rows: ownerRows,
    html,
    brandSlug: slug,
    brandConfig,
    registryAssets: [],
  });

  const shouldRenderFull = shouldRenderFullBrandExplorerProfile(brand) === true;
  const heldByAccidentalList = isLegacyVisibilityUnlockHeld(slug, { intentionalSlugs: intentional });
  const intentionalRestore = isIntentionalPublicRestoreSlug(slug, { intentionalSlugs: intentional });
  const onHoldList = ACCIDENTAL_LEGACY_UNLOCK_HOLD_SLUGS.includes(slug);

  // Accidental unlock = public-full without intentional restore listing while on hold list
  const accidentalUnlock =
    shouldRenderFull === true && onHoldList && intentionalRestore !== true;

  const publicVisibility =
    intentionalRestore || (shouldRenderFull && !onHoldList)
      ? "already_public_or_intentional"
      : heldByAccidentalList || !shouldRenderFull
        ? "held_from_public"
        : "unknown";

  const visibilityOk =
    publicVisibility === "held_from_public" || publicVisibility === "already_public_or_intentional";

  // Owner-facing copy = what the renderer surfaces (plus Basics chips).
  // Residual Airtable economics rows with FDD/LOI that do not render should not block.
  const renderedText = stripRenderedOwnerText(html);
  const corpus = `${renderedText}\n${basicsCorpus(brand)}`;
  const forbidden = scanForbiddenLanguage(corpus);
  const mechanical = scanMechanicalCopy(corpus).filter((h) => ["high", "medium"].includes(h.severity));
  const externalQl = evaluateBrandExternalQualityLock(brand, html, {
    brandSlug: slug,
    presentationRows: ownerRows,
  });
  const airtableResidualForbidden = scanForbiddenLanguage(ownerFacingCorpus(ownerRows));

  const gates = {
    tab_factory: tabFactory.auditPass === true,
    rendered_field_completeness: tabFactory.gates?.rendered_field_completeness === true,
    no_empty_rendered_components: tabFactory.gates?.no_empty_rendered_components === true,
    source_provenance_by_tab: tabFactory.gates?.source_provenance_by_tab === true,
    image_uniqueness: tabFactory.gates?.image_distinctiveness === true,
    image_role_match: tabFactory.gates?.image_role_match === true,
    section_pattern_parity: tabFactory.gates?.section_pattern_parity === true,
    golden_content_quality: tabFactory.gates?.golden_content_quality === true,
  };

  const gateSuitePass = Object.values(gates).every((v) => v === true);
  const copyOk = forbidden.length === 0 && mechanical.length === 0;

  const recommendation = deriveFounderRecommendation({
    gateSuitePass,
    visibilityOk,
    accidentalUnlock,
    copyOk,
  });

  const companyValidated = brand.companyValidated === true || brand.companyValidated === "Yes";
  const companyValidationDate = brand.companyValidationDate || brand.companyValidatedDate || null;

  const failures = [];
  for (const [k, v] of Object.entries(gates)) {
    if (!v) failures.push(`gate_fail:${k}`);
  }
  if (accidentalUnlock) failures.push("accidental_public_full_unlock");
  if (!visibilityOk) failures.push("visibility_state_unclear");
  if (forbidden.length) failures.push(...forbidden.map((f) => `forbidden:${f.id}`));
  if (mechanical.length) failures.push(...mechanical.map((f) => `mechanical:${f.id}`));
  if ((tabFactory.golden?.failures || []).length) {
    failures.push(...tabFactory.golden.failures.slice(0, 8).map((f) => `golden:${f}`));
  }

  return {
    brandSlug: slug,
    brandName: brand.name || brandName,
    recordId: brand.id || identity?.recordId || null,
    lane,
    refused: false,
    recommendation,
    readyForPublicRestore: recommendation === "approve_for_active_release",
    gateSuitePass,
    gates,
    tabFactorySummary: {
      auditPass: tabFactory.auditPass,
      failFindings: tabFactory.failFindings,
      emptyRenderFailFindings: tabFactory.emptyRenderFailFindings,
      goldenFailures: tabFactory.golden?.failures || [],
    },
    visibility: {
      publicVisibility,
      shouldRenderFullProfile: shouldRenderFull,
      onAccidentalHoldList: onHoldList,
      heldByAccidentalList,
      intentionalRestore,
      accidentalUnlock,
      visibilityOk,
    },
    companyValidatedSnapshot: {
      companyValidated,
      companyValidationDate,
      untouchedByThisAudit: true,
    },
    copyQuality: {
      pass: copyOk,
      forbiddenHits: forbidden.slice(0, 12),
      mechanicalHits: mechanical.slice(0, 12),
      airtableResidualForbiddenCount: airtableResidualForbidden.length,
      airtableResidualForbiddenSample: airtableResidualForbidden.slice(0, 6),
      rawUrlsExcludedSlots: [...URL_ALLOWED_SLOTS],
      scanBasis: "rendered_html_plus_basics_chips",
      externalQualityLockPass: externalQl?.pass !== false,
    },
    failures,
    guardrails: {
      publicRestoreApplied: false,
      releaseFieldsWritten: false,
      companyValidatedChanged: false,
      sourceLibraryChanged: false,
      registryChanged: false,
      protectedBaselineUntouched: true,
    },
  };
}

export async function runFinalPublicRestoreReadiness({
  brands = [...ALL_RESTORE_CANDIDATES],
  dryRun = true,
  reportsDir = path.join(ROOT, "reports"),
} = {}) {
  const resolved = resolveRestoreCandidateList(brands);
  const intentional = readIntentionalPublicRestoreSlugs();
  const brandResults = [];

  for (const slug of resolved) {
    try {
      brandResults.push(await auditBrandFinalPublicRestoreReadiness(slug, { intentionalSlugs: intentional }));
    } catch (err) {
      brandResults.push({
        brandSlug: slug,
        lane: laneFor(slug),
        refused: false,
        recommendation: "remediation_required",
        readyForPublicRestore: false,
        gateSuitePass: false,
        gates: {},
        failures: [`audit_error:${err.message}`],
        error: err.message,
      });
    }
  }

  const lane1 = brandResults.filter((b) => b.lane === "lane1");
  const lane2 = brandResults.filter((b) => b.lane === "lane2");
  const ready = brandResults.filter((b) => b.readyForPublicRestore);
  const notReady = brandResults.filter((b) => !b.readyForPublicRestore && !b.refused);

  const result = {
    version: READINESS_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: dryRun !== false,
    mode: "read_only",
    brands: resolved,
    intentionalRestoreSlugs: intentional,
    brandResults,
    lane1,
    lane2,
    summary: {
      brandCount: brandResults.length,
      readyCount: ready.length,
      notReadyCount: notReady.length,
      lane1Ready: lane1.filter((b) => b.readyForPublicRestore).length,
      lane2Ready: lane2.filter((b) => b.readyForPublicRestore).length,
      allApproveForActiveRelease: brandResults.every(
        (b) => b.refused || b.recommendation === "approve_for_active_release"
      ),
      allGateSuitePass: brandResults.every((b) => b.refused || b.gateSuitePass === true),
      anyAccidentalUnlock: brandResults.some((b) => b.visibility?.accidentalUnlock === true),
      allVisibilityHeldOrIntentional: brandResults.every(
        (b) => b.refused || b.visibility?.visibilityOk === true
      ),
      protectedBaselineUntouched: true,
      publicRestoreApplied: false,
    },
    acceptance: {
      allApproveForActiveRelease: null, // filled below
      allGateSuitePass: null,
      allHeldOrAlreadyPublic: null,
      noAccidentalUnlock: null,
      protectedBaselineUntouched: true,
      pass: false,
    },
  };

  result.acceptance.allApproveForActiveRelease = result.summary.allApproveForActiveRelease;
  result.acceptance.allGateSuitePass = result.summary.allGateSuitePass;
  result.acceptance.allHeldOrAlreadyPublic = result.summary.allVisibilityHeldOrIntentional;
  result.acceptance.noAccidentalUnlock = !result.summary.anyAccidentalUnlock;
  result.acceptance.pass =
    result.acceptance.allApproveForActiveRelease &&
    result.acceptance.allGateSuitePass &&
    result.acceptance.allHeldOrAlreadyPublic &&
    result.acceptance.noAccidentalUnlock &&
    result.acceptance.protectedBaselineUntouched;

  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, REPORT_JSON);
  fs.writeFileSync(jsonPath, `${JSON.stringify(result, null, 2)}\n`, "utf8");

  const mdLines = [
    `# Brand Explorer — Final Public Restore Readiness`,
    ``,
    `- Generated: ${result.generatedAt}`,
    `- Mode: **read-only** (no public restore / release / CV / Source / Registry writes)`,
    `- Brands: ${result.brands.length}`,
    `- Ready: **${result.summary.readyCount}/${result.summary.brandCount}**`,
    `- Acceptance pass: **${result.acceptance.pass}**`,
    ``,
    `## Acceptance`,
    ``,
    `- All \`approve_for_active_release\`: **${result.acceptance.allApproveForActiveRelease}**`,
    `- All gate suite pass: **${result.acceptance.allGateSuitePass}**`,
    `- All held or already public: **${result.acceptance.allHeldOrAlreadyPublic}**`,
    `- No accidental unlock: **${result.acceptance.noAccidentalUnlock}**`,
    `- Protected public-full baseline untouched: **${result.acceptance.protectedBaselineUntouched}**`,
    ``,
    `## Per brand`,
    ``,
    `| Brand | Lane | Recommendation | Gates | Visibility | Failures |`,
    `| --- | --- | --- | --- | --- | --- |`,
    ...brandResults.map((b) => {
      const fail = (b.failures || []).slice(0, 4).join("; ") || "—";
      return `| ${b.brandName || b.brandSlug} | ${b.lane} | \`${b.recommendation}\` | ${
        b.gateSuitePass ? "PASS" : "FAIL"
      } | ${b.visibility?.publicVisibility || "—"} | ${fail} |`;
    }),
    ``,
    `## Next step`,
    ``,
    `Only after founder approval, run public restore governance with explicit confirm flags.`,
    `This readiness audit does **not** apply restore.`,
    ``,
  ];
  const mdPath = path.join(reportsDir, REPORT_MD);
  fs.writeFileSync(mdPath, `${mdLines.join("\n")}\n`, "utf8");

  const writeLaneMd = (laneResults, fileName, title) => {
    const lines = [
      `# ${title}`,
      ``,
      `- Generated: ${result.generatedAt}`,
      `- Brands: ${laneResults.length}`,
      `- Ready: ${laneResults.filter((b) => b.readyForPublicRestore).length}/${laneResults.length}`,
      ``,
      ...laneResults.map((b) => {
        const gateLines = Object.entries(b.gates || {})
          .map(([k, v]) => `  - ${k}: **${v}**`)
          .join("\n");
        return [
          `## ${b.brandName || b.brandSlug}`,
          ``,
          `- Slug: \`${b.brandSlug}\``,
          `- Recommendation: **${b.recommendation}**`,
          `- Gate suite: **${b.gateSuitePass}**`,
          `- Visibility: **${b.visibility?.publicVisibility}** (shouldRenderFull=${b.visibility?.shouldRenderFullProfile})`,
          `- Accidental unlock: **${b.visibility?.accidentalUnlock}**`,
          `- Company Validated (snapshot): **${b.companyValidatedSnapshot?.companyValidated}** (untouched=${b.companyValidatedSnapshot?.untouchedByThisAudit})`,
          `- Failures: ${(b.failures || []).join(", ") || "none"}`,
          ``,
          `### Gates`,
          ``,
          gateLines || "  - (none)",
          ``,
        ].join("\n");
      }),
    ];
    const p = path.join(reportsDir, fileName);
    fs.writeFileSync(p, `${lines.join("\n")}\n`, "utf8");
    return p;
  };

  const lane1Path = writeLaneMd(lane1, REPORT_LANE1_MD, "Final Public Restore Readiness — Lane 1");
  const lane2Path = writeLaneMd(lane2, REPORT_LANE2_MD, "Final Public Restore Readiness — Lane 2");

  return {
    ...result,
    paths: { jsonPath, mdPath, lane1Path, lane2Path },
  };
}

export default {
  runFinalPublicRestoreReadiness,
  auditBrandFinalPublicRestoreReadiness,
  ALL_RESTORE_CANDIDATES,
};
