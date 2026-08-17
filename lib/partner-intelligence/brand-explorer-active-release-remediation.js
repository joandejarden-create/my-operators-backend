/**
 * v40 — Brand Explorer Batch Active Release Remediation (dry-run).
 *
 * Produces owner-copy scrub patch plans for release candidates.
 * Does not unlock, approve, or write Airtable by default.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getActiveProfileBrandConfig } from "./brand-explorer-active-profile-brand-config.js";
import { getDiscoveryBrandConfig } from "./brand-explorer-brand-asset-image-governance.js";
import { loadBrandFactoryContext } from "./brand-explorer-active-profile-factory.js";
import { evaluateExternalOwnerReadinessRule } from "./brand-explorer-external-owner-readiness-rules.js";
import { auditExternalOwnerPhrase } from "./brand-explorer-external-owner-content-governance.js";
import { evaluateBrandExternalQualityLock } from "./brand-explorer-display-quality-lock.js";
import { renderBrandExplorerHtmlForTest } from "./brand-explorer-atelier-render-test-loader.js";
import { scrubPresentationRow, scrubOwnerFacingCopy } from "./brand-explorer-owner-copy-scrubber.js";
import {
  buildReleaseRemediationPatchPlan,
  renderBrandRemediationMarkdown,
  V40_PATCH_PLAN_VERSION,
  PRESENTATION_TABLE,
} from "./brand-explorer-release-remediation-patch-plan.js";

export const V40_REMEDIATION_VERSION = "v40";
export const REPORT_JSON = "brand-explorer-v40-active-release-remediation.json";
export const REPORT_MD = "brand-explorer-v40-active-release-remediation.md";

export const DEFAULT_RELEASE_CANDIDATES = Object.freeze([
  "everhome-suites",
  "kimpton",
  "radisson-individuals-by-choice",
]);

export const DEFAULT_INCOMPLETE_CONTROL = Object.freeze([
  "hotel-indigo",
  "mgallery-collection",
  "design-hotels",
  "small-luxury-hotels-of-the-world",
]);

export const APPLY_FLAGS = Object.freeze({
  approve: "--approve-brand-explorer-v40-active-release-remediation",
  noCompanyValidation: "--confirm-no-company-validation-claim",
  noActiveApproval: "--confirm-no-active-profile-approval",
  noSourceLibrary: "--confirm-no-source-library-changes",
  noRegistry: "--confirm-no-registry-changes",
  copyClean: "--confirm-external-owner-copy-clean",
  brandOnly: "--confirm-brand-only",
});

const ALLOWED_PRESENTATION_FIELDS = new Set([
  "Title",
  "Body",
  "Case Summary Overview",
  "Case Summary Brand Relevance",
  "Case Summary Owner Objective",
  "Case Summary Interpretation",
  "Case Summary Tags",
  "External Display Status",
]);

const FORBIDDEN_WRITE_FIELDS = new Set([
  "Image",
  "Images",
  "Scenario Image",
  "Attachments",
  "Company Validated",
  "Company Validation Date",
  "Ready for Active Profile",
  "Active Profile Approved",
  "Active Profile Approved Date",
  "Founder Visual Review Pass",
  "Summary URL",
  "View Summary URL",
]);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function readJsonIfExists(p) {
  if (!fs.existsSync(p)) return null;
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return null;
  }
}

function resolveConfig(slug) {
  return getActiveProfileBrandConfig(slug) || getDiscoveryBrandConfig(slug) || null;
}

/**
 * Parse CLI flags for gated apply.
 */
export function parseV40ApplyFlags(argv = []) {
  const has = (flag) => argv.includes(flag);
  return {
    apply: has("--apply"),
    approve: has(APPLY_FLAGS.approve),
    noCompanyValidation: has(APPLY_FLAGS.noCompanyValidation),
    noActiveApproval: has(APPLY_FLAGS.noActiveApproval),
    noSourceLibrary: has(APPLY_FLAGS.noSourceLibrary),
    noRegistry: has(APPLY_FLAGS.noRegistry),
    copyClean: has(APPLY_FLAGS.copyClean),
    brandOnly: has(APPLY_FLAGS.brandOnly),
  };
}

export function validateV40ApplyGates({ flags, brands, brandResults } = {}) {
  const blockers = [];
  if (!flags?.apply) return { allowed: false, blockers: ["not_apply_mode"], missingFlags: [] };

  const required = [
    ["approve", APPLY_FLAGS.approve],
    ["noCompanyValidation", APPLY_FLAGS.noCompanyValidation],
    ["noActiveApproval", APPLY_FLAGS.noActiveApproval],
    ["noSourceLibrary", APPLY_FLAGS.noSourceLibrary],
    ["noRegistry", APPLY_FLAGS.noRegistry],
    ["copyClean", APPLY_FLAGS.copyClean],
    ["brandOnly", APPLY_FLAGS.brandOnly],
  ];
  const missingFlags = required.filter(([k]) => !flags[k]).map(([, flag]) => flag);
  if (missingFlags.length) blockers.push(...missingFlags.map((f) => `missing_flag:${f}`));

  if (flags.brandOnly) {
    const illegal = (brands || []).filter((s) => !DEFAULT_RELEASE_CANDIDATES.includes(s));
    if (illegal.length) blockers.push(`brand_only_violation:${illegal.join(",")}`);
  }

  for (const b of brandResults || []) {
    if (!b.projection?.ownerCopyBlockersProjectedZero) {
      blockers.push(`owner_copy_not_projected_clean:${b.brandSlug}`);
    }
    const unsafe = (b.patchPlan?.patches || []).filter((p) => !p.safeForGenericApply);
    if (unsafe.length) blockers.push(`unsafe_patches:${b.brandSlug}:${unsafe.length}`);
    for (const p of b.patchPlan?.patches || []) {
      if (!ALLOWED_PRESENTATION_FIELDS.has(p.field)) {
        blockers.push(`forbidden_field:${b.brandSlug}:${p.field}`);
      }
      if (FORBIDDEN_WRITE_FIELDS.has(p.field)) {
        blockers.push(`blocked_field:${b.brandSlug}:${p.field}`);
      }
    }
  }

  return { allowed: blockers.length === 0, blockers, missingFlags };
}

function groupPatchesByRecord(patches = []) {
  const byId = new Map();
  for (const p of patches) {
    if (!p.recordId || !ALLOWED_PRESENTATION_FIELDS.has(p.field)) continue;
    if (!byId.has(p.recordId)) {
      byId.set(p.recordId, { recordId: p.recordId, slotKey: p.slotKey, fields: {} });
    }
    byId.get(p.recordId).fields[p.field] = p.after;
  }
  return [...byId.values()];
}

async function airtablePatchPresentation(baseId, apiKey, recordId, fields) {
  const safe = {};
  for (const [k, v] of Object.entries(fields || {})) {
    if (!ALLOWED_PRESENTATION_FIELDS.has(k)) continue;
    if (FORBIDDEN_WRITE_FIELDS.has(k)) continue;
    safe[k] = v;
  }
  if (!Object.keys(safe).length) return { skipped: true, recordId };
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}/${recordId}`;
  const res = await fetch(url, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields: safe, typecast: true }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `Airtable PATCH failed: ${res.status}`);
  return json;
}

async function applyPresentationPatchesForBrand(brandResult) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const grouped = groupPatchesByRecord(brandResult.patchPlan?.patches || []);
  const patched = [];
  const errors = [];
  for (const g of grouped) {
    try {
      await airtablePatchPresentation(baseId, apiKey, g.recordId, g.fields);
      patched.push({
        recordId: g.recordId,
        slotKey: g.slotKey,
        fields: Object.keys(g.fields),
      });
    } catch (err) {
      errors.push({ recordId: g.recordId, slotKey: g.slotKey, message: err.message });
    }
  }
  return { patched, errors, recordsTouched: patched.length };
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
    throw new Error(`brand API fetch failed for ${slug}`);
  }
  return res.payload.brand;
}

function loadV39Brand(slug) {
  const j = readJsonIfExists(path.join(ROOT, "reports", "brand-explorer-v39-active-release-audit.json"));
  return (j?.brandResults || []).find((b) => b.brandSlug === slug) || null;
}

function countVisibleUrls(rows) {
  let n = 0;
  for (const r of rows || []) {
    if (r.visible === false) continue;
    if (r.slotKey === "footprint.openings" || r.slotKey === "footprint.momentum") continue;
    const hits = auditExternalOwnerPhrase(`${r.title}\n${r.body}`, r.slotKey);
    if (hits.some((h) => h.patternId === "http_url")) n += 1;
  }
  return n;
}

function countInternalNotes(rows) {
  let n = 0;
  for (const r of rows || []) {
    if (r.visible === false) continue;
    const blob = `${r.title}\n${r.body}`;
    if (/output note|internal review|supports internal review|staging/i.test(blob)) n += 1;
  }
  return n;
}

/**
 * Project post-scrub presentation rows (in-memory only).
 */
function projectScrubbedRows(presentationRows, brandSlug) {
  return (presentationRows || []).map((row) => {
    if (row.visible === false) return { ...row };
    const scrub = scrubPresentationRow(row, { brandSlug });
    if (!scrub.changed) return { ...row };
    return {
      ...row,
      title: scrub.fields.Title != null ? scrub.fields.Title : row.title,
      body: scrub.fields.Body != null ? scrub.fields.Body : row.body,
      caseSummaryOverview:
        scrub.fields["Case Summary Overview"] != null
          ? scrub.fields["Case Summary Overview"]
          : row.caseSummaryOverview,
      caseSummaryBrandRelevance:
        scrub.fields["Case Summary Brand Relevance"] != null
          ? scrub.fields["Case Summary Brand Relevance"]
          : row.caseSummaryBrandRelevance,
      caseSummaryOwnerObjective:
        scrub.fields["Case Summary Owner Objective"] != null
          ? scrub.fields["Case Summary Owner Objective"]
          : row.caseSummaryOwnerObjective,
      caseSummaryInterpretation:
        scrub.fields["Case Summary Interpretation"] != null
          ? scrub.fields["Case Summary Interpretation"]
          : row.caseSummaryInterpretation,
      caseSummaryTags:
        scrub.fields["Case Summary Tags"] != null ? scrub.fields["Case Summary Tags"] : row.caseSummaryTags,
    };
  });
}

function projectDomQuality({ brandSlug, brandApi, presentationRows, v39 }) {
  const beforeRule = evaluateExternalOwnerReadinessRule(presentationRows || []);
  const projectedRows = projectScrubbedRows(presentationRows, brandSlug);
  const afterRule = evaluateExternalOwnerReadinessRule(projectedRows);

  const urlsBefore = countVisibleUrls(presentationRows);
  const urlsAfter = countVisibleUrls(projectedRows);
  const notesBefore = countInternalNotes(presentationRows);
  const notesAfter = countInternalNotes(projectedRows);

  // Live DOM while locked: quality lock should still pass (prep only)
  let liveDom = null;
  try {
    const html = renderBrandExplorerHtmlForTest(brandApi, { allPanels: true });
    liveDom = evaluateBrandExternalQualityLock(brandApi, html, { brandSlug });
  } catch (err) {
    liveDom = { externalQualityLockPass: false, error: err.message };
  }

  const forbiddenBefore = (beforeRule.blockers || []).length;
  const forbiddenAfter = (afterRule.blockers || []).length;

  return {
    forbiddenStringsBefore: forbiddenBefore,
    forbiddenStringsAfter: forbiddenAfter,
    visibleUrlsBefore: urlsBefore,
    visibleUrlsAfter: urlsAfter,
    internalNotesBefore: notesBefore,
    internalNotesAfter: notesAfter,
    emptyCardsBefore: beforeRule.emptyCardCount || 0,
    emptyCardsAfter: afterRule.emptyCardCount || 0,
    externalOwnerRulePassBefore: beforeRule.pass === true,
    externalOwnerRulePassAfter: afterRule.pass === true,
    liveDomQualityLockPass: liveDom?.externalQualityLockPass === true,
    expectedDisplayStateAfter: "draft_applied_with_defects",
    stillBlockedByFounderReview: true,
    stillBlockedByActiveApproval: true,
    unlockInV40: false,
    ownerCopyBlockersProjectedZero: afterRule.pass === true,
    afterBlockers: afterRule.blockers || [],
  };
}

export function buildActiveReleaseRemediationApplyDesign(brandSlugs = []) {
  return {
    command: [
      "npm run brand-explorer-v40-active-release-remediation --",
      `--brands ${(brandSlugs || []).join(",") || "<slugs>"}`,
      "--apply",
      APPLY_FLAGS.approve,
      APPLY_FLAGS.noCompanyValidation,
      APPLY_FLAGS.noActiveApproval,
      APPLY_FLAGS.noSourceLibrary,
      APPLY_FLAGS.noRegistry,
      APPLY_FLAGS.copyClean,
      APPLY_FLAGS.brandOnly,
    ].join(" "),
    allowedWrites: ["Brand Explorer Presentation Title/Body/Case Summary fields", "External Display Status (hide duplicates only)"],
    forbiddenWrites: [
      "Company Validated",
      "Ready for Active Profile",
      "Active Profile Approved",
      "Source Library",
      "Brand Asset Registry",
      "Image / Scenario Image fields",
    ],
    status: "designed_not_executed",
  };
}

function runIncompleteControlCheck(controlSlugs = DEFAULT_INCOMPLETE_CONTROL) {
  return (async () => {
    const rows = [];
    for (const slug of controlSlugs) {
      const brandApi = await fetchBrandApiShape(slug);
      const html = renderBrandExplorerHtmlForTest(brandApi, { allPanels: true });
      const ql = evaluateBrandExternalQualityLock(brandApi, html, { brandSlug: slug });
      const locked =
        brandApi.shouldRenderFullProfile !== true &&
        !["external_owner_ready", "active_profile_ready"].includes(brandApi.brandExplorerDisplayState);
      rows.push({
        brandSlug: slug,
        shouldRenderFullProfile: brandApi.shouldRenderFullProfile === true,
        displayState: brandApi.brandExplorerDisplayState,
        profileInPreparation: ql.profileInPreparationRendered === true,
        forbiddenStringsFound: ql.forbiddenStringsFound,
        externalQualityLockPass: ql.externalQualityLockPass,
        noActiveApprovalPath: true,
        controlPass:
          brandApi.shouldRenderFullProfile !== true &&
          locked &&
          ql.externalQualityLockPass === true &&
          (ql.forbiddenStringsFound || 0) === 0,
      });
    }
    return {
      allControlPass: rows.every((r) => r.controlPass),
      rows,
    };
  })();
}

export async function runV40ActiveReleaseRemediation({
  brands = DEFAULT_RELEASE_CANDIDATES,
  dryRun = true,
  apply = false,
  flags = null,
} = {}) {
  const resolvedFlags = flags || {
    apply: false,
    approve: false,
    noCompanyValidation: false,
    noActiveApproval: false,
    noSourceLibrary: false,
    noRegistry: false,
    copyClean: false,
    brandOnly: false,
  };

  const brandResults = [];
  for (const brandSlug of brands) {
    const config = resolveConfig(brandSlug);
    const ctx = await loadBrandFactoryContext(brandSlug).catch(() => null);
    const brandApi = await fetchBrandApiShape(brandSlug);
    const v39 = loadV39Brand(brandSlug);
    const presentationRows = ctx?.presentationRows || brandApi?.brandExplorer?.blocks || [];
    const v39FailedGates = v39?.gateInventory?.failedGates || [];

    const patchPlan = buildReleaseRemediationPatchPlan({
      brandSlug,
      presentationRows,
      v39FailedGates,
    });

    const projection = projectDomQuality({
      brandSlug,
      brandApi,
      presentationRows,
      v39,
    });

    brandResults.push({
      brandSlug,
      recordId: brandApi.id || config?.recordId,
      brandName: brandApi.name,
      dryRun: !apply,
      displayState: brandApi.brandExplorerDisplayState,
      shouldRenderFullProfile: brandApi.shouldRenderFullProfile === true,
      v39Outcome: v39?.classification?.outcome || null,
      v39FailedGates,
      patchPlan,
      projection,
      unlockInV40: false,
      readyForActiveApproval: false,
      companyValidatedUntouched: true,
      applyResult: null,
    });
  }

  const incompleteControl = await runIncompleteControlCheck();
  const gateCheck = validateV40ApplyGates({
    flags: { ...resolvedFlags, apply },
    brands,
    brandResults,
  });

  let applyExecuted = false;
  let applyBlocked = false;
  if (apply) {
    if (!gateCheck.allowed) {
      applyBlocked = true;
    } else {
      for (const b of brandResults) {
        b.applyResult = await applyPresentationPatchesForBrand(b);
      }
      applyExecuted = true;
    }
  }

  return {
    version: V40_REMEDIATION_VERSION,
    patchPlanVersion: V40_PATCH_PLAN_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: !applyExecuted,
    applyRequested: Boolean(apply),
    applyExecuted,
    applyBlocked,
    applyGateCheck: gateCheck,
    brands,
    brandResults,
    incompleteControl,
    applyDesign: buildActiveReleaseRemediationApplyDesign(brands),
    summary: {
      brandsRemediated: brandResults.length,
      totalPatches: brandResults.reduce((n, b) => n + (b.patchPlan.summary.patchCount || 0), 0),
      totalBlockers: brandResults.reduce((n, b) => n + (b.patchPlan.summary.blockerCount || 0), 0),
      ownerCopyProjectedCleanCount: brandResults.filter((b) => b.projection.ownerCopyBlockersProjectedZero)
        .length,
      recordsPatched: brandResults.reduce((n, b) => n + (b.applyResult?.recordsTouched || 0), 0),
      applyErrors: brandResults.reduce((n, b) => n + (b.applyResult?.errors?.length || 0), 0),
      incompleteControlPass: incompleteControl.allControlPass,
      anyUnlock: false,
      activeApprovalTouched: false,
      companyValidatedTouched: false,
    },
    guardrails: {
      airtableWrites: applyExecuted,
      presentationWrites: applyExecuted,
      registryWrites: false,
      sourceLibraryWrites: false,
      imageFieldWrites: false,
      companyValidatedChanges: false,
      activeProfileApproval: false,
      activeReleaseApply: false,
    },
  };
}

export function writeV40Reports(report, reportsDir = path.join(ROOT, "reports")) {
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, REPORT_JSON);
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));

  const mdLines = [
    "# v40 Brand Explorer Active Release Remediation",
    "",
    `Generated: ${report.generatedAt}`,
    "",
    report.applyExecuted
      ? "Apply executed: Presentation owner-copy patches only. No unlock / no active approval / no Company Validated."
      : report.applyBlocked
        ? `Apply blocked. Gate failures: ${(report.applyGateCheck?.blockers || []).join(", ")}`
        : "Dry-run only. No Airtable writes. No unlock.",
    "",
    "## Summary",
    `- Brands: ${report.summary.brandsRemediated}`,
    `- Total blockers: ${report.summary.totalBlockers}`,
    `- Total patches: ${report.summary.totalPatches}`,
    `- Owner copy projected clean: ${report.summary.ownerCopyProjectedCleanCount}/${report.summary.brandsRemediated}`,
    `- Records patched: ${report.summary.recordsPatched ?? 0}`,
    `- Apply errors: ${report.summary.applyErrors ?? 0}`,
    `- Incomplete control pass: **${report.summary.incompleteControlPass ? "yes" : "no"}**`,
    `- Unlock in v40: **no**`,
    `- Active approval touched: **no**`,
    "",
  ];

  for (const b of report.brandResults) {
    mdLines.push(`## ${b.brandSlug}`);
    mdLines.push(`- displayState: \`${b.displayState}\``);
    mdLines.push(`- patches: ${b.patchPlan.summary.patchCount}`);
    mdLines.push(
      `- owner copy after: ${b.projection.ownerCopyBlockersProjectedZero ? "projected clean" : "still dirty"}`
    );
    mdLines.push(
      `- still blocked: founder=${b.projection.stillBlockedByFounderReview} approval=${b.projection.stillBlockedByActiveApproval}`
    );
    if (b.applyResult) {
      mdLines.push(
        `- apply: records=${b.applyResult.recordsTouched} errors=${b.applyResult.errors?.length || 0}`
      );
    }
    mdLines.push("");
  }

  mdLines.push(report.applyExecuted ? "## Apply command used" : "## Designed apply");
  mdLines.push("```");
  mdLines.push(report.applyDesign.command);
  mdLines.push("```");

  const mdPath = path.join(reportsDir, REPORT_MD);
  fs.writeFileSync(mdPath, mdLines.join("\n"));

  const slugFile = {
    "everhome-suites": "everhome-suites",
    kimpton: "kimpton",
    "radisson-individuals-by-choice": "radisson-individuals-by-choice",
  };

  for (const b of report.brandResults) {
    const file = slugFile[b.brandSlug] || b.brandSlug;
    fs.writeFileSync(
      path.join(reportsDir, `brand-explorer-v40-remediation-${file}.md`),
      renderBrandRemediationMarkdown(b.patchPlan, b.projection)
    );
  }

  const controlLines = [
    "# v40 Incomplete Brand Control Check",
    "",
    `All control pass: **${report.incompleteControl.allControlPass ? "yes" : "no"}**`,
    "",
  ];
  for (const r of report.incompleteControl.rows || []) {
    controlLines.push(`## ${r.brandSlug}`);
    controlLines.push(`- shouldRenderFullProfile: **${r.shouldRenderFullProfile}**`);
    controlLines.push(`- displayState: \`${r.displayState}\``);
    controlLines.push(`- Profile in Preparation: ${r.profileInPreparation}`);
    controlLines.push(`- forbiddenStringsFound: ${r.forbiddenStringsFound}`);
    controlLines.push(`- control pass: **${r.controlPass ? "PASS" : "FAIL"}**`);
    controlLines.push("");
  }
  fs.writeFileSync(
    path.join(reportsDir, "brand-explorer-v40-incomplete-brand-control-check.md"),
    controlLines.join("\n")
  );

  const docsDir = path.join(ROOT, "docs", "data-intelligence");
  fs.mkdirSync(docsDir, { recursive: true });
  fs.writeFileSync(
    path.join(docsDir, "brand-explorer-v40-active-release-remediation.md"),
    [
      "# v40 Brand Explorer Active Release Remediation",
      "",
      "Generic batch remediation for Everhome, Kimpton, and Radisson Individuals.",
      "Removes LOI/FDD/Item 19/fee-stack/net-contribution/URL language from owner-facing Presentation copy.",
      "",
      "```bash",
      "npm run brand-explorer-v40-active-release-remediation -- --brands everhome-suites,kimpton,radisson-individuals-by-choice --dry-run",
      "```",
      "",
      "## Rules",
      "- Dry-run by default",
      "- No active-profile approval",
      "- No Company Validated / Source Library / Registry / image-field writes",
      "- No incomplete brand unlock",
      "- Founder review + active approval remain required after copy scrub",
      "",
      "## Property examples",
      "Minimum 3 visible with imageUrl. Extras allowed unless duplicates (then Do Not Display hide).",
    ].join("\n")
  );

  return { jsonPath, mdPath };
}
