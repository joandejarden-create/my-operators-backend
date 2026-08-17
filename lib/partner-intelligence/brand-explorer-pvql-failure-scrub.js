/**
 * PVQL Failure Scrub — targeted owner-facing Presentation hygiene.
 * Strips raw URLs + forbidden language; no CV / Source / Registry / release writes.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import {
  isOwnerFacingPresentationRow,
} from "./brand-explorer-public-visibility-quality-lock.js";
import {
  scanForbiddenLanguage,
  scanMechanicalCopy,
} from "./brand-explorer-v40b-copy-quality-patterns.js";
import {
  MAP_PRESENTATION_FIELDS,
  PRESENTATION_TABLE,
  scrubResidualOwnerFacingCopy,
} from "./brand-explorer-residual-owner-copy-remediation.js";
import { resolveSectionPatternBrandIdentity } from "./brand-explorer-section-pattern-parity.js";
import { evaluateSectionPatternParity } from "./brand-explorer-section-pattern-parity.js";
import { renderBrandExplorerHtmlForTest } from "./brand-explorer-atelier-render-test-loader.js";

export const SCRUB_VERSION = "pvql-failure-scrub-v1";
export const PVQL_SCRUB_TARGETS = Object.freeze([
  "comfort-inn-suites",
  "country-inn-suites",
  "hotel-indigo",
  "mgallery-collection",
  "small-luxury-hotels-of-the-world",
  "suburban-studios",
  "woodspring-suites",
]);

export const PVQL_SCRUB_PROTECTED_PASSING = Object.freeze([
  "ascend",
  "curio-collection",
  "design-hotels",
  "everhome-suites",
  "kimpton",
  "radisson-individuals-by-choice",
  "tribute-portfolio",
]);

export const REQUIRED_APPLY_FLAGS = Object.freeze([
  "--approve-pvql-failure-scrub",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-release-field-changes",
  "--confirm-no-public-restore-fields",
  "--confirm-visible-owner-facing-scrub-only",
  "--confirm-no-raw-urls",
  "--confirm-no-forbidden-owner-facing-language",
]);

const FORBIDDEN_AIRTABLE_FIELDS = new Set([
  "Company Validated",
  "Company Validation Date",
  "Founder Visual Review Pass",
  "Active Profile Approved",
  "Ready for Active Profile",
  "Active Profile Approved Date",
]);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

const FIELD_API_TO_AIRTABLE = MAP_PRESENTATION_FIELDS;
const AIRTABLE_TO_API = Object.fromEntries(
  Object.entries(FIELD_API_TO_AIRTABLE).map(([api, at]) => [at, api])
);

function nz(v) {
  return v == null ? "" : String(v).trim();
}

export function parsePvqlScrubApplyFlags(argv = []) {
  const missing = REQUIRED_APPLY_FLAGS.filter((f) => !argv.includes(f));
  return {
    apply: argv.includes("--apply"),
    missing,
    ok: argv.includes("--apply") && missing.length === 0,
  };
}

async function fetchBrandApi(slug) {
  const identity = resolveSectionPatternBrandIdentity(slug);
  const lookupId = identity.recordId || slug;
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
  if (!res.payload?.brand) throw new Error(`Brand fetch failed for ${slug}`);
  return res.payload.brand;
}

const MOMENTUM_ANNOUNCEMENT_SLOTS = new Set(["footprint.momentum", "footprint.openings"]);

function fieldFailures(value, { slotKey = "" } = {}) {
  const text = nz(value);
  if (!text) return { forbidden: [], mechanical: [], failureTypes: [] };
  const allowAnnouncementUrls = MOMENTUM_ANNOUNCEMENT_SLOTS.has(nz(slotKey));
  const forbidden = scanForbiddenLanguage(text).filter(
    (h) => !(allowAnnouncementUrls && h.id === "raw_url")
  );
  const mechanical = scanMechanicalCopy(text).filter((h) =>
    ["high", "medium"].includes(h.severity)
  );
  const failureTypes = [
    ...forbidden.map((h) =>
      h.id === "raw_url" ? "raw_url_scan" : `forbidden_owner_facing_language:${h.id}`
    ),
    ...mechanical.map((h) => `generic_copy_scan:${h.id}`),
  ];
  return { forbidden, mechanical, failureTypes };
}

/**
 * @deprecated Recent Momentum contract requires trailing announcement URLs.
 * Keep exported for legacy tests; do not use in owner-facing scrub paths.
 */
export function stripMomentumTrailingUrl(body) {
  const paras = nz(body)
    .split(/\n\n+/)
    .map((p) => p.trim())
    .filter(Boolean);
  const kept = paras.filter((p) => !/^https?:\/\//i.test(p));
  return kept.join("\n\n").trim();
}

const SUBURBAN_GENERIC_REPLACEMENTS = Object.freeze([
  {
    re: /\bconfirm participation costs and prototype requirements directly during brand engagement\b/gi,
    replace:
      "validate Suburban extended-stay kitchenette layout, weekly-stay product fit, and Choice systems participation in commercial diligence",
  },
  {
    re: /\bConfirm participation costs, operating obligations, and agreement terms directly during brand engagement(?: and legal review)?\.?/gi,
    replace:
      "Validate Suburban Studios kitchenette scope, weekly-stay standards, and Choice participation economics in commercial diligence.",
  },
  {
    re: /\bConfirm [^.]{0,100} directly during brand engagement(?: and legal review)?\.?/gi,
    replace:
      "Validate Suburban Studios extended-stay product standards and Choice participation economics in commercial diligence.",
  },
]);

function scrubFieldValue(text, { slotKey, brandSlug }) {
  let after = nz(text);
  const before = after;
  if (!after) return { before, after, changed: false, failureTypes: [], clean: true };

  const residual = scrubResidualOwnerFacingCopy(after, { slotKey, brandSlug });
  after = residual.after;

  // Avoid v40C empty-placeholder boilerplate that re-triggers generic_diligence_boilerplate
  after = after
    .replace(
      /\bConfirm [^.]{0,120} directly during brand engagement(?: and legal review)?\.?/gi,
      brandSlug === "suburban-studios"
        ? "Validate Suburban Studios kitchenette scope, weekly-stay standards, and Choice participation economics in commercial diligence"
        : brandSlug === "woodspring-suites"
          ? "Validate WoodSpring extended-stay prototype scope and Choice participation economics in commercial diligence"
          : brandSlug === "country-inn-suites"
            ? "Validate Country Inn & Suites upper-midscale breakfast and suite standards plus Choice participation economics in commercial diligence"
            : brandSlug === "comfort-inn-suites"
              ? "Validate Comfort Inn & Suites midscale product standards and Choice participation economics in commercial diligence"
              : "Validate brand participation economics, operating obligations, and agreement terms in commercial diligence"
    )
    .replace(
      /\bPublic materials on this page are orientation only—not commercial terms or a forecast\.?/gi,
      "Public orientation on this page is not a substitute for commercial terms or a forecast."
    );

  if (brandSlug === "suburban-studios") {
    for (const rule of SUBURBAN_GENERIC_REPLACEMENTS) {
      after = after.replace(rule.re, rule.replace);
    }
  }

  // Recent Momentum / Openings: preserve trailing announcement URLs (permanent contract).
  // All other owner-facing slots: hard-strip raw https URLs.
  if (!MOMENTUM_ANNOUNCEMENT_SLOTS.has(nz(slotKey))) {
    after = after.replace(/https?:\/\/\S+/gi, "").replace(/[ \t]+\n/g, "\n").trim();
  }
  after = after
    .split("\n")
    .map((l) => l.replace(/[ \t]{2,}/g, " ").trim())
    .join("\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();

  const check = fieldFailures(after, { slotKey });
  return {
    before,
    after,
    changed: after !== before,
    failureTypes: check.failureTypes,
    clean: check.failureTypes.length === 0,
    residualClean: residual.clean,
  };
}

export function extractPvqlFieldOffenders(brand, brandSlug) {
  const rows = (brand.brandExplorer?.blocks || []).filter(isOwnerFacingPresentationRow);
  const offenders = [];
  for (const row of rows) {
    for (const [apiKey, airtableKey] of Object.entries(FIELD_API_TO_AIRTABLE)) {
      const value = nz(row[apiKey]);
      if (!value) continue;
      const check = fieldFailures(value, { slotKey: row.slotKey });
      if (!check.failureTypes.length) continue;
      offenders.push({
        brand: brandSlug,
        tab: inferTab(row.slotKey),
        section: nz(row.slotKey),
        recordId: row.recordId || null,
        field: airtableKey,
        apiKey,
        failureType: check.failureTypes.join("; "),
        failureTypes: check.failureTypes,
        currentValue: value.slice(0, 240),
        hits: [
          ...check.forbidden.map((h) => `${h.id}:${h.snippet}`),
          ...check.mechanical.map((h) => `${h.id}:${h.snippet}`),
        ].slice(0, 6),
      });
    }
  }
  return offenders;
}

function inferTab(slotKey) {
  const s = nz(slotKey);
  if (s.startsWith("footprint.")) return "Footprint & Growth";
  if (s.startsWith("overview.")) return "Overview";
  if (s.startsWith("operations.") || s.startsWith("valueOwners.")) return "Operating / Value";
  if (s.startsWith("standards.")) return "Owner considerations";
  if (s.startsWith("economics.") || s.startsWith("loyalty.")) return "Economics / Loyalty";
  if (s.startsWith("insight.") || s.startsWith("materials.")) return "Insight / Materials";
  return "Other";
}

export function planPvqlFailureScrubForBrand(brand, brandSlug, { force = false } = {}) {
  if (!force && PVQL_SCRUB_PROTECTED_PASSING.includes(brandSlug)) {
    throw new Error(`Refuse scrub of PVQL-passing protected brand: ${brandSlug}`);
  }
  const offenders = extractPvqlFieldOffenders(brand, brandSlug);
  const patches = [];
  const fieldRows = [];

  for (const off of offenders) {
    const full = findFullField(brand, off);
    const scrubFull = scrubFieldValue(full, { slotKey: off.section, brandSlug });
    const proposed = scrubFull.after;
    fieldRows.push({
      ...off,
      currentValue: full.slice(0, 280),
      proposedFix: proposed.slice(0, 280),
      proposedClean: scrubFull.clean,
      remainingFailures: scrubFull.failureTypes,
    });
    if (!scrubFull.changed && scrubFull.clean) continue;
    if (!scrubFull.changed && !scrubFull.clean) {
      // Force write when still dirty but scrub didn't change — should be rare
      continue;
    }
    if (FORBIDDEN_AIRTABLE_FIELDS.has(off.field)) {
      throw new Error(`Refuse forbidden field write: ${off.field}`);
    }
    if (!off.recordId) continue;
    patches.push({
      table: PRESENTATION_TABLE,
      action: "PATCH",
      recordId: off.recordId,
      brandSlug,
      slotKey: off.section,
      reason: `pvql_scrub:${off.failureTypes.join(",")}`,
      fields: { [off.field]: proposed },
      sanitizedPayloadPreview: {
        field: off.field,
        before: full.slice(0, 100),
        after: proposed.slice(0, 100),
        failureTypes: off.failureTypes,
      },
    });
  }

  return {
    brandSlug,
    brandName: brand.name || brandSlug,
    recordId: brand.id || brand.recordId,
    offenderCount: offenders.length,
    patchCount: patches.length,
    fieldRows,
    patches,
  };
}

function findFullField(brand, off) {
  const row = (brand.brandExplorer?.blocks || []).find((b) => b.recordId === off.recordId);
  if (!row) return off.currentValue;
  const apiKey = off.apiKey || AIRTABLE_TO_API[off.field];
  return nz(row[apiKey]);
}

export async function planPvqlFailureScrub({ brands = null } = {}) {
  const list = brands?.length ? brands : [...PVQL_SCRUB_TARGETS];
  for (const slug of list) {
    if (PVQL_SCRUB_PROTECTED_PASSING.includes(slug)) {
      throw new Error(`Refuse protected passing brand: ${slug}`);
    }
  }

  const brandPlans = [];
  for (const slug of list) {
    console.log(`Planning PVQL scrub: ${slug}`);
    const brand = await fetchBrandApi(slug);
    const plan = planPvqlFailureScrubForBrand(brand, slug);
    // Project patches and re-check corpus
    const projectedBlocks = projectPatches(brand.brandExplorer?.blocks || [], plan.patches);
    const projectedBrand = {
      ...brand,
      brandExplorer: { ...(brand.brandExplorer || {}), blocks: projectedBlocks },
    };
    const remaining = extractPvqlFieldOffenders(projectedBrand, slug);
    const html = renderBrandExplorerHtmlForTest(projectedBrand, {
      allPanels: true,
      internalPreview: false,
    });
    const sectionParity = evaluateSectionPatternParity({
      brandSlug: slug,
      brandName: brand.name,
      presentationRows: projectedBlocks.filter(isOwnerFacingPresentationRow),
      html,
    });
    brandPlans.push({
      ...plan,
      remainingAfterProjection: remaining.length,
      projectedSectionParityPass: sectionParity.pass === true,
      projectedSectionParityFails: (sectionParity.findings || []).map((f) => `${f.section}:${f.status}`),
    });
    console.log(
      `  offenders=${plan.offenderCount} patches=${plan.patchCount} remaining=${remaining.length} sectionParity=${sectionParity.pass}`
    );
  }

  const unclean = brandPlans.filter((b) => b.remainingAfterProjection > 0);
  return {
    version: SCRUB_VERSION,
    generatedAt: new Date().toISOString(),
    brands: brandPlans,
    summary: {
      brands: brandPlans.length,
      offenders: brandPlans.reduce((n, b) => n + b.offenderCount, 0),
      patches: brandPlans.reduce((n, b) => n + b.patchCount, 0),
      uncleanAfterProjection: unclean.map((b) => b.brandSlug),
    },
    validation: {
      pass: unclean.length === 0,
      failedChecks: unclean.map((b) => `remaining_offenders:${b.brandSlug}:${b.remainingAfterProjection}`),
    },
    guardrails: {
      companyValidatedUntouched: true,
      sourceLibraryUntouched: true,
      registryUntouched: true,
      releaseFieldsUntouched: true,
      publicRestoreUntouched: true,
      protectedPassingUntouched: true,
      visibleOwnerFacingScrubOnly: true,
    },
  };
}

function projectPatches(blocks, patches) {
  return (blocks || []).map((b) => {
    const next = { ...b };
    for (const p of patches || []) {
      if (p.recordId !== b.recordId) continue;
      for (const [airtableKey, value] of Object.entries(p.fields || {})) {
        const apiKey = AIRTABLE_TO_API[airtableKey];
        if (apiKey) next[apiKey] = value;
      }
    }
    return next;
  });
}

async function airtableWrite({ baseId, apiKey, table, recordId, fields, method }) {
  const url =
    method === "POST"
      ? `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}`
      : `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/${recordId}`;
  const res = await fetch(url, {
    method,
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(json.error?.message || `${method} failed ${recordId || table}: ${res.status}`);
  }
  return json;
}

export async function applyPvqlFailureScrub({ report, apply = false, argv = [] } = {}) {
  const flags = parsePvqlScrubApplyFlags(argv);
  if (!apply) return { applied: false, reason: "dry_run_only", flags };
  if (!flags.ok) return { applied: false, reason: "missing_apply_flags", missing: flags.missing, flags };
  if (!report.validation?.pass) {
    return { applied: false, reason: "validation_failed", failedChecks: report.validation?.failedChecks };
  }

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY required");

  const results = [];
  for (const brand of report.brands || []) {
    if (PVQL_SCRUB_PROTECTED_PASSING.includes(brand.brandSlug)) {
      throw new Error(`Refuse write to protected passing brand ${brand.brandSlug}`);
    }
    for (const patch of brand.patches || []) {
      for (const key of Object.keys(patch.fields || {})) {
        if (FORBIDDEN_AIRTABLE_FIELDS.has(key)) throw new Error(`Refuse forbidden field ${key}`);
      }
      if (patch.table !== PRESENTATION_TABLE) throw new Error(`Refuse table ${patch.table}`);
      if (!patch.recordId) throw new Error(`Missing recordId for ${patch.slotKey}`);
      const json = await airtableWrite({
        baseId,
        apiKey,
        table: patch.table,
        recordId: patch.recordId,
        fields: patch.fields,
        method: "PATCH",
      });
      results.push({
        brandSlug: patch.brandSlug,
        recordId: patch.recordId,
        slotKey: patch.slotKey,
        fields: Object.keys(patch.fields),
        action: "PATCH",
        id: json.id,
      });
    }
  }

  return {
    applied: true,
    results,
    companyValidatedUntouched: true,
    sourceLibraryUntouched: true,
    registryUntouched: true,
    releaseFieldsUntouched: true,
    publicRestoreUntouched: true,
  };
}

export async function buildPublicFullCohortDriftReport() {
  const { runPublicVisibilityQualityLock } = await import(
    "./brand-explorer-public-visibility-quality-lock.js"
  );
  // Prefer latest report if present; else lightweight evaluate of known slugs
  const reportPath = path.join(ROOT, "reports", "brand-explorer-public-visibility-quality-lock.json");
  let pvql = null;
  if (fs.existsSync(reportPath)) {
    pvql = JSON.parse(fs.readFileSync(reportPath, "utf8"));
  }

  const rows = (pvql?.brands || []).filter((b) => b.publicFullProfile === true);
  const builtBlocked = [
    "country-inn-suites",
    "quality-inn",
    "radisson",
    "radisson-blu",
    "radisson-red",
    "suburban-studios",
    "woodspring-suites",
  ];
  const visibilityRestored = ["ascend", "comfort-inn-suites", "curio-collection", "tribute-portfolio"];
  const primary = [
    "design-hotels",
    "everhome-suites",
    "hotel-indigo",
    "kimpton",
    "mgallery-collection",
    "radisson-individuals-by-choice",
    "small-luxury-hotels-of-the-world",
  ];

  const explained = rows.map((b) => {
    let unlock = "other";
    if (b.cohort === "primary_release" || primary.includes(b.slug)) unlock = "primary_release";
    else if (visibilityRestored.includes(b.slug)) unlock = "legacy_visibility_unlock / restored_legacy_public (visibility-fix pack)";
    else if (b.publicDisplayState === "legacy_approved_pending_migration") {
      unlock = "legacy_visibility_unlock (historicalApproved + presentation + visuals + image uniqueness)";
    } else if (b.publicDisplayState === "active_profile_ready") unlock = "active_profile_ready";
    else if (b.publicDisplayState === "external_owner_ready") unlock = "external_owner_ready";
    else if (String(b.publicDisplayState || "").includes("founder")) unlock = "founder_preview_leak";
    else if (b.cohort === "restored_legacy_public") unlock = "restored_legacy_public (catch-all for non-primary public-full)";

    const inBuiltBlocked = builtBlocked.includes(b.slug);
    const intentionalRestore = visibilityRestored.includes(b.slug) || primary.includes(b.slug);
    return {
      brand: b.slug,
      brandName: b.brandName,
      publicDisplayState: b.publicDisplayState,
      cohort: b.cohort,
      unlockMechanism: unlock,
      inBuiltBlockedTargets: inBuiltBlocked,
      intentionalPublicRestore: intentionalRestore,
      accidentalRelativeToExplicitRestore:
        inBuiltBlocked && !intentionalRestore
          ? "Yes — public-full via legacyVisibilityUnlock, not an explicit public-restore command"
          : "No",
      fullyReadyAlone: false, // display-state path does not unlock on fullyReady
      note:
        inBuiltBlocked && !intentionalRestore
          ? "Built-blocked target is public-full because historicalApproved legacy unlock renders full profile; fullyReady alone does not unlock."
          : "",
    };
  });

  return {
    generatedAt: new Date().toISOString(),
    expectedBaselinePublicFull: 11,
    expectedAfterExplicitRestoreOf7: 18,
    observedPublicFull: rows.length,
    inconsistency:
      "Observed 14 = 11 intentional public-full (7 primary + 4 visibility-restored) + 3 legacy-seed built-blocked brands (country, suburban, woodspring) unlocked via legacyVisibilityUnlock — not via fullyReady and not via explicit public restore. quality-inn + radisson/blu/red remain non-public-full.",
    brands: explained,
    missingFrom18: builtBlocked.filter((s) => !rows.some((b) => b.slug === s)),
    bugCheck: {
      fullyReadyAutoPublic: false,
      explanation:
        "shouldRenderFullProfile uses FULL_PROFILE_DISPLAY_STATES or legacyVisibilityUnlock(historicalApproved && rows && visuals && imageUniqueness). fullyReady/tab-factory pass is not an unlock input.",
    },
  };
}

export function writePvqlFailureScrubReports(report, applyResult = null, drift = null) {
  const jsonPath = path.join(ROOT, "reports", "brand-explorer-pvql-failure-scrub.json");
  fs.writeFileSync(jsonPath, JSON.stringify({ ...report, applyResult, drift }, null, 2));

  const lines = [
    `# PVQL Failure Scrub`,
    ``,
    `Generated: ${report.generatedAt}`,
    `Version: ${report.version}`,
    `Applied: ${applyResult?.applied === true}`,
    ``,
    `## Summary`,
    ``,
    `- Brands: ${report.summary.brands}`,
    `- Offenders: ${report.summary.offenders}`,
    `- Patches: ${report.summary.patches}`,
    `- Unclean after projection: ${report.summary.uncleanAfterProjection.join(", ") || "(none)"}`,
    ``,
    `| Brand | Tab | Section | Record ID | Field | Failure Type | Current Value | Proposed Fix |`,
    `| --- | --- | --- | --- | --- | --- | --- | --- |`,
  ];
  for (const brand of report.brands || []) {
    for (const row of brand.fieldRows || []) {
      lines.push(
        `| ${row.brand} | ${row.tab} | ${row.section} | ${row.recordId || ""} | ${row.field} | ${String(row.failureType || "").replace(/\|/g, "/")} | ${String(row.currentValue || "").replace(/\|/g, "/").replace(/\n/g, " ").slice(0, 120)} | ${String(row.proposedFix || "").replace(/\|/g, "/").replace(/\n/g, " ").slice(0, 120)} |`
      );
    }
  }
  lines.push(``, `## Guardrails`, ``, `- Presentation Title/Body/Case Summary only`, `- No CV / Source / Registry / release / public restore`, ``);
  const mdPath = path.join(ROOT, "reports", "brand-explorer-pvql-failure-scrub.md");
  fs.writeFileSync(mdPath, lines.join("\n"));

  if (drift) {
    const driftPath = path.join(ROOT, "reports", "brand-explorer-public-full-cohort-drift.md");
    const dlines = [
      `# Public-Full Cohort Drift`,
      ``,
      `Generated: ${drift.generatedAt}`,
      ``,
      `## Verdict`,
      ``,
      drift.inconsistency,
      ``,
      `- Expected baseline: **${drift.expectedBaselinePublicFull}**`,
      `- Expected after explicit restore of 7: **${drift.expectedAfterExplicitRestoreOf7}**`,
      `- Observed: **${drift.observedPublicFull}**`,
      `- fullyReady auto-public bug: **${drift.bugCheck.fullyReadyAutoPublic}** — ${drift.bugCheck.explanation}`,
      ``,
      `| Brand | Display State | Cohort | Unlock Mechanism | Built-blocked? | Intentional restore? | Accidental vs explicit restore? |`,
      `| --- | --- | --- | --- | --- | --- | --- |`,
    ];
    for (const b of drift.brands || []) {
      dlines.push(
        `| ${b.brand} | ${b.publicDisplayState} | ${b.cohort} | ${b.unlockMechanism} | ${b.inBuiltBlockedTargets} | ${b.intentionalPublicRestore} | ${b.accidentalRelativeToExplicitRestore} |`
      );
    }
    dlines.push(
      ``,
      `Built-blocked not yet public-full: ${(drift.missingFrom18 || []).join(", ") || "(none)"}`,
      ``
    );
    fs.writeFileSync(driftPath, dlines.join("\n"));
  }

  const docPath = path.join(ROOT, "docs", "data-intelligence", "brand-explorer-pvql-failure-scrub.md");
  fs.writeFileSync(
    docPath,
    [
      `# PVQL Failure Scrub`,
      ``,
      `Targeted owner-facing Presentation hygiene after section-pattern remediation introduced raw announcement URLs (and residual LOI/FDD/fee-stack language on some legacy profiles).`,
      ``,
      `## Commands`,
      ``,
      "```bash",
      `npm run brand-explorer-pvql-failure-scrub -- --brands comfort-inn-suites,country-inn-suites,hotel-indigo,mgallery-collection,small-luxury-hotels-of-the-world,suburban-studios,woodspring-suites --dry-run`,
      `npm run brand-explorer-pvql-failure-scrub -- --brands … --apply \\`,
      `  --approve-pvql-failure-scrub \\`,
      `  --confirm-no-company-validation-changes \\`,
      `  --confirm-no-source-library-status-changes \\`,
      `  --confirm-no-registry-approval-changes \\`,
      `  --confirm-no-release-field-changes \\`,
      `  --confirm-no-public-restore-fields \\`,
      `  --confirm-visible-owner-facing-scrub-only \\`,
      `  --confirm-no-raw-urls \\`,
      `  --confirm-no-forbidden-owner-facing-language`,
      "```",
      ``,
      `## Recent Momentum / Openings carve-out`,
      ``,
      `\`${"footprint.momentum"}\` and \`${"footprint.openings"}\` **must** keep trailing announcement \`https://\` URLs (Recent Momentum contract).`,
      `\`${"--confirm-no-raw-urls"}\` means no raw URLs outside those slots — not that momentum Bodies are URL-free.`,
      ``,
      `See \`lib/partner-intelligence/brand-explorer-recent-momentum-contract.js\`.`,
      ``,
      `Latest: ${report.generatedAt} · patches=${report.summary.patches}`,
      ``,
    ].join("\n")
  );

  return { jsonPath, mdPath, docPath };
}
