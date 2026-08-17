/**
 * Legacy approved Brand Explorer profile reconciliation.
 *
 * Discovers historically finished/approved brands that are locked under the new OS
 * because they are missing PRIMARY_RELEASE_SLUGS / Active Profile Approved fields.
 * Dry-run by default. Apply only with explicit founder confirmation flags.
 *
 * Never writes Company Validated, Source Library, or Registry approval fields.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { listActiveProfileBrandSlugs, getActiveProfileBrandConfig } from "./brand-explorer-active-profile-brand-config.js";
import { FACTORY_SUPPORTED_SLUGS } from "./brand-explorer-active-profile-factory-rules.js";
import { PRIMARY_RELEASE_SLUGS } from "./brand-explorer-os-state-machine.js";
import { evaluateImageUniqueness } from "./brand-explorer-image-uniqueness.js";
import { resolveBrandExplorerDisplayState } from "./brand-explorer-display-state.js";

export const LEGACY_RECONCILIATION_VERSION = "legacy-approved-profile-reconciliation-v1";

export const REPORT_JSON = "brand-explorer-legacy-approved-profile-reconciliation.json";
export const REPORT_MD = "brand-explorer-legacy-approved-profile-reconciliation.md";

export const REQUIRED_APPLY_FLAGS = Object.freeze([
  "--approve-legacy-approved-profile-reconciliation",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-content-rewrites",
  "--confirm-legacy-status-evidence-reviewed",
]);

/**
 * Explicit founder-named / historically finished Brand Explorer profiles.
 * recordId lets reconciliation resolve brands outside PRIMARY_RELEASE_SLUGS /
 * ACTIVE_PROFILE_BRAND_CONFIGS without inventing Airtable fields.
 */
export const LEGACY_SEED_BRANDS = Object.freeze([
  {
    slug: "autograph-collection",
    recordId: "recEJCTDj1zrsjPM6",
    name: "Autograph Collection",
    aliases: ["autograph", "autograph-hotels"],
  },
  {
    slug: "ascend",
    recordId: "reclkgOzvAcBheUSo",
    name: "Ascend Hotel Collection",
    aliases: ["ascend-hotel-collection"],
  },
  {
    slug: "comfort-inn-suites",
    recordId: "recOzH5iAE1xEjyD0",
    name: "Comfort Inn & Suites",
    aliases: ["comfort", "comfort-inn"],
  },
  {
    slug: "country-inn-suites",
    recordId: "recaayt9u7YYg8h7Y",
    name: "Country Inn & Suites by Choice",
    aliases: ["country", "country-inn", "country-inn-suites-by-choice"],
  },
  {
    slug: "curio-collection",
    recordId: "receQkxgjlezsc1xg",
    name: "Curio Collection by Hilton",
    aliases: ["curio"],
  },
  {
    slug: "tribute-portfolio",
    recordId: "recCvV0PuZOi8c3hC",
    name: "Tribute Portfolio",
    aliases: [],
  },
  {
    slug: "vignette-collection",
    recordId: "recDwzv86TWnz2gGB",
    name: "Vignette Collection",
    aliases: [],
  },
  {
    slug: "handwritten-collection",
    recordId: "rec7hTXwMRC81EPqz",
    name: "Handwritten Collection",
    aliases: [],
  },
  {
    slug: "woodspring-suites",
    recordId: "recsOd51NzRPYsMko",
    name: "WoodSpring Suites",
    aliases: [],
  },
  {
    slug: "suburban-studios",
    recordId: "reclcjg5Foa9Vs5TC",
    name: "Suburban Studios",
    aliases: [],
  },
]);

export const LEGACY_SEED_SLUGS = Object.freeze(LEGACY_SEED_BRANDS.map((b) => b.slug));

export function getLegacySeedBrand(slugOrAlias) {
  const raw = nz(slugOrAlias);
  if (!raw) return null;
  // Airtable record IDs are case-sensitive — never lowercase before comparing.
  if (/^rec[a-zA-Z0-9]{10,}$/.test(raw)) {
    return LEGACY_SEED_BRANDS.find((b) => b.recordId === raw) || null;
  }
  const key = raw.toLowerCase();
  return (
    LEGACY_SEED_BRANDS.find(
      (b) =>
        b.slug === key ||
        (b.aliases || []).includes(key) ||
        nz(b.name).toLowerCase() === key
    ) || null
  );
}

export function isLegacyApprovedSeedSlug(slugOrAlias) {
  return getLegacySeedBrand(slugOrAlias) != null;
}

/**
 * Resolve legacy seed from slug, alias, Airtable record id, or brand display name.
 * Use on every Brand Library lookup path so record-id deep links keep historical approval.
 */
export function resolveLegacyApprovedSeed({ slug, recordId, brandName } = {}) {
  return (
    getLegacySeedBrand(slug) ||
    getLegacySeedBrand(recordId) ||
    getLegacySeedBrand(brandName) ||
    null
  );
}

export function isLegacyApprovedSeedIdentity({ slug, recordId, brandName } = {}) {
  return resolveLegacyApprovedSeed({ slug, recordId, brandName }) != null;
}

const FORBIDDEN_WRITE_FIELDS = new Set([
  "Company Validated",
  "Company Validation Date",
]);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

export function parseLegacyApplyFlags(argv = []) {
  const missing = REQUIRED_APPLY_FLAGS.filter((f) => !argv.includes(f));
  return {
    apply: argv.includes("--apply"),
    missing,
    ok: argv.includes("--apply") && missing.length === 0,
  };
}

function discoverCandidateSlugs() {
  const set = new Set([
    ...LEGACY_SEED_SLUGS,
    ...FACTORY_SUPPORTED_SLUGS,
    ...listActiveProfileBrandSlugs(),
  ]);
  // Drop alias-only keys that resolve to a canonical seed slug
  return [...set]
    .filter(Boolean)
    .filter((slug) => {
      const seed = getLegacySeedBrand(slug);
      return !seed || seed.slug === slug;
    })
    .sort();
}

function markHistoricalSignal(signals, slug, file, ready) {
  const key = nz(slug).toLowerCase();
  if (!key) return;
  if (!signals.has(key)) signals.set(key, { sources: [], ready: false });
  const entry = signals.get(key);
  if (ready) {
    entry.ready = true;
    entry.sources.push(file);
  }
}

function loadHistoricalReportSignals(reportsDir) {
  const signals = new Map(); // slug -> { sources: [], ready: bool }
  if (!fs.existsSync(reportsDir)) return signals;
  const files = fs.readdirSync(reportsDir).filter((f) => /brand-explorer.*\.(json)$/i.test(f));
  for (const file of files.slice(0, 120)) {
    try {
      const raw = JSON.parse(fs.readFileSync(path.join(reportsDir, file), "utf8"));
      const rows = raw.brandResults || raw.brands || raw.results || [];
      const list = Array.isArray(rows) ? rows : [];
      for (const row of list) {
        const slug = nz(row.brandSlug || row.slug || row.brand?.slug).toLowerCase();
        if (!slug) continue;
        const ready =
          row.readyForActiveProfile === true ||
          row.summary?.readyForActiveProfile === true ||
          row.overallStatus === "ready" ||
          row.eligibility?.status === "active_profile_ready" ||
          row.canonicalState === "active_profile_ready" ||
          row.displayState === "active_profile_ready" ||
          row.liveState?.displayState === "active_profile_ready" ||
          (row.halted === false && /complete-build/i.test(file));
        markHistoricalSignal(signals, slug, file, ready);
      }
      // Complete-build orchestrator shape: { brand: { slug }, readyForActiveProfile, halted }
      const completeSlug = nz(raw.brand?.slug || raw.resolution?.resolvedSlug).toLowerCase();
      if (completeSlug) {
        const ready =
          raw.readyForActiveProfile === true ||
          (raw.halted === false && /complete-build/i.test(file));
        markHistoricalSignal(signals, completeSlug, file, ready);
        const seed = getLegacySeedBrand(completeSlug);
        if (seed && seed.slug !== completeSlug) {
          markHistoricalSignal(signals, seed.slug, file, ready);
        }
      }
    } catch {
      // skip unreadable / non-matching reports
    }
  }
  return signals;
}

async function fetchBrandApi(slug) {
  const { getBrandLibraryBrandById } = await import("../../api/brand-library.js");
  const seed = getLegacySeedBrand(slug);
  const lookupId = seed?.recordId || slug;
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
  if (res.statusCode !== 200 || !res.payload?.brand) return null;
  return res.payload.brand;
}

function classifyBrand({
  brand,
  slug,
  inPrimaryRelease,
  historicalReportReady,
  reportSignalReady = false,
  imageUniqueness,
  display,
}) {
  const activeApproved = display.completeness?.activeProfileApproved === true;
  const founderPass = display.completeness?.founderVisualReviewPass === true;
  const historicalApproved =
    display.completeness?.historicalApproved === true || historicalReportReady === true;
  const full = display.shouldRenderFullProfile === true;
  const locked = !full;

  const missingReleaseFields = [];
  if (!activeApproved) missingReleaseFields.push("Active Profile Approved");
  if (!founderPass) missingReleaseFields.push("Founder Visual Review Pass");

  let recommendedAction = "unresolved_manual_review";
  let classification = "unresolved_manual_review";

  if (activeApproved && full && imageUniqueness.pass) {
    classification = "already_active_profile_ready";
    recommendedAction = "no_action";
  } else if (historicalApproved && !display.completeness?.hasPresentationRows) {
    classification = "content_remediation_required";
    recommendedAction = "content_remediation_required";
  } else if (historicalApproved && !imageUniqueness.pass) {
    classification = "image_remediation_required";
    recommendedAction = "image_remediation_required";
  } else if (historicalApproved && imageUniqueness.pass && display.completeness?.visualsCountReady) {
    classification = "migrate_to_active_profile_ready";
    recommendedAction = "migrate_to_active_profile_ready";
  } else if (historicalApproved) {
    classification = "needs_new_gate_validation_before_migration";
    recommendedAction = "needs_new_gate_validation_before_migration";
  } else if (locked) {
    classification = "not_historically_approved_or_incomplete";
    recommendedAction = "unresolved_manual_review";
  }

  const historicalStatus = getLegacySeedBrand(slug)
    ? reportSignalReady
      ? "founder_named_legacy_seed_and_historical_report"
      : "founder_named_legacy_seed"
    : historicalReportReady
      ? "historical_report_ready"
      : display.completeness?.historicalApproved
        ? "historical_basics_signal"
        : "no_historical_ready_signal";

  const patches = [];
  if (recommendedAction === "migrate_to_active_profile_ready" && brand?.id) {
    const fields = {};
    if (!activeApproved) {
      fields["Active Profile Approved"] = true;
      fields["Ready for Active Profile"] = true;
      fields["Active Profile Approved Date"] = new Date().toISOString().slice(0, 10);
    }
    if (!founderPass && historicalApproved) {
      fields["Founder Visual Review Pass"] = true;
    }
    if (Object.keys(fields).length) {
      patches.push({
        table: "Brand Setup - Brand Basics",
        action: "PATCH",
        recordId: brand.id,
        brandSlug: slug,
        fields,
        reason: "legacy_approved_profile_reconciliation",
      });
    }
  }

  return {
    brand: brand?.name || slug,
    brandSlug: slug,
    recordId: brand?.id || null,
    historicalStatus,
    currentOsState: display.brandExplorerDisplayState,
    currentExternalState: full ? "full_profile" : "locked_preparation_shell",
    missingReleaseFields,
    missingPrimaryReleaseSlugs: !inPrimaryRelease,
    inPrimaryReleaseSlugs: inPrimaryRelease,
    imageUniquenessPass: imageUniqueness.pass === true,
    galleryDistinctCount: imageUniqueness.galleryDistinctCount,
    recommendedAction,
    classification,
    blockers: display.blockers || [],
    patches,
    companyValidatedUntouched: true,
  };
}

export async function runLegacyApprovedProfileReconciliation({
  reportsDir = path.join(ROOT, "reports"),
  slugs = null,
} = {}) {
  const candidates = slugs?.length ? slugs : discoverCandidateSlugs();
  const reportSignals = loadHistoricalReportSignals(reportsDir);
  const brandResults = [];

  for (const slug of candidates) {
    const config = getActiveProfileBrandConfig(slug);
    let brand = null;
    try {
      brand = await fetchBrandApi(slug);
    } catch (err) {
      brandResults.push({
        brand: slug,
        brandSlug: slug,
        historicalStatus: "fetch_failed",
        currentOsState: "unknown",
        currentExternalState: "unknown",
        missingReleaseFields: [],
        missingPrimaryReleaseSlugs: !PRIMARY_RELEASE_SLUGS.includes(slug),
        recommendedAction: "unresolved_manual_review",
        classification: "unresolved_manual_review",
        error: err.message,
        patches: [],
      });
      continue;
    }
    if (!brand) {
      // Skip unresolved aliases (e.g. "comfort" vs comfort-inn) quietly when no config
      if (!config && !reportSignals.get(slug)?.ready) continue;
      brandResults.push({
        brand: slug,
        brandSlug: slug,
        historicalStatus: "not_found",
        currentOsState: "not_found",
        currentExternalState: "not_found",
        missingReleaseFields: [],
        missingPrimaryReleaseSlugs: !PRIMARY_RELEASE_SLUGS.includes(slug),
        recommendedAction: "unresolved_manual_review",
        classification: "unresolved_manual_review",
        patches: [],
      });
      continue;
    }

    const blocks = brand.brandExplorer?.blocks || [];
    const imageUniqueness = evaluateImageUniqueness({
      brand,
      presentationRows: blocks,
      brandSlug: slug,
    });
    const historicalReportReady = reportSignals.get(slug)?.ready === true;
    const seedNamed = isLegacyApprovedSeedSlug(slug);
    const display = resolveBrandExplorerDisplayState(brand, {
      legacyHistoricalApproved: historicalReportReady || seedNamed,
    });

    brandResults.push(
      classifyBrand({
        brand,
        slug,
        inPrimaryRelease: PRIMARY_RELEASE_SLUGS.includes(slug),
        historicalReportReady: historicalReportReady || seedNamed,
        reportSignalReady: historicalReportReady,
        imageUniqueness,
        display,
      })
    );
  }

  const byClass = {};
  for (const b of brandResults) {
    byClass[b.classification] = (byClass[b.classification] || 0) + 1;
  }

  return {
    version: LEGACY_RECONCILIATION_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: true,
    primaryReleaseSlugs: [...PRIMARY_RELEASE_SLUGS],
    candidateCount: candidates.length,
    brandResults,
    summary: {
      brandCount: brandResults.length,
      byClassification: byClass,
      migrateCount: brandResults.filter((b) => b.classification === "migrate_to_active_profile_ready")
        .length,
      lockedHistoricallyApproved: brandResults.filter(
        (b) =>
          b.currentExternalState === "locked_preparation_shell" &&
          /historical|founder_named|legacy_seed/i.test(b.historicalStatus || "")
      ).length,
    },
  };
}

export async function applyLegacyApprovedProfileReconciliation({
  report,
  apply = false,
  argv = [],
} = {}) {
  const flagCheck = parseLegacyApplyFlags(argv);
  if (!apply) return { applied: false, reason: "dry_run_only", flagCheck };
  if (!flagCheck.ok) {
    return { applied: false, reason: "missing_apply_flags", missing: flagCheck.missing };
  }
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const results = [];
  for (const brand of report.brandResults || []) {
    if (brand.classification !== "migrate_to_active_profile_ready") {
      results.push({ brandSlug: brand.brandSlug, applied: false, reason: brand.classification });
      continue;
    }
    for (const patch of brand.patches || []) {
      for (const key of Object.keys(patch.fields || {})) {
        if (FORBIDDEN_WRITE_FIELDS.has(key)) {
          throw new Error(`Refuse forbidden field write: ${key}`);
        }
      }
      const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(patch.table)}/${patch.recordId}`;
      const res = await fetch(url, {
        method: "PATCH",
        headers: {
          Authorization: `Bearer ${apiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ fields: patch.fields }),
      });
      const json = await res.json().catch(() => ({}));
      if (!res.ok) {
        throw new Error(json.error?.message || `PATCH failed for ${brand.brandSlug}: ${res.status}`);
      }
      results.push({ brandSlug: brand.brandSlug, applied: true, recordId: patch.recordId, fields: Object.keys(patch.fields) });
    }
  }
  return { applied: true, results, companyValidatedUntouched: true };
}

export function writeLegacyReconciliationReports(report, { reportsDir = path.join(ROOT, "reports") } = {}) {
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, REPORT_JSON);
  const mdPath = path.join(reportsDir, REPORT_MD);
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));
  const md = [
    "# Brand Explorer Legacy Approved Profile Reconciliation",
    "",
    `Generated: ${report.generatedAt}`,
    `Version: ${report.version}`,
    "",
    `- Candidates evaluated: **${report.summary.brandCount}**`,
    `- Migrate recommended: **${report.summary.migrateCount}**`,
    `- Locked with historical approval signal: **${report.summary.lockedHistoricallyApproved}**`,
    "",
    "## Classification counts",
    "",
  ];
  for (const [k, v] of Object.entries(report.summary.byClassification || {})) {
    md.push(`- ${k}: **${v}**`);
  }
  md.push(
    "",
    "## Matrix",
    "",
    "| Brand | Slug | Historical Status | Current OS State | Current External State | Missing Release Fields | Missing PRIMARY_RELEASE_SLUGS | Recommended Action |",
    "| --- | --- | --- | --- | --- | --- | --- | --- |"
  );
  for (const b of report.brandResults) {
    md.push(
      `| ${b.brand} | ${b.brandSlug} | ${b.historicalStatus} | ${b.currentOsState} | ${b.currentExternalState} | ${(b.missingReleaseFields || []).join("; ") || "—"} | ${b.missingPrimaryReleaseSlugs ? "yes" : "no"} | ${b.recommendedAction} |`
    );
  }
  md.push("");
  fs.writeFileSync(mdPath, md.join("\n"));
  return { jsonPath, mdPath };
}
