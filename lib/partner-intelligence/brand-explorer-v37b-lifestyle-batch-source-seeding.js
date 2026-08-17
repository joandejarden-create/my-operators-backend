/**
 * Brand Explorer v37B — Lifestyle Batch Config Registration + Source Library Seeding.
 *
 * Hotel Indigo factory config registration (code), MGallery config validation,
 * Source Library plan apply (gated), image risk review, post-seed contract runs.
 *
 * Guardrails: Source Library writes only on full apply gate set.
 * No Presentation, Registry, image fields, Company Validated, or active-profile approval.
 */
import fs from "fs";
import path from "path";
import { spawnSync } from "child_process";
import { fileURLToPath } from "url";
import {
  MAP_PARTNER_SOURCE,
} from "../../api/lib/partner-intelligence-field-map.js";
import {
  createPartnerSource,
  listPartnerSources,
  patchPartnerSource,
} from "./airtable-source.js";
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import { getActiveProfileBrandConfig } from "./brand-explorer-active-profile-brand-config.js";
import { FACTORY_SUPPORTED_SLUGS } from "./brand-explorer-active-profile-factory-rules.js";
import {
  buildSourceLibraryRowPlan,
  probeSourceUrl,
} from "./brand-explorer-lifestyle-affiliation-source-capture-v35C.js";
import { V37A_BRAND_TARGETS } from "./brand-explorer-v37a-lifestyle-batch-intake.js";
import { isBlockedSourceUrl } from "./brand-explorer-choice-extended-stay-source-capture-writer.js";
import {
  isGenericBrandOrLifestyleImageUrl,
  isLogoImageUrl,
} from "./brand-explorer-footprint-opening-image-governance.js";

export const V37B_VERSION = "v37B";
export const REPORT_JSON = "brand-explorer-v37b-lifestyle-batch-source-seeding.json";
export const REPORT_MD = "brand-explorer-v37b-lifestyle-batch-source-seeding.md";
export const DOC_MD = "docs/data-intelligence/brand-explorer-v37b-lifestyle-batch-source-seeding.md";

export const DEFAULT_V37B_BRANDS = Object.freeze(["hotel-indigo", "mgallery-collection"]);

export const TASK_SPEC_RECORD_IDS = Object.freeze({
  "hotel-indigo": "recebXrqaPiSLGCIe",
  "mgallery-collection": "recrWCD1LMqu864oU",
});

export const APPLY_FLAG_APPROVE = "--approve-brand-explorer-v37B-lifestyle-batch-source-seeding";
export const APPLY_FLAG_SOURCE_ONLY = "--confirm-source-library-only";
export const APPLY_FLAG_NO_VALIDATION = "--confirm-no-company-validation-claim";
export const APPLY_FLAG_NO_PRESENTATION = "--confirm-no-presentation-row-changes";
export const APPLY_FLAG_NO_REGISTRY = "--confirm-no-registry-changes";
export const APPLY_FLAG_NO_IMAGE_FIELDS = "--confirm-no-image-field-changes";
export const APPLY_FLAG_NO_ACTIVE_APPROVAL = "--confirm-no-active-profile-approval";
export const APPLY_FLAG_BRANDS_ONLY = "--confirm-hotel-indigo-mgallery-only";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

const SOURCE_TYPE_MAP = Object.freeze({
  "Brand Website": "Brand Page",
  "Development Brochure": "Development Page",
  "Corporate Overview": "Brand Page",
  "Loyalty Program": "Brand Page",
  "Hotel Directory": "Portfolio Page",
});

const OFFICIAL_PARENT_NAMING = Object.freeze({
  "hotel-indigo": {
    parentCompanyOfficialName: "InterContinental Hotels Group",
    consumerParentLabel: "IHG Hotels & Resorts",
    validatedFrom: [
      "https://www.ihg.com/content/us/en/about/brands",
      "https://development.ihg.com/brand/hotel-indigo",
    ],
    note: "IHG is the consumer-facing parent label; InterContinental Hotels Group is the corporate entity — do not confuse with InterContinental luxury brand.",
  },
  "mgallery-collection": {
    parentCompanyOfficialName: "Accor",
    consumerParentLabel: "Accor",
    validatedFrom: [
      "https://group.accor.com/en/brands-and-experiences/mgallery",
      "https://group.accor.com/en/brands-and-experiences",
    ],
    note: "Accor is the official parent; avoid Handwritten, Pullman, Sofitel, or generic Accor boilerplate in MGallery-specific sources.",
  },
});

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function normalizeUrlKey(url) {
  return nz(url).toLowerCase().replace(/\/+$/, "").split("?")[0];
}

function loadJsonIfExists(relativePath) {
  const full = path.join(ROOT, relativePath);
  if (!fs.existsSync(full)) return null;
  try {
    return JSON.parse(fs.readFileSync(full, "utf8"));
  } catch {
    return null;
  }
}

function mapV37ASourceToCatalogEntry(source, brandMeta) {
  const sourceType = SOURCE_TYPE_MAP[source.sourceType] || "Brand Page";
  return {
    role: source.role,
    sourceTitle: source.sourceTitle,
    sourceUrl: source.sourceUrl,
    sourceType,
    evidenceUseCases: (source.intendedUse || "")
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean),
    region: "Global",
    approveForExplorer: source.confidence === "high" || source.confidence === "medium",
    confidenceLevel: source.confidence === "high" ? "High" : source.confidence === "medium" ? "Medium" : "Low",
    copyGuidance: brandMeta.copyGuidance,
  };
}

export function isGenericIhgBrandHeroImage(url) {
  const u = nz(url).toLowerCase();
  if (!u) return false;
  if (/digital\.ihg\.com\/is\/image\/ihg\/hotel-indigo-[a-z0-9-]+/i.test(u)) return false;
  if (/ihg-maldives|ihg\/ihg-[a-z0-9-]+-\d+x\d+-\d/i.test(u)) return true;
  if (/digital\.ihg\.com\/is\/image\/ihg\/ihg-/i.test(u) && !/hotelindigo|indigo-/i.test(u)) return true;
  return false;
}

export function isAccorGenericBrandGraphic(url) {
  const u = nz(url).toLowerCase();
  if (!u) return false;
  if (/accor-brand|accor\.com\/static\/brand|group\.accor\.com.*logo/i.test(u)) return true;
  if (/\.svg$/i.test(u) && /accor/i.test(u)) return true;
  return false;
}

export function validateSourceForBrand(source, brandSlug) {
  const url = nz(source.sourceUrl || source.finalUrl).toLowerCase();
  const title = nz(source.sourceTitle).toLowerCase();
  const blockers = [];

  const urlCheck = isBlockedSourceUrl(source.sourceUrl || source.finalUrl);
  if (urlCheck.blocked) blockers.push(`blocked_url:${urlCheck.reason}`);

  if (source.confidenceLevel === "Low" || source.confidence === "low") {
    blockers.push("low_confidence_third_party");
  }

  if (/company validated|company-approved|company validation/i.test(title + source.note)) {
    blockers.push("implies_company_validated");
  }

  if (brandSlug === "hotel-indigo") {
    if (/accor|mgallery|sofitel|pullman|handwritten/.test(url)) blockers.push("wrong_brand_accor");
    if (/intercontinental\.com\/.*hotel|\/intercontinental\/hotels/i.test(url)) {
      blockers.push("intercontinental_brand_confusion");
    }
    if (!/ihg\.com|development\.ihg\.com/.test(url)) {
      blockers.push("not_official_ihg_public_source");
    }
  }

  if (brandSlug === "mgallery-collection") {
    if (/ihg\.com|hotelindigo|intercontinental\.com/.test(url)) blockers.push("wrong_brand_ihg");
    if (/\/pullman\/|\/sofitel\/|handwritten-collection|\/novotel\//.test(url)) {
      blockers.push("accor_sibling_brand_confusion");
    }
    if (!/accor\.com|all\.accor\.com/.test(url)) {
      blockers.push("not_official_accor_public_source");
    }
  }

  return { ok: blockers.length === 0, blockers };
}

export function validateFactoryConfig(brandSlug, v37aBrandResult = null) {
  const config = getActiveProfileBrandConfig(brandSlug);
  const parentNaming = OFFICIAL_PARENT_NAMING[brandSlug] || null;
  const issues = [];
  const checks = [];

  if (!config) {
    return { ok: false, slug: brandSlug, issues: ["missing_factory_config"], checks: [] };
  }

  checks.push({
    check: "factory_supported_slug",
    pass: FACTORY_SUPPORTED_SLUGS.includes(brandSlug),
    value: FACTORY_SUPPORTED_SLUGS.includes(brandSlug),
  });
  if (!FACTORY_SUPPORTED_SLUGS.includes(brandSlug)) issues.push("not_in_factory_supported_slugs");

  const taskRecordId = TASK_SPEC_RECORD_IDS[brandSlug];
  if (taskRecordId && config.recordId !== taskRecordId) {
    checks.push({
      check: "task_spec_record_id",
      pass: false,
      configured: config.recordId,
      taskSpec: taskRecordId,
      note: "v37A live discovery validated alternate ID — using configured recordId from factory config",
    });
  } else {
    checks.push({ check: "record_id", pass: true, value: config.recordId });
  }

  if (brandSlug === "hotel-indigo") {
    const expected = {
      slug: "hotel-indigo",
      brandModelType: "lifestyle_full_brand",
      copyGovernanceMode: "lifestyle_full_brand",
      geographicFallbackRule: "cala_first_then_us_then_global",
    };
    for (const [k, v] of Object.entries(expected)) {
      const pass = config[k] === v;
      checks.push({ check: k, pass, expected: v, actual: config[k] });
      if (!pass) issues.push(`config_mismatch:${k}`);
    }
    if (parentNaming && config.parentCompany !== parentNaming.parentCompanyOfficialName) {
      checks.push({
        check: "parentCompanyOfficialName",
        pass: false,
        expected: parentNaming.parentCompanyOfficialName,
        actual: config.parentCompany,
      });
      issues.push("parent_company_naming_review");
    }
  }

  if (brandSlug === "mgallery-collection") {
    const expected = {
      slug: "mgallery-collection",
      brandModelType: "soft_brand_collection",
      copyGovernanceMode: "soft_brand_collection",
      geographicFallbackRule: "cala_first_then_us_then_global",
    };
    for (const [k, v] of Object.entries(expected)) {
      const pass = config[k] === v;
      checks.push({ check: k, pass, expected: v, actual: config[k] });
      if (!pass) issues.push(`config_mismatch:${k}`);
    }
    if (v37aBrandResult?.modelClassification?.selectedBrandModelType === "soft_brand_collection") {
      checks.push({ check: "v37a_model_match", pass: true });
    }
  }

  if (!config.propertyCatalog?.length) issues.push("missing_property_catalog");
  if (!config.propertyExampleCatalog?.length) issues.push("missing_property_example_catalog");

  return {
    ok: issues.length === 0,
    slug: brandSlug,
    config: {
      slug: config.slug,
      recordId: config.recordId,
      name: config.name,
      parentCompany: config.parentCompany,
      brandModelType: config.brandModelType,
      copyGovernanceMode: config.copyGovernanceMode,
      propertyExampleCount: config.propertyExampleCatalog?.length || 0,
      geographicFallbackRule: config.geographicFallbackRule,
    },
    parentNaming,
    checks,
    issues,
  };
}

export function reviewBrandImages(brandSlug) {
  const visualPack = loadJsonIfExists(`reports/visual-asset-pack-${brandSlug}-v37a.json`);
  const candidates = [];
  const rejected = [];
  const replacementNeeded = [];

  if (!visualPack) {
    return {
      brandSlug,
      status: "missing_v37a_visual_pack",
      candidates,
      rejected,
      replacementNeeded,
      sectionLabelRecommendation: null,
      propertyExamplesWithSpecificImagery: 0,
    };
  }

  const gallery = visualPack.gallery || [];
  const openings = visualPack.propertyExamples || visualPack.openings || [];
  const seen = new Set();
  const allEntries = [];
  for (const entry of [...gallery, ...openings]) {
    const key = `${nz(entry.propertyName)}|${nz(entry.imageUrl)}|${nz(entry.sourcePageUrl)}`;
    if (seen.has(key)) continue;
    seen.add(key);
    allEntries.push(entry);
  }

  const urlCounts = {};
  for (const entry of allEntries) {
    const u = nz(entry.imageUrl);
    if (u) urlCounts[u] = (urlCounts[u] || 0) + 1;
  }

  for (const entry of allEntries) {
    const imageUrl = nz(entry.imageUrl);
    const propertyName = nz(entry.propertyName);
    let verdict = "candidate_approved_for_registry_later";
    const reasons = [];

    if (!imageUrl) {
      verdict = "replacement_needed";
      reasons.push("missing_image_url");
    } else if (isLogoImageUrl(imageUrl)) {
      verdict = "candidate_rejected";
      reasons.push("logo_image");
    } else if (isGenericBrandOrLifestyleImageUrl(imageUrl)) {
      verdict = "candidate_rejected";
      reasons.push("generic_lifestyle");
    } else if (brandSlug === "hotel-indigo" && isGenericIhgBrandHeroImage(imageUrl)) {
      verdict = "candidate_rejected";
      reasons.push("generic_ihg_brand_hero");
    } else if (brandSlug === "hotel-indigo" && urlCounts[imageUrl] > 2) {
      verdict = "candidate_rejected";
      reasons.push("shared_ihg_image_across_properties");
    } else if (brandSlug === "mgallery-collection" && isAccorGenericBrandGraphic(imageUrl)) {
      verdict = "candidate_rejected";
      reasons.push("accor_generic_brand_graphic");
    }

    if (/lima miraflores/i.test(propertyName) && verdict !== "candidate_rejected" && brandSlug === "hotel-indigo") {
      if (isGenericIhgBrandHeroImage(imageUrl) || urlCounts[imageUrl] > 1) {
        verdict = "replacement_needed";
        reasons.push("lima_miraflores_requires_property_specific_image");
      }
    }

    const row = {
      propertyName,
      propertyMarket: entry.propertyMarket || entry.marketCity,
      propertyRegion: entry.propertyRegion || entry.geographyLabel,
      sourcePageUrl: entry.sourcePageUrl,
      imageUrl,
      verdict,
      reasons,
    };

    if (verdict === "candidate_approved_for_registry_later") candidates.push(row);
    else if (verdict === "candidate_rejected") rejected.push(row);
    else replacementNeeded.push(row);
  }

  let propertyExamplesWithSpecificImagery = 0;
  const calaProperties = (visualPack.propertyExamples || openings || []).filter((p) =>
    /cala|mexico|peru|argentina|uruguay|brazil/i.test(
      `${p.propertyRegion || ""} ${p.geographyLabel || ""}`
    )
  );
  for (const p of calaProperties) {
    const u = nz(p.imageUrl);
    if (
      u &&
      !isGenericIhgBrandHeroImage(u) &&
      !isAccorGenericBrandGraphic(u) &&
      !(brandSlug === "hotel-indigo" && urlCounts[u] > 2)
    ) {
      propertyExamplesWithSpecificImagery += 1;
    }
  }

  let sectionLabelRecommendation = visualPack.propertyExampleSectionLabel || "Curated CALA examples · Not a full directory";
  if (brandSlug === "hotel-indigo") {
    if (propertyExamplesWithSpecificImagery < 3) {
      sectionLabelRecommendation = "Curated CALA + U.S. examples · Not a full directory";
    }
    if (propertyExamplesWithSpecificImagery < 2) {
      sectionLabelRecommendation = "Curated global examples · Not a full directory";
    }
  }

  return {
    brandSlug,
    status: rejected.length + replacementNeeded.length > 0 ? "image_remediation_required" : "review_complete",
    candidates,
    rejected,
    replacementNeeded,
    sectionLabelRecommendation,
    propertyExamplesWithSpecificImagery,
    calaCandidateCount: visualPack.calaCandidateCount || calaProperties.length,
    incompleteThirdProperty:
      brandSlug === "hotel-indigo"
        ? "Hotel Indigo Lima Miraflores — incomplete unless property-specific hotel image verified"
        : null,
  };
}

function companyValidatedSnapshot(brandBasics) {
  const fields = brandBasics?.fields || brandBasics || {};
  return {
    companyValidated: fields["Company Validated"] ?? null,
    companyValidationDate: fields["Company Validation Date"] ?? null,
  };
}

async function fetchAllBrandSources(brandRecordId) {
  const all = [];
  let offset = "";
  do {
    const page = await listPartnerSources({ brandId: brandRecordId, limit: 100, offset });
    all.push(...(page.sources || []));
    offset = page.offset || "";
  } while (offset);
  return all;
}

function findExistingByUrl(sources, url) {
  const key = normalizeUrlKey(url);
  return sources.find((s) => normalizeUrlKey(s.sourceUrl) === key) || null;
}

async function processBrandSourceSeeding(brandTarget, options = {}) {
  const brandConfig = getActiveProfileBrandConfig(brandTarget.slug);
  const brandMeta = {
    slug: brandTarget.slug,
    name: brandTarget.exactNames?.[0] || brandConfig?.name,
    recordId: brandConfig?.recordId || brandTarget.recordId,
    modelType: brandConfig?.brandModelType,
    copyGuidance:
      brandTarget.slug === "hotel-indigo"
        ? "Lifestyle full brand — IHG parent context only; no InterContinental brand confusion; no generic boutique copy."
        : "Soft-brand collection — Accor context only; no Handwritten/Pullman/Sofitel confusion.",
  };

  const catalog = (brandTarget.officialSources || []).map((s) =>
    mapV37ASourceToCatalogEntry(s, brandMeta)
  );

  const brandBasics = await fetchBrandBasics(brandMeta.recordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasics);
  const existingSources = await fetchAllBrandSources(brandMeta.recordId);

  const probedSources = [];
  for (const entry of catalog) {
    probedSources.push(await probeSourceUrl(entry));
    await new Promise((r) => setTimeout(r, 150));
  }

  const proposedCreates = [];
  const proposedUpdates = [];
  const blockedProposals = [];

  for (const source of probedSources) {
    const brandGate = validateSourceForBrand(source, brandTarget.slug);
    if (!brandGate.ok) {
      blockedProposals.push({ role: source.role, reason: "brand_source_gate", blockers: brandGate.blockers, source });
      continue;
    }
    if (!source.registerRecommended) {
      blockedProposals.push({ role: source.role, reason: "unreachable_or_failed_probe", source });
      continue;
    }

    const existing = findExistingByUrl(existingSources, source.finalUrl || source.sourceUrl);
    const plan = buildSourceLibraryRowPlan(
      {
        ...source,
        sourceUrl: source.finalUrl || source.sourceUrl,
        confidenceLevel: source.jsShellRisk === "high" ? "Low" : source.confidenceLevel || "High",
      },
      brandMeta.recordId,
      brandMeta
    );

    if (!plan.ok) {
      blockedProposals.push({ role: source.role, reason: "validation_failed", errors: plan.errors });
      continue;
    }

    const notes = nz(plan.fields?.[MAP_PARTNER_SOURCE.notes]);
    if (/company validated|company-approved/i.test(notes)) {
      blockedProposals.push({ role: source.role, reason: "company_validation_language_in_notes" });
      continue;
    }

    if (existing) {
      proposedUpdates.push({
        action: "patch_metadata",
        recordId: existing.id,
        role: source.role,
        sourceUrl: source.sourceUrl,
        fields: {
          [MAP_PARTNER_SOURCE.notes]: plan.fields[MAP_PARTNER_SOURCE.notes],
          [MAP_PARTNER_SOURCE.lastReviewed]: plan.fields[MAP_PARTNER_SOURCE.lastReviewed],
          [MAP_PARTNER_SOURCE.approvedForExplorerUse]: plan.fields[MAP_PARTNER_SOURCE.approvedForExplorerUse],
          [MAP_PARTNER_SOURCE.sourceQuality]: plan.fields[MAP_PARTNER_SOURCE.sourceQuality],
        },
        governance: plan.governance,
      });
    } else {
      proposedCreates.push({
        action: "create",
        role: source.role,
        sourceUrl: source.sourceUrl,
        fields: plan.fields,
        governance: plan.governance,
      });
    }
  }

  return {
    slug: brandTarget.slug,
    name: brandMeta.name,
    recordId: brandMeta.recordId,
    modelType: brandMeta.modelType,
    brandConfigPresent: Boolean(brandConfig),
    companyValidatedBefore,
    existingSourceCount: existingSources.length,
    sourceDiscovery: probedSources,
    proposedCreates,
    proposedUpdates,
    blockedProposals,
  };
}

export function runNpmStage(commandParts, cwd = ROOT) {
  const res = spawnSync("npm", commandParts, {
    cwd,
    shell: true,
    encoding: "utf8",
    env: process.env,
    maxBuffer: 10 * 1024 * 1024,
  });
  return {
    command: `npm ${commandParts.join(" ")}`,
    status: res.status === 0 ? "pass" : "fail",
    exitCode: res.status,
    stdoutTail: (res.stdout || "").slice(-2000),
    stderrTail: (res.stderr || "").slice(-2000),
  };
}

export function runPostSeedContractRuns(brands) {
  const brandList = brands.join(",");
  const runs = {
    v36b: runNpmStage([
      "run",
      "brand-explorer-v36b-contract-validation",
      "--",
      "--brands",
      brandList,
      "--dry-run",
    ]),
    v36c: runNpmStage([
      "run",
      "brand-explorer-v36c-remediation-planner",
      "--",
      "--brands",
      brandList,
      "--dry-run",
    ]),
    perBrand: Object.fromEntries(
      brands.flatMap((slug) => [
        [
          `${slug}.preflight`,
          runNpmStage(["run", "brand-explorer-active-profile-preflight", "--", "--brand", slug, "--dry-run"]),
        ],
        [
          `${slug}.assetPack`,
          runNpmStage(["run", "brand-explorer-active-profile-asset-pack", "--", "--brand", slug, "--dry-run"]),
        ],
        [
          `${slug}.buildDraft`,
          runNpmStage(["run", "brand-explorer-active-profile-build-draft", "--", "--brand", slug, "--dry-run"]),
        ],
      ])
    ),
  };

  const v36bReport = loadJsonIfExists("reports/brand-explorer-v36b-contract-validation.json");
  const v36cReport = loadJsonIfExists("reports/brand-explorer-v36c-remediation-planner.json");

  return { ...runs, v36bReport, v36cReport };
}

export function determineDraftApplyEligibility(brandResult, { dryRun = true } = {}) {
  const {
    configValidation,
    sourceSeeding,
    imageReview,
    v36b,
    v36c,
    factoryStages,
    sourcesApplied,
  } = brandResult;

  const blockers = [];
  const founderExceptions = [];
  const slug = brandResult.slug;

  if (!configValidation?.ok) blockers.push("config_remediation_required");
  if (imageReview?.status === "image_remediation_required") blockers.push("image_remediation_required");

  const blockedProposals = sourceSeeding?.blockedProposals || [];
  const gateBlocked = blockedProposals.filter((b) =>
    ["brand_source_gate", "validation_failed", "company_validation_language_in_notes"].includes(b.reason)
  );
  const unreachableBlocked = blockedProposals.filter((b) => b.reason === "unreachable_or_failed_probe");
  const proposedCreates = sourceSeeding?.proposedCreates?.length || 0;
  const existingSources = sourceSeeding?.existingSourceCount || 0;

  if (gateBlocked.length) blockers.push("source_remediation_required");
  if (!sourcesApplied && proposedCreates === 0 && existingSources < 3) {
    blockers.push("source_remediation_required");
  }

  if (v36b?.status === "fail" || v36b?.pass === false) blockers.push("v36b_contract_fail");
  if (v36c?.founderReviewRequired) founderExceptions.push("v36c_founder_review_items");

  const preflight = factoryStages?.[`${slug}.preflight`];
  const assetPack = factoryStages?.[`${slug}.assetPack`];
  const buildDraft = factoryStages?.[`${slug}.buildDraft`];

  if (preflight?.status === "fail") blockers.push("preflight_fail");
  if (assetPack?.status === "fail") blockers.push("asset_pack_fail");
  if (buildDraft?.status === "fail") blockers.push("build_draft_fail");

  let allowedNextAction = "no_apply_allowed";
  if (blockers.includes("config_remediation_required")) {
    allowedNextAction = "config_remediation_required";
  } else if (blockers.includes("source_remediation_required")) {
    allowedNextAction = "source_remediation_required";
  } else if (blockers.includes("image_remediation_required")) {
    allowedNextAction = "image_remediation_required";
  } else if (founderExceptions.length) {
    allowedNextAction = "founder_exception_review_required";
  } else if (
    (sourcesApplied || proposedCreates > 0) &&
    preflight?.status === "pass" &&
    assetPack?.status === "pass" &&
    buildDraft?.status === "pass"
  ) {
    allowedNextAction = "apply_draft_allowed";
  } else if (!sourcesApplied && proposedCreates > 0 && dryRun) {
    allowedNextAction = "source_seed_apply_required";
  } else if (preflight?.status === "pass" && buildDraft?.status === "pass") {
    allowedNextAction = "apply_draft_allowed_with_caveats";
  }

  const applySourceCommand =
    "npm run brand-explorer-v37b-lifestyle-batch-source-seeding -- " +
    `--brands ${slug} --apply --approve-brand-explorer-v37B-lifestyle-batch-source-seeding ` +
    "--confirm-source-library-only --confirm-no-company-validation-claim " +
    "--confirm-no-presentation-row-changes --confirm-no-registry-changes " +
    "--confirm-no-image-field-changes --confirm-no-active-profile-approval " +
    "--confirm-hotel-indigo-mgallery-only";

  let recommendedNextCommand = null;
  if (allowedNextAction === "apply_draft_allowed" || allowedNextAction === "apply_draft_allowed_with_caveats") {
    recommendedNextCommand = `npm run brand-explorer-active-profile-apply-draft -- --brand ${slug} --dry-run`;
  } else if (allowedNextAction === "source_seed_apply_required" || (dryRun && proposedCreates > 0 && !sourcesApplied)) {
    recommendedNextCommand = applySourceCommand;
  } else if (allowedNextAction === "source_remediation_required") {
    recommendedNextCommand = `npm run brand-explorer-v37b-lifestyle-batch-source-seeding -- --brands ${slug} --dry-run`;
  } else if (allowedNextAction === "image_remediation_required") {
    recommendedNextCommand = `# Image remediation — review reports/brand-explorer-v37b-${slug === "hotel-indigo" ? "hotel-indigo" : "mgallery"}-image-review.md`;
    if (proposedCreates > 0 && !sourcesApplied) {
      recommendedNextCommand = `${applySourceCommand}\n# Then image remediation per image review report`;
    }
  }

  return {
    draftApplyEligible: allowedNextAction.startsWith("apply_draft"),
    readyForActiveApproval: false,
    allowedNextAction,
    blockers: [...new Set(blockers)],
    founderReviewExceptions: founderExceptions,
    sourcePlan: {
      proposedCreates,
      existingSources,
      unreachableBlocked: unreachableBlocked.length,
      gateBlocked: gateBlocked.length,
    },
    recommendedNextCommand,
  };
}

function buildApplyCommand(brands = DEFAULT_V37B_BRANDS) {
  return [
    "npm run brand-explorer-v37b-lifestyle-batch-source-seeding --",
    `--brands ${brands.join(",")}`,
    "--apply",
    APPLY_FLAG_APPROVE,
    APPLY_FLAG_SOURCE_ONLY,
    APPLY_FLAG_NO_VALIDATION,
    APPLY_FLAG_NO_PRESENTATION,
    APPLY_FLAG_NO_REGISTRY,
    APPLY_FLAG_NO_IMAGE_FIELDS,
    APPLY_FLAG_NO_ACTIVE_APPROVAL,
    APPLY_FLAG_BRANDS_ONLY,
  ].join(" ");
}

function buildHotelIndigoConfigMarkdown(configValidation) {
  const lines = [];
  lines.push("# Hotel Indigo — v37B Factory Config Registration");
  lines.push("");
  lines.push("## Config status");
  lines.push(`- Registered: **${configValidation.ok ? "yes" : "needs review"}**`);
  lines.push(`- Slug: \`hotel-indigo\``);
  lines.push(`- Record ID: \`${configValidation.config?.recordId}\``);
  lines.push(`- Model: \`${configValidation.config?.brandModelType}\``);
  lines.push(`- Copy governance: \`${configValidation.config?.copyGovernanceMode}\``);
  lines.push("");
  lines.push("## Parent naming (official sources — not external copy)");
  const pn = configValidation.parentNaming;
  if (pn) {
    lines.push(`- **Corporate entity:** ${pn.parentCompanyOfficialName}`);
    lines.push(`- **Consumer label:** ${pn.consumerParentLabel}`);
    lines.push(`- **Note:** ${pn.note}`);
    lines.push("- Validated from:");
    for (const u of pn.validatedFrom) lines.push(`  - ${u}`);
  }
  lines.push("");
  lines.push("## Property example priority");
  lines.push("- CALA first (Guanajuato, Guadalajara Expo, Lima Miraflores)");
  lines.push("- U.S. fallback (Nashville)");
  lines.push("- Global expansion only when CALA/U.S. imagery insufficient");
  lines.push("");
  lines.push("## Language rules");
  lines.push("- Use: lifestyle hotel brand; neighborhood/local discovery (source-supported); IHG system/distribution/loyalty (source-supported)");
  lines.push("- Avoid: generic boutique copy; InterContinental brand confusion; luxury soft-brand language; unsupported loyalty/performance claims");
  if (configValidation.issues?.length) {
    lines.push("");
    lines.push("## Issues");
    for (const i of configValidation.issues) lines.push(`- ${i}`);
  }
  return lines.join("\n");
}

function buildImageReviewMarkdown(review, brandLabel) {
  const lines = [];
  lines.push(`# ${brandLabel} — v37B Image Risk Review`);
  lines.push("");
  lines.push(`Status: **${review.status}**`);
  if (review.incompleteThirdProperty) {
    lines.push("");
    lines.push(`## Incomplete property example`);
    lines.push(`- ${review.incompleteThirdProperty}`);
  }
  lines.push("");
  lines.push(`## Section label recommendation`);
  lines.push(`- ${review.sectionLabelRecommendation}`);
  lines.push(`- CALA candidates: ${review.calaCandidateCount}`);
  lines.push(`- Property-specific imagery count: ${review.propertyExamplesWithSpecificImagery}`);
  lines.push("");
  lines.push("## Candidate approved for registry later");
  for (const c of review.candidates) {
    lines.push(`- **${c.propertyName}** — ${c.imageUrl || "no url"}`);
  }
  if (!review.candidates.length) lines.push("- _(none)_");
  lines.push("");
  lines.push("## Candidate rejected");
  for (const c of review.rejected) {
    lines.push(`- **${c.propertyName}** — ${c.reasons.join(", ")} — ${c.imageUrl}`);
  }
  if (!review.rejected.length) lines.push("- _(none)_");
  lines.push("");
  lines.push("## Replacement needed");
  for (const c of review.replacementNeeded) {
    lines.push(`- **${c.propertyName}** — ${c.reasons.join(", ")}`);
  }
  if (!review.replacementNeeded.length) lines.push("- _(none)_");
  return lines.join("\n");
}

export async function runV37BLifestyleBatchSourceSeeding(options = {}) {
  const slugs = (options.brands || DEFAULT_V37B_BRANDS).map((s) => nz(s).toLowerCase()).filter(Boolean);
  const apply = Boolean(options.apply);
  const dryRun = !apply;

  const applyBlockers = [];
  if (apply && !options.approveBatch) applyBlockers.push(`missing_${APPLY_FLAG_APPROVE}`);
  if (apply && !options.sourceOnly) applyBlockers.push(`missing_${APPLY_FLAG_SOURCE_ONLY}`);
  if (apply && !options.noValidationClaim) applyBlockers.push(`missing_${APPLY_FLAG_NO_VALIDATION}`);
  if (apply && !options.noPresentation) applyBlockers.push(`missing_${APPLY_FLAG_NO_PRESENTATION}`);
  if (apply && !options.noRegistry) applyBlockers.push(`missing_${APPLY_FLAG_NO_REGISTRY}`);
  if (apply && !options.noImageFields) applyBlockers.push(`missing_${APPLY_FLAG_NO_IMAGE_FIELDS}`);
  if (apply && !options.noActiveApproval) applyBlockers.push(`missing_${APPLY_FLAG_NO_ACTIVE_APPROVAL}`);
  if (apply && !options.brandsOnly) applyBlockers.push(`missing_${APPLY_FLAG_BRANDS_ONLY}`);

  const invalidSlugs = slugs.filter((s) => !DEFAULT_V37B_BRANDS.includes(s));
  if (invalidSlugs.length) applyBlockers.push(`invalid_brand_slug:${invalidSlugs.join(",")}`);

  const v37aReport = loadJsonIfExists("reports/brand-explorer-v37a-lifestyle-batch-intake.json");
  const brandResults = [];

  for (const slug of slugs) {
    const brandTarget = V37A_BRAND_TARGETS.find((b) => b.slug === slug);
    if (!brandTarget) {
      brandResults.push({ slug, error: "missing_v37a_brand_target" });
      continue;
    }

    const v37aBrand = v37aReport?.brandResults?.find((b) => b.brandSlug === slug);
    const configValidation = validateFactoryConfig(slug, v37aBrand);
    const imageReview = reviewBrandImages(slug);
    const sourceSeeding = await processBrandSourceSeeding(brandTarget, options);

    brandResults.push({
      slug,
      brandName: brandTarget.exactNames[0],
      configValidation,
      sourceSeeding,
      imageReview,
      sourcesApplied: false,
      v36b: null,
      v36c: null,
    });
  }

  const postSeedRuns = options.skipPostSeedRuns
    ? { v36b: { status: "skipped" }, v36c: { status: "skipped" }, perBrand: {}, v36bReport: loadJsonIfExists("reports/brand-explorer-v36b-contract-validation.json"), v36cReport: loadJsonIfExists("reports/brand-explorer-v36c-remediation-planner.json") }
    : runPostSeedContractRuns(slugs);
  const v36bReport = postSeedRuns.v36bReport;
  const v36cReport = postSeedRuns.v36cReport;

  for (const result of brandResults) {
    const v36bBrand = v36bReport?.brandResults?.find((b) => b.brandSlug === result.slug || b.slug === result.slug);
    result.v36b = v36bBrand
      ? { pass: v36bBrand.pass !== false, status: v36bBrand.pass === false ? "fail" : "pass", summary: v36bBrand }
      : {
          pass: postSeedRuns.v36b?.status === "pass",
          status: postSeedRuns.v36b?.status || "unknown",
          summary: v36bReport,
        };

    const v36cBrand = v36cReport?.brandResults?.find((b) => b.brandSlug === result.slug || b.slug === result.slug);
    result.v36c = v36cBrand
      ? {
          founderReviewRequired: Boolean(v36cBrand.founderReviewRequired || v36cBrand.remediationItems?.length),
          summary: v36cBrand,
        }
      : { founderReviewRequired: false, summary: v36cReport };

    result.factoryStages = {
      preflight: postSeedRuns.perBrand[`${result.slug}.preflight`],
      assetPack: postSeedRuns.perBrand[`${result.slug}.assetPack`],
      buildDraft: postSeedRuns.perBrand[`${result.slug}.buildDraft`],
    };
  }

  const allCreates = brandResults.flatMap((b) => b.sourceSeeding?.proposedCreates || []);
  const allUpdates = brandResults.flatMap((b) => b.sourceSeeding?.proposedUpdates || []);
  const canApply = apply && applyBlockers.length === 0 && (allCreates.length > 0 || allUpdates.length > 0);

  let applyResults = { created: [], updated: [], errors: [] };
  let airtableModified = false;

  if (canApply) {
    for (const result of brandResults) {
      const seeding = result.sourceSeeding;
      const before = seeding.companyValidatedBefore;
      for (const create of seeding.proposedCreates) {
        try {
          const created = await createPartnerSource(create.fields);
          applyResults.created.push({
            slug: result.slug,
            recordId: created.id,
            role: create.role,
            sourceUrl: create.sourceUrl,
          });
          airtableModified = true;
          await new Promise((r) => setTimeout(r, 220));
        } catch (err) {
          applyResults.errors.push({ slug: result.slug, role: create.role, message: err.message });
        }
      }
      for (const update of seeding.proposedUpdates) {
        try {
          const patched = await patchPartnerSource(update.recordId, update.fields);
          applyResults.updated.push({
            slug: result.slug,
            recordId: patched.id,
            role: update.role,
          });
          airtableModified = true;
          await new Promise((r) => setTimeout(r, 220));
        } catch (err) {
          applyResults.errors.push({ slug: result.slug, role: update.role, message: err.message });
        }
      }
      const afterBasics = await fetchBrandBasics(seeding.recordId);
      const after = companyValidatedSnapshot(afterBasics);
      if (
        before.companyValidated !== after.companyValidated ||
        before.companyValidationDate !== after.companyValidationDate
      ) {
        applyBlockers.push(`company_validated_changed:${result.slug}`);
      }
      if (applyResults.created.some((c) => c.slug === result.slug) || applyResults.updated.some((u) => u.slug === result.slug)) {
        result.sourcesApplied = true;
      }
    }
  }

  for (const result of brandResults) {
    result.readiness = determineDraftApplyEligibility(
      {
        ...result,
        factoryStages: postSeedRuns.perBrand,
        sourcesApplied: result.sourcesApplied,
      },
      { dryRun }
    );
  }

  const presentationPlanReadiness = Object.fromEntries(
    slugs.map((slug) => {
      const plan = loadJsonIfExists(`reports/presentation-plan-${slug}-v37a.json`);
      return [
        slug,
        {
          mode: plan?.mode || "unknown",
          rowCount: plan?.rows?.length || 0,
          planReady: Boolean(plan?.rows?.length),
        },
      ];
    })
  );

  return {
    version: V37B_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun,
    applyRequested: apply,
    canApply,
    applyBlockers,
    applyResults,
    airtableModified,
    guardrails: {
      sourceLibraryOnly: true,
      noPresentationWrites: true,
      noRegistryWrites: true,
      noImageFieldWrites: true,
      noCompanyValidatedChanges: true,
      noActiveProfileApproval: true,
      readyForActiveApproval: false,
    },
    applyCommand: buildApplyCommand(slugs),
    brandResults,
    postSeedRuns,
    presentationPlanReadiness,
    summary: {
      brandsProcessed: brandResults.length,
      sourcesProposedCreates: allCreates.length,
      sourcesProposedUpdates: allUpdates.length,
      sourcesBlocked: brandResults.reduce((n, b) => n + (b.sourceSeeding?.blockedProposals?.length || 0), 0),
      sourcesCreated: applyResults.created.length,
      sourcesUpdated: applyResults.updated.length,
      configRegistered: brandResults.filter((b) => b.configValidation?.ok).map((b) => b.slug),
    },
  };
}

export function writeV37BReports(report, rootDir = ROOT) {
  const reportsDir = path.join(rootDir, "reports");
  const docsDir = path.join(rootDir, "docs/data-intelligence");
  fs.mkdirSync(reportsDir, { recursive: true });
  fs.mkdirSync(docsDir, { recursive: true });

  const jsonPath = path.join(reportsDir, REPORT_JSON);
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));

  const mdLines = [];
  mdLines.push("# Brand Explorer v37B — Lifestyle Batch Source Seeding");
  mdLines.push("");
  mdLines.push(`Generated: ${report.generatedAt}`);
  mdLines.push(`Mode: **${report.dryRun ? "dry-run" : "apply"}**`);
  mdLines.push("");
  mdLines.push("## Summary");
  mdLines.push(`- Brands: ${report.summary.brandsProcessed}`);
  mdLines.push(`- Source creates proposed: ${report.summary.sourcesProposedCreates}`);
  mdLines.push(`- Source updates proposed: ${report.summary.sourcesProposedUpdates}`);
  mdLines.push(`- Sources blocked: ${report.summary.sourcesBlocked}`);
  mdLines.push(`- Config registered: ${report.summary.configRegistered.join(", ") || "none"}`);
  mdLines.push("");
  mdLines.push("## Guardrails");
  for (const [k, v] of Object.entries(report.guardrails)) {
    mdLines.push(`- ${k}: ${v}`);
  }
  mdLines.push("");
  mdLines.push("## Batch readiness");
  for (const brand of report.brandResults) {
    mdLines.push(`### ${brand.brandName || brand.slug}`);
    mdLines.push(`- Config: ${brand.configValidation?.ok ? "pass" : "fail"}`);
    mdLines.push(`- Sources create/update: ${brand.sourceSeeding?.proposedCreates?.length || 0}/${brand.sourceSeeding?.proposedUpdates?.length || 0}`);
    mdLines.push(`- Image review: ${brand.imageReview?.status}`);
    mdLines.push(`- v36B: ${brand.v36b?.status}`);
    mdLines.push(`- Draft apply: **${brand.readiness?.allowedNextAction}**`);
    mdLines.push(`- Next: ${brand.readiness?.recommendedNextCommand || "—"}`);
    mdLines.push("");
  }
  if (report.applyBlockers?.length) {
    mdLines.push("## Apply blockers");
    for (const b of report.applyBlockers) mdLines.push(`- ${b}`);
  }
  mdLines.push("");
  mdLines.push("## Apply command (Source Library only)");
  mdLines.push("```");
  mdLines.push(report.applyCommand);
  mdLines.push("```");

  const mdPath = path.join(reportsDir, REPORT_MD);
  fs.writeFileSync(mdPath, mdLines.join("\n"));

  const perBrand = {};
  for (const brand of report.brandResults) {
    if (brand.slug === "hotel-indigo") {
      const configMd = buildHotelIndigoConfigMarkdown(brand.configValidation);
      const configPath = path.join(reportsDir, "brand-explorer-v37b-hotel-indigo-config.md");
      fs.writeFileSync(configPath, configMd);
      perBrand.hotelIndigoConfig = configPath;
    }
    const imageMd = buildImageReviewMarkdown(
      brand.imageReview,
      brand.brandName || brand.slug
    );
    const imageFile =
      brand.slug === "hotel-indigo"
        ? "brand-explorer-v37b-hotel-indigo-image-review.md"
        : "brand-explorer-v37b-mgallery-image-review.md";
    const imagePath = path.join(reportsDir, imageFile);
    fs.writeFileSync(imagePath, imageMd);
    perBrand[`${brand.slug}ImageReview`] = imagePath;
  }

  const docLines = [];
  docLines.push("# Brand Explorer v37B — Lifestyle Batch Source Seeding");
  docLines.push("");
  docLines.push("## Purpose");
  docLines.push("Register Hotel Indigo factory config, validate MGallery config, seed Source Library rows only, and produce batch readiness for draft-apply eligibility.");
  docLines.push("");
  docLines.push("## Command");
  docLines.push("```bash");
  docLines.push("npm run brand-explorer-v37b-lifestyle-batch-source-seeding -- --brands hotel-indigo,mgallery-collection --dry-run");
  docLines.push("```");
  docLines.push("");
  docLines.push("## Apply gates (Source Library only)");
  docLines.push(`- \`${APPLY_FLAG_APPROVE}\``);
  docLines.push(`- \`${APPLY_FLAG_SOURCE_ONLY}\``);
  docLines.push(`- \`${APPLY_FLAG_NO_VALIDATION}\``);
  docLines.push(`- \`${APPLY_FLAG_NO_PRESENTATION}\``);
  docLines.push(`- \`${APPLY_FLAG_NO_REGISTRY}\``);
  docLines.push(`- \`${APPLY_FLAG_NO_IMAGE_FIELDS}\``);
  docLines.push(`- \`${APPLY_FLAG_NO_ACTIVE_APPROVAL}\``);
  docLines.push(`- \`${APPLY_FLAG_BRANDS_ONLY}\``);
  docLines.push("");
  docLines.push("## Outputs");
  docLines.push(`- \`reports/${REPORT_JSON}\``);
  docLines.push(`- \`reports/${REPORT_MD}\``);
  docLines.push("- `reports/brand-explorer-v37b-hotel-indigo-config.md`");
  docLines.push("- `reports/brand-explorer-v37b-hotel-indigo-image-review.md`");
  docLines.push("- `reports/brand-explorer-v37b-mgallery-image-review.md`");

  const docPath = path.join(docsDir, path.basename(DOC_MD));
  fs.writeFileSync(docPath, docLines.join("\n"));

  return { jsonPath, mdPath, docPath, perBrand };
}
