/**
 * v43 — Brand Explorer Active Release Apply.
 *
 * Discovers live Brand Basics release fields, gates unlock, and (when approved)
 * writes only active-profile / founder-review release fields — never content,
 * Company Validated, Source Library, Registry, or images.
 *
 * Default: dry-run / read-only.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getActiveProfileBrandConfig } from "./brand-explorer-active-profile-brand-config.js";
import { getDiscoveryBrandConfig } from "./brand-explorer-brand-asset-image-governance.js";
import { loadBrandFactoryContext } from "./brand-explorer-active-profile-factory.js";
import { evaluateBrandExternalQualityLock } from "./brand-explorer-display-quality-lock.js";
import { renderBrandExplorerHtmlForTest } from "./brand-explorer-atelier-render-test-loader.js";
import { resolveBrandExplorerDisplayState } from "./brand-explorer-display-state.js";
import { scanForbiddenLanguage } from "./brand-explorer-v40b-copy-quality-patterns.js";
import { scanInternalPreviewOwnerCopy } from "./brand-explorer-economics-chrome-remediation.js";
import { evaluateBrandExplorerOsBrand } from "./brand-explorer-os-run.js";
import {
  INCOMPLETE_CONTROL_SLUGS,
  PRIMARY_RELEASE_SLUGS,
} from "./brand-explorer-os-state-machine.js";
import { metaFetch } from "./brand-residences-status-setup.js";

export const V43_VERSION = "v43";

export const V43_FIRST_RELEASE = Object.freeze(["radisson-individuals-by-choice"]);
export const V43_SECOND_BATCH = Object.freeze(["everhome-suites", "kimpton"]);
export const V43_THIRD_BATCH = Object.freeze(["design-hotels"]);
export const V43_FOURTH_BATCH = Object.freeze([
  "hotel-indigo",
  "mgallery-collection",
  "small-luxury-hotels-of-the-world",
]);
export const V43_DEFAULT_BRANDS = V43_FIRST_RELEASE;

/** Remaining incomplete-control brands — never unlock via v43 (empty after fourth batch). */
export const V43_FORBIDDEN_BRANDS = Object.freeze([...INCOMPLETE_CONTROL_SLUGS]);

export const BRAND_BASICS_TABLE = "Brand Setup - Brand Basics";

/** Canonical release field names expected by display-state / OS gates. */
export const V43_RELEASE_FIELD_DEFS = Object.freeze([
  {
    name: "Founder Visual Review Pass",
    type: "checkbox",
    description:
      "Dealality founder visual review passed for Brand Explorer internal preview. Does not imply Company Validated.",
    options: { icon: "check", color: "greenBright" },
  },
  {
    name: "Active Profile Approved",
    type: "checkbox",
    description:
      "Active Brand Explorer profile approved for external owner render when other live gates pass.",
    options: { icon: "check", color: "greenBright" },
  },
  {
    name: "Active Profile Approved Date",
    type: "date",
    description: "Date active Brand Explorer profile approval was set.",
    options: { dateFormat: { name: "iso" } },
  },
  {
    name: "Ready for Active Profile",
    type: "checkbox",
    description:
      "Brand Explorer marked ready for active profile (legacy alias; prefer Active Profile Approved).",
    options: { icon: "check", color: "greenBright" },
  },
]);

export const V43_APPLY_FLAGS = Object.freeze({
  approve: "--approve-brand-explorer-v43-active-release",
  founderPassed: "--confirm-founder-visual-review-passed",
  externalLock: "--confirm-external-quality-lock-passed",
  internalCopy: "--confirm-internal-preview-owner-copy-clean",
  gallery: "--confirm-six-gallery-imageurls",
  openings: "--confirm-three-property-example-imageurls",
  noCompanyValidation: "--confirm-no-company-validation-claim",
  noContentWrites: "--confirm-no-content-writes",
  noSourceLibrary: "--confirm-no-source-library-changes",
  noRegistry: "--confirm-no-registry-changes",
  brandOnly: "--confirm-brand-only",
  createMissingFields: "--confirm-create-missing-release-fields",
});

export const REPORT_JSON = "brand-explorer-v43-active-release-apply.json";
export const REPORT_MD = "brand-explorer-v43-active-release-apply.md";

const GALLERY_MIN = 6;
const PROPERTY_MIN = 3;

const FORBIDDEN_WRITE_FIELDS = Object.freeze([
  "Company Validated",
  "Company Validation Date",
  "Validation Status",
  "Title",
  "Body",
  "Image",
  "Images",
  "Scenario Image",
]);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function resolveConfig(slug) {
  return getActiveProfileBrandConfig(slug) || getDiscoveryBrandConfig(slug) || null;
}

function readJsonIfExists(p) {
  if (!fs.existsSync(p)) return null;
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch (err) {
    console.warn(`[v43] failed to parse ${p}: ${err.message}`);
    return null;
  }
}

function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}

function stripHtml(html) {
  return nz(html)
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/\s(?:href|src|srcset|data-src)=["'][^"']*["']/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/\s+/g, " ")
    .trim();
}

function countGallery(blocks = []) {
  return blocks.filter((b) => /^materials\.gallery\.\d+$/.test(nz(b.slotKey)) && nz(b.imageUrl)).length;
}

function countOpenings(blocks = []) {
  return blocks.filter((b) => nz(b.slotKey) === "footprint.openings" && nz(b.imageUrl)).length;
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
    },
  };
  await getBrandLibraryBrandById({ query: { brandId: slug }, headers: {} }, res);
  if (res.statusCode !== 200 || !res.payload?.brand) {
    throw new Error(`brand API fetch failed for ${slug}: HTTP ${res.statusCode}`);
  }
  return res.payload.brand;
}

/**
 * Discover which Brand Basics fields exist for release / display control.
 */
export async function discoverBrandBasicsReleaseFields() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const { res, json } = await metaFetch(baseId, apiKey, "/tables");
  if (!res.ok) throw new Error(`Airtable meta tables failed: ${res.status}`);

  const table = (json.tables || []).find((t) => t.name === BRAND_BASICS_TABLE);
  if (!table) throw new Error(`Table not found: ${BRAND_BASICS_TABLE}`);

  const fields = (table.fields || []).map((f) => ({
    id: f.id,
    name: f.name,
    type: f.type,
  }));
  const byName = new Map(fields.map((f) => [f.name, f]));

  const expected = V43_RELEASE_FIELD_DEFS.map((def) => {
    const live = byName.get(def.name);
    return {
      name: def.name,
      expectedType: def.type,
      exists: Boolean(live),
      liveType: live?.type || null,
      liveId: live?.id || null,
      role:
        def.name === "Founder Visual Review Pass"
          ? "founder_visual_review_passed"
          : def.name === "Active Profile Approved" || def.name === "Ready for Active Profile"
            ? "active_profile_approval"
            : def.name === "Active Profile Approved Date"
              ? "release_timestamp"
              : "release_support",
    };
  });

  const relatedLive = fields.filter((f) =>
    /active|founder|validat|ready|release|approval|display|company|explorer/i.test(f.name)
  );

  const missing = expected.filter((e) => !e.exists);
  const present = expected.filter((e) => e.exists);

  return {
    table: BRAND_BASICS_TABLE,
    tableId: table.id,
    fieldCount: fields.length,
    expectedReleaseFields: expected,
    presentReleaseFields: present.map((p) => p.name),
    missingReleaseFields: missing.map((m) => m.name),
    relatedLiveFields: relatedLive,
    companyValidatedFieldExists: byName.has("Company Validated"),
    companyValidationDateExists: byName.has("Company Validation Date"),
    canWriteReleaseWithoutSchemaCreate: missing.length === 0,
    ensurePlan: missing.map((m) => {
      const def = V43_RELEASE_FIELD_DEFS.find((d) => d.name === m.name);
      return {
        name: def.name,
        type: def.type,
        description: def.description,
        options: def.options || undefined,
      };
    }),
    note:
      missing.length === 0
        ? "All expected release fields exist on Brand Basics."
        : "Expected Active Profile / Founder Visual Review fields are missing on Brand Basics. v43 can create them only with --confirm-create-missing-release-fields during apply.",
  };
}

export async function ensureMissingReleaseFields({ dryRun = true } = {}) {
  const discovery = await discoverBrandBasicsReleaseFields();
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  const created = [];
  const failed = [];
  const wouldCreate = [...(discovery.ensurePlan || [])];

  if (dryRun || !wouldCreate.length) {
    return { ...discovery, created, failed, wouldCreate, schemaModified: false };
  }

  for (const fieldDef of wouldCreate) {
    const payload = {
      name: fieldDef.name,
      type: fieldDef.type,
      ...(fieldDef.description ? { description: fieldDef.description } : {}),
      ...(fieldDef.options ? { options: fieldDef.options } : {}),
    };
    const { res, json } = await metaFetch(baseId, apiKey, `/tables/${discovery.tableId}/fields`, {
      method: "POST",
      body: JSON.stringify(payload),
    });
    if (!res.ok) {
      failed.push({ name: fieldDef.name, error: json?.error?.message || JSON.stringify(json) });
    } else {
      created.push({ name: fieldDef.name, id: json.id, type: fieldDef.type });
    }
  }

  const after = await discoverBrandBasicsReleaseFields();
  return {
    ...after,
    created,
    failed,
    wouldCreate,
    schemaModified: created.length > 0,
  };
}

function readReleaseFieldValues(brandBasicsFields = {}, discovery) {
  const f = brandBasicsFields || {};
  const present = new Set(discovery?.presentReleaseFields || []);
  const val = (name) => (present.has(name) || name in f ? f[name] : undefined);

  return {
    founderVisualReviewPass: val("Founder Visual Review Pass") === true,
    activeProfileApproved: val("Active Profile Approved") === true,
    readyForActiveProfile: val("Ready for Active Profile") === true,
    activeProfileApprovedDate: val("Active Profile Approved Date") || null,
    companyValidated: f["Company Validated"] === true,
    companyValidationDate: f["Company Validation Date"] || null,
    validationStatus: f["Validation Status"] || null,
    externalDisplayStatus: f["External Display Status"] || null,
    brandStatus: f["Brand Status"] || null,
  };
}

function priorFounderRecommendation(brandSlug) {
  const v42 = readJsonIfExists(path.join(ROOT, "reports", "brand-explorer-v42-founder-visual-review.json"));
  const v42a = readJsonIfExists(path.join(ROOT, "reports", "brand-explorer-v42a-founder-minor-cleanup.json"));
  const v42aR1 = readJsonIfExists(
    path.join(ROOT, "reports", "brand-explorer-v42a-r1-design-hotels-minor-cleanup.json")
  );
  const fromV42 = (v42?.brandResults || []).find((b) => b.brandSlug === brandSlug);
  const fromV42a = (v42a?.brandResults || []).find((b) => b.brandSlug === brandSlug);
  const fromV42aR1 =
    v42aR1?.brand === brandSlug || v42aR1?.brandResult?.brandSlug === brandSlug
      ? v42aR1?.brandResult || v42aR1
      : null;
  return {
    v42: fromV42?.releaseRecommendation?.recommendation || null,
    v42a: fromV42a?.projection?.recommendation || null,
    v42aR1: fromV42aR1?.projection?.recommendation || null,
    approveForActiveRelease:
      fromV42?.releaseRecommendation?.recommendation === "approve_for_active_release" ||
      fromV42a?.projection?.recommendation === "approve_for_active_release" ||
      fromV42aR1?.projection?.recommendation === "approve_for_active_release" ||
      (brandSlug === "radisson-individuals-by-choice" &&
        fromV42?.releaseRecommendation?.recommendation === "approve_for_active_release"),
  };
}

function osReleaseEligible(osBrand, priorRec) {
  const state = osBrand?.canonicalState;
  if (["active_release_ready", "active_profile_ready"].includes(state)) {
    return { pass: true, reason: `OS state ${state}` };
  }
  if (state === "founder_review_ready" && priorRec.approveForActiveRelease) {
    return {
      pass: true,
      reason:
        "OS founder_review_ready + prior founder packet recommendation approve_for_active_release (v42/v42A)",
    };
  }
  if (
    state === "founder_review_ready" &&
    (osBrand?.routing?.allowedNextAction === "founder_visual_review" ||
      osBrand?.routing?.allowedNextAction === "apply_active_release")
  ) {
    return {
      pass: true,
      reason: `OS founder_review_ready with next action ${osBrand.routing.allowedNextAction}`,
    };
  }
  return {
    pass: false,
    reason: `OS state ${state || "unknown"} is not release-eligible (need founder_review_ready with approve_for_active_release, or active_release_ready)`,
  };
}

function buildReleasePatch({ discovery, currentValues, founderCliConfirmed }) {
  const fields = {};
  const mapping = [];
  const present = new Set(discovery.presentReleaseFields || []);

  // After ensure, present set may be stale — also allow writing defs we are creating
  const writable = new Set([
    ...present,
    ...(discovery.ensurePlan || []).map((e) => e.name),
    ...(discovery.created || []).map((c) => c.name),
  ]);

  if (writable.has("Founder Visual Review Pass") && (founderCliConfirmed || !currentValues.founderVisualReviewPass)) {
    fields["Founder Visual Review Pass"] = true;
    mapping.push({
      airtableField: "Founder Visual Review Pass",
      before: currentValues.founderVisualReviewPass,
      after: true,
      role: "founder_visual_review_passed",
    });
  }
  if (writable.has("Active Profile Approved")) {
    fields["Active Profile Approved"] = true;
    mapping.push({
      airtableField: "Active Profile Approved",
      before: currentValues.activeProfileApproved,
      after: true,
      role: "active_profile_approval",
    });
  }
  if (writable.has("Ready for Active Profile")) {
    fields["Ready for Active Profile"] = true;
    mapping.push({
      airtableField: "Ready for Active Profile",
      before: currentValues.readyForActiveProfile,
      after: true,
      role: "active_profile_approval_alias",
    });
  }
  if (writable.has("Active Profile Approved Date")) {
    const date = todayIsoDate();
    fields["Active Profile Approved Date"] = date;
    mapping.push({
      airtableField: "Active Profile Approved Date",
      before: currentValues.activeProfileApprovedDate,
      after: date,
      role: "release_timestamp",
    });
  }

  for (const key of Object.keys(fields)) {
    if (FORBIDDEN_WRITE_FIELDS.includes(key)) {
      throw new Error(`Refusing forbidden field in release patch: ${key}`);
    }
  }

  return { fields, mapping };
}

export function parseV43ApplyFlags(argv = []) {
  const has = (flag) => argv.includes(flag);
  return {
    apply: has("--apply"),
    approve: has(V43_APPLY_FLAGS.approve),
    founderPassed: has(V43_APPLY_FLAGS.founderPassed),
    externalLock: has(V43_APPLY_FLAGS.externalLock),
    internalCopy: has(V43_APPLY_FLAGS.internalCopy),
    gallery: has(V43_APPLY_FLAGS.gallery),
    openings: has(V43_APPLY_FLAGS.openings),
    noCompanyValidation: has(V43_APPLY_FLAGS.noCompanyValidation),
    noContentWrites: has(V43_APPLY_FLAGS.noContentWrites),
    noSourceLibrary: has(V43_APPLY_FLAGS.noSourceLibrary),
    noRegistry: has(V43_APPLY_FLAGS.noRegistry),
    brandOnly: has(V43_APPLY_FLAGS.brandOnly),
    createMissingFields: has(V43_APPLY_FLAGS.createMissingFields),
  };
}

export function validateV43ApplyGates({ flags, brands, brandResults, discovery } = {}) {
  const blockers = [];
  if (!flags?.apply) return { allowed: false, blockers: ["not_apply_mode"], missingFlags: [] };

  const required = [
    ["approve", V43_APPLY_FLAGS.approve],
    ["founderPassed", V43_APPLY_FLAGS.founderPassed],
    ["externalLock", V43_APPLY_FLAGS.externalLock],
    ["internalCopy", V43_APPLY_FLAGS.internalCopy],
    ["gallery", V43_APPLY_FLAGS.gallery],
    ["openings", V43_APPLY_FLAGS.openings],
    ["noCompanyValidation", V43_APPLY_FLAGS.noCompanyValidation],
    ["noContentWrites", V43_APPLY_FLAGS.noContentWrites],
    ["noSourceLibrary", V43_APPLY_FLAGS.noSourceLibrary],
    ["noRegistry", V43_APPLY_FLAGS.noRegistry],
    ["brandOnly", V43_APPLY_FLAGS.brandOnly],
  ];
  const missingFlags = required.filter(([k]) => !flags[k]).map(([, flag]) => flag);
  if (missingFlags.length) blockers.push(...missingFlags.map((f) => `missing_flag:${f}`));

  const allowed = new Set([...PRIMARY_RELEASE_SLUGS]);
  const illegal = (brands || []).filter((s) => !allowed.has(s) || V43_FORBIDDEN_BRANDS.includes(s));
  if (illegal.length) blockers.push(`brand_only_violation:${illegal.join(",")}`);

  const missingFields = discovery?.missingReleaseFields || [];
  if (missingFields.length && !flags.createMissingFields) {
    blockers.push(`missing_release_fields:${missingFields.join(",")}`);
    blockers.push(`missing_flag:${V43_APPLY_FLAGS.createMissingFields}`);
  }

  for (const b of brandResults || []) {
    if (!b.preApplyGate?.pass) {
      blockers.push(`pre_apply_gate_failed:${b.brandSlug}`);
    }
    if (!b.releasePatch?.fields || !Object.keys(b.releasePatch.fields).length) {
      blockers.push(`empty_release_patch:${b.brandSlug}`);
    }
    for (const field of Object.keys(b.releasePatch?.fields || {})) {
      if (FORBIDDEN_WRITE_FIELDS.includes(field)) {
        blockers.push(`forbidden_field:${b.brandSlug}:${field}`);
      }
    }
  }

  return { allowed: blockers.length === 0, blockers, missingFlags };
}

async function airtablePatchBasics(recordId, fields) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  for (const key of Object.keys(fields)) {
    if (FORBIDDEN_WRITE_FIELDS.includes(key)) {
      throw new Error(`Refusing forbidden Brand Basics write: ${key}`);
    }
  }
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(BRAND_BASICS_TABLE)}/${recordId}`;
  const res = await fetch(url, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields, typecast: true }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `Airtable PATCH failed: ${res.status}`);
  return json;
}

async function auditIncompleteStillLocked() {
  const results = [];
  for (const brandSlug of INCOMPLETE_CONTROL_SLUGS) {
    try {
      const brandApi = await fetchBrandApiShape(brandSlug);
      const html = renderBrandExplorerHtmlForTest(brandApi, {
        allPanels: true,
        internalPreview: false,
      });
      const ql = evaluateBrandExternalQualityLock(brandApi, html, { brandSlug });
      const pass =
        brandApi.shouldRenderFullProfile !== true &&
        ql.profileInPreparationRendered === true &&
        (ql.tabsRenderedExternally || []).length <= 1;
      results.push({
        brandSlug,
        pass,
        displayState: brandApi.brandExplorerDisplayState,
        shouldRenderFullProfile: brandApi.shouldRenderFullProfile === true,
      });
    } catch (err) {
      results.push({ brandSlug, pass: false, error: err.message });
    }
  }
  return { allLocked: results.every((r) => r.pass), results };
}

/**
 * Audit one brand for active release (read-only unless caller applies).
 */
export async function auditBrandV43ActiveRelease(brandSlug, { discovery = null, founderCliConfirmed = false } = {}) {
  if (V43_FORBIDDEN_BRANDS.includes(brandSlug)) {
    throw new Error(`v43 refuses incomplete-control brand ${brandSlug}`);
  }
  if (!PRIMARY_RELEASE_SLUGS.includes(brandSlug)) {
    throw new Error(`v43 only supports primary release brands: ${PRIMARY_RELEASE_SLUGS.join(", ")}`);
  }

  const config = resolveConfig(brandSlug);
  const ctx = await loadBrandFactoryContext(brandSlug);
  const brandApi = await fetchBrandApiShape(brandSlug);
  const presentationRows = ctx?.presentationRows || brandApi?.brandExplorer?.blocks || [];
  const blocks = brandApi?.brandExplorer?.blocks || presentationRows;
  const brandBasicsFields = ctx?.brandBasics?.fields || ctx?.brandBasics || {};

  const fieldDiscovery = discovery || (await discoverBrandBasicsReleaseFields());
  const currentValues = readReleaseFieldValues(brandBasicsFields, fieldDiscovery);
  const priorRec = priorFounderRecommendation(brandSlug);

  const galleryCount = countGallery(blocks);
  const openingsCount = countOpenings(blocks);

  const internalHtml = renderBrandExplorerHtmlForTest(brandApi, {
    allPanels: true,
    internalPreview: true,
  });
  const externalHtml = renderBrandExplorerHtmlForTest(brandApi, {
    allPanels: true,
    internalPreview: false,
  });
  const internalText = stripHtml(internalHtml);
  const forbiddenInternal = scanForbiddenLanguage(internalText);
  const internalOwnerHits = scanInternalPreviewOwnerCopy(internalText);
  const externalQl = evaluateBrandExternalQualityLock(brandApi, externalHtml, {
    brandSlug,
    brandBasics: brandBasicsFields,
  });

  const osBrand = await evaluateBrandExplorerOsBrand(brandSlug).catch((err) => ({
    error: err.message,
    canonicalState: null,
    routing: null,
  }));
  const osElig = osReleaseEligible(osBrand, priorRec);

  const displayNow = {
    brandExplorerDisplayState: brandApi.brandExplorerDisplayState,
    shouldRenderFullProfile: brandApi.shouldRenderFullProfile === true,
    blockers: brandApi.brandExplorerDisplayBlockers || [],
  };

  // Project display state as if release fields were set (without writing)
  const projectedBasics = {
    ...brandBasicsFields,
    "Founder Visual Review Pass": true,
    "Active Profile Approved": true,
    "Ready for Active Profile": true,
    "Active Profile Approved Date": todayIsoDate(),
  };
  const projectedDisplay = resolveBrandExplorerDisplayState(
    {
      ...brandApi,
      founderVisualReviewPass: true,
      activeProfileApproved: true,
      readyForActiveProfile: true,
    },
    { brandBasics: projectedBasics, presentationRows: blocks }
  );

  const gateChecks = [
    {
      name: "os_release_eligible",
      pass: osElig.pass,
      detail: osElig.reason,
    },
    {
      name: "founder_visual_review_attested",
      pass: currentValues.founderVisualReviewPass === true || founderCliConfirmed || priorRec.approveForActiveRelease,
      detail: currentValues.founderVisualReviewPass
        ? "Founder Visual Review Pass already true"
        : founderCliConfirmed
          ? "CLI --confirm-founder-visual-review-passed"
          : priorRec.approveForActiveRelease
            ? "Prior v42/v42A recommend approve_for_active_release (apply still requires CLI founder confirm)"
            : "Founder approval not attested",
    },
    {
      name: "internal_preview_owner_copy_clean",
      pass: internalOwnerHits.length === 0 && forbiddenInternal.length === 0,
      detail:
        internalOwnerHits.length || forbiddenInternal.length
          ? `hits=${[...internalOwnerHits, ...forbiddenInternal].map((h) => h.label || h.id).join(", ")}`
          : "clean",
    },
    {
      name: "external_quality_lock_pass",
      pass:
        externalQl.externalQualityLockPass === true ||
        externalQl.profileInPreparationRendered === true,
      detail: `prep=${externalQl.profileInPreparationRendered === true} fullNow=${displayNow.shouldRenderFullProfile}`,
    },
    {
      name: "gallery_six_imageurl",
      pass: galleryCount >= GALLERY_MIN,
      detail: `${galleryCount}/${GALLERY_MIN}`,
    },
    {
      name: "property_examples_three_imageurl",
      pass: openingsCount >= PROPERTY_MIN,
      detail: `${openingsCount}/${PROPERTY_MIN}`,
    },
    {
      name: "company_validated_untouched",
      pass: true,
      detail: `Company Validated=${currentValues.companyValidated} (will not write)`,
    },
    {
      name: "release_fields_available_or_creatable",
      pass:
        fieldDiscovery.canWriteReleaseWithoutSchemaCreate ||
        (fieldDiscovery.missingReleaseFields || []).length > 0,
      detail: fieldDiscovery.canWriteReleaseWithoutSchemaCreate
        ? "fields present"
        : `missing: ${(fieldDiscovery.missingReleaseFields || []).join(", ")}`,
    },
    {
      name: "projected_full_profile_after_release",
      pass: projectedDisplay.shouldRenderFullProfile === true,
      detail: `projectedState=${projectedDisplay.brandExplorerDisplayState} blockers=${(projectedDisplay.blockers || []).join(",") || "none"}`,
    },
  ];

  const preApplyGate = {
    pass: gateChecks.every((g) => g.pass),
    failed: gateChecks.filter((g) => !g.pass).map((g) => g.name),
    checks: gateChecks,
  };

  const releasePatch = buildReleasePatch({
    discovery: fieldDiscovery,
    currentValues,
    founderCliConfirmed: founderCliConfirmed || priorRec.approveForActiveRelease,
  });

  return {
    brandSlug,
    brandName: brandApi.name || config?.name || brandSlug,
    recordId: brandApi.id || config?.recordId || null,
    cohort: "primary",
    fieldDiscoverySummary: {
      present: fieldDiscovery.presentReleaseFields,
      missing: fieldDiscovery.missingReleaseFields,
    },
    currentReleaseFieldValues: currentValues,
    priorFounderRecommendation: priorRec,
    os: {
      canonicalState: osBrand.canonicalState || null,
      allowedNextAction: osBrand.routing?.allowedNextAction || null,
      error: osBrand.error || null,
    },
    liveDisplay: displayNow,
    visuals: {
      galleryCount,
      openingsCount,
      galleryReady: galleryCount >= GALLERY_MIN,
      openingsReady: openingsCount >= PROPERTY_MIN,
    },
    copyGates: {
      internalForbidden: forbiddenInternal,
      internalOwnerHits,
      internalClean: internalOwnerHits.length === 0 && forbiddenInternal.length === 0,
    },
    externalLock: {
      pass:
        externalQl.externalQualityLockPass === true ||
        externalQl.profileInPreparationRendered === true,
      profileInPreparation: externalQl.profileInPreparationRendered === true,
      tabsRendered: (externalQl.tabsRenderedExternally || []).length,
    },
    preApplyGate,
    releasePatch,
    fieldMapping: releasePatch.mapping,
    projectedAfterRelease: {
      brandExplorerDisplayState: projectedDisplay.brandExplorerDisplayState,
      shouldRenderFullProfile: projectedDisplay.shouldRenderFullProfile,
      blockers: projectedDisplay.blockers,
      completeness: projectedDisplay.completeness,
    },
    applyResult: null,
    postApply: null,
    guardrails: {
      contentWrites: false,
      companyValidatedChanges: false,
      sourceLibraryChanges: false,
      registryChanges: false,
      imageFieldChanges: false,
      incompleteUnlock: false,
    },
  };
}

export async function runV43ActiveReleaseApply({
  brands = V43_DEFAULT_BRANDS,
  dryRun = true,
  apply = false,
  flags = null,
} = {}) {
  for (const b of brands) {
    if (V43_FORBIDDEN_BRANDS.includes(b)) throw new Error(`v43 refuses ${b}`);
  }

  const resolvedFlags = flags || parseV43ApplyFlags([]);
  const founderCliConfirmed = resolvedFlags.founderPassed === true;

  let discovery = await discoverBrandBasicsReleaseFields();
  const brandResults = [];
  for (const brandSlug of brands) {
    brandResults.push(
      await auditBrandV43ActiveRelease(brandSlug, {
        discovery,
        founderCliConfirmed: founderCliConfirmed || !apply,
      })
    );
  }

  const incompleteControl = await auditIncompleteStillLocked();

  const gateCheck = validateV43ApplyGates({
    flags: { ...resolvedFlags, apply },
    brands,
    brandResults,
    discovery,
  });

  let applyExecuted = false;
  let applyBlocked = false;
  let schemaEnsure = null;

  if (apply) {
    if (!gateCheck.allowed) {
      applyBlocked = true;
    } else {
      if ((discovery.missingReleaseFields || []).length && resolvedFlags.createMissingFields) {
        schemaEnsure = await ensureMissingReleaseFields({ dryRun: false });
        discovery = schemaEnsure;
        // Rebuild patches against refreshed discovery
        for (const b of brandResults) {
          b.releasePatch = buildReleasePatch({
            discovery,
            currentValues: b.currentReleaseFieldValues,
            founderCliConfirmed: true,
          });
          b.fieldMapping = b.releasePatch.mapping;
        }
      }

      for (const b of brandResults) {
        try {
          const patched = await airtablePatchBasics(b.recordId, b.releasePatch.fields);
          b.applyResult = {
            ok: true,
            recordId: patched.id,
            fieldsWritten: Object.keys(b.releasePatch.fields),
          };

          // Post-apply live re-fetch
          const afterApi = await fetchBrandApiShape(b.brandSlug);
          const afterExternal = renderBrandExplorerHtmlForTest(afterApi, {
            allPanels: true,
            internalPreview: false,
          });
          const afterQl = evaluateBrandExternalQualityLock(afterApi, afterExternal, {
            brandSlug: b.brandSlug,
          });
          b.postApply = {
            displayState: afterApi.brandExplorerDisplayState,
            shouldRenderFullProfile: afterApi.shouldRenderFullProfile === true,
            externalProfileInPreparation: afterQl.profileInPreparationRendered === true,
            tabsRenderedExternally: (afterQl.tabsRenderedExternally || []).length,
            externalForbidden: afterQl.forbiddenStringsFound || [],
          };
        } catch (err) {
          b.applyResult = { ok: false, error: err.message };
        }
      }
      applyExecuted = true;
    }
  }

  const summary = {
    brands: brandResults.length,
    preApplyPassCount: brandResults.filter((b) => b.preApplyGate.pass).length,
    projectedFullProfileCount: brandResults.filter((b) => b.projectedAfterRelease.shouldRenderFullProfile)
      .length,
    recordsPatched: brandResults.filter((b) => b.applyResult?.ok).length,
    applyErrors: brandResults.filter((b) => b.applyResult && b.applyResult.ok === false).length,
    incompleteLocked: incompleteControl.allLocked,
    releaseFieldsMissing: discovery.missingReleaseFields || [],
    schemaModified: schemaEnsure?.schemaModified === true,
    anyContentWrite: false,
    anyCompanyValidatedWrite: false,
  };

  return {
    version: V43_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: !applyExecuted,
    applyRequested: Boolean(apply),
    applyExecuted,
    applyBlocked,
    applyGateCheck: gateCheck,
    brands,
    fieldDiscovery: discovery,
    schemaEnsure,
    brandResults,
    incompleteControl,
    summary,
    exactApplyCommand: [
      "npm run brand-explorer-v43-active-release-apply --",
      `--brands ${brands.join(",")}`,
      "--apply",
      V43_APPLY_FLAGS.approve,
      V43_APPLY_FLAGS.founderPassed,
      V43_APPLY_FLAGS.externalLock,
      V43_APPLY_FLAGS.internalCopy,
      V43_APPLY_FLAGS.gallery,
      V43_APPLY_FLAGS.openings,
      V43_APPLY_FLAGS.noCompanyValidation,
      V43_APPLY_FLAGS.noContentWrites,
      V43_APPLY_FLAGS.noSourceLibrary,
      V43_APPLY_FLAGS.noRegistry,
      V43_APPLY_FLAGS.brandOnly,
      ...(discovery.missingReleaseFields?.length ? [V43_APPLY_FLAGS.createMissingFields] : []),
    ].join(" "),
    guardrails: {
      contentWrites: false,
      companyValidatedChanges: false,
      sourceLibraryChanges: false,
      registryChanges: false,
      imageFieldChanges: false,
      incompleteUnlock: false,
      allowedBasicsFields: V43_RELEASE_FIELD_DEFS.map((d) => d.name),
      forbiddenBasicsFields: [...FORBIDDEN_WRITE_FIELDS],
    },
  };
}

export function renderRadissonReleaseProofMarkdown(brand, report) {
  const lines = [
    `# v43 Release Proof — ${brand.brandName}`,
    "",
    `Slug: \`${brand.brandSlug}\` · Record: \`${brand.recordId}\``,
    `Generated: ${report.generatedAt}`,
    "",
    "## Live before",
    "",
    `- displayState: **${brand.liveDisplay.brandExplorerDisplayState}**`,
    `- shouldRenderFullProfile: **${brand.liveDisplay.shouldRenderFullProfile}**`,
    `- gallery ${brand.visuals.galleryCount}/6 · openings ${brand.visuals.openingsCount}/3`,
    `- Founder Visual Review Pass: ${brand.currentReleaseFieldValues.founderVisualReviewPass}`,
    `- Active Profile Approved: ${brand.currentReleaseFieldValues.activeProfileApproved}`,
    `- Company Validated: ${brand.currentReleaseFieldValues.companyValidated} (untouched)`,
    "",
    "## Field discovery",
    "",
    `- Present: ${(brand.fieldDiscoverySummary.present || []).join(", ") || "(none)"}`,
    `- Missing: ${(brand.fieldDiscoverySummary.missing || []).join(", ") || "(none)"}`,
    "",
    "## Planned release writes",
    "",
  ];
  for (const m of brand.fieldMapping || []) {
    lines.push(`- \`${m.airtableField}\`: ${JSON.stringify(m.before)} → ${JSON.stringify(m.after)} (${m.role})`);
  }
  if (!(brand.fieldMapping || []).length) lines.push("- (none — fields missing or already set)");

  lines.push(
    "",
    "## Projected after release",
    "",
    `- displayState: **${brand.projectedAfterRelease.brandExplorerDisplayState}**`,
    `- shouldRenderFullProfile: **${brand.projectedAfterRelease.shouldRenderFullProfile}**`,
    `- blockers: ${(brand.projectedAfterRelease.blockers || []).join(", ") || "none"}`,
    "",
    "## Pre-apply gate",
    "",
    `- pass: **${brand.preApplyGate.pass}**`,
    brand.preApplyGate.failed?.length
      ? `- failed: ${brand.preApplyGate.failed.join(", ")}`
      : "- failed: none",
    ""
  );

  if (brand.applyResult) {
    lines.push("## Apply result", "");
    lines.push(`- ok: **${brand.applyResult.ok}**`);
    if (brand.applyResult.fieldsWritten) {
      lines.push(`- fields: ${brand.applyResult.fieldsWritten.join(", ")}`);
    }
    if (brand.applyResult.error) lines.push(`- error: ${brand.applyResult.error}`);
    lines.push("");
  }
  if (brand.postApply) {
    lines.push("## Post-apply live", "");
    lines.push(`- displayState: **${brand.postApply.displayState}**`);
    lines.push(`- shouldRenderFullProfile: **${brand.postApply.shouldRenderFullProfile}**`);
    lines.push(`- external Profile in Preparation: ${brand.postApply.externalProfileInPreparation}`);
    lines.push(`- tabs rendered externally: ${brand.postApply.tabsRenderedExternally}`);
    lines.push("");
  }

  lines.push(
    "## Guardrails",
    "",
    "- No content / Presentation / image writes",
    "- No Company Validated",
    "- No Source Library / Registry",
    "- Incomplete brands remain locked",
    ""
  );
  return lines.join("\n");
}

export function writeV43Reports(report, reportsDir = path.join(ROOT, "reports")) {
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, REPORT_JSON);
  const mdPath = path.join(reportsDir, REPORT_MD);
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2), "utf8");

  const md = [
    "# v43 Brand Explorer Active Release Apply",
    "",
    `Generated: ${report.generatedAt}`,
    `dryRun=${report.dryRun} applyExecuted=${report.applyExecuted} applyBlocked=${report.applyBlocked}`,
    "",
    "## Field discovery (Brand Basics)",
    "",
    `- Present release fields: ${(report.fieldDiscovery.presentReleaseFields || []).join(", ") || "(none)"}`,
    `- Missing release fields: ${(report.fieldDiscovery.missingReleaseFields || []).join(", ") || "(none)"}`,
    `- ${report.fieldDiscovery.note}`,
    "",
    "### Related live fields (name contains active/founder/validat/ready/…)",
    "",
  ];
  for (const f of report.fieldDiscovery.relatedLiveFields || []) {
    md.push(`- \`${f.name}\` (${f.type})`);
  }
  md.push("");
  md.push("## Summary", "");
  md.push(`- Brands: ${report.summary.brands}`);
  md.push(`- Pre-apply pass: ${report.summary.preApplyPassCount}/${report.summary.brands}`);
  md.push(`- Projected full profile: ${report.summary.projectedFullProfileCount}/${report.summary.brands}`);
  md.push(`- Records patched: ${report.summary.recordsPatched}`);
  md.push(`- Incomplete locked: **${report.summary.incompleteLocked ? "yes" : "no"}**`);
  md.push("");
  md.push("## Exact apply command (founder OK required)", "");
  md.push("```");
  md.push(report.exactApplyCommand);
  md.push("```");
  md.push("");

  for (const b of report.brandResults) {
    md.push(`### ${b.brandName}`);
    md.push(`- OS: ${b.os.canonicalState} → ${b.os.allowedNextAction}`);
    md.push(`- live display: ${b.liveDisplay.brandExplorerDisplayState} full=${b.liveDisplay.shouldRenderFullProfile}`);
    md.push(
      `- projected: ${b.projectedAfterRelease.brandExplorerDisplayState} full=${b.projectedAfterRelease.shouldRenderFullProfile}`
    );
    md.push(`- pre-apply gate: **${b.preApplyGate.pass ? "pass" : "fail"}** ${b.preApplyGate.failed?.join(", ") || ""}`);
    md.push(`- writes: ${Object.keys(b.releasePatch.fields || {}).join(", ") || "(none)"}`);
    md.push("");
  }

  md.push("## Incomplete control", "");
  for (const r of report.incompleteControl.results || []) {
    md.push(`- \`${r.brandSlug}\`: ${r.pass ? "locked" : "**NOT LOCKED**"} · full=${r.shouldRenderFullProfile}`);
  }
  md.push("");
  md.push("## Guardrails");
  md.push("- No content writes · no Company Validated · no Source Library · no Registry · no incomplete unlock");
  md.push("");

  fs.writeFileSync(mdPath, md.join("\n"), "utf8");

  const proofPaths = {};
  for (const b of report.brandResults) {
    if (b.brandSlug === "radisson-individuals-by-choice") {
      const fpath = path.join(reportsDir, "brand-explorer-v43-radisson-release-proof.md");
      fs.writeFileSync(fpath, renderRadissonReleaseProofMarkdown(b, report), "utf8");
      proofPaths[b.brandSlug] = fpath;
    }
  }

  return { jsonPath, mdPath, proofPaths };
}
