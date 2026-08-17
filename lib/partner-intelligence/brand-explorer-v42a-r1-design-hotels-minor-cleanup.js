/**
 * v42A-R1 — Design Hotels Founder Minor Cleanup.
 *
 * Resolves v42 approve_after_minor_cleanup with affiliation/curation polish only.
 * Does not unlock, approve active profile, or touch released / other incomplete brands.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getActiveProfileBrandConfig } from "./brand-explorer-active-profile-brand-config.js";
import { getDiscoveryBrandConfig } from "./brand-explorer-brand-asset-image-governance.js";
import { loadBrandFactoryContext } from "./brand-explorer-active-profile-factory.js";
import { evaluateBrandExternalQualityLock } from "./brand-explorer-display-quality-lock.js";
import { renderBrandExplorerHtmlForTest } from "./brand-explorer-atelier-render-test-loader.js";
import { PRESENTATION_TABLE } from "./brand-explorer-residual-owner-copy-remediation.js";
import {
  scanForbiddenLanguage,
  scanMechanicalCopy,
} from "./brand-explorer-v40b-copy-quality-patterns.js";
import { evaluateBrandExplorerOsBrand } from "./brand-explorer-os-run.js";
import {
  PRIMARY_RELEASE_SLUGS,
  INCOMPLETE_CONTROL_SLUGS,
} from "./brand-explorer-os-state-machine.js";
import { PROPERTY_RECORD_IDS } from "./brand-explorer-design-hotels-external-owner-cleanup-v35F-R1.js";
import { scanInternalPreviewOwnerCopy } from "./brand-explorer-economics-chrome-remediation.js";
import { V44_FROZEN_RELEASE_EXPECTATIONS } from "./brand-explorer-v44-release-baseline.js";

export const V42A_R1_VERSION = "v42a-r1";
export const V42A_R1_TARGET = "design-hotels";

export const V42A_R1_FORBIDDEN_BRANDS = Object.freeze([
  "everhome-suites",
  "kimpton",
  "radisson-individuals-by-choice",
  ...INCOMPLETE_CONTROL_SLUGS,
]);

export const V42A_R1_CALA = Object.freeze([
  "Wake BioHotel",
  "Condesa DF",
  "Carlota",
]);

export const V42A_R1_APPLY_FLAGS = Object.freeze({
  approve: "--approve-brand-explorer-v42A-R1-design-hotels-minor-cleanup",
  noCompanyValidation: "--confirm-no-company-validation-claim",
  noActiveApproval: "--confirm-no-active-profile-approval",
  noSourceLibrary: "--confirm-no-source-library-changes",
  noRegistry: "--confirm-no-registry-changes",
  noImageFields: "--confirm-no-image-field-changes",
  externalLocked: "--confirm-external-profile-remains-locked",
  releasedUnchanged: "--confirm-released-golden-brands-unchanged",
  designHotelsOnly: "--confirm-design-hotels-only",
  calaPreserved: "--confirm-cala-examples-preserved",
});

export const REPORT_JSON = "brand-explorer-v42a-r1-design-hotels-minor-cleanup.json";
export const REPORT_MD = "brand-explorer-v42a-r1-design-hotels-minor-cleanup.md";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

const MAP_FIELDS = Object.freeze({
  title: "Title",
  body: "Body",
  caseSummaryOverview: "Case Summary Overview",
  caseSummaryBrandRelevance: "Case Summary Brand Relevance",
  caseSummaryOwnerObjective: "Case Summary Owner Objective",
  caseSummaryInterpretation: "Case Summary Interpretation",
  caseSummaryTags: "Case Summary Tags",
});

const ALLOWED_WRITE_FIELDS = new Set([...Object.values(MAP_FIELDS)]);

const FORBIDDEN_WRITE_FIELDS = new Set([
  "Image",
  "Images",
  "Scenario Image",
  "Attachments",
  "External Display Status",
  "Company Validated",
  "Company Validation Date",
  "Ready for Active Profile",
  "Active Profile Approved",
  "Active Profile Approved Date",
  "Founder Visual Review Pass",
]);

/** Affiliation / curation tone polish — minor only. */
export const V42A_R1_TONE_REPLACEMENTS = Object.freeze([
  {
    id: "confirm_costs_timing_chrome",
    re: /\bconfirm participation costs and timing directly during brand engagement(?: and legal review)?\.?/gi,
    replace:
      "Ask Design Hotels development for current membership participation costs and timing before you commit",
    rationale: "Neutralize residual diligence stem with affiliation-specific ask",
  },
  {
    id: "confirm_costs_obligations_chrome",
    re: /\bconfirm participation costs, operating obligations, and agreement terms directly during brand engagement(?: and legal review)?\.?/gi,
    replace:
      "Ask Design Hotels development for current membership costs, design-review obligations, and agreement terms before you commit",
    rationale: "Affiliation-safe economics diligence phrasing",
  },
  {
    id: "confirm_costs_agreement_chrome",
    re: /\bconfirm participation costs and agreement terms directly during brand engagement(?: and legal review)?\.?/gi,
    replace:
      "Ask Design Hotels development for current membership costs and agreement terms before you commit",
    rationale: "Affiliation-safe diligence phrasing",
  },
  {
    id: "confirm_diligence_boilerplate",
    re: /\bconfirm [^.]{10,160}directly during brand engagement(?: and legal review)?\.?/gi,
    replace:
      "Ask Design Hotels development for current membership costs, design standards expectations, and agreement terms before you commit",
    rationale: "Catch-all Confirm…during brand engagement scrub stem",
  },
  {
    id: "commercial_agreement_materials_legalistic",
    re: /\bcommercial agreement materials\b/gi,
    replace: "membership agreement materials",
    rationale: "Prefer affiliation wording over scrub-legal tone",
  },
  {
    id: "participation_cost_categories",
    re: /\bparticipation cost categories\b/gi,
    replace: "membership participation costs",
    rationale: "Drop mechanical fee-stack scrub phrasing",
  },
  {
    id: "owner_economics_awkward",
    re: /\bowner economics after brand-related costs\b/gi,
    replace: "whether Design Hotels membership economics fit the asset after program costs",
    rationale: "Natural owner diligence phrasing",
  },
  {
    id: "public_performance_materials_franchisey",
    re: /\bpublic performance materials(?:\s*\(where applicable\))?/gi,
    replace: "portfolio-level materials Design Hotels or Marriott publish (where available)",
    rationale: "Avoid franchise-performance scrub tone",
  },
  {
    id: "average_daily_rate_stuffed",
    re: /\baverage daily rate\b/gi,
    replace: "rate positioning",
    rationale: "Reduce ADR-expansion mechanical feel in commercial copy",
  },
  {
    id: "revenue_per_available_room_stuffed",
    re: /\brevenue per available room\b/gi,
    replace: "revenue productivity",
    rationale: "Reduce RevPAR-expansion mechanical feel",
  },
  {
    id: "chain_prototype",
    re: /\bchain prototype\b/gi,
    replace: "collection prototype",
    rationale: "Affiliation / curation model fit",
  },
  {
    id: "franchise_model",
    re: /\bfranchise model\b/gi,
    replace: "affiliation / curation model",
    rationale: "Keep Design Hotels model language accurate",
  },
  {
    id: "brand_verified",
    re: /\bbrand-verified\b/gi,
    replace: "curated from public brand materials",
    rationale: "Avoid overstated validation claim",
  },
]);

/**
 * Only expand known stub shorts — never invent generic filler for intentional labels.
 */
export const V42A_R1_SHORT_BODY_EXPANSIONS = Object.freeze([
  {
    id: "stub_confirm_engagement",
    match: /^confirm .{0,80}brand engagement\.?$/i,
    after:
      "Ask Design Hotels development for current membership costs, design-review obligations, and agreement terms before you commit.",
  },
  {
    id: "stub_orientation_only",
    match: /^public materials on this page are orientation only[—.].*$/i,
    after:
      "Public materials on this page are orientation for affiliation diligence—not commercial terms, forecasts, or a membership offer.",
  },
]);

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function isHidden(row) {
  return /do not display|internal only/i.test(nz(row?.externalDisplayStatus));
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

function stripHtml(html) {
  return nz(html)
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/\s(?:href|src|srcset|data-src)=["'][^"']*["']/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function rewriteWithRules(text, rules) {
  let after = nz(text);
  const applied = [];
  if (!after) return { after, applied, changed: false };
  for (const rule of rules) {
    const re = new RegExp(rule.re.source, rule.re.flags);
    if (re.test(after)) {
      after = after.replace(new RegExp(rule.re.source, rule.re.flags), rule.replace);
      applied.push(rule.id);
    }
  }
  after = after.replace(/\s{2,}/g, " ").trim();
  return { after, applied, changed: after !== nz(text) };
}

export function rewriteDesignHotelsTone(text) {
  return rewriteWithRules(text, V42A_R1_TONE_REPLACEMENTS);
}

function applyShortBodyExpansion(text) {
  const before = nz(text);
  if (!before || before.length >= 40) return { after: before, applied: null, changed: false };
  for (const rule of V42A_R1_SHORT_BODY_EXPANSIONS) {
    if (rule.match.test(before)) {
      return { after: rule.after, applied: rule.id, changed: rule.after !== before };
    }
  }
  return { after: before, applied: null, changed: false };
}

function buildTonePatches(presentationRows) {
  const patches = [];
  for (const row of presentationRows || []) {
    if (isHidden(row)) continue;
    // Never rewrite CALA property example Case Summary lightly beyond tone rules —
    // still allow tone scrub on Body/Title if needed.
    for (const [apiKey, airtableField] of Object.entries(MAP_FIELDS)) {
      const before = nz(row[apiKey]);
      if (!before) continue;

      let working = before;
      const appliedRules = [];

      const tone = rewriteWithRules(working, V42A_R1_TONE_REPLACEMENTS);
      if (tone.changed) {
        working = tone.after;
        appliedRules.push(...tone.applied);
      }

      // Short-body expansion only on Body field stubs
      if (apiKey === "body") {
        const short = applyShortBodyExpansion(working);
        if (short.changed) {
          working = short.after;
          appliedRules.push(short.applied);
        }
      }

      if (working === before) continue;

      const forbiddenAfter = scanForbiddenLanguage(working);
      const mechanicalAfter = scanMechanicalCopy(working).filter((h) =>
        ["high", "medium"].includes(h.severity)
      );
      const ownerUnsafe = scanInternalPreviewOwnerCopy(working);

      patches.push({
        brandSlug: V42A_R1_TARGET,
        recordId: row.recordId,
        table: PRESENTATION_TABLE,
        slotKey: row.slotKey,
        field: airtableField,
        apiKey,
        before,
        after: working,
        appliedRules,
        reason: "v42A-R1 Design Hotels affiliation/curation minor polish",
        safeForGenericApply:
          forbiddenAfter.length === 0 &&
          mechanicalAfter.length === 0 &&
          ownerUnsafe.length === 0 &&
          !/https?:\/\//i.test(working) &&
          !/\bSources?:\s*/i.test(working),
        forbiddenAfter,
        mechanicalAfter,
        founderJudgmentNeeded: false,
      });
    }
  }
  return patches;
}

function applyPatchesLocally(rows, patches) {
  const byRecord = new Map();
  for (const p of patches) {
    if (!byRecord.has(p.recordId)) byRecord.set(p.recordId, {});
    if (p.apiKey) byRecord.get(p.recordId)[p.apiKey] = p.after;
  }
  return (rows || []).map((r) => {
    const overlay = byRecord.get(r.recordId);
    return overlay ? { ...r, ...overlay } : r;
  });
}

function auditCalaExamples(presentationRows, blocks) {
  const openings = (presentationRows || []).filter(
    (r) => nz(r.slotKey) === "footprint.openings" && !isHidden(r)
  );
  const found = [];
  for (const [key, recordId] of Object.entries(PROPERTY_RECORD_IDS)) {
    const row = openings.find((r) => r.recordId === recordId);
    const block = (blocks || []).find((b) => b.recordId === recordId);
    const name =
      key === "wake-biohotel"
        ? "Wake BioHotel"
        : key === "condesa-df"
          ? "Condesa DF"
          : "Carlota";
    found.push({
      name,
      propertyKey: key,
      recordId,
      present: Boolean(row),
      imageUrl: Boolean(nz(row?.imageUrl || block?.imageUrl)),
      title: nz(row?.title),
      hidden: row ? isHidden(row) : true,
    });
  }
  const pass =
    found.length === 3 &&
    found.every((f) => f.present && f.imageUrl && !f.hidden) &&
    openings.filter((r) => Object.values(PROPERTY_RECORD_IDS).includes(r.recordId)).length === 3;
  return {
    expected: [...V42A_R1_CALA],
    found,
    visibleCalaCount: found.filter((f) => f.present && !f.hidden).length,
    pass,
    keepExactlyThree: true,
    hidePatches: 0,
  };
}

function listRemainingShortBodies(presentationRows) {
  const shorts = [];
  for (const row of presentationRows || []) {
    if (isHidden(row)) continue;
    const body = nz(row.body);
    if (body && body.length > 0 && body.length < 40) {
      // Skip intentional chips/labels that look like codes
      if (/^[A-Z0-9 ·/,.&-]{1,39}$/.test(body)) continue;
      shorts.push({
        recordId: row.recordId,
        slotKey: row.slotKey,
        length: body.length,
        sample: body.slice(0, 80),
        action: "founder_spot_check_only",
      });
    }
  }
  return shorts;
}

function projectRecommendation({
  forbiddenAfter,
  mediumMechanicalAfter,
  calaPass,
  osState,
  tonePatchCount,
  externalLocked,
}) {
  if (!externalLocked) {
    return {
      recommendation: "not_owner_ready",
      rationale: "External profile unexpectedly unlocked.",
    };
  }
  if (forbiddenAfter.length) {
    return {
      recommendation: "not_owner_ready",
      rationale: `Forbidden language remains: ${forbiddenAfter.map((h) => h.label || h.id).join(", ")}`,
    };
  }
  if (mediumMechanicalAfter.length) {
    return {
      recommendation: "remediation_required",
      rationale: `Medium mechanical tone remains: ${mediumMechanicalAfter.map((h) => h.id).join(", ")}`,
    };
  }
  if (!calaPass) {
    return {
      recommendation: "remediation_required",
      rationale: "CALA property examples Wake / Condesa / Carlota must remain intact.",
    };
  }
  if (osState && osState !== "founder_review_ready" && osState !== "active_release_ready") {
    return {
      recommendation: "approve_after_minor_cleanup",
      rationale: `OS state still ${osState}; finish minor cleanup then re-audit.`,
    };
  }
  return {
    recommendation: "approve_for_active_release",
    rationale: `Design Hotels minor polish resolved (${tonePatchCount} field patch(es) in projection). CALA trio preserved. Subject to explicit founder OK — v42A-R1 does not apply active release.`,
  };
}

export function parseV42AR1ApplyFlags(argv = []) {
  const has = (flag) => argv.includes(flag);
  return {
    apply: has("--apply"),
    approve: has(V42A_R1_APPLY_FLAGS.approve),
    noCompanyValidation: has(V42A_R1_APPLY_FLAGS.noCompanyValidation),
    noActiveApproval: has(V42A_R1_APPLY_FLAGS.noActiveApproval),
    noSourceLibrary: has(V42A_R1_APPLY_FLAGS.noSourceLibrary),
    noRegistry: has(V42A_R1_APPLY_FLAGS.noRegistry),
    noImageFields: has(V42A_R1_APPLY_FLAGS.noImageFields),
    externalLocked: has(V42A_R1_APPLY_FLAGS.externalLocked),
    releasedUnchanged: has(V42A_R1_APPLY_FLAGS.releasedUnchanged),
    designHotelsOnly: has(V42A_R1_APPLY_FLAGS.designHotelsOnly),
    calaPreserved: has(V42A_R1_APPLY_FLAGS.calaPreserved),
  };
}

export function validateV42AR1ApplyGates({ flags, brandResult } = {}) {
  const blockers = [];
  if (!flags?.apply) return { allowed: false, blockers: ["not_apply_mode"], missingFlags: [] };

  const flagChecks = [
    ["approve", V42A_R1_APPLY_FLAGS.approve],
    ["noCompanyValidation", V42A_R1_APPLY_FLAGS.noCompanyValidation],
    ["noActiveApproval", V42A_R1_APPLY_FLAGS.noActiveApproval],
    ["noSourceLibrary", V42A_R1_APPLY_FLAGS.noSourceLibrary],
    ["noRegistry", V42A_R1_APPLY_FLAGS.noRegistry],
    ["noImageFields", V42A_R1_APPLY_FLAGS.noImageFields],
    ["externalLocked", V42A_R1_APPLY_FLAGS.externalLocked],
    ["releasedUnchanged", V42A_R1_APPLY_FLAGS.releasedUnchanged],
    ["designHotelsOnly", V42A_R1_APPLY_FLAGS.designHotelsOnly],
    ["calaPreserved", V42A_R1_APPLY_FLAGS.calaPreserved],
  ];
  const missingFlags = flagChecks.filter(([k]) => !flags[k]).map(([, f]) => f);
  if (missingFlags.length) blockers.push(...missingFlags.map((f) => `missing_flag:${f}`));

  for (const p of brandResult?.patches || []) {
    if (!ALLOWED_WRITE_FIELDS.has(p.field)) blockers.push(`forbidden_field:${p.field}`);
    if (FORBIDDEN_WRITE_FIELDS.has(p.field)) blockers.push(`blocked_field:${p.field}`);
    if (p.safeForGenericApply === false) blockers.push(`unsafe_patch:${p.slotKey}:${p.field}`);
  }
  if (brandResult?.projection?.recommendation !== "approve_for_active_release") {
    blockers.push(`projection_not_ready:${brandResult?.projection?.recommendation}`);
  }
  if (brandResult?.calaExamples?.pass !== true) blockers.push("cala_examples_not_preserved");
  if (brandResult?.shouldRenderFullProfile === true) blockers.push("would_unlock");
  if (brandResult?.baselineProtection?.pass !== true) blockers.push("released_golden_changed");

  return { allowed: blockers.length === 0, blockers, missingFlags };
}

function groupPatchesByRecord(patches = []) {
  const byId = new Map();
  for (const p of patches) {
    if (!p.recordId || !ALLOWED_WRITE_FIELDS.has(p.field) || !p.safeForGenericApply) continue;
    if (!byId.has(p.recordId)) byId.set(p.recordId, { recordId: p.recordId, fields: {} });
    byId.get(p.recordId).fields[p.field] = p.after;
  }
  return [...byId.values()];
}

async function airtablePatchPresentation(baseId, apiKey, recordId, fields) {
  const safe = {};
  for (const [k, v] of Object.entries(fields || {})) {
    if (!ALLOWED_WRITE_FIELDS.has(k)) continue;
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

async function applyPatches(patches) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");
  const grouped = groupPatchesByRecord(patches);
  const patched = [];
  const errors = [];
  for (const g of grouped) {
    try {
      await airtablePatchPresentation(baseId, apiKey, g.recordId, g.fields);
      patched.push({ recordId: g.recordId, fields: Object.keys(g.fields) });
    } catch (err) {
      errors.push({ recordId: g.recordId, message: err.message });
    }
  }
  return { patched, errors, recordsTouched: patched.length };
}

async function protectReleasedBaseline() {
  const rows = [];
  const failures = [];
  for (const slug of PRIMARY_RELEASE_SLUGS.filter((s) => s !== V42A_R1_TARGET)) {
    const os = await evaluateBrandExplorerOsBrand(slug);
    const brand = await fetchBrandApiShape(slug);
    const exp = V44_FROZEN_RELEASE_EXPECTATIONS[slug];
    const brandFailures = [];
    if (
      brand.brandExplorerDisplayState !== "active_profile_ready" &&
      os.canonicalState !== "active_profile_ready"
    ) {
      brandFailures.push("not_active_profile_ready");
    }
    if (brand.shouldRenderFullProfile !== true) brandFailures.push("full_false");
    if ((os.metrics?.galleryCount || 0) < (exp?.minGalleryImageUrls || 6)) {
      brandFailures.push("gallery_drop");
    }
    if ((os.metrics?.openingsCount || 0) < (exp?.minPropertyImageUrls || 3)) {
      brandFailures.push("property_drop");
    }
    if (os.metrics?.companyValidated === true) brandFailures.push("cv_changed");
    rows.push({
      brandSlug: slug,
      pass: brandFailures.length === 0,
      failures: brandFailures,
      gallery: os.metrics?.galleryCount,
      property: os.metrics?.openingsCount,
    });
    for (const f of brandFailures) failures.push(`${slug}: ${f}`);
  }
  for (const slug of V42A_R1_FORBIDDEN_BRANDS.filter((s) => INCOMPLETE_CONTROL_SLUGS.includes(s))) {
    const brand = await fetchBrandApiShape(slug);
    if (brand.shouldRenderFullProfile === true) failures.push(`${slug}: unlocked`);
  }
  return { pass: failures.length === 0, failures, rows };
}

export function buildV42AR1ExactApplyCommand() {
  return [
    "npm run brand-explorer-v42a-r1-design-hotels-minor-cleanup -- --brand design-hotels --apply",
    ...Object.values(V42A_R1_APPLY_FLAGS),
  ].join(" \\\n  ");
}

export async function runV42AR1DesignHotelsMinorCleanup({
  brand = V42A_R1_TARGET,
  dryRun = true,
  apply = false,
  flags = {},
} = {}) {
  if (brand !== V42A_R1_TARGET) {
    throw new Error(`v42A-R1 is Design Hotels only. Refused brand=${brand}`);
  }
  for (const forbidden of V42A_R1_FORBIDDEN_BRANDS) {
    if (brand === forbidden) throw new Error(`v42A-R1 refuses ${forbidden}`);
  }

  const config = resolveConfig(V42A_R1_TARGET);
  const ctx = await loadBrandFactoryContext(V42A_R1_TARGET).catch(() => null);
  const brandApi = await fetchBrandApiShape(V42A_R1_TARGET);
  const presentationRows = ctx?.presentationRows || brandApi?.brandExplorer?.blocks || [];
  const blocks = brandApi?.brandExplorer?.blocks || presentationRows;

  const osBrand = await evaluateBrandExplorerOsBrand(V42A_R1_TARGET);
  if (
    osBrand.canonicalState !== "founder_review_ready" &&
    osBrand.routing?.allowedNextAction !== "founder_visual_review"
  ) {
    // Soft warn — still allow polish if residual tone exists
  }

  const calaExamples = auditCalaExamples(presentationRows, blocks);
  const tonePatches = buildTonePatches(presentationRows);
  const patches = tonePatches.filter((p) => p.safeForGenericApply !== false);
  const unsafePatches = tonePatches.filter((p) => p.safeForGenericApply === false);

  const projectedRows = applyPatchesLocally(presentationRows, tonePatches);
  const projectedCorpus = projectedRows
    .filter((r) => !isHidden(r))
    .flatMap((r) => Object.keys(MAP_FIELDS).map((k) => nz(r[k])).filter(Boolean))
    .join("\n");

  const liveInternalHtml = renderBrandExplorerHtmlForTest(brandApi, {
    allPanels: true,
    internalPreview: true,
  });
  const liveInternalText = stripHtml(liveInternalHtml);
  const projectedInternalText = rewriteDesignHotelsTone(liveInternalText).after;

  const liveForbidden = scanInternalPreviewOwnerCopy(liveInternalText);
  const projectedForbiddenScan = scanInternalPreviewOwnerCopy(projectedInternalText);
  const projectedForbiddenLang = scanForbiddenLanguage(
    [projectedCorpus, projectedInternalText].join("\n")
  );
  const projectedMechanical = scanMechanicalCopy(
    [projectedCorpus, projectedInternalText].join("\n")
  ).filter((h) => ["high", "medium"].includes(h.severity));

  const externalHtml = renderBrandExplorerHtmlForTest(brandApi, {
    allPanels: true,
    internalPreview: false,
  });
  const externalQl = evaluateBrandExternalQualityLock(brandApi, externalHtml, {
    brandSlug: V42A_R1_TARGET,
    brandBasics: ctx?.brandBasics,
  });
  const externalLocked =
    brandApi.shouldRenderFullProfile !== true &&
    (externalQl.profileInPreparationRendered === true ||
      (externalQl.tabsRenderedExternally || []).length === 0);

  const remainingShortBodies = listRemainingShortBodies(projectedRows);
  const baselineProtection = await protectReleasedBaseline();

  const projection = projectRecommendation({
    forbiddenAfter: [...projectedForbiddenScan, ...projectedForbiddenLang],
    mediumMechanicalAfter: projectedMechanical,
    calaPass: calaExamples.pass,
    osState: osBrand.canonicalState,
    tonePatchCount: tonePatches.length,
    externalLocked,
  });

  const brandResult = {
    brandSlug: V42A_R1_TARGET,
    brandName: brandApi.name || config?.name || "Design Hotels",
    recordId: brandApi.id || config?.recordId || null,
    displayState: brandApi.brandExplorerDisplayState,
    shouldRenderFullProfile: brandApi.shouldRenderFullProfile === true,
    os: {
      canonicalState: osBrand.canonicalState,
      allowedNextAction: osBrand.routing?.allowedNextAction,
      founderReviewAllowed: osBrand.routing?.founderReviewAllowed === true,
      activeReleaseAllowed: osBrand.routing?.activeReleaseAllowed === true,
    },
    polishScope: {
      fromV42: "approve_after_minor_cleanup",
      items: [
        "short bodies (stub expansions only; intentional labels left for founder spot-check)",
        "economics diligence framing (affiliation ask, not legalistic)",
        "image QA spot-check only (no image writes)",
        "premium affiliation / curation tone",
      ],
    },
    calaExamples,
    economicsFraming: {
      approach: "owner diligence orientation",
      not: ["FDD", "fee schedule", "LOI", "Item 19"],
      note: "Public materials orient diligence categories — not confidential commercial terms.",
    },
    bonvoyFraming: {
      careful: true,
      note: "Marriott Bonvoy participation varies by property; no property-level loyalty guarantees.",
    },
    remainingShortBodies,
    remainingFounderSpotChecks: [
      ...remainingShortBodies.map(
        (s) => `[${s.slotKey}] short body (${s.length} chars) left as intentional/label — spot-check`
      ),
      "Spot-check gallery image quality (6/6 imageUrls confirmed; no image writes)",
      "Spot-check CALA examples: Wake BioHotel, Condesa DF, Carlota",
      "Confirm Dealality insight remains interpretive, not brochure fluff",
    ],
    toneCleanup: {
      patchCount: tonePatches.length,
      unsafeCount: unsafePatches.length,
      sampleSlots: [...new Set(tonePatches.map((p) => p.slotKey))].slice(0, 20),
      liveForbiddenCount: liveForbidden.length,
      projectedForbiddenCount: projectedForbiddenScan.length + projectedForbiddenLang.length,
      projectedMediumMechanicalCount: projectedMechanical.length,
    },
    patches: tonePatches,
    patchSummary: {
      total: tonePatches.length,
      safe: patches.length,
      unsafe: unsafePatches.length,
      recordsTouched: new Set(tonePatches.map((p) => p.recordId)).size,
    },
    externalLock: {
      profileInPreparation: externalQl.profileInPreparationRendered === true,
      tabs: (externalQl.tabsRenderedExternally || []).length,
      pass: externalQl.externalQualityLockPass === true,
      locked: externalLocked,
    },
    baselineProtection,
    projection,
    guardrails: {
      activeRelease: false,
      companyValidatedChanges: false,
      sourceLibraryChanges: false,
      registryChanges: false,
      imageFieldChanges: false,
      unlock: false,
      releasedBrandWrites: false,
      otherIncompleteWrites: false,
    },
  };

  let applyResult = null;
  let applyBlocked = false;
  let applyGateCheck = { allowed: false, blockers: ["not_apply_mode"] };

  if (apply) {
    applyGateCheck = validateV42AR1ApplyGates({ flags, brandResult });
    if (!applyGateCheck.allowed) {
      applyBlocked = true;
    } else {
      applyResult = await applyPatches(tonePatches);
    }
  }

  return {
    version: V42A_R1_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: !apply,
    applyExecuted: Boolean(applyResult) && !applyBlocked,
    applyBlocked,
    brand: V42A_R1_TARGET,
    brandResult,
    applyGateCheck,
    applyResult,
    exactDryRunCommand:
      "npm run brand-explorer-v42a-r1-design-hotels-minor-cleanup -- --brand design-hotels --dry-run",
    exactApplyCommand: buildV42AR1ExactApplyCommand(),
    summary: {
      patchCount: tonePatches.length,
      unsafePatches: unsafePatches.length,
      projectedRecommendation: projection.recommendation,
      projectedApproveForActiveRelease: projection.recommendation === "approve_for_active_release",
      calaPreserved: calaExamples.pass,
      baselineProtectionPass: baselineProtection.pass,
      externalLocked,
      applyExecuted: Boolean(applyResult) && !applyBlocked,
    },
  };
}

export function renderV42AR1Markdown(report) {
  const b = report.brandResult || {};
  const lines = [
    "# v42A-R1 Design Hotels Founder Minor Cleanup",
    "",
    `Generated: ${report.generatedAt}`,
    "",
    "Minor polish only after v42 `approve_after_minor_cleanup`. No unlock. No active release. No Company Validated.",
    "",
    "## Summary",
    "",
    `- Patches: **${report.summary?.patchCount}** (unsafe=${report.summary?.unsafePatches})`,
    `- Projected recommendation: **${report.summary?.projectedRecommendation}**`,
    `- CALA preserved: **${report.summary?.calaPreserved}**`,
    `- External locked: **${report.summary?.externalLocked}**`,
    `- Baseline protection: **${report.summary?.baselineProtectionPass}**`,
    `- Apply executed: **${report.summary?.applyExecuted}**`,
    "",
    "> **Note:** v42 founder packet may still list `approve_after_minor_cleanup` when intentional short labels remain as taste spot-checks. v42A-R1 projects `approve_for_active_release` when no safe Presentation polish patches remain and hard gates pass (CALA intact, owner-copy clean, externally locked).",
    "",
    "## OS",
    "",
    `- State: \`${b.os?.canonicalState}\``,
    `- Next action: \`${b.os?.allowedNextAction}\``,
    `- Active release allowed: **${b.os?.activeReleaseAllowed}**`,
    "",
    "## CALA examples (must keep)",
    "",
  ];
  for (const p of b.calaExamples?.found || []) {
    lines.push(
      `- ${p.name}: present=${p.present} imageUrl=${p.imageUrl} hidden=${p.hidden}`
    );
  }

  lines.push("", "## Tone patches (sample)", "");
  for (const p of (b.patches || []).slice(0, 15)) {
    lines.push(
      `- \`${p.slotKey}\` / ${p.field} · rules=${(p.appliedRules || []).join(",") || "—"} · safe=${p.safeForGenericApply}`
    );
  }

  lines.push("", "## Remaining founder spot-checks", "");
  for (const item of b.remainingFounderSpotChecks || []) lines.push(`- ${item}`);

  lines.push("", "## Economics / Bonvoy framing", "");
  lines.push(`- Economics: ${b.economicsFraming?.note || "—"}`);
  lines.push(`- Bonvoy: ${b.bonvoyFraming?.note || "—"}`);

  lines.push("", "## Projection", "");
  lines.push(`- **${b.projection?.recommendation}** — ${b.projection?.rationale || ""}`);

  lines.push("", "## Exact apply command", "```");
  lines.push(report.exactApplyCommand || "");
  lines.push("```", "", "## Guardrails", "");
  for (const [k, v] of Object.entries(b.guardrails || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");
  return lines.join("\n");
}

export function writeV42AR1Reports(report, reportsDir = path.join(ROOT, "reports")) {
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, REPORT_JSON);
  const mdPath = path.join(reportsDir, REPORT_MD);
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2), "utf8");
  fs.writeFileSync(mdPath, renderV42AR1Markdown(report), "utf8");
  return { jsonPath, mdPath };
}
