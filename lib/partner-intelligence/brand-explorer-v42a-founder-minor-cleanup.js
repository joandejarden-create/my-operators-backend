/**
 * v42A — Founder minor cleanup for Everhome + Kimpton.
 *
 * Resolves approve_after_minor_cleanup gates only:
 * - Everhome: property-example extras decision (keep vs hide)
 * - Kimpton: soft tone / mechanical Item-19-rewrite phrasing + extras decision
 *
 * Does not touch Radisson or incomplete brands. No active release / unlock.
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
import { INCOMPLETE_CONTROL_SLUGS } from "./brand-explorer-os-state-machine.js";

export const V42A_VERSION = "v42a";

export const V42A_DEFAULT_BRANDS = Object.freeze(["everhome-suites", "kimpton"]);

export const V42A_FORBIDDEN_BRANDS = Object.freeze([
  "radisson-individuals-by-choice",
  ...INCOMPLETE_CONTROL_SLUGS,
]);

export const V42A_APPLY_FLAGS = Object.freeze({
  approve: "--approve-brand-explorer-v42A-founder-minor-cleanup",
  noCompanyValidation: "--confirm-no-company-validation-claim",
  noActiveApproval: "--confirm-no-active-profile-approval",
  noSourceLibrary: "--confirm-no-source-library-changes",
  noRegistry: "--confirm-no-registry-changes",
  noIncompleteUnlock: "--confirm-no-incomplete-brand-unlock",
  brandOnly: "--confirm-brand-only",
});

export const REPORT_JSON = "brand-explorer-v42a-founder-minor-cleanup.json";
export const REPORT_MD = "brand-explorer-v42a-founder-minor-cleanup.md";

const HIDE_DISPLAY = "Do Not Display";
const PROPERTY_MIN = 3;

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

const ALLOWED_WRITE_FIELDS = new Set([
  ...Object.values(MAP_FIELDS),
  "External Display Status",
]);

/** Kimpton lifestyle rewrites for residual scrub / legalistic phrasing. */
export const V42A_KIMPTON_TONE_REPLACEMENTS = Object.freeze([
  {
    id: "public_franchise_performance",
    re: /\bpublic franchise performance disclosures(?:\s*\(where applicable\))?/gi,
    replace: "portfolio-level brand performance materials the company publishes",
    rationale: "Remove Item-19-rewrite franchise/legal tone; keep owner-useful diligence cue",
  },
  {
    id: "public_performance_materials_franchisey",
    re: /\bpublic performance materials(?:\s*\(where applicable\))?/gi,
    replace: "portfolio-level brand performance materials the company publishes",
    rationale: "Prefer lifestyle / owner diligence phrasing over scrub placeholder",
  },
  {
    id: "franchise_disclosure_materials",
    re: /\bfranchise disclosure materials\b/gi,
    replace: "commercial agreement materials",
    rationale: "Avoid franchise-disclosure framing",
  },
  {
    id: "letter_of_intent_boilerplate",
    re: /\bletter of intent or commercial proposal\b/gi,
    replace: "commercial proposal",
    rationale: "Drop LOI-adjacent boilerplate",
  },
  {
    id: "participation_cost_categories",
    re: /\bparticipation cost categories\b/gi,
    replace: "participation costs and program fees",
    rationale: "Replace mechanical fee-stack scrub phrasing",
  },
  {
    id: "owner_economics_awkward",
    re: /\bowner economics after brand-related costs\b/gi,
    replace: "whether Kimpton economics fit the asset after program costs",
    rationale: "Natural owner diligence phrasing",
  },
  {
    id: "confirm_costs_timing_chrome",
    re: /\bconfirm participation costs and timing directly during brand engagement(?: and legal review)?\.?/gi,
    replace:
      "Ask IHG development for current Kimpton participation costs and timing before you commit",
    rationale: "Neutralize v40C economics chrome diligence stem",
  },
  {
    id: "confirm_costs_obligations_chrome",
    re: /\bconfirm participation costs, operating obligations, and agreement terms directly during brand engagement(?: and legal review)?\.?/gi,
    replace:
      "Ask IHG development for current Kimpton participation costs, F&B operating obligations, and agreement terms before you commit",
    rationale: "Neutralize v40C economics chrome diligence stem",
  },
  {
    id: "confirm_costs_agreement_chrome",
    re: /\bconfirm participation costs and agreement terms directly during brand engagement(?: and legal review)?\.?/gi,
    replace:
      "Ask IHG development for current Kimpton participation costs and agreement terms before you commit",
    rationale: "Neutralize v40C economics chrome diligence stem",
  },
  {
    id: "confirm_diligence_boilerplate",
    re: /\bconfirm [^.]{10,160}directly during brand engagement(?: and legal review)?\.?/gi,
    replace:
      "Ask IHG development for current Kimpton participation costs, F&B operating obligations, and agreement terms before you commit",
    rationale: "Catch-all Confirm…during brand engagement scrub stem",
  },
]);

/** Everhome extended-stay rewrites — residual scrub stems + economics chrome diligence. */
export const V42A_EVERHOME_TONE_REPLACEMENTS = Object.freeze([
  {
    id: "confirm_costs_timing_chrome",
    re: /\bconfirm participation costs and timing directly during brand engagement(?: and legal review)?\.?/gi,
    replace:
      "Ask Choice development for current Everhome participation costs and timing before you commit",
    rationale: "Neutralize v40C economics chrome diligence stem",
  },
  {
    id: "confirm_costs_obligations_chrome",
    re: /\bconfirm participation costs, operating obligations, and agreement terms directly during brand engagement(?: and legal review)?\.?/gi,
    replace:
      "Ask Choice development for current Everhome suite costs, housekeeping model, and agreement terms before you commit",
    rationale: "Neutralize v40C economics chrome diligence stem",
  },
  {
    id: "confirm_costs_agreement_chrome",
    re: /\bconfirm participation costs and agreement terms directly during brand engagement(?: and legal review)?\.?/gi,
    replace:
      "Ask Choice development for current Everhome participation costs and agreement terms before you commit",
    rationale: "Neutralize v40C economics chrome diligence stem",
  },
  {
    id: "confirm_diligence_boilerplate",
    re: /\bconfirm [^.]{10,160}directly during brand engagement(?: and legal review)?\.?/gi,
    replace:
      "Ask Choice development for current Everhome suite prototype costs, housekeeping model, and agreement terms before you commit",
    rationale: "Catch-all Confirm…during brand engagement scrub stem",
  },
  {
    id: "participation_cost_categories",
    re: /\bparticipation cost categories\b/gi,
    replace: "participation costs and program fees",
    rationale: "Replace mechanical fee-stack scrub phrasing",
  },
  {
    id: "owner_economics_awkward",
    re: /\bowner economics after brand-related costs\b/gi,
    replace: "whether Everhome economics fit the asset after program costs",
    rationale: "Natural owner diligence phrasing",
  },
]);

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
    console.warn(`[v42a] failed to parse ${p}: ${err.message}`);
    return null;
  }
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

function isHidden(row) {
  return /do not display|internal only/i.test(nz(row?.externalDisplayStatus));
}

function visibleOpenings(rows = []) {
  return (rows || []).filter(
    (r) => nz(r.slotKey) === "footprint.openings" && !isHidden(r) && nz(r.imageUrl)
  );
}

/**
 * Score property examples for keep vs hide-weakest decision.
 * Prefer distinct titles / markets; demote thin titles or duplicate stems.
 */
function auditPropertyExamples(openings = []) {
  const scored = openings.map((r, idx) => {
    const title = nz(r.title);
    const body = nz(r.body);
    let score = 50;
    if (nz(r.imageUrl)) score += 20;
    if (title.length >= 20) score += 10;
    if (body.length >= 40) score += 10;
    if (/resort|spa|beach|downtown|georgetown|austin|suite/i.test(title)) score += 5;
    if (/untitled|example|placeholder|tbd/i.test(title)) score -= 40;
    if (!title) score -= 30;
    // Prefer keeping geographic diversity: slight demotion for generic short titles
    if (title.split(/\s+/).length <= 2) score -= 5;
    return {
      recordId: r.recordId,
      slotKey: r.slotKey,
      title,
      bodySample: body.slice(0, 120),
      imageUrl: nz(r.imageUrl).slice(0, 120),
      sortOrder: r.sortOrder ?? idx,
      score,
      externalDisplayStatus: nz(r.externalDisplayStatus),
    };
  });

  scored.sort((a, b) => a.score - b.score || String(a.title).localeCompare(String(b.title)));
  return scored;
}

function decidePropertyExamples(brandSlug, openings) {
  const scored = auditPropertyExamples(openings);
  const count = scored.length;
  const extras = Math.max(0, count - PROPERTY_MIN);

  // All Everhome / Kimpton current sets are distinct titled properties with imageUrls.
  // Keep extras when every card scores ≥50 and titles are unique — UI remains balanced at 4–5.
  const uniqueTitles = new Set(scored.map((s) => s.title.toLowerCase()));
  const allQuality =
    scored.every((s) => s.score >= 50 && s.imageUrl && s.title) &&
    uniqueTitles.size === scored.length;

  if (extras === 0) {
    return {
      decision: "at_minimum",
      action: "none",
      keepCount: count,
      hideCandidates: [],
      rationale: `Already at ${PROPERTY_MIN} visible property examples.`,
      inventory: scored,
    };
  }

  if (allQuality) {
    return {
      decision: "keep_all",
      action: "none",
      keepCount: count,
      hideCandidates: [],
      rationale:
        brandSlug === "everhome-suites"
          ? `Keep all ${count} Everhome examples — distinct U.S. markets, each with imageUrl, non-duplicative; layout remains balanced.`
          : `Keep all ${count} Kimpton examples — distinct lifestyle/resort properties across markets; breadth supports lifestyle positioning.`,
      inventory: scored.sort((a, b) => b.score - a.score),
      extrasAccepted: true,
    };
  }

  const hideCount = extras;
  const hideCandidates = scored.slice(0, hideCount);
  return {
    decision: "hide_weakest",
    action: "hide_external_display",
    keepCount: count - hideCount,
    hideCandidates,
    rationale: `Hide ${hideCount} weakest extra(s) via External Display Status = Do Not Display to land at ${PROPERTY_MIN}.`,
    inventory: scored.sort((a, b) => b.score - a.score),
    extrasAccepted: false,
  };
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

function rewriteKimptonTone(text) {
  return rewriteWithRules(text, V42A_KIMPTON_TONE_REPLACEMENTS);
}

function rewriteEverhomeTone(text) {
  return rewriteWithRules(text, V42A_EVERHOME_TONE_REPLACEMENTS);
}

export { rewriteKimptonTone, rewriteEverhomeTone };

function buildTonePatches(brandSlug, presentationRows) {
  const rules =
    brandSlug === "kimpton"
      ? V42A_KIMPTON_TONE_REPLACEMENTS
      : brandSlug === "everhome-suites"
        ? V42A_EVERHOME_TONE_REPLACEMENTS
        : [];
  if (!rules.length) return [];

  const patches = [];
  for (const row of presentationRows || []) {
    if (isHidden(row)) continue;
    for (const [apiKey, airtableField] of Object.entries(MAP_FIELDS)) {
      const before = nz(row[apiKey]);
      if (!before) continue;
      const { after, applied, changed } = rewriteWithRules(before, rules);
      if (!changed) continue;
      const forbiddenAfter = scanForbiddenLanguage(after);
      const mechanicalAfter = scanMechanicalCopy(after).filter((h) =>
        ["high", "medium"].includes(h.severity)
      );
      patches.push({
        brandSlug,
        recordId: row.recordId,
        table: PRESENTATION_TABLE,
        slotKey: row.slotKey,
        field: airtableField,
        apiKey,
        before,
        after,
        appliedRules: applied,
        reason:
          brandSlug === "kimpton"
            ? "v42A Kimpton lifestyle tone cleanup"
            : "v42A Everhome residual diligence-phrasing cleanup",
        safeForGenericApply: forbiddenAfter.length === 0 && mechanicalAfter.length === 0,
        forbiddenAfter,
        mechanicalAfter,
      });
    }
  }
  return patches;
}

function buildHidePatches(brandSlug, hideCandidates) {
  return (hideCandidates || []).map((c) => ({
    brandSlug,
    recordId: c.recordId,
    table: PRESENTATION_TABLE,
    slotKey: c.slotKey,
    field: "External Display Status",
    apiKey: "externalDisplayStatus",
    before: c.externalDisplayStatus || "",
    after: HIDE_DISPLAY,
    title: c.title,
    reason: "v42A hide weakest extra property example (Do Not Display; row retained)",
    safeForGenericApply: true,
    appliedRules: ["hide_extra_property_example"],
  }));
}

function applyPatchesLocally(rows, patches) {
  const byRecord = new Map();
  for (const p of patches) {
    if (!byRecord.has(p.recordId)) byRecord.set(p.recordId, {});
    if (p.apiKey) byRecord.get(p.recordId)[p.apiKey] = p.after;
    if (p.field === "External Display Status") {
      byRecord.get(p.recordId).externalDisplayStatus = p.after;
    }
  }
  return (rows || []).map((r) => {
    const overlay = byRecord.get(r.recordId);
    return overlay ? { ...r, ...overlay } : r;
  });
}

function projectRecommendation({
  brandSlug,
  propertyDecision,
  tonePatches,
  projectedOpenings,
  mediumMechanicalAfter,
  forbiddenAfter,
  osState,
}) {
  if (forbiddenAfter.length) {
    return {
      recommendation: "not_owner_ready",
      rationale: `Forbidden language remains after projection: ${forbiddenAfter.map((h) => h.label).join(", ")}`,
    };
  }
  if (projectedOpenings < PROPERTY_MIN) {
    return {
      recommendation: "remediation_required",
      rationale: `Property examples would drop below ${PROPERTY_MIN}.`,
    };
  }
  if (mediumMechanicalAfter.length) {
    return {
      recommendation: "remediation_required",
      rationale: `Medium mechanical tone still projected: ${mediumMechanicalAfter.map((h) => h.id).join(", ")}`,
    };
  }
  if (osState && osState !== "founder_review_ready" && osState !== "active_release_ready") {
    return {
      recommendation: "approve_after_minor_cleanup",
      rationale: `OS state still ${osState}; finish minor cleanup then re-audit.`,
    };
  }
  const extrasNote =
    propertyDecision.decision === "keep_all"
      ? ` Extras kept by founder decision (${propertyDecision.keepCount} examples).`
      : propertyDecision.decision === "hide_weakest"
        ? ` Extras hidden down to ${propertyDecision.keepCount}.`
        : "";
  const toneNote = tonePatches.length
    ? ` ${tonePatches.length} tone field patch(es) applied in projection.`
    : "";
  return {
    recommendation: "approve_for_active_release",
    rationale: `Minor cleanup resolved for ${brandSlug}.${extrasNote}${toneNote} Subject to explicit founder OK — v42A does not apply active release.`,
  };
}

export function parseV42AApplyFlags(argv = []) {
  const has = (flag) => argv.includes(flag);
  return {
    apply: has("--apply"),
    approve: has(V42A_APPLY_FLAGS.approve),
    noCompanyValidation: has(V42A_APPLY_FLAGS.noCompanyValidation),
    noActiveApproval: has(V42A_APPLY_FLAGS.noActiveApproval),
    noSourceLibrary: has(V42A_APPLY_FLAGS.noSourceLibrary),
    noRegistry: has(V42A_APPLY_FLAGS.noRegistry),
    noIncompleteUnlock: has(V42A_APPLY_FLAGS.noIncompleteUnlock),
    brandOnly: has(V42A_APPLY_FLAGS.brandOnly),
  };
}

export function validateV42AApplyGates({ flags, brands, brandResults } = {}) {
  const blockers = [];
  if (!flags?.apply) return { allowed: false, blockers: ["not_apply_mode"], missingFlags: [] };

  const required = [
    ["approve", V42A_APPLY_FLAGS.approve],
    ["noCompanyValidation", V42A_APPLY_FLAGS.noCompanyValidation],
    ["noActiveApproval", V42A_APPLY_FLAGS.noActiveApproval],
    ["noSourceLibrary", V42A_APPLY_FLAGS.noSourceLibrary],
    ["noRegistry", V42A_APPLY_FLAGS.noRegistry],
    ["noIncompleteUnlock", V42A_APPLY_FLAGS.noIncompleteUnlock],
    ["brandOnly", V42A_APPLY_FLAGS.brandOnly],
  ];
  const missingFlags = required.filter(([k]) => !flags[k]).map(([, flag]) => flag);
  if (missingFlags.length) blockers.push(...missingFlags.map((f) => `missing_flag:${f}`));

  const illegal = (brands || []).filter((s) => !V42A_DEFAULT_BRANDS.includes(s));
  if (illegal.length) blockers.push(`brand_only_violation:${illegal.join(",")}`);

  for (const b of V42A_FORBIDDEN_BRANDS) {
    if ((brands || []).includes(b)) blockers.push(`forbidden_brand:${b}`);
  }

  for (const b of brandResults || []) {
    for (const p of b.patches || []) {
      if (!ALLOWED_WRITE_FIELDS.has(p.field)) {
        blockers.push(`forbidden_field:${b.brandSlug}:${p.field}`);
      }
      if (p.safeForGenericApply === false) {
        blockers.push(`unsafe_patch:${b.brandSlug}:${p.slotKey}:${p.field}`);
      }
    }
    if (b.projection?.recommendation !== "approve_for_active_release") {
      blockers.push(`projection_not_ready:${b.brandSlug}:${b.projection?.recommendation}`);
    }
  }

  return {
    allowed: blockers.length === 0,
    blockers,
    missingFlags,
  };
}

async function airtablePatchPresentation(baseId, apiKey, recordId, fields) {
  for (const key of Object.keys(fields)) {
    if (!ALLOWED_WRITE_FIELDS.has(key)) {
      throw new Error(`Refusing forbidden Presentation field write: ${key}`);
    }
  }
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}/${recordId}`;
  const res = await fetch(url, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `Airtable PATCH failed: ${res.status}`);
  return json;
}

function groupPatchesByRecord(patches = []) {
  const map = new Map();
  for (const p of patches) {
    if (!p.recordId) continue;
    if (!map.has(p.recordId)) {
      map.set(p.recordId, { recordId: p.recordId, slotKey: p.slotKey, fields: {} });
    }
    const g = map.get(p.recordId);
    g.fields[p.field] = p.after;
  }
  return [...map.values()];
}

async function applyPatchesForBrand(brandResult) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const grouped = groupPatchesByRecord(brandResult.patches || []);
  const patched = [];
  const errors = [];
  for (const g of grouped) {
    try {
      await airtablePatchPresentation(baseId, apiKey, g.recordId, g.fields);
      patched.push({ recordId: g.recordId, slotKey: g.slotKey, fields: Object.keys(g.fields) });
    } catch (err) {
      errors.push({ recordId: g.recordId, slotKey: g.slotKey, message: err.message });
    }
  }
  return { patched, errors, recordsTouched: patched.length };
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

export async function auditBrandV42A(brandSlug) {
  if (V42A_FORBIDDEN_BRANDS.includes(brandSlug)) {
    throw new Error(`v42A refuses brand ${brandSlug}`);
  }
  if (!V42A_DEFAULT_BRANDS.includes(brandSlug)) {
    throw new Error(`v42A only supports ${V42A_DEFAULT_BRANDS.join(", ")}`);
  }

  const config = resolveConfig(brandSlug);
  const ctx = await loadBrandFactoryContext(brandSlug);
  const brandApi = await fetchBrandApiShape(brandSlug);
  const presentationRows = ctx?.presentationRows || brandApi?.brandExplorer?.blocks || [];
  const openings = visibleOpenings(presentationRows);

  const propertyDecision = decidePropertyExamples(brandSlug, openings);
  const hidePatches =
    propertyDecision.action === "hide_external_display"
      ? buildHidePatches(brandSlug, propertyDecision.hideCandidates)
      : [];

  const tonePatches = buildTonePatches(brandSlug, presentationRows);
  const patches = [...hidePatches, ...tonePatches];

  const projectedRows = applyPatchesLocally(presentationRows, patches);
  const projectedOpenings = visibleOpenings(projectedRows).length;

  // Project internal preview copy on patched corpus
  const projectedCorpus = projectedRows
    .filter((r) => !isHidden(r))
    .flatMap((r) => Object.keys(MAP_FIELDS).map((k) => nz(r[k])).filter(Boolean))
    .join("\n");

  const liveInternalHtml = renderBrandExplorerHtmlForTest(brandApi, {
    allPanels: true,
    internalPreview: true,
  });
  const liveInternalText = stripHtml(liveInternalHtml);
  const liveMechanical = scanMechanicalCopy(liveInternalText).filter((h) =>
    ["high", "medium"].includes(h.severity)
  );

  // Approximate post-patch internal text by also scrubbing live DOM text with same rules
  let projectedInternalText = liveInternalText;
  if (brandSlug === "kimpton") {
    projectedInternalText = rewriteKimptonTone(liveInternalText).after;
  } else if (brandSlug === "everhome-suites") {
    projectedInternalText = rewriteEverhomeTone(liveInternalText).after;
  }
  const projectedMechanical = scanMechanicalCopy(
    [projectedCorpus, projectedInternalText].join("\n")
  ).filter((h) => ["high", "medium"].includes(h.severity));
  const projectedForbidden = scanForbiddenLanguage(
    [projectedCorpus, projectedInternalText].join("\n")
  );

  const externalHtml = renderBrandExplorerHtmlForTest(brandApi, {
    allPanels: true,
    internalPreview: false,
  });
  const externalQl = evaluateBrandExternalQualityLock(brandApi, externalHtml, {
    brandSlug,
    brandBasics: ctx?.brandBasics,
  });

  const osBrand = await evaluateBrandExplorerOsBrand(brandSlug).catch((err) => ({
    error: err.message,
    canonicalState: null,
  }));

  const projection = projectRecommendation({
    brandSlug,
    propertyDecision,
    tonePatches,
    projectedOpenings,
    mediumMechanicalAfter: projectedMechanical,
    forbiddenAfter: projectedForbidden,
    osState: osBrand.canonicalState,
  });

  return {
    brandSlug,
    brandName: brandApi.name || config?.name || brandSlug,
    recordId: brandApi.id || config?.recordId || null,
    displayState: brandApi.brandExplorerDisplayState,
    shouldRenderFullProfile: brandApi.shouldRenderFullProfile === true,
    os: {
      canonicalState: osBrand.canonicalState || null,
      error: osBrand.error || null,
    },
    propertyExamples: {
      beforeCount: openings.length,
      afterCount: projectedOpenings,
      decision: propertyDecision,
    },
    toneCleanup: {
      applicable: true,
      liveMediumMechanical: liveMechanical,
      patchCount: tonePatches.length,
      sampleSlots: [...new Set(tonePatches.map((p) => p.slotKey))].slice(0, 20),
    },
    patches,
    patchSummary: {
      total: patches.length,
      hidePatches: hidePatches.length,
      tonePatches: tonePatches.length,
      recordsTouched: new Set(patches.map((p) => p.recordId)).size,
    },
    externalLock: {
      pass:
        externalQl.externalQualityLockPass === true ||
        externalQl.profileInPreparationRendered === true,
      profileInPreparation: externalQl.profileInPreparationRendered === true,
    },
    projection: {
      ...projection,
      projectedOpenings,
      projectedForbiddenCount: projectedForbidden.length,
      projectedMediumMechanicalCount: projectedMechanical.length,
      projectedCanonicalStateTarget: "founder_review_ready",
    },
    applyResult: null,
    guardrails: {
      airtableWrites: false,
      activeProfileApproval: false,
      companyValidatedChanges: false,
      unlock: false,
      sourceLibraryChanges: false,
      registryChanges: false,
      imageFieldChanges: false,
      activeReleaseApplied: false,
      radissonUntouched: true,
    },
  };
}

export async function runV42AFounderMinorCleanup({
  brands = V42A_DEFAULT_BRANDS,
  dryRun = true,
  apply = false,
  flags = null,
} = {}) {
  for (const b of brands) {
    if (V42A_FORBIDDEN_BRANDS.includes(b)) {
      throw new Error(`v42A refuses to modify ${b}`);
    }
  }

  const resolvedFlags = flags || {
    apply: false,
    approve: false,
    noCompanyValidation: false,
    noActiveApproval: false,
    noSourceLibrary: false,
    noRegistry: false,
    noIncompleteUnlock: false,
    brandOnly: false,
  };

  const brandResults = [];
  for (const brandSlug of brands) {
    brandResults.push(await auditBrandV42A(brandSlug));
  }

  const incompleteControl = await auditIncompleteStillLocked();
  const v42Prior = readJsonIfExists(
    path.join(ROOT, "reports", "brand-explorer-v42-founder-visual-review.json")
  );

  const gateCheck = validateV42AApplyGates({
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
        if (!(b.patches || []).length) {
          b.applyResult = { patched: [], errors: [], recordsTouched: 0, skipped: "no_patches" };
          continue;
        }
        b.applyResult = await applyPatchesForBrand(b);
        b.guardrails.airtableWrites = true;
      }
      applyExecuted = true;
    }
  }

  return {
    version: V42A_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: !applyExecuted,
    applyRequested: Boolean(apply),
    applyExecuted,
    applyBlocked,
    applyGateCheck: gateCheck,
    brands,
    brandResults,
    incompleteControl,
    priorV42: v42Prior
      ? {
          generatedAt: v42Prior.generatedAt,
          recommendations: (v42Prior.brandResults || [])
            .filter((b) => brands.includes(b.brandSlug))
            .map((b) => ({
              brandSlug: b.brandSlug,
              recommendation: b.releaseRecommendation?.recommendation,
            })),
        }
      : null,
    summary: {
      brands: brandResults.length,
      totalPatches: brandResults.reduce((n, b) => n + (b.patches?.length || 0), 0),
      recordsPatched: brandResults.reduce((n, b) => n + (b.applyResult?.recordsTouched || 0), 0),
      applyErrors: brandResults.reduce((n, b) => n + (b.applyResult?.errors?.length || 0), 0),
      projectedApproveForActiveRelease: brandResults.filter(
        (b) => b.projection.recommendation === "approve_for_active_release"
      ).length,
      incompleteLocked: incompleteControl.allLocked,
      anyUnlock: false,
      activeApprovalTouched: false,
      companyValidatedTouched: false,
      radissonUntouched: true,
    },
    guardrails: {
      airtableWrites: applyExecuted,
      presentationWritesOnly: true,
      allowedFields: [...ALLOWED_WRITE_FIELDS],
      registryWrites: false,
      sourceLibraryWrites: false,
      imageFieldWrites: false,
      companyValidatedChanges: false,
      activeProfileApproval: false,
      unlock: false,
      activeReleaseApplied: false,
      radissonUntouched: true,
    },
  };
}

export function renderBrandCleanupMarkdown(brand) {
  const lines = [
    `# v42A Cleanup — ${brand.brandName}`,
    "",
    `Slug: \`${brand.brandSlug}\` · Record: \`${brand.recordId || "n/a"}\``,
    `Generated: ${new Date().toISOString()} · ${V42A_VERSION}`,
    "",
    "## Projected release recommendation",
    "",
    `**${brand.projection.recommendation}**`,
    "",
    brand.projection.rationale,
    "",
    `Target OS state: **${brand.projection.projectedCanonicalStateTarget}** (current: ${brand.os.canonicalState || "n/a"})`,
    "",
    "## Property examples",
    "",
    `- Before: **${brand.propertyExamples.beforeCount}** · After: **${brand.propertyExamples.afterCount}**`,
    `- Decision: **${brand.propertyExamples.decision.decision}**`,
    `- ${brand.propertyExamples.decision.rationale}`,
    "",
  ];

  lines.push("### Inventory (scored)", "");
  for (const item of brand.propertyExamples.decision.inventory || []) {
    lines.push(`- score=${item.score} · ${item.title} (\`${item.recordId}\`)`);
  }
  lines.push("");

  if (brand.propertyExamples.decision.hideCandidates?.length) {
    lines.push("### Hide candidates (Do Not Display)", "");
    for (const h of brand.propertyExamples.decision.hideCandidates) {
      lines.push(`- ${h.title} (\`${h.recordId}\`) score=${h.score}`);
    }
    lines.push("");
  }

  if (brand.toneCleanup.patchCount > 0 || brand.toneCleanup.liveMediumMechanical?.length) {
    lines.push(
      brand.brandSlug === "kimpton" ? "## Kimpton tone cleanup" : "## Tone / diligence phrasing cleanup",
      ""
    );
    lines.push(`- Tone patches: **${brand.toneCleanup.patchCount}**`);
    lines.push(
      `- Live medium mechanical before: ${
        brand.toneCleanup.liveMediumMechanical.map((h) => h.id).join(", ") || "none"
      }`
    );
    lines.push(
      `- Projected medium mechanical after: ${brand.projection.projectedMediumMechanicalCount}`
    );
    if (brand.toneCleanup.sampleSlots.length) {
      lines.push(`- Sample slots: ${brand.toneCleanup.sampleSlots.map((s) => `\`${s}\``).join(", ")}`);
    }
    lines.push("");
  }

  lines.push("## Patches", "");
  lines.push(
    `- Total: ${brand.patchSummary.total} (hide=${brand.patchSummary.hidePatches}, tone=${brand.patchSummary.tonePatches})`
  );
  lines.push(`- Records: ${brand.patchSummary.recordsTouched}`);
  if (!brand.patches.length) {
    lines.push("- No Airtable writes required for this brand (decision-only / already clean).");
  } else {
    for (const p of brand.patches.slice(0, 30)) {
      lines.push(
        `- \`${p.slotKey}\` · ${p.field} · ${p.reason}${p.appliedRules?.length ? ` · rules=${p.appliedRules.join(",")}` : ""}`
      );
    }
  }
  lines.push("");

  if (brand.applyResult) {
    lines.push("## Apply result", "");
    lines.push(`- Records touched: ${brand.applyResult.recordsTouched}`);
    lines.push(`- Errors: ${brand.applyResult.errors?.length || 0}`);
    if (brand.applyResult.skipped) lines.push(`- Skipped: ${brand.applyResult.skipped}`);
    lines.push("");
  }

  lines.push(
    "## Guardrails",
    "",
    "- No active release",
    "- No active-profile approval",
    "- No Company Validated",
    "- No Source Library / Registry / image-field writes (except External Display Status hide if selected)",
    "- Radisson + incomplete brands untouched",
    ""
  );

  return lines.join("\n");
}

export function writeV42AReports(report, reportsDir = path.join(ROOT, "reports")) {
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, REPORT_JSON);
  const mdPath = path.join(reportsDir, REPORT_MD);
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2), "utf8");

  const md = [
    "# v42A Founder Minor Cleanup",
    "",
    `Generated: ${report.generatedAt}`,
    `dryRun=${report.dryRun} applyExecuted=${report.applyExecuted} applyBlocked=${report.applyBlocked}`,
    "",
    "## Summary",
    "",
    `- Brands: ${report.summary.brands}`,
    `- Patches: ${report.summary.totalPatches}`,
    `- Records patched: ${report.summary.recordsPatched}`,
    `- Projected approve_for_active_release: ${report.summary.projectedApproveForActiveRelease}/${report.summary.brands}`,
    `- Incomplete locked: **${report.summary.incompleteLocked ? "yes" : "no"}**`,
    `- Radisson untouched: **yes**`,
    "",
  ];

  for (const b of report.brandResults) {
    md.push(`### ${b.brandName}`);
    md.push(`- property decision: **${b.propertyExamples.decision.decision}** (${b.propertyExamples.beforeCount} → ${b.propertyExamples.afterCount})`);
    md.push(`- tone patches: ${b.toneCleanup.patchCount}`);
    md.push(`- projected: **${b.projection.recommendation}**`);
    md.push(`- ${b.projection.rationale}`);
    md.push("");
  }

  md.push("## Incomplete control", "");
  for (const r of report.incompleteControl.results || []) {
    md.push(`- \`${r.brandSlug}\`: ${r.pass ? "locked" : "**NOT LOCKED**"}`);
  }
  md.push("");
  md.push("## Guardrails");
  md.push("- No active release · no approval · no Company Validated · no unlock · Radisson untouched");
  md.push("");

  fs.writeFileSync(mdPath, md.join("\n"), "utf8");

  const brandPaths = {};
  for (const b of report.brandResults) {
    const short = b.brandSlug === "everhome-suites" ? "everhome" : b.brandSlug;
    const fname = `brand-explorer-v42a-${short}-cleanup.md`;
    const fpath = path.join(reportsDir, fname);
    fs.writeFileSync(fpath, renderBrandCleanupMarkdown(b), "utf8");
    brandPaths[b.brandSlug] = fpath;
  }

  return { jsonPath, mdPath, brandPaths };
}
