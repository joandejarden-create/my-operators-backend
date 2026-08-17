/**
 * Brand Explorer 62 — Webhound Public Tabs Batch C (owner-facing claims).
 *
 * Scope: C_public_tab_high from reconciliation v1, limited to Overview,
 * Value to Owners, Operating Model, Owner Considerations, Commercial Engine,
 * Economics (Economic Obligation), Loyalty Program.
 *
 * Presentation Title/Body softens only. No Setup/Census/Status/CV/BV.
 * Excludes Recent Momentum, Property Examples, Parent/Family (Batches A/B/D/E/F).
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  EXPECTED_ACTIVE_COUNT_62,
  FREEZE_DECISION_62,
  ROOT,
} from "./brand-explorer-62-active-public-full-baseline.js";
import { loadActiveUniverse } from "./brand-explorer-active-universe.js";
import { PRESENTATION_TABLE } from "./brand-explorer-62-webhound-claim-validation.js";

export const BATCH_C_VERSION =
  "brand-explorer-62-webhound-public-tabs-batch-c-owner-facing-claims-v1";
export const BATCH_C_OBJECTIVE =
  "brand-explorer-62-webhound-public-tabs-batch-c-owner-facing-claims-v1";
export const BATCH_C_STATUS_COMPLETE =
  "brand_explorer_62_webhound_public_tabs_batch_c_owner_facing_claims_complete_ready_for_loyalty_or_economic_spotcheck";
export const BATCH_C_STATUS_PARTIAL =
  "brand_explorer_62_webhound_public_tabs_batch_c_owner_facing_claims_partial_steward_review_needed";
export const BATCH_C_STATUS_BLOCKED =
  "brand_explorer_62_webhound_public_tabs_batch_c_owner_facing_claims_blocked";
export const BATCH_C_STATUS_DRY_RUN =
  "brand_explorer_62_webhound_public_tabs_batch_c_owner_facing_claims_dry_run_ready";

export const RECON_JSON =
  "reports/brand-explorer/brand-explorer-62-webhound-airtable-reconciliation-v1.json";
export const BATCH_A_JSON =
  "reports/brand-explorer/brand-explorer-62-webhound-claim-patch-batch-a-momentum-blockers.json";
export const REPORT_JSON =
  "brand-explorer-62-webhound-public-tabs-batch-c-owner-facing-claims.json";
export const REPORT_MD =
  "brand-explorer-62-webhound-public-tabs-batch-c-owner-facing-claims.md";
export const DOCS_MD =
  "brand-explorer-62-webhound-public-tabs-batch-c-owner-facing-claims.md";

export const BATCH_C_NAME = "C_public_tab_high";
export const EXPECTED_BATCH_ITEM_COUNT = 24;
export const EXPECTED_IN_SCOPE_COUNT = 23;

export const ALLOWED_TABS = Object.freeze([
  "Overview",
  "Value to Owners",
  "Operating Model",
  "Owner Considerations",
  "Commercial Engine",
  "Economics",
  "Loyalty Program",
]);

export const APPLY_FLAGS = Object.freeze([
  "--confirm-batch-c-only",
  "--confirm-no-batch-a-reapply",
  "--confirm-no-batch-b-d-e-f",
  "--confirm-public-tabs-presentation-only",
  "--confirm-no-brand-setup-writes",
  "--confirm-no-census-writes",
  "--confirm-no-brand-status-changes",
  "--confirm-no-release-field-writes",
  "--confirm-no-company-validated-writes",
  "--confirm-no-brand-verified-writes",
  "--confirm-founder-approved-batch-c",
]);

const ALLOWED_WRITE_FIELDS = new Set(["Title", "Body"]);
const FORBIDDEN_WRITE_FIELDS = Object.freeze([
  "Company Validated",
  "Company Validation Date",
  "Brand Verified",
  "Brand Status",
  "Release",
  "Ready for Active Profile",
  "Active Profile Approved",
  "Active Profile Approved Date",
  "Founder Visual Review Pass",
]);

const FORBIDDEN_PUBLIC_RES = [
  /\bWebhound\b/i,
  /\bsource pack\b/i,
  /\bsource-supported\b/i,
  /\bsource data\b/i,
  /\bmetadata\b/i,
  /\bFDD\b/,
  /\bItem\s*19\b/i,
  /\bLOI\b/,
  /\bPVQL\b/i,
  /\bQA gate\b|\bQA checklist\b|\bfactory QA\b/i,
  /\bvalidation lane\b/i,
  /\bfactory\b/i,
  /\bStage\s*\d/i,
  /\bactive universe\b/i,
  /\bCensus\b/,
  /\bCvent\b/i,
  /\bHBX\b/,
  /\bHotelbeds\b/i,
  /\bDataForSEO\b/i,
  /\binternal review\b/i,
  /\bsteward\b/i,
  /\bscraped\b/i,
  /\bevidence\b/i,
  /\bclaim patch\b/i,
];

const WRITE_THROTTLE_MS = 320;
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPORTS_DIR = path.join(ROOT, "reports", "brand-explorer");
const DOCS_DIR = path.join(ROOT, "docs", "data-intelligence");

/**
 * Curated softens keyed by recon-rem id. Only Title/Body values that change.
 * Keep = empty patch (unchanged).
 */
export const BATCH_C_SOFTENS = Object.freeze({
  "recon-rem-3": {
    outcome: "softened",
    fields: {
      Body: [
        "Marriott Bonvoy participation supports the commercial case for AC Hotels.",
        "Marriott International distribution and commercial infrastructure are part of the affiliation evaluation.",
        "Systems and loyalty readiness should be sequenced with product and staffing work.",
        "Compare commercial obligations across peer brands before selecting AC Hotels.",
      ].join("\n"),
    },
  },
  "recon-rem-7": {
    outcome: "softened",
    fields: {
      Body: "Coordinate Marriott Bonvoy readiness, training, staffing, and commercial launch with product completion for Aloft Hotels. Clarify owner, operator, and brand responsibilities before opening so the guest promise is deliverable from day one. Keep Aloft Hotels product and service responsibilities clear among owner, operator, and brand teams so the brand stays deliverable after affiliation and through ongoing operations.",
    },
  },
  "recon-rem-8": {
    outcome: "softened",
    fields: {
      Title: "Reservation and distribution participation",
    },
    note: "Title-only label soften; no fee percentages introduced.",
  },
  "recon-rem-15": {
    outcome: "softened",
    fields: {
      Body: "Clarify how IHG Hotels & Resorts reporting, loyalty, and distribution expectations interact with the operator's reporting role under the specific agreement.",
    },
  },
  "recon-rem-17": {
    outcome: "softened",
    fields: {
      Body: "Quality review is typically most important around conversion, opening, and remediation. Confirm current timing and escalation procedures for Bunkhouse Hotels with brand and operator teams.",
    },
  },
  "recon-rem-19": {
    outcome: "softened",
    fields: {
      Body: "Within BWH's independent-hotel offering, BW Premier Collection is commonly compared with BW Signature Collection as a more design-intensive soft-brand path. Owners should compare the two on product readiness, desired guest experience, capital scope, and degree of independent expression before selecting a path.",
    },
  },
  "recon-rem-24": {
    outcome: "softened",
    fields: {
      Body: "Open with the independent identity and BWH commercial presence working together: guest-facing storytelling should remain clear while booking, loyalty, and service processes operate reliably. Confirm launch support, channel activation, and operational coverage rather than assuming affiliation alone resets market position overnight.",
    },
  },
  "recon-rem-30": {
    outcome: "softened",
    fields: {
      Body: [
        "Typical guest: business and leisure travelers who want value, convenience, and a consistent breakfast-led stay.",
        "Owner journey: feasibility on breakfast economics → prototype or property-improvement plan for refreshed guestroom and lobby → systems cutover → opening quality checks on smoke-free and breakfast execution.",
        "Ramp: loyalty and enterprise mix typically build after opening—do not assume peer upscale brand rate curves.",
        "Ongoing: maintain brand consistency through property-improvement and quality cycles rather than relying on portfolio headlines alone.",
      ].join("\n"),
    },
  },
  "recon-rem-32": {
    outcome: "softened",
    fields: {
      Body: "Choice enterprise channels and Choice Privileges participation give owners familiar commercial tools when the physical product matches upper-midscale expectations. Validate channel mix, loyalty fulfillment, and agreement-specific participation costs before opening—execute on central reservations connectivity and member-benefit delivery rather than assuming automatic pricing power from portfolio headlines alone.",
    },
  },
  "recon-rem-37": {
    outcome: "softened",
    fields: {
      Body: "Launch with the Courtyard guest promise consistently expressed across service and channels while platform systems stabilize. Keep escalation paths clear for the first operating weeks and confirm quality readiness against Courtyard's Marriott select-service positioning for business and bleisure demand.",
    },
  },
  "recon-rem-40": {
    outcome: "softened",
    fields: {
      Body: "Design review, F&B program approval, and recurring quality interaction with Hilton brand teams are common across conversion and stabilized operations. Budget owner and management time for milestone reviews, remediation, and culinary quality checks—interaction intensity often rises around opening and major property-improvement cycles.",
    },
  },
  "recon-rem-43": {
    outcome: "softened",
    fields: {
      Body: "Providing a recognizable upscale lifestyle option in markets where Dazzler's design identity and Wyndham Rewards distribution have established presence.",
    },
  },
  "recon-rem-47": {
    outcome: "softened",
    fields: {
      Body: "Affiliation within Marriott International's portfolio can support credibility with lenders, operators, and guests—confirm commercial participation details in the specific agreement.",
    },
    tabBucket: "commercial",
  },
  "recon-rem-48": {
    outcome: "softened",
    fields: {
      Body: "Standards should protect DoubleTree by Hilton brand identity while remaining executable for the specific asset, market, and operator. DoubleTree is distinct from Hilton Hotels & Resorts (flagship). Align property-improvement and lifecycle capital planning from the asset review.",
    },
  },
  "recon-rem-53": {
    outcome: "softened",
    fields: {
      Body: "Brand interaction typically centers on development, conversion, systems, quality, and commercial readiness. Establish a practical decision calendar among owner, operator, and brand teams.",
    },
  },
  "recon-rem-57": {
    outcome: "softened",
    fields: {
      Body: "Strong fit when the operator has midscale extended-stay depth, can meet prototype or property-improvement requirements, works with professional third-party management, and models owner economics under the specific agreement. Weak fit when the market cannot support the required amenity stack or the operator lacks extended-stay experience.",
    },
    economicsNote: "Removed unsupported 6% royalty percentage.",
  },
  "recon-rem-59": {
    outcome: "softened",
    fields: {
      Body: "Fairmont positioning is anchored in official brand documentation and verified property-level examples. The Fairmont Accor Group brand page provides the canonical identity reference for owner review. Operating examples such as Riviera Maya, Mexico help owners evaluate regional fit—confirm current positioning against the specific asset.",
    },
  },
  "recon-rem-65": {
    outcome: "softened",
    fields: {
      Body: "Conversion-led affiliation for existing independent boutique properties rather than a standardized new-build prototype. Sponsors should model design and story review timelines and Accor systems integration before treating affiliation as a light administrative step.",
    },
  },
  "recon-rem-69": {
    outcome: "unchanged",
    fields: {},
    note: "Recommended keep — Hilton Honors training claim directionally supported; leave Body unchanged.",
  },
  "recon-rem-74": {
    outcome: "softened",
    fields: {
      Body: "Within IHG Hotels & Resorts, Holiday Inn Express functions as a scaled upper-midscale limited-service select brand. Portfolio decisions should compare segment, design intensity, and operating complexity against peers such as Holiday Inn and avid hotels, and recognize EVEN Hotels sits in a different IHG tier.",
    },
  },
  "recon-rem-76": {
    outcome: "softened",
    fields: {
      Body: "Underwrite Home2 Suites by Hilton as midscale, cost-conscious extended-stay with flexible suite configurations and efficient operations—not as upscale Homewood Suites by Hilton.",
    },
  },
  "recon-rem-78": {
    outcome: "softened",
    fields: {
      Body: "Training should connect Hilton Honors expectations with the Homewood Suites by Hilton service identity. Align modules, timing, and refresh expectations in the pre-opening plan.",
    },
  },
  "recon-rem-83": {
    outcome: "softened",
    fields: {
      Body: [
        "Hotel Indigo standards should keep the property's local story visible while meeting IHG lifestyle brand expectations. Owners should underwrite to design narrative, public-space activation, and service delivery—not marketing language alone.",
        "Design and conversion detail: Adaptive reuse and urban conversions can fit when the building and district support a coherent guest journey.",
        "Property-improvement / lifecycle capital: Confirm opening and conversion scope directly; do not assume a light refresh is enough.",
        "Localization: Local storytelling is encouraged within brand guardrails and must be operationally deliverable.",
        "Design flexibility: Prototype-driven with conversion paths—confirm the design manual for your asset.",
        "Conversion and development: Conversion and new construction—confirm scope in the commercial agreement.",
        "Localization detail: Brand standards govern guest-facing consistency; limited localization of core guest touchpoints.",
        "Property-improvement detail: Confirm opening, conversion, and renewal scope per agreement and sequence with financing.",
        "Prototype dependence: Brand prototype with efficient footprint where applicable.",
      ].join("\n"),
    },
  },
});

function nz(v) {
  if (v == null) return "";
  if (Array.isArray(v)) return v.length ? String(v[0] ?? "").trim() : "";
  return String(v).trim();
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function readJson(rel) {
  const p = path.isAbsolute(rel) ? rel : path.join(ROOT, rel);
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function hasForbiddenPublicLanguage(text) {
  const t = nz(text);
  return FORBIDDEN_PUBLIC_RES.filter((re) => re.test(t)).map((re) => re.source);
}

function tabBucket(publicTab) {
  if (publicTab === "Economics") return "economic_obligation";
  if (publicTab === "Loyalty Program") return "loyalty";
  if (publicTab === "Commercial Engine") return "commercial_engine";
  if (publicTab === "Operating Model") return "operating_model";
  if (publicTab === "Value to Owners") return "value_to_owners";
  if (publicTab === "Owner Considerations") return "owner_considerations";
  if (publicTab === "Overview") return "overview";
  return "other";
}

export function loadBatchCItems(recon = readJson(RECON_JSON)) {
  if (!recon) throw new Error(`Missing ${RECON_JSON}`);
  const batch = (recon.proposedRemediationBatches || []).find((b) => b.batch === BATCH_C_NAME);
  if (!batch) throw new Error(`Missing batch ${BATCH_C_NAME}`);
  const idSet = new Set(batch.itemIds || []);
  const all = (recon.remediationQueue || []).filter((q) => idSet.has(q.id));
  const excluded = [];
  const inScope = [];
  for (const item of all) {
    if (
      item.publicTab === "Recent Momentum" ||
      /^footprint\.momentum/i.test(nz(item.slotKey)) ||
      item.claimType === "recent_momentum"
    ) {
      excluded.push({ id: item.id, reason: "recent_momentum_excluded_from_batch_c", item });
      continue;
    }
    if (!ALLOWED_TABS.includes(item.publicTab)) {
      excluded.push({ id: item.id, reason: `tab_out_of_scope:${item.publicTab}`, item });
      continue;
    }
    if (item.airtableTable !== PRESENTATION_TABLE) {
      excluded.push({ id: item.id, reason: `non_presentation:${item.airtableTable}`, item });
      continue;
    }
    inScope.push(item);
  }
  return { batch, all, inScope, excluded };
}

export function planBatchCPatches(inScopeItems) {
  const patches = [];
  const steward = [];
  for (const item of inScopeItems) {
    const soften = BATCH_C_SOFTENS[item.id];
    if (!soften) {
      steward.push({
        id: item.id,
        brandSlug: item.brandSlug,
        reason: "missing_curated_soften_map_entry",
      });
      continue;
    }
    if (soften.outcome === "unchanged" || !Object.keys(soften.fields || {}).length) {
      patches.push({
        remId: item.id,
        recordId: item.recordId,
        table: PRESENTATION_TABLE,
        brand: item.brand,
        brandSlug: item.brandSlug,
        publicTab: item.publicTab,
        tabBucket: tabBucket(item.publicTab),
        slotKey: item.slotKey,
        fieldName: item.fieldName,
        outcome: "unchanged",
        fields: {},
        sourceUrl: item.sourceUrl,
        beforeValue: item.currentValue,
        note: soften.note || null,
        economicsNote: soften.economicsNote || null,
      });
      continue;
    }
    for (const k of Object.keys(soften.fields)) {
      if (!ALLOWED_WRITE_FIELDS.has(k)) {
        throw new Error(`${item.id}: disallowed field ${k}`);
      }
      const hits = hasForbiddenPublicLanguage(soften.fields[k]);
      if (hits.length) {
        throw new Error(`${item.id}: forbidden public language in soften (${hits.join(",")})`);
      }
    }
    patches.push({
      remId: item.id,
      recordId: item.recordId,
      table: PRESENTATION_TABLE,
      brand: item.brand,
      brandSlug: item.brandSlug,
      publicTab: item.publicTab,
      tabBucket: tabBucket(item.publicTab),
      slotKey: item.slotKey,
      fieldName: item.fieldName,
      outcome: soften.outcome,
      fields: soften.fields,
      sourceUrl: item.sourceUrl,
      beforeValue: item.currentValue,
      note: soften.note || null,
      economicsNote: soften.economicsNote || null,
    });
  }
  return { patches, steward };
}

async function airtableGet(baseId, token, table, recordId) {
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/${recordId}`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `GET ${recordId} failed ${res.status}`);
  return json;
}

async function airtablePatch(baseId, token, table, recordId, fields) {
  for (const k of Object.keys(fields)) {
    if (!ALLOWED_WRITE_FIELDS.has(k)) throw new Error(`Refused write to ${k}`);
    if (FORBIDDEN_WRITE_FIELDS.includes(k)) throw new Error(`Forbidden field ${k}`);
  }
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/${recordId}`;
  const res = await fetch(url, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `PATCH ${recordId} failed ${res.status}`);
  return json;
}

function preflight({ batch, inScope, patches, steward, universe, flagsOk, apply, batchA }) {
  const issues = [];
  if ((batch?.itemIds || []).length !== EXPECTED_BATCH_ITEM_COUNT) {
    issues.push(`batch_item_count_${batch?.itemIds?.length}_expected_${EXPECTED_BATCH_ITEM_COUNT}`);
  }
  if (inScope.length !== EXPECTED_IN_SCOPE_COUNT) {
    issues.push(`in_scope_${inScope.length}_expected_${EXPECTED_IN_SCOPE_COUNT}`);
  }
  if (universe?.totalCount !== EXPECTED_ACTIVE_COUNT_62) {
    issues.push(`active_universe_${universe?.totalCount}_expected_${EXPECTED_ACTIVE_COUNT_62}`);
  }
  if (apply && !flagsOk) issues.push("missing_required_apply_flags");
  if (
    batchA &&
    !String(batchA.status || "").includes("batch_a_momentum_blockers_complete")
  ) {
    issues.push("batch_a_not_marked_complete");
  }
  if (steward.length) issues.push(`steward_missing_softens:${steward.length}`);
  for (const p of patches) {
    if (p.table !== PRESENTATION_TABLE) issues.push(`bad_table:${p.recordId}`);
    if (/momentum/i.test(p.slotKey || "")) issues.push(`momentum_slot_in_batch_c:${p.recordId}`);
    for (const f of Object.keys(p.fields || {})) {
      if (!ALLOWED_WRITE_FIELDS.has(f)) issues.push(`disallowed_field:${f}`);
    }
  }
  return issues;
}

export async function runBatchCPublicTabsPatch({ apply = false, argv = [], token, baseId } = {}) {
  const apiKey = token || process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  const bid = baseId || process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !bid) throw new Error("Set AIRTABLE_API_KEY/AIRTABLE_PAT and AIRTABLE_BASE_ID");

  const flagsOk = APPLY_FLAGS.every((f) => argv.includes(f));
  const recon = readJson(RECON_JSON);
  const batchA = readJson(BATCH_A_JSON);
  const { batch, all, inScope, excluded } = loadBatchCItems(recon);
  const { patches, steward } = planBatchCPatches(inScope);
  const universe = await loadActiveUniverse({ includeBrandApi: false });
  const issues = preflight({
    batch,
    inScope,
    patches,
    steward,
    universe,
    flagsOk,
    apply,
    batchA,
  });

  const planned = [];
  for (const p of patches) {
    const live = await airtableGet(bid, apiKey, p.table, p.recordId);
    const fields = live.fields || {};
    const liveSlot = nz(fields["Slot Key"]);
    if (liveSlot && p.slotKey && liveSlot !== p.slotKey) {
      issues.push(`live_slot_mismatch:${p.recordId}:${liveSlot}!=${p.slotKey}`);
    }
    const nextFields = {};
    for (const [k, v] of Object.entries(p.fields || {})) {
      if (nz(fields[k]) !== nz(v)) nextFields[k] = v;
    }
    planned.push({
      ...p,
      liveBefore: {
        title: fields.Title ?? null,
        body: fields.Body ?? null,
        slotKey: liveSlot,
        active: fields.Active,
        eds: fields["External Display Status"] ?? null,
      },
      fieldsToWrite: nextFields,
      noop: p.outcome === "unchanged" || Object.keys(nextFields).length === 0,
    });
    await sleep(80);
  }

  let status = apply ? BATCH_C_STATUS_BLOCKED : BATCH_C_STATUS_DRY_RUN;
  const applyResults = [];
  let airtableWrites = 0;

  if (issues.length) {
    status = BATCH_C_STATUS_BLOCKED;
  } else if (apply) {
    for (const p of planned) {
      if (p.noop) {
        applyResults.push({
          remId: p.remId,
          recordId: p.recordId,
          brandSlug: p.brandSlug,
          outcome: p.outcome === "unchanged" ? "unchanged" : "already_matches_soften",
          fieldsWritten: {},
        });
        continue;
      }
      try {
        const updated = await airtablePatch(bid, apiKey, p.table, p.recordId, p.fieldsToWrite);
        airtableWrites += 1;
        applyResults.push({
          remId: p.remId,
          recordId: p.recordId,
          brandSlug: p.brandSlug,
          outcome: p.outcome,
          fieldsWritten: p.fieldsToWrite,
          afterTitle: updated.fields?.Title ?? null,
          afterBody: String(updated.fields?.Body || "").slice(0, 240),
        });
      } catch (err) {
        applyResults.push({
          remId: p.remId,
          recordId: p.recordId,
          brandSlug: p.brandSlug,
          outcome: "error",
          error: err?.message || String(err),
        });
      }
      await sleep(WRITE_THROTTLE_MS);
    }
    const errCount = applyResults.filter((r) => r.outcome === "error").length;
    status = errCount || steward.length ? BATCH_C_STATUS_PARTIAL : BATCH_C_STATUS_COMPLETE;
  }

  const byTab = {};
  for (const p of planned) {
    byTab[p.publicTab] = byTab[p.publicTab] || { reviewed: 0, patched: 0, unchanged: 0 };
    byTab[p.publicTab].reviewed += 1;
    if (p.outcome === "unchanged" || p.noop) byTab[p.publicTab].unchanged += 1;
    else byTab[p.publicTab].patched += 1;
  }

  const brands = [...new Set(planned.map((p) => p.brandSlug))].sort();
  const softened = planned.filter((p) => p.outcome === "softened" && !p.noop);
  const unchanged = planned.filter((p) => p.outcome === "unchanged" || p.noop);
  const economicsChanged = planned.filter(
    (p) => p.tabBucket === "economic_obligation" && p.outcome === "softened" && !p.noop
  );
  const loyaltyChanged = planned.filter(
    (p) => p.tabBucket === "loyalty" && p.outcome === "softened" && !p.noop
  );
  const commercialChanged = planned.filter(
    (p) => p.tabBucket === "commercial_engine" && p.outcome === "softened" && !p.noop
  );
  const feeRemoved = planned.filter((p) => p.economicsNote);

  const report = {
    version: BATCH_C_VERSION,
    objective: BATCH_C_OBJECTIVE,
    generatedAt: new Date().toISOString(),
    status,
    mode: apply ? (issues.length ? "blocked" : "apply") : "dry-run",
    freezeBaseline: FREEZE_DECISION_62,
    reconSource: RECON_JSON,
    batch: BATCH_C_NAME,
    founderDecision:
      "Reconcile and correct key owner-facing public tabs only; do not apply Batches B/D/E/F; preserve Batch A.",
    summary: {
      batchItems: all.length,
      inScopeReviewed: inScope.length,
      excludedCount: excluded.length,
      brandsReviewed: brands.length,
      rowsReviewed: planned.length,
      rowsSoftened: softened.length,
      rowsUnchanged: unchanged.length,
      rowsRemovedOrDoNotDisplay: 0,
      rowsStewardReview: steward.length,
      economicObligationChanged: economicsChanged.length,
      loyaltyChanged: loyaltyChanged.length,
      commercialEngineChanged: commercialChanged.length,
      unsupportedFeeClaimsRemoved: feeRemoved.length,
      airtableWrites,
      activeUniverse: universe.totalCount,
      wouldPatch: planned.filter((p) => !p.noop).length,
    },
    byTab,
    brandsReviewed: brands,
    scopeGuards: {
      batchCOnly: true,
      batchAPreserved: true,
      batchAStatus: batchA?.status || null,
      batchesBDEFUntouched: true,
      presentationOnly: true,
      allowedTabs: [...ALLOWED_TABS],
      brandSetupWrites: false,
      hotelPropertyCensusWrites: false,
      brandStatusChanges: false,
      releaseFieldWrites: false,
      companyValidatedWrites: false,
      brandVerifiedWrites: false,
      allowedWriteFields: [...ALLOWED_WRITE_FIELDS],
      recentMomentumExcluded: true,
      propertyExamplesUntouched: true,
      parentFamilyUntouched: true,
    },
    excluded: excluded.map((e) => ({
      id: e.id,
      reason: e.reason,
      publicTab: e.item?.publicTab,
      slotKey: e.item?.slotKey,
    })),
    steward,
    preflightIssues: issues,
    applyFlagsRequired: APPLY_FLAGS,
    applyFlagsPresent: flagsOk,
    patches: planned.map((p) => ({
      remId: p.remId,
      recordId: p.recordId,
      brand: p.brand,
      brandSlug: p.brandSlug,
      publicTab: p.publicTab,
      tabBucket: p.tabBucket,
      slotKey: p.slotKey,
      fieldName: p.fieldName,
      outcome: p.outcome,
      noop: p.noop,
      sourceUrl: p.sourceUrl,
      note: p.note,
      economicsNote: p.economicsNote,
      before: {
        title: p.liveBefore?.title,
        body: String(p.liveBefore?.body || "").slice(0, 500),
      },
      after: p.fields,
      fieldsToWrite: p.fieldsToWrite,
    })),
    applyResults,
    postApplyGates: null,
    exactApplyCommand: [
      "node scripts/brand-explorer-62-webhound-public-tabs-batch-c-owner-facing-claims.mjs --apply \\",
      ...APPLY_FLAGS.map((f, i) => `  ${f}${i === APPLY_FLAGS.length - 1 ? "" : " \\"}`),
    ].join("\n"),
  };

  return report;
}

export function renderBatchCMarkdown(report) {
  const s = report.summary || {};
  const gates = report.postApplyGates || {};
  const lines = [
    `# Brand Explorer 62 — Webhound Public Tabs Batch C (owner-facing claims)`,
    ``,
    `**Version:** \`${report.version}\`  `,
    `**Objective:** \`${report.objective}\`  `,
    `**Status:** \`${report.status}\`  `,
    `**Mode:** ${report.mode}  `,
    `**Generated:** ${report.generatedAt}  `,
    `**Freeze baseline:** \`${report.freezeBaseline}\``,
    ``,
    `## Founder decision`,
    ``,
    `- Correct Overview, Value to Owners, Operating Model, Owner Considerations, Commercial Engine, Economic Obligation, Loyalty Program only.`,
    `- Preserve Batch A. Do not apply Batches B / D / E / F.`,
    `- No Census, Brand Setup, Brand Status, release, Company Validated, or Brand Verified writes.`,
    ``,
    `## Summary`,
    ``,
    `| Metric | Value |`,
    `| --- | ---: |`,
    `| Brands reviewed | ${s.brandsReviewed} |`,
    `| Rows reviewed | ${s.rowsReviewed} |`,
    `| Softened | ${s.rowsSoftened} |`,
    `| Unchanged | ${s.rowsUnchanged} |`,
    `| Removed / Do Not Display | ${s.rowsRemovedOrDoNotDisplay} |`,
    `| Steward review | ${s.rowsStewardReview} |`,
    `| Economic Obligation changed | ${s.economicObligationChanged} |`,
    `| Loyalty changed | ${s.loyaltyChanged} |`,
    `| Commercial Engine changed | ${s.commercialEngineChanged} |`,
    `| Unsupported fee claims removed | ${s.unsupportedFeeClaimsRemoved} |`,
    `| Airtable writes | ${s.airtableWrites} |`,
    `| Active universe | ${s.activeUniverse} |`,
    ``,
    `## By tab`,
    ``,
    `| Tab | Reviewed | Patched | Unchanged |`,
    `| --- | ---: | ---: | ---: |`,
  ];
  for (const [tab, v] of Object.entries(report.byTab || {})) {
    lines.push(`| ${tab} | ${v.reviewed} | ${v.patched} | ${v.unchanged} |`);
  }
  lines.push(``);
  lines.push(`## Scope confirmation`);
  lines.push(``);
  lines.push(`- Batch A preserved: **${report.scopeGuards?.batchAPreserved ? "yes" : "no"}** (\`${report.scopeGuards?.batchAStatus || "—"}\`)`);
  lines.push(`- Batches B/D/E/F untouched: **yes**`);
  lines.push(`- Brand Setup writes: **no**`);
  lines.push(`- Hotel Property Census writes: **no**`);
  lines.push(`- Protected fields untouched: **yes**`);
  lines.push(`- Recent Momentum excluded from Batch C: **yes**`);
  lines.push(``);
  if (report.excluded?.length) {
    lines.push(`## Excluded from Batch C`);
    lines.push(``);
    for (const e of report.excluded) {
      lines.push(`- \`${e.id}\` · ${e.reason} · ${e.publicTab || "—"} · \`${e.slotKey || ""}\``);
    }
    lines.push(``);
  }
  lines.push(`## Patches`);
  lines.push(``);
  for (const p of report.patches || []) {
    lines.push(`### ${p.brand} · ${p.publicTab} · \`${p.remId}\``);
    lines.push(``);
    lines.push(`- Record: \`${p.recordId}\` · slot \`${p.slotKey}\` · field \`${p.fieldName}\``);
    lines.push(`- Outcome: \`${p.outcome}\`${p.noop ? " (noop)" : ""}`);
    if (p.sourceUrl) lines.push(`- Source: ${p.sourceUrl}`);
    if (p.economicsNote) lines.push(`- Economics note: ${p.economicsNote}`);
    if (p.note) lines.push(`- Note: ${p.note}`);
    lines.push(`- Before: ${String(p.before?.body || p.before?.title || "").slice(0, 220).replace(/\n/g, " ")}`);
    if (p.after && Object.keys(p.after).length) {
      const afterText = p.after.Body || p.after.Title || "";
      lines.push(`- After: ${String(afterText).slice(0, 220).replace(/\n/g, " ")}`);
    }
    lines.push(``);
  }
  if (report.preflightIssues?.length) {
    lines.push(`## Preflight issues`);
    lines.push(``);
    for (const i of report.preflightIssues) lines.push(`- ${i}`);
    lines.push(``);
  }
  lines.push(`## Post-apply gates`);
  lines.push(``);
  if (!gates || !Object.keys(gates).length) {
    lines.push(`_Pending — run after apply._`);
  } else {
    lines.push(`| Gate | Result |`);
    lines.push(`| --- | --- |`);
    lines.push(`| Active universe | ${gates.activeUniverse ?? "—"} |`);
    lines.push(`| test:brand-explorer | ${gates.testBrandExplorer ?? "—"} |`);
    lines.push(`| brand-explorer:pvql | ${gates.pvql ?? "—"} |`);
    lines.push(`| brand-explorer:semantic-audit | ${gates.semantic ?? "—"} |`);
    lines.push(`| dealality:batch-learning-audit | ${gates.batchLearning ?? "—"} |`);
  }
  lines.push(``);
  lines.push(`## Apply command`);
  lines.push(``);
  lines.push("```bash");
  lines.push(report.exactApplyCommand);
  lines.push("```");
  lines.push(``);
  lines.push(`## Change impact`);
  lines.push(``);
  lines.push(`- **Classification:** High (Presentation public-tab copy)`);
  lines.push(`- **Rollback:** restore Title/Body from \`before\` snapshots in report JSON.`);
  lines.push(`- **Modules/pages:** Brand Explorer public tabs listed above for Batch C brands only.`);
  lines.push(``);
  return lines.join("\n");
}

export function writeBatchCArtifacts(report) {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  fs.mkdirSync(DOCS_DIR, { recursive: true });
  const jsonPath = path.join(REPORTS_DIR, REPORT_JSON);
  const mdPath = path.join(REPORTS_DIR, REPORT_MD);
  const docsPath = path.join(DOCS_DIR, DOCS_MD);
  const md = renderBatchCMarkdown(report);
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");
  fs.writeFileSync(mdPath, md.endsWith("\n") ? md : `${md}\n`, "utf8");
  fs.writeFileSync(docsPath, md.endsWith("\n") ? md : `${md}\n`, "utf8");
  return { jsonPath, mdPath, docsPath };
}
