/**
 * Global Active — Medium semantic review + targeted cleanup (final pre-54 freeze check).
 *
 * Reviews remaining Medium findings from the refreshed global semantic audit.
 * Patches only patch_now / escalate_to_high items with targeted Presentation
 * Title/Body edits. Does not freeze 54.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { listPresentationRowsLight } from "./brand-explorer-lane2-common.js";
import { isOwnerFacingPresentationRow } from "./brand-explorer-public-visibility-quality-lock.js";
import { getWave13ActiveIdentityBySlug } from "./brand-explorer-wave13-active-identity-anchors.js";
import { toProperCaseScenarioTitle } from "./brand-explorer-scenario-owner-value-bar.js";
import {
  EXPECTED_ACTIVE_UNIVERSE_COUNT,
  EXCLUDED_FROM_ACTIVE_SEMANTIC_AUDIT,
} from "./brand-explorer-global-active-semantic-audit.js";

export const GLOBAL_MEDIUM_SEVERITY_CLEANUP_VERSION =
  "global-medium-severity-cleanup-v1";

export const GLOBAL_MEDIUM_SEVERITY_CLEANUP_APPLY_FLAGS = Object.freeze([
  "--approve-global-medium-severity-cleanup",
  "--confirm-fresh-global-audit-used",
  "--confirm-medium-findings-only",
  "--confirm-targeted-visible-copy-only",
  "--confirm-no-critical-regression",
  "--confirm-no-high-regression",
  "--confirm-no-brand-status-changes",
  "--confirm-no-release-field-writes",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-image-writes",
  "--confirm-no-four-points-flex-writes",
  "--confirm-no-house-of-originals-writes",
  "--confirm-no-morgans-originals-writes",
  "--confirm-no-radisson-collection-changes",
  "--confirm-no-broad-rewrites",
  "--confirm-no-gate-weakening",
]);

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const WRITE_THROTTLE_MS = 320;
const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const REPORTS_DIR = path.join(ROOT, "reports");
const DOCS_DIR = path.join(ROOT, "docs", "data-intelligence");

const NEVER_WRITE_BRANDS = new Set(
  EXCLUDED_FROM_ACTIVE_SEMANTIC_AUDIT.map((s) => s.toLowerCase())
);

/**
 * Explicit steward decisions for the 12 Medium findings (fresh audit 2026-07-31).
 * Keys: `${slug}::${failureType}` (slot appended when needed for uniqueness).
 */
export const MEDIUM_REVIEW_DECISIONS = Object.freeze([
  {
    brand: "Aloft Hotels",
    brandSlug: "aloft-hotels",
    brandRecordId: "recJ1GZQpttX7qHgw",
    presentationRecordId: "recJA2RNLEZG9SqM5",
    section: "Value Creation Scenarios",
    field: "Title/Body",
    slotKey: "valueOwners.scenario.3",
    failureType: "thin_body_valueOwners.scenario.3:23",
    mediumIssue: "Value Creation card body is 23 words (bar requires ≥26).",
    ownerFacingRisk: "Low — card is owner-useful but slightly thin versus Ascend bar.",
    recommendedAction: "patch_now",
    patch: {
      Body:
        "Secondary lifestyle markets suit Aloft when competitors lack modern select-service energy. Compare AC Hotels when European contemporary select-service is the stronger product and capital story for the asset.",
    },
  },
  {
    brand: "Residence Inn by Marriott",
    brandSlug: "residence-inn-by-marriott",
    brandRecordId: "rec9Ufbpa0GxJGzt8",
    presentationRecordId: "recs8UbXAiVLc75dX",
    section: "Where This Brand Creates the Most Value",
    field: "Title/Body",
    slotKey: "overview.scenario.2",
    failureType: "weak_owner_value_cues_overview.scenario.2",
    mediumIssue: "Overview scenario has only one owner-value cue token (needs ≥2).",
    ownerFacingRisk: "Low — substance is owner-facing; cue lexicon undersampled.",
    recommendedAction: "patch_now",
    patch: {
      Body:
        "Suite-and-kitchen product creates Residence Inn owner value when length-of-stay economics—not short-stay select-service rate math—drive the underwriting. Owners should capitalize residential suite mix, social space, and housekeeping rhythms for multi-night guests. Affiliation helps when the asset can deliver a residential stay guests will book for weeks, not a transient all-suite night.",
    },
  },
  {
    brand: "Residence Inn by Marriott",
    brandSlug: "residence-inn-by-marriott",
    brandRecordId: "rec9Ufbpa0GxJGzt8",
    presentationRecordId: "recziWUpqEixBJfqq",
    section: "Where This Brand Creates the Most Value",
    field: "Title/Body",
    slotKey: "overview.scenario.3",
    failureType: "weak_owner_value_cues_overview.scenario.3",
    mediumIssue: "Overview scenario has only one owner-value cue token (needs ≥2).",
    ownerFacingRisk: "Low — substance is owner-facing; cue lexicon undersampled.",
    recommendedAction: "patch_now",
    patch: {
      Body:
        "Suburban and urban extended-stay assets fit Residence Inn when Marriott distribution can support recurring corporate, medical, and relocation demand at upscale suite quality. Underwrite staffing and product depth for residential stays rather than SpringHill short-stay suite logic. Owner value is weaker when the thesis drifts into TownePlace midscale kitchens or StudioRes prototype simplicity.",
    },
  },
  {
    brand: "Residence Inn by Marriott",
    brandSlug: "residence-inn-by-marriott",
    brandRecordId: "rec9Ufbpa0GxJGzt8",
    presentationRecordId: "recpeCOjslsuYAu4g",
    section: "Value Creation Scenarios",
    field: "Title/Body",
    slotKey: "valueOwners.scenario.2",
    failureType: "thin_body_valueOwners.scenario.2:25",
    mediumIssue: "Value Creation card body is 25 words (bar requires ≥26).",
    ownerFacingRisk: "Low — one-word short of Ascend short-paragraph bar.",
    recommendedAction: "patch_now",
    patch: {
      Body:
        "Suburban employment nodes and urban extended-stay corridors create Residence Inn value when sponsors underwrite weekly housekeeping models, parking, and suite mix against real length-of-stay demand patterns.",
    },
  },
  {
    brand: "Residence Inn by Marriott",
    brandSlug: "residence-inn-by-marriott",
    brandRecordId: "rec9Ufbpa0GxJGzt8",
    presentationRecordId: "recduQsyRRLkGhiDW",
    section: "Value Creation Scenarios",
    field: "Title/Body",
    slotKey: "valueOwners.scenario.4",
    failureType: "sentence_case_title_valueOwners.scenario.4",
    mediumIssue: 'Title uses "For" mid-title; Proper Case bar expects "for".',
    ownerFacingRisk: "Cosmetic title casing only.",
    recommendedAction: "patch_now",
    patch: {
      Title: toProperCaseScenarioTitle("Network Reach For Longer Stays"),
    },
  },
  {
    brand: "SO/",
    brandSlug: "so-hotels-and-resorts",
    brandRecordId: "recTJdPlr4mDs9app",
    presentationRecordId: "recHvd9yiePKd36A2",
    section: "Geographic Footprint",
    field: "Title/Body",
    slotKey: "footprint.region.cala",
    failureType: "cala_label_without_support",
    mediumIssue:
      "Honest CALA-unavailable copy still matches audit dual pattern (no verified CALA + operating examples).",
    ownerFacingRisk:
      "Low — message is protective; wording trips Medium gate without implying false presence.",
    recommendedAction: "patch_now",
    patch: {
      Title: "Caribbean & Latin America",
      Body:
        "CALA inventory for SO/ Hotels & Resorts is not yet confirmed on official SO/ or Accor Group brand pages. Keep CALA cleanly unavailable and use International Reference hotels for brand-fit diligence until named CALA properties are published.",
    },
  },
  {
    brand: "SpringHill Suites by Marriott",
    brandSlug: "springhill-suites-by-marriott",
    brandRecordId: "recBzdGfkMUN9fYsv",
    presentationRecordId: "recEaUxBg3Dgxdjyg",
    section: "Geographic Footprint",
    field: "Title/Body",
    slotKey: "footprint.region.cala",
    failureType: "cala_label_without_support",
    mediumIssue:
      "Honest CALA-unavailable copy still matches audit dual pattern (do not imply + operating examples).",
    ownerFacingRisk: "Low — protective copy; Medium is wording-pattern noise.",
    recommendedAction: "patch_now",
    patch: {
      Body:
        "CALA inventory for SpringHill Suites is not yet confirmed in official brand materials. Do not imply regional presence—add a CALA card only after a property-name-matched official URL is verified.",
    },
  },
  {
    brand: "SpringHill Suites by Marriott",
    brandSlug: "springhill-suites-by-marriott",
    brandRecordId: "recBzdGfkMUN9fYsv",
    presentationRecordId: "reckv8EBBxmEJ3DIH",
    section: "Value Creation Scenarios",
    field: "Title/Body",
    slotKey: "valueOwners.scenario.1",
    failureType: "thin_body_valueOwners.scenario.1:25",
    mediumIssue: "Value Creation card body is 25 words (bar requires ≥26).",
    ownerFacingRisk: "Low — one-word short of Ascend bar.",
    recommendedAction: "patch_now",
    patch: {
      Body:
        "SpringHill Suites fits owners building upper-midscale suite product for bleisure and leisure demand when efficient select-service operations can sustain suite sizing without extended-stay kitchen economics or complexity.",
    },
  },
  {
    brand: "SpringHill Suites by Marriott",
    brandSlug: "springhill-suites-by-marriott",
    brandRecordId: "recBzdGfkMUN9fYsv",
    presentationRecordId: "recGQjGaxbrpKpqMH",
    section: "Value Creation Scenarios",
    field: "Title/Body",
    slotKey: "valueOwners.scenario.3",
    failureType: "thin_body_valueOwners.scenario.3:24",
    mediumIssue: "Value Creation card body is 24 words (bar requires ≥26).",
    ownerFacingRisk: "Low — slightly thin versus Ascend bar.",
    recommendedAction: "patch_now",
    patch: {
      Body:
        "Markets crowded with standard kings need SpringHill when suites differentiate rate without Courtyard meeting intensity or Fairfield limited-service constraints—owners should price suite PIP and operating scope honestly.",
    },
  },
  {
    brand: "StudioRes",
    brandSlug: "studiores",
    brandRecordId: "recDM0LAD8jVRA2x3",
    presentationRecordId: "recSjZmroXnQZaWqj",
    section: "Value Creation Scenarios",
    field: "Title/Body",
    slotKey: "valueOwners.scenario.1",
    failureType: "thin_body_valueOwners.scenario.1:25",
    mediumIssue: "Value Creation card body is 25 words (bar requires ≥26).",
    ownerFacingRisk: "Low — one-word short of Ascend bar.",
    recommendedAction: "patch_now",
    patch: {
      Body:
        "StudioRes fits owners building efficient midscale longer-stay studios when prototype economics and simplified operations match demand—without Residence Inn upscale residential intensity or Apartments soft-brand operating complexity.",
    },
  },
  {
    brand: "StudioRes",
    brandSlug: "studiores",
    brandRecordId: "recDM0LAD8jVRA2x3",
    presentationRecordId: "rec1iXPogdtPxblUz",
    section: "Value Creation Scenarios",
    field: "Title/Body",
    slotKey: "valueOwners.scenario.4",
    failureType: "sentence_case_title_valueOwners.scenario.4",
    mediumIssue: 'Title uses "For" mid-title; Proper Case bar expects "for".',
    ownerFacingRisk: "Cosmetic title casing only.",
    recommendedAction: "patch_now",
    patch: {
      Title: toProperCaseScenarioTitle("Bonvoy Access For Affordable Longer Stays"),
    },
  },
  {
    brand: "TownePlace Suites by Marriott",
    brandSlug: "towneplace-suites-by-marriott",
    brandRecordId: "recUPiPDivkhNUogr",
    presentationRecordId: "recSYO3T3GDmpDnOV",
    section: "Geographic Footprint",
    field: "Title/Body",
    slotKey: "footprint.region.cala",
    failureType: "cala_label_without_support",
    mediumIssue:
      "Honest CALA-unavailable copy still matches audit dual pattern (do not imply + operating examples).",
    ownerFacingRisk: "Low — protective copy; Medium is wording-pattern noise.",
    recommendedAction: "patch_now",
    patch: {
      Body:
        "CALA inventory for TownePlace Suites is not yet confirmed in official brand materials. Do not imply regional presence—add a CALA card only after a property-name-matched official URL is verified.",
    },
  },
]);

function nz(v) {
  return v == null ? "" : String(v).trim();
}
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
function checkFlags(required, argv, apply) {
  const missing = required.filter((f) => !argv.includes(f));
  return { apply: apply === true, ok: apply === true && missing.length === 0, missing, required: [...required] };
}

async function airtablePatch(baseId, apiKey, table, recordId, fields) {
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/${recordId}`;
  const res = await fetch(url, {
    method: "PATCH",
    headers: { Authorization: `Bearer ${apiKey}`, "Content-Type": "application/json" },
    body: JSON.stringify({ fields }),
  });
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`PATCH ${recordId} → ${res.status}: ${text.slice(0, 300)}`);
  }
  return res.json();
}

async function fetchPresentationRowById(recordId) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey || !recordId) return null;
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}/${recordId}`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
  if (!res.ok) return null;
  const rec = await res.json();
  const f = rec.fields || {};
  return {
    recordId: rec.id,
    slotKey: nz(f["Slot Key"]),
    title: nz(f.Title),
    body: nz(f.Body),
    brandName: nz(f["Brand Name"]),
    active: f.Active !== false,
    externalDisplayStatus: nz(f["External Display Status"]),
    sortOrder: f["Sort Order"] ?? 0,
  };
}

function brandNameCandidates(brandSlug, brandName) {
  const names = [];
  const push = (v) => {
    const s = nz(v);
    if (s && !names.includes(s)) names.push(s);
  };
  push(brandName);
  if (brandSlug === "so-hotels-and-resorts") {
    push("SO/ Hotels & Resorts");
    push("SO/");
  }
  const anchor = getWave13ActiveIdentityBySlug(brandSlug);
  if (anchor) {
    push(anchor.name);
    for (const a of anchor.nameAliases || []) push(a);
  }
  return names;
}

async function listOwnerRowsForCleanup(brandSlug, brandName, brandRecordId) {
  const seen = new Map();
  for (const name of brandNameCandidates(brandSlug, brandName)) {
    const live = await listPresentationRowsLight(brandRecordId, name);
    for (const r of live.rows || []) {
      if (r?.recordId && !seen.has(r.recordId)) seen.set(r.recordId, r);
    }
    if (seen.size > 0) break;
  }
  return [...seen.values()].filter(isOwnerFacingPresentationRow);
}

/**
 * Build review rows from decisions + live audit Medium findings.
 */
export function buildMediumSeverityReview(auditReport) {
  const auditMediums = [];
  for (const b of auditReport.brandResults || []) {
    for (const f of b.findings || []) {
      if (String(f.severity).toLowerCase() !== "medium") continue;
      auditMediums.push({
        brand: b.brandName,
        brandSlug: b.brandSlug,
        brandRecordId: b.recordId,
        presentationRecordId: f.recordId || null,
        section: f.section,
        failureType: f.failureType,
        slotKey: f.slotKey || null,
        currentValue: nz(f.currentValue),
      });
    }
  }

  const reviews = MEDIUM_REVIEW_DECISIONS.map((d) => {
    const live =
      auditMediums.find(
        (m) =>
          m.brandSlug === d.brandSlug &&
          (m.failureType === d.failureType ||
            (d.slotKey && m.failureType.includes(d.slotKey.replace(/\./g, "."))))
      ) ||
      auditMediums.find((m) => m.brandSlug === d.brandSlug && m.failureType === d.failureType);

    return {
      brand: d.brand,
      brandSlug: d.brandSlug,
      recordId: d.presentationRecordId || live?.presentationRecordId || d.brandRecordId,
      brandRecordId: d.brandRecordId,
      section: d.section,
      field: d.field,
      slotKey: d.slotKey,
      currentVisibleCopy: live?.currentValue || "",
      mediumIssue: d.mediumIssue,
      ownerFacingRisk: d.ownerFacingRisk,
      recommendedAction: d.recommendedAction,
      failureType: d.failureType,
      patch: d.patch || null,
    };
  });

  const actionCounts = {
    patch_now: reviews.filter((r) => r.recommendedAction === "patch_now").length,
    accept_as_minor_non_blocking: reviews.filter((r) => r.recommendedAction === "accept_as_minor_non_blocking")
      .length,
    defer_to_future_visual_polish: reviews.filter(
      (r) => r.recommendedAction === "defer_to_future_visual_polish"
    ).length,
    escalate_to_high: reviews.filter((r) => r.recommendedAction === "escalate_to_high").length,
  };

  return {
    version: GLOBAL_MEDIUM_SEVERITY_CLEANUP_VERSION,
    generatedAt: new Date().toISOString(),
    auditGeneratedAt: auditReport.generatedAt || null,
    universe: {
      activeCount: auditReport.activeCount,
      expected: EXPECTED_ACTIVE_UNIVERSE_COUNT,
      reconciled: auditReport.universeReconciled,
      critical: auditReport.severityTotals?.critical ?? null,
      high: auditReport.severityTotals?.high ?? null,
      medium: auditReport.severityTotals?.medium ?? auditMediums.length,
    },
    mediumFindingCountFromAudit: auditMediums.length,
    reviewedCount: reviews.length,
    actionCounts,
    reviews,
    unmatchedAuditFindings: auditMediums.filter(
      (m) => !reviews.some((r) => r.brandSlug === m.brandSlug && r.failureType === m.failureType)
    ),
  };
}

export function writeMediumSeverityReviewReports(review) {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  const jsonPath = path.join(REPORTS_DIR, "brand-explorer-global-medium-severity-review.json");
  const mdPath = path.join(REPORTS_DIR, "brand-explorer-global-medium-severity-review.md");
  fs.writeFileSync(jsonPath, `${JSON.stringify(review, null, 2)}\n`, "utf8");

  const lines = [
    "# Global Active — Medium Severity Semantic Review",
    "",
    `Generated: ${review.generatedAt}`,
    `Audit generated: ${review.auditGeneratedAt}`,
    `Medium findings in audit: **${review.mediumFindingCountFromAudit}**`,
    `Reviewed: **${review.reviewedCount}**`,
    "",
    "## Action counts",
    "",
    ...Object.entries(review.actionCounts).map(([k, v]) => `- **${k}**: ${v}`),
    "",
    "| Brand | Slug | Record ID | Section | Field | Current Visible Copy | Medium Issue | Owner-Facing Risk | Recommended Action |",
    "|-------|------|-----------|---------|-------|----------------------|--------------|-------------------|--------------------|",
  ];
  for (const r of review.reviews) {
    lines.push(
      `| ${r.brand} | \`${r.brandSlug}\` | ${r.recordId || "—"} | ${String(r.section || "").replace(/\|/g, "/")} | ${r.field} | ${String(r.currentVisibleCopy || "").replace(/\|/g, "/").replace(/\n/g, " ").slice(0, 80)} | ${String(r.mediumIssue || "").replace(/\|/g, "/")} | ${String(r.ownerFacingRisk || "").replace(/\|/g, "/")} | **${r.recommendedAction}** |`
    );
  }
  lines.push("");
  if (review.unmatchedAuditFindings?.length) {
    lines.push("## Unmatched audit findings");
    lines.push("");
    for (const u of review.unmatchedAuditFindings) {
      lines.push(`- \`${u.brandSlug}\` · ${u.failureType}`);
    }
    lines.push("");
  }
  fs.writeFileSync(mdPath, lines.join("\n"), "utf8");
  return { jsonPath, mdPath };
}

async function planPatchesFromReview(reviews) {
  const patchable = reviews.filter(
    (r) =>
      (r.recommendedAction === "patch_now" || r.recommendedAction === "escalate_to_high") &&
      r.patch &&
      r.recordId &&
      !NEVER_WRITE_BRANDS.has(r.brandSlug)
  );

  const patches = [];
  for (const r of patchable) {
    const row = await fetchPresentationRowById(r.recordId);
    if (!row) {
      patches.push({
        brandSlug: r.brandSlug,
        recordId: r.recordId,
        slotKey: r.slotKey,
        skipped: "presentation_row_not_found",
      });
      continue;
    }
    const fields = {};
    if (r.patch.Title != null && nz(r.patch.Title) !== nz(row.title)) fields.Title = r.patch.Title;
    if (r.patch.Body != null && nz(r.patch.Body) !== nz(row.body)) fields.Body = r.patch.Body;
    if (!Object.keys(fields).length) {
      patches.push({
        brandSlug: r.brandSlug,
        recordId: r.recordId,
        slotKey: r.slotKey,
        skipped: "already_matches_target",
      });
      continue;
    }
    patches.push({
      action: "PATCH",
      table: PRESENTATION_TABLE,
      brandSlug: r.brandSlug,
      recordId: r.recordId,
      slotKey: row.slotKey || r.slotKey,
      failureType: r.failureType,
      recommendedAction: r.recommendedAction,
      fields,
      before: { title: nz(row.title).slice(0, 120), body: nz(row.body).slice(0, 180) },
      after: {
        title: (fields.Title != null ? fields.Title : nz(row.title)).slice(0, 120),
        body: (fields.Body != null ? fields.Body : nz(row.body)).slice(0, 180),
      },
    });
    await sleep(200);
  }
  return patches;
}

export async function runGlobalMediumSeverityCleanup({
  dryRun = true,
  argv = [],
  auditReport = null,
} = {}) {
  const apply = argv.includes("--apply") && dryRun === false;
  const flagCheck = checkFlags(GLOBAL_MEDIUM_SEVERITY_CLEANUP_APPLY_FLAGS, argv, apply);

  if (!auditReport) {
    return {
      version: GLOBAL_MEDIUM_SEVERITY_CLEANUP_VERSION,
      generatedAt: new Date().toISOString(),
      applyPerformed: false,
      pass: false,
      readyStatement: "global_medium_review_blocked_missing_fresh_audit",
    };
  }

  if ((auditReport.severityTotals?.critical ?? 0) > 0 || (auditReport.severityTotals?.high ?? 0) > 0) {
    return {
      version: GLOBAL_MEDIUM_SEVERITY_CLEANUP_VERSION,
      generatedAt: new Date().toISOString(),
      applyPerformed: false,
      pass: false,
      readyStatement: "global_medium_review_found_high_regression_do_not_freeze",
      severityTotals: auditReport.severityTotals,
    };
  }

  const review = buildMediumSeverityReview(auditReport);
  writeMediumSeverityReviewReports(review);

  if (review.unmatchedAuditFindings?.length) {
    return {
      version: GLOBAL_MEDIUM_SEVERITY_CLEANUP_VERSION,
      generatedAt: new Date().toISOString(),
      applyPerformed: false,
      pass: false,
      readyStatement: "global_medium_review_incomplete_unmatched_findings",
      review,
    };
  }

  const needsWrite = review.actionCounts.patch_now + review.actionCounts.escalate_to_high > 0;

  if (apply && needsWrite && !flagCheck.ok) {
    return {
      version: GLOBAL_MEDIUM_SEVERITY_CLEANUP_VERSION,
      generatedAt: new Date().toISOString(),
      applyPerformed: false,
      pass: false,
      readyStatement: "global_medium_severity_cleanup_blocked_missing_flags",
      missingFlags: flagCheck.missing,
      review,
    };
  }

  const planned = needsWrite ? await planPatchesFromReview(review.reviews) : [];
  const patches = planned.filter((p) => p.action === "PATCH");

  let applyResult = { applied: 0, errors: [] };
  if (apply && patches.length) {
    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    if (!baseId || !apiKey) throw new Error("Missing AIRTABLE_BASE_ID / AIRTABLE_API_KEY");
    for (const p of patches) {
      try {
        await airtablePatch(baseId, apiKey, p.table, p.recordId, p.fields);
        applyResult.applied += 1;
      } catch (err) {
        applyResult.errors.push({
          brandSlug: p.brandSlug,
          recordId: p.recordId,
          slotKey: p.slotKey,
          error: err?.message || String(err),
        });
      }
      await sleep(WRITE_THROTTLE_MS);
    }
  }

  const deferred =
    review.actionCounts.accept_as_minor_non_blocking +
    review.actionCounts.defer_to_future_visual_polish;
  const escalated = review.actionCounts.escalate_to_high;

  let readyStatement;
  if (escalated > 0) {
    readyStatement = "global_medium_review_found_high_regression_do_not_freeze";
  } else if (deferred > 0 && review.actionCounts.patch_now === 0) {
    readyStatement = "global_medium_review_complete_non_blocking_items_deferred_ready_for_54_freeze";
  } else if (deferred > 0) {
    readyStatement = "global_medium_review_complete_non_blocking_items_deferred_ready_for_54_freeze";
  } else {
    readyStatement = apply
      ? applyResult.errors.length === 0
        ? "global_medium_semantic_cleanup_complete_ready_for_54_freeze"
        : "global_medium_severity_cleanup_applied_with_errors"
      : "global_medium_severity_cleanup_dry_run_ready";
  }

  const report = {
    version: GLOBAL_MEDIUM_SEVERITY_CLEANUP_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: !apply,
    applyPerformed: apply === true,
    writePerformed: apply === true && applyResult.applied > 0,
    auditGeneratedAt: auditReport.generatedAt,
    mediumFindingsBefore: review.mediumFindingCountFromAudit,
    actionCounts: review.actionCounts,
    patchCount: patches.length,
    applyResult,
    brandsPatched: [...new Set(patches.map((p) => p.brandSlug))],
    patches,
    skipped: planned.filter((p) => p.skipped),
    forbiddenWritesAvoided: [
      "Brand Status",
      "release fields",
      "Active Profile Approved / Ready for Active Profile / Founder Visual Review Pass",
      "Company Validated / Company Validation Date",
      "Source Library status",
      "Registry approval/status",
      "images",
      "four-points-flex-by-sheraton",
      "the-house-of-originals",
      "morgans-originals",
      "radisson-collection",
      "broad profile rewrites",
      "baseline freeze artifacts",
    ],
    readyStatement,
    freezeNote:
      "This task does not freeze 54. After post-apply validation, founder may decide on protected 54 freeze in a separate task.",
    reviewSummary: {
      reviewedCount: review.reviewedCount,
      actionCounts: review.actionCounts,
    },
  };

  writeGlobalMediumSeverityCleanupReports(report);
  return report;
}

export function writeGlobalMediumSeverityCleanupReports(report) {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  fs.mkdirSync(DOCS_DIR, { recursive: true });

  const jsonPath = path.join(REPORTS_DIR, "brand-explorer-global-medium-severity-cleanup.json");
  const mdPath = path.join(REPORTS_DIR, "brand-explorer-global-medium-severity-cleanup.md");
  const docsPath = path.join(DOCS_DIR, "brand-explorer-global-medium-severity-cleanup.md");

  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");

  const lines = [
    "# Global Active — Medium Severity Semantic Cleanup",
    "",
    `Version: \`${report.version}\``,
    `Generated: ${report.generatedAt}`,
    `Mode: **${report.applyPerformed ? "APPLY" : "dry-run"}**`,
    `Audit used: ${report.auditGeneratedAt || "—"}`,
    "",
    `Ready: \`${report.readyStatement}\``,
    "",
    "## Counts",
    "",
    `| Item | Count |`,
    `|------|------:|`,
    `| Medium findings before | ${report.mediumFindingsBefore ?? 0} |`,
    `| patch_now | ${report.actionCounts?.patch_now ?? 0} |`,
    `| accept_as_minor_non_blocking | ${report.actionCounts?.accept_as_minor_non_blocking ?? 0} |`,
    `| defer_to_future_visual_polish | ${report.actionCounts?.defer_to_future_visual_polish ?? 0} |`,
    `| escalate_to_high | ${report.actionCounts?.escalate_to_high ?? 0} |`,
    `| Patches planned | ${report.patchCount ?? 0} |`,
    `| Patches applied | ${report.applyResult?.applied ?? 0} |`,
    `| Apply errors | ${report.applyResult?.errors?.length ?? 0} |`,
    "",
    "## Brands patched",
    "",
    ...((report.brandsPatched || []).length
      ? report.brandsPatched.map((s) => `- \`${s}\``)
      : ["_None_"]),
    "",
    "## Patches",
    "",
  ];
  for (const p of report.patches || []) {
    lines.push(
      `- \`${p.brandSlug}\` · \`${p.slotKey}\` · ${p.recordId}: ${(p.before?.title || "").slice(0, 40)} → ${(p.after?.title || "").slice(0, 40)}`
    );
  }
  lines.push("");
  lines.push("## Forbidden writes avoided");
  lines.push("");
  for (const f of report.forbiddenWritesAvoided || []) lines.push(`- ${f}`);
  lines.push("");
  lines.push(report.freezeNote || "");
  lines.push("");

  const md = lines.join("\n");
  fs.writeFileSync(mdPath, md, "utf8");
  fs.writeFileSync(docsPath, md, "utf8");
  return { jsonPath, mdPath, docsPath };
}
