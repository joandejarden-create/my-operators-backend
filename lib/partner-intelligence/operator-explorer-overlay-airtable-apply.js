/**
 * Operator Explorer overlay → Airtable apply gate.
 *
 * v1: dry-run preview + fixture-overlay writes only.
 * Live Airtable Setup writes are intentionally blocked until a validated field map
 * and writer path are approved (High impact).
 *
 * Protected quality baselines (Arbor, HE) require --confirm-baseline-revision.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  getOperatorQualityBaselineEntry,
  isProtectedOperatorQualityBaseline,
} from "./operator-explorer-quality-baseline.js";
import { getOperatorFactoryQueueEntry } from "./operator-explorer-factory-queue.js";
import { OVERLAY_DIR } from "./operator-explorer-baseline-gap-remediation.js";

export const OPERATOR_OVERLAY_AIRTABLE_APPLY_VERSION = "operator-overlay-airtable-apply-v1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

/** Confirm flags required before any future live Airtable write path can run. */
export const AIRTABLE_APPLY_REQUIRED_FLAGS = Object.freeze([
  "approveOverlayAirtableApply",
  "confirmAirtableWrite",
  "confirmNoCompanyValidationChanges",
  "confirmNoSourceLibraryStatusChanges",
  "confirmNoRegistryApprovalChanges",
  "confirmFixtureOverlayReviewed",
]);

function resolveIdentity(id) {
  return (
    getOperatorQualityBaselineEntry(id) ||
    getOperatorFactoryQueueEntry(id) ||
    null
  );
}

function loadOverlay(slug) {
  const p = path.join(OVERLAY_DIR, `${slug}.json`);
  if (!fs.existsSync(p)) return null;
  try {
    return {
      path: path.relative(ROOT, p).replace(/\\/g, "/"),
      payload: JSON.parse(fs.readFileSync(p, "utf8")),
    };
  } catch (err) {
    return { path: p, error: err?.message || String(err) };
  }
}

/**
 * Build sanitized preview of overlay keys (no secrets).
 */
export function buildOverlayAirtablePreview(overlayPayload, identity) {
  const prefillOverlay =
    overlayPayload?.prefillOverlay && typeof overlayPayload.prefillOverlay === "object"
      ? overlayPayload.prefillOverlay
      : {};
  const suppress =
    overlayPayload?._meta?.intentionalSuppress &&
    typeof overlayPayload._meta.intentionalSuppress === "object"
      ? overlayPayload._meta.intentionalSuppress
      : {};

  const fieldMapping = Object.keys(prefillOverlay).map((prefillKey) => ({
    prefillKey,
    airtableField: "(TODO — map via operator Setup / explorer presentation writer)",
    valuePreview:
      typeof prefillOverlay[prefillKey] === "string"
        ? prefillOverlay[prefillKey].slice(0, 120)
        : Array.isArray(prefillOverlay[prefillKey])
          ? `[array:${prefillOverlay[prefillKey].length}]`
          : typeof prefillOverlay[prefillKey],
  }));

  return {
    operatorSlug: identity.slug,
    recordId: identity.recordId,
    companyName: identity.companyName,
    validation: {
      pass: fieldMapping.length > 0 || Object.keys(suppress).length > 0,
      checksFailed:
        fieldMapping.length === 0 && Object.keys(suppress).length === 0
          ? ["empty_overlay"]
          : [],
    },
    sanitizedPayloadPreview: {
      prefillKeyCount: fieldMapping.length,
      suppressKeyCount: Object.keys(suppress).length,
      keys: fieldMapping.map((f) => f.prefillKey),
    },
    exactFieldMapping: fieldMapping,
    intentionalSuppressKeys: Object.keys(suppress),
    errorHandling: {
      validationError: "Reject apply; fix overlay JSON",
      apiError: "N/A — Airtable write path not enabled in v1",
      networkError: "N/A — Airtable write path not enabled in v1",
      userFacing:
        "Overlay apply is fixture-only in v1. Live Setup writes remain blocked.",
    },
  };
}

/**
 * @param {{
 *   operators?: string[],
 *   apply?: boolean,
 *   approveOverlayAirtableApply?: boolean,
 *   confirmAirtableWrite?: boolean,
 *   confirmNoCompanyValidationChanges?: boolean,
 *   confirmNoSourceLibraryStatusChanges?: boolean,
 *   confirmNoRegistryApprovalChanges?: boolean,
 *   confirmFixtureOverlayReviewed?: boolean,
 *   confirmBaselineRevision?: boolean,
 *   confirmFixtureOverlayOnly?: boolean
 * }} [opts]
 */
export function runOperatorExplorerOverlayAirtableApply(opts = {}) {
  const apply = opts.apply === true;
  const fixtureOnly = opts.confirmFixtureOverlayOnly !== false;
  const wantAirtable = opts.confirmAirtableWrite === true;

  if (apply && !opts.approveOverlayAirtableApply) {
    throw new Error("Apply requires --approve-operator-overlay-airtable-apply");
  }

  if (apply && wantAirtable) {
    const missing = AIRTABLE_APPLY_REQUIRED_FLAGS.filter((k) => opts[k] !== true);
    if (missing.length) {
      throw new Error(
        `Live Airtable apply blocked — missing flags: ${missing.join(", ")}`
      );
    }
    // Explicit hard stop: no writer / schema map yet
    throw new Error(
      "Live Airtable overlay apply is not enabled in v1. Re-run with --confirm-fixture-overlay-only (default) after reviewing dry-run preview. Schema-mapped Setup writer required before any live write."
    );
  }

  const operators = opts.operators?.length
    ? opts.operators
    : ["arbor-lodging-cala", "hotel-equities-cala"];

  const results = [];
  for (const id of operators) {
    const identity = resolveIdentity(id);
    if (!identity) {
      results.push({ operatorSlug: id, error: "unknown_operator" });
      continue;
    }
    if (isProtectedOperatorQualityBaseline(identity.slug) && apply && !opts.confirmBaselineRevision) {
      results.push({
        operatorSlug: identity.slug,
        recordId: identity.recordId,
        blocked: true,
        reason:
          "Protected quality baseline — pass --confirm-baseline-revision for any apply touching goldens (still fixture-only in v1)",
      });
      continue;
    }

    const overlay = loadOverlay(identity.slug);
    if (!overlay) {
      results.push({
        operatorSlug: identity.slug,
        recordId: identity.recordId,
        overlayPath: null,
        validation: { pass: false, checksFailed: ["overlay_missing"] },
        note: `No overlay at fixtures/operator-explorer-baseline-overlays/${identity.slug}.json — run baseline-gap-remediation --apply first`,
      });
      continue;
    }
    if (overlay.error) {
      results.push({
        operatorSlug: identity.slug,
        error: overlay.error,
        overlayPath: overlay.path,
      });
      continue;
    }

    const preview = buildOverlayAirtablePreview(overlay.payload, identity);
    results.push({
      ...preview,
      overlayPath: overlay.path,
      airtableWritePerformed: false,
      writeKind: apply && fixtureOnly ? "fixture_overlay_reviewed_no_airtable" : "dry_run_preview",
    });
  }

  return {
    version: OPERATOR_OVERLAY_AIRTABLE_APPLY_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: !apply,
    applyPerformed: apply,
    airtableWrites: false,
    writeKind: apply ? "fixture_overlay_gate_only" : "none",
    operators,
    results,
    summary: {
      operators: results.length,
      overlaysFound: results.filter((r) => r.overlayPath).length,
      validationPass: results.filter((r) => r.validation?.pass).length,
      blocked: results.filter((r) => r.blocked).length,
    },
  };
}

export function writeOperatorExplorerOverlayAirtableApplyReports(
  report,
  reportsDir = path.join(ROOT, "reports")
) {
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, "operator-explorer-overlay-airtable-apply.json");
  const mdPath = path.join(reportsDir, "operator-explorer-overlay-airtable-apply.md");
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));
  const lines = [
    "# Operator Explorer overlay → Airtable apply",
    "",
    `Version: \`${report.version}\` · dryRun: **${report.dryRun}** · airtableWrites: **${report.airtableWrites}**`,
    `Generated: ${report.generatedAt}`,
    "",
    "> Live Airtable writes are **blocked in v1**. This gate previews overlay → field mapping only.",
    "",
  ];
  for (const r of report.results) {
    lines.push(`## ${r.companyName || r.operatorSlug}`, "");
    if (r.error) lines.push(`- error: ${r.error}`, "");
    if (r.blocked) lines.push(`- blocked: ${r.reason}`, "");
    if (r.overlayPath) lines.push(`- overlay: \`${r.overlayPath}\``);
    if (r.validation) {
      lines.push(
        `- validation: pass=${r.validation.pass} failed=${(r.validation.checksFailed || []).join(",") || "—"}`
      );
    }
    if (r.sanitizedPayloadPreview) {
      lines.push(
        `- prefill keys: ${r.sanitizedPayloadPreview.prefillKeyCount}`,
        `- suppress keys: ${r.sanitizedPayloadPreview.suppressKeyCount}`
      );
    }
    lines.push("");
  }
  fs.writeFileSync(mdPath, lines.join("\n"));
  return { jsonPath, mdPath };
}
