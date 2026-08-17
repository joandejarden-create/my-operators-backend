/**
 * Brand Explorer Active Profile Copy Governance Builder v34C.
 *
 * Generic safety rules + brand-specific rewrites. Never emits interchangeable boilerplate.
 */
import { scanCopySafety } from "./brand-explorer-choice-expansion-partial-profile-backfill-writer.js";
import { COPY_SAFETY_PATTERNS } from "./brand-explorer-active-profile-factory-rules.js";
import { getActiveProfileBrandConfig } from "./brand-explorer-active-profile-brand-config.js";
import {
  COPY_GOVERNANCE_VERSION,
  COPY_SANITIZE_REPLACEMENTS,
  GENERIC_BOILERPLATE_PATTERNS,
  getCopyGovernanceConfig,
} from "./brand-explorer-active-profile-copy-governance-config.js";
import {
  buildFounderQueueAuditMarkdown,
  mergeQueueResolutionIntoPlan,
  resolveFounderReviewQueue,
} from "./brand-explorer-active-profile-copy-governance-queue-resolver.js";

export { COPY_GOVERNANCE_VERSION };

const HIDDEN_DISPLAY = /^(do not display|internal only)$/i;

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function isVisibleRow(row) {
  const status = nz(row?.externalDisplayStatus);
  return row?.visible !== false && !HIDDEN_DISPLAY.test(status);
}

/** Extended v34C audit patterns (factory + governance-specific). */
export const COPY_GOVERNANCE_AUDIT_PATTERNS = Object.freeze([
  ...COPY_SAFETY_PATTERNS,
  { id: "revpar", re: /\brevpar\b/i, severity: "medium" },
  { id: "fee_stack", re: /\bfee stack\b/i, severity: "medium" },
  { id: "estimated_contribution", re: /\bestimated contribution\b/i, severity: "high" },
  { id: "booking_path", re: /\bbooking path\b/i, severity: "medium" },
  { id: "franchise_disclosure_long", re: /\bfranchise disclosure document\b/i, severity: "high" },
]);

export function auditPresentationCopy(text) {
  const combined = nz(text);
  const findings = [];
  if (!combined) return findings;

  for (const id of scanCopySafety(combined)) {
    findings.push({ patternId: id, severity: id === "fdd" || id === "item_19" ? "high" : "medium" });
  }
  for (const pat of COPY_GOVERNANCE_AUDIT_PATTERNS) {
    if (pat.re.test(combined)) {
      findings.push({ patternId: pat.id, severity: pat.severity || "medium" });
    }
  }
  return findings;
}

export function sanitizeUnsafeCopy(text) {
  let out = nz(text);
  for (const { re, replace } of COPY_SANITIZE_REPLACEMENTS) {
    out = out.replace(re, replace);
  }
  return out.replace(/\s{2,}/g, " ").trim();
}

export function isGenericBoilerplate(text, { brandName = "", parentPlatform = "" } = {}) {
  const t = nz(text);
  if (!t) return false;
  for (const re of GENERIC_BOILERPLATE_PATTERNS) {
    if (re.test(t)) return true;
  }
  const brandLc = nz(brandName).toLowerCase();
  const parentLc = nz(parentPlatform).toLowerCase();
  const hasBrandAnchor =
    (brandLc && t.toLowerCase().includes(brandLc.split(" ")[0])) ||
    (parentLc && t.toLowerCase().includes("choice"));
  if (t.length > 80 && !hasBrandAnchor && /owners should/i.test(t)) {
    return true;
  }
  return false;
}

function resolveSourceRefs(sourceRefs = [], brandConfig, governanceConfig, approvedSources = []) {
  const resolved = [];
  for (const ref of sourceRefs) {
    if (ref === "consumerUrl" && brandConfig?.consumerUrl) {
      resolved.push({ type: "official_consumer", url: brandConfig.consumerUrl });
    } else if (ref === "developmentUrl" && governanceConfig?.developmentUrl) {
      resolved.push({ type: "official_development", url: governanceConfig.developmentUrl });
    } else if (ref.startsWith("source:")) {
      const id = ref.replace("source:", "");
      const src = approvedSources.find((s) => s.id === id || s.sourceId === id);
      if (src?.sourceUrl) {
        resolved.push({ type: "source_library", url: src.sourceUrl, recordId: id });
      }
    } else if (ref.startsWith("http")) {
      resolved.push({ type: "url", url: ref });
    }
  }
  for (const url of brandConfig?.momentumSourceUrls || []) {
    if (!resolved.some((r) => r.url === url)) {
      resolved.push({ type: "momentum_url", url });
    }
  }
  return resolved;
}

function brandSpecificityScore(text, { brandName, segment, positioningPillars = [] } = {}) {
  const t = nz(text).toLowerCase();
  let score = 0;
  const brandToken = nz(brandName).toLowerCase().split(" ")[0];
  if (brandToken && t.includes(brandToken)) score += 2;
  if (nz(segment) && t.includes(nz(segment).toLowerCase().split(" ")[0])) score += 1;
  for (const pillar of positioningPillars) {
    const words = nz(pillar).toLowerCase().split(/\s+/).slice(0, 2).join(" ");
    if (words && t.includes(words)) score += 1;
  }
  if (t.includes("choice")) score += 1;
  return score;
}

function proposeRegexRepair(row, targets = []) {
  for (const target of targets) {
    if (row.slotKey !== target.slotKey) continue;
    if (target.titleMatch && !target.titleMatch.test(row.title)) continue;
    if (!target.match?.test?.(row.body)) continue;
    const proposedBody = row.body.replace(target.match, target.replace);
    if (proposedBody === row.body) continue;
    return {
      proposedTitle: row.title,
      proposedBody,
      rewriteStrategy: "regex_repair",
      reason: target.reason,
    };
  }
  return null;
}

function proposeSlotRewrite(row, governanceConfig, brandConfig, approvedSources) {
  const pkg = governanceConfig.slotRewrites?.[row.slotKey];
  if (!pkg?.body) return null;

  const proposedTitle = pkg.title || row.title;
  const proposedBody = pkg.body;
  const sources = resolveSourceRefs(pkg.sourceRefs, brandConfig, governanceConfig, approvedSources);

  return {
    proposedTitle,
    proposedBody,
    rewriteStrategy: "brand_slot_rewrite",
    reason: "brand_specific_slot_rewrite",
    sourceSupport: sources,
    positioningPillars: governanceConfig.positioningPillars,
  };
}

function buildRewriteRationale({ brandName, rewrite, governanceConfig }) {
  return {
    brandSpecific: `Copy names ${brandName} and reflects ${governanceConfig.segment} positioning.`,
    sourceSupported: (rewrite.sourceSupport || [])
      .map((s) => s.url)
      .filter(Boolean)
      .slice(0, 3),
    ownerFacing: "Written for owner diligence — no FDD or performance representation language.",
    nonPerformance: "Avoids ADR, RevPAR, net contribution, item 19, and franchise disclosure references.",
    safeForExplorer: rewrite.proposedBody
      ? auditPresentationCopy(`${rewrite.proposedTitle}\n${rewrite.proposedBody}`).filter(
          (f) => f.severity === "high"
        ).length === 0
      : false,
  };
}

export function buildCopyGovernancePlan({
  brandSlug,
  presentationRows = [],
  brandConfig: brandConfigIn = null,
  approvedSources = [],
  assetPack = null,
} = {}) {
  const brandConfig = brandConfigIn || getActiveProfileBrandConfig(brandSlug);
  const governanceConfig = getCopyGovernanceConfig(brandSlug);
  if (!brandConfig || !governanceConfig) {
    throw new Error(`No copy governance config for brand: ${brandSlug}`);
  }

  const audits = [];
  const repairs = [];
  const founderReviewQueue = [];
  const skipped = [];

  for (const row of presentationRows) {
    if (!isVisibleRow(row)) continue;
    const beforeText = `${nz(row.title)}\n${nz(row.body)}`;
    if (!nz(row.body) && !nz(row.title)) continue;

    const findingsBefore = auditPresentationCopy(beforeText);
    const genericBefore = isGenericBoilerplate(beforeText, {
      brandName: governanceConfig.brandName,
      parentPlatform: governanceConfig.parentPlatform,
    });

    if (!findingsBefore.length && !genericBefore) {
      skipped.push({ recordId: row.recordId, slotKey: row.slotKey, reason: "clean" });
      continue;
    }

    audits.push({
      recordId: row.recordId,
      slotKey: row.slotKey,
      beforeTitle: row.title,
      beforeBody: row.body,
      findingsBefore,
      genericBefore,
    });

    let rewrite =
      proposeSlotRewrite(row, governanceConfig, brandConfig, approvedSources) ||
      proposeRegexRepair(row, governanceConfig.copyRepairTargets);

    if (!rewrite) {
      const sanitizedBody = sanitizeUnsafeCopy(row.body);
      const sanitizedTitle = sanitizeUnsafeCopy(row.title);
      if (
        (sanitizedBody !== row.body || sanitizedTitle !== row.title) &&
        auditPresentationCopy(`${sanitizedTitle}\n${sanitizedBody}`).filter((f) => f.severity === "high")
          .length === 0
      ) {
        rewrite = {
          proposedTitle: sanitizedTitle,
          proposedBody: sanitizedBody,
          rewriteStrategy: "sanitize_only",
          reason: "unsafe_pattern_sanitization",
          sourceSupport: resolveSourceRefs(["consumerUrl"], brandConfig, governanceConfig, approvedSources),
        };
      }
    }

    if (!rewrite) {
      founderReviewQueue.push({
        recordId: row.recordId,
        slotKey: row.slotKey,
        reason: "no_brand_specific_rewrite_available",
        findingsBefore: findingsBefore.map((f) => f.patternId),
      });
      continue;
    }

    const combined = `${rewrite.proposedTitle}\n${rewrite.proposedBody}`;
    const findingsAfter = auditPresentationCopy(combined);
    const highAfter = findingsAfter.filter((f) => f.severity === "high");
    const genericAfter = isGenericBoilerplate(combined, {
      brandName: governanceConfig.brandName,
      parentPlatform: governanceConfig.parentPlatform,
    });
    const specificity = brandSpecificityScore(combined, {
      brandName: governanceConfig.brandName,
      segment: governanceConfig.segment,
      positioningPillars: governanceConfig.positioningPillars,
    });

    if (highAfter.length || genericAfter || specificity < 2) {
      founderReviewQueue.push({
        recordId: row.recordId,
        slotKey: row.slotKey,
        reason: highAfter.length
          ? "rewrite_still_unsafe"
          : genericAfter
            ? "rewrite_too_generic"
            : "insufficient_brand_specificity",
        proposedTitle: rewrite.proposedTitle,
        proposedBody: rewrite.proposedBody,
        findingsAfter: findingsAfter.map((f) => f.patternId),
        specificityScore: specificity,
      });
      continue;
    }

    if (
      normalizeCompare(row.title, rewrite.proposedTitle) &&
      normalizeCompare(row.body, rewrite.proposedBody)
    ) {
      skipped.push({ recordId: row.recordId, slotKey: row.slotKey, reason: "already_matches_proposal" });
      continue;
    }

    repairs.push({
      recordId: row.recordId,
      slotKey: row.slotKey,
      action: "update",
      beforeTitle: row.title,
      beforeBody: row.body,
      proposedTitle: rewrite.proposedTitle,
      proposedBody: rewrite.proposedBody,
      findingsBefore,
      findingsAfter,
      rewriteStrategy: rewrite.rewriteStrategy,
      fixReason: rewrite.reason,
      sourceSupport: rewrite.sourceSupport || [],
      rationale: buildRewriteRationale({
        brandName: governanceConfig.brandName,
        rewrite,
        governanceConfig,
      }),
      fields: {
        Title: rewrite.proposedTitle,
        Body: rewrite.proposedBody,
      },
    });
  }

  const plan = {
    copyGovernanceVersion: COPY_GOVERNANCE_VERSION,
    brandSlug,
    brandName: governanceConfig.brandName,
    segment: governanceConfig.segment,
    positioningPillars: governanceConfig.positioningPillars,
    mode: "dry-run",
    audits,
    repairs,
    visibilityPatches: [],
    founderReviewQueue,
    skipped,
    summary: {
      rowsAudited: audits.length,
      repairsProposed: repairs.length,
      founderReviewRequired: founderReviewQueue.length,
      skippedClean: skipped.filter((s) => s.reason === "clean").length,
      projectedHighFindingsAfter: repairs.filter((r) =>
        r.findingsAfter.some((f) => f.severity === "high")
      ).length,
      pass: founderReviewQueue.length === 0 && repairs.every((r) => r.rationale.safeForExplorer),
    },
    assetPackContext: assetPack
      ? {
          propertyExamples: assetPack.propertyExamples?.length || 0,
          galleryReady: assetPack.summary?.galleryReady,
        }
      : null,
  };

  const resolution = resolveFounderReviewQueue({
    plan,
    presentationRows,
    brandConfig,
    approvedSources,
  });

  return mergeQueueResolutionIntoPlan(plan, resolution);
}

function normalizeCompare(a, b) {
  return nz(a).replace(/\s+/g, " ").trim() === nz(b).replace(/\s+/g, " ").trim();
}

export function buildCopyGovernanceMarkdown(plan) {
  const lines = [];
  lines.push(`# Active Profile Copy Governance ${plan.copyGovernanceVersion}`);
  lines.push("");
  lines.push(`- Brand: **${plan.brandName}** (\`${plan.brandSlug}\`)`);
  lines.push(`- Segment: ${plan.segment}`);
  lines.push(`- Rows audited (unsafe): **${plan.summary.rowsAudited}**`);
  lines.push(`- Repairs proposed: **${plan.summary.repairsProposed}**`);
  lines.push(`- Founder review required: **${plan.summary.founderReviewRequired}**`);
  lines.push("");

  if (plan.positioningPillars?.length) {
    lines.push("## Positioning pillars");
    for (const p of plan.positioningPillars) lines.push(`- ${p}`);
    lines.push("");
  }

  for (const repair of plan.repairs) {
    lines.push(`## ${repair.slotKey}`);
    lines.push(`- Strategy: ${repair.rewriteStrategy}`);
    lines.push(`- Unsafe patterns: ${repair.findingsBefore.map((f) => f.patternId).join(", ")}`);
    lines.push("");
    lines.push("**Before**");
    lines.push(`> ${nz(repair.beforeBody).slice(0, 300)}${repair.beforeBody?.length > 300 ? "…" : ""}`);
    lines.push("");
    lines.push("**After**");
    lines.push(`> ${nz(repair.proposedBody).slice(0, 400)}${repair.proposedBody?.length > 400 ? "…" : ""}`);
    lines.push("");
    lines.push("**Why this rewrite**");
    lines.push(`- Brand-specific: ${repair.rationale.brandSpecific}`);
    lines.push(`- Source-supported: ${(repair.rationale.sourceSupported || []).join(", ") || "(brand config URLs)"}`);
    lines.push(`- Owner-facing / non-performance: ${repair.rationale.ownerFacing}`);
    lines.push("");
  }

  if (plan.founderReviewQueue.length) {
    lines.push("## Founder review queue (no generic filler applied)");
    for (const item of plan.founderReviewQueue) {
      lines.push(`- \`${item.slotKey}\`: ${item.resolverNote || item.reason}`);
    }
    lines.push("");
  }

  if (plan.founderQueueResolution) {
    lines.push("## Founder queue resolver (v34C-R1)");
    lines.push(`- Resolved by rewrite: **${plan.founderQueueResolution.summary.resolvedByRewrite}**`);
    lines.push(`- Resolved by hide: **${plan.founderQueueResolution.summary.resolvedByHide}**`);
    lines.push(`- Remaining manual: **${plan.founderQueueResolution.summary.remainingManual}**`);
    lines.push("");
    if (plan.visibilityPatches?.length) {
      lines.push("### Hidden rows");
      for (const h of plan.visibilityPatches) {
        lines.push(`- \`${h.slotKey}\`: ${h.reason}`);
      }
      lines.push("");
    }
  }

  return lines.join("\n");
}

const BLOCKED_APPLY_FIELDS = new Set([
  "Image",
  "Images",
  "Scenario Image",
  "Attachments",
  "Company Validated",
  "Company Validation Date",
  "Summary URL",
  "View Summary URL",
  "Brand Asset Registry",
]);

export async function applyCopyGovernancePlan({
  plan,
  apply = false,
  guardFlags = {},
  baseId,
  apiKey,
} = {}) {
  if (!apply) {
    return { applied: false, reason: "dry_run_only", preview: plan.summary };
  }
  if (!guardFlags.approveCopyGovernance) {
    return { applied: false, reason: "missing_approve_copy_governance_flag" };
  }
  if (plan.founderReviewQueue.length) {
    return {
      applied: false,
      reason: "founder_review_queue_not_empty",
      queueCount: plan.founderReviewQueue.length,
    };
  }

  const results = { updated: [], hidden: [], errors: [] };
  const presentationTable = "Brand Setup - Brand Explorer Presentation";

  const allPatches = [
    ...plan.repairs.map((r) => ({ ...r, patchType: "copy" })),
    ...(plan.visibilityPatches || []).map((h) => ({ ...h, patchType: "hide" })),
  ];

  for (const patch of allPatches) {
    for (const field of Object.keys(patch.fields || {})) {
      if (BLOCKED_APPLY_FIELDS.has(field)) {
        return { applied: false, reason: "blocked_field_in_patch", field };
      }
    }
  }

  for (const patch of allPatches) {
    try {
      const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(presentationTable)}/${patch.recordId}`;
      const res = await fetch(url, {
        method: "PATCH",
        headers: {
          Authorization: `Bearer ${apiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ fields: patch.fields, typecast: true }),
      });
      const json = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(json.error?.message || `PATCH failed: ${res.status}`);
      if (patch.patchType === "hide") results.hidden.push(patch.recordId);
      else results.updated.push(patch.recordId);
    } catch (err) {
      results.errors.push({ recordId: patch.recordId, message: err.message });
    }
  }

  return { applied: results.errors.length === 0, results };
}
