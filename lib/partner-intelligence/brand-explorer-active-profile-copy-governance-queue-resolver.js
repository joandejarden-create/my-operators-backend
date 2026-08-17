/**
 * Brand Explorer Active Profile Copy Governance Founder Queue Resolver v34C-R1.
 *
 * Resolves founderReviewQueue items via brand config — not brand-specific writers.
 */
import { scanCopySafety } from "./brand-explorer-choice-expansion-partial-profile-backfill-writer.js";
import { COPY_SAFETY_PATTERNS } from "./brand-explorer-active-profile-factory-rules.js";
import {
  GENERIC_BOILERPLATE_PATTERNS,
  getCopyGovernanceConfig,
} from "./brand-explorer-active-profile-copy-governance-config.js";

export const FOUNDER_QUEUE_RESOLVER_VERSION = "v34C-R1";

const HIDE_DISPLAY = "Do Not Display";

const COPY_GOVERNANCE_AUDIT_PATTERNS = Object.freeze([
  ...COPY_SAFETY_PATTERNS,
  { id: "revpar", re: /\brevpar\b/i, severity: "medium" },
  { id: "fee_stack", re: /\bfee stack\b/i, severity: "medium" },
  { id: "estimated_contribution", re: /\bestimated contribution\b/i, severity: "high" },
  { id: "booking_path", re: /\bbooking path\b/i, severity: "medium" },
  { id: "franchise_disclosure_long", re: /\bfranchise disclosure document\b/i, severity: "high" },
]);

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function auditPresentationCopy(text) {
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

function isGenericBoilerplate(text, { brandName = "", parentPlatform = "" } = {}) {
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

function resolveSourceEvidence(sourceRefs = [], brandConfig, governanceConfig, approvedSources = []) {
  const evidence = [];
  if (sourceRefs.includes("consumerUrl") && brandConfig?.consumerUrl) {
    evidence.push({ type: "official_consumer", url: brandConfig.consumerUrl });
  }
  if (sourceRefs.includes("developmentUrl") && governanceConfig?.developmentUrl) {
    evidence.push({ type: "official_development", url: governanceConfig.developmentUrl });
  }
  for (const src of approvedSources.slice(0, 5)) {
    const url = nz(src.sourceUrl);
    if (url) evidence.push({ type: "source_library", url, recordId: src.id || src.sourceId });
  }
  return evidence;
}

function recommendResolution(queueItem, governanceConfig) {
  const slotConfig = governanceConfig.founderQueueResolutions?.[queueItem.slotKey];
  if (slotConfig?.strategy) return slotConfig.strategy;
  if (governanceConfig.slotRewrites?.[queueItem.slotKey]?.body) return "add_brand_specific_rewrite_package";
  if (queueItem.reason === "no_brand_specific_rewrite_available") return "hide_or_manual";
  return "add_brand_specific_rewrite_package";
}

export function buildConfiguredQueueAudit({
  governanceConfig,
  presentationRows = [],
  brandConfig,
  approvedSources = [],
  plan = null,
} = {}) {
  const targets =
    governanceConfig.founderQueueTargets ||
    Object.entries(governanceConfig.founderQueueResolutions || {}).map(([slotKey, cfg]) => ({
      slotKey,
      ...cfg,
    }));

  const repairByRecord = new Map((plan?.repairs || []).map((r) => [r.recordId, r]));
  const hideByRecord = new Map((plan?.visibilityPatches || []).map((h) => [h.recordId, h]));
  const remainingByRecord = new Map((plan?.founderReviewQueue || []).map((q) => [q.recordId, q]));

  return targets.map((target) => {
    const row = target.recordId
      ? presentationRows.find((r) => r.recordId === target.recordId)
      : presentationRows.find((r) => r.slotKey === target.slotKey);
    const beforeText = row ? `${nz(row.title)}\n${nz(row.body)}` : "";
    const findings = beforeText ? auditPresentationCopy(beforeText) : [];

    let outcome = "pending";
    const rid = row?.recordId || target.recordId;
    if (rid && repairByRecord.has(rid)) outcome = "resolved_by_rewrite";
    else if (rid && hideByRecord.has(rid)) outcome = "resolved_by_hide";
    else if (rid && remainingByRecord.has(rid)) outcome = "founder_manual_review";
    else if (row && !findings.length) outcome = "clean_or_skipped";

    return {
      recordId: rid || null,
      slotKey: target.slotKey,
      title: row?.title || "",
      body: row?.body || "",
      unsafePhrases: [...new Set(findings.map((f) => f.patternId))],
      configuredStrategy: target.strategy,
      configuredReason: target.reason,
      outcome,
      sourceEvidenceAvailable: resolveSourceEvidence(
        governanceConfig.slotRewrites?.[target.slotKey]?.sourceRefs || ["consumerUrl", "developmentUrl"],
        brandConfig,
        governanceConfig,
        approvedSources
      ),
    };
  });
}

export function buildFounderQueueAudit({
  queueItems = [],
  presentationRows = [],
  governanceConfig,
  brandConfig,
  approvedSources = [],
} = {}) {
  const rowById = new Map(presentationRows.map((r) => [r.recordId, r]));

  return queueItems.map((item) => {
    const row = rowById.get(item.recordId) || {};
    const beforeText = `${nz(row.title || item.beforeTitle)}\n${nz(row.body || item.beforeBody)}`;
    const findings = auditPresentationCopy(beforeText);
    const sourceEvidence = resolveSourceEvidence(
      governanceConfig.slotRewrites?.[item.slotKey]?.sourceRefs || ["consumerUrl", "developmentUrl"],
      brandConfig,
      governanceConfig,
      approvedSources
    );

    return {
      recordId: item.recordId,
      slotKey: item.slotKey,
      title: row.title || item.beforeTitle || "",
      body: row.body || item.beforeBody || "",
      unsafePhrases: [...new Set(findings.map((f) => f.patternId))],
      whyV34CCouldNotRewrite: item.reason,
      sourceEvidenceAvailable: sourceEvidence,
      recommendedResolution: recommendResolution(item, governanceConfig),
      v34cProposedTitle: item.proposedTitle || null,
      v34cProposedBody: item.proposedBody || null,
      specificityScore: item.specificityScore ?? null,
    };
  });
}

function validateRewrite(rewrite, governanceConfig) {
  const combined = `${nz(rewrite.proposedTitle)}\n${nz(rewrite.proposedBody)}`;
  const highAfter = auditPresentationCopy(combined).filter((f) => f.severity === "high");
  const genericAfter = isGenericBoilerplate(combined, {
    brandName: governanceConfig.brandName,
    parentPlatform: governanceConfig.parentPlatform,
  });
  const specificity = brandSpecificityScore(combined, {
    brandName: governanceConfig.brandName,
    segment: governanceConfig.segment,
    positioningPillars: governanceConfig.positioningPillars,
  });
  return {
    pass: highAfter.length === 0 && !genericAfter && specificity >= 2,
    highAfter,
    genericAfter,
    specificity,
  };
}

/**
 * Second pass: resolve founder queue via config (rewrites + hide).
 */
export function resolveFounderReviewQueue({
  plan,
  presentationRows = [],
  brandConfig = null,
  approvedSources = [],
} = {}) {
  const governanceConfig = getCopyGovernanceConfig(plan?.brandSlug);
  if (!governanceConfig) throw new Error(`No copy governance config for: ${plan?.brandSlug}`);

  if (!plan?.founderReviewQueue?.length) {
    const configuredQueueAudit = buildConfiguredQueueAudit({
      governanceConfig,
      presentationRows,
      brandConfig,
      approvedSources,
      plan,
    });
    return {
      resolverVersion: FOUNDER_QUEUE_RESOLVER_VERSION,
      queueAudit: [],
      configuredQueueAudit,
      resolvedRepairs: [],
      resolvedHides: [],
      remainingQueue: [],
      summary: {
        inputQueue: 0,
        resolvedByRewrite: 0,
        resolvedByHide: 0,
        remainingManual: 0,
        configuredSlots: configuredQueueAudit.length,
      },
    };
  }

  const queueAudit = buildFounderQueueAudit({
    queueItems: plan.founderReviewQueue,
    presentationRows,
    governanceConfig,
    brandConfig,
    approvedSources,
  });

  const resolvedRepairs = [];
  const resolvedHides = [];
  const remainingQueue = [];
  const rowById = new Map(presentationRows.map((r) => [r.recordId, r]));

  for (const item of plan.founderReviewQueue) {
    const row = rowById.get(item.recordId);
    if (!row) {
      remainingQueue.push({ ...item, resolverNote: "row_not_found" });
      continue;
    }

    const slotResolution = governanceConfig.founderQueueResolutions?.[item.slotKey];
    const slotRewrite = governanceConfig.slotRewrites?.[item.slotKey];

    if (slotResolution?.strategy === "hide" || slotResolution?.strategy === "not_needed_for_active_profile") {
      resolvedHides.push({
        recordId: item.recordId,
        slotKey: item.slotKey,
        action: "hide",
        reason: slotResolution.reason || "not_needed_for_active_profile",
        fields: { "External Display Status": HIDE_DISPLAY },
        beforeTitle: row.title,
        beforeBody: row.body,
      });
      continue;
    }

    if (slotResolution?.strategy === "founder_manual_review") {
      remainingQueue.push({
        ...item,
        resolverNote: slotResolution.reason || "founder_manual_review_required",
      });
      continue;
    }

    const pkg = slotRewrite;
    if (!pkg?.body) {
      remainingQueue.push({
        ...item,
        resolverNote: "no_rewrite_package_in_config",
      });
      continue;
    }

    const rewrite = {
      proposedTitle: pkg.title || row.title,
      proposedBody: pkg.body,
      rewriteStrategy: "founder_queue_slot_rewrite",
      reason: slotResolution?.reason || "v34c_r1_queue_resolver",
      sourceSupport: resolveSourceEvidence(
        pkg.sourceRefs || ["consumerUrl", "developmentUrl"],
        brandConfig,
        governanceConfig,
        approvedSources
      ),
    };

    const validation = validateRewrite(rewrite, governanceConfig);
    if (!validation.pass) {
      if (slotResolution?.fallbackStrategy === "hide") {
        resolvedHides.push({
          recordId: item.recordId,
          slotKey: item.slotKey,
          action: "hide",
          reason: "rewrite_failed_validation_hide_instead",
          fields: { "External Display Status": HIDE_DISPLAY },
          beforeTitle: row.title,
          beforeBody: row.body,
          validation,
        });
      } else {
        remainingQueue.push({
          ...item,
          resolverNote: "rewrite_failed_validation",
          validation,
        });
      }
      continue;
    }

    resolvedRepairs.push({
      recordId: item.recordId,
      slotKey: item.slotKey,
      action: "update",
      beforeTitle: row.title,
      beforeBody: row.body,
      proposedTitle: rewrite.proposedTitle,
      proposedBody: rewrite.proposedBody,
      findingsBefore: auditPresentationCopy(`${row.title}\n${row.body}`),
      findingsAfter: auditPresentationCopy(`${rewrite.proposedTitle}\n${rewrite.proposedBody}`),
      rewriteStrategy: rewrite.rewriteStrategy,
      fixReason: rewrite.reason,
      sourceSupport: rewrite.sourceSupport,
      fields: {
        Title: rewrite.proposedTitle,
        Body: rewrite.proposedBody,
      },
      rationale: {
        brandSpecific: `Resolved via v34C-R1 config for ${governanceConfig.brandName}.`,
        sourceSupported: rewrite.sourceSupport.map((s) => s.url).filter(Boolean),
        safeForExplorer: true,
      },
    });
  }

  const mergedPlanForAudit = {
    ...plan,
    repairs: [...(plan.repairs || []), ...resolvedRepairs],
    visibilityPatches: [...(plan.visibilityPatches || []), ...resolvedHides],
    founderReviewQueue: remainingQueue,
  };
  const configuredQueueAudit = buildConfiguredQueueAudit({
    governanceConfig,
    presentationRows,
    brandConfig,
    approvedSources,
    plan: mergedPlanForAudit,
  });

  return {
    resolverVersion: FOUNDER_QUEUE_RESOLVER_VERSION,
    queueAudit,
    configuredQueueAudit,
    resolvedRepairs,
    resolvedHides,
    remainingQueue,
    summary: {
      inputQueue: plan.founderReviewQueue.length,
      resolvedByRewrite: resolvedRepairs.length,
      resolvedByHide: resolvedHides.length,
      remainingManual: remainingQueue.length,
      configuredSlots: configuredQueueAudit.length,
    },
  };
}

export function mergeQueueResolutionIntoPlan(plan, resolution) {
  if (!resolution) return plan;

  const repairRecordIds = new Set(plan.repairs.map((r) => r.recordId));
  for (const repair of resolution.resolvedRepairs) {
    if (!repairRecordIds.has(repair.recordId)) {
      plan.repairs.push(repair);
      repairRecordIds.add(repair.recordId);
    }
  }

  plan.visibilityPatches = [...(plan.visibilityPatches || []), ...resolution.resolvedHides];
  plan.founderReviewQueue = resolution.remainingQueue;
  plan.founderQueueResolution = resolution;
  plan.queueAudit = resolution.queueAudit;
  plan.configuredQueueAudit = resolution.configuredQueueAudit;

  plan.summary = {
    ...plan.summary,
    repairsProposed: plan.repairs.length,
    founderReviewRequired: resolution.remainingQueue.length,
    queueResolvedByRewrite: resolution.summary.resolvedByRewrite,
    queueResolvedByHide: resolution.summary.resolvedByHide,
    pass:
      resolution.remainingQueue.length === 0 &&
      plan.repairs.every((r) => {
        const high = auditPresentationCopy(`${r.proposedTitle}\n${r.proposedBody}`).filter(
          (f) => f.severity === "high"
        );
        return high.length === 0;
      }),
  };

  return plan;
}

export function buildFounderQueueAuditMarkdown(resolution, brandName) {
  const lines = [];
  lines.push(`# Founder Queue Resolver ${FOUNDER_QUEUE_RESOLVER_VERSION}`);
  lines.push("");
  lines.push(`- Brand: **${brandName}**`);
  lines.push(`- Input queue (v34C pass 1): **${resolution.summary.inputQueue}**`);
  lines.push(`- Resolved by rewrite (pass 2): **${resolution.summary.resolvedByRewrite}**`);
  lines.push(`- Resolved by hide (pass 2): **${resolution.summary.resolvedByHide}**`);
  lines.push(`- Remaining manual: **${resolution.summary.remainingManual}**`);
  lines.push("");

  if (resolution.configuredQueueAudit?.length) {
    lines.push("## Full configured queue audit (8 slots)");
    for (const a of resolution.configuredQueueAudit) {
      lines.push(`### ${a.slotKey} (\`${a.recordId || "n/a"}\`)`);
      lines.push(`- Outcome: **${a.outcome}**`);
      lines.push(`- Configured strategy: ${a.configuredStrategy}`);
      lines.push(`- Unsafe phrases (live): ${a.unsafePhrases.join(", ") || "none"}`);
      lines.push(`- Source evidence: ${a.sourceEvidenceAvailable.map((s) => s.url).join(", ") || "brand config URLs"}`);
      lines.push("");
      if (a.body) {
        lines.push("**Body (excerpt)**");
        lines.push(`> ${nz(a.body).slice(0, 280)}${a.body.length > 280 ? "…" : ""}`);
        lines.push("");
      }
    }
  }

  if (resolution.resolvedHides.length) {
    lines.push("## Hidden / internal");
    for (const h of resolution.resolvedHides) {
      lines.push(`- \`${h.slotKey}\` (${h.recordId}): ${h.reason}`);
    }
    lines.push("");
  }

  if (resolution.remainingQueue.length) {
    lines.push("## Still requires founder judgment");
    for (const r of resolution.remainingQueue) {
      lines.push(`- \`${r.slotKey}\`: ${r.resolverNote || r.reason}`);
    }
    lines.push("");
  }

  return lines.join("\n");
}
