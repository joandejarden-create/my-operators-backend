/**
 * v41 — Internal founder review packet (replaces manual tab hunting).
 */
export function buildFounderReviewPacket(brandResult = {}) {
  const {
    brandSlug,
    brandName,
    recordId,
    canonicalState,
    routing,
    gateEval,
    metrics,
    visuals,
  } = brandResult;

  const decision =
    routing?.allowedNextAction === "apply_remediation"
      ? "remediation_apply_ready"
      : routing?.allowedNextAction === "founder_visual_review"
        ? "founder_visual_review_ready"
        : routing?.allowedNextAction === "apply_active_release"
          ? "active_release_ready"
          : "not_ready";

  const failed = gateEval?.failedGates || [];
  const liveHits = gateEval?.liveInternalHits || [];
  const residualCount = gateEval?.residualPlan?.summary?.patchCount || 0;

  return {
    brandSlug,
    brandName,
    recordId,
    generatedFor: "internal_founder_review",
    canonicalState,
    decisionRecommendation: decision,
    allowedNextAction: routing?.allowedNextAction,
    exactNextCommand: routing?.exactNextCommand,
    internalPreview: {
      status: metrics?.liveInternalPreviewClean ? "clean_live_dom" : "blocked",
      forbiddenHits: liveHits,
      note:
        residualCount > 0
          ? "Live DOM may be clean via renderer scrub while Presentation rows still need v40C residual apply."
          : "Live Presentation residual clean.",
    },
    externalLock: {
      profileInPreparation: metrics?.externalQualityLockPass === true,
      fullProfileLeaked: metrics?.externalFullProfileRendered === true,
    },
    tabCompleteness: {
      failedGates: failed,
      galleryCount: visuals?.galleryCount ?? metrics?.galleryCount,
      propertyExampleCount: visuals?.openingsCount ?? metrics?.openingsCount,
    },
    imageReadiness: {
      galleryReady: metrics?.galleryReady === true,
      propertyExamplesReady: metrics?.propertyExamplesReady === true,
    },
    copyRisks: [
      ...(liveHits || []).map((h) => `live_internal:${h.label}`),
      ...(residualCount ? [`residual_presentation_patches_pending:${residualCount}`] : []),
      ...(gateEval?.presentationForbidden || []).slice(0, 10).map((h) => `presentation:${h.label}`),
    ],
    economicsLoyaltyStandardsRisks: (gateEval?.gates || [])
      .filter((g) => ["renderer_chrome_clean", "no_forbidden_copy_presentation"].includes(g.name) && !g.pass)
      .map((g) => g.name),
    propertyExamples: {
      count: metrics?.openingsCount ?? 0,
      ready: metrics?.propertyExamplesReady === true,
    },
    rowsChangedByLastRemediation: {
      v40cResidualPatchesPlanned: residualCount,
      sampleSlots: (gateEval?.residualPlan?.patches || []).slice(0, 12).map((p) => p.slotKey),
    },
    remainingFounderJudgmentItems: [
      residualCount > 0 ? "Approve/apply v40C residual Presentation remediation first" : null,
      "Run v42 founder visual review packet (tab readiness + release recommendation)",
      "Review gallery + property example image quality in internal preview",
      "Confirm brand-model tone (extended-stay / lifestyle / soft-brand)",
      "Do not set active approval until founder visual pass is explicit",
      "Company Validated must remain untouched unless true company validation exists",
    ].filter(Boolean),
  };
}

export function renderFounderReviewPacketMarkdown(packet) {
  const lines = [
    `# Founder Review Packet — ${packet.brandName || packet.brandSlug}`,
    "",
    `Slug: \`${packet.brandSlug}\` · Record: \`${packet.recordId || "n/a"}\``,
    `Canonical state: **${packet.canonicalState}**`,
    "",
    "> Internal packet. External owners still see Profile in Preparation until active release.",
    "",
    "## Decision recommendation",
    "",
    `**${packet.decisionRecommendation}**`,
    "",
    `- Allowed next action: \`${packet.allowedNextAction}\``,
    packet.exactNextCommand ? `- Exact next command:\n\`\`\`\n${packet.exactNextCommand}\n\`\`\`` : "",
    "",
    "## Internal preview",
    "",
    `- Status: **${packet.internalPreview.status}**`,
    `- ${packet.internalPreview.note}`,
    packet.internalPreview.forbiddenHits?.length
      ? `- Live forbidden: ${packet.internalPreview.forbiddenHits.map((h) => h.label).join(", ")}`
      : "- Live forbidden: none",
    "",
    "## External lock",
    "",
    `- Profile in Preparation / lock pass: **${packet.externalLock.profileInPreparation ? "yes" : "no"}**`,
    `- Full profile leaked: **${packet.externalLock.fullProfileLeaked ? "yes" : "no"}**`,
    "",
    "## Image readiness",
    "",
    `- Gallery: ${packet.tabCompleteness.galleryCount} (ready=${packet.imageReadiness.galleryReady})`,
    `- Property examples: ${packet.tabCompleteness.propertyExampleCount} (ready=${packet.imageReadiness.propertyExamplesReady})`,
    "",
    "## Copy risks",
    "",
  ];
  if (!packet.copyRisks?.length) lines.push("- None flagged.");
  else for (const r of packet.copyRisks) lines.push(`- ${r}`);

  lines.push("", "## Economics / loyalty / standards risks", "");
  if (!packet.economicsLoyaltyStandardsRisks?.length) lines.push("- None flagged.");
  else for (const r of packet.economicsLoyaltyStandardsRisks) lines.push(`- ${r}`);

  lines.push("", "## Rows changed / planned by last remediation", "");
  lines.push(`- v40C residual patches planned: ${packet.rowsChangedByLastRemediation.v40cResidualPatchesPlanned}`);
  for (const s of packet.rowsChangedByLastRemediation.sampleSlots || []) {
    lines.push(`- \`${s}\``);
  }

  lines.push("", "## Remaining founder judgment items", "");
  for (const item of packet.remainingFounderJudgmentItems || []) lines.push(`- ${item}`);

  lines.push("", "## Failed gates", "");
  for (const g of packet.tabCompleteness.failedGates || []) lines.push(`- \`${g}\``);
  if (!packet.tabCompleteness.failedGates?.length) lines.push("- none");
  lines.push("");

  return lines.filter((l) => l !== undefined).join("\n");
}
