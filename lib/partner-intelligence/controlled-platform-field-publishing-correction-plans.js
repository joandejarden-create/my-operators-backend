/**
 * Steward correction plan metadata (read-only report context).
 * @see docs/data-intelligence/controlled-platform-field-publishing-v2.md §9
 */

export const GHL_SPECIFIC_MARKETS_CORRECTION = {
  entityName: "GHL Hoteles (GHL Holding)",
  entityType: "operator",
  targetRecId: "reciI2tYQBfMoMK9G",
  destinationTable: "Operator Setup - Platform & Markets",
  destinationField: "specificMarkets",
  destinationFieldKey: "specificMarkets",
  destinationRecordId: "recbY3IGCh2LZQ3Mi",
  sourceFactId: "reccszsLnWjA5fPnp",
  sourceFactKey: "op.markets.regionsSupported",
  originalPublishSourceId: "recoOcRjSD3VZb3qt",
  originalPublishSourceTitle: "GHL Hoteles events",
  originalPublishSourceUrl: "https://www.ghlhoteles.com/en/events/",
  evidenceSource: {
    sourceId: "reckrUB2WmnSm02g3",
    title: "GHL Hoteles destinations",
    url: "https://www.ghlhoteles.com/en/destinations/",
  },
  recommendedCorrectedValue: "Colombia, Chile, Guatemala, Peru",
  correctionReason:
    "Destinations page lists specific markets as Colombia, Chile, Guatemala, and Peru; Latin America is regional context.",
  regionalContextNote:
    '"Latin America" appears on the official destinations page as regional framing ("Destinations in Latin America") but `specificMarkets` should store the four country names only. Keep regional scope in PI evidence and fact context — not in this platform field unless the field definition is explicitly regional.',
  isGovernanceCorrection: false,
  companyValidatedUntouched: true,
  requiresStewardApproval: true,
  destinationsListed: ["Colombia", "Chile", "Guatemala", "Peru"],
};

export function buildGhlSpecificMarketsCorrectionPlan(liveValue) {
  const plan = GHL_SPECIFIC_MARKETS_CORRECTION;
  return {
    correctionVersion: "v2.1-steward-plan",
    generatedAt: new Date().toISOString(),
    entity: {
      name: plan.entityName,
      type: plan.entityType,
      targetRecId: plan.targetRecId,
    },
    destination: {
      table: plan.destinationTable,
      field: plan.destinationField,
      fieldKey: plan.destinationFieldKey,
      recordId: plan.destinationRecordId,
    },
    values: {
      currentLive: liveValue ?? null,
      recommendedCorrected: plan.recommendedCorrectedValue,
      priorControlledPublishValue:
        "Latin America, Colombia, Peru, Chile, Guatemala",
    },
    correction: {
      reason: plan.correctionReason,
      whyNotBlindControlledPublish:
        "Controlled publish v2 requires a blank destination; field is now populated after first publish.",
      whyStewardReviewRequired:
        "Overwrite of populated allowlisted field requires explicit steward approval and correction mode.",
    },
    evidence: {
      primarySource: plan.evidenceSource,
      destinationsListed: plan.destinationsListed,
      regionalFraming: "Destinations in Latin America",
      regionalFramingNote:
        "Regional label is evidence/context only — not written to specificMarkets.",
    },
    priorPublish: {
      sourceFactId: plan.sourceFactId,
      sourceFactKey: plan.sourceFactKey,
      originalSourceId: plan.originalPublishSourceId,
      originalSourceTitle: plan.originalPublishSourceTitle,
      originalSourceUrl: plan.originalPublishSourceUrl,
    },
    safety: {
      isGovernanceCorrection: plan.isGovernanceCorrection,
      companyValidatedUntouched: plan.companyValidatedUntouched,
      piFactsUntouched: true,
      sourcesUntouched: true,
      scoringUntouched: true,
      requiresStewardApproval: plan.requiresStewardApproval,
      approvalFlag: "--approve-controlled-field-correction",
    },
    commands: {
      dryRun: `npm run controlled-platform-field-publishing -- --entity-type operator --target-rec-id ${plan.targetRecId} --destination-field specificMarkets --correct-value "${plan.recommendedCorrectedValue}" --reason "${plan.correctionReason}" --dry-run`,
      applyWhenApproved: `npm run controlled-platform-field-publishing -- --entity-type operator --target-rec-id ${plan.targetRecId} --destination-field specificMarkets --correct-value "${plan.recommendedCorrectedValue}" --reason "${plan.correctionReason}" --apply --approve-controlled-field-correction`,
      applyNote: "Run apply only after founder/steward approves dry-run correction plan.",
    },
    stewardContext: plan,
  };
}

export function buildGhlSpecificMarketsCorrectionMarkdown(planReport) {
  const p = planReport;
  const lines = [
    "# GHL specificMarkets — Steward Correction Plan",
    "",
    `Generated: ${p.generatedAt}`,
    "",
    "## 1. Entity",
    "",
    `- **${p.entity.name}**`,
    `- Operator record: \`${p.entity.targetRecId}\``,
    "",
    "## 2. Destination",
    "",
    `- Table: **${p.destination.table}**`,
    `- Field: \`${p.destination.field}\``,
    `- Destination record: \`${p.destination.recordId}\``,
    "",
    "## 3. Current live value",
    "",
    `\`${p.values.currentLive ?? "—"}\``,
    "",
    "## 4. Recommended corrected value",
    "",
    `**${p.values.recommendedCorrected}**`,
    "",
    "## 5. Reason for correction",
    "",
    p.correction.reason,
    "",
    "The prior controlled publish used the events-page source, which included regional framing. The official destinations page lists four specific countries.",
    "",
    "## 6. Evidence source",
    "",
    `- \`${p.evidence.primarySource.sourceId}\` — ${p.evidence.primarySource.title}`,
    `- ${p.evidence.primarySource.url}`,
    `- Destinations listed: ${p.evidence.destinationsListed.join(", ")}`,
    "",
    "## 7. Latin America as context only",
    "",
    p.stewardContext.regionalContextNote,
    "",
    "## 8. Not a governance correction",
    "",
    "This updates only `specificMarkets` on Platform & Markets. Validation Status, external display, and profile governance are unchanged.",
    "",
    "## 9. Company Validated untouched",
    "",
    "Company Validated and Company Validation Date are not read or written by this correction path.",
    "",
    "## 10. Steward approval required",
    "",
    "Do not apply without reviewing this plan and the correction dry-run report. Use `--approve-controlled-field-correction` only after explicit approval.",
    "",
    "## Prior controlled publish",
    "",
    `- Fact: \`${p.priorPublish.sourceFactId}\` (\`${p.priorPublish.sourceFactKey}\`)`,
    `- Original source: \`${p.priorPublish.originalSourceId}\` — ${p.priorPublish.originalSourceTitle}`,
    `- Prior value: \`${p.values.priorControlledPublishValue}\``,
    "",
    "## Commands",
    "",
    "**Dry-run (run now):**",
    "",
    "```bash",
    p.commands.dryRun,
    "```",
    "",
    "**Apply (founder approval only — do not run without sign-off):**",
    "",
    "```bash",
    p.commands.applyWhenApproved,
    "```",
    "",
    p.commands.applyNote,
    "",
  ];
  return lines.join("\n");
}
