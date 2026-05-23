/**
 * Operator Capability Snapshot — display copy (deal-only; no operator recommendations).
 */

export const OCS_OUTPUT_STATUS = "Draft for validation";

export const OCS_METHODOLOGY_NOTE =
  "Capability areas are derived from canonical deal intake fields and legacy operator inputs. " +
  "This snapshot organizes operating capabilities that may warrant structured review before brand or operator outreach. " +
  "It does not recommend, rank, endorse, or select any management company.";

export const OCS_DISCLAIMER =
  "This Operator Capability Snapshot summarizes operating capability signals from current deal inputs. " +
  "It supports internal owner/advisor review and does not constitute operator selection, match scoring, " +
  "commercial terms, legal advice, or investment advice.";

/** Version 1 advisor-facing disclaimer (required in UI). */
export const OCS_DISCLAIMER_V1 =
  "This snapshot identifies operator capabilities that may be relevant to this opportunity based on current deal inputs. " +
  "It is intended to support owner/advisor review and does not recommend, rank, endorse, or select operators.";

export const OCS_COVER_NOTE =
  "Organizes operator capability themes from deal data only. No named operators, rankings, or best-fit language.";

export function executiveSummaryLines(ctx, capabilityCount, clarificationCount) {
  const lines = [];
  if (!ctx.operatorInScope) {
    lines.push(
      "Based on current bid audience and operating model selections, structured third-party operator capability review may be limited for this deal."
    );
    return lines;
  }
  lines.push(
    `Operating context: ${ctx.currentOperatingModel} today → ${ctx.preferredFutureOperatingModel} target model (${ctx.projectType}).`
  );
  if (capabilityCount > 0) {
    lines.push(
      `${capabilityCount} capability area${capabilityCount === 1 ? "" : "s"} surfaced for review from stated priorities and deal context.`
    );
  }
  if (clarificationCount > 0) {
    lines.push(
      `${clarificationCount} clarification item${clarificationCount === 1 ? "" : "s"} should be resolved to strengthen the snapshot.`
    );
  }
  return lines;
}
