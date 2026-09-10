/**
 * ADP_AGGREGATE_CLAIM_SUPPORT_PARITY_V1
 *
 * Every evidence-backed customer aggregate must declare a support set where:
 *   displayedCount === supportCount === supportIds.length
 * unless the UI explicitly states a different grain.
 *
 * METHODOLOGY_IS_GOVERNED; QUALITY_CONTROLS_LEARN. — presentation contract only.
 */

export const ADP_AGGREGATE_CLAIM_SUPPORT_PARITY_V1 =
  "ADP_AGGREGATE_CLAIM_SUPPORT_PARITY_V1";

/**
 * @param {{
 *   claimId: string,
 *   claimType: string,
 *   aggregationGrain: string,
 *   supportIds: string[],
 *   displayedCount: number,
 *   supportCount?: number,
 *   uiGrainNote?: string|null,
 * }} claim
 */
export function buildAggregateClaimSupportParity(claim = {}) {
  const supportIds = Array.isArray(claim.supportIds) ? claim.supportIds.filter(Boolean) : [];
  const supportCount =
    claim.supportCount != null ? Number(claim.supportCount) : supportIds.length;
  const displayedCount = Number(claim.displayedCount) || 0;
  const differentGrainExplained = Boolean(claim.uiGrainNote);
  const pass =
    differentGrainExplained ||
    (displayedCount === supportCount && supportCount === supportIds.length);

  return {
    contract: ADP_AGGREGATE_CLAIM_SUPPORT_PARITY_V1,
    claimId: claim.claimId || null,
    claimType: claim.claimType || null,
    aggregationGrain: claim.aggregationGrain || "support_id",
    supportIds,
    displayedCount,
    supportCount,
    pass,
    failReason: pass
      ? null
      : `displayedCount(${displayedCount}) !== supportCount(${supportCount}) or supportIds.length(${supportIds.length})`,
  };
}

export function assertAggregateClaimSupportParity(claim) {
  const result = buildAggregateClaimSupportParity(claim);
  if (!result.pass) {
    const err = new Error(
      `${ADP_AGGREGATE_CLAIM_SUPPORT_PARITY_V1} FAIL: ${result.failReason} claimId=${result.claimId}`
    );
    err.code = ADP_AGGREGATE_CLAIM_SUPPORT_PARITY_V1;
    err.result = result;
    throw err;
  }
  return result;
}
