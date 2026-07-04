/**
 * Owner Pilot Runbook access — platform admin from /api/me only.
 *
 * Uses dealality.isAdmin (Workspace Access Admin on Company Profile).
 * Does NOT use dev nav override, workspace preview role, or legacy role tokens.
 *
 * Customer pilot owners provisioned with Admin + Owner (e.g. AO Hospitality) are
 * intentionally included — they use the runbook to validate pilot setup.
 */

/**
 * @param {{
 *   email?: string|null,
 *   dealality?: { isAdmin?: boolean, flags?: { isAdmin?: boolean } }|null,
 *   companyName?: string|null,
 * }} ctx
 */
export function isInternalRunbookAdmin(ctx = {}) {
  const dealality = ctx.dealality || {};
  if (dealality.isAdmin === true) return true;
  if (dealality.flags && dealality.flags.isAdmin === true) return true;
  return false;
}

export function getInternalRunbookAdminConfigPreview() {
  return {
    source: "dealality.isAdmin",
  };
}
