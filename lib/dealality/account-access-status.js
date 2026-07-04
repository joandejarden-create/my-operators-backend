/**
 * Derive account access state for /api/me and Webflow pending-approval UI.
 * Basic-plan signups stay pending until admin assigns workspace + Memberstack role plan.
 */

function statusTokens(envValue, fallback) {
  return String(envValue || fallback)
    .split("|")
    .map((s) => s.trim().toLowerCase())
    .filter(Boolean);
}

function hasAnyWorkspaceAccess(dealalityRole) {
  if (!dealalityRole || typeof dealalityRole !== "object") return false;
  if (dealalityRole.isAdmin) return true;
  if (dealalityRole.canAccessOwnerWorkspace) return true;
  if (dealalityRole.canAccessBrandWorkspace) return true;
  if (dealalityRole.canAccessOperatorWorkspace) return true;
  if (dealalityRole.canAccessDemoWorkspace) return true;
  const ws = dealalityRole.workspaceAccess;
  return Array.isArray(ws) && ws.length > 0;
}

/**
 * @param {{ dealalityRole: object, accountStatusRaw?: string | null }} input
 * @returns {{
 *   state: 'pending_approval' | 'active',
 *   pendingApproval: boolean,
 *   accountStatus: string | null,
 *   userTitle: string | null,
 *   userMessage: string | null,
 *   suppressBrandAssignmentToast: boolean,
 * }}
 */
export function resolveAccountAccessStatus({ dealalityRole, accountStatusRaw }) {
  const accountStatus =
    typeof accountStatusRaw === "string" && accountStatusRaw.trim()
      ? accountStatusRaw.trim()
      : null;
  const statusLower = accountStatus ? accountStatus.toLowerCase() : "";

  const pendingTokens = statusTokens(process.env.SIGNUP_AIRTABLE_PENDING_STATUS, "Pending");
  const activeTokens = statusTokens(
    process.env.SIGNUP_AIRTABLE_APPROVED_STATUS || process.env.SIGNUP_AIRTABLE_ACTIVE_STATUS,
    "Active"
  );

  if (dealalityRole?.isAdmin) {
    return {
      state: "active",
      pendingApproval: false,
      accountStatus,
      userTitle: null,
      userMessage: null,
      suppressBrandAssignmentToast: false,
    };
  }

  const workspaceReady = hasAnyWorkspaceAccess(dealalityRole);
  const statusExplicitlyPending = accountStatus && pendingTokens.includes(statusLower);
  const statusExplicitlyActive = accountStatus && activeTokens.includes(statusLower);

  const pendingApproval =
    statusExplicitlyPending || (!workspaceReady && !statusExplicitlyActive);

  if (!pendingApproval) {
    return {
      state: "active",
      pendingApproval: false,
      accountStatus,
      userTitle: null,
      userMessage: null,
      suppressBrandAssignmentToast: false,
    };
  }

  return {
    state: "pending_approval",
    pendingApproval: true,
    accountStatus,
    userTitle: "Account pending approval",
    userMessage:
      "Thanks for signing up. Our team is reviewing your application and will enable platform access shortly. You will receive an email when your account is ready.",
    suppressBrandAssignmentToast: true,
  };
}
