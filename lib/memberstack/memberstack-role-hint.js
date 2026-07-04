/**
 * Memberstack onboarding role / company-type hints — not authoritative for Workspace Access.
 */
import { normalizeWorkspaceLabel } from "../company-workspace-access.js";

function normalizeToken(value) {
  return String(value || "")
    .trim()
    .toLowerCase();
}

/**
 * Map Memberstack custom-field company type or plan label to a coarse workspace hint (for warnings only).
 * @param {string} roleHint
 * @returns {string[]} workspace labels implied by hint (Owner, Operator, Brand, Demo, Admin)
 */
export function workspacesImpliedByMemberstackRoleHint(roleHint) {
  const r = normalizeToken(roleHint);
  if (!r) return [];
  const out = new Set();
  if (r.includes("owner-operator") || r.includes("owner operator") || (r.includes("owner") && r.includes("operator"))) {
    out.add("Owner");
    out.add("Operator");
  } else if (r.includes("owner")) {
    out.add("Owner");
  } else if (
    r.includes("operator") ||
    r.includes("management") ||
    r.includes("mgmt") ||
    r.includes("3rd party")
  ) {
    out.add("Operator");
  } else if (r.includes("brand") || r.includes("franchise")) {
    out.add("Brand");
  }
  if (r.includes("demo") || r.includes("sandbox")) {
    out.add("Demo");
  }
  if (r.includes("admin")) {
    out.add("Admin");
  }
  return [...out].map((w) => normalizeWorkspaceLabel(w)).filter(Boolean);
}

/**
 * @param {string} roleHint
 * @param {string[]} effectiveWorkspaceAccess from Airtable (post-merge)
 * @returns {string[]} warning codes
 */
export function memberstackRoleHintConflictWarnings(roleHint, effectiveWorkspaceAccess) {
  const implied = workspacesImpliedByMemberstackRoleHint(roleHint);
  const effective = Array.isArray(effectiveWorkspaceAccess) ? effectiveWorkspaceAccess : [];
  if (!implied.length || !effective.length) return [];

  const impliedSet = new Set(implied);
  const effectiveSet = new Set(effective);
  const missing = implied.filter((w) => !effectiveSet.has(w));
  if (!missing.length) return [];

  if (impliedSet.has("Owner") && effectiveSet.has("Owner") && missing.length < implied.length) {
    return ["memberstack_role_hint_partial_mismatch"];
  }
  return ["memberstack_role_hint_conflicts_with_airtable_workspace"];
}
