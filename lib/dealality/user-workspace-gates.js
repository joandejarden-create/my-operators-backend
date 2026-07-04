/**
 * Workspace gates from req.dealalityUser (populated by resolveDealalityUser).
 * Use flags / workspaceAccess — not legacy role string alone.
 *
 * Demo is preview/sandbox access only — NOT Admin and does NOT grant production
 * Owner/Operator/Brand write gates unless those workspaces are also present.
 */

import {
  WORKSPACE_BRAND,
  WORKSPACE_DEMO,
  WORKSPACE_OPERATOR,
  WORKSPACE_OWNER,
} from "../company-workspace-access.js";

function workspaceList(u) {
  return Array.isArray(u?.workspaceAccess) ? u.workspaceAccess : [];
}

function isAdminUser(u) {
  return !!(u?.isAdmin || u?.flags?.isAdmin);
}

/** @param {object|null|undefined} u req.dealalityUser */
export function userCanAccessDemoWorkspace(u) {
  if (!u) return false;
  if (u.canAccessDemoWorkspace === true) return true;
  if (u.flags?.isDemo === true) return true;
  if (u.isDemo === true) return true;
  return workspaceList(u).includes(WORKSPACE_DEMO);
}

/** @param {object|null|undefined} u req.dealalityUser */
export function userCanAccessOwnerWorkspace(u) {
  if (!u) return false;
  if (isAdminUser(u)) return true;
  if (u.canAccessOwnerWorkspace === true) return true;
  if (u.flags?.isOwner === true) return true;
  if (workspaceList(u).includes(WORKSPACE_OWNER)) return true;
  if (u.isOwner === true) return true;
  return false;
}

/** @param {object|null|undefined} u req.dealalityUser */
export function userCanAccessOperatorWorkspace(u) {
  if (!u) return false;
  if (isAdminUser(u)) return true;
  if (u.canAccessOperatorWorkspace === true) return true;
  if (u.flags?.isOperator === true) return true;
  if (workspaceList(u).includes(WORKSPACE_OPERATOR)) return true;
  if (u.isOperator === true) return true;
  return false;
}

/** @param {object|null|undefined} u req.dealalityUser */
export function userCanAccessBrandWorkspace(u) {
  if (!u) return false;
  if (isAdminUser(u)) return true;
  if (u.canAccessBrandWorkspace === true) return true;
  if (u.flags?.isBrand === true) return true;
  if (workspaceList(u).includes(WORKSPACE_BRAND)) return true;
  if (u.isBrand === true) return true;
  return false;
}
