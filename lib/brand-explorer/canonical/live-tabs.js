/**
 * Live 10-tab validation. A server restart or a new build signature invalidates the run.
 * Inactive-tab / 0×0 shortcuts do not pass.
 */

export const LIVE_TAB_TARGET = 10;

export function validateLiveTabs(session) {
  const blockers = [];
  if (!session || typeof session !== "object") {
    return { pass: false, code: "LIVE_APP_ALL_TABS_VALIDATED", blockers: ["missing_session"] };
  }
  if (!session.pid) blockers.push("missing_pid");
  if (!session.buildSignature) blockers.push("missing_build_signature");
  if (session.serverRestarted === true) blockers.push("server_restarted");
  if (session.buildChanged === true) blockers.push("build_changed");
  if (session.inactiveBypass === true || session.zeroByZeroBypass === true) blockers.push("inactive_tab_bypass");
  for (const key of ["expected", "activated", "measurable", "audited"]) {
    if (session[key] !== LIVE_TAB_TARGET) blockers.push(`${key}_not_10`);
  }
  return {
    pass: blockers.length === 0,
    code: "LIVE_APP_ALL_TABS_VALIDATED",
    blockers,
  };
}
