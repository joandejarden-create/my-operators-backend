/**
 * Founder Research inventory access — server-side entitlement for ?live=1.
 * Customer / external share sessions must never receive unfinalized research.
 */

export const GDI_FOUNDER_RESEARCH_ACCESS_V1 = "gdi-founder-research-access-v1";

/**
 * @param {object} req
 * @returns {{ ok: true, via: string } | { ok: false, error: string, status: number, message: string }}
 */
export function assertFounderResearchAccess(req = {}) {
  // External customer share must never open research inventory.
  if (req.gdiExternalShare === true) {
    return {
      ok: false,
      error: "UNAUTHORIZED_RESEARCH_ACCESS",
      status: 403,
      message:
        "Unfinalized research inventory is not available on customer share surfaces.",
    };
  }

  // Authenticated Dealality session (founder/admin Memberstack).
  if (req.dealalityUser) {
    return { ok: true, via: "dealalityUser" };
  }

  // Local/pilot founder read (same boundary as other founder-only GDI tools).
  // Never treat this as customer authorization.
  if (req.gdiPilotUnauthRead === true && req.gdiExternalShare !== true) {
    return { ok: true, via: "gdiPilotUnauthRead" };
  }

  return {
    ok: false,
    error: "UNAUTHORIZED_RESEARCH_ACCESS",
    status: 401,
    message:
      "Founder Research inventory requires an authorized founder/admin session.",
  };
}

export default {
  GDI_FOUNDER_RESEARCH_ACCESS_V1,
  assertFounderResearchAccess,
};
