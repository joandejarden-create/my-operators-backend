/**
 * Golden-demo isolation — KGPV and fixture demos must not claim production readiness.
 */

export const KGPV_HPC_ID = "recUNycnMwOVFX0hc";

export const GOLDEN_DEMO_HOTEL_IDS = Object.freeze([KGPV_HPC_ID]);

export function isGoldenDemoHotel(hpcId) {
  return GOLDEN_DEMO_HOTEL_IDS.includes(String(hpcId || "").trim());
}

export function assertNotGoldenDemoAsProduction(hpcId, opts = {}) {
  if (isGoldenDemoHotel(hpcId) && opts.allowGoldenDemo !== true) {
    return {
      ok: false,
      error: "GOLDEN_DEMO_ISOLATION",
      hpcId: String(hpcId),
      message:
        "KGPV golden-demo is a regression fixture only — excluded from HE-INTEL-001 pilot production research",
    };
  }
  return { ok: true, hpcId: String(hpcId || "") };
}

/**
 * Fixture / state-machine traversal alone must not imply research-complete.
 */
export function assertResearchCompleteClaim(staged, opts = {}) {
  if (!staged?.research_complete) {
    return { ok: true, research_complete: false };
  }
  if (opts.allowClaim !== true) {
    return {
      ok: false,
      error: "RESEARCH_COMPLETE_CLAIM_FORBIDDEN",
      message:
        "P1 forbids research_complete=true from fixture traversal or historical data alone",
    };
  }
  if (!staged?.validation?.ok || !(staged?.evidence || []).length) {
    return {
      ok: false,
      error: "RESEARCH_COMPLETE_WITHOUT_EVIDENCE",
    };
  }
  return { ok: true, research_complete: true };
}
