/**
 * Hotel Intelligence Factory — mode permissions (founder-approved two-mode architecture).
 */

export const HIF_MODES_V1 = "hotel-intelligence-factory-modes-v1";

export const HIF_MODE = Object.freeze({
  RESEARCH: "RESEARCH",
  CANONICAL_PROMOTION: "CANONICAL_PROMOTION",
});

/** Stages allowed per mode (PROMOTE only in MODE 2). */
export const MODE_ALLOWED_STAGES = Object.freeze({
  [HIF_MODE.RESEARCH]: Object.freeze([
    "CONFIG",
    "DISCOVER",
    "EXTRACT",
    "VERIFY",
    "ENRICH_CONTRACT_TERMS",
    "RECONCILE",
    "PERSIST_RESEARCH",
    "MANIFEST",
    "REPORT",
  ]),
  [HIF_MODE.CANONICAL_PROMOTION]: Object.freeze([
    "CONFIG",
    "RECONCILE",
    "MANIFEST",
    "PROMOTE",
    "READBACK",
    "QUEUE_HE",
    "REPORT",
  ]),
});

export function normalizeMode(mode) {
  const m = String(mode || "")
    .trim()
    .toUpperCase()
    .replace(/-/g, "_");
  if (m === "RESEARCH" || m === "MODE1" || m === "MODE_1") return HIF_MODE.RESEARCH;
  if (
    m === "CANONICAL_PROMOTION" ||
    m === "PROMOTION" ||
    m === "MODE2" ||
    m === "MODE_2" ||
    m === "CANONICALPROMOTION"
  ) {
    return HIF_MODE.CANONICAL_PROMOTION;
  }
  return null;
}

/**
 * Fail-closed permission check before any mutating stage.
 */
export function assertModePermissions(opts = {}) {
  const mode = normalizeMode(opts.mode);
  if (!mode) {
    return { ok: false, error: "INVALID_MODE", code: 2 };
  }
  const stages = (opts.stages || []).map((s) => String(s).toUpperCase());
  const allowed = new Set(MODE_ALLOWED_STAGES[mode]);
  for (const s of stages) {
    if (!allowed.has(s)) {
      return { ok: false, error: "STAGE_NOT_ALLOWED_IN_MODE", mode, stage: s, code: 2 };
    }
  }
  if (stages.includes("PROMOTE") || opts.applyPromotion) {
    if (mode !== HIF_MODE.CANONICAL_PROMOTION) {
      return { ok: false, error: "PROMOTE_REQUIRES_CANONICAL_PROMOTION_MODE", code: 2 };
    }
    if (!opts.applyPromotion) {
      return { ok: false, error: "PROMOTE_REQUIRES_APPLY_PROMOTION_FLAG", code: 2 };
    }
    if (!opts.manifestPath) {
      return { ok: false, error: "PROMOTE_REQUIRES_MANIFEST", code: 2 };
    }
    // Full binding (hash, actions, base, identities, batch, runId, expiry) is enforced by
    // promotion-authorization-v1.assertPromotionAuthorization at PROMOTE time.
    if (String(process.env.HIF_APPLY_PROMOTION || "") !== "1") {
      return { ok: false, error: "HIF_APPLY_PROMOTION_ENV_REQUIRED", code: 2 };
    }
    if (!String(process.env.HIF_FOUNDER_MANIFEST_SHA256 || "").trim()) {
      return { ok: false, error: "HIF_FOUNDER_MANIFEST_SHA256_REQUIRED", code: 2 };
    }
    if (!opts.authorization && !opts.authorizationPath) {
      return {
        ok: false,
        error: "PROMOTION_AUTHORIZATION_OBJECT_OR_PATH_REQUIRED",
        code: 2,
        hint: "Bind manifestSha256, allowedActions, canonicalBaseId, recordIdentities, maxBatchSize, runId, expiresAt",
      };
    }
  }
  if (opts.applyResearch || stages.includes("PERSIST_RESEARCH")) {
    if (mode !== HIF_MODE.RESEARCH) {
      return { ok: false, error: "PERSIST_RESEARCH_REQUIRES_RESEARCH_MODE", code: 2 };
    }
    if (opts.applyResearch && String(process.env.HIF_APPLY_RESEARCH || "") !== "1") {
      return { ok: false, error: "HIF_APPLY_RESEARCH_ENV_REQUIRED", code: 2 };
    }
  }
  if (String(process.env.HIF_PUBLICATION_AUTHORIZED || "") === "1") {
    // Publication is independent — factory still does not auto-publish in V4
    return {
      ok: true,
      mode,
      warning: "PUBLICATION_FLAG_SET_BUT_V4_DOES_NOT_PUBLISH",
    };
  }
  return { ok: true, mode };
}

export default {
  HIF_MODE,
  MODE_ALLOWED_STAGES,
  normalizeMode,
  assertModePermissions,
};
