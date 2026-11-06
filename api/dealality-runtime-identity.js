/**
 * Authenticated Dealality runtime identity — verify production against release manifest.
 *
 * GET /api/dealality/runtime-identity
 *   - Public minimal: { ok, gitSha } when DEALALITY_RUNTIME_IDENTITY_PUBLIC=1
 *   - Authenticated (Memberstack admin / internal token): full payload
 */
import crypto from "node:crypto";

function resolveGitSha(env = process.env) {
  const candidates = [
    env.DEALALITY_APPROVED_RELEASE_SHA,
    env.RAILWAY_GIT_COMMIT_SHA,
    env.GITHUB_SHA,
    env.COMMIT_SHA,
    env.DEALALITY_GIT_SHA,
  ];
  for (const c of candidates) {
    const v = String(c || "").trim();
    if (v) return v;
  }
  return null;
}

function timingSafeEqualString(a, b) {
  const aa = Buffer.from(String(a || ""));
  const bb = Buffer.from(String(b || ""));
  if (aa.length !== bb.length) return false;
  return crypto.timingSafeEqual(aa, bb);
}

export function buildDealalityRuntimeIdentity(env = process.env, opts = {}) {
  const gitSha = resolveGitSha(env);
  return {
    contract: "DEALALITY_RUNTIME_IDENTITY_V1",
    ok: true,
    gitSha,
    buildSha: gitSha,
    service: env.RAILWAY_SERVICE_NAME || env.DEALALITY_RAILWAY_SERVICE || null,
    environment: env.RAILWAY_ENVIRONMENT_NAME || env.NODE_ENV || null,
    durablePublishedMode: env.ADP_DURABLE_PUBLISHED_MODE || null,
    releaseId: env.DEALALITY_RELEASE_ID || null,
    product: env.DEALALITY_RELEASE_PRODUCT || null,
    timestamp: new Date().toISOString(),
    matchedManifestSha: opts.expectedSha
      ? String(opts.expectedSha).toLowerCase() === String(gitSha || "").toLowerCase()
      : null,
  };
}

export function createDealalityRuntimeIdentityHandler(deps = {}) {
  const {
    memberstackAuth = null,
    requireDealalityUser = null,
    env = process.env,
  } = deps;

  return function dealalityRuntimeIdentity(req, res) {
    const publicAllowed = String(env.DEALALITY_RUNTIME_IDENTITY_PUBLIC || "") === "1";
    const internalToken = String(env.DEALALITY_RUNTIME_IDENTITY_TOKEN || "").trim();
    const provided =
      String(req.get("x-dealality-runtime-token") || "").trim() ||
      String(req.query.token || "").trim();

    const authorizedInternal =
      internalToken && provided && timingSafeEqualString(internalToken, provided);

    const identity = buildDealalityRuntimeIdentity(env);

    if (!identity.gitSha) {
      return res.status(503).json({
        ok: false,
        code: "RUNTIME_SHA_UNAVAILABLE",
        message: "Runtime git SHA not configured on this process",
      });
    }

    // Public: SHA-only identity for post-deploy verification (never invent a SHA).
    // Authenticated / public-full: include release metadata.
    if (publicAllowed || authorizedInternal) {
      return res.json(identity);
    }

    return res.json({
      contract: "DEALALITY_RUNTIME_IDENTITY_V1",
      ok: true,
      gitSha: identity.gitSha,
      buildSha: identity.buildSha,
      authenticated: false,
      note: "SHA-only public view; send x-dealality-runtime-token for full identity",
    });
  };
}

export default createDealalityRuntimeIdentityHandler;
