/**
 * Production release channel contract.
 *
 * Live ADP/AVA/GDI production uploads must run through GitHub Actions
 * (workflow dealality-production-release). Local Cursor/CLI sessions cannot
 * provide cross-machine exclusivity.
 */

export const GITHUB_ACTIONS_CHANNEL = "github-actions";
export const AUTHORIZATION_PHRASE = "I_AUTHORIZE_PRODUCTION_RELEASE";

export function normalizeChannel(value) {
  return String(value || "")
    .trim()
    .toLowerCase();
}

/**
 * Live deploy channel gate.
 * Dry-run / local tests may omit the channel.
 */
export function assertReleaseChannelAllowed(env = process.env, opts = {}) {
  if (String(env.DEALALITY_RELEASE_LOCK_BYPASS || "").trim() === "1") {
    return { ok: true, bypass: true, channel: "bypass" };
  }
  const liveDeploy = opts.liveDeploy === true;
  if (!liveDeploy) {
    return {
      ok: true,
      channel: normalizeChannel(env.DEALALITY_RELEASE_CHANNEL) || "local-dry-run",
      dryRun: true,
    };
  }
  const channel = normalizeChannel(
    opts.channel || env.DEALALITY_RELEASE_CHANNEL
  );
  if (channel !== GITHUB_ACTIONS_CHANNEL) {
    return {
      ok: false,
      code: "RELEASE_CHANNEL_REQUIRED",
      message:
        "Live production deploy requires DEALALITY_RELEASE_CHANNEL=github-actions (GitHub Actions workflow dealality-production-release). Local Cursor/CLI deploys are blocked.",
      channel: channel || null,
    };
  }
  return { ok: true, channel: GITHUB_ACTIONS_CHANNEL };
}

export function assertAuthorizationPhrase(phrase) {
  const value = String(phrase || "").trim();
  if (value !== AUTHORIZATION_PHRASE) {
    return {
      ok: false,
      code: "AUTHORIZATION_PHRASE_INVALID",
      message: `authorization_phrase must be exactly ${AUTHORIZATION_PHRASE}`,
    };
  }
  return { ok: true, phrase: AUTHORIZATION_PHRASE };
}

export function assertApprovedShaFormat(sha) {
  const value = String(sha || "")
    .trim()
    .toLowerCase();
  if (!/^[0-9a-f]{40}$/.test(value)) {
    return {
      ok: false,
      code: "INVALID_SHA",
      message: "approved_sha must be full 40-char lowercase hex",
    };
  }
  return { ok: true, sha: value };
}

export function assertProduct(product) {
  const value = String(product || "")
    .trim()
    .toUpperCase();
  if (!["ADP", "AVA", "GDI"].includes(value)) {
    return {
      ok: false,
      code: "INVALID_PRODUCT",
      message: "product must be ADP|AVA|GDI",
    };
  }
  return { ok: true, product: value };
}
