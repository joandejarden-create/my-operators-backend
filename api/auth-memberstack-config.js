/**
 * Public Memberstack client config for local app shell / embed parents (app id only — safe for browser).
 */
export function getMemberstackPublicConfig(req, res) {
  const appId = (process.env.MEMBERSTACK_APP_ID || "").trim();
  res.json({
    success: true,
    appId,
    configured: Boolean(appId),
  });
}
