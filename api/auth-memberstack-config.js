import { getMemberstackPublicClientConfig } from "../lib/memberstack/client-config.js";

/**
 * Public Memberstack client config for local app shell / embed parents (app id only — safe for browser).
 */
export function getMemberstackPublicConfig(req, res) {
  res.json(getMemberstackPublicClientConfig());
}
