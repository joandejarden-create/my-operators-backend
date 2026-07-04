import { memberstackSecretEnvironment } from "./environment.js";

/** Same DOM script as dealality.com Webflow (v2). */
export const MEMBERSTACK_DOM_SCRIPT_URL =
  "https://static.memberstack.com/scripts/v2/memberstack.js";

/**
 * Public Memberstack client settings for browser (app id + script URL only — no secrets).
 */
export function getMemberstackPublicClientConfig() {
  const appId = (process.env.MEMBERSTACK_APP_ID || "").trim();
  const adminApiEnvironment = memberstackSecretEnvironment(process.env.MEMBERSTACK_SECRET_KEY);

  const localhostAuthNote =
    "Memberstack does not allow localhost in Application Domains (Live or Test). " +
    "Local login uses Test Mode (mem_sb_… ids); /api/me resolves your Airtable Users row by email " +
    "without overwriting the Live mem_cmq… id used on dealality.com. " +
    "Set MEMBERSTACK_TEST_SECRET_KEY=sk_sb_… in .env so the server can look up sandbox member email. " +
    "For live mem_cmq… ids on localhost, log in on dealality.com and open this URL with ?msToken=<eyJ…>.";

  return {
    success: true,
    appId,
    configured: Boolean(appId),
    memberstackScript: MEMBERSTACK_DOM_SCRIPT_URL,
    adminApiEnvironment,
    localhostAuthNote,
  };
}
