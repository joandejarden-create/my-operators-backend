import { getDealalityPublicHomeUrl } from "../lib/dealality-public-home-url.js";
import { memberstackSecretEnvironment } from "../lib/memberstack/environment.js";

/**
 * GET /api/signup/config — public signup page settings (no secrets).
 */
export default function signupConfig(req, res) {
  if (req.method !== "GET") {
    return res.status(405).json({ error: "Method not allowed" });
  }

  const appId = (process.env.MEMBERSTACK_APP_ID || "").trim();
  const pendingPlanId = (process.env.MEMBERSTACK_SIGNUP_PENDING_PLAN_ID || "").trim();
  const useDomSignup = process.env.SIGNUP_USE_DOM_SIGNUP !== "false";
  const adminApiEnvironment = memberstackSecretEnvironment(process.env.MEMBERSTACK_SECRET_KEY);

  res.json({
    ok: true,
    homeUrl: getDealalityPublicHomeUrl(),
    appId,
    pendingPlanId,
    useDomSignup: useDomSignup && Boolean(appId),
    memberstackScript: "https://static.memberstack.com/scripts/v1/memberstack.js",
    /** sandbox = Test Mode in dashboard; live = Production */
    adminApiEnvironment,
    environmentChecklist:
      adminApiEnvironment === "sandbox"
        ? "Use Test Mode in Memberstack dashboard; App ID from Test mode; secret sk_sb_…"
        : adminApiEnvironment === "live"
          ? "Use Live mode in dashboard; App ID from Live mode; secret sk_… (not sk_sb_)"
          : "Set MEMBERSTACK_SECRET_KEY (sk_sb_ for Test Mode)",
  });
}
