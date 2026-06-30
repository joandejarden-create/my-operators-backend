/**
 * GET /api/marketing/landing-config
 * Public config for Railway landing iframe (Clarity project id, analytics flag).
 */
export default function marketingLandingConfig(req, res) {
  if (req.method !== "GET") {
    return res.status(405).json({ error: "Method not allowed" });
  }

  const clarityProjectId = (process.env.CLARITY_PROJECT_ID || "").trim();
  const analyticsEnabled = process.env.LANDING_ANALYTICS_ENABLED !== "0";
  const publicUrl = (process.env.PUBLIC_URL || "").trim().replace(/\/$/, "");

  return res.status(200).json({
    clarityProjectId: clarityProjectId || null,
    analyticsEnabled,
    apiBase: publicUrl || null,
  });
}
