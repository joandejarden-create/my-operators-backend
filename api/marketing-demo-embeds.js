/**
 * GET /api/marketing/demo-embeds
 * Public embed URL manifest for Webflow marketing pages (live app iframes + CALA demo deal).
 */

const DEFAULT_DEMO_DEAL_ID = "recqGVET08a8faagy";
const DEFAULT_DEMO_DEAL_NAME = "Mérida Centro Select-Service";

function resolveAppBaseUrl(req) {
  const envBase = (process.env.PUBLIC_URL || process.env.DEALALITY_APP_BASE_URL || "").trim().replace(/\/$/, "");
  if (envBase) return envBase;
  const proto = req.get("x-forwarded-proto") || req.protocol || "https";
  const host = req.get("x-forwarded-host") || req.get("host") || "localhost:8080";
  return `${proto}://${host}`;
}

function buildEmbeds(baseUrl, dealId) {
  const id = encodeURIComponent(dealId);
  return {
    heroDashboard: `${baseUrl}/app/home.html?embed=1&appShell=1`,
    brandExplorer: `${baseUrl}/brand-education-atelier-north.html?embed=1`,
    operatorExplorer: `${baseUrl}/operator-explorer-gold-mock.html?embed=1`,
    marketIntelligence: `${baseUrl}/market-alerts.html?embed=1`,
    termComparison: `${baseUrl}/deal-compare.html?embed=1&dealId=${id}`,
    dealRoom: `${baseUrl}/deal-room-owner.html?embed=1&dealId=${id}&marketingEmbed=1`,
    loiHandoff: `${baseUrl}/deal-setup.html?embed=1&id=${id}&edit=1&marketingEmbed=1`,
  };
}

export function getMarketingDemoEmbeds(req, res) {
  const baseUrl = resolveAppBaseUrl(req);
  const demoDealId = (process.env.MARKETING_DEMO_DEAL_ID || DEFAULT_DEMO_DEAL_ID).trim();
  const demoDealName = (process.env.MARKETING_DEMO_DEAL_NAME || DEFAULT_DEMO_DEAL_NAME).trim();

  res.json({
    success: true,
    baseUrl,
    demoDealId,
    demoDealName,
    notes: {
      heroDashboard: "Command Center with sample KPI layout (no login required).",
      brandExplorer: "Static brand education profile (Atelier North demo).",
      operatorExplorer: "Operator DNA gold mock with built-in sample operator.",
      marketIntelligence: "Market Alerts with sample preview banner when live feed is disconnected.",
      termComparison: "Live Deal Compare for CALA demo deal (submitted proposals from Airtable).",
      dealRoom: "Live Deal Room folders and NDA table for CALA demo deal.",
      loiHandoff: "Deal Setup workflow shell for CALA demo deal (full data when viewer is signed in).",
    },
    embeds: buildEmbeds(baseUrl, demoDealId),
  });
}
