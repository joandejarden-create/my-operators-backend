/** Public marketing site (Webflow / dealality.com) — not the Railway app origin. */
export function getDealalityPublicHomeUrl() {
  const raw = (process.env.DEALALITY_PUBLIC_HOME_URL || "https://www.dealality.com").trim();
  if (!raw) return "https://www.dealality.com/";
  return raw.endsWith("/") ? raw : `${raw}/`;
}
