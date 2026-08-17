/**
 * Extract + normalize Hyatt DAM image URLs from a browser page HTML string.
 * Used by the Wave 17 harvest accumulator.
 */
export function extractHyattDamFromHtml(html, { propertyCode = "", propertyName = "" } = {}) {
  const urls = new Set();
  for (const m of String(html || "").matchAll(/https?:\\?\/\\?\/assets\.hyatt\.com[^"'\\\s<>]+/g)) {
    const u = m[0]
      .replace(/\\u002F/g, "/")
      .replace(/\\\//g, "/")
      .replace(/&amp;/g, "&");
    urls.add(u);
  }
  const codeRe = propertyCode
    ? new RegExp(propertyCode.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i")
    : null;
  const nameRe = propertyName
    ? new RegExp(
        propertyName
          .replace(/[.*+?^${}()|[\]\\]/g, "\\$&")
          .replace(/\s+/g, "[-\\s]+"),
        "i"
      )
    : null;

  const byKey = new Map();
  for (const raw of urls) {
    if (!/\.(jpe?g|png|webp)/i.test(raw)) continue;
    if (/logo|icon|svg|sprite|favicon|bookend/i.test(raw)) continue;
    // Prefer property-tagged assets when code/name provided
    if (codeRe || nameRe) {
      const ok = (codeRe && codeRe.test(raw)) || (nameRe && nameRe.test(raw));
      if (!ok) continue;
    }
    const key = raw
      .split("?")[0]
      .replace(/\.(16x9|4x3|1x1)\.jpe?g$/i, "")
      .toLowerCase();
    const w = Number((raw.match(/imwidth=(\d+)/i) || [])[1] || 0);
    const normalized = raw.includes("imwidth=")
      ? raw.replace(/imwidth=\d+/i, "imwidth=1280")
      : `${raw}${raw.includes("?") ? "&" : "?"}imwidth=1280`;
    const prev = byKey.get(key);
    if (!prev || w >= prev.w) {
      byKey.set(key, {
        imageUrl: normalized,
        w,
        role: guessRole(raw),
      });
    }
  }
  return [...byKey.values()].map(({ imageUrl, role }) => ({ imageUrl, role }));
}

function guessRole(u) {
  if (/lobby|reception|arrival|porte/i.test(u)) return "public_space_lobby";
  if (/exterior|facade|aerial|heliport|building/i.test(u)) return "exterior_arrival";
  if (/pool|spa|wellness/i.test(u)) return "wellness_pool_spa";
  if (/dining|restaurant|breakfast|bar|kitchen|market|teppan|rulfo|atelier|fb|food/i.test(u))
    return "food_beverage";
  if (/meeting|ballroom|business|conference|boardroom/i.test(u)) return "meeting_space";
  if (/fitness|gym|amenity/i.test(u)) return "amenity";
  if (/guest|room|bed|suite|bathroom/i.test(u)) return "guest_room";
  return "property";
}
