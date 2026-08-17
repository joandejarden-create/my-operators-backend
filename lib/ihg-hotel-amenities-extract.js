/**
 * Extract amenity labels from official IHG hoteldetail HTML only (no invention).
 */

/**
 * @param {string} html
 * @returns {string[]}
 */
export function extractIhgAmenitiesFromHtml(html) {
  /** @type {string[]} */
  const labels = [];
  const seen = new Set();

  const push = (raw) => {
    const s = String(raw || "")
      .replace(/<[^>]+>/g, " ")
      .replace(/\s+/g, " ")
      .replace(/\u200b/g, "")
      .trim();
    if (!s || s.length < 2 || s.length > 100) return;
    const key = s.toLowerCase();
    if (seen.has(key)) return;
    seen.add(key);
    labels.push(s);
  };

  // Highlight amenities (server-rendered on hoteldetail)
  for (const m of html.matchAll(/<span class="amenity-title">([^<]+)<\/span>/gi)) {
    push(m[1]);
  }
  // Legacy / alternate list item class mentioned in prior audits
  for (const m of html.matchAll(/class="[^"]*amenity-list__item[^"]*"[^>]*>([\s\S]*?)<\/(?:li|div|span)>/gi)) {
    push(m[1]);
  }
  // img alt inside highlight items as fallback when title span missing
  for (const m of html.matchAll(
    /<li class="[^"]*vx-highlight-item[^"]*"[^>]*>[\s\S]*?<img[^>]*alt="([^"]+)"[\s\S]*?<\/li>/gi
  )) {
    push(m[1]);
  }

  // JSON-LD amenityFeature
  for (const block of html.matchAll(/<script type="application\/ld\+json">([\s\S]*?)<\/script>/gi)) {
    try {
      const json = JSON.parse(block[1]);
      const arr = Array.isArray(json) ? json : [json];
      for (const obj of arr) {
        const feats = obj?.amenityFeature;
        const list = Array.isArray(feats) ? feats : feats ? [feats] : [];
        for (const f of list) {
          if (f?.name) push(f.name);
        }
      }
    } catch {
      /* skip invalid JSON-LD */
    }
  }

  labels.sort((a, b) => a.localeCompare(b));
  return labels;
}

/**
 * Format for census Amenities multilineText (semicolon-separated).
 * @param {string[]} labels
 */
export function formatIhgAmenitiesText(labels) {
  return (labels || []).map((s) => String(s).trim()).filter(Boolean).join("; ");
}

/**
 * Heuristic: page is a bot interstitial rather than hotel content.
 * @param {string} html
 * @param {string} finalUrl
 */
export function ihgHoteldetailLooksBlocked(html, finalUrl) {
  if (/\/explore\/?$/i.test(String(finalUrl || ""))) return true;
  if (/access denied|attention required|please enable javascript/i.test(html)) return true;
  // Real hoteldetail pages include hotel-amenities or amenity-title
  if (/amenity-title|hotel-amenities|cmp-card__title/i.test(html)) return false;
  if (/captcha|akamai/i.test(html) && html.length < 50000) return true;
  return false;
}
