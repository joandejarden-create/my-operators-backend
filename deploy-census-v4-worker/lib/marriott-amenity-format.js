/**
 * Marriott overview amenities → census Amenities field (semicolon-separated, Hilton parity).
 */

/**
 * @param {string[]} labels Amenity labels in source page order.
 * @param {{ sort?: boolean }} [opts]
 * @returns {string}
 */
export function formatMarriottAmenitiesText(labels, opts = {}) {
  /** @type {string[]} */
  const unique = [];
  const seen = new Set();
  for (const raw of labels || []) {
    const label = String(raw || "")
      .replace(/\s+/g, " ")
      .trim();
    if (!label) continue;
    const key = label.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    unique.push(label);
  }
  if (opts.sort) unique.sort((a, b) => a.localeCompare(b));
  return unique.join("; ");
}

/**
 * @param {string} amenitiesText
 * @returns {string[]}
 */
export function parseMarriottAmenitiesText(amenitiesText) {
  return String(amenitiesText || "")
    .split(/[;\n]+/)
    .map((part) => part.trim())
    .filter(Boolean);
}
