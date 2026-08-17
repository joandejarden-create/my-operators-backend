/**
 * Stale-result detection for Early Signal discovery (deterministic).
 */

const MONTHS =
  "january|february|march|april|may|june|july|august|september|october|november|december";

/**
 * @param {{ title?: string, summary?: string, pubDate?: string|null, now?: Date }} input
 * @returns {{ stale: boolean, reason: string|null }}
 */
export function detectStaleEarlySignal(input = {}) {
  const title = String(input.title || "");
  const summary = String(input.summary || "");
  const text = `${title} ${summary}`;
  const now = input.now || new Date();
  const year = now.getFullYear();

  const openingYear =
    text.match(new RegExp(`\\b(?:${MONTHS})\\s+(20\\d{2})\\s+opening\\b`, "i")) ||
    title.match(/\bplanning\s+(?:january|february|march|april|may|june|july|august|september|october|november|december)\s+(20\d{2})\s+opening\b/i) ||
    title.match(/\b(20(?:0\d|1[0-9]|2[0-2]))\s+opening\b/i);

  if (openingYear) {
    const y = parseInt(openingYear[1], 10);
    if (Number.isFinite(y) && y <= year - 2) {
      return { stale: true, reason: "stale:historical_opening_year" };
    }
  }

  if (input.pubDate) {
    const t = new Date(input.pubDate).getTime();
    if (Number.isFinite(t)) {
      const ageDays = (now.getTime() - t) / (24 * 60 * 60 * 1000);
      if (ageDays > 400) {
        return { stale: true, reason: "stale:pubdate" };
      }
    }
  }

  return { stale: false, reason: null };
}
