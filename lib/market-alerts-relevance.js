/**
 * Relevance gate for Market Alerts RSS items.
 * Drops travel-adjacent noise that is not useful hotel deal / market signal.
 */
import { assessContentQuality } from "./market-alerts-content-quality.js";

const IRRELEVANT_RE = [
  // Airlines / aviation (unless title is clearly hotel-led — checked separately)
  /\b(airline|airlines|air canada|air india|american airlines|delta air|united airlines|easyjet|aeroplan|aviation forum|airport.?to.?spaceport|c-suite overhaul)\b/i,
  // Pure F&B / nightlife openings (not hotel supply)
  /\b(restaurant|cafe|café|bar|nightclub|drunk dracula|food hall)\b.{0,40}\b(opening|opens|open at)\b/i,
  /\b(opening|opens)\b.{0,40}\b(restaurant|cafe|café|bar|nightclub)\b/i,
  // People moves / appointments
  /\b(appoints?|appointed|names? as|named as)\b.{0,80}\b(director of food(?:\s*&\s*|\s+and\s+)?beverage|director of f\s*&\s*b|executive chef|(?:head|group)\s+chef|director of housekeeping|director of rooms|director of front office|front office manager|director of sales|revenue manager|spa director|director of human resources|sales manager|marketing manager)\b/i,
  /\b(general manager|front office manager|director of (?:operations|services))\b.{0,40}\b(appoint|join|takes? (?:over|charge))\b/i,
  // Evergreen SEO / vendor how-tos
  /\bwhat is a hotel reservation system\b/i,
  /\bchatgpt ads for hotels\b/i,
  /\b\d+\s+best (?:systems|pms|channel managers)\b/i,
  // Sponsored ops / energy content
  /\b(energy efficiency rebates|verdant thermostats|reduce energy costs without compromising)\b/i,
  // Award-nomination fluff with no market move
  /\bopens nominations for\b/i,
  /\bannouncing the return of .{0,40}forum\b/i,
  // Non-hotel real estate / community noise that still matches "opening|for sale"
  /\b(community center|commercial land|industrial land|vacant land|warehouse for sale)\b/i,
  /\breal estate market\b/i,
];

/** Keep if clearly a hotel transaction / opening / capital / brand move. */
const STRONG_KEEP_RE =
  /\b(hotels?|resorts?|inns?|suites|hospitality|lodging|marriott|hilton|hyatt|ihg|accor|radisson|wyndham|choice hotels|cambria|kimpton|sheraton|westin|four points|holiday inn|crowne plaza|intercontinental)\b/i;

const HOTEL_DEAL_RE =
  /\b(acquisition|acquires|acquired|buys?|bought|sold|sale|for sale|offered for|opening|opens|to open|pipeline|construction|financing|franchise|rebrand|revpar|occupancy|portfolio|signs?\b|signed)\b/i;

/**
 * @param {{ title?: string, summary?: string, source?: string, sourceName?: string }} item
 * @returns {{ keep: boolean, reason?: string }}
 */
export function assessMarketAlertRelevance(item) {
  const title = (item.title || "").trim();
  const summary = (item.summary || "").trim();
  const source = (item.source || item.sourceName || "").trim();
  const text = `${title} ${summary}`;

  if (!title) return { keep: false, reason: "missing_title" };

  const contentQuality = assessContentQuality({ title, summary });
  if (contentQuality.ignore) {
    return { keep: false, reason: contentQuality.reason || "content_quality" };
  }

  for (const re of IRRELEVANT_RE) {
    if (re.test(text)) {
      // Allow airline-adjacent only if hotel deal language is dominant in title.
      if (
        /airline|aviation|aeroplan|air canada|air india|american airlines|delta air|easyjet/i.test(text) &&
        STRONG_KEEP_RE.test(title) &&
        HOTEL_DEAL_RE.test(title)
      ) {
        continue;
      }
      return { keep: false, reason: `noise:${re.source.slice(0, 40)}` };
    }
  }

  // Google News is high-recall / high-noise — require hotel signal in the title.
  if (/\bgoogle news\b/i.test(source)) {
    if (!STRONG_KEEP_RE.test(title)) {
      return { keep: false, reason: "google_news_no_hotel_in_title" };
    }
  }

  // Skift can drift into pure travel/aviation — require hotel or lodging deal language.
  if (/\bskift\b/i.test(source)) {
    if (!STRONG_KEEP_RE.test(text) && !HOTEL_DEAL_RE.test(title)) {
      return { keep: false, reason: "weak_hotel_signal" };
    }
  }

  return { keep: true };
}

export function isMarketAlertRelevant(item) {
  return assessMarketAlertRelevance(item).keep;
}
