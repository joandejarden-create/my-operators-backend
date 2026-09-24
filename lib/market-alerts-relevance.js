/**
 * Relevance gate for Market Alerts RSS items.
 * Drops travel-adjacent noise that is not useful hotel deal / market signal.
 * Pre-Opening Capture V1: hard consumer-fluff reject + GM carve-out for pre-opening only.
 */
import { assessContentQuality, isPreOpeningLeadershipNews } from "./market-alerts-content-quality.js";
import { assessConsumerFluff } from "./market-alerts-consumer-fluff.js";

const IRRELEVANT_RE = [
  // Airlines / aviation (unless material hotel-demand implication — checked via fluff module)
  /\b(airline|airlines|air canada|air india|american airlines|delta air|united airlines|easyjet|aeroplan|aviation forum|airport.?to.?spaceport|c-suite overhaul)\b/i,
  // Pure F&B / nightlife openings (not hotel supply)
  /\b(restaurant|cafe|café|bar|nightclub|drunk dracula|food hall)\b.{0,40}\b(opening|opens|open at)\b/i,
  /\b(opening|opens)\b.{0,40}\b(restaurant|cafe|café|bar|nightclub)\b/i,
  // People moves / appointments — pre-opening leadership carved out below
  /\b(appoints?|appointed|names? as|named as)\b.{0,80}\b(director of food(?:\s*&\s*|\s+and\s+)?beverage|director of f\s*&\s*b|executive chef|(?:head|group)\s+chef|director of housekeeping|director of rooms|director of front office|front office manager|director of sales|revenue manager|spa director|director of human resources|sales manager|marketing manager)\b/i,
  /\b(general manager|front office manager|director of (?:operations|services))\b.{0,40}\b(appoint|join|takes? (?:over|charge))\b/i,
  // Evergreen SEO / vendor how-tos
  /\bwhat is a hotel reservation system\b/i,
  /\bchatgpt ads for hotels\b/i,
  /\b\d+\s+best (?:systems|pms|channel managers)\b/i,
  // Vendor / proptech product adoption (not hotel deal signal)
  /\b(?:use|uses|using|deploys?|deployments?|powered by)\b.{0,40}\b(?:agilysys|oracle opera|mews|cloudbeds|guestline)\b/i,
  /\b(?:pms|property management system|golf\s+tee\s+sheet|tee\s+sheet)\b.{0,40}\b(?:deploy|adoption|customer|install)/i,
  /\blaunches?\b.{0,60}\b(?:revenue\s+agent|ai\s+technologies|autonomous\s+decision)\b/i,
  /\blaunches?\b.{0,80}\b(?:net\s+zero\s+hotels?\s+program|verified\s+net\s+zero)\b/i,
  /\b(?:siteminder|lighthouse)\b.{0,60}\b(?:traveler\s+demands|revenue\s+agent|material\s+consequences)\b/i,
  // Amenity / F&B / club openings inside an existing hotel (not new supply)
  /\b(?:social\s+club|club|bar|restaurant|spa|venue)\s+opens?\s+at\b/i,
  /\bopens?\s+at\s+(?:the\s+)?[\w\s]{0,40}(?:hotel|resort|inn)\b.{0,80}\b(?:amenit|venue|socializing|entertainment|bar|restaurant)\b/i,
  // Promo / experiential marketing (not openings)
  /\b(?:racquet\s+cruiser|hospitality\s+on\s+the\s+road|complimentary\s+motorcoach)\b/i,
  /\bopen\s+hotel\s+commerce\b/i,
  // Training / webcast / registration fluff
  /\b(?:webcast|staff\s+training|complimentary\s+registration)\b/i,
  // Pure opinion / thought-leadership AI essays (no project or market move)
  /\b(?:can ai make|the hotel worker inside your ai|ai (?:in|for) hospitality\b.{0,40}(?:authentic|essay|opinion|argument))\b/i,
  /\b(?:argues that ai|thought leadership|op[- ]?ed)\b/i,
  // Brand marketing / personalization fluff (no deal, opening, or demand metric)
  /\bcourts?\s+travell?ers?\b/i,
  /\bpersonalised\s+luxury\b/i,
  /\bowner[- ]operated\s+approach\b.{0,60}\b(?:draw|travell?ers?|guests?)\b/i,
  // Corporate travel agency leadership reshuffles (not hotel deals)
  /\b(?:bcd travel|bcd announces).{0,60}\b(?:leadership|c-suite|appointments?)\b/i,
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
  const title = String(item.title || "").trim();
  const summary = String(item.summary || "").trim();
  const source = String(item.source || item.sourceName || "").trim();
  const text = `${title} ${summary}`;

  if (!title) return { keep: false, reason: "missing_title" };

  // Hotel performance / demand metrics always survive relevance (not listicles).
  const hotelPerformanceKeep =
    /\b(?:revpar|occupancy|adr\b|hotel\s+performance|lodging\s+performance|hotel\s+results?|booking\s+pace|room\s+rates?)\b/i.test(
      text
    );

  const fluff = assessConsumerFluff({ title, summary });
  if (fluff.reject && !hotelPerformanceKeep) {
    return { keep: false, reason: fluff.reason };
  }

  const contentQuality = assessContentQuality({ title, summary });
  if (contentQuality.ignore) {
    return { keep: false, reason: contentQuality.reason || "content_quality" };
  }

  for (const re of IRRELEVANT_RE) {
    if (re.test(text)) {
      // Pre-opening leadership carve-out for GM / DOSM / revenue appointment patterns.
      if (
        isPreOpeningLeadershipNews(title, summary) &&
        /general manager|director of sales|revenue manager|director of revenue|gm\b/i.test(re.source)
      ) {
        continue;
      }
      if (
        isPreOpeningLeadershipNews(title, summary) &&
        /\b(general manager|director of sales|revenue)\b/i.test(text)
      ) {
        continue;
      }

      // Allow airline-adjacent only if hotel deal language is dominant in title
      // OR material business implication already cleared fluff gate.
      if (
        /airline|aviation|aeroplan|air canada|air india|american airlines|delta air|easyjet/i.test(text) &&
        STRONG_KEEP_RE.test(title) &&
        HOTEL_DEAL_RE.test(title)
      ) {
        continue;
      }
      // Material hotel-demand airline/cruise stories already passed fluff; skip legacy airline drop.
      if (
        /airline|aviation|airlift|cruise terminal/i.test(text) &&
        /\b(hotel demand|room supply|additional hotel rooms|visitors?|materially)\b/i.test(text)
      ) {
        continue;
      }
      return { keep: false, reason: `noise:${re.source.slice(0, 40)}` };
    }
  }

  // Google News is high-recall / high-noise — require hotel signal in the title.
  if (/\bgoogle news\b/i.test(source)) {
    if (!STRONG_KEEP_RE.test(title) && !/\b(hotel|resort|hoteleiro|hotelero|proyecto hotel|projeto hotel)\b/i.test(title)) {
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
