/**
 * Opening-announcement / secondary corroboration for Pipeline→Open (V1.1 follow-on).
 * Secondary press alone never upgrades to High-confidence material update.
 */

import { fetchText } from "./adapters/adapter-utils.js";

export const OPENING_CORROBORATION_CONFIG = Object.freeze({
  version: "opening-corroboration-v1",
  /** Domains treated as official brand/parent announcement sources */
  officialDomains: [
    "ihg.com",
    "ihgplc.com",
    "newsroom.ihg.com",
    "marriott.com",
    "news.marriott.com",
    "choicehotels.com",
    "investor.choicehotels.com",
    "hilton.com",
    "newsroom.hilton.com",
    "minorhotels.com",
    "avanihotels.com",
  ],
  tradePressDomains: [
    "hotelnewsresource.com",
    "hospitalitynet.org",
    "hotelmanagement.net",
    "costar.com",
    "businesswire.com",
    "prnewswire.com",
  ],
});

/**
 * @param {string} url
 */
export function classifyEvidenceDomain(url) {
  const host = String(url || "")
    .toLowerCase()
    .replace(/^https?:\/\//, "")
    .split("/")[0]
    .replace(/^www\./, "");
  if (OPENING_CORROBORATION_CONFIG.officialDomains.some((d) => host === d || host.endsWith(`.${d}`))) {
    return "official_opening_announcement";
  }
  if (OPENING_CORROBORATION_CONFIG.tradePressDomains.some((d) => host === d || host.endsWith(`.${d}`))) {
    return "reputable_trade_press";
  }
  return "secondary";
}

/**
 * Probe whether an official hoteldetail / property page mentions opening language.
 * Does not scrape search engines — uses already-known official URL only.
 * @param {object} observation - adapter observation from checkHotelFreshness
 * @param {{ currentStatus: string }} hotel
 */
export async function assessOpeningCorroborationFromOfficialPage(observation, hotel) {
  const url = observation?.officialUrl;
  if (!url || !observation.hotelFound) {
    return {
      upgraded: false,
      band: "Insufficient Evidence",
      signals: [],
      reason: "No official property URL to corroborate opening",
    };
  }

  const signals = [];
  if (observation.bookable || observation.rawSignals?.hasBookNow) signals.push("bookable_primary");
  if (observation.rawSignals?.newHotelBanner) signals.push("new_hotel_banner");

  let html = "";
  try {
    const page = await fetchText(url);
    html = page.text || "";
  } catch (err) {
    return {
      upgraded: false,
      band: "Medium",
      signals,
      reason: `Could not re-fetch official page: ${err?.message || err}`,
    };
  }

  const openingLang =
    /now open|newly opened|grand opening|opened (on|in)|opening date|welcoming guests|accepting reservations/i.test(
      html
    );
  if (openingLang) signals.push("opening_language_on_official_page");

  const hasOpenDate = /"(openDate|openingDate)"\s*:\s*"([^"]+)"/i.exec(html);
  if (hasOpenDate) signals.push(`open_date:${hasOpenDate[2]}`);

  const domainType = classifyEvidenceDomain(url);
  const officialSecondary = domainType === "official_opening_announcement" || /hoteldetail|\/hotels\//i.test(url);

  // Upgrade Medium single-primary → High only with official opening language OR new-hotel + bookable already dual
  if (
    hotel.currentStatus === "Pipeline" &&
    /open/i.test(String(observation.operatingStatus || "")) &&
    signals.includes("bookable_primary") &&
    (signals.includes("opening_language_on_official_page") || signals.includes("new_hotel_banner"))
  ) {
    return {
      upgraded: true,
      band: "High",
      signals,
      reason: "Official property page bookable + opening corroboration language/banner",
      sourceType: officialSecondary ? "official_opening_announcement" : observation.sourceType,
      evidenceUrl: url,
    };
  }

  if (signals.includes("bookable_primary") && !signals.includes("opening_language_on_official_page")) {
    return {
      upgraded: false,
      band: "Medium",
      signals,
      reason: "Single primary bookable source — trade press alone would not upgrade; needs official announcement",
      evidenceUrl: url,
    };
  }

  return {
    upgraded: false,
    band: "Low",
    signals,
    reason: "Insufficient opening corroboration",
    evidenceUrl: url,
  };
}

/**
 * Rule: secondary/trade press alone cannot produce High material update.
 * @param {{ primaryOk: boolean, secondaryType: string }} input
 */
export function canUpgradeToHigh(input) {
  if (!input.primaryOk) return false;
  if (input.secondaryType === "reputable_trade_press" || input.secondaryType === "secondary") return false;
  return (
    input.secondaryType === "official_opening_announcement" ||
    input.secondaryType === "official_parent_page" ||
    input.secondaryType === "official_hotel_website" ||
    input.secondaryType === "official_brand_directory"
  );
}
