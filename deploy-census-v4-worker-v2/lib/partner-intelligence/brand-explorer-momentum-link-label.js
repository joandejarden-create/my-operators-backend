/**
 * Brand Explorer momentum source URL classification + link label rules.
 * Shared by v31M-R2 writer audits and documented parity with frontend rendering.
 */

function nz(v) {
  return v == null ? "" : String(v).trim();
}

const PR_URL_RE =
  /newsroom|press-release|press_release|\/news\/|media\.choicehotels\.com|ihgplc\.com\/news/i;

const WEAK_SOURCE_RE = /\b(census|dealality|internal|localhost|airtable\.com)\b/i;

const CHOICE_PROPERTY_LISTING_RE =
  /choicehotels\.com\/[^/]+\/([^/]+)\/radisson-individuals-hotels\//i;

export { CHOICE_PROPERTY_LISTING_RE };

export function isChoicePropertyListingUrl(url) {
  return CHOICE_PROPERTY_LISTING_RE.test(nz(url));
}

export function isMomentumInappropriatePropertyListing(url) {
  return isChoicePropertyListingUrl(url);
}

/** Evidence-source hierarchy rank (higher = stronger for Recent Momentum). */
export function momentumEvidenceSourceRank(url) {
  const u = nz(url).toLowerCase();
  if (!u) return 0;
  if (/newsroom|press-release|press_release|\/news\//.test(u) && !u.includes("press-kit")) return 100;
  if (u.includes("press-kit") || u.includes("press_kit")) return 85;
  if (/hotelbusiness\.com|hotelmanagement\.net|lodgingmagazine\.com|insights\.ehotelier\.com/.test(u)) {
    return 75;
  }
  if (/choicehotelsdevelopment\.com/.test(u)) return 70;
  if (/travelweekly|hotel-online|journaldespalaces|prnewswire|globenewswire/.test(u)) return 65;
  if (u.includes("media.choicehotels.com")) return 60;
  if (isChoicePropertyListingUrl(url)) return 10;
  if (/choicehotels\.com/.test(u)) return 20;
  return 30;
}

export function followsTributeMomentumRules(url) {
  if (!nz(url)) return { ok: false, reason: "missing_source_url" };
  if (isMomentumInappropriatePropertyListing(url)) {
    return { ok: false, reason: "property_listing_not_momentum_evidence" };
  }
  if (momentumEvidenceSourceRank(url) >= 60) {
    return { ok: true, reason: "event_supporting_evidence_source" };
  }
  return { ok: false, reason: "weak_or_generic_source" };
}

export function parseMomentumPresentationBody(body, title = "") {
  const raw = nz(body);
  let paras = raw
    .split(/\n\n+/)
    .map((s) => s.trim())
    .filter(Boolean);

  if (paras.length <= 1) {
    const lines = raw
      .split(/\n/)
      .map((s) => s.trim())
      .filter(Boolean);
    if (lines.length > 1) {
      paras = lines;
    } else {
      const leadingDate = raw.match(/^(\d{4}(?:\s*[–—-]\s*\d{4})?)\s+([\s\S]+)$/);
      if (leadingDate) {
        const rest = leadingDate[2].trim();
        const trailingUrl = rest.match(/^(.*?)\s+(https?:\/\/\S+)\s*$/i);
        paras = trailingUrl
          ? [leadingDate[1], trailingUrl[1].trim(), trailingUrl[2].trim()].filter(Boolean)
          : [leadingDate[1], rest];
      }
    }
  }

  const dateLine = paras[0] || "";
  let sourceUrl = "";
  const descParts = [];
  for (let i = 1; i < paras.length; i++) {
    if (/^https?:\/\//i.test(paras[i])) sourceUrl = paras[i];
    else descParts.push(paras[i]);
  }
  return {
    dateLine,
    headline: nz(title),
    description: descParts.join("\n\n"),
    sourceUrl,
  };
}

function titleCaseWords(text) {
  return nz(text)
    .split(/\s+/)
    .filter(Boolean)
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase())
    .join(" ");
}

/** Title-case link label tail after "View " (keeps Choice Hotels, IHG, etc.). */
function properCaseLinkTail(text) {
  return titleCaseWords(text);
}

function viewLinkLabel(tail) {
  return `View ${properCaseLinkTail(tail)}`;
}

/** Extract market/city label from Choice property listing URL path segment. */
export function choiceHotelsPropertyMarketFromUrl(url) {
  const m = nz(url).match(CHOICE_PROPERTY_LISTING_RE);
  if (!m) return "";
  return titleCaseWords(m[1].replace(/-/g, " "));
}

export function classifyMomentumSourceType(url) {
  const u = nz(url).toLowerCase();
  if (!u) {
    return { sourceType: "missing", category: "missing", isOfficial: false, isWeak: true };
  }
  if (WEAK_SOURCE_RE.test(u) || /airtableusercontent\.com/i.test(u)) {
    return { sourceType: "unsupported", category: "temporary_or_internal", isOfficial: false, isWeak: true };
  }
  if (/hotelbusiness\.com/.test(u)) {
    return { sourceType: "credible_trade_article", category: "credible_hospitality_trade", isOfficial: false, isWeak: false };
  }
  if (/hotelmanagement\.net/.test(u)) {
    return { sourceType: "credible_trade_article", category: "credible_hospitality_trade", isOfficial: false, isWeak: false };
  }
  if (/lodgingmagazine\.com/.test(u)) {
    return { sourceType: "credible_trade_article", category: "credible_hospitality_trade", isOfficial: false, isWeak: false };
  }
  if (/insights\.ehotelier\.com/.test(u)) {
    return { sourceType: "credible_trade_article", category: "credible_hospitality_trade", isOfficial: false, isWeak: false };
  }
  if (/choicehotelsdevelopment\.com/.test(u)) {
    return { sourceType: "choice_development_news", category: "official_development_news", isOfficial: true, isWeak: false };
  }
  if (u.includes("press-kit") || u.includes("press_kit")) {
    return { sourceType: "official_press_kit", category: "official_brand_press_kit", isOfficial: true, isWeak: false };
  }
  if (CHOICE_PROPERTY_LISTING_RE.test(u)) {
    return { sourceType: "property_listing", category: "official_property_listing", isOfficial: true, isWeak: false };
  }
  if (PR_URL_RE.test(u) && (u.includes("/news") || u.includes("press-release") || u.includes("newsroom"))) {
    return { sourceType: "press_release", category: "official_press_release", isOfficial: true, isWeak: false };
  }
  if (u.includes("media.choicehotels.com")) {
    return { sourceType: "official_announcement_hub", category: "official_company_media", isOfficial: true, isWeak: false };
  }
  if (/marriott\.com/.test(u) && /\/hotels\//.test(u)) {
    return { sourceType: "marriott_property_page", category: "official_property_page", isOfficial: true, isWeak: false };
  }
  if (/prnewswire|globenewswire|businesswire|hotel-online|journaldespalaces|travelweekly/i.test(u)) {
    return { sourceType: "third_party_news", category: "third_party_news", isOfficial: false, isWeak: false };
  }
  if (/marriott\.com\/newsroom|marriott\.pressarea/i.test(u)) {
    return { sourceType: "brand_newsroom", category: "official_brand_newsroom", isOfficial: true, isWeak: false };
  }
  if (u.includes("choicehotels.com")) {
    return { sourceType: "choice_other", category: "official_company_page", isOfficial: true, isWeak: false };
  }
  return { sourceType: "other", category: "other", isOfficial: false, isWeak: true };
}

/**
 * Tribute-parity momentum link label from URL (v31M-R2 rules).
 * Differentiates press kit, property listings, press releases, and trade news.
 */
export function momentumLinkLabelForUrl(url, brand = {}) {
  const u = nz(url).toLowerCase();
  if (!u) return "View source";

  if (u.includes("marriott.com") && u.includes("/hotels/")) {
    return viewLinkLabel("property");
  }
  if (u.includes("designhotels.com") && u.includes("/hotels/")) {
    const market = nz(url)
      .match(/designhotels\.com\/hotels\/[^/]+\/([^/]+)\//i)?.[1]
      ?.replace(/-/g, " ");
    if (market) return `View ${titleCaseWords(market)} Property Listing`;
    return viewLinkLabel("Design Hotels Property Listing");
  }
  if (u.includes("tribute-portfolio.marriott.com")) {
    return viewLinkLabel("Tribute Portfolio Site");
  }
  if (/hotel-online\.com\/press/i.test(u)) {
    return viewLinkLabel("Owner Announcement");
  }
  if (
    /travelweekly|traveldailynews|hotel-online|journaldespalaces|hotelmanagement-network|travelprnews|breakingtravelnews|hotelnewsresource|newsismybusiness|ladevi|ithic|semana/i.test(
      u
    )
  ) {
    return viewLinkLabel("Article");
  }
  if (/marriott\.com\/newsroom|marriott\.pressarea|marriott\.africa-newsroom/i.test(u)) {
    return viewLinkLabel("Marriott Announcement");
  }
  if (/prnewswire|globenewswire|businesswire/i.test(u) && /marriott|tribute[-_]portfolio/i.test(u)) {
    return viewLinkLabel("Marriott Announcement");
  }
  if (u.includes("ihgplc.com") || u.includes("ihg.com") || u.includes("kimptonhotels.com")) {
    return "View IHG Announcement";
  }

  if (/hotelbusiness\.com/i.test(u)) {
    return viewLinkLabel("Hotel Business Article");
  }
  if (/hotelmanagement\.net/i.test(u)) {
    return viewLinkLabel("Hotel Management Article");
  }
  if (/lodgingmagazine\.com/i.test(u)) {
    return viewLinkLabel("Lodging Article");
  }
  if (u.includes("einpresswire.com")) {
    return viewLinkLabel("Press Release");
  }
  if (u.includes("insights.ehotelier.com")) {
    return "View eHotelier Article";
  }
  if (u.includes("investor.choicehotels.com") || u.includes("media.choicehotels.com")) {
    return viewLinkLabel("Choice Hotels Press Release");
  }
  if (u.includes("press.accor.com")) {
    return viewLinkLabel("Accor Announcement");
  }
  if (u.includes("travelpulse.com")) {
    return viewLinkLabel("Article");
  }
  if (/choicehotelsdevelopment\.com/i.test(u)) {
    return viewLinkLabel("Choice Development News");
  }

  if (u.includes("press-kit") || u.includes("press_kit")) {
    return viewLinkLabel("Choice Hotels Press Kit");
  }
  if (CHOICE_PROPERTY_LISTING_RE.test(u)) {
    const market = choiceHotelsPropertyMarketFromUrl(url);
    if (market) return `View ${market} Property Listing`;
    return viewLinkLabel("Property Listing");
  }
  if (u.includes("choicehotels.com") && (u.includes("/news") || u.includes("newsroom"))) {
    return viewLinkLabel("Choice Hotels Press Release");
  }
  if (u.includes("media.choicehotels.com")) {
    return viewLinkLabel("Choice Hotels Press Kit");
  }
  if (u.includes("choicehotels.com")) {
    return viewLinkLabel("Choice Hotels Source");
  }
  if (/prnewswire|globenewswire|businesswire/i.test(u)) {
    return viewLinkLabel("Press Release");
  }
  if (u.includes("marriott.com")) {
    return viewLinkLabel("Marriott Source");
  }

  const publisher = nz(brand.parentCompany || brand.name);
  if (publisher && PR_URL_RE.test(u)) {
    return `View ${properCaseLinkTail(publisher)} Announcement`;
  }
  return viewLinkLabel("Source");
}

/** Legacy frontend label (pre v31M-R2) — generic Choice announcement for all Choice URLs. */
export function legacyMomentumLinkLabel(url) {
  const u = nz(url).toLowerCase();
  if (u.includes("media.choicehotels.com") || u.includes("choicehotels.com")) {
    return "View Choice Hotels announcement";
  }
  if (u.includes("marriott.com") && u.includes("/hotels/")) {
    return "View property";
  }
  if (/prnewswire|globenewswire|businesswire/i.test(u)) {
    return "View press release";
  }
  return "View announcement";
}

export function momentumLinkLabelStorage() {
  return "generated_by_frontend_from_body_url";
}

export function buildMomentumBody({ dateLine, summary, sourceUrl }) {
  return [dateLine, summary, sourceUrl].filter(Boolean).join("\n\n");
}

export function allLabelsGeneric(labels) {
  const normalized = (labels || []).map((l) => nz(l).toLowerCase());
  if (!normalized.length) return true;
  const generic = normalized.filter((l) => l === "view choice hotels announcement");
  return generic.length === normalized.length;
}

export function labelsAreDifferentiated(labels) {
  const unique = new Set((labels || []).map((l) => nz(l).toLowerCase()).filter(Boolean));
  if (unique.size < 2) return false;
  return !allLabelsGeneric(labels);
}
