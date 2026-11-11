/**
 * Normalize Apify dataset items → GDI discovery candidates (pre-ORS).
 * V7.1: typed organizer extraction; Knot name de-duplication; nested field support.
 */

function text(v) {
  if (v == null) return "";
  if (typeof v === "object") return "";
  return String(v).replace(/\s+/g, " ").trim();
}

function first(...vals) {
  for (const v of vals) {
    const t = text(v);
    if (t && t !== "[object Object]") return t;
  }
  return "";
}

/**
 * Extract organizer/organization fields from nested or flat Actor rows.
 * Never String(object) — that produced Stage 1 `[object Object]`.
 */
export function extractOrganizerFields(item = {}) {
  const orgObj =
    item.organizer && typeof item.organizer === "object" && !Array.isArray(item.organizer)
      ? item.organizer
      : null;

  const name = first(
    orgObj?.name,
    item["organizer.name"],
    item.organizerName,
    typeof item.organizer === "string" ? item.organizer : null,
    item.groupName,
    item.companyName,
    item.vendorName,
    item.headline,
  );

  const id = first(
    orgObj?.id,
    item["organizer.id"],
    item.organizerId,
    item.vendorId,
    item.displayId,
  );

  const url = first(
    orgObj?.url,
    item["organizer.url"],
    orgObj?.websiteUrl,
    item["organizer.websiteUrl"],
    item.organizerUrl,
    item.organizationUrl,
    item.profileUrl,
    item.website,
  );

  const websiteUrl = first(
    orgObj?.websiteUrl,
    item["organizer.websiteUrl"],
    item.website,
  );

  return {
    organizationName: name || null,
    organizerId: id || null,
    organizerUrl: url || null,
    organizerWebsiteUrl: websiteUrl || null,
    rawOrganizer:
      orgObj ||
      (item["organizer.name"]
        ? {
            name: item["organizer.name"],
            id: item["organizer.id"] || null,
            url: item["organizer.url"] || null,
            websiteUrl: item["organizer.websiteUrl"] || null,
          }
        : null),
  };
}

/**
 * Knot Actor sometimes concatenates name twice ("Ten Events GroupTen Events Group").
 */
export function dedupeRepeatedDisplayName(name) {
  const t = text(name);
  if (!t || t.length < 4) return t;
  const mid = Math.floor(t.length / 2);
  const a = t.slice(0, mid);
  const b = t.slice(mid);
  if (a === b) return a;
  // also handle odd-length near-duplicates
  for (let i = Math.floor(t.length / 3); i <= mid; i++) {
    if (t.slice(0, i) === t.slice(i, i * 2) && t.slice(i * 2) === "") return t.slice(0, i);
  }
  return t;
}

function extractVenueFields(item = {}) {
  const venueObj =
    item.venue && typeof item.venue === "object" && !Array.isArray(item.venue)
      ? item.venue
      : null;
  return {
    venueName: first(
      venueObj?.name,
      item["venue.name"],
      item.venueName,
      typeof item.venue === "string" ? item.venue : null,
      item.locationName,
      item.address,
    ),
    city: first(
      venueObj?.city,
      item["venue.city"],
      item.city,
      item.locationQuery,
    ),
    region: first(venueObj?.region, item["venue.region"], item.region, item.state),
  };
}

function isThin(item, title) {
  const url = first(item.url, item.eventUrl, item.website, item.link, item.profileUrl);
  return !title || title.length < 4;
}

function looksLikeHotelMarketing(item) {
  const blob = `${item.title || ""} ${item.name || ""} ${item.url || ""} ${item.website || ""}`.toLowerCase();
  return /\b(hilton\.com|marriott\.com|hyatt\.com|ihg\.com|book.?now|hotel.?deals)\b/.test(blob);
}

function classifyRecordKind(item, actorId) {
  const id = String(actorId || "");
  if (id.includes("wedding") || id.includes("knot") || id.includes("weddingwire")) {
    return "VENDOR_OR_VENUE_LISTING";
  }
  if (id.includes("google-places") || id.includes("crawler-google-places")) {
    return "LOCAL_BUSINESS_LISTING";
  }
  if (id.includes("eventbrite") || id.includes("meetup") || id.includes("facebook-events") || id.includes("espn")) {
    return "PUBLISHED_EVENT";
  }
  if (id.includes("website-content")) return "WEB_PAGE_EXTRACTION";
  if (id.includes("google-search")) return "SEARCH_RESULT";
  return "GENERIC_RECORD";
}

/**
 * @returns {{ candidates: object[], rejected: object[] }}
 */
export function normalizeApifyDatasetItems(items = [], ctx = {}) {
  const candidates = [];
  const rejected = [];
  const actorId = ctx.actorId;

  for (const [idx, raw] of (items || []).entries()) {
    const item = raw && typeof raw === "object" ? raw : {};
    const kind = classifyRecordKind(item, actorId);
    const rawTitle = first(item.title, item.name, item.eventName, item.placeName, item.vendorName);
    const title = dedupeRepeatedDisplayName(rawTitle);
    const url = first(item.url, item.eventUrl, item.website, item.link, item.profileUrl);
    const startDate = first(item.startDate, item.start, item.dateFrom, item.eventStartDate, item.startsAt, item["dates.startDate"]);
    const endDate = first(item.endDate, item.end, item.dateTo, item.eventEndDate, item.endsAt, item["dates.endDate"]);
    const orgFields = extractOrganizerFields(item);
    const venueFields = extractVenueFields(item);
    const city = first(venueFields.city, ctx.hotel?.city);

    // Prefer headline for Knot when name was mangled
    const displayTitle =
      kind === "VENDOR_OR_VENUE_LISTING"
        ? first(item.headline, title, orgFields.organizationName) || title
        : title;

    if (isThin(item, displayTitle)) {
      rejected.push({ reason: "THIN_OR_INCOMPLETE", index: idx, title: displayTitle, url });
      continue;
    }
    if (looksLikeHotelMarketing(item)) {
      rejected.push({ reason: "HOTEL_MARKETING_PAGE", index: idx, title: displayTitle, url });
      continue;
    }

    const org =
      orgFields.organizationName ||
      (kind === "VENDOR_OR_VENUE_LISTING" ? displayTitle : null);

    const provenance = {
      provider: "apify",
      actorId,
      runId: ctx.runId || null,
      sourceUrl: url || null,
      retrievedAt: new Date().toISOString(),
      researchObjective: ctx.researchObjective || null,
      organizerId: orgFields.organizerId,
      organizerUrl: orgFields.organizerUrl,
    };

    const relationshipOnly =
      kind === "VENDOR_OR_VENUE_LISTING" || kind === "LOCAL_BUSINESS_LISTING";

    candidates.push({
      discoveryProvider: "apify",
      recordKind: kind,
      title: displayTitle,
      organizationName: org || null,
      organizerId: orgFields.organizerId,
      organizerUrl: orgFields.organizerUrl,
      organizerWebsiteUrl: orgFields.organizerWebsiteUrl,
      venueName: venueFields.venueName || null,
      city: city || null,
      region: venueFields.region || null,
      startDate: startDate || null,
      endDate: endDate || null,
      sourceUrl: url || null,
      category: first(item.category, item.subcategory, item.venueType) || null,
      summary: first(item.summary, item.description, item.headline) || null,
      relationshipTrackOnly: relationshipOnly,
      timingStatus: startDate
        ? "CANDIDATE_DATED_UNVERIFIED"
        : relationshipOnly
          ? "TIMING_UNCONFIRMED_RELATIONSHIP"
          : "TIMING_UNCONFIRMED",
      commercialScreen: "PENDING_ORS",
      accessibilityClass: null,
      dedupeKey: buildDedupeKey({
        title: displayTitle,
        org,
        startDate,
        url,
        city,
      }),
      provenance,
      rawRef: { index: idx },
      rawOrganizerPreserved: orgFields.rawOrganizer,
    });
  }

  return { candidates, rejected };
}

export function buildDedupeKey({ title, org, startDate, url, city }) {
  const norm = (s) =>
    text(s)
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, " ")
      .trim();
  return [norm(org) || norm(title), norm(startDate).slice(0, 10), norm(city), norm(url).slice(0, 80)]
    .filter(Boolean)
    .join("|");
}
