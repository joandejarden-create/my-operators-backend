/**
 * Buyer-first classification for GDI discovery results.
 * A query term is not event geography. News and legislatures are not group buyers.
 */

const NEWS_HOST = /excelsior\.com|eluniversal\.com|milenio\.com|oem\.com|sdpnoticias\.com|alcaldesdemexico\.com|elinsurgente|washingtonpost|nytimes|bbc\.com|cnn\.com|bizjournals\.com|wtop\.com|\/news\/|\/newsroom\/|\/blog\//i;
const WORKFORCE = /indeed\.com|glassdoor\.|ziprecruiter\.|roberthalf\.|linkedin\.com\/jobs|careers\.|\/jobs\/|\/careers\/|\/job\/|staffing|apprenticeship|workforce|recruitment|hospitality jobs/i;
const DIRECTORY_HOST = /eventbrite\.|peerspace\.com|cvent\.com\/venues|northstarmeetingsgroup|guiadehoy\.com|fxva\.com|clocate\.com|eventective\.com|10times\.com|nferias\.com|feverup\.com|cartelera\./i;
const VENUE_HOST = /liquidspace\.com|officeevolution\.com|meetingspace|conference-rooms|salones|instalaciones|recintos|\/venues\//i;
const LEGISLATURE = /congresocdmx|congreso\s*cdmx|congreso de la ciudad|asamblea legislativa|c[aá]mara de diputados|\bdiputados\b/i;
const ELSEWHERE = /\b(kalahari|canc[uú]n|monterrey|guadalajara|acapulco|orlando|las vegas|miami|chicago)\b/i;
const DEMAND_MECHANISM = /annual|anual|conference|convention|convenci[oó]n|meeting|asamblea|simposio|congreso|reuni[oó]n|symposium|jornada|session/i;

export function buyerSegmentForQuery(query = "") {
  const text = String(query).toLowerCase();
  if (/directorio|directory/.test(text)) return "organization_directory";
  if (/associat|asociaci[oó]n|c[aá]mara|colegio|sociedad/.test(text)) return "professional_trade_association";
  if (/univers|unam|acad[eé]m/.test(text)) return "academic";
  if (/m[eé]dic|simposio|hospital|cl[ií]nic/.test(text)) return "medical_scientific";
  if (/corporat|empresarial|offsite|capacitaci[oó]n/.test(text)) return "corporate";
  if (/federal|gobierno|instituci[oó]n/.test(text)) return "institutional";
  if (/deport|soccer|sport|f[uú]tbol/.test(text)) return "sports";
  if (/boda|wedding|social/.test(text)) return "social";
  if (/congreso|conference|convenc|reuni[oó]n|meeting/.test(text)) return "meeting_organizer";
  return "unspecified";
}

export function queryInvitesLegislature(query = "", language = "en") {
  const text = String(query).toLowerCase();
  return language === "es" && /\bcongreso\b/.test(text);
}

export function planBuyerQueries({ language = "en", territoryLabel = "", segments = [] } = {}) {
  const place = String(territoryLabel || "").trim();
  const wanted = new Set(segments.length ? segments : [
    "professional_trade_association",
    "organization_directory",
    "corporate",
    "medical_scientific",
    "academic",
    "institutional",
  ]);
  const spanish = [
    ["professional_trade_association", `asociación profesional ${place} reunión anual`],
    ["organization_directory", `directorio de asociaciones y cámaras empresariales ${place}`],
    ["corporate", `capacitación empresarial ${place} grupos`],
    ["medical_scientific", `sociedad médica ${place} simposio`],
    ["academic", `universidad ${place} reunión académica`],
    ["institutional", `institución pública ${place} reunión de trabajo`],
  ];
  const english = [
    ["professional_trade_association", `${place} professional association annual meeting`],
    ["organization_directory", `${place} professional association directory annual meeting`],
    ["corporate", `${place} corporate training meeting`],
    ["medical_scientific", `${place} medical society symposium`],
    ["academic", `${place} university academic conference`],
    ["institutional", `${place} agency meeting attendees`],
  ];
  const rows = language === "es" ? spanish : english;
  return rows
    .filter(([segment]) => wanted.has(segment))
    .map(([segment, query]) => ({
      segment,
      query: query.replace(/\s+/g, " ").trim(),
      language,
      legislatureRisk: queryInvitesLegislature(query, language),
    }));
}

export function classifySource(url = "", title = "") {
  const blob = `${url} ${title}`;
  if (WORKFORCE.test(blob)) return "workforce";
  if (NEWS_HOST.test(url)) return "news";
  if (LEGISLATURE.test(url) || LEGISLATURE.test(title)) return "legislature";
  if (DIRECTORY_HOST.test(url)) return "directory";
  if (VENUE_HOST.test(url) || /meeting room|conference room|event space|salones de eventos|recintos/i.test(title)) return "venue_listing";
  if (/\/category\/|archive/i.test(blob)) return "archive";
  if (/\.org\b|asociaci[oó]n|association|c[aá]mara|society/i.test(blob)) return "official_organization";
  return "other";
}

export function classifyResultGeography({ title = "", snippet = "", url = "", territoryTerms = [] } = {}) {
  const resultText = `${title} ${snippet} ${url}`.toLowerCase();
  const terms = territoryTerms.map((term) => String(term).toLowerCase()).filter(Boolean);
  const inResult = terms.some((term) => resultText.includes(term));
  if (ELSEWHERE.test(resultText) && !terms.some((term) => ELSEWHERE.test(term) && resultText.includes(term))) {
    return "OUT_OF_MARKET";
  }
  const dated = /\b20(2[6-9]|[3-9]\d)\b/.test(resultText);
  const event = DEMAND_MECHANISM.test(resultText);
  if (inResult && dated && event) return "VERIFIED_FUTURE_LOCAL_EVENT";
  if (inResult && event && /association|asociaci[oó]n|c[aá]mara|society|universidad|university/i.test(resultText)) {
    return "VERIFIED_LOCAL_DEMAND";
  }
  if (event && /annual|anual/.test(resultText) && !inResult) return "INFERRED_ROTATIONAL_OR_TRANSFERABLE_DEMAND";
  return "GEOGRAPHY_UNRESOLVED";
}

export function classifyBuyerDiscoveryResult(row = {}) {
  const sourceType = classifySource(row.sourceUrl, row.title || row.organizationName);
  const geography = classifyResultGeography({
    title: row.title || row.organizationName,
    snippet: row.snippet,
    url: row.sourceUrl,
    territoryTerms: row.territoryTerms,
    query: row.query,
  });
  const segment = buyerSegmentForQuery(`${row.query || ""} ${row.title || row.organizationName || ""}`);
  let decision = "reject";
  let reason = "NO_GROUP_BUYER";
  let nextAction = "NO_FURTHER_SEARCH";
  const past = /\b202[0-5]\b/.test(`${row.title || row.organizationName || ""} ${row.sourceUrl || ""}`)
    && !/\b20(2[6-9]|[3-9]\d)\b/.test(`${row.title || row.organizationName || ""} ${row.sourceUrl || ""}`);
  if (sourceType === "news") {
    reason = "SOURCE_NEWS";
  } else if (sourceType === "legislature") {
    reason = "SOURCE_LEGISLATURE";
  } else if (sourceType === "workforce") {
    reason = "SOURCE_WORKFORCE";
  } else if (sourceType === "directory" || sourceType === "venue_listing" || sourceType === "archive") {
    reason = sourceType === "directory" ? "SOURCE_DIRECTORY" : sourceType === "archive" ? "SOURCE_ARCHIVE" : "VENUE_NOT_A_BUYER";
  } else if (past) {
    reason = "PAST_EVENT";
  } else if (geography === "OUT_OF_MARKET") {
    reason = "OUT_OF_MARKET";
  } else if (!DEMAND_MECHANISM.test(`${row.title || row.organizationName || ""} ${row.sourceUrl || ""}`)) {
    reason = "NO_DEMAND_MECHANISM";
  } else if (geography === "VERIFIED_FUTURE_LOCAL_EVENT" || geography === "VERIFIED_LOCAL_DEMAND" || geography === "INFERRED_ROTATIONAL_OR_TRANSFERABLE_DEMAND") {
    decision = "investigate";
    reason = geography === "INFERRED_ROTATIONAL_OR_TRANSFERABLE_DEMAND"
      ? "PRESERVE_FOR_GEOGRAPHY_CHECK"
      : "ORGANIZATION_WITH_LOCAL_OR_ROTATIONAL_SIGNAL";
    nextAction = geography === "INFERRED_ROTATIONAL_OR_TRANSFERABLE_DEMAND"
      ? "READ_OFFICIAL_PAGE_FOR_VENUE"
      : "READ_OFFICIAL_CONTACT_PAGE";
  } else {
    reason = "GEOGRAPHY_OR_BUYER_UNRESOLVED";
  }
  if (decision === "investigate" && /cvent\.com\/event|eventbrite\./i.test(row.sourceUrl || "")) {
    nextAction = "FIND_ORGANIZER_BEYOND_EVENT_LISTING";
  }
  const contactClass = sourceType === "news" || sourceType === "legislature"
    ? "unrelated_contact"
    : sourceType === "directory" || /cvent\.com\/event|eventbrite\./i.test(row.sourceUrl || "")
      ? "event_listing_contact"
      : sourceType === "venue_listing"
        ? "venue_contact"
        : decision === "investigate"
          ? "relevant_organizational_route"
          : "generic_organization_inquiry";
  return {
    sourceType,
    geography,
    segment,
    decision,
    reason,
    nextAction,
    contactClass,
    hotelFacing: false,
    verifiedOrganizerRoute: false,
  };
}

export function reconcileCanaryAccount({ entries = [], openingPlanSearchesLeft, closingPlanSearchesLeft } = {}) {
  const accountCredits = Number(openingPlanSearchesLeft) - Number(closingPlanSearchesLeft);
  const attributedEntries = entries.filter((entry) => entry.searchId && entry.status === "billed");
  const attributedCredits = attributedEntries.reduce((sum, entry) => sum + Number(entry.credits || 0), 0);
  const byHotel = {};
  for (const entry of attributedEntries) {
    byHotel[entry.hotelId] = (byHotel[entry.hotelId] || 0) + Number(entry.credits || 0);
  }
  const failuresWithoutSearchId = entries.filter((entry) => !entry.searchId && entry.httpOk === false);
  const unmatched = accountCredits - attributedCredits;
  return {
    accountCredits,
    attributedCredits,
    unmatchedCredits: unmatched,
    attributedByHotel: byHotel,
    clientFailuresWithoutSearchId: failuresWithoutSearchId.length,
    concurrentOutsideUseShown: false,
    searchesWereSequential: true,
    quarantine: unmatched === 0 ? null : {
      credits: unmatched,
      hotelId: null,
      allocatedUsd: null,
      reason: "CLIENT_FAILURE_WITHOUT_SEARCH_ID",
    },
  };
}
