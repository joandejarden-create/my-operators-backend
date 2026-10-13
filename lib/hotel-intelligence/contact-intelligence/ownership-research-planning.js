/** Bounded search/reading helpers for the existing Context.dev handoff, not a new crawler.
 * Search hits are leads. Candidate extraction never proves ownership or authorizes enrichment.
 */
import { createHash } from "node:crypto";
import {
  mentions,
  mentionsHotel,
  hotelCoreName,
  classifyOwnershipLead,
  ownershipPassageSupport,
} from "./research-evidence.js";
import {
  resolveOwnershipResearchStrategy,
  sourceTierRankDelta,
  shouldScrapeForOwnership,
  selectUrlsForPaidScrape,
} from "./ownership-research-strategy/index.js";

/** How-to / KYC guides that matched Arm A generic "hotel ownership" queries — not property evidence. */
export const OWNERSHIP_GUIDE_HOST_RE =
  /wikihow\.com|entitycheck\.com|buythathotel\.com|how-to-research-hotel-ownership|business entity search/i;

export function ownershipQueries(hotel = {}) {
  const full = String(hotel.hotel_name || "").replace(/["\n\r]/g, " ").trim();
  const core =
    full
      .replace(
        /,?\s+(?:an? |by )?(?:autograph collection|tapestry collection|lxr hotels|all[- ]inclusive|adults only|an? slh hotel).*$/i,
        ""
      )
      .trim() || full;
  const location = [hotel.city, hotel.country].filter(Boolean).join(" ");
  const local =
    hotel.language === "pt"
      ? "(proprietário OR \"de propriedade\" OR adquiriu OR aquisição OR investidor OR desenvolvedor OR CNPJ OR \"razão social\")"
      : hotel.language === "es"
        ? "(propietario OR \"propiedad de\" OR adquirió OR adquisición OR inversionista OR desarrollador)"
        : "(owner OR \"owned by\" OR \"property owner\" OR acquired OR purchased OR investor OR developer)";
  const succession =
    hotel.language === "pt"
      ? "(família OR proprietário OR fundador OR herdeiro OR portfólio)"
      : hotel.language === "es"
        ? "(familia OR propietario OR fundador OR heredero OR portafolio)"
        : "(owner OR owners OR proprietor OR \"family-owned\" OR \"family owned\" OR founder OR portfolio)";
  const txn =
    hotel.language === "pt"
      ? "(adquiriu OR aquisição OR comprou OR venda OR investidor)"
      : hotel.language === "es"
        ? "(adquirió OR adquisición OR compró OR venta OR inversionista)"
        : "(acquired OR acquisition OR purchased OR investor OR sale OR \"owned by\")";
  const history =
    hotel.language === "pt"
      ? "(histórico OR \"mudança de proprietário\" OR sucessão OR \"troca de dono\" OR vendido)"
      : hotel.language === "es"
        ? "(historial OR \"cambio de propietario\" OR sucesión OR vendido)"
        : "(ownership history OR \"former owner\" OR \"sold to\" OR succession OR \"change of ownership\")";
  const sponsor =
    hotel.language === "pt"
      ? "(investidor OR patrocinador OR desenvolvedor OR sponsor OR FII OR CVM)"
      : hotel.language === "es"
        ? "(inversionista OR patrocinador OR desarrollador OR sponsor)"
        : "(investor OR developer OR sponsor OR \"economic owner\" OR \"hotel owner\")";

  // Country strategy ladder queries first (Brazil V1: CNPJ / address / razão social).
  const strategy = resolveOwnershipResearchStrategy(hotel);
  const strategyQueries =
    strategy && typeof strategy.buildLegalEntityQueries === "function"
      ? strategy.buildLegalEntityQueries(hotel)
      : [];

  // Ownership-bearing queries first: thin Context budgets (≤4) used to burn credits on
  // bare-name / city identity searches that surface OTAs before ownership language queries run.
  // Bare-name identity queries are last and should not dominate early scrape selection.
  return [
    ...new Set(
      [
        ...strategyQueries,
        location ? `"${core}" ${location} ${local}`.trim() : `"${core}" ${local}`,
        location ? `"${core}" ${location} ${succession}`.trim() : `"${core}" ${succession}`,
        `"${core}" (${txn})`,
        location ? `"${core}" ${location} ${history}`.trim() : `"${core}" ${history}`,
        location ? `"${core}" ${location} ${sponsor}`.trim() : `"${core}" ${sponsor}`,
        `"${full}" (ownership OR propietario OR proprietario OR annual report OR filing)`,
        `"${core}"`,
        location ? `"${core}" ${location}` : null,
      ].filter(Boolean)
    ),
  ];
}

export function ownershipSearchResults(data) {
  // Known named envelopes only; unexpected shape is an execution failure, not zero evidence.
  const rows = Array.isArray(data?.results)
    ? data.results
    : Array.isArray(data?.data?.results)
      ? data.data.results
      : null;
  return rows === null
    ? null
    : rows
        .map((r) => ({
          title: r.title || "",
          url: r.url || r.link,
          snippet: r.description || r.snippet || "",
        }))
        .filter((r) => /^https?:\/\//.test(r.url || ""));
}

/** Prefer ownership language; do not require exact census casing / leading "the". */
function hotelMentionedInOwnershipBlob(blob, hotelName = "") {
  if (mentionsHotel(blob, hotelName)) return true;
  const core = hotelCoreName(hotelName)
    .replace(/^(?:the|el|la|le|les|los|las)\s+/i, "")
    .trim();
  if (core.length >= 6 && mentions(blob, core)) return true;
  // Token overlap for long names (e.g. "anguilla great house" inside "Anguilla Great House Beach Resort")
  const tokens = core
    .toLowerCase()
    .split(/[^a-z0-9]+/i)
    .filter((t) => t.length >= 4 && !/^(hotel|resort|beach|apart|spa|the)$/i.test(t));
  if (tokens.length < 2) return false;
  const norm = String(blob || "").toLowerCase();
  return tokens.filter((t) => norm.includes(t)).length >= Math.min(3, tokens.length);
}

/**
 * Rank SERP leads for ownership document fetch.
 * Returns ranked + rejected (with reasons) so traces can distinguish
 * "not retrieved" from "retrieved and filtered".
 */
export function rankOwnershipSourcesDetailed(rows, hotel = {}, opts = {}) {
  const seen = new Set();
  const rejected = [];
  const scored = [];
  const researchBlob = `${opts.researchGoal || ""} ${opts.evidence_gap || ""}`.toLowerCase();
  const wantsCurrentness = /current|sold|sale|successor|divest|currency|registry|who owns/i.test(researchBlob);
  const wantsNamedOwner = /owner|propriet|sponsor|investor|acquisition|history/i.test(researchBlob);

  for (const r of rows || []) {
    if (!r?.url) continue;
    if (seen.has(r.url)) {
      rejected.push({ ...r, reject_reason: "DUPLICATE_URL", score: null });
      continue;
    }
    seen.add(r.url);
    const blob = `${r.title} ${r.snippet} ${r.url}`;
    let score = 0;
    const reasons = [];

    if (
      /acquir|acquis|purchas|owned|owners?\b|proprietor|propietari|shareholder|adquiri|compra|vendid|family[- ]owned|original owners?|property owner|\"owned by\"|is the owner/i.test(
        blob
      )
    ) {
      score += 5;
      reasons.push("OWNERSHIP_LANGUAGE");
    }
    if (/press|news-release|investor|annual|filing|relatorio|registry|deed|\.pdf(?:\?|$)/i.test(blob)) {
      score += 4;
      reasons.push("INVESTOR_OR_REGISTRY");
    }
    if (/gov\.|\.gov\/|municipal|prefeitura|registro|cadastro/i.test(blob)) {
      score += 3;
      reasons.push("GOVERNMENT_OR_REGISTRY_HOST");
    }
    if (hotelMentionedInOwnershipBlob(blob, hotel.hotel_name)) {
      score += 2;
      reasons.push("HOTEL_IDENTITY_MATCH");
    }
    if (
      hotelMentionedInOwnershipBlob(blob, hotel.hotel_name) &&
      (/linkedin\.com\/in\//i.test(r.url) || /\/(news|article|lifestyle)\//i.test(r.url))
    ) {
      score += 3;
      reasons.push("PERSON_OR_PRESS_WITH_HOTEL");
    }
    // Biography / interview / history / talent pages — evidence may appear without "owner" in the URL.
    if (
      hotelMentionedInOwnershipBlob(blob, hotel.hotel_name) &&
      reasons.includes("OWNERSHIP_LANGUAGE") &&
      (/\/(culinary_talent|talent|bio|biography|interview|history|people|team|founder|about-us|our-story)\b/i.test(
        r.url
      ) ||
        /\b(biography|interview|history|proprietor|culinary talent|our story|founded)\b/i.test(blob))
    ) {
      score += 2;
      reasons.push("BIO_INTERVIEW_OR_HISTORY_CONTEXT");
    }
    // First-party-ish owner/sponsor pages (not OTAs)
    if (
      hotelMentionedInOwnershipBlob(blob, hotel.hotel_name) &&
      !/booking|tripadvisor|expedia|hotels\.com/i.test(r.url) &&
      /about|ownership|investor|portfolio|company/i.test(blob)
    ) {
      score += 2;
      reasons.push("FIRST_PARTY_OR_SPONSOR_SIGNAL");
    }
    if (wantsCurrentness && /sold|sale|divest|current owner|as of|acquired by|registry|cnpj/i.test(blob)) {
      score += 1;
      reasons.push("RESEARCH_QUESTION_ALIGNMENT");
    }
    if (wantsNamedOwner && reasons.includes("OWNERSHIP_LANGUAGE")) {
      score += 1;
      reasons.push("RESEARCH_QUESTION_ALIGNMENT");
    }

    // Country strategy source tiers (Brazil V1: CNPJ/registry boost; OTA suppress).
    const tierDelta = sourceTierRankDelta(r, {
      hotel_address: hotel.address || hotel.street_address || "",
    });
    if (tierDelta.delta) {
      score += tierDelta.delta;
      reasons.push(...tierDelta.rank_reasons);
    }
    const scrapeGate = shouldScrapeForOwnership(r, {
      hotel_address: hotel.address || hotel.street_address || "",
    });
    r.ownership_scrape_gate = scrapeGate;

    if (/booking\.com|tripadvisor|whoistheownerof|whoownsthebrand|careers|jobs|trip\.com|agoda\.|yelp\./i.test(r.url)) {
      score -= 20;
      reasons.push("OTA_OR_JOB_BOARD");
    }
    if (
      /cvent\.com|expedia\.|hotels\.com|travelocity\.|skyscanner\.|hotel\.info|lesserantilleshotels\.com|evendo\.com|execstays\.com|momondo\.|rentbyowner\.com|findyello\.com|caribbean\.com\/hotels|allinclusiveweddings\.com|hotel-rez\.com|hotelscombined|kayak\.|trivago\./i.test(
        r.url
      )
    ) {
      score -= 8;
      reasons.push("VENUE_OR_AGGREGATOR");
    }
    // Brand-central / chain marketing pages without ownership language burn thin budgets.
    if (
      /all\.accor\.com|marriott\.com|hilton\.com|ihg\.com|wyndhamhotels\.com|hyatt\.com|radissonhotels\.com|choicehotels\.com|bestwestern\.com/i.test(
        r.url
      ) &&
      !reasons.includes("OWNERSHIP_LANGUAGE") &&
      !reasons.includes("INVESTOR_OR_REGISTRY")
    ) {
      score -= 7;
      reasons.push("BRAND_CENTRAL_WITHOUT_OWNERSHIP");
    }
    if (/\/tag\/|\/author\/|\/search\//i.test(r.url)) {
      score -= 6;
      reasons.push("INDEX_OR_TAG_PAGE");
    }
    if (OWNERSHIP_GUIDE_HOST_RE.test(blob)) {
      score -= 25;
      reasons.push("OWNERSHIP_HOWTO_GUIDE");
    }
    // Generic investor-education / dictionary pages (not property-specific filings).
    if (
      /investopedia\.com|wikipedia\.org\/wiki\/(Hotel|Real_estate|Private_equity)|dictionary\.com|wikihow\.com/i.test(
        r.url
      ) ||
      (/\b(what is hotel ownership|how to buy a hotel|hotel ownership guide)\b/i.test(blob) &&
        !hotelMentionedInOwnershipBlob(blob, hotel.hotel_name))
    ) {
      score -= 15;
      reasons.push("GENERIC_INVESTOR_OR_DICTIONARY");
    }

    // Identity-only travel / social / amenity listings: hotel identity ≠ ownership evidence.
    const identityOnlyTravel =
      hotelMentionedInOwnershipBlob(blob, hotel.hotel_name) &&
      !reasons.includes("OWNERSHIP_LANGUAGE") &&
      !reasons.includes("INVESTOR_OR_REGISTRY") &&
      (/facebook\.com|instagram\.com|travelweekly\.com|forbestravelguide\.com|americanexpress\.com\/.*\/travel|lartisien\.com|tripmasters\.com|meetings-conventions\.com|hotelscombined|kayak\.|trivago\./i.test(
        r.url
      ) ||
        /\b(book |rates|amenities|traveler reviews|check reviews|rooms? |suite|spa services|meeting venue)\b/i.test(
          blob
        ));
    if (identityOnlyTravel) {
      score -= 3;
      reasons.push("IDENTITY_ONLY_TRAVEL_LISTING");
    }

    const row = { ...r, score, rank_reasons: reasons };
    if (score < 0) {
      rejected.push({ ...row, reject_reason: reasons.join("|") || "SCORE_BELOW_ZERO" });
    } else {
      scored.push(row);
    }
  }

  scored.sort((a, b) => b.score - a.score);

  // Collapse equivalent travel-listing hosts (keep best) when none carry ownership language.
  const kept = [];
  const listingHostSeen = new Set();
  for (const row of scored) {
    let host = "";
    try {
      host = new URL(row.url).hostname.replace(/^www\./, "").toLowerCase();
    } catch {
      host = "";
    }
    const isListingHost =
      /facebook\.com|travelweekly\.com|tripadvisor\.|expedia\.|hotels\.com|booking\.|forbestravelguide|americanexpress\.com|lartisien\.com|tripmasters\.com|meetings-conventions\.com/i.test(
        host
      );
    const ownershipBearing =
      (row.rank_reasons || []).includes("OWNERSHIP_LANGUAGE") ||
      (row.rank_reasons || []).includes("INVESTOR_OR_REGISTRY");
    if (isListingHost && !ownershipBearing) {
      if (listingHostSeen.has(host)) {
        rejected.push({
          ...row,
          reject_reason: "EQUIVALENT_TRAVEL_LISTING_DUPLICATE",
          score: row.score,
        });
        continue;
      }
      listingHostSeen.add(host);
    }
    kept.push(row);
  }

  return {
    ranked: kept,
    rejected,
    candidate_urls_before_filter: (rows || []).map((r) => r.url).filter(Boolean),
  };
}

/** Backward-compatible: ranked list only. */
export function rankOwnershipSources(rows, hotel = {}) {
  return rankOwnershipSourcesDetailed(rows, hotel).ranked;
}

/**
 * Rank then apply Brazil V1.1 hard scrape-selection gate (goal-aware; OTA suppress).
 * @param {Array} rows
 * @param {object} hotel
 * @param {{ researchGoal?: string, evidence_gap?: string, research_goal_kind?: string, docs_limit?: number }} opts
 */
export function selectOwnershipUrlsForPaidScrape(rows, hotel = {}, opts = {}) {
  const detail = rankOwnershipSourcesDetailed(rows, hotel, opts);
  const gate = selectUrlsForPaidScrape(detail.ranked, {
    hotel,
    researchGoal: opts.researchGoal,
    evidence_gap: opts.evidence_gap,
    research_goal_kind: opts.research_goal_kind,
  });
  const limit = Number(opts.docs_limit) > 0 ? Number(opts.docs_limit) : gate.selected.length;
  return {
    ...detail,
    scrape_selection: gate,
    selected_for_scrape: gate.selected.slice(0, limit),
    rejected_from_scrape: gate.rejected,
  };
}

/** True when Context SERP top ranks lack ownership language — trigger for bounded alt-provider search. */
export function ownershipSerpNeedsAltProviderFallback(rankedRows = [], { minTopScore = 5, inspect = 4 } = {}) {
  const top = (rankedRows || []).slice(0, Math.max(1, inspect));
  if (!top.length) return { needed: true, reason: "empty_ranked_results" };
  const best = Math.max(...top.map((r) => Number(r.score) || 0));
  const hasOwnershipLanguage = top.some((r) =>
    /acquir|owned|owners?\b|proprietor|propietari|shareholder|family[- ]owned|investor|sale\b/i.test(
      `${r.title || ""} ${r.snippet || ""}`
    )
  );
  if (!hasOwnershipLanguage) {
    return { needed: true, reason: "no_ownership_language_in_top_ranks", best_score: best };
  }
  if (best < minTopScore) return { needed: true, reason: "weak_ownership_signal_in_top_ranks", best_score: best };
  return { needed: false, reason: null, best_score: best };
}

export function ownershipDocumentPassages(text, hotel = {}) {
  const raw = String(text || "");
  const core = hotelCoreName(hotel.hotel_name);
  const matches = [];
  for (const hit of raw.matchAll(
    /owned|owner|acquir|acquis|purchas|propiedad|propietari|adquiri|compra|portfolio|family[- ]owned/gi
  )) {
    const start = Math.max(0, hit.index - 350);
    const end = Math.min(raw.length, hit.index + 900);
    const excerpt = raw.slice(start, end);
    const hotelOk =
      mentionsHotel(excerpt, hotel.hotel_name) || hotelMentionedInOwnershipBlob(excerpt, hotel.hotel_name);
    if (hotelOk && !matches.some((p) => Math.abs(p.start - start) < 100)) {
      matches.push({ start, end, excerpt });
    }
  }
  return {
    sha256: createHash("sha256").update(raw).digest("hex"),
    characters: raw.length,
    passages: matches.slice(0, 8),
    hotel_core: core,
  };
}

/**
 * Bounded deterministic section selection for oversized documents.
 * Preserves original offsets; never silently truncates or claims unread text has no evidence.
 */
export function selectOwnershipDocumentSectionsForReader(sourceText, hotel = {}, { maxChars = 22000 } = {}) {
  const raw = String(sourceText ?? "");
  const original_length = raw.length;
  if (original_length === 0) {
    return {
      status: "EMPTY",
      text_for_reader: "",
      sections_selected: [],
      sections_unread: [],
      unread_may_contain_evidence: false,
      original_length: 0,
      source_truncated: false,
    };
  }
  if (original_length <= maxChars) {
    return {
      status: "FULL_DOCUMENT",
      text_for_reader: raw,
      sections_selected: [{ start: 0, end: original_length, reason: "FULL_DOCUMENT" }],
      sections_unread: [],
      unread_may_contain_evidence: false,
      original_length,
      source_truncated: false,
    };
  }

  const passages = ownershipDocumentPassages(raw, hotel).passages || [];
  const windows = [];
  for (const p of passages) {
    windows.push({
      start: Math.max(0, p.start),
      end: Math.min(original_length, p.end),
      reason: "OWNERSHIP_KEYWORD_WINDOW",
    });
  }
  // Heading / blank-line section anchors near hotel identity when few ownership hits.
  if (windows.length < 2) {
    const headingRe = /(?:^|\n)#{1,3}\s+[^\n]{3,80}\n/g;
    let m;
    while ((m = headingRe.exec(raw)) && windows.length < 6) {
      const start = m.index;
      const end = Math.min(original_length, start + 2000);
      windows.push({ start, end, reason: "MARKDOWN_HEADING_WINDOW" });
    }
  }
  if (!windows.length) {
    // Still cannot claim unread has no evidence — surface prefix + explicit unread tail.
    const end = Math.min(original_length, maxChars);
    return {
      status: "PREFIX_WITH_UNREAD_REMAINDER",
      text_for_reader: raw.slice(0, end),
      sections_selected: [{ start: 0, end, reason: "PREFIX_FALLBACK_NO_OWNERSHIP_HITS" }],
      sections_unread: [{ start: end, end: original_length, reason: "UNREAD_REMAINDER" }],
      unread_may_contain_evidence: true,
      original_length,
      source_truncated: false,
      note: "Document exceeds reader allowance; unread sections may still contain ownership evidence.",
    };
  }

  // Merge overlapping windows, greedily keep highest-priority (earliest ownership) within budget.
  windows.sort((a, b) => a.start - b.start);
  const merged = [];
  for (const w of windows) {
    const last = merged[merged.length - 1];
    if (last && w.start <= last.end + 80) {
      last.end = Math.max(last.end, w.end);
      last.reason = `${last.reason}+${w.reason}`;
    } else {
      merged.push({ ...w });
    }
  }

  const selected = [];
  let used = 0;
  const sep = "\n\n<!-- section boundary -->\n\n";
  const parts = [];
  for (const w of merged) {
    const len = w.end - w.start;
    const extra = parts.length ? sep.length : 0;
    if (used + extra + len > maxChars) {
      if (selected.length === 0 && len > maxChars) {
        // Single oversized window: take bounded slice with original offsets noted.
        const sliceEnd = w.start + maxChars;
        selected.push({
          start: w.start,
          end: sliceEnd,
          reason: `${w.reason}|BOUNDED_WITHIN_WINDOW`,
        });
        parts.push(raw.slice(w.start, sliceEnd));
        used = maxChars;
      }
      break;
    }
    selected.push(w);
    parts.push(raw.slice(w.start, w.end));
    used += extra + len;
  }

  const covered = new Set();
  for (const s of selected) {
    for (let i = s.start; i < s.end; i += 1) covered.add(i);
  }
  const unread = [];
  let i = 0;
  while (i < original_length) {
    if (covered.has(i)) {
      i += 1;
      continue;
    }
    const start = i;
    while (i < original_length && !covered.has(i)) i += 1;
    unread.push({ start, end: i, reason: "UNREAD_SECTION" });
  }

  return {
    status: "SECTION_SELECTION",
    text_for_reader: parts.join(sep),
    sections_selected: selected,
    sections_unread: unread,
    unread_may_contain_evidence: unread.length > 0,
    original_length,
    source_truncated: false,
    note:
      unread.length > 0
        ? "Unread sections remain; absence of claims in selected sections does not prove the unread text lacks ownership evidence."
        : null,
  };
}

export function ownershipCandidates(document, hotel = {}) {
  const out = [];
  const hotelName = hotel.hotel_name || document.hotel_name || "";
  // Capitalized names with bounded tokens, distinct from nearby prose. Leads only.
  const name = "([A-ZÀ-Ý][\\p{L}\\d&'’-]+(?:[ \\t]+(?:[A-ZÀ-Ý][\\p{L}\\d&'’-]+|&)){0,5})";
  for (const p of document.passages || []) {
    const lead = classifyOwnershipLead(p.excerpt, hotelName);
    // Décor metaphors never yield candidates. Operator boilerplate alone is rejected,
    // but the same passage may still contain a real "acquisition by X" clause.
    if (lead.reasons.includes("DECOR_OR_MARKETING_METAPHOR")) {
      out.push({
        name: null,
        excerpt: p.excerpt,
        classification: "REJECTED_NEGATIVE_CONTROL",
        currentness: "N/A",
        lead_reasons: lead.reasons,
      });
      continue;
    }
    if (
      lead.reasons.includes("OPERATOR_SELF_DESCRIPTION") &&
      !lead.reasons.includes("TRANSACTION_ACQUISITION_LANGUAGE")
    ) {
      out.push({
        name: null,
        excerpt: p.excerpt,
        classification: "REJECTED_NEGATIVE_CONTROL",
        currentness: "N/A",
        lead_reasons: lead.reasons,
      });
      continue;
    }
    for (const re of [
      new RegExp(
        "(?:owned by|acquired by|purchased by|acquisition by|propiedad de|adquirido por|adquirida por)\\s+" + name,
        "gu"
      ),
      new RegExp(
        name + "\\s+(?:has |today )?(?:acquired|purchased|adquiriu|adquirió|compró)\\b",
        "gu"
      ),
    ]) {
      for (const m of p.excerpt.matchAll(re)) {
        const candName = m[1].trim();
        const txnSupported = hotelName
          ? ownershipPassageSupport(p.excerpt, hotelName, candName)
          : false;
        out.push({
          name: candName,
          excerpt: p.excerpt,
          classification: "OWNER_CANDIDATE",
          currentness: "UNRESOLVED",
          lead_reasons: lead.reasons,
          // Passage supports a named-party transaction claim — NOT current ownership.
          supported_transaction_claim: txnSupported,
          // Deprecated alias kept for older readers; same meaning as supported_transaction_claim.
          supported_ownership: txnSupported,
        });
      }
    }
    if (lead.reasons.includes("FAMILY_OWNED_UNNAMED") && !out.some((o) => o.excerpt === p.excerpt && o.name)) {
      out.push({
        name: null,
        excerpt: p.excerpt,
        classification: "FAMILY_OWNED_UNNAMED_LEAD",
        currentness: "UNRESOLVED",
        lead_reasons: lead.reasons,
        supported_transaction_claim: false,
        supported_ownership: false,
      });
    }
    // Named present-tense owner statements → research leads only (never auto-current).
    if (lead.named_owner_statement_lead || lead.reasons.includes("NAMED_CURRENT_OWNER_STATEMENT")) {
      for (const re of [
        new RegExp(name + "\\s+is the owner of\\b", "gu"),
        new RegExp(name + ",\\s+founder and owner of\\b", "gu"),
        new RegExp(
          name + "\\s+Proprietor\\b",
          "gu"
        ),
        new RegExp(
          "(?:main shareholder of[^.]{0,40})|" + name + "\\s+is the main shareholder of\\b",
          "gu"
        ),
        new RegExp("(?:Mr\\.?\\s+)?" + name + ",\\s+founder and owner of\\b", "gu"),
      ]) {
        for (const m of p.excerpt.matchAll(re)) {
          const candName = (m[1] || "").trim();
          if (!candName || candName.length < 3) continue;
          if (out.some((o) => o.name === candName && o.excerpt === p.excerpt)) continue;
          out.push({
            name: candName,
            excerpt: p.excerpt,
            // Evidenced ownership statement — not stuck as permanent LEAD solely for lacking
            // an acquisition verb. Still requires currentness research; not auto-current.
            classification: "OWNERSHIP_STATEMENT_EVIDENCED",
            currentness: "UNRESOLVED",
            lead_reasons: lead.reasons,
            supported_transaction_claim: false,
            supported_ownership: false,
            implies_current_ownership: false,
            requires_currency_research: true,
            party_role: "ECONOMIC_SPONSOR_OR_PROPERTY_OWNER_STATEMENT",
            note:
              "Hotel-bound named ownership statement — evidence for review + currentness research; not deed UBO; not auto-current",
          });
        }
      }
    }
  }
  return out;
}
