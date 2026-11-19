/**
 * Canonical commercial source register for Fact Selection / Brief / Supporting Research.
 * Reuses Exa source-evidence URL identity helpers — does not invent URLs, titles, or dates.
 */
import {
  isValidHttpUrl,
  shouldDisplayEvidenceUrl,
} from "../research/exa-agent/exa-agent-source-evidence-v1.js";

export const GDI_COMMERCIAL_SOURCE_REGISTER_V1 =
  "gdi-commercial-source-register-v1";

export const DRAWER_SOURCE_SECTIONS = Object.freeze([
  "demand_context",
  "potential_hotel_business",
  "hotel_relevance",
  "commercial_timing",
  "buyer_contacts",
  "competitors_venues",
  "recommended_next_step",
  "supporting_research",
  "all_sources",
]);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

export function stableCommercialSourceId(url, index = 0) {
  const u = text(url).toLowerCase().replace(/\/$/, "");
  let h = 0;
  for (let i = 0; i < u.length; i += 1) {
    h = (h * 31 + u.charCodeAt(i)) >>> 0;
  }
  return `csrc_${(h >>> 0).toString(16)}_${index}`;
}

/**
 * Customer-facing title: never invent publishers; when stored title is generic
 * (opportunity name / placeholder), humanize the URL path segment that exists.
 */
export function displayTitleForSource(source = {}, opts = {}) {
  const url = text(source.url || source.sourceUrl);
  const raw = text(source.title || source.name || source.statement);
  const generic = text(opts.genericTitle || opts.opportunityTitle);
  const looksGeneric =
    !raw ||
    raw === url ||
    (generic && raw.toLowerCase() === generic.toLowerCase()) ||
    /^(official source|lodging evidence|contact source|contact —)/i.test(raw);
  if (!looksGeneric) return raw;
  if (!url) return raw || null;
  try {
    const u = new URL(url);
    const parts = u.pathname.split("/").filter(Boolean);
    const last = (parts[parts.length - 1] || "").replace(/\.[a-z0-9]+$/i, "");
    const human = last.replace(/[-_]+/g, " ").trim();
    if (human.length >= 3) {
      return human.replace(/\b[a-z]/g, (c) => c.toUpperCase());
    }
    if (u.hostname) return u.hostname.replace(/^www\./i, "");
  } catch {
    /* keep raw */
  }
  return raw || url;
}

/** Deduplicate source rows by canonical URL for one section Sources button. */
export function dedupeSourcesForSectionButton(rows = [], opts = {}) {
  const seen = new Set();
  const out = [];
  for (const s of rows || []) {
    const url = text(s?.url || s?.sourceUrl);
    if (!url) continue;
    const key = url.toLowerCase().replace(/\/$/, "");
    if (seen.has(key)) continue;
    seen.add(key);
    out.push({
      url,
      title: displayTitleForSource(s, opts),
    });
  }
  return out;
}

function classifySourceType(url, meta = {}) {
  if (meta.sourceType) return text(meta.sourceType);
  const u = text(url).toLowerCase();
  if (/nist\.gov|nrc\.gov|pdac\.ca|owwa\.ca|weao\.org/i.test(u)) return "official_web";
  if (/linkedin\.com/i.test(u)) return "professional_profile";
  if (/\.pdf(\?|$)/i.test(u)) return "pdf_document";
  return "web";
}

function pushSource(byUrl, entry, index) {
  const url = text(entry.url || entry.sourceUrl);
  if (!url || !isValidHttpUrl(url) || !shouldDisplayEvidenceUrl(url)) {
    return null;
  }
  const key = url.toLowerCase().replace(/\/$/, "");
  const existing = byUrl.get(key);
  if (existing) {
    // Preserve corroboration: merge titles/publishers only when previously empty
    if (!existing.title && entry.title) existing.title = text(entry.title);
    if (!existing.publisher && entry.publisher) existing.publisher = text(entry.publisher);
    if (!existing.publicationDate && entry.publicationDate) {
      existing.publicationDate = entry.publicationDate;
    }
    if (!existing.researchVerificationDate && entry.researchVerificationDate) {
      existing.researchVerificationDate = entry.researchVerificationDate;
    }
    existing.corroborationCount = (existing.corroborationCount || 1) + 1;
    for (const sid of entry.drawerSections || []) {
      if (!existing.drawerSections.includes(sid)) existing.drawerSections.push(sid);
    }
    return existing;
  }
  const rec = {
    sourceId: text(entry.sourceId) || stableCommercialSourceId(url, index),
    url,
    title: text(entry.title || entry.name || entry.statement) || null,
    publisher: text(entry.publisher) || null,
    sourceType: classifySourceType(url, entry),
    publicationDate: entry.publicationDate || entry.date || null,
    researchVerificationDate: entry.researchVerificationDate || entry.date || null,
    associatedFactIds: [],
    drawerSections: [...new Set((entry.drawerSections || []).map(text).filter(Boolean))],
    visibility: entry.hotelPrivate || entry.sharedMarketForbidden ? "HOTEL_PRIVATE" : "PUBLIC",
    verificationStatus: text(entry.verificationStatus) || "RECOVERED",
    availabilityStatus: "AVAILABLE",
    artifactRef: text(entry.artifactRef) || null,
    corroborationCount: 1,
  };
  byUrl.set(key, rec);
  return rec;
}

/**
 * Build deduplicated source register from opportunity + catalog fact URLs.
 * Never invents missing metadata.
 */
export function buildCommercialSourceRegister(opp = {}, opts = {}) {
  const byUrl = new Map();
  let idx = 0;
  const catalogFacts = Array.isArray(opts.catalog?.materialFacts)
    ? opts.catalog.materialFacts
    : [];

  const candidates = [];
  for (const s of opp.sources || []) {
    if (typeof s === "string") candidates.push({ url: s });
    else if (s && typeof s === "object") candidates.push(s);
  }
  if (opp.officialSource) candidates.push({ url: opp.officialSource, title: "Official source" });
  if (opp.lodgingEvidenceUrl) {
    candidates.push({
      url: opp.lodgingEvidenceUrl,
      title: "Lodging evidence",
      drawerSections: ["demand_context", "competitors_venues"],
    });
  }
  const c = opp.primaryContact || {};
  if (c.sourceUrl || c.emailSource || c.source) {
    candidates.push({
      url: c.sourceUrl || c.emailSource || c.source,
      title: text(c.name) ? `Contact — ${c.name}` : "Contact source",
      drawerSections: ["buyer_contacts", "recommended_next_step"],
    });
  }
  for (const f of catalogFacts) {
    const urls = [
      ...(f.sourceUrl ? [f.sourceUrl] : []),
      ...(Array.isArray(f.sourceUrls) ? f.sourceUrls : []),
    ];
    for (const u of urls) {
      candidates.push({
        url: u,
        title: null,
        hotelPrivate: f.hotelPrivate,
        sharedMarketForbidden: f.sharedMarketForbidden,
        researchVerificationDate: f.verificationDate || null,
        publicationDate: f.activityDate || null,
        drawerSections: [],
        artifactRef: f.evidenceLocation || null,
      });
    }
  }
  for (const e of opp.evidence || []) {
    if (e && (e.url || e.sourceUrl)) {
      candidates.push({
        url: e.url || e.sourceUrl,
        title: e.title || e.name || null,
        date: e.date || null,
      });
    }
  }

  for (const entry of candidates) {
    pushSource(byUrl, entry, idx);
    idx += 1;
  }

  const sources = [...byUrl.values()];
  return {
    version: GDI_COMMERCIAL_SOURCE_REGISTER_V1,
    sources,
    byUrl: Object.fromEntries(
      sources.map((s) => [s.url.toLowerCase().replace(/\/$/, ""), s.sourceId]),
    ),
  };
}

export function resolveSourceIdsForFact(fact = {}, register = {}) {
  const urls = [
    ...(fact.sourceUrl ? [fact.sourceUrl] : []),
    ...(Array.isArray(fact.sourceUrls) ? fact.sourceUrls : []),
  ]
    .map(text)
    .filter(Boolean);
  const ids = [];
  const missingUrls = [];
  for (const u of urls) {
    if (!isValidHttpUrl(u) || !shouldDisplayEvidenceUrl(u)) {
      missingUrls.push(u);
      continue;
    }
    const key = u.toLowerCase().replace(/\/$/, "");
    const id = register.byUrl?.[key];
    if (id) ids.push(id);
    else missingUrls.push(u);
  }
  return { sourceIds: [...new Set(ids)], missingUrls };
}

/**
 * Infer drawer section(s) for a fact from content / axes — no invention of evidence.
 */
export function inferFactDrawerSections(fact = {}) {
  const claim = text(fact.claim);
  const axes = (fact.affectsAxes || []).map((a) => String(a).toLowerCase());
  const sections = new Set();
  // Avoid treating job titles like "Director, Convention" as demand-context events
  if (
    axes.includes("demand") ||
    /conference|expo|\bhub\b|expansion|venue|Enercare|MTCC|dates|\b20\d{2}\b.*\bconvention\b|\bconvention\b.*\b20\d{2}\b/i.test(
      claim,
    )
  ) {
    sections.add("demand_context");
  }
  if (axes.includes("hotel_fit") || /partner|overflow|host|guestroom|meeting/i.test(claim)) {
    sections.add("hotel_relevance");
  }
  if (axes.includes("timing") || /cutoff|October 20|why act|past cycle|2026-09-30/i.test(claim)) {
    sections.add("commercial_timing");
  }
  // People / org routes only — do not attach lodging pages to Who to contact
  if (/contact|@|Sampson|Petryna|Wetzel|Baker|HelpDesk|convention@/i.test(claim)) {
    sections.add("buyer_contacts");
    sections.add("recommended_next_step");
  } else if (axes.includes("buyer_access") && /housing bureau|book directly|direct book/i.test(claim)) {
    sections.add("competitors_venues");
    sections.add("recommended_next_step");
  } else if (axes.includes("buyer_access")) {
    sections.add("buyer_contacts");
    sections.add("recommended_next_step");
  }
  if (/Hotel X|Sheraton|Hyatt|Le M[eé]ridien|competitor|housing bureau|room block/i.test(claim)) {
    sections.add("competitors_venues");
  }
  if (axes.includes("pursuit") || /do not pursue|RFP|qualify|overflow/i.test(claim)) {
    sections.add("recommended_next_step");
  }
  if (/FIU|New America|logistics|attendance|historical/i.test(claim)) {
    sections.add("supporting_research");
  }
  if (!sections.size) sections.add("supporting_research");
  sections.add("all_sources");
  return [...sections];
}

/**
 * Attach fact IDs onto register sources; build section → sourceId map.
 */
export function linkFactsToSourceRegister(facts = [], register = {}) {
  const sources = (register.sources || []).map((s) => ({ ...s, associatedFactIds: [] }));
  const byId = new Map(sources.map((s) => [s.sourceId, s]));
  const sectionMap = Object.fromEntries(DRAWER_SOURCE_SECTIONS.map((s) => [s, []]));

  const linkedFacts = facts.map((f) => {
    const { sourceIds, missingUrls } = resolveSourceIdsForFact(f, register);
    const sections = inferFactDrawerSections(f);
    for (const sid of sourceIds) {
      const src = byId.get(sid);
      if (!src) continue;
      if (!src.associatedFactIds.includes(f.id)) src.associatedFactIds.push(f.id);
      for (const sec of sections) {
        if (!src.drawerSections.includes(sec)) src.drawerSections.push(sec);
        if (!sectionMap[sec].includes(sid)) sectionMap[sec].push(sid);
      }
    }
    // Provenance honesty: establishedFacts without recoverable URL are incomplete
    let verificationStatus = text(f.verificationStatus) || "UNKNOWN";
    let provenanceStatus = "LINKED";
    if (f.origin === "ESTABLISHED_FACTS" && !sourceIds.length) {
      verificationStatus = "PROVENANCE_INCOMPLETE";
      provenanceStatus = "UNVERIFIED_TEXT_ONLY";
    } else if (f.origin === "ABSENT_FROM_RESEARCH") {
      verificationStatus = "UNKNOWN";
      provenanceStatus = "ABSENT";
    } else if (!sourceIds.length && f.sourceUrl) {
      provenanceStatus = "URL_NOT_IN_REGISTER";
    } else if (!sourceIds.length) {
      provenanceStatus = "NO_SOURCE_URL";
      if (verificationStatus === "CONFIRMED") {
        verificationStatus = "PROVENANCE_INCOMPLETE";
      }
    }
    return {
      ...f,
      sourceIds,
      missingSourceUrls: missingUrls,
      drawerSections: sections,
      verificationStatus,
      provenanceStatus,
    };
  });

  return {
    version: GDI_COMMERCIAL_SOURCE_REGISTER_V1,
    sources: [...byId.values()],
    facts: linkedFacts,
    sectionSourceIds: sectionMap,
  };
}

/**
 * Presentation DTO: compact section citations + full All Sources list.
 */
export function buildSectionCitationPresentation(linked = {}, opts = {}) {
  const sources = linked.sources || [];
  const byId = new Map(sources.map((s) => [s.sourceId, s]));
  const publicOnly = opts.excludeHotelPrivate === true;
  const opportunityTitle = text(opts.opportunityTitle);

  function rowsFor(sectionId) {
    const ids = linked.sectionSourceIds?.[sectionId] || [];
    return dedupeSourcesForSectionButton(
      ids
        .map((id) => byId.get(id))
        .filter(Boolean)
        .filter((s) => !(publicOnly && s.visibility === "HOTEL_PRIVATE"))
        .map((s) => ({
          sourceId: s.sourceId,
          url: s.url,
          title: displayTitleForSource(s, { opportunityTitle }),
          publisher: s.publisher,
          sourceType: s.sourceType,
          associatedFactIds: s.associatedFactIds,
          visibility: s.visibility,
        })),
      { opportunityTitle },
    ).map((row) => {
      const full = ids
        .map((id) => byId.get(id))
        .find((s) => s && text(s.url).toLowerCase().replace(/\/$/, "") === row.url.toLowerCase().replace(/\/$/, ""));
      return {
        ...row,
        sourceId: full?.sourceId || null,
        publisher: full?.publisher || null,
        sourceType: full?.sourceType || null,
        associatedFactIds: full?.associatedFactIds || [],
        visibility: full?.visibility || "PUBLIC",
      };
    });
  }

  const sectionCitations = {};
  for (const sec of DRAWER_SOURCE_SECTIONS) {
    if (sec === "all_sources") continue;
    sectionCitations[sec] = rowsFor(sec);
  }

  const allSources = sources
    .filter((s) => !(publicOnly && s.visibility === "HOTEL_PRIVATE"))
    .map((s) => ({
      sourceId: s.sourceId,
      url: s.url,
      title: displayTitleForSource(s, { opportunityTitle }),
      publisher: s.publisher,
      sourceType: s.sourceType,
      publicationDate: s.publicationDate,
      researchVerificationDate: s.researchVerificationDate,
      associatedFactIds: s.associatedFactIds,
      drawerSections: s.drawerSections.filter((d) => d !== "all_sources"),
      visibility: s.visibility,
      corroborationCount: s.corroborationCount,
      verificationStatus: s.verificationStatus,
      availabilityStatus: s.availabilityStatus,
      artifactRef: s.artifactRef,
    }));

  return {
    version: GDI_COMMERCIAL_SOURCE_REGISTER_V1,
    sectionCitations,
    allSources,
  };
}

/**
 * Soft persistence probe — does not write. Reports whether register would survive slim.
 */
export function assessSourceRegisterPersistence(registerPresentation = {}, slimSources = []) {
  const all = registerPresentation.allSources || [];
  const slimSet = new Set(
    (slimSources || []).map((u) => text(typeof u === "string" ? u : u?.url).toLowerCase().replace(/\/$/, "")),
  );
  const retained = [];
  const dropped = [];
  for (const s of all) {
    const key = text(s.url).toLowerCase().replace(/\/$/, "");
    if (!key) continue;
    if (slimSet.has(key) || slimSet.size === 0) retained.push(s.sourceId);
    else dropped.push({ sourceId: s.sourceId, url: s.url });
  }
  return {
    assessed: true,
    certified: false,
    note:
      slimSet.size === 0
        ? "No slim payload provided — durability gap reported, not certified"
        : dropped.length
          ? "Some source URLs absent from slim probe — preserve via durable research store ref"
          : "All register URLs present in slim probe (URL-only; metadata may still require durable ref)",
    retainedSourceIds: retained,
    droppedFromSlim: dropped,
    durableRefRequired: dropped.length > 0 || all.some((s) => s.artifactRef),
    // Slice 2A slim allowlist keeps URL evidence rows, not full fact↔source register
    knownGaps: [
      "commercialSourceRegister / sectionCitations / fact.sourceIds are not in Slice 2A slim allowlist",
      "Slim may keep evidence URLs only (title/publisher/fact links may drop)",
      "Full register must live in durable research store + durableRef until slim contract is extended (founder approval)",
    ],
  };
}

export default {
  GDI_COMMERCIAL_SOURCE_REGISTER_V1,
  DRAWER_SOURCE_SECTIONS,
  buildCommercialSourceRegister,
  linkFactsToSourceRegister,
  buildSectionCitationPresentation,
  assessSourceRegisterPersistence,
  inferFactDrawerSections,
  stableCommercialSourceId,
  displayTitleForSource,
  dedupeSourcesForSectionButton,
};
