/**
 * Real direct_public executor for maturation.
 * Source-grounded only — never invents people, dates, or demand from model text alone.
 */

import { createHash } from "node:crypto";
import { fetchOfficialPage } from "../official-page-fetch.js";
import { PROVIDER_KIND } from "./provider-cost-router-v1.js";

const EMAIL_RE =
  /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g;
const PHONE_RE =
  /(?:\+|00)?[\d][\d\s().-]{7,}\d/g;
const FUTURE_YEAR_RE = /\b(202[7-9]|203[0-2])\b/g;
const ATTENDEE_RE =
  /\b(\d{2,5})\s*(?:\+|plus)?\s*(?:attendees?|delegates?|participants?|guests?)\b/i;
const ROOM_BLOCK_RE =
  /\b(\d{2,4})\s*(?:room|guest.?room|key)s?\b/i;

function uniq(arr) {
  return [...new Set((arr || []).filter(Boolean))];
}

function fingerprintUrl(url) {
  return createHash("sha1").update(String(url || "")).digest("hex").slice(0, 12);
}

/**
 * Prefer functional department inboxes over arbitrary first-match emails.
 * Proven regression: CONTACT_ROUTE took emails[0], often a footer/webmaster address,
 * while events@/meetings@/groups@ appeared later on the same page.
 */
export function selectPreferredPublicEmail(emails = []) {
  const list = uniq((emails || []).map((e) => String(e || "").toLowerCase().trim()).filter(Boolean));
  if (!list.length) return null;
  const functionalStrong = list.find((e) =>
    /^(events?|meetings?|groups?|housing|sales|booking|reservations?|catering|banquets?|travel|workplace|facilities|procurement)@/i.test(e),
  );
  if (functionalStrong) return functionalStrong;
  const functionalGeneral = list.find((e) =>
    /^(info|contact|hello|office|inquiry|enquiry|admin)@/i.test(e),
  );
  if (functionalGeneral) return functionalGeneral;
  const personish = list.find((e) => {
    const local = e.split("@")[0] || "";
    return /^[a-z]+[._-][a-z]+$/i.test(local) && !/noreply|no-reply|donotreply/i.test(local);
  });
  if (personish) return personish;
  return list[0];
}

/**
 * Extract only what appears in page text — no invented fields.
 */
export function extractPublicEvidenceFromText(text = "", url = "") {
  const blob = String(text || "");
  const emails = uniq((blob.match(EMAIL_RE) || []).map((e) => e.toLowerCase()))
    .filter((e) => !/example\.(com|org)|sentry\.|wixpress|schema\.org/i.test(e))
    .slice(0, 8);
  const phones = uniq((blob.match(PHONE_RE) || []).map((p) => p.replace(/\s+/g, " ").trim()))
    .filter((p) => {
      const digits = p.replace(/\D/g, "");
      if (digits.length < 10 || digits.length > 15) return false;
      // Reject year/date chains mistaken for phones (e.g. "2025 2024 2023 2022")
      if (/^(19|20)\d{2}(\s*(19|20)\d{2})+$/.test(p.trim())) return false;
      if (/^\d{4}([-\s./]\d{2}){1,2}$/.test(p.trim())) return false;
      return true;
    })
    .slice(0, 3);
  const years = uniq(blob.match(FUTURE_YEAR_RE) || []);
  const attendeeMatch = blob.match(ATTENDEE_RE);
  const roomMatch = blob.match(ROOM_BLOCK_RE);
  const hasHousing =
    /housing|hotel block|room block|accommodation|official hotel/i.test(blob);
  const hasStaff =
    /director of (events|meetings)|head of events|meetings manager|event manager/i.test(
      blob
    );
  const hasContactForm = /contact us|enquiry|inquiry|get in touch|email us/i.test(blob);

  const preferredEmail = selectPreferredPublicEmail(emails);
  return {
    url,
    emails: preferredEmail
      ? [preferredEmail, ...emails.filter((e) => e !== preferredEmail)].slice(0, 5)
      : emails.slice(0, 5),
    preferredEmail,
    phones,
    futureYears: years,
    attendeeCount: attendeeMatch ? Number(attendeeMatch[1]) : null,
    roomHint: roomMatch ? Number(roomMatch[1]) : null,
    hasHousingLanguage: hasHousing,
    hasStaffRoleLanguage: hasStaff,
    hasContactLanguage: hasContactForm,
    charCount: blob.length,
    // Keep an excerpt so ORS public buyer intelligence can scan the same page text
    // (runPublicBuyerIntelligencePass requires statement/quote/text length ≥ 40).
    excerpt: blob.replace(/\s+/g, " ").trim().slice(0, 4000),
  };
}

/**
 * Build deepPatch only from extracted source facts.
 */
export function buildEvidencePatchFromExtraction(extracted, blockerKey) {
  const deepPatch = {};
  const evidence = [];
  let newMaterialEvidence = false;

  if (!extracted?.url || extracted.charCount < 40) {
    return { deepPatch, evidence, newMaterialEvidence };
  }

  evidence.push({
    sourceFamily: "official",
    publisher: "ORGANIZATION_OR_PUBLIC_PAGE",
    url: extracted.url,
    kind: "PUBLIC_PAGE_EXTRACT",
    isMaterialDemandEvidence: true,
    retrievedAt: new Date().toISOString(),
    // Required for ORS V2 public buyer pass (CI extract on evidence text).
    quote: extracted.excerpt || null,
    statement: extracted.excerpt || null,
    text: extracted.excerpt || null,
    excerpt: extracted.excerpt || null,
  });

  if (blockerKey === "future_demand" && extracted.futureYears.length) {
    deepPatch.futureDemand = {
      supported: true,
      dateWindow: extracted.futureYears.join(" "),
      sourceUrl: extracted.url,
    };
    newMaterialEvidence = true;
  }

  if (
    blockerKey === "demand_scale" &&
    (extracted.attendeeCount || extracted.roomHint || extracted.hasHousingLanguage)
  ) {
    deepPatch.demandScale = {
      supported: true,
      hotelRelevant: true,
      estimatedPeakRooms: extracted.roomHint || null,
      attendeeCount: extracted.attendeeCount || null,
      sourceUrl: extracted.url,
    };
    newMaterialEvidence = true;
  }

  if (blockerKey === "contact_route") {
    const contact = {};
    const preferred = extracted.preferredEmail || selectPreferredPublicEmail(extracted.emails || []);
    if (preferred) {
      contact.businessEmail = preferred;
      contact.emailSelection = "PREFERRED_FUNCTIONAL_OR_BEST_AVAILABLE";
    }
    if (extracted.phones[0]) contact.businessPhone = extracted.phones[0];
    if (extracted.hasContactLanguage && !contact.businessEmail) {
      contact.contactFormUrl = extracted.url;
    }
    if (Object.keys(contact).length) {
      deepPatch.contact = contact;
      newMaterialEvidence = true;
    }
  }

  if (blockerKey === "buyer_identity" && extracted.hasStaffRoleLanguage) {
    // Role language alone does NOT invent a person name — only flags role evidence present
    deepPatch.buyerRoleEvidence = {
      supported: true,
      sourceUrl: extracted.url,
      note: "staff_role_language_on_page_person_name_not_inferred",
    };
    // Do not set personName from regex — prevents WHO invent
  }

  if (blockerKey === "corroboration") {
    newMaterialEvidence = true;
  }

  if (blockerKey === "hotel_fit" && /london|marylebone|portman|west end/i.test(
    `${extracted.url} ${extracted.charCount}`
  )) {
    // Weak: only if URL/domain already hotel-market scoped — leave fit to assess_fit usually
  }

  return { deepPatch, evidence, newMaterialEvidence };
}

/**
 * Resolve URLs to fetch: prior evidence + optional search results.
 */
async function resolveCandidateUrls({ opportunity, action, options = {} }) {
  const urls = uniq([
    ...(opportunity.evidence || []).map((e) => e.url).filter(Boolean),
    opportunity.officialUrl,
    opportunity.sourceUrl,
    opportunity.website,
    opportunity.organizationWebsite,
  ]).filter((u) => /^https?:\/\//i.test(u));

  if (urls.length || options.skipSearch) return urls.slice(0, 4);

  // Optional SerpAPI — only when key present and queries provided
  const queries = action?.queries || [];
  if (!queries.length || options.skipSearch) return urls;

  try {
    const { serpapiSearch } = await import(
      "../../research-engine-v2/providers/serpapi-google-hotels/client.js"
    );
    const q = String(queries[0] || "").slice(0, 200);
    const serp = await serpapiSearch({
      engine: "google",
      q,
      num: 5,
      hl: "en",
      gl: "uk",
    });
    const organic = serp?.organic_results || serp?.organic || [];
    for (const row of organic) {
      const link = row.link || row.url;
      if (link && /^https?:\/\//i.test(link)) urls.push(link);
    }
  } catch (err) {
    // Missing key or network — continue with known URLs only
    if (options.throwOnSearchError) throw err;
  }

  return uniq(urls).slice(0, 5);
}

/**
 * @returns researcher.execute compatible result
 */
export async function executeDirectPublicResearch({
  opportunity,
  blocker,
  action,
  options = {},
} = {}) {
  const blockerKey = blocker?.key || blocker;
  const started = Date.now();
  const urls = await resolveCandidateUrls({ opportunity, action, options });

  if (!urls.length) {
    return {
      newMaterialEvidence: false,
      provider: PROVIDER_KIND.DIRECT_PUBLIC,
      sourceFamilies: ["web"],
      costUsd: 0,
      disposition: "NO_URLS_TO_FETCH",
      notes: "No prior evidence URLs and search unavailable/empty",
      evidence: [],
    };
  }

  const attempted = new Set(
    (opportunity.researchAttempts || [])
      .filter((a) => a.provider === PROVIDER_KIND.DIRECT_PUBLIC)
      .flatMap((a) => a.evidenceIds || [])
  );

  let mergedPatch = {};
  const evidence = [];
  let newMaterial = false;
  const fetched = [];

  for (const url of urls) {
    const id = `pub_${fingerprintUrl(url)}`;
    if (attempted.has(id) && !options.forceRefetch) continue;

    let page;
    try {
      page = await fetchOfficialPage(url, {
        allowRender: false,
        timeoutMs: options.fetchTimeoutMs || 15000,
      });
    } catch (err) {
      fetched.push({ url, ok: false, error: err?.message || String(err) });
      continue;
    }

    const text =
      page?.text ||
      page?.searchableText ||
      page?.bodyText ||
      htmlToRoughText(page?.html || "");
    const extracted = extractPublicEvidenceFromText(text, url);
    const built = buildEvidencePatchFromExtraction(extracted, blockerKey);
    if (built.newMaterialEvidence) newMaterial = true;
    deepAssign(mergedPatch, built.deepPatch);
    for (const e of built.evidence) {
      evidence.push({ ...e, id });
    }
    fetched.push({
      url,
      ok: true,
      method: page?.method || page?.fetchMethod || null,
      chars: extracted.charCount,
    });

    if (newMaterial && options.stopAfterFirstMaterial !== false) break;
  }

  return {
    newMaterialEvidence: newMaterial,
    provider: PROVIDER_KIND.DIRECT_PUBLIC,
    providerRunId: null,
    sourceFamilies: uniq(evidence.map((e) => e.sourceFamily).concat(["official"])),
    costUsd: 0,
    durationMs: Date.now() - started,
    disposition: newMaterial ? "COMPLETED" : "NO_NEW_MATERIAL_EVIDENCE",
    deepPatch: Object.keys(mergedPatch).length ? mergedPatch : undefined,
    evidence,
    evidenceIds: evidence.map((e) => e.id || e.url).filter(Boolean),
    notes: `Fetched ${fetched.filter((f) => f.ok).length}/${urls.length} public URLs`,
    fetchLog: fetched,
    isCompletionAuthority: false,
  };
}

function htmlToRoughText(html) {
  return String(html || "")
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function deepAssign(target, patch) {
  for (const [k, v] of Object.entries(patch || {})) {
    if (v && typeof v === "object" && !Array.isArray(v)) {
      target[k] = target[k] || {};
      deepAssign(target[k], v);
    } else {
      target[k] = v;
    }
  }
  return target;
}

export function createDirectPublicExecutor(defaults = {}) {
  return {
    async execute(args) {
      return executeDirectPublicResearch({
        ...args,
        options: { ...defaults, ...(args.options || {}) },
      });
    },
  };
}
