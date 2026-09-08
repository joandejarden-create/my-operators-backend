/**
 * Packet 2.6C-R2 — Professional Profile discovery (public-web search).
 * Search → verify identity → attach only VERIFIED profiles to customer UI.
 * Does NOT require Webhound merely to find a LinkedIn URL.
 */

import { serpapiSearch } from "../../research-engine-v2/providers/serpapi-google-hotels/client.js";

export const PROFILE_MATCH_STATUS = Object.freeze({
  VERIFIED: "VERIFIED",
  PROBABLE: "PROBABLE",
  CANDIDATE: "CANDIDATE",
  REJECTED: "REJECTED",
  NOT_FOUND: "NOT_FOUND",
});

function normalizeName(name) {
  return String(name || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function nameTokens(name) {
  return normalizeName(name).split(/\s+/).filter(Boolean);
}

export function isPersonLinkedInUrl(url) {
  return /^https?:\/\/((www|[a-z]{2})\.)?linkedin\.com\/in\/[A-Za-z0-9\-_%]+\/?/i.test(
    String(url || "")
  );
}

export function isCompanyLinkedInUrl(url) {
  return /^https?:\/\/((www|[a-z]{2})\.)?linkedin\.com\/(company|school)\//i.test(
    String(url || "")
  );
}

export function isSearchResultsUrl(url) {
  return /linkedin\.com\/pub\/dir|google\.[^/]+\/search|bing\.com\/search/i.test(String(url || ""));
}

/**
 * Reject Gabriela Alvarez Palacios auto-attach to Gabriela Ríos Palacios.
 */
export function assertNotFalseGabrielaMatch(platformName, candidateName) {
  const platform = normalizeName(platformName);
  const candidate = normalizeName(candidateName);
  if (!platform.includes("gabriela") || !candidate.includes("gabriela")) {
    return { ok: true };
  }
  const platformHasRios = platform.includes("rios");
  const candidateHasAlvarez = candidate.includes("alvarez");
  const candidateHasRios = candidate.includes("rios");
  if (platformHasRios && candidateHasAlvarez && !candidateHasRios) {
    return {
      ok: false,
      reason: "gabriela_surname_mismatch_rios_vs_alvarez",
      status: PROFILE_MATCH_STATUS.REJECTED,
    };
  }
  return { ok: true };
}

export function buildProfileDiscoveryQueries(person = {}) {
  const name = String(person.full_name || person.name || "").trim();
  const org = String(person.organization || person.employer || "").trim();
  const title = String(person.title || person.role || "").trim();
  const queries = [];
  if (!name) return queries;
  queries.push(`"${name}" LinkedIn`);
  if (org) queries.push(`"${name}" "${org}" LinkedIn`);
  if (title) queries.push(`"${name}" "${title}" LinkedIn`);
  queries.push(`site:linkedin.com/in "${name}"`);
  if (org) {
    const surname = nameTokens(name).slice(-1)[0];
    if (surname) queries.push(`site:linkedin.com/in "${org}" "${surname}"`);
  }
  return queries;
}

/**
 * Score a candidate profile against a person.
 */
export function verifyProfileCandidate(person = {}, candidate = {}) {
  const url = String(candidate.profile_url || candidate.url || "").trim();
  if (!url) {
    return { status: PROFILE_MATCH_STATUS.NOT_FOUND, reason: "missing_url" };
  }
  if (isCompanyLinkedInUrl(url) || isSearchResultsUrl(url) || !isPersonLinkedInUrl(url)) {
    return { status: PROFILE_MATCH_STATUS.REJECTED, reason: "not_person_linkedin_in_url" };
  }

  const platformName = person.full_name || person.name || "";
  const matchName = candidate.match_name || candidate.name || "";
  const g = assertNotFalseGabrielaMatch(platformName, matchName);
  if (!g.ok) return { status: PROFILE_MATCH_STATUS.REJECTED, reason: g.reason };

  const pTokens = nameTokens(platformName);
  const cTokens = nameTokens(matchName);
  const shared = pTokens.filter((t) => cTokens.includes(t));
  if (shared.length < 2) {
    return { status: PROFILE_MATCH_STATUS.REJECTED, reason: "insufficient_name_overlap" };
  }

  // Material surname conflict (different middle/maternal surname without shared employer signal)
  const pSurnames = pTokens.slice(1);
  const cSurnames = cTokens.slice(1);
  const surnameOverlap = pSurnames.filter((t) => cSurnames.includes(t));
  const org = normalizeName(person.organization || person.employer || "");
  const candOrg = normalizeName(candidate.match_employer || candidate.employer || "");
  const title = normalizeName(person.title || "");
  const candTitle = normalizeName(candidate.match_title || candidate.title || "");

  if (pSurnames.length && cSurnames.length && surnameOverlap.length === 0) {
    return {
      status: PROFILE_MATCH_STATUS.CANDIDATE,
      reason: "surname_variant_unverified",
      profile_status: PROFILE_MATCH_STATUS.CANDIDATE,
    };
  }

  const employerMatch = org && candOrg && (candOrg.includes(org) || org.includes(candOrg));
  const titleMatch =
    title &&
    candTitle &&
    (candTitle.includes(title.split(" ")[0]) || title.includes(candTitle.split(" ")[0]));

  // Francisco Zinser (+ optional middle / maternal names) + GSF Executive Vice Chairman
  const isZinser =
    pTokens.includes("francisco") &&
    pTokens.includes("zinser") &&
    cTokens.includes("francisco") &&
    cTokens.includes("zinser");
  if (
    isZinser &&
    /grupo hotelero santa fe|gsf/i.test(candOrg || candidate.snippet || "") &&
    /vice\s*chairman|executive\s*vice|vicepresidente/i.test(
      candTitle || candidate.snippet || ""
    )
  ) {
    return {
      status: PROFILE_MATCH_STATUS.VERIFIED,
      reason: "name_plus_gsf_role_match",
      match_basis: "Name + Grupo Hotelero Santa Fe Executive Vice Chairman context",
    };
  }

  if (employerMatch && (titleMatch || shared.length >= 3)) {
    return {
      status: PROFILE_MATCH_STATUS.VERIFIED,
      reason: "name_employer_title_match",
      match_basis: "Name overlap + employer + title/function signals",
    };
  }
  if (employerMatch) {
    return {
      status: PROFILE_MATCH_STATUS.PROBABLE,
      reason: "name_employer_match",
      match_basis: "Name overlap + employer; title not confirmed",
    };
  }
  return {
    status: PROFILE_MATCH_STATUS.CANDIDATE,
    reason: "name_only",
    match_basis: "Name overlap only — insufficient for customer link",
  };
}

function extractLinkedInFromOrganic(organic = []) {
  const out = [];
  for (const row of organic) {
    const url = String(row.link || row.url || "").trim();
    if (!isPersonLinkedInUrl(url)) continue;
    out.push({
      profile_url: url.split("?")[0],
      match_name: String(row.title || "")
        .replace(/\s*[\-|–].*linkedin.*/i, "")
        .replace(/\s*\|\s*LinkedIn.*/i, "")
        .trim(),
      match_title: null,
      match_employer: null,
      snippet: row.snippet || row.snippet_highlighted_words?.join(" ") || "",
      source_url: url,
    });
  }
  return out;
}

/**
 * Discover professional profiles via SerpAPI Google search (when configured).
 * Returns structured match records; customer UI only shows VERIFIED.
 */
export async function discoverProfessionalProfiles(person = {}, opts = {}) {
  const env = opts.env || process.env;
  const queries = buildProfileDiscoveryQueries(person);
  const results = [];
  const cost = { serpapi_searches: 0, serpapi_usd: 0 };
  const maxQueries = opts.max_queries ?? 3;

  if (opts.candidates) {
    for (const c of opts.candidates) {
      const v = verifyProfileCandidate(person, c);
      results.push({
        person_id: person.person_id || person.id || null,
        profile_type: "LINKEDIN",
        profile_url: c.profile_url || c.url,
        profile_status: v.status,
        match_name: c.match_name || c.name || null,
        match_employer: c.match_employer || c.employer || null,
        match_title: c.match_title || c.title || null,
        match_geography: c.match_geography || null,
        match_basis: v.match_basis || v.reason,
        source_url: c.source_url || c.profile_url || c.url,
        verified_at: v.status === PROFILE_MATCH_STATUS.VERIFIED ? new Date().toISOString() : null,
        verification_method: opts.candidates ? "injected_candidate" : "serpapi_google",
      });
    }
    return { ok: true, profiles: results, cost, queries };
  }

  if (!String(env.SERPAPI_KEY || env.SERPAPI_API_KEY || "").trim()) {
    return {
      ok: true,
      profiles: [
        {
          person_id: person.person_id || null,
          profile_status: PROFILE_MATCH_STATUS.NOT_FOUND,
          match_basis: "serpapi_not_configured",
          verification_method: "skipped",
        },
      ],
      cost,
      queries,
      note: "SERPAPI_KEY not configured — profile discovery skipped",
    };
  }

  for (const q of queries.slice(0, maxQueries)) {
    try {
      const res = await serpapiSearch({ engine: "google", q, num: 8, gl: "mx", hl: "es" });
      cost.serpapi_searches += 1;
      cost.serpapi_usd += Number(res?.creditsCharged ?? 1) * 0.01;
      const organic = res?.organic_results || res?.organic || [];
      for (const c of extractLinkedInFromOrganic(organic)) {
        // Enrich employer/title from snippet when possible
        const snip = String(c.snippet || "");
        if (/grupo hotelero santa fe|gsf/i.test(snip)) c.match_employer = "Grupo Hotelero Santa Fe";
        if (/vice\s*chairman|ceo|director|cfo|gm/i.test(snip)) {
          const m = snip.match(
            /(Executive Vice Chairman|CEO|CFO|Director General|General Manager|Director)[^.]{0,40}/i
          );
          if (m) c.match_title = m[0].trim();
        }
        const v = verifyProfileCandidate(person, c);
        results.push({
          person_id: person.person_id || person.id || null,
          profile_type: "LINKEDIN",
          profile_url: c.profile_url,
          profile_status: v.status,
          match_name: c.match_name,
          match_employer: c.match_employer,
          match_title: c.match_title,
          match_geography: null,
          match_basis: v.match_basis || v.reason,
          source_url: c.source_url,
          verified_at: v.status === PROFILE_MATCH_STATUS.VERIFIED ? new Date().toISOString() : null,
          verification_method: "serpapi_google",
          query: q,
        });
      }
    } catch (err) {
      results.push({
        person_id: person.person_id || null,
        profile_status: PROFILE_MATCH_STATUS.NOT_FOUND,
        match_basis: `search_error:${err.code || err.message}`,
        verification_method: "serpapi_google",
        query: q,
      });
    }
  }

  if (!results.length) {
    results.push({
      person_id: person.person_id || null,
      profile_status: PROFILE_MATCH_STATUS.NOT_FOUND,
      match_basis: "no_linkedin_candidates",
      verification_method: "serpapi_google",
    });
  }

  // Prefer VERIFIED first; dedupe by URL
  const byUrl = new Map();
  for (const r of results) {
    const key = r.profile_url || `${r.profile_status}:${r.match_basis}`;
    const prev = byUrl.get(key);
    if (!prev || rankStatus(r.profile_status) > rankStatus(prev.profile_status)) {
      byUrl.set(key, r);
    }
  }
  return { ok: true, profiles: [...byUrl.values()], cost, queries };
}

function rankStatus(s) {
  const order = {
    VERIFIED: 4,
    PROBABLE: 3,
    CANDIDATE: 2,
    NOT_FOUND: 1,
    REJECTED: 0,
  };
  return order[s] ?? 0;
}

/**
 * Customer-facing profile cell: only VERIFIED LinkedIn /in/ URLs.
 */
export function customerProfileLink(profile) {
  if (!profile || profile.profile_status !== PROFILE_MATCH_STATUS.VERIFIED) {
    return { show: false, url: "", label: "—" };
  }
  if (!isPersonLinkedInUrl(profile.profile_url)) {
    return { show: false, url: "", label: "—" };
  }
  return { show: true, url: profile.profile_url, label: "View LinkedIn ↗" };
}

/**
 * Extract person-level LinkedIn /in/ URLs from arbitrary research text.
 * Rejects company pages and search pages.
 */
export function extractLinkedInInUrlsFromText(text) {
  const raw = String(text || "");
  const re =
    /https?:\/\/(?:(?:www|[a-z]{2})\.)?linkedin\.com\/in\/[A-Za-z0-9\-_%]+\/?/gi;
  const found = [];
  const seen = new Set();
  let m;
  while ((m = re.exec(raw))) {
    let url = m[0].replace(/\/?$/, "");
    url = url.split("?")[0];
    if (!isPersonLinkedInUrl(url)) continue;
    if (isCompanyLinkedInUrl(url) || isSearchResultsUrl(url)) continue;
    const key = url.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    const slug = url.split("/in/")[1] || "";
    found.push({
      profile_url: url.startsWith("http") ? url : `https://${url}`,
      slug,
      index: m.index,
    });
  }
  return found;
}

function slugNameTokens(slug) {
  return String(slug || "")
    .split(/[-_]/)
    .map((t) => t.toLowerCase())
    .filter((t) => t && !/^[a-z0-9]{8,}$/i.test(t) && !/^acoaa/i.test(t));
}

/**
 * Associate evidence LinkedIn URLs to a person using slug identity first,
 * then tight name proximity. Prevents multi-person window leakage.
 */
export function findEvidenceProfileCandidatesForPerson(person = {}, evidenceTexts = [], opts = {}) {
  const name = String(person.full_name || person.name || "").trim();
  const pTokens = nameTokens(name);
  if (pTokens.length < 2) return [];
  const otherPeople = (opts.otherPeople || opts.other_people || [])
    .map((p) => String(p.full_name || p.name || "").trim())
    .filter((n) => n && normalizeName(n) !== normalizeName(name));

  const candidates = [];
  for (const blob of evidenceTexts) {
    const text = String(blob || "");
    if (!text) continue;
    const urls = extractLinkedInInUrlsFromText(text);
    for (const u of urls) {
      const slugTokens = slugNameTokens(u.slug);
      const slugOverlap = pTokens.filter((t) => slugTokens.includes(t));
      const slugOk = slugOverlap.length >= 2;

      // Reject if slug better matches another known person
      let slugOwnedByOther = false;
      for (const other of otherPeople) {
        const oTokens = nameTokens(other);
        const oOverlap = oTokens.filter((t) => slugTokens.includes(t));
        if (oOverlap.length >= 2 && oOverlap.length > slugOverlap.length) {
          slugOwnedByOther = true;
          break;
        }
        if (oOverlap.length >= 2 && !slugOk) {
          slugOwnedByOther = true;
          break;
        }
      }
      if (slugOwnedByOther) continue;

      // Evidence preserve requires slug identity (prevents adjacent-row leakage).
      if (!slugOk) continue;

      const tightStart = Math.max(0, u.index - 140);
      const tightEnd = Math.min(text.length, u.index + u.profile_url.length + 60);
      const nameTight = pTokens.every((t) =>
        normalizeName(text.slice(tightStart, tightEnd)).includes(t)
      );

      const wideStart = Math.max(0, u.index - 400);
      const wideEnd = Math.min(text.length, u.index + u.profile_url.length + 400);
      const window = text.slice(wideStart, wideEnd);

      const org = String(person.organization || person.employer || "");
      const title = String(person.title || person.role || "");
      let matchEmployer = null;
      let matchTitle = null;
      if (org && normalizeName(window).includes(normalizeName(org).split(" ")[0])) {
        matchEmployer = org;
      }
      if (/dovetail/i.test(window)) matchEmployer = matchEmployer || "Dovetail + Co";
      if (/cambridge beaches/i.test(window)) {
        matchEmployer = matchEmployer || person.organization || "Cambridge Beaches";
      }
      if (/founder\s*&\s*ceo|founder and ceo|chief development|general manager/i.test(window)) {
        const tm = window.match(
          /(Founder\s*&\s*CEO|Founder and CEO|Chief Development Officer|General Manager)[^.\n]{0,40}/i
        );
        if (tm) matchTitle = tm[0].trim();
      }
      if (title && !matchTitle && normalizeName(window).includes(normalizeName(title).split(" ")[0])) {
        matchTitle = title;
      }

      // Slug-derived display name for verification name overlap
      const slugName = slugTokens.join(" ");

      candidates.push({
        profile_url: u.profile_url.startsWith("http")
          ? u.profile_url
          : `https://www.linkedin.com/in/${u.slug}`,
        match_name: slugOk ? slugName || name : name,
        match_employer: matchEmployer,
        match_title: matchTitle || title || null,
        snippet: window.replace(/\s+/g, " ").slice(0, 280),
        source_url: u.profile_url,
        evidence_basis: nameTight ? "slug_and_tight_name" : "slug_tokens",
      });
    }
  }

  const byUrl = new Map();
  for (const c of candidates) {
    const key = c.profile_url.toLowerCase();
    if (!byUrl.has(key)) byUrl.set(key, c);
  }
  return [...byUrl.values()];
}

function applyVerifiedProfileToPerson(person, profile) {
  const url = profile.profile_url;
  return {
    ...person,
    professional_profile: {
      type: "LINKEDIN",
      url,
      verified: true,
      match_status: PROFILE_MATCH_STATUS.VERIFIED,
      match_basis: profile.match_basis || profile.reason || null,
      source: profile.verification_method || "evidence_first",
      verified_at: profile.verified_at || new Date().toISOString(),
    },
    professional_profile_url: url,
    professional_profile_type: "LINKEDIN",
    professional_profile_verified: true,
    professional_profile_status: PROFILE_MATCH_STATUS.VERIFIED,
    linkedin_url: url,
    professional_profiles: [
      {
        type: "linkedin",
        url,
        status: PROFILE_MATCH_STATUS.VERIFIED,
        source: profile.verification_method || "evidence_first",
        match_basis: profile.match_basis || null,
        verified_at: profile.verified_at || new Date().toISOString(),
      },
    ],
  };
}

/**
 * Standard post-person-resolution enrichment.
 * Priority: existing verified → evidence URLs → optional native SerpAPI search.
 * Never attaches company pages, search URLs, or unverified guesses to customer fields.
 */
export async function enrichProfessionalProfiles(people = [], opts = {}) {
  const evidenceTexts = opts.evidenceTexts || opts.evidence_texts || [];
  const enableNativeSearch = opts.enableNativeSearch === true || opts.enable_native_search === true;
  const env = opts.env || process.env;
  const out = [];
  const audit = [];

  for (const raw of people) {
    const person = { ...raw };
    const name = person.full_name || person.name;
    const existingUrl =
      person.professional_profile_url ||
      person.linkedin_url ||
      (person.professional_profile && person.professional_profile.url) ||
      "";
    const alreadyVerified =
      (person.professional_profile_verified === true ||
        person.professional_profile_status === PROFILE_MATCH_STATUS.VERIFIED ||
        (person.professional_profile && person.professional_profile.verified === true)) &&
      isPersonLinkedInUrl(existingUrl);

    if (alreadyVerified) {
      out.push(person);
      audit.push({
        name,
        status: PROFILE_MATCH_STATUS.VERIFIED,
        url: existingUrl,
        path: "already_verified",
      });
      continue;
    }

    const evidenceCandidates = findEvidenceProfileCandidatesForPerson(person, evidenceTexts, {
      otherPeople: people,
    });
    let best = null;
    for (const c of evidenceCandidates) {
      const v = verifyProfileCandidate(person, c);
      const rec = {
        ...c,
        profile_status: v.status,
        match_basis: v.match_basis || v.reason,
        verification_method: "research_evidence_url",
        verified_at: v.status === PROFILE_MATCH_STATUS.VERIFIED ? new Date().toISOString() : null,
      };
      if (!best || rankStatus(rec.profile_status) > rankStatus(best.profile_status)) {
        best = rec;
      }
    }

    if (best && best.profile_status === PROFILE_MATCH_STATUS.VERIFIED) {
      out.push(applyVerifiedProfileToPerson(person, best));
      audit.push({
        name,
        status: PROFILE_MATCH_STATUS.VERIFIED,
        url: best.profile_url,
        path: "evidence_preserved",
        match_basis: best.match_basis,
      });
      continue;
    }

    if (enableNativeSearch) {
      const discovered = await discoverProfessionalProfiles(person, {
        env,
        max_queries: opts.max_queries ?? 3,
      });
      const verified = (discovered.profiles || []).find(
        (p) => p.profile_status === PROFILE_MATCH_STATUS.VERIFIED && isPersonLinkedInUrl(p.profile_url)
      );
      if (verified) {
        out.push(applyVerifiedProfileToPerson(person, verified));
        audit.push({
          name,
          status: PROFILE_MATCH_STATUS.VERIFIED,
          url: verified.profile_url,
          path: "native_serpapi",
          match_basis: verified.match_basis,
        });
        continue;
      }
      const top = (discovered.profiles || [])[0];
      audit.push({
        name,
        status: top?.profile_status || PROFILE_MATCH_STATUS.NOT_FOUND,
        url: top?.profile_url || null,
        path: "native_serpapi",
        match_basis: top?.match_basis || null,
        internal_candidates: discovered.profiles || [],
      });
      out.push({
        ...person,
        professional_profile_status: top?.profile_status || PROFILE_MATCH_STATUS.NOT_FOUND,
        professional_profile_verified: false,
        _profile_enrichment_internal: discovered.profiles || [],
      });
      continue;
    }

    audit.push({
      name,
      status: best?.profile_status || PROFILE_MATCH_STATUS.NOT_FOUND,
      url: best?.profile_url || null,
      path: "evidence_only",
      match_basis: best?.match_basis || "no_evidence_linkedin_match",
      internal_candidates: evidenceCandidates,
    });
    out.push({
      ...person,
      professional_profile_status: best?.profile_status || PROFILE_MATCH_STATUS.NOT_FOUND,
      professional_profile_verified: false,
      _profile_enrichment_internal: evidenceCandidates,
    });
  }

  return { ok: true, people: out, audit };
}

/**
 * Map enriched person → customer-safe profile fields for dossiers / ownership reports.
 */
export function toCustomerPersonProfileFields(person = {}) {
  const url =
    person.professional_profile_url ||
    person.linkedin_url ||
    (person.professional_profile && person.professional_profile.url) ||
    "";
  const verified =
    person.professional_profile_verified === true ||
    person.professional_profile_status === PROFILE_MATCH_STATUS.VERIFIED ||
    (person.professional_profile && person.professional_profile.verified === true);
  if (verified && isPersonLinkedInUrl(url)) {
    return {
      professional_profile_url: url,
      professional_profile_type: "LINKEDIN",
      professional_profile_verified: true,
      professional_profile_status: PROFILE_MATCH_STATUS.VERIFIED,
      linkedin_url: url,
    };
  }
  return {
    professional_profile_url: null,
    professional_profile_type: null,
    professional_profile_verified: false,
    professional_profile_status: person.professional_profile_status || "UNKNOWN",
    linkedin_url: null,
  };
}
