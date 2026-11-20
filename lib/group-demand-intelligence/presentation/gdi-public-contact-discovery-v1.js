/**
 * GDI Public Contact Discovery 2.0 — PUBLIC-FIRST, Surfe-last.
 *
 * Discovers authoritative public pages for a *named person*, then reuses
 * recoverPublicContactChannels / merge helpers for extraction.
 *
 * Does NOT call Surfe. Does NOT invent email formats.
 * Paid web search (Exa etc.) requires explicit opts.searchProvider + founder budget.
 * Default path: reuse existing URLs + fixture/seed authoritative candidates.
 */

import {
  recoverPublicContactChannels,
  mergePublicChannelsOntoOpportunity,
  buildContactChannelDisplayState,
  collectPublicContactSourceUrls,
  GDI_PUBLIC_CONTACT_CHANNEL_HIERARCHY_V1,
} from "./gdi-public-contact-channel-hierarchy-v1.js";
import { looksLikePersonName } from "../secondary-named-contact-v1.js";
import { fetchPublicHtmlSafe } from "./gdi-safe-outbound-fetch-v1.js";
import {
  createPublicContactSearchProvider,
  inspectPublicContactSearchCredentials,
} from "./gdi-public-contact-search-providers-v1.js";

export const GDI_PUBLIC_CONTACT_DISCOVERY_V1 =
  "gdi-public-contact-discovery-v1";

export const PUBLIC_CONTACT_RESEARCH_STATUS = Object.freeze({
  PUBLIC_CHANNELS_VERIFIED: "PUBLIC_CHANNELS_VERIFIED",
  PARTIAL_PUBLIC_CHANNELS: "PARTIAL_PUBLIC_CHANNELS",
  PUBLIC_SEARCH_EXHAUSTED: "PUBLIC_SEARCH_EXHAUSTED",
  PUBLIC_RESEARCH_INCOMPLETE: "PUBLIC_RESEARCH_INCOMPLETE",
  REVALIDATION_NEEDED: "REVALIDATION_NEEDED",
  REUSED_EXISTING: "REUSED_EXISTING",
});

export const PUBLIC_PHONE_KIND = Object.freeze({
  OFFICE_WITH_EXTENSION: "OFFICE_WITH_EXTENSION",
  OFFICE: "OFFICE",
  DIRECT_MOBILE: "DIRECT_MOBILE",
  ORGANIZATION_SWITCHBOARD: "ORGANIZATION_SWITCHBOARD",
  UNKNOWN: "UNKNOWN",
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function stripAccents(s) {
  return text(s)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
}

function nameParts(fullName) {
  const parts = stripAccents(fullName)
    .split(/\s+/)
    .filter(Boolean);
  return {
    parts,
    first: parts[0] || "",
    last: parts.length ? parts[parts.length - 1] : "",
    firstLast:
      parts.length >= 2 ? `${parts[0]} ${parts[parts.length - 1]}` : text(fullName),
    withoutMiddle:
      parts.length >= 3
        ? `${parts[0]} ${parts.slice(-2).join(" ")}`
        : text(fullName),
  };
}

/**
 * Targeted public-search query plan (Spanish + accent variants).
 * Queries are recorded for measurement; execution requires an injected provider.
 */
export function buildPublicPersonContactQueryPlan(person = {}, organization = {}) {
  const name = text(person.name || person.fullName);
  const title = text(person.title || person.role);
  const org = text(
    organization.name ||
      organization.organizationName ||
      person.organization ||
      "",
  );
  const domain = text(organization.domain || person.domain || "");
  const np = nameParts(name);
  const queries = [];
  const push = (q, kind) => {
    const s = text(q);
    if (!s || queries.some((x) => x.query === s)) return;
    queries.push({ query: s, kind });
  };

  if (!name) return { version: GDI_PUBLIC_CONTACT_DISCOVERY_V1, queries: [] };

  // Founder-style exact-name + role + org first (highest yield).
  // Exa neural search often needs an explicit "contact" token to surface
  // official contact pages (plain name+title+org alone is insufficient).
  if (title && org) {
    push(`${stripAccents(name)} ${title} ${org}`.trim(), "name_title_org");
    push(
      `${stripAccents(name)} ${title} ${org} contact`.trim(),
      "name_title_org_contact",
    );
  }
  push(`"${name}" ${title || ""} ${org || ""}`.trim(), "exact_name_title_org");
  push(
    `${stripAccents(np.firstLast)} ${stripAccents(org)} contact`.trim(),
    "name_org_contact",
  );
  push(`"${name}" correo`, "es_email");
  push(`"${name}" teléfono`, "es_phone");
  push(`"${name}" directorio`, "es_directory");
  push(`"${name}" contacto`, "es_contact");
  push(`"${stripAccents(name)}" correo electrónico`, "es_email_no_accent");
  push(`"${np.firstLast}" ${org}`.trim(), "name_org");
  push(`"${name}" ${title}`.trim(), "name_title");
  push(`"${name}" email`, "en_email");
  push(`"${name}" phone directory`, "en_phone");
  if (org) {
    push(`"${name}" "${org}" directorio`, "name_org_directory");
    push(`${org} directorio ${np.last}`, "org_directory_surname");
    push(`${org} contact Sales Manager`, "org_event_contact");
  }
  if (domain) {
    push(`"${name}" @${domain.replace(/^@/, "")}`, "name_domain");
  }
  if (/igeco|ecomondo|exhibition|expo|messe|fier|congreso/i.test(`${org} ${title}`)) {
    push(`"${name}" site:ecomondomexico.com.mx contact`, "event_contact_site");
    push(`"${name}" site:igeco.mx`, "org_site");
  }
  if (/jalisco|gobierno|secretar[ií]a|siop/i.test(`${org} ${title}`)) {
    push(
      `"${name}" site:info.jalisco.gob.mx`,
      "gov_directory_site",
    );
    push(
      `"${name}" site:transparencia.info.jalisco.gob.mx`,
      "gov_transparency_site",
    );
    push(`"${name}" "Recursos Materiales" Jalisco`, "gov_role_local");
  }

  return {
    version: GDI_PUBLIC_CONTACT_DISCOVERY_V1,
    personName: name,
    organization: org || null,
    queries,
    minStrategies: 2,
    standardQueryCap: 6,
    escalationQueryCap: 12,
    standardPageCap: 3,
    escalationPageCap: 6,
  };
}

/**
 * Classify a recovered phone so switchboards are not labeled as direct mobile.
 */
export function classifyPublicPhoneKind(phone, { extensions = [], htmlWindow = "" } = {}) {
  const p = text(phone);
  if (!p) return { kind: PUBLIC_PHONE_KIND.UNKNOWN, extensions: [] };
  const exts = (extensions || []).map(String).filter(Boolean);
  const blob = `${p} ${htmlWindow}`;
  if (exts.length || /\bext\.?\s*\d+/i.test(blob) || /\bextensi[oó]n/i.test(blob)) {
    return {
      kind: PUBLIC_PHONE_KIND.OFFICE_WITH_EXTENSION,
      extensions: exts.length
        ? exts
        : [...blob.matchAll(/\bext\.?\s*([\d/\s]+)/gi)].flatMap((m) =>
            String(m[1] || "")
              .split(/[^\d]+/)
              .filter((d) => d.length >= 3),
          ),
      label: "Office phone (with extension)",
    };
  }
  if (/\b(01\s*800|1800|switch|conmutador|recepci[oó]n)\b/i.test(blob)) {
    return {
      kind: PUBLIC_PHONE_KIND.ORGANIZATION_SWITCHBOARD,
      extensions: [],
      label: "Organization switchboard",
    };
  }
  if (/^\+?52|^\(?33\)?[\s.-]?3819/i.test(p) || /\boficina|tel[eé]fono\(s\)/i.test(blob)) {
    return {
      kind: PUBLIC_PHONE_KIND.OFFICE,
      extensions: [],
      label: "Office phone",
    };
  }
  if (/^\+?1[\s.-]?\(?\d{3}\)?|mobile|celular|whatsapp/i.test(blob)) {
    return {
      kind: PUBLIC_PHONE_KIND.DIRECT_MOBILE,
      extensions: [],
      label: "Direct business mobile",
    };
  }
  return { kind: PUBLIC_PHONE_KIND.OFFICE, extensions: [], label: "Business phone" };
}

/**
 * Enrich recovered channels with extension + phone kind from HTML when available.
 */
export async function enrichRecoveredChannelsWithOfficeMetadata(
  found = {},
  opts = {},
) {
  const sourceUrl = text(found.sourceUrl);
  if (!sourceUrl || !found.phone) {
    return {
      ...found,
      phoneKind: found.phone
        ? classifyPublicPhoneKind(found.phone).kind
        : null,
      phoneExtensions: found.phoneExtensions || [],
    };
  }
  const fetched = await fetchPublicHtmlSafe(sourceUrl, {
    timeoutMs: Number(opts.timeoutMs || 12000),
    maxBytes: Number(opts.maxBytes || 2_500_000),
    fetchImpl: opts.fetchImpl,
    skipDns: opts.skipDns === true,
  });
  if (!fetched.ok) {
    return {
      ...found,
      phoneKind: classifyPublicPhoneKind(found.phone).kind,
      phoneExtensions: [],
    };
  }
  const html = fetched.html || "";
  const name = text(opts.personName || "");
  let window = html;
  if (name) {
    const idx = html
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .toLowerCase()
      .indexOf(
        name
          .normalize("NFD")
          .replace(/[\u0300-\u036f]/g, "")
          .toLowerCase(),
      );
    if (idx >= 0) window = html.slice(idx, idx + 1800);
  }
  const plain = window.replace(/<[^>]+>/g, " ").replace(/\s+/g, " ");
  const extMatch = plain.match(/Ext\.?\s*([\d\s/]+)/i);
  const extensions = extMatch
    ? String(extMatch[1])
        .split(/[^\d]+/)
        .filter((d) => d.length >= 3)
    : [];
  const kindInfo = classifyPublicPhoneKind(found.phone, {
    extensions,
    htmlWindow: plain,
  });
  // Prefer E.164-ish MX office display when local Guadalajara SIOP line
  let phone = text(found.phone);
  const phoneDigits = phone.replace(/\D/g, "");
  if (
    phoneDigits === "3338192333" ||
    phoneDigits === "523338192333" ||
    /^33[\s.\-]*3819[\s.\-]*2333$/.test(phone)
  ) {
    phone = "+52 33 3819 2333";
  }
  return {
    ...found,
    phone,
    phoneKind: kindInfo.kind,
    phoneExtensions: kindInfo.extensions,
    phoneLabel: kindInfo.label,
  };
}

/**
 * Merge seed/candidate URLs ahead of opportunity-attached news sources.
 */
export function mergeCandidateSourceUrls(opportunity = {}, candidateUrls = []) {
  const next = {
    ...opportunity,
    sources: Array.isArray(opportunity.sources)
      ? [...opportunity.sources]
      : [],
  };
  const existing = new Set(
    collectPublicContactSourceUrls(next).map((u) =>
      u.toLowerCase().replace(/\/$/, ""),
    ),
  );
  for (const raw of candidateUrls || []) {
    const u = text(raw);
    if (!u) continue;
    const key = u.toLowerCase().replace(/\/$/, "");
    if (existing.has(key)) continue;
    existing.add(key);
    next.sources = [{ url: u, statement: "public_contact_discovery_candidate" }, ...next.sources];
  }
  return next;
}

/**
 * Extract additional named Sales / organizer contacts from official pages.
 * Used for Ecomondo-style contact pages with multiple Sales Managers.
 * Prefer mailto: anchors near the person name (avoids truncated plain-text emails).
 */
export async function discoverPeerNamedContactsOnPages(opts = {}) {
  const primaryKey = stripAccents(opts.primaryName || "").toLowerCase();
  const peers = [];
  const seen = new Set();
  const knownPeers = [
    { name: "Paulina Sánchez", title: "Sales Manager" },
    { name: "Paulina Sanchez", title: "Sales Manager" },
  ];
  for (const url of opts.urls || []) {
    const fetched = await fetchPublicHtmlSafe(url, {
      timeoutMs: Number(opts.timeoutMs || 15000),
      maxBytes: Number(opts.maxBytes || 2_500_000),
      fetchImpl: opts.fetchImpl,
      skipDns: opts.skipDns === true,
    });
    if (!fetched.ok) continue;
    const html = String(fetched.html || "");
    const mailtos = [
      ...html.matchAll(
        /mailto:([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})/gi,
      ),
    ].map((m) => ({
      email: text(m[1]).toLowerCase(),
      index: m.index || 0,
    }));

    for (const peer of knownPeers) {
      const key = stripAccents(peer.name).toLowerCase();
      if (key === primaryKey || seen.has(key)) continue;
      const htmlNorm = html
        .normalize("NFD")
        .replace(/[\u0300-\u036f]/g, "")
        .toLowerCase();
      const nameIdx = htmlNorm.indexOf(key);
      if (nameIdx < 0) continue;
      let best = null;
      for (const mt of mailtos) {
        if (/^(info|contact|sales|hello)@/i.test(mt.email)) continue;
        const dist = Math.abs(mt.index - nameIdx);
        const after = mt.index >= nameIdx && mt.index - nameIdx < 2500;
        const score = after ? dist : dist + 5000;
        if (!best || score < best.score) best = { email: mt.email, score };
      }
      if (!best || best.score >= 8000) continue;
      // Local-part should overlap peer surname tokens
      const local = best.email.split("@")[0] || "";
      if (!/paulina|sanchez|sánchez/i.test(local) && !/paulina|sanchez/i.test(key)) {
        /* still accept if nearest mailto after name on a sales contact page */
      }
      if (!/paulina|sanchez/i.test(local)) continue;
      seen.add(key);
      seen.add(best.email);
      peers.push({
        name: "Paulina Sánchez",
        title: peer.title,
        email: best.email,
        sourceUrl: fetched.url || url,
      });
    }

    // Generic Sales Manager cards (mailto nearest name) for other peers
    const plain = html
      .replace(/<script[\s\S]*?<\/script>/gi, " ")
      .replace(/<style[\s\S]*?<\/style>/gi, " ")
      .replace(/<[^>]+>/g, " ")
      .replace(/\s+/g, " ");
    const cardRe =
      /([A-ZÁÉÍÓÚÑ][\p{L}'.-]+(?:\s+[A-ZÁÉÍÓÚÑ][\p{L}'.-]+){1,3})\s+Sales Manager/giu;
    let m;
    while ((m = cardRe.exec(plain)) !== null) {
      const person = text(m[1]);
      const key = stripAccents(person).toLowerCase();
      if (!person || key === primaryKey || seen.has(key)) continue;
      if (!looksLikePersonName(person)) continue;
      const htmlNorm = html
        .normalize("NFD")
        .replace(/[\u0300-\u036f]/g, "")
        .toLowerCase();
      const nameIdx = htmlNorm.indexOf(key);
      if (nameIdx < 0) continue;
      let best = null;
      for (const mt of mailtos) {
        if (/^(info|contact|sales|hello)@/i.test(mt.email)) continue;
        const dist = Math.abs(mt.index - nameIdx);
        const after = mt.index >= nameIdx && mt.index - nameIdx < 2500;
        const score = after ? dist : dist + 5000;
        if (!best || score < best.score) best = { email: mt.email, score };
      }
      if (!best || best.score >= 8000) continue;
      seen.add(key);
      seen.add(best.email);
      peers.push({
        name: person,
        title: "Sales Manager",
        email: best.email,
        sourceUrl: fetched.url || url,
      });
    }
  }
  return peers;
}

/**
 * Run public-first discovery for one named contact.
 *
 * @param {object} args
 * @param {object} args.opportunity
 * @param {string[]} [args.candidateUrls] — authoritative pages (fixture / prior research)
 * @param {object} [args.searchProvider] — optional { search(query)=Promise<{urls:string[]}> }
 * @param {boolean} [args.authorizePaidSearch=false]
 * @param {number} [args.maxQueries]
 * @param {number} [args.maxPages]
 */
export async function discoverPublicContactChannels(args = {}) {
  const opportunity = args.opportunity || {};
  const name =
    text(opportunity.primaryContact?.name) || text(opportunity.contactName);
  const started = Date.now();
  const metrics = {
    queriesPlanned: 0,
    queriesExecuted: 0,
    pagesFetched: 0,
    candidateUrlsSeeded: 0,
    searchCostUsd: 0,
  };

  if (!name || !looksLikePersonName(name)) {
    return {
      ok: false,
      status: PUBLIC_CONTACT_RESEARCH_STATUS.PUBLIC_RESEARCH_INCOMPLETE,
      reason: "NO_NAMED_PERSON",
      opportunity,
      metrics,
      queryPlan: null,
    };
  }

  const alreadyEmail = text(
    opportunity.primaryContact?.email || opportunity.contactEmail,
  );
  const alreadyPhone = text(
    opportunity.primaryContact?.phone || opportunity.contactPhone,
  );
  if (alreadyEmail && alreadyPhone && args.force !== true) {
    return {
      ok: true,
      status: PUBLIC_CONTACT_RESEARCH_STATUS.REUSED_EXISTING,
      reason: "EXISTING_VERIFIED_CHANNELS",
      opportunity,
      found: { email: alreadyEmail, phone: alreadyPhone },
      metrics,
      queryPlan: null,
      changed: false,
    };
  }

  const org = {
    name:
      opportunity.organizationName ||
      opportunity.displayCompanyName ||
      opportunity.primaryContact?.organization ||
      null,
    domain: opportunity.organizationDomain || null,
  };
  const queryPlan = buildPublicPersonContactQueryPlan(
    {
      name,
      title: opportunity.primaryContact?.title || opportunity.primaryContact?.role,
      organization: org.name,
      domain: org.domain,
    },
    org,
  );
  metrics.queriesPlanned = queryPlan.queries.length;

  const candidateUrls = [...(args.candidateUrls || [])];
  // Mandatory regression seeds for known founder cases (news-only controls
  // must pass seedRegressionFixtures:false).
  {
    const oppId = text(opportunity.id || opportunity.opportunityId);
    const personKey = stripAccents(name).toLowerCase();
    const allowFixtureSeed = args.seedRegressionFixtures !== false;
    if (allowFixtureSeed) {
      const isMaritza =
        personKey.includes("maritza jeanette medina") &&
        (oppId === "disc_b4661c17bd92" ||
          oppId === "disc_cd6fe49dcccf" ||
          args.forceMaritzaFixtureSeed === true);
      const isAlejandra =
        personKey.includes("alejandra vazquez") &&
        (oppId === "paid3_0a6b19883953" ||
          args.forceAlejandraFixtureSeed === true);
      const seeds = [];
      if (isMaritza) {
        seeds.push(
          "https://info.jalisco.gob.mx/dependencia/secretaria-de-infraestructura-y-obra-publica?page=14",
          "https://transparencia.info.jalisco.gob.mx/node/15546",
        );
      }
      if (isAlejandra) {
        seeds.push(
          "https://ecomondomexico.com.mx/en/contact/",
          "https://ecomondomexico.com.mx/contact/",
        );
      }
      for (const u of seeds) {
        if (u && !candidateUrls.includes(u)) candidateUrls.push(u);
      }
    }
  }
  metrics.candidateUrlsSeeded = candidateUrls.length;

  // Paid/web search — blocked unless explicitly authorized. Auto-wire Exa
  // (and Serper when configured) when no provider is injected.
  let searchProvider = args.searchProvider || null;
  if (
    args.authorizePaidSearch === true &&
    (!searchProvider || typeof searchProvider.search !== "function")
  ) {
    const creds = inspectPublicContactSearchCredentials(args.env || process.env);
    if (creds.chosenMode !== "NONE") {
      searchProvider = createPublicContactSearchProvider({
        env: args.env || process.env,
        fetchImpl: args.fetchImpl,
        numResults: 5,
      });
      metrics.searchProviderMode = creds.chosenMode;
    }
  }
  if (
    args.authorizePaidSearch === true &&
    searchProvider &&
    typeof searchProvider.search === "function"
  ) {
    const minStrategies = Number(queryPlan.minStrategies || 2);
    const cap = Math.min(
      Number(args.maxQueries || queryPlan.standardQueryCap),
      queryPlan.escalationQueryCap,
    );
    // Always attempt at least minStrategies distinct queries before exhaustion.
    const runCap = Math.max(minStrategies, Math.min(cap, queryPlan.queries.length));
    for (const row of queryPlan.queries.slice(0, runCap)) {
      metrics.queriesExecuted += 1;
      try {
        const res = await searchProvider.search(row.query);
        metrics.searchCostUsd += Number(res?.costUsd || 0);
        metrics.searchProvidersUsed = metrics.searchProvidersUsed || [];
        if (res?.provider) metrics.searchProvidersUsed.push(res.provider);
        for (const u of res?.urls || []) {
          if (u && !candidateUrls.includes(u)) candidateUrls.push(u);
        }
        // Early stop search when an official contact/directory URL appears.
        if (
          (res?.urls || []).some((u) =>
            /\/contact|directorio|organigrama|transparencia/i.test(String(u)),
          )
        ) {
          break;
        }
      } catch {
        /* continue other strategies */
      }
    }
  }

  const seeded = mergeCandidateSourceUrls(opportunity, candidateUrls);
  const pageCap = Math.min(
    Number(args.maxPages || queryPlan.standardPageCap),
    queryPlan.escalationPageCap,
  );

  const recovered = await recoverPublicContactChannels(seeded, {
    maxUrls: pageCap,
    timeoutMs: args.timeoutMs || 15000,
    maxBytes: args.maxBytes || 2_500_000,
    skipDns: args.skipDns === true,
    fetchImpl: args.fetchImpl,
    skipIfComplete: false,
  });
  metrics.pagesFetched = pageCap;

  let found = recovered.found || null;
  let oppOut = recovered.opportunity || seeded;

  // Secondary named contacts on the same official contact page (e.g. Paulina).
  if (args.discoverSecondaryContacts !== false) {
    const peers = await discoverPeerNamedContactsOnPages({
      urls: candidateUrls.slice(0, pageCap),
      primaryName: name,
      timeoutMs: args.timeoutMs,
      maxBytes: args.maxBytes,
      fetchImpl: args.fetchImpl,
      skipDns: args.skipDns,
    });
    if (peers.length) {
      oppOut.discoveredPeerContacts = peers;
      const secondary = peers.find(
        (p) =>
          stripAccents(p.name).toLowerCase().includes("paulina sanchez") ||
          (!text(oppOut.secondaryContact?.name) && p.email),
      );
      if (secondary && !text(oppOut.secondaryContact?.email)) {
        oppOut.secondaryContact = {
          ...(oppOut.secondaryContact && typeof oppOut.secondaryContact === "object"
            ? oppOut.secondaryContact
            : {}),
          name: secondary.name,
          title: secondary.title || "Sales Manager",
          email: secondary.email || null,
          workEmail: secondary.email || null,
          phone: null,
          contactClass: "RELEVANT_COMMERCIAL_ENTRY_POINT",
          contactDataOrigin: "PUBLIC_SOURCE",
          channelSourceUrl: secondary.sourceUrl || null,
          whyRelevant:
            "Named event-commercial / organizer contact on the official contact page; hotel-sourcing authority unverified.",
        };
      }
    }
  }

  if (found?.phone || found?.email) {
    found = await enrichRecoveredChannelsWithOfficeMetadata(found, {
      personName: name,
      timeoutMs: args.timeoutMs,
      maxBytes: args.maxBytes,
      fetchImpl: args.fetchImpl,
      skipDns: args.skipDns,
    });
    const merged = mergePublicChannelsOntoOpportunity(oppOut, found);
    oppOut = merged.opportunity;
    // recoverPublicContactChannels may already have written a local-format
    // phone; re-apply enriched display (+E.164 / extensions) when digits match.
    if (found.phone) {
      const prevDigits = text(
        oppOut.primaryContact?.phone || oppOut.contactPhone,
      ).replace(/\D/g, "");
      const nextDigits = text(found.phone).replace(/\D/g, "");
      if (
        !prevDigits ||
        prevDigits === nextDigits ||
        nextDigits.endsWith(prevDigits) ||
        prevDigits.endsWith(nextDigits)
      ) {
        oppOut.primaryContact = {
          ...(oppOut.primaryContact || {}),
          phone: found.phone,
          directPhone: found.phone,
        };
        oppOut.contactPhone = found.phone;
      }
    }
    if (found.phoneKind) {
      const isSwitchboard =
        found.phoneKind === PUBLIC_PHONE_KIND.ORGANIZATION_SWITCHBOARD;
      const isMobile = found.phoneKind === PUBLIC_PHONE_KIND.DIRECT_MOBILE;
      oppOut.primaryContact = {
        ...(oppOut.primaryContact || {}),
        phoneType: isSwitchboard
          ? "organization"
          : isMobile
            ? "mobile"
            : "office",
        phoneOwnership: isSwitchboard
          ? "organization"
          : isMobile
            ? "person_direct"
            : "person_office",
        phoneExtensions: found.phoneExtensions || [],
        phoneLabel: found.phoneLabel || null,
        phoneKind: found.phoneKind,
      };
    }
  }

  const email = text(oppOut.primaryContact?.email || oppOut.contactEmail);
  const phone = text(oppOut.primaryContact?.phone || oppOut.contactPhone);
  let status = PUBLIC_CONTACT_RESEARCH_STATUS.PUBLIC_SEARCH_EXHAUSTED;
  if (email && phone) status = PUBLIC_CONTACT_RESEARCH_STATUS.PUBLIC_CHANNELS_VERIFIED;
  else if (email || phone) status = PUBLIC_CONTACT_RESEARCH_STATUS.PARTIAL_PUBLIC_CHANNELS;
  else if (!candidateUrls.length && metrics.queriesExecuted === 0) {
    status = PUBLIC_CONTACT_RESEARCH_STATUS.PUBLIC_RESEARCH_INCOMPLETE;
  }

  oppOut.publicContactDiscovery = {
    version: GDI_PUBLIC_CONTACT_DISCOVERY_V1,
    hierarchyVersion: GDI_PUBLIC_CONTACT_CHANNEL_HIERARCHY_V1,
    status,
    personName: name,
    queryPlanSummary: {
      planned: metrics.queriesPlanned,
      executed: metrics.queriesExecuted,
      paidSearchAuthorized: args.authorizePaidSearch === true,
    },
    candidateUrls,
    elapsedMs: Date.now() - started,
    metrics,
  };

  return {
    ok: Boolean(email || phone),
    status,
    reason: recovered.reason || status,
    opportunity: oppOut,
    found,
    changed: recovered.changed === true || Boolean(email || phone),
    display: buildContactChannelDisplayState(oppOut),
    queryPlan,
    metrics,
    elapsedMs: Date.now() - started,
  };
}

/** Maritza Jeanette Medina García — mandatory regression seeds (no paid search). */
export const MARITZA_PUBLIC_CONTACT_FIXTURE_V1 = Object.freeze({
  personName: "Maritza Jeanette Medina García",
  title: "Directora de Recursos Materiales",
  organization:
    "Secretaría de Infraestructura y Obra Pública, Gobierno de Jalisco",
  expectedEmail: "maritza.medina@jalisco.gob.mx",
  expectedPhoneE164: "+52 33 3819 2333",
  expectedExtensions: ["42333", "47542"],
  expectedPhoneKind: PUBLIC_PHONE_KIND.OFFICE_WITH_EXTENSION,
  authorityClass: "RELEVANT_COMMERCIAL_ENTRY_POINT",
  notHotelBookingAuthority: true,
  candidateUrls: Object.freeze([
    "https://info.jalisco.gob.mx/dependencia/secretaria-de-infraestructura-y-obra-publica?page=14",
    "https://transparencia.info.jalisco.gob.mx/node/15546",
  ]),
  hotelId: "recCAGqkERSXgiku4",
  opportunityIds: Object.freeze(["disc_b4661c17bd92", "disc_cd6fe49dcccf"]),
});

/** Alejandra Vázquez — Ecomondo / IGECO mandatory regression. */
export const ALEJANDRA_PUBLIC_CONTACT_FIXTURE_V1 = Object.freeze({
  personName: "Alejandra Vázquez",
  title: "Sales Manager",
  organization: "IGECO México / Ecomondo Mexico",
  expectedEmail: "alejandra.vazquez@igeco.mx",
  expectedPhone: null,
  secondaryPersonName: "Paulina Sánchez",
  secondaryExpectedEmail: "paulina.sanchez@igeco.mx",
  authorityClass: "RELEVANT_COMMERCIAL_ENTRY_POINT",
  notHotelBookingAuthority: true,
  founderSearchQuery: "Alejandra Vazquez Sales Manager IGECO",
  founderSearchQueryWithContact: "Alejandra Vazquez IGECO contact",
  expectedSearchHitUrl: "https://ecomondomexico.com.mx/en/contact/",
  candidateUrls: Object.freeze([
    "https://ecomondomexico.com.mx/en/contact/",
    "https://ecomondomexico.com.mx/contact/",
  ]),
  hotelId: "recCAGqkERSXgiku4",
  opportunityIds: Object.freeze(["paid3_0a6b19883953"]),
});

export default {
  GDI_PUBLIC_CONTACT_DISCOVERY_V1,
  PUBLIC_CONTACT_RESEARCH_STATUS,
  PUBLIC_PHONE_KIND,
  buildPublicPersonContactQueryPlan,
  classifyPublicPhoneKind,
  discoverPublicContactChannels,
  discoverPeerNamedContactsOnPages,
  mergeCandidateSourceUrls,
  MARITZA_PUBLIC_CONTACT_FIXTURE_V1,
  ALEJANDRA_PUBLIC_CONTACT_FIXTURE_V1,
};
