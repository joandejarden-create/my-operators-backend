/**
 * GDI 3.0 — Contact email & phone hierarchy (founder directive).
 *
 * Priority:
 * 1) Verified named direct commercial buyer / decision-maker
 * 2) Verified named commercially relevant influencer / referral
 * 3) Verified organizational / department route
 * 4) Published company switchboard / contact form / legitimate business route
 * 5) On-demand enrichment only for unresolved channels (Surfe — ephemeral)
 *
 * Native public research first. Never invent emails from guessed formats.
 * Never persist Surfe/provider PII. No production Airtable writes here.
 */
import {
  extractContactIntelligenceFromSource,
  CONTACT_DATA_ORIGIN,
} from "../extract-contact-intelligence-from-source.js";
import { looksLikePersonName } from "../secondary-named-contact-v1.js";
import {
  fetchPublicHtmlSafe,
  validatePublicOutboundUrl,
} from "./gdi-safe-outbound-fetch-v1.js";

export const GDI_PUBLIC_CONTACT_CHANNEL_HIERARCHY_V1 =
  "gdi-public-contact-channel-hierarchy-v1";

export const CONTACT_HIERARCHY_RANK = Object.freeze({
  NAMED_DIRECT_BUYER: 1,
  NAMED_RELEVANT_INFLUENCER: 2,
  ORGANIZATIONAL_ROUTE: 3,
  PUBLISHED_BUSINESS_ROUTE: 4,
  ON_DEMAND_ENRICHMENT: 5,
});

export const CONTACT_DISPLAY_LABELS = Object.freeze({
  directEmail: "Direct Business Email",
  directPhone: "Direct Business Phone",
  orgEmail: "Organizational Contact Route",
  orgPhone: "Organizational Contact Route",
  findEmail: "Find Email",
  findPhone: "Find Phone",
  findBoth: "Find Email & Phone",
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function nameKey(name) {
  return text(name)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function isGenericMailbox(email) {
  const local = text(email).split("@")[0] || "";
  return /^(info|contact|hello|office|admin|support|sales|inquiries|enquiry|reception|mail|team|press|media)$/i.test(
    local,
  );
}

function isSafeHttpUrl(url) {
  try {
    const u = new URL(String(url || ""));
    return u.protocol === "http:" || u.protocol === "https:";
  } catch {
    return false;
  }
}

export function classifyNamedContactRole(person = {}, opportunity = {}) {
  const title = text(person.title || person.role);
  const klass = text(
    person.contactClass ||
      person.contactClassification ||
      person.relevanceClass ||
      "",
  ).toUpperCase();
  const why = text(
    person.whyThisContact ||
      person.whyRelevant ||
      person.relationshipToOpportunity ||
      "",
  );
  const blob = `${title} ${klass} ${why}`;

  if (
    /DIRECT_BUYER|VERIFIED_HOTEL_BUYER|BOOKING_AUTHORITY|DECISION.?MAKER/i.test(
      klass,
    ) ||
    /procurement|adquisicion|hotel.?buyer|meeting.?planner|housing|sourcing/i.test(
      blob,
    )
  ) {
    return {
      rank: CONTACT_HIERARCHY_RANK.NAMED_DIRECT_BUYER,
      code: "NAMED_DIRECT_BUYER",
      label: "Verified named commercial buyer / decision-maker",
      buyerAuthorityClaimed: /DIRECT_BUYER|VERIFIED_HOTEL_BUYER|BOOKING_AUTHORITY/i.test(
        klass,
      ),
    };
  }
  if (
    /RELEVANT|INFLUENCER|REFERRAL|ENTRY_POINT|COMMERCIAL/i.test(klass) ||
    /sales manager|account manager|event manager|coordinator|director/i.test(
      title,
    )
  ) {
    return {
      rank: CONTACT_HIERARCHY_RANK.NAMED_RELEVANT_INFLUENCER,
      code: "NAMED_RELEVANT_INFLUENCER",
      label: "Named commercially relevant influencer / referral",
      buyerAuthorityClaimed: false,
    };
  }
  if (looksLikePersonName(person.name || opportunity.contactName)) {
    return {
      rank: CONTACT_HIERARCHY_RANK.NAMED_RELEVANT_INFLUENCER,
      code: "NAMED_RELEVANT_INFLUENCER",
      label: "Named contact — commercial authority unverified",
      buyerAuthorityClaimed: false,
    };
  }
  return {
    rank: CONTACT_HIERARCHY_RANK.ORGANIZATIONAL_ROUTE,
    code: "ORGANIZATIONAL_ROUTE",
    label: "Organizational / department route",
    buyerAuthorityClaimed: false,
  };
}

export function collectPublicContactSourceUrls(opportunity = {}) {
  const urls = [];
  const push = (u) => {
    const s = text(u);
    if (!isSafeHttpUrl(s)) return;
    const key = s.toLowerCase().replace(/\/$/, "");
    if (urls.some((x) => x.toLowerCase().replace(/\/$/, "") === key)) return;
    urls.push(s);
  };

  const pc = opportunity.primaryContact || {};
  push(pc.sourceUrl);
  push(pc.profileUrl);
  push(pc.officialContactUrl);
  push(opportunity.contactOfficialUrl);
  push(opportunity.contactUrl);
  push(opportunity.publicContactPath);

  for (const s of opportunity.sources || []) push(s?.url || s);
  for (const e of opportunity.evidence || []) push(e?.sourceUrl || e?.url);

  // Prefer official contact / directory pages ahead of about/news/PDFs.
  const score = (u) => {
    const s = u.toLowerCase();
    if (/\/contact\/?|contacto|directorio|directory|organigrama|staff-directory/i.test(s))
      return 0;
    if (/\/team|\/equipo|\/people|\/staff\b/i.test(s)) return 1;
    if (/about|nosotros|hoteles|hotel|accommodation|housing/i.test(s)) return 2;
    if (/\.pdf(\?|$)/i.test(s)) return 5;
    if (/linkedin\.com\/in\//i.test(s)) return 4;
    if (/informador|eleconomista|news|press|blog/i.test(s)) return 3;
    return 2;
  };
  return urls.sort((a, b) => score(a) - score(b));
}

function matchPersonOnExtraction(extraction, personName) {
  const want = nameKey(personName);
  if (!want || !extraction) return null;
  const people = [
    extraction.primaryContactCandidate,
    ...(extraction.backupContactCandidates || []),
    ...(extraction.people || []),
  ].filter(Boolean);
  for (const p of people) {
    if (nameKey(p.name) === want) return p;
    // Allow first+last overlap for accent / middle-name variance
    const tokens = want.split(" ").filter((t) => t.length >= 3);
    const other = nameKey(p.name);
    if (tokens.length >= 2 && tokens.every((t) => other.includes(t))) return p;
  }
  return null;
}

function collectOrgRoutesFromExtraction(extraction) {
  const emails = [];
  const phones = [];
  for (const f of extraction?.functional || extraction?.functionalContacts || []) {
    if (f?.email && isGenericMailbox(f.email)) emails.push(text(f.email));
    else if (f?.email && /^(info|contact|sales|events|housing)@/i.test(f.email)) {
      emails.push(text(f.email));
    }
    if (f?.phone) phones.push(text(f.phone));
  }
  // Bare generic emails often appear on contact pages
  const htmlEmails = extraction?.allEmails || [];
  for (const e of htmlEmails) {
    if (isGenericMailbox(e)) emails.push(text(e));
  }
  return {
    organizationEmail: emails[0] || null,
    organizationPhone: phones[0] || null,
  };
}

const MAILTO_RE =
  /mailto:([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})/gi;
const PHONE_RE =
  /(?:\+|M\s*)?(?:\d{1,3}[\s.-]*)?(?:\(?\d{2,4}\)?[\s.-]*){2,4}\d{2,4}/g;

function extractMailtos(html) {
  const out = [];
  let m;
  const re = new RegExp(MAILTO_RE.source, "gi");
  while ((m = re.exec(String(html || ""))) !== null) {
    out.push({ email: text(m[1]), index: m.index });
  }
  return out;
}

/**
 * Person-scoped channels: window from the person's name until the next
 * distinct person name (or ~280 chars). Prefer mailto nearest the name in HTML.
 */
function extractPersonScopedChannels(html, personName) {
  const name = text(personName);
  if (!name || !html) return { email: null, phone: null };
  const lowerHtml = String(html);
  const want = nameKey(name);
  const visible = lowerHtml
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ");
  const visNorm = visible
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
  const idx = visNorm.indexOf(want);
  if (idx < 0) return { email: null, phone: null };

  // End window at next Title-Case person-like name after a short gap
  const after = visible.slice(idx + name.length);
  const nextPerson = after.match(
    /\s{1,40}([A-ZÁÉÍÓÚÑ][a-záéíóúñ'.-]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ'.-]+){1,3})\s+(?:Sales|CEO|Managing|Strategic|Representative|Director|Manager|Consultant|Coordinator|President)/,
  );
  const end = nextPerson ? nextPerson.index : Math.min(after.length, 280);
  const window = visible.slice(idx, idx + name.length + end);

  let phone = null;
  const phoneMatches = [...window.matchAll(PHONE_RE)]
    .map((m) => text(m[0]))
    .filter((p) => p.replace(/\D/g, "").length >= 10);
  // Prefer numbers that look like direct lines (+1 / local 10+ digits), not truncated fragments
  phone =
    phoneMatches.find((p) => /\+1|\(\d{3}\)|\d{3}[\s.-]\d{3}/.test(p)) ||
    phoneMatches[0] ||
    null;

  // Mailto nearest the HTML occurrence of the person name
  const htmlNorm = lowerHtml
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
  const htmlNameIdx = htmlNorm.indexOf(want);
  const mailtos = extractMailtos(lowerHtml);
  let email = null;
  if (htmlNameIdx >= 0 && mailtos.length) {
    let best = null;
    for (const mt of mailtos) {
      if (isGenericMailbox(mt.email)) continue;
      const dist = Math.abs(mt.index - htmlNameIdx);
      // Prefer mailto that appears after the name within ~2500 chars (card layout)
      const afterName = mt.index >= htmlNameIdx && mt.index - htmlNameIdx < 2500;
      const score = afterName ? dist : dist + 5000;
      if (!best || score < best.score) best = { email: mt.email, score };
    }
    if (best && best.score < 8000) email = best.email;
  }
  // Fallback: email printed in the visible person window
  if (!email) {
    const em = window.match(
      /\b([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})\b/,
    );
    if (em?.[1] && !isGenericMailbox(em[1])) email = text(em[1]);
  }

  return { email, phone };
}

/**
 * Merge publicly verified channels onto an existing named primary.
 * Never invents channels. Never overwrites newer verified values with blanks.
 */
export function mergePublicChannelsOntoOpportunity(opportunity = {}, found = {}) {
  const next = { ...opportunity };
  const pc = {
    ...(next.primaryContact && typeof next.primaryContact === "object"
      ? next.primaryContact
      : {}),
  };
  const name = text(pc.name || next.contactName);
  if (!name) return { opportunity: next, changed: false };

  let changed = false;
  if (found.email && !text(pc.email || next.contactEmail)) {
    pc.email = found.email;
    pc.workEmail = found.email;
    next.contactEmail = found.email;
    changed = true;
  }
  if (found.phone && !text(pc.phone || next.contactPhone)) {
    pc.phone = found.phone;
    pc.directPhone = found.phone;
    next.contactPhone = found.phone;
    changed = true;
  }
  if (found.organizationEmail && !text(next.organizationContactEmail || pc.organizationEmail)) {
    pc.organizationEmail = found.organizationEmail;
    next.organizationContactEmail = found.organizationEmail;
    changed = true;
  }
  if (found.organizationPhone && !text(next.companyPhone || pc.organizationPhone)) {
    pc.organizationPhone = found.organizationPhone;
    next.companyPhone = found.organizationPhone;
    changed = true;
  }
  if (found.sourceUrl) {
    if (!text(pc.sourceUrl)) {
      pc.sourceUrl = found.sourceUrl;
      changed = true;
    }
    // Surface on contact card / Sources without inventing a LinkedIn profile.
    if (!text(pc.officialContactUrl)) {
      pc.officialContactUrl = found.sourceUrl;
      changed = true;
    }
    if (!text(next.contactOfficialUrl)) {
      next.contactOfficialUrl = found.sourceUrl;
      changed = true;
    }
    if (!text(next.contactSourceUrl)) {
      next.contactSourceUrl = found.sourceUrl;
      changed = true;
    }
  }
  if (found.profileUrl && !text(pc.profileUrl)) {
    pc.profileUrl = found.profileUrl;
    changed = true;
  }

  if (changed) {
    pc.contactDataOrigin = CONTACT_DATA_ORIGIN.PUBLIC_SOURCE;
    pc.channelVerificationStatus = "PUBLICLY_VERIFIED";
    pc.channelLastVerifiedAt = new Date().toISOString().slice(0, 10);
    pc.channelSourceUrl = found.sourceUrl || pc.sourceUrl || null;
    next.primaryContact = pc;
    next.contactDataOrigin = CONTACT_DATA_ORIGIN.PUBLIC_SOURCE;
    next.contactVerifiedAt = new Date().toISOString();
    next.publicChannelRecovery = {
      version: GDI_PUBLIC_CONTACT_CHANNEL_HIERARCHY_V1,
      email: Boolean(found.email),
      phone: Boolean(found.phone),
      organizationEmail: Boolean(found.organizationEmail),
      sourceUrl: found.sourceUrl || null,
    };
  }
  return { opportunity: next, changed };
}

/**
 * Research-pipeline only: fetch opportunity source pages and recover public
 * channels for the named primary. Uses SSRF-hardened outbound fetch.
 *
 * Do NOT call from authenticated customer opportunity-detail requests —
 * use `ensureDrawerContactParityPresentation` (stored data only) instead.
 */
export async function recoverPublicContactChannels(opportunity = {}, opts = {}) {
  const name =
    text(opportunity.primaryContact?.name) || text(opportunity.contactName);
  if (!name || !looksLikePersonName(name)) {
    return {
      ok: false,
      reason: "NO_NAMED_PRIMARY",
      opportunity,
      found: null,
    };
  }

  const alreadyEmail = text(
    opportunity.primaryContact?.email || opportunity.contactEmail,
  );
  const alreadyPhone = text(
    opportunity.primaryContact?.phone || opportunity.contactPhone,
  );
  if (alreadyEmail && alreadyPhone && opts.skipIfComplete !== false) {
    return {
      ok: true,
      reason: "ALREADY_COMPLETE",
      opportunity,
      found: {
        email: alreadyEmail,
        phone: alreadyPhone,
        sourceUrl: opportunity.primaryContact?.sourceUrl || null,
      },
      changed: false,
    };
  }

  const urls = collectPublicContactSourceUrls(opportunity).slice(
    0,
    Number(opts.maxUrls || 3),
  );
  if (!urls.length) {
    return { ok: false, reason: "NO_SOURCE_URLS", opportunity, found: null };
  }

  const timeoutMs = Number(opts.timeoutMs || 8000);
  const rejectedUrls = [];
  let best = {
    email: alreadyEmail || null,
    phone: alreadyPhone || null,
    organizationEmail: text(opportunity.organizationContactEmail) || null,
    organizationPhone: text(opportunity.companyPhone) || null,
    sourceUrl: null,
    profileUrl: null,
  };

  for (const url of urls) {
    if (/linkedin\.com/i.test(url)) {
      if (!best.profileUrl) best.profileUrl = url;
      continue;
    }
    const precheck = await validatePublicOutboundUrl(url, {
      skipDns: opts.skipDns === true,
    });
    if (!precheck.ok) {
      rejectedUrls.push({ url, reason: precheck.reason });
      continue;
    }
    const fetched = await fetchPublicHtmlSafe(url, {
      timeoutMs,
      maxBytes: opts.maxBytes,
      maxRedirects: opts.maxRedirects,
      skipDns: opts.skipDns === true,
      fetchImpl: opts.fetchImpl,
      headers: {
        "User-Agent": "DealalityGdiPublicContactHierarchy/1.0",
      },
    });
    if (!fetched.ok) {
      rejectedUrls.push({ url, reason: fetched.reason });
      continue;
    }
    const html = fetched.html || "";
    const extraction = extractContactIntelligenceFromSource({
      source: { url: fetched.url || url, sourceType: "official_web" },
      pageContent: html,
      opportunityContext: opportunity,
    });
    const matched = matchPersonOnExtraction(extraction, name);
    const org = collectOrgRoutesFromExtraction(extraction);
    const scoped = extractPersonScopedChannels(html, name);
    const infoMailto = extractMailtos(html).find((m) =>
      isGenericMailbox(m.email),
    );

    if (matched?.email && !isGenericMailbox(matched.email) && !best.email) {
      best.email = text(matched.email);
      best.sourceUrl = fetched.url || url;
    }
    if (matched?.phone && !best.phone) {
      best.phone = text(matched.phone);
      best.sourceUrl = best.sourceUrl || fetched.url || url;
    }
    if (scoped.email && !best.email) {
      best.email = scoped.email;
      best.sourceUrl = fetched.url || url;
    }
    if (scoped.phone && !best.phone) {
      best.phone = scoped.phone;
      best.sourceUrl = best.sourceUrl || fetched.url || url;
    }
    if (!best.organizationEmail) {
      best.organizationEmail =
        org.organizationEmail || (infoMailto ? infoMailto.email : null);
    }
    if (!best.organizationPhone) {
      best.organizationPhone = org.organizationPhone || null;
    }
    if (!best.profileUrl && matched?.profileUrl) {
      best.profileUrl = matched.profileUrl;
    }

    if (best.email && best.phone) break;
  }

  const foundSomething = Boolean(
    (best.email && best.email !== alreadyEmail) ||
      (best.phone && best.phone !== alreadyPhone) ||
      best.organizationEmail ||
      best.profileUrl,
  );
  if (!foundSomething && !alreadyEmail && !alreadyPhone) {
    return {
      ok: false,
      reason: "NO_PUBLIC_CHANNELS_FOUND",
      opportunity,
      found: best,
      rejectedUrls,
    };
  }

  const merged = mergePublicChannelsOntoOpportunity(opportunity, best);
  return {
    ok: true,
    reason: merged.changed ? "PUBLIC_CHANNELS_RECOVERED" : "NO_NEW_CHANNELS",
    opportunity: merged.opportunity,
    found: best,
    changed: merged.changed,
    rejectedUrls,
  };
}

/**
 * Display + Surfe eligibility snapshot for a contact slot / opportunity.
 */
export function buildContactChannelDisplayState(opportunity = {}) {
  const pc = opportunity.primaryContact || {};
  const name = text(pc.name || opportunity.contactName);
  const role = classifyNamedContactRole(pc, opportunity);
  const email = text(pc.email || pc.workEmail || opportunity.contactEmail);
  const phone = text(pc.phone || pc.directPhone || opportunity.contactPhone);
  const orgEmail = text(
    pc.organizationEmail ||
      opportunity.organizationContactEmail ||
      opportunity.functionalContactEmail,
  );
  const orgPhone = text(pc.organizationPhone || opportunity.companyPhone);
  const profileUrl = text(pc.profileUrl || pc.linkedinUrl || pc.sourceUrl);
  const linkedIn = /linkedin\.com\/in\//i.test(profileUrl) ? profileUrl : text(pc.linkedinUrl);

  let surfeChannel = null;
  if (name && looksLikePersonName(name)) {
    if (!email && !phone) surfeChannel = "both";
    else if (!email) surfeChannel = "email";
    else if (!phone) surfeChannel = "phone";
  }

  let demoState = "D";
  if (email && phone) demoState = "A";
  else if (email && !phone) demoState = "B";
  else if (!email && phone) demoState = "C";
  else if (name && (orgEmail || orgPhone)) demoState = "D";
  else if (name) demoState = "D";
  if (role.code === "NAMED_RELEVANT_INFLUENCER" && !role.buyerAuthorityClaimed) {
    if (demoState === "A" || demoState === "B" || demoState === "C" || demoState === "D") {
      /* keep channel state; flag influencer separately */
    }
  }

  return {
    opportunityId: opportunity.id || null,
    namedContact: name || null,
    title: text(pc.title || pc.role) || null,
    organization: text(pc.organization || opportunity.organizationName) || null,
    roleClassification: role,
    publicDirectEmail: email || null,
    publicDirectPhone: phone || null,
    organizationalEmail: orgEmail || null,
    organizationalPhone: orgPhone || null,
    linkedInOrProfile: linkedIn || profileUrl || null,
    emailLabel: email ? CONTACT_DISPLAY_LABELS.directEmail : null,
    phoneLabel: phone
      ? text(pc.phoneLabel) ||
        (Array.isArray(pc.phoneExtensions) && pc.phoneExtensions.length
          ? `Office phone (ext. ${pc.phoneExtensions.join(" / ")})`
          : CONTACT_DISPLAY_LABELS.directPhone)
      : null,
    organizationEmailLabel: orgEmail ? CONTACT_DISPLAY_LABELS.orgEmail : null,
    organizationPhoneLabel: orgPhone ? CONTACT_DISPLAY_LABELS.orgPhone : null,
    surfeEnrichmentEligible: Boolean(surfeChannel),
    surfeEnrichmentChannel: surfeChannel,
    surfeCtaLabel:
      surfeChannel === "email"
        ? CONTACT_DISPLAY_LABELS.findEmail
        : surfeChannel === "phone"
          ? CONTACT_DISPLAY_LABELS.findPhone
          : surfeChannel === "both"
            ? CONTACT_DISPLAY_LABELS.findBoth
            : null,
    demoState,
    dataOrigin: text(pc.contactDataOrigin || opportunity.contactDataOrigin) || null,
    channelSourceUrl: text(pc.channelSourceUrl || pc.sourceUrl) || null,
    channelLastVerifiedAt: text(pc.channelLastVerifiedAt) || null,
  };
}

/**
 * Proposed Airtable public-contact upsert (dry-run only).
 * Does not execute writes.
 */
export function proposeAirtablePublicContactUpsert(opportunity = {}, display = null) {
  const d = display || buildContactChannelDisplayState(opportunity);
  if (!d.namedContact) return null;
  if (!d.publicDirectEmail && !d.publicDirectPhone && !d.organizationalEmail) {
    return null;
  }
  // Only propose when public recovery marked or channels exist with PUBLIC_SOURCE
  const origin = d.dataOrigin || opportunity.contactDataOrigin || null;
  const ready =
    opportunity.publicChannelRecovery?.version ===
      GDI_PUBLIC_CONTACT_CHANNEL_HIERARCHY_V1 ||
    origin === CONTACT_DATA_ORIGIN.PUBLIC_SOURCE;
  if (!ready && !opportunity.publicChannelRecovery) {
    // Still propose reuse if channels already on record
    if (!d.publicDirectEmail && !d.publicDirectPhone) return null;
  }

  return {
    action: "UPSERT_PROPOSED_DRY_RUN",
    execute: false,
    target: "Existing GDI opportunity / embedded contact intelligence (no new Contacts table)",
    dedupeKey: [
      nameKey(d.namedContact),
      nameKey(d.organization),
      text(d.publicDirectEmail).toLowerCase(),
    ]
      .filter(Boolean)
      .join("|"),
    fields: {
      personName: d.namedContact,
      company: d.organization,
      role: d.title,
      opportunityId: d.opportunityId,
      opportunityTitle: text(opportunity.title || opportunity.displayTitle),
      classification: d.roleClassification.code,
      publicBusinessEmail: d.publicDirectEmail,
      publicBusinessPhone: d.publicDirectPhone,
      organizationalRouteEmail: d.organizationalEmail,
      organizationalRoutePhone: d.organizationalPhone,
      routeType: d.publicDirectEmail
        ? "DIRECT"
        : d.organizationalEmail
          ? "ORGANIZATIONAL"
          : "UNKNOWN",
      sourceUrl: d.channelSourceUrl,
      verificationStatus: "PUBLICLY_VERIFIED",
      lastVerifiedDate: d.channelLastVerifiedAt || new Date().toISOString().slice(0, 10),
      provenance: CONTACT_DATA_ORIGIN.PUBLIC_SOURCE,
      permittedUse: "CUSTOMER_FACING_WHEN_PUBLISHED",
      surfeDerived: false,
    },
    overwritePolicy: "DO_NOT_OVERWRITE_NEWER_VERIFIED_WITH_STALE_OR_BLANK",
  };
}

export default {
  GDI_PUBLIC_CONTACT_CHANNEL_HIERARCHY_V1,
  CONTACT_HIERARCHY_RANK,
  CONTACT_DISPLAY_LABELS,
  classifyNamedContactRole,
  collectPublicContactSourceUrls,
  mergePublicChannelsOntoOpportunity,
  recoverPublicContactChannels,
  buildContactChannelDisplayState,
  proposeAirtablePublicContactUpsert,
};
