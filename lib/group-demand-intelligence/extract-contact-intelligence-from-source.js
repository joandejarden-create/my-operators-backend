/**
 * GDI Contact Intelligence V1 — extractContactIntelligenceFromSource
 *
 * Dual-use: run on the SAME HTML/text already fetched for demand evidence.
 * WHO-first. No Surfe. No invented people. Public-source data only.
 */

import { extractContactsFromHtmlV5 } from "./contact-candidate/native-who-v3/html-contact-extract.js";
import { extractSameDomainLinks, scoreStaffUrl } from "./contact-candidate/native-who-v3/staff-directory-crawler.js";
import {
  classifyGdiContactRole,
  classifyRoleRelevance,
  classifyEventFamily,
  GDI_CONTACT_ROLE,
} from "./contact-candidate/ontology.js";
import { isLikelyPersonName, hasNamedPerson } from "./contact-resolution.js";
import {
  CONTACT_TIER,
  classifyContactTier,
  stripSurfeProviderPii,
} from "./contact-tiers-v1-2.js";

export const CONTACT_DATA_ORIGIN = Object.freeze({
  PUBLIC_SOURCE: "PUBLIC_SOURCE",
  PROVIDER_EPHEMERAL: "PROVIDER_EPHEMERAL",
});

export const FOLLOWUP_LINK_CLASS = Object.freeze({
  CONTACT_PAGE: "CONTACT_PAGE",
  STAFF_DIRECTORY: "STAFF_DIRECTORY",
  EVENT_TEAM: "EVENT_TEAM",
  HOUSING_PAGE: "HOUSING_PAGE",
  REGISTRATION_PAGE: "REGISTRATION_PAGE",
  LEADERSHIP_PAGE: "LEADERSHIP_PAGE",
  OTHER_RELEVANT: "OTHER_RELEVANT",
});

const IRRELEVANT_ROLE_RE =
  /\b(ceo|chief executive|chairman|chairperson|board member|trustee|press|media|communications|public relations|webmaster|privacy|legal counsel|general counsel|attorney|speaker|keynote|panelist|moderator|sponsor liaison|customer service|help desk|it support|webmaster)\b/i;

const FUNCTIONAL_LOCAL_RE =
  /^(events?|meetings?|conference|housing|registration|groupsales|group\.?sales|inquiries|sales|weddings?|banquets?|planning|planner)@/i;
const GENERIC_LOCAL_RE = /^(info|contact|office|hello|admin|support|mail|webmaster|privacy|legal)@/i;

function clean(s) {
  return String(s || "").trim();
}

function stripTagsLite(html) {
  return String(html || "")
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<br\s*\/?>/gi, "\n")
    .replace(/<\/(p|div|li|tr|h\d)>/gi, "\n")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/\s+/g, " ")
    .trim();
}

const NAME_PREFIX_NOISE =
  /^(Staff|Team|Contact|About|Our|The|Meet|Conference|Meeting|Event|Housing|Registration|Program|Group|Sales|Office|Directory|Leadership|Board|Committee|Keynote|Speaker|Panel)\b/i;

const ROLE_BODY =
  "(?:Conference|Meetings|Housing|Event|Tournament|Registration|Program|Group|VP|Vice President|Director|Manager|Coordinator|Executive)[^.\\n|]{0,70}";

function normalizeLoosePersonName(rawName) {
  let name = clean(rawName);
  if (!name) return null;
  // Drop trailing title fragments: "Danielle Davis Vice" → "Danielle Davis"
  const titleStrip = name.split(/\s+/);
  while (
    titleStrip.length >= 3 &&
    /^(Vice|President|Director|Manager|Coordinator|Officer|Chief|Senior|Junior|Jr|Sr|Email|Phone)$/i.test(
      titleStrip[titleStrip.length - 1]
    )
  ) {
    titleStrip.pop();
  }
  name = titleStrip.join(" ");
  if (NAME_PREFIX_NOISE.test(name) || (!hasNamedPerson({ name }) && !isLikelyPersonName(name))) {
    const parts = name.split(/\s+/).filter(Boolean);
    for (let take = Math.min(3, parts.length); take >= 2; take--) {
      const candidate = parts.slice(-take).join(" ");
      if (NAME_PREFIX_NOISE.test(candidate)) continue;
      if (hasNamedPerson({ name: candidate }) || isLikelyPersonName(candidate)) {
        return candidate;
      }
    }
    return null;
  }
  return name;
}

function cleanLooseRole(role) {
  return clean(role)
    .replace(/&nbsp;/gi, " ")
    .replace(/&ndash;|&mdash;|&amp;|&lt;|&gt;|&#\d+;/gi, " ")
    .replace(/\s+[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}.*$/i, "")
    .replace(/\s+(?:email|phone|tel|ext)\b.*$/i, "")
    .replace(/\s+[a-z][a-z0-9._-]{0,24}$/i, (tail) => {
      if (/^(ada|jane|john|info|contact|email|smith|planner)$/i.test(tail.trim())) return "";
      return tail;
    })
    .replace(/\s{2,}/g, " ")
    .slice(0, 90);
}

/**
 * Supplementary HTML sweep — catches simple "Name, Role mailto:" patterns
 * that structural V5 extraction may miss on sparse pages.
 */
function extractLooseHtmlPeople(html, url) {
  const people = [];
  const functional = [];
  const h = String(html || "");
  if (!h) return { people, functional };
  const seenPeople = new Set();

  const pushPerson = (entry) => {
    const name = normalizeLoosePersonName(entry.name);
    if (!name) return;
    const role = cleanLooseRole(entry.role || "");
    if (role && IRRELEVANT_ROLE_RE.test(role)) return;
    const key = `${name.toLowerCase()}|${(entry.email || "").toLowerCase()}|${entry.phone || ""}`;
    if (seenPeople.has(`${name.toLowerCase()}|`) && !entry.email && !entry.phone) {
      // already have this name; allow upgrade with reachability below
    }
    const existing = people.find((p) => p.name.toLowerCase() === name.toLowerCase());
    if (existing) {
      if (entry.email && !existing.email) existing.email = entry.email;
      if (entry.phone && !existing.phone) existing.phone = entry.phone;
      if (role && (!existing.role || existing.role.length < role.length)) existing.role = role;
      return;
    }
    seenPeople.add(key);
    people.push({
      name,
      role: role || null,
      email: entry.email || null,
      phone: entry.phone || null,
      sourceUrl: url,
      evidenceType: entry.evidenceType || "LOOSE_HTML",
    });
  };

  // mailto functional / person
  const mailtoRe = /mailto:([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})/gi;
  let m;
  while ((m = mailtoRe.exec(h))) {
    const email = m[1];
    const windowHtml = h.slice(Math.max(0, m.index - 320), m.index + m[0].length + 40);
    const plain = stripTagsLite(windowHtml);
    if (FUNCTIONAL_LOCAL_RE.test(email) || GENERIC_LOCAL_RE.test(email)) {
      functional.push({
        email,
        phone: null,
        role: mapFunctionalLabel(email, null),
        sourceUrl: url,
      });
      continue;
    }
    const pair = plain.match(
      new RegExp(
        String.raw`\b([A-Z][a-z]+(?:\s+[A-Z][a-z'.-]+){1,3})\s*[,|–\-—:]\s*(${ROLE_BODY})`,
        "i"
      )
    );
    if (pair) {
      pushPerson({
        name: pair[1],
        role: pair[2],
        email,
        evidenceType: "LOOSE_HTML_MAILTO_PAIR",
      });
      continue;
    }
    const nameOnly = plain.match(/\b([A-Z][a-z]+(?:\s+[A-Z][a-z'.-]+){1,2})\b/);
    if (nameOnly) {
      pushPerson({
        name: nameOnly[1],
        role: null,
        email,
        evidenceType: "LOOSE_HTML_MAILTO_NAME",
      });
    }
  }

  // Name, Role lines (normalize heading noise like "Staff Jane Smith")
  const text = stripTagsLite(h);
  const lineRe = new RegExp(
    String.raw`\b([A-Z][a-z]+(?:\s+[A-Z][a-z'.-]+){1,3})\s*[,|–\-—]\s*(${ROLE_BODY})`,
    "g"
  );
  let lm;
  while ((lm = lineRe.exec(text))) {
    const around = text.slice(Math.max(0, lm.index - 40), lm.index);
    if (/\bspeaker\b/i.test(around)) continue;
    pushPerson({
      name: lm[1],
      role: lm[2],
      email: null,
      evidenceType: "LOOSE_HTML_NAME_ROLE",
    });
  }

  // Attach nearby mailto emails to people missing email
  mailtoRe.lastIndex = 0;
  while ((m = mailtoRe.exec(h))) {
    const email = m[1];
    if (FUNCTIONAL_LOCAL_RE.test(email) || GENERIC_LOCAL_RE.test(email)) continue;
    const windowHtml = h.slice(Math.max(0, m.index - 320), m.index + m[0].length + 60);
    const plain = stripTagsLite(windowHtml);
    const local = email.split("@")[0].toLowerCase();
    for (const p of people) {
      if (p.email) continue;
      const last = p.name.split(/\s+/).pop()?.toLowerCase() || "";
      if (
        plain.includes(p.name) ||
        (last.length >= 3 && plain.toLowerCase().includes(last)) ||
        (local.includes(last) && plain.toLowerCase().includes(last))
      ) {
        p.email = email;
        p.evidenceType = `${p.evidenceType}+MAILTO`;
      }
    }
  }

  // Plain-text emails next to person names
  for (const p of people) {
    if (p.email) continue;
    const idx = text.toLowerCase().indexOf(p.name.toLowerCase());
    if (idx < 0) continue;
    const nearby = text.slice(idx, idx + p.name.length + 140);
    const em = nearby.match(/\b([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})\b/);
    if (em && !FUNCTIONAL_LOCAL_RE.test(em[1]) && !GENERIC_LOCAL_RE.test(em[1])) {
      p.email = em[1];
      p.evidenceType = `${p.evidenceType}+PLAIN_EMAIL`;
    }
  }

  // tel:
  const telRe = /tel:([+\d][\d\-.\s()]{6,}\d)/gi;
  let tm;
  while ((tm = telRe.exec(h))) {
    const phone = tm[1];
    const windowHtml = h.slice(Math.max(0, tm.index - 220), tm.index + 80);
    const plain = stripTagsLite(windowHtml);
    const pair = plain.match(
      new RegExp(
        String.raw`\b([A-Z][a-z]+(?:\s+[A-Z][a-z'.-]+){1,3})\s*[,|–\-—:]\s*(${ROLE_BODY})`,
        "i"
      )
    );
    if (pair) {
      pushPerson({
        name: pair[1],
        role: pair[2],
        phone,
        evidenceType: "LOOSE_HTML_TEL_PAIR",
      });
    }
  }

  return { people, functional };
}

function classifyFollowupLink(url = "", anchor = "") {
  const blob = `${url} ${anchor}`.toLowerCase();
  if (/hous(e|ing)|hotel|accommodat|lodging|travel|stay/.test(blob)) {
    return FOLLOWUP_LINK_CLASS.HOUSING_PAGE;
  }
  if (/registrat|rsvp|sign[- ]?up|enroll/.test(blob)) {
    return FOLLOWUP_LINK_CLASS.REGISTRATION_PAGE;
  }
  if (/staff|directory|our[- ]?team|people|who[- ]?we/.test(blob)) {
    return FOLLOWUP_LINK_CLASS.STAFF_DIRECTORY;
  }
  if (/leadership|board|about[- ]?us|executives?/.test(blob)) {
    return FOLLOWUP_LINK_CLASS.LEADERSHIP_PAGE;
  }
  if (/event[- ]?team|meetings|conference[- ]?staff|organizer|planner/.test(blob)) {
    return FOLLOWUP_LINK_CLASS.EVENT_TEAM;
  }
  if (/contact|inquiry|enquire|reach/.test(blob)) {
    return FOLLOWUP_LINK_CLASS.CONTACT_PAGE;
  }
  return FOLLOWUP_LINK_CLASS.OTHER_RELEVANT;
}

function mapFunctionalLabel(email, roleHint) {
  const local = String(email || "").split("@")[0].toLowerCase();
  const hint = clean(roleHint).toLowerCase();
  if (/hous|hotel|lodg|accommod/.test(local + hint)) return "Housing Team";
  if (/registr/.test(local + hint)) return "Registration";
  if (/wedding|banquet/.test(local + hint)) return "Venue Event Sales";
  if (/group|sales/.test(local + hint)) return "Group Sales";
  if (/meeting/.test(local + hint)) return "Meetings Office";
  if (/conference/.test(local + hint)) return "Conference Office";
  if (/event/.test(local + hint)) return "Events Team";
  if (/planner|planning/.test(local + hint)) return "Meeting Planning";
  if (hint && !/functional_backup|unknown|n\/a/i.test(hint)) return clean(roleHint);
  return "Organization contact desk";
}

function isIrrelevantPerson(person = {}, opportunity = {}) {
  const role = `${person.role || ""} ${person.title || ""}`;
  if (IRRELEVANT_ROLE_RE.test(role)) return true;
  // Speakers / agenda noise
  if (/speaker|keynote|panel/i.test(role) && !/director|manager|coordinator|owner/i.test(role)) {
    return true;
  }
  // Reject placeholder / enum leakage
  if (/^(FUNCTIONAL_BACKUP|PRIMARY_CONTACT|UNRESOLVED|UNKNOWN)$/i.test(clean(person.name))) {
    return true;
  }
  return false;
}

function personScore(person, opportunity = {}) {
  const eventFamily = classifyEventFamily(opportunity);
  const role = classifyGdiContactRole(
    { name: person.personName || person.name, role: person.role, title: person.role },
    opportunity
  );
  const relevance = classifyRoleRelevance(role, eventFamily);
  let score = 10;
  if (relevance === "PRIMARY_DECISION_MAKER") score += 40;
  else if (relevance === "STRONG_INFLUENCER") score += 28;
  else if (relevance === "OPERATIONAL_CONTACT") score += 18;
  else if (relevance === "BACKUP_CONTACT") score += 8;
  else score -= 5;

  if (/conference director|director of meetings|meetings manager|housing coordinator|tournament director|event director|group sales/i.test(person.role || "")) {
    score += 15;
  }
  if (person.email && FUNCTIONAL_LOCAL_RE.test(person.email) === false && !GENERIC_LOCAL_RE.test(person.email)) {
    score += 8;
  }
  if (person.phone) score += 4;
  if (person.sourceType === "official_web") score += 5;
  if (IRRELEVANT_ROLE_RE.test(person.role || "")) score -= 50;
  return { score, role, relevance };
}

/**
 * Extract contact / staff follow-up links from HTML.
 */
export function extractContactFollowupLinks(html, baseUrl) {
  const links = extractSameDomainLinks(html || "", baseUrl) || [];
  const out = [];
  const seen = new Set();
  for (const link of links) {
    const href = typeof link === "string" ? link : link.url || link.href;
    const anchor = typeof link === "string" ? "" : link.anchor || link.text || "";
    if (!href || seen.has(href)) continue;
    const score = scoreStaffUrl(href, anchor);
    if (score < 1 && !/contact|staff|hous|registr|event|meeting|leadership|team/i.test(`${href} ${anchor}`)) {
      continue;
    }
    seen.add(href);
    out.push({
      url: href,
      anchor: clean(anchor) || null,
      class: classifyFollowupLink(href, anchor),
      score: Number(score) || 0,
    });
  }
  return out
    .sort((a, b) => b.score - a.score)
    .slice(0, 12);
}

/**
 * Core service: extract contact intelligence from already-fetched page content.
 *
 * @param {object} args
 * @param {object} [args.source] — { url, sourceType }
 * @param {string} [args.pageContent] — HTML or text
 * @param {object} [args.opportunityContext]
 * @param {object} [args.organizationContext]
 * @param {object} [args.eventContext]
 */
export function extractContactIntelligenceFromSource({
  source = {},
  pageContent = "",
  opportunityContext = {},
  organizationContext = {},
  eventContext = {},
} = {}) {
  const url = clean(source.url || source.sourceUrl || opportunityContext.officialSource);
  const html = String(pageContent || "");
  const opportunity = {
    ...opportunityContext,
    organizationName:
      opportunityContext.organizationName ||
      organizationContext.name ||
      organizationContext.organizationName,
    title: opportunityContext.title || eventContext.name || eventContext.title,
  };

  const raw = html.includes("<")
    ? extractContactsFromHtmlV5(html, url)
    : { people: [], functional: [], sections: [] };

  if (html.includes("<")) {
    const loose = extractLooseHtmlPeople(html, url);
    raw.people = [...(raw.people || []), ...(loose.people || [])];
    raw.functional = [...(raw.functional || []), ...(loose.functional || [])];
  }

  // Text / PDF-extracted text: name/role + nearby public email/phone
  if (html && !html.includes("<")) {
    const text = html;
    const pairRe = new RegExp(
      String.raw`\b([A-Z][a-z]+(?:\s+[A-Z][a-z'.-]+){1,3})\s*[,|–\-—:]\s*(${ROLE_BODY})`,
      "g"
    );
    let m;
    while ((m = pairRe.exec(text))) {
      const name = normalizeLoosePersonName(m[1]);
      if (!name) continue;
      const role = cleanLooseRole(m[2]);
      if (IRRELEVANT_ROLE_RE.test(role)) continue;
      const nearby = text.slice(m.index, m.index + m[0].length + 120);
      const emailMatch = nearby.match(/\b([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})\b/);
      const phoneMatch = nearby.match(/\b(\+?1?[\s.-]?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4})\b/);
      const email = emailMatch?.[1] || null;
      if (email && (FUNCTIONAL_LOCAL_RE.test(email) || GENERIC_LOCAL_RE.test(email))) {
        raw.functional.push({
          email,
          role,
          sourceUrl: url,
          evidenceType: "TEXT_PDF_FUNCTIONAL",
        });
      }
      raw.people.push({
        name,
        role,
        email: email && !FUNCTIONAL_LOCAL_RE.test(email) && !GENERIC_LOCAL_RE.test(email) ? email : null,
        phone: phoneMatch?.[1] || null,
        sourceUrl: url,
        evidenceType: "TEXT_PDF_NAME_ROLE",
      });
    }
    // Bare functional inboxes in prospectus / registration PDFs
    const emailRe = /\b([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})\b/g;
    let em;
    while ((em = emailRe.exec(text))) {
      const email = em[1];
      if (!(FUNCTIONAL_LOCAL_RE.test(email) || GENERIC_LOCAL_RE.test(email))) continue;
      const ctx = text.slice(Math.max(0, em.index - 80), em.index + 40);
      if (!/registr|housing|event|meeting|conference|group|sales|contact/i.test(ctx)) continue;
      raw.functional.push({
        email,
        role: /registr/i.test(ctx)
          ? "Registration"
          : /hous/i.test(ctx)
            ? "Housing"
            : "Events",
        sourceUrl: url,
        evidenceType: "TEXT_PDF_FUNCTIONAL_BARE",
      });
    }
  }

  const people = [];
  const seenPeople = new Set();
  for (const p of raw.people || []) {
    if (isIrrelevantPerson(p, opportunity)) continue;
    const normalizedName = normalizeLoosePersonName(p.name) || (isLikelyPersonName(p.name) ? clean(p.name) : null);
    if (!normalizedName) continue;
    p.name = normalizedName;
    const nameOk = hasNamedPerson({ name: p.name }) || isLikelyPersonName(p.name);
    if (!nameOk) continue;
    // Soft-allow names rejected only by org-token last names when role is event-relevant
    if (
      !hasNamedPerson({ name: p.name }) &&
      !/director|manager|coordinator|owner|planner|housing|meetings|conference|events/i.test(
        p.role || ""
      )
    ) {
      continue;
    }
    const key = `${String(p.name).toLowerCase()}|${String(p.email || "").toLowerCase()}`;
    if (seenPeople.has(key)) continue;
    seenPeople.add(key);
    const scored = personScore(
      { personName: p.name, role: p.role, email: p.email, phone: p.phone },
      opportunity
    );
    if (scored.relevance === "WEAK_RELEVANCE" && scored.score < 15) continue;
    people.push({
      personName: p.name,
      role:
        cleanLooseRole(
          String(p.role || "")
            .replace(/&[a-z]+;/gi, " ")
            .replace(/Email\b.*/i, "")
            .replace(/Phone\b.*/i, "")
        ) || null,
      organization:
        p.organization ||
        opportunity.organizationName ||
        organizationContext.name ||
        null,
      stakeholderClass: scored.role || GDI_CONTACT_ROLE.OTHER_RELEVANT,
      relevance: scored.relevance,
      whyRelevant: `Public source lists ${p.name} as ${p.role || "staff"} for this event/program.`,
      email: p.email || null,
      phone: p.phone || null,
      publicProfileUrl: p.linkedinUrl || p.profileUrl || null,
      sourceUrl: url,
      sourceType: source.sourceType || "official_web",
      confidence: Math.min(95, 45 + scored.score),
      contactDataOrigin: CONTACT_DATA_ORIGIN.PUBLIC_SOURCE,
      nameSource: url,
      roleSource: url,
      emailSource: p.email ? url : null,
      phoneSource: p.phone ? url : null,
      profileSource: p.linkedinUrl || p.profileUrl ? url : null,
      verifiedAt: new Date().toISOString(),
      _score: scored.score,
    });
  }

  const functionalContacts = [];
  for (const f of raw.functional || []) {
    const email = f.email || null;
    if (email && GENERIC_LOCAL_RE.test(email) && !FUNCTIONAL_LOCAL_RE.test(email)) {
      // Keep as organizationContacts / weak functional — still record
    }
    const label = mapFunctionalLabel(email, f.role);
    const functionType = FUNCTIONAL_LOCAL_RE.test(email || "")
      ? "FUNCTIONAL_INBOX"
      : GENERIC_LOCAL_RE.test(email || "")
        ? "GENERIC_INBOX"
        : "FUNCTIONAL_DESK";
    functionalContacts.push({
      label,
      functionType,
      email,
      phone: f.phone || null,
      contactUrl: url,
      organization: opportunity.organizationName || null,
      sourceUrl: url,
      confidence: FUNCTIONAL_LOCAL_RE.test(email || "") ? 70 : 45,
      contactDataOrigin: CONTACT_DATA_ORIGIN.PUBLIC_SOURCE,
      emailSource: email ? url : null,
      phoneSource: f.phone ? url : null,
      verifiedAt: new Date().toISOString(),
    });
  }

  const followups = extractContactFollowupLinks(html, url);
  const contactPages = followups.filter((l) => l.class === FOLLOWUP_LINK_CLASS.CONTACT_PAGE);
  const staffDirectoryLinks = followups.filter(
    (l) =>
      l.class === FOLLOWUP_LINK_CLASS.STAFF_DIRECTORY ||
      l.class === FOLLOWUP_LINK_CLASS.LEADERSHIP_PAGE ||
      l.class === FOLLOWUP_LINK_CLASS.EVENT_TEAM
  );

  people.sort((a, b) => (b._score || 0) - (a._score || 0));
  const primary = people[0] || null;
  const backups = people.slice(1, 4);

  // Prefer strong functional over generic-only when no named person
  let primaryFunctional = null;
  const strongFn = functionalContacts.find((f) => f.functionType === "FUNCTIONAL_INBOX");
  const weakFn = functionalContacts.find((f) => f.functionType === "GENERIC_INBOX");
  primaryFunctional = strongFn || weakFn || null;

  const organizationContacts = [];
  if (!primary && primaryFunctional) {
    organizationContacts.push(primaryFunctional);
  }

  const publicProfileLeads = people
    .filter((p) => p.publicProfileUrl)
    .map((p) => ({
      personName: p.personName,
      role: p.role,
      profileUrl: p.publicProfileUrl,
      sourceUrl: p.profileSource || url,
    }));

  const hasClues = Boolean(
    people.length ||
      functionalContacts.length ||
      contactPages.length ||
      staffDirectoryLinks.length
  );

  return {
    sourceUrl: url,
    hasContactClues: hasClues,
    people: people.map(({ _score, ...rest }) => rest),
    functionalContacts,
    contactPages,
    staffDirectoryLinks,
    publicProfileLeads,
    organizationContacts,
    followupLinks: followups,
    primaryContactCandidate: primary
      ? stripSurfeProviderPii({
          name: primary.personName,
          role: primary.role,
          organization: primary.organization,
          email: primary.email,
          phone: primary.phone,
          sourceUrl: primary.sourceUrl,
          gdiContactRole: primary.stakeholderClass,
          whyThisContact: primary.whyRelevant,
          functionalEntity: false,
          contactDataOrigin: CONTACT_DATA_ORIGIN.PUBLIC_SOURCE,
        })
      : primaryFunctional
        ? stripSurfeProviderPii({
            name: primaryFunctional.label,
            role: primaryFunctional.label,
            organization: primaryFunctional.organization,
            email: primaryFunctional.functionType === "GENERIC_INBOX" ? null : primaryFunctional.email,
            phone: primaryFunctional.phone,
            sourceUrl: primaryFunctional.sourceUrl,
            functionalEntity: true,
            whyThisContact: `Official ${primaryFunctional.label} path on source page.`,
            gdiContactRole: GDI_CONTACT_ROLE.GENERAL_ORGANIZATION_CONTACT,
            contactDataOrigin: CONTACT_DATA_ORIGIN.PUBLIC_SOURCE,
          })
        : null,
    backupContactCandidates: backups.map((p) =>
      stripSurfeProviderPii({
        name: p.personName,
        role: p.role,
        organization: p.organization,
        email: p.email,
        phone: p.phone,
        sourceUrl: p.sourceUrl,
        functionalEntity: false,
        whyThisContact: p.whyRelevant,
        contactDataOrigin: CONTACT_DATA_ORIGIN.PUBLIC_SOURCE,
      })
    ),
    metrics: {
      namedPeople: people.length,
      functionalContacts: functionalContacts.length,
      publicEmails: [
        ...people.filter((p) => p.email).map((p) => p.email),
        ...functionalContacts.filter((f) => f.email).map((f) => f.email),
      ].length,
      publicPhones: [
        ...people.filter((p) => p.phone),
        ...functionalContacts.filter((f) => f.phone),
      ].length,
      staffContactLinks: staffDirectoryLinks.length + contactPages.length,
      hasContactClues: hasClues,
    },
  };
}

/**
 * Apply extraction result onto an opportunity (in-memory). Never sets NEW.
 * Surfe PII never enters.
 */
export function applySourceContactExtractionToOpportunity(opportunity = {}, extraction = {}) {
  const beforeTier = classifyContactTier(opportunity);
  if (!extraction || !extraction.hasContactClues) {
    return {
      opportunity,
      beforeTier,
      afterTier: beforeTier,
      improved: false,
      extraction,
    };
  }

  const primary = extraction.primaryContactCandidate;
  if (!primary) {
    return {
      opportunity: {
        ...opportunity,
        contactOfficialUrl:
          opportunity.contactOfficialUrl ||
          extraction.contactPages[0]?.url ||
          extraction.sourceUrl ||
          null,
        contactFollowupLinks: extraction.followupLinks || [],
      },
      beforeTier,
      afterTier: beforeTier,
      improved: false,
      extraction,
    };
  }

  // Do not downgrade a stronger named contact with weaker functional
  const existingNamed = hasNamedPerson(opportunity.primaryContact || {});
  const incomingNamed = hasNamedPerson(primary);
  if (existingNamed && !incomingNamed) {
    return {
      opportunity: {
        ...opportunity,
        backupContacts: [
          ...(opportunity.backupContacts || []),
          primary,
        ].slice(0, 3),
        contactFollowupLinks: extraction.followupLinks || opportunity.contactFollowupLinks,
      },
      beforeTier,
      afterTier: beforeTier,
      improved: false,
      extraction,
    };
  }

  const next = {
    ...opportunity,
    primaryContact: primary,
    primaryContactName: primary.name,
    primaryContactRole: primary.role,
    primaryContactEmail: primary.email || null,
    primaryContactPhone: primary.phone || null,
    backupContacts: extraction.backupContactCandidates || [],
    contactOfficialUrl:
      extraction.contactPages[0]?.url ||
      extraction.sourceUrl ||
      opportunity.contactOfficialUrl ||
      null,
    contactSourceUrl: extraction.sourceUrl || null,
    contactSourceType: "official_web",
    contactDataOrigin: CONTACT_DATA_ORIGIN.PUBLIC_SOURCE,
    contactVerifiedAt: new Date().toISOString(),
    contactFollowupLinks: extraction.followupLinks || [],
    // Preserve weekly NEW if already NEW
    weeklyDeltaState: opportunity.weeklyDeltaState,
    isNewThisWeek: opportunity.isNewThisWeek,
  };
  next.contactTier = classifyContactTier(next);
  const afterTier = next.contactTier;
  const improved =
    beforeTier !== afterTier ||
    (!opportunity.primaryContactName && next.primaryContactName) ||
    (!opportunity.primaryContactEmail && next.primaryContactEmail);

  return { opportunity: next, beforeTier, afterTier, improved, extraction };
}

/**
 * Dual extract helper for harvest paths that already hold page HTML/text.
 */
export function extractDemandAndContactFromPage({
  url,
  pageContent,
  opportunityContext = {},
  demandFlags = {},
} = {}) {
  const contact = extractContactIntelligenceFromSource({
    source: { url, sourceType: "official_web" },
    pageContent,
    opportunityContext,
  });
  return {
    demand: {
      hasLodging: Boolean(demandFlags.hasLodging),
      hasFuture: Boolean(demandFlags.hasFuture),
      ...demandFlags,
    },
    contact,
  };
}

export { classifyContactTier, CONTACT_TIER };
