/**
 * Page/document section semantics for Native WHO V5.
 * People inherit section context; source authority cannot override bad sections.
 */

export const SECTION_KIND = Object.freeze({
  CONTACT: "CONTACT",
  EVENT_TEAM: "EVENT_TEAM",
  CONFERENCE_STAFF: "CONFERENCE_STAFF",
  TOURNAMENT_LEADERSHIP: "TOURNAMENT_LEADERSHIP",
  ORGANIZERS: "ORGANIZERS",
  HOUSING: "HOUSING",
  REGISTRATION: "REGISTRATION",
  MEETINGS: "MEETINGS",
  EVENT_OPERATIONS: "EVENT_OPERATIONS",
  STAFF: "STAFF",
  LEADERSHIP: "LEADERSHIP",
  ABOUT: "ABOUT",
  PROGRAM_COMMITTEE: "PROGRAM_COMMITTEE",
  SPEAKERS: "SPEAKERS",
  SPONSORS: "SPONSORS",
  EXHIBITORS: "EXHIBITORS",
  BOARD: "BOARD",
  NEWS: "NEWS",
  PRESS: "PRESS",
  TESTIMONIALS: "TESTIMONIALS",
  MEMBERS: "MEMBERS",
  ATTENDEES: "ATTENDEES",
  PARTNERS: "PARTNERS",
  VENDORS: "VENDORS",
  VOLUNTEERS: "VOLUNTEERS",
  UNKNOWN: "UNKNOWN",
});

/** Role-evidence prior from section alone. */
export const SECTION_PRIOR = Object.freeze({
  [SECTION_KIND.CONTACT]: "HIGH",
  [SECTION_KIND.EVENT_TEAM]: "HIGH",
  [SECTION_KIND.CONFERENCE_STAFF]: "HIGH",
  [SECTION_KIND.TOURNAMENT_LEADERSHIP]: "HIGH",
  [SECTION_KIND.ORGANIZERS]: "HIGH",
  [SECTION_KIND.HOUSING]: "HIGH",
  [SECTION_KIND.REGISTRATION]: "HIGH",
  [SECTION_KIND.MEETINGS]: "HIGH",
  [SECTION_KIND.EVENT_OPERATIONS]: "HIGH",
  [SECTION_KIND.STAFF]: "MEDIUM",
  [SECTION_KIND.LEADERSHIP]: "MEDIUM_LOW",
  [SECTION_KIND.ABOUT]: "MEDIUM_LOW",
  [SECTION_KIND.PROGRAM_COMMITTEE]: "MEDIUM",
  [SECTION_KIND.SPEAKERS]: "REJECT",
  [SECTION_KIND.SPONSORS]: "REJECT",
  [SECTION_KIND.EXHIBITORS]: "REJECT",
  [SECTION_KIND.BOARD]: "LOW",
  [SECTION_KIND.NEWS]: "IDENTITY_ONLY",
  [SECTION_KIND.PRESS]: "IDENTITY_ONLY",
  [SECTION_KIND.TESTIMONIALS]: "REJECT",
  [SECTION_KIND.MEMBERS]: "REJECT",
  [SECTION_KIND.ATTENDEES]: "REJECT",
  [SECTION_KIND.PARTNERS]: "REJECT",
  [SECTION_KIND.VENDORS]: "REJECT",
  [SECTION_KIND.VOLUNTEERS]: "REJECT",
  [SECTION_KIND.UNKNOWN]: "LOW",
});

const SECTION_PATTERNS = [
  { kind: SECTION_KIND.SPEAKERS, re: /\b(speakers?|keynotes?|panelists?|presenters?|faculty)\b/i },
  { kind: SECTION_KIND.SPONSORS, re: /\b(sponsors?|sponsorship|underwriters?)\b/i },
  { kind: SECTION_KIND.EXHIBITORS, re: /\b(exhibitors?|exhibit hall|booth)\b/i },
  { kind: SECTION_KIND.BOARD, re: /\b(board of directors|board members?|trustees)\b/i },
  { kind: SECTION_KIND.PRESS, re: /\b(press release|newsroom|media alert)\b/i },
  { kind: SECTION_KIND.NEWS, re: /\b(news|articles?|blog|viewpoint)\b/i },
  { kind: SECTION_KIND.VOLUNTEERS, re: /\b(volunteers?|volunteer descriptions?)\b/i },
  { kind: SECTION_KIND.PARTNERS, re: /\b(partners?|partner organizations?)\b/i },
  { kind: SECTION_KIND.VENDORS, re: /\b(vendors?|suppliers?)\b/i },
  { kind: SECTION_KIND.TESTIMONIALS, re: /\b(testimonials?|what people say)\b/i },
  { kind: SECTION_KIND.ATTENDEES, re: /\b(attendees?|registrants?)\b/i },
  { kind: SECTION_KIND.MEMBERS, re: /\b(member directory|our members)\b/i },
  {
    kind: SECTION_KIND.TOURNAMENT_LEADERSHIP,
    re: /\b(tournament (?:leadership|directors?|staff)|executive staff)\b/i,
  },
  {
    kind: SECTION_KIND.CONFERENCE_STAFF,
    re: /\b(conference staff|event team|convention (?:team|staff)|event operations)\b/i,
  },
  { kind: SECTION_KIND.ORGANIZERS, re: /\b(organizers?|organising committee)\b/i },
  { kind: SECTION_KIND.HOUSING, re: /\b(housing|hotel block|room block|accommodations?)\b/i },
  { kind: SECTION_KIND.REGISTRATION, re: /\b(registration|registrar)\b/i },
  { kind: SECTION_KIND.MEETINGS, re: /\b(meetings?|events? department|member services)\b/i },
  { kind: SECTION_KIND.CONTACT, re: /\b(contact us|contact|get in touch)\b/i },
  { kind: SECTION_KIND.EVENT_TEAM, re: /\b(event team|events team)\b/i },
  { kind: SECTION_KIND.EVENT_OPERATIONS, re: /\b(event operations|operations team)\b/i },
  { kind: SECTION_KIND.PROGRAM_COMMITTEE, re: /\b(program committee|planning committee)\b/i },
  { kind: SECTION_KIND.STAFF, re: /\b(staff directory|our staff|our team|people behind)\b/i },
  { kind: SECTION_KIND.LEADERSHIP, re: /\b(leadership|executive team)\b/i },
  { kind: SECTION_KIND.ABOUT, re: /\b(about us|who we are)\b/i },
];

/**
 * Classify URL path / title into a section kind.
 */
export function classifySectionFromUrl(url = "", title = "") {
  const blob = `${url} ${title}`.toLowerCase();
  if (/\/press\/|press-release|newsroom|newsdesk|submenu=press/i.test(blob)) {
    return SECTION_KIND.PRESS;
  }
  if (/\/news\/|\/articles?\/|\/blog\//i.test(blob)) return SECTION_KIND.NEWS;
  if (/speaker/i.test(blob)) return SECTION_KIND.SPEAKERS;
  // Org partnership/BD contact pages are CONTACT, not sponsor/partner directories.
  if (
    /partner-us|partner-with-us|become-a-partner|partnerships?\/contact|\/partner\/?(\?|$)/i.test(
      blob
    )
  ) {
    return SECTION_KIND.CONTACT;
  }
  if (/sponsor/i.test(blob)) return SECTION_KIND.SPONSORS;
  if (/volunteer/i.test(blob)) return SECTION_KIND.VOLUNTEERS;
  if (/board/i.test(blob)) return SECTION_KIND.BOARD;
  if (/contact/i.test(blob)) return SECTION_KIND.CONTACT;
  if (/staff|team|people|directory/i.test(blob)) return SECTION_KIND.STAFF;
  if (/housing|hotel/i.test(blob)) return SECTION_KIND.HOUSING;
  if (/registration/i.test(blob)) return SECTION_KIND.REGISTRATION;
  if (/tournament-schedule|\/schedule|event-staff|usta-|nationalclays/i.test(blob)) {
    return SECTION_KIND.TOURNAMENT_LEADERSHIP;
  }
  // Official event detail pages are event-team evidence, not UNKNOWN.
  if (/\/events?\/|\/conference|\/summit|\/forum|\/tournament/i.test(blob)) {
    return SECTION_KIND.EVENT_TEAM;
  }
  return SECTION_KIND.UNKNOWN;
}

/**
 * Classify local text window (heading + body) into section kind.
 */
export function classifySectionFromText(text = "") {
  const t = String(text || "");
  for (const { kind, re } of SECTION_PATTERNS) {
    if (re.test(t)) return kind;
  }
  return SECTION_KIND.UNKNOWN;
}

/**
 * Segment HTML into coarse sections by headings.
 * @param {string} html
 * @returns {Array<{ kind: string, heading: string, text: string, html: string }>}
 */
export function segmentHtmlByHeadings(html) {
  const raw = String(html || "");
  if (!raw) return [];

  const parts = raw.split(/<(h[1-6]|section|article)[^>]*>/i);
  // Simpler: split on heading open tags with capture of heading text
  const re = /<(h[1-6])[^>]*>([\s\S]*?)<\/\1>/gi;
  const headings = [];
  let m;
  while ((m = re.exec(raw))) {
    headings.push({
      index: m.index,
      end: m.index + m[0].length,
      heading: stripTags(m[2]).trim().slice(0, 120),
    });
  }

  if (!headings.length) {
    const text = stripTags(raw).replace(/\s+/g, " ").trim().slice(0, 12000);
    return [
      {
        kind: classifySectionFromText(text),
        heading: "",
        text,
        html: raw.slice(0, 100_000),
      },
    ];
  }

  const sections = [];
  for (let i = 0; i < headings.length; i++) {
    const start = headings[i].index;
    const end = i + 1 < headings.length ? headings[i + 1].index : Math.min(raw.length, start + 40_000);
    const chunkHtml = raw.slice(start, end);
    const text = stripTags(chunkHtml).replace(/\s+/g, " ").trim().slice(0, 8000);
    const heading = headings[i].heading;
    sections.push({
      kind: classifySectionFromText(`${heading} ${text.slice(0, 400)}`),
      heading,
      text,
      html: chunkHtml.slice(0, 80_000),
    });
  }
  return sections.slice(0, 40);
}

function stripTags(html) {
  return String(html || "")
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&");
}

export function sectionAllowsWhoConfirmation(kind) {
  const prior = SECTION_PRIOR[kind] || "LOW";
  return prior === "HIGH" || prior === "MEDIUM" || prior === "MEDIUM_LOW";
}

export function sectionIsReject(kind) {
  return SECTION_PRIOR[kind] === "REJECT";
}

export function sectionIsIdentityOnly(kind) {
  return SECTION_PRIOR[kind] === "IDENTITY_ONLY";
}
