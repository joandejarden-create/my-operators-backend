/**
 * Structured PDF entity + housing signal extraction for hidden demand V2.
 * Reuses pdf-parse via Native WHO fetchPdfText (no OCR).
 */

import { fetchPdfText } from "../contact-candidate/native-who-v3/pdf-contact-extraction.js";
import { ENTITY_TYPE, PARTICIPATION_ROLE, SOURCE_TYPE } from "./v2-constants.js";
import { normalizeOrganizationName } from "./entity-normalize.js";
import { isPlausibleOrganizationName } from "./quality-gate.js";

const ORG_LINE_RE =
  /\b([A-Z][A-Za-z0-9&.\-']+(?:\s+[A-Z][A-Za-z0-9&.\-']+){1,6})\b/g;

const ROLE_HINTS = [
  { re: /\b(exhibitor|booth)\b/i, role: PARTICIPATION_ROLE.EXHIBITOR, type: ENTITY_TYPE.COMPANY },
  { re: /\b(sponsor|presented by|powered by)\b/i, role: PARTICIPATION_ROLE.SPONSOR, type: ENTITY_TYPE.SPONSOR },
  { re: /\b(committee|task force|working group|special interest)\b/i, role: PARTICIPATION_ROLE.COMMITTEE, type: ENTITY_TYPE.SUBGROUP },
  { re: /\b(board of directors|advisory board|leadership council)\b/i, role: PARTICIPATION_ROLE.BOARD, type: ENTITY_TYPE.SUBGROUP },
  { re: /\b(speaker|keynote|panelist|presenter)\b/i, role: PARTICIPATION_ROLE.SPEAKER, type: ENTITY_TYPE.COMPANY },
  { re: /\b(production|staging|av\b|technical services)\b/i, role: PARTICIPATION_ROLE.CREW, type: ENTITY_TYPE.PRODUCTION_COMPANY },
  { re: /\b(tour operator|escorted|departure)\b/i, role: PARTICIPATION_ROLE.TOUR_OPERATOR, type: ENTITY_TYPE.TOUR_OPERATOR },
  { re: /\b(delegation|trade mission)\b/i, role: PARTICIPATION_ROLE.DELEGATION, type: ENTITY_TYPE.DELEGATION },
];

/**
 * Extract entities from PDF text with role context.
 */
export function extractEntitiesFromProgramText(text = "", meta = {}) {
  const sourceType = meta.sourceType || SOURCE_TYPE.PROGRAM_PDF;
  const sourceURL = meta.sourceURL || null;
  const entities = [];
  const seen = new Set();
  const t = String(text || "");
  const lines = t.split(/(?<=[.!?])\s+|\n+/).slice(0, 2000);

  const push = (name, role, type, snippet, confidence = 0.55) => {
    const { displayName, normalizeKey } = normalizeOrganizationName(name);
    if (!isPlausibleOrganizationName(displayName)) return;
    if (seen.has(normalizeKey)) return;
    seen.add(normalizeKey);
    entities.push({
      entityName: displayName,
      normalizeKey,
      entityType: type,
      sourceType,
      sourceURL,
      sourceAuthority: "program_document",
      demandGeneratorId: meta.demandGeneratorId || null,
      demandGeneratorName: meta.demandGeneratorName || null,
      participationRole: role,
      futureTiming: meta.futureTiming !== false,
      year: meta.year || null,
      eventCycleId: meta.eventCycleId || (meta.year ? String(meta.year) : null),
      evidenceSnippet: String(snippet || displayName).slice(0, 240),
      confidence,
      family: meta.family || null,
    });
  };

  // Section-aware: when we see "Exhibitors" / "Sponsors" / "Committees", harvest following Title Case names
  let sectionRole = PARTICIPATION_ROLE.PARTICIPANT;
  let sectionType = ENTITY_TYPE.COMPANY;
  for (const line of lines) {
    const low = line.toLowerCase();
    if (/^exhibitors?\b|^who's exhibiting|^exhibitor list/.test(low)) {
      sectionRole = PARTICIPATION_ROLE.EXHIBITOR;
      sectionType = ENTITY_TYPE.COMPANY;
      continue;
    }
    if (/^sponsors?\b|^sponsorship/.test(low)) {
      sectionRole = PARTICIPATION_ROLE.SPONSOR;
      sectionType = ENTITY_TYPE.SPONSOR;
      continue;
    }
    if (/committee|task force|working group|board of|advisory board|leadership/.test(low) && line.length < 80) {
      sectionRole = /board|advisory|leadership/.test(low)
        ? PARTICIPATION_ROLE.BOARD
        : PARTICIPATION_ROLE.COMMITTEE;
      sectionType = ENTITY_TYPE.SUBGROUP;
      // The section title itself may be a subgroup
      const { displayName } = normalizeOrganizationName(line.replace(/[:\-].*$/, "").trim());
      if (isPlausibleOrganizationName(displayName) && displayName.split(/\s+/).length >= 2) {
        push(displayName, sectionRole, sectionType, line, 0.7);
      }
      continue;
    }

    for (const hint of ROLE_HINTS) {
      if (hint.re.test(line)) {
        sectionRole = hint.role;
        sectionType = hint.type;
        break;
      }
    }

    // Title-case org on same line as role word — require corp/org marker
    for (const hint of ROLE_HINTS) {
      if (!hint.re.test(line)) continue;
      if (!/\b(Inc\.?|LLC|Ltd\.?|Corp\.?|Group|Systems|Solutions|Partners|International|Company|&)\b/i.test(line)) {
        continue;
      }
      let mm;
      ORG_LINE_RE.lastIndex = 0;
      while ((mm = ORG_LINE_RE.exec(line)) && entities.length < 300) {
        const cand = mm[1];
        if (/^(The|And|For|With|From|January|February|March|April|May|June|July|August|September|October|November|December|How|To|Manage)$/.test(cand)) {
          continue;
        }
        push(cand, hint.role, hint.type, line, 0.62);
      }
    }

    // Under active section, harvest plausible org lines — require corp marker or booth context
    if (
      (sectionRole === PARTICIPATION_ROLE.EXHIBITOR ||
        sectionRole === PARTICIPATION_ROLE.SPONSOR) &&
      /^[A-Z][A-Za-z0-9&.\-']+(?:\s+[A-Z][A-Za-z0-9&.\-']+){1,5}$/.test(line.trim()) &&
      line.trim().length >= 5 &&
      line.trim().length <= 80 &&
      /\b(Inc\.?|LLC|Ltd\.?|Corp\.?|Corporation|Company|Group|Systems|Solutions|Partners|International|Holdings|&)\b/i.test(
        line
      )
    ) {
      push(line.trim(), sectionRole, sectionType, line, 0.72);
    }
  }

  // Contacts from PDF (names near emails) — functional path only; not invented
  const emailRe = /([A-Z][a-z]+(?:\s+[A-Z][a-z.'-]+){1,2})\s*[,|\-|–]?\s*([A-Za-z][A-Za-z\s/&]{2,40})?\s*([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})/g;
  let em;
  const contacts = [];
  while ((em = emailRe.exec(t)) && contacts.length < 20) {
    contacts.push({
      name: em[1],
      title: (em[2] || "").trim() || null,
      email: em[3],
      source: sourceURL,
      scope: "first_party_pdf",
    });
  }

  return { entities, contacts };
}

/**
 * Parse housing / registration PDF for lodging signals (not room counts invented).
 */
export function extractHousingSignalsFromText(text = "", url = null) {
  const t = String(text || "");
  const out = {
    sourceUrl: url,
    housingPageFound: /hous(ing|e)|accommodation|hotel\s*block|room\s*block/i.test(t),
    roomBlockMentioned: /hotel\s*block|room\s*block|group\s*rate|official\s*hotel|housing\s*bureau/i.test(t),
    hostHotelMentioned: /host\s*hotel|headquarters\s*hotel|official\s*hotel/i.test(t),
    overflowMentioned: /overflow|additional\s*hotels?|rooming\s*list/i.test(t),
    housingOpen: null,
    housingDeadline: null,
    housingCompany: null,
    groupCoordinator: null,
    internationalDelegationMentioned: /international\s+delegat|overseas\s+attendee|travel\s+from\s+abroad/i.test(t),
    participantManagedTravel: /book\s+your\s+own|arrange\s+own\s+accommodation|participant[- ]managed/i.test(t),
    unresolvedRoomSourcing: /housing\s+(tbd|coming\s+soon|not\s+yet)|hotel\s+block\s+tbd/i.test(t),
    snippets: [],
  };
  if (/housing\s*(is\s*)?(now\s*)?open|register\s+for\s+housing|book\s+housing/i.test(t)) {
    out.housingOpen = true;
  } else if (/housing\s*(not\s*yet|coming\s+soon|will\s+open)/i.test(t)) {
    out.housingOpen = false;
  }
  const deadline = t.match(
    /(?:housing|hotel\s*block|room\s*block)\s*(?:deadline|closes|cut[\s-]?off)[:\s]+([A-Za-z]+\s+\d{1,2},?\s+\d{4}|\d{4}-\d{2}-\d{2})/i
  );
  if (deadline) out.housingDeadline = deadline[1];
  const company = t.match(
    /\b([A-Z][A-Za-z0-9&.\-]+(?:\s+[A-Z][A-Za-z0-9&.\-]+){0,4})\s+(?:Housing|Travel|Experient|onPeak|Connections\s+Housing)/i
  );
  if (company) out.housingCompany = company[1];
  if (out.roomBlockMentioned) out.snippets.push("room_block_language");
  if (out.overflowMentioned) out.snippets.push("overflow_language");
  if (out.internationalDelegationMentioned) out.snippets.push("international_delegation");
  return out;
}

/**
 * Fetch PDF and extract entities + housing signals.
 */
export async function fetchAndExtractPdf(url, meta = {}) {
  const fetched = await fetchPdfText(url, { timeoutMs: 22000 });
  if (!fetched.ok) {
    return { ok: false, url, reason: fetched.reason || "fetch_failed", entities: [], contacts: [], housing: null };
  }
  const sourceType =
    meta.sourceType ||
    (/hous|hotel.?block|accommodation/i.test(`${url} ${fetched.text.slice(0, 500)}`)
      ? SOURCE_TYPE.HOUSING_PDF
      : /registrat/i.test(url)
        ? SOURCE_TYPE.REGISTRATION_PDF
        : SOURCE_TYPE.PROGRAM_PDF);

  const { entities, contacts } = extractEntitiesFromProgramText(fetched.text, {
    ...meta,
    sourceType,
    sourceURL: fetched.url || url,
  });
  const housing =
    sourceType === SOURCE_TYPE.HOUSING_PDF || sourceType === SOURCE_TYPE.REGISTRATION_PDF
      ? extractHousingSignalsFromText(fetched.text, fetched.url || url)
      : extractHousingSignalsFromText(fetched.text, fetched.url || url);

  return {
    ok: true,
    url: fetched.url || url,
    sourceType,
    textLen: fetched.text.length,
    entities,
    contacts,
    housing,
  };
}
