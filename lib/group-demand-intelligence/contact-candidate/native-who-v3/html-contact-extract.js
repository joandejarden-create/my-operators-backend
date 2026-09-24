/**
 * Structural HTML person extraction with section inheritance (Native WHO V5).
 * Prefer cards / list items / table rows / mailto blocks over flattened text.
 */

import { isStrictPersonName, expandMultiPersonWithSharedRole } from "./person-boundary.js";
import {
  segmentHtmlByHeadings,
  classifySectionFromUrl,
  classifySectionFromText,
  sectionIsReject,
  SECTION_KIND,
} from "./section-semantics.js";

const MAILTO_RE = /mailto:([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})/gi;
const TEL_RE = /tel:([+\d][\d\-.\s()]{6,}\d)/gi;

/**
 * Extract people from one HTML page using section + structural units.
 */
export function extractContactsFromHtmlV5(html, url) {
  const people = [];
  const functional = [];
  if (!html) return { people, functional, sections: [] };

  const pageSection = classifySectionFromUrl(url);
  if (sectionIsReject(pageSection) && pageSection !== SECTION_KIND.UNKNOWN) {
    // Still allow functional inboxes from reject pages? No — skip person extract.
    return { people, functional, sections: [{ kind: pageSection, skipped: true }] };
  }

  const sections = segmentHtmlByHeadings(html);
  const units = [];

  for (const sec of sections) {
    if (sectionIsReject(sec.kind)) continue;

    // Structural units within section HTML
    const blocks = splitStructuralUnits(sec.html || "");
    for (const blockHtml of blocks) {
      units.push({
        sectionKind: sec.kind !== SECTION_KIND.UNKNOWN ? sec.kind : pageSection,
        heading: sec.heading,
        html: blockHtml,
        text: stripTags(blockHtml).replace(/\s+/g, " ").trim(),
      });
    }
  }

  // If no units, fall back to whole-page contact section only
  if (!units.length) {
    units.push({
      sectionKind: pageSection,
      heading: "",
      html: html.slice(0, 100_000),
      text: stripTags(html).replace(/\s+/g, " ").trim().slice(0, 8000),
    });
  }

  for (const unit of units) {
    if (sectionIsReject(unit.sectionKind)) continue;

    // mailto in this unit only
    MAILTO_RE.lastIndex = 0;
    let m;
    while ((m = MAILTO_RE.exec(unit.html))) {
      pushMailtoPerson(people, functional, m, unit.html, url, unit.sectionKind);
    }

    // Executive staff / leadership lines inside HIGH sections only
    if (
      /TOURNAMENT_LEADERSHIP|CONFERENCE_STAFF|ORGANIZERS|EVENT_TEAM|STAFF|CONTACT|MEETINGS/i.test(
        unit.sectionKind
      )
    ) {
      const pairRe =
        /\b([A-Z][a-z]+(?:\s+[A-Z][a-z'.-]+){1,3})\s*[,|–\-—]\s*((?:VP|Vice President|Director|Manager|Coordinator|Executive Director|Tournament Director|Member Services|Senior Manager|Partnerships?)[^.\n|]{0,70})/g;
      let pm;
      while ((pm = pairRe.exec(unit.text))) {
        const expanded = expandMultiPersonWithSharedRole(pm[1], pm[2]);
        for (const e of expanded) {
          if (!isStrictPersonName(e.name)) continue;
          people.push({
            name: e.name,
            role: e.role || pm[2],
            email: null,
            phone: null,
            sourceUrl: url,
            sectionKind: unit.sectionKind,
            evidenceQuote: `${pm[1]}, ${pm[2]}`.slice(0, 200),
            evidenceType: "HTML_UNIT_NAME_TITLE",
            fromHtml: true,
            fromMultiPersonBlock: e.fromMultiPersonBlock,
            eventSpecificEvidence: true,
          });
        }
      }

      // Multi-person leadership headers: "Tournament Directors: A / B"
      const multi = unit.text.match(
        /((?:Tournament|Executive|Co-)?Directors?|Executive Staff|Leadership)\s*[:\-–]\s*([A-Z][^.]{5,120})/i
      );
      if (multi) {
        const expanded = expandMultiPersonWithSharedRole(multi[2], multi[1]);
        for (const e of expanded) {
          people.push({
            name: e.name,
            role: e.role || multi[1],
            email: null,
            phone: null,
            sourceUrl: url,
            sectionKind: unit.sectionKind || SECTION_KIND.TOURNAMENT_LEADERSHIP,
            evidenceQuote: multi[0].slice(0, 200),
            evidenceType: "HTML_MULTI_PERSON_BLOCK",
            fromHtml: true,
            fromMultiPersonBlock: true,
            eventSpecificEvidence: true,
          });
        }
      }
    }
  }

  // Whole-page mailto sweep — catches staff cards missed by structural unit splits
  if (!sectionIsReject(pageSection)) {
    MAILTO_RE.lastIndex = 0;
    let wm;
    while ((wm = MAILTO_RE.exec(html))) {
      pushMailtoPerson(people, functional, wm, html, url, pageSection || SECTION_KIND.STAFF);
    }
  }

  // Dedupe by name+email
  const seen = new Set();
  const deduped = [];
  for (const p of people) {
    if (!isStrictPersonName(p.name)) continue;
    const key = `${p.name.toLowerCase()}|${(p.email || "").toLowerCase()}`;
    if (seen.has(key)) continue;
    seen.add(key);
    deduped.push(p);
  }

  return {
    people: deduped.slice(0, 40),
    functional: dedupeFunctional(functional).slice(0, 8),
    sections: sections.map((s) => ({ kind: s.kind, heading: s.heading })),
  };
}

function splitStructuralUnits(html) {
  const h = String(html || "");
  const units = [];
  const tags = ["li", "tr", "article", "div"];
  for (const tag of tags) {
    const re = new RegExp(`<${tag}[^>]*>([\\s\\S]*?)<\\/${tag}>`, "gi");
    let m;
    let count = 0;
    while ((m = re.exec(h)) && count < 80) {
      const inner = m[1];
      if (inner.length < 40 || inner.length > 4000) continue;
      if (!/mailto:|[A-Z][a-z]+\s+[A-Z]|Director|Manager|Coordinator/i.test(inner)) continue;
      units.push(inner);
      count += 1;
    }
  }
  if (!units.length && h.length) units.push(h.slice(0, 20_000));
  return units;
}

function pushMailtoPerson(people, functional, m, html, url, sectionKind) {
  const email = m[1];
  const local = html.slice(Math.max(0, m.index - 220), m.index + m[0].length + 320);
  const plain = stripTags(local).replace(/\s+/g, " ");
  if (
    /^(info|events|contact|hello|office|admin|support|meetings|housing|registration|conference)@/i.test(
      email
    )
  ) {
    functional.push({
      name: "Functional inbox",
      email,
      phone: null,
      role: "FUNCTIONAL_BACKUP",
      sourceUrl: url,
      sectionKind,
      evidenceType: "HTML_MAILTO_FUNCTIONAL",
    });
    return;
  }
  // Name often sits inside the enclosing <a mailto>…</a> text
  let nameGuess = null;
  const aStart = html.lastIndexOf("<a", m.index);
  const aEnd = html.indexOf("</a>", m.index);
  if (aStart >= 0 && aEnd > aStart && m.index - aStart < 500) {
    const anchorPlain = stripTags(html.slice(aStart, aEnd + 4)).replace(/\s+/g, " ").trim();
    if (isStrictPersonName(anchorPlain)) nameGuess = anchorPlain;
    else nameGuess = guessNameFromBlock(anchorPlain, email);
  }
  if (!nameGuess) {
    const aroundAnchor = stripTags(
      html.slice(Math.max(0, m.index - 80), m.index + m[0].length + 120)
    ).replace(/\s+/g, " ");
    nameGuess = guessNameFromBlock(aroundAnchor, email);
  }
  if (!nameGuess) nameGuess = guessNameFromBlock(plain, email);
  if (!nameGuess || !isStrictPersonName(nameGuess)) return;
  const after = stripTags(html.slice(m.index, m.index + 450)).replace(/\s+/g, " ");
  const title = guessTitleFromBlock(plain) || guessTitleFromBlock(after);
  people.push({
    name: nameGuess,
    role: title,
    email,
    phone: null,
    sourceUrl: url,
    sectionKind,
    evidenceQuote: plain.slice(0, 200),
    evidenceType: "HTML_UNIT_MAILTO",
    fromHtml: true,
    fromMultiPersonBlock: false,
    eventSpecificEvidence: /CONTACT|EVENT|TOURNAMENT|CONFERENCE|MEETINGS|HOUSING|REGISTRATION|STAFF/i.test(
      sectionKind
    ),
  });
}

function guessNameFromBlock(plain, email) {
  const emailLc = String(email || "").toLowerCase();
  const idx = emailLc ? String(plain).toLowerCase().indexOf(emailLc) : -1;
  const before =
    idx >= 0 ? String(plain).slice(Math.max(0, idx - 140), idx) : String(plain || "");
  const matches = [
    ...before.matchAll(/\b([A-Z][a-z]+(?:\s+[A-Z](?:\.|[a-z'.-]+)){1,2})\b/g),
  ].map((x) => x[1]);
  const BAD =
    /^(audience|engagement|education|programs?|tony|awards|executive|director|manager|senior|staff|officer|digital|content|labor|relations|membership|services|governmental|affairs|benefit|funds|accountant|people|technology|chief|financial)$/i;
  for (let i = matches.length - 1; i >= 0; i--) {
    const n = matches[i];
    if (!isStrictPersonName(n)) continue;
    if (n.split(/\s+/).some((p) => BAD.test(p))) continue;
    return n;
  }
  return null;
}

function guessTitleFromBlock(plain) {
  const cleaned = String(plain || "")
    .replace(/<\/?[^>]+>/g, " ")
    .replace(/\s+/g, " ");
  const m = cleaned.match(
    /\b((?:Chief\s+\w+|VP|Vice President|Director|Manager|Coordinator|Executive Director|Tournament Director|Senior Manager|Member Services|Senior Staff)[^|]{0,70})/i
  );
  return m ? m[1].trim().slice(0, 90) : null;
}

function stripTags(html) {
  return String(html || "")
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<br\s*\/?>/gi, "\n")
    .replace(/<\/(p|div|li|tr|h\d)>/gi, "\n")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/g, " ")
    .replace(/&#160;/g, " ")
    .replace(/&amp;/g, "&");
}

function dedupeFunctional(list) {
  const seen = new Set();
  const out = [];
  for (const f of list) {
    const key = String(f.email || f.phone || "").toLowerCase();
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(f);
  }
  return out;
}

// Keep legacy export name used by V4 path
export { extractContactsFromHtmlV5 as extractContactsFromHtml };
