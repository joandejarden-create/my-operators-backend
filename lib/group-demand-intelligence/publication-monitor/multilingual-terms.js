/**
 * Multilingual publication-trigger terms (ES / GL / EN).
 */

import { PUBLICATION_TRIGGER_TYPE } from "./constants.js";

/** @type {Array<{ triggerType: string, languages: string[], patterns: RegExp[] }>} */
export const PUBLICATION_TERM_GROUPS = [
  {
    triggerType: PUBLICATION_TRIGGER_TYPE.PROGRAMME_PUBLISHED,
    languages: ["es", "gl", "en"],
    patterns: [
      /\bprograma\b/i,
      /\bprogramme\b/i,
      /\bprogramaci[oó]n\b/i,
      /\bschedule\b/i,
    ],
  },
  {
    triggerType: PUBLICATION_TRIGGER_TYPE.SPEAKER_LIST_PUBLISHED,
    languages: ["es", "gl", "en"],
    patterns: [
      /\bponentes?\b/i,
      /\bspeakers?\b/i,
      /\bfaculty\b/i,
      /\bkeynotes?\b/i,
      /\blista de ponentes\b/i,
    ],
  },
  {
    triggerType: PUBLICATION_TRIGGER_TYPE.PARTICIPANT_LIST_PUBLISHED,
    languages: ["es", "gl", "en"],
    patterns: [
      /\bparticipantes?\b/i,
      /\bparticipants?\b/i,
      /\bentidades participantes\b/i,
      /\bempresas participantes\b/i,
      /\blista de participantes\b/i,
    ],
  },
  {
    triggerType: PUBLICATION_TRIGGER_TYPE.EXHIBITOR_LIST_PUBLISHED,
    languages: ["es", "gl", "en"],
    patterns: [
      /\bexpositores?\b/i,
      /\bexhibitors?\b/i,
      /\blista de expositores\b/i,
      /\bdirectorio de expositores\b/i,
      /\bcat[aá]logo de expositores\b/i,
    ],
  },
  {
    triggerType: PUBLICATION_TRIGGER_TYPE.SPONSOR_LIST_PUBLISHED,
    languages: ["es", "gl", "en"],
    patterns: [
      /\bpatrocinadores?\b/i,
      /\bsponsors?\b/i,
      /\bpatrocinios?\b/i,
      /\blista de patrocinadores\b/i,
    ],
  },
  {
    triggerType: PUBLICATION_TRIGGER_TYPE.ACCEPTED_PAPERS_PUBLISHED,
    languages: ["es", "en"],
    patterns: [
      /\bcomunicaciones aceptadas\b/i,
      /\baccepted papers?\b/i,
      /\babstracts? accepted\b/i,
      /\btrabajos aceptados\b/i,
    ],
  },
  {
    triggerType: PUBLICATION_TRIGGER_TYPE.DELEGATION_LIST_PUBLISHED,
    languages: ["es", "en"],
    patterns: [/\bdelegaciones?\b/i, /\bdelegations?\b/i, /\bdelegates?\b/i],
  },
  {
    triggerType: PUBLICATION_TRIGGER_TYPE.EXHIBITOR_MANUAL_PUBLISHED,
    languages: ["es", "en"],
    patterns: [
      /\bmanual del expositor\b/i,
      /\bexhibitor manual\b/i,
      /\bgu[ií]a del expositor\b/i,
    ],
  },
  {
    triggerType: PUBLICATION_TRIGGER_TYPE.LODGING_PAGE_PUBLISHED,
    languages: ["es", "gl", "en"],
    patterns: [
      /\balojamiento\b/i,
      /\baloxamento\b/i,
      /\baccommodation\b/i,
      /\bhousing\b/i,
      /\bhospedaje\b/i,
      /\bgu[ií]a del participante\b/i,
    ],
  },
  {
    triggerType: PUBLICATION_TRIGGER_TYPE.HOTEL_LIST_PUBLISHED,
    languages: ["es", "gl", "en"],
    patterns: [
      /\bhoteles?\b/i,
      /\bhoteis\b/i,
      /\bhotels?\b/i,
      /\bhotel oficial\b/i,
      /\bofficial hotel\b/i,
      /\bhoteles recomendados\b/i,
    ],
  },
  {
    triggerType: PUBLICATION_TRIGGER_TYPE.REGISTRATION_OPENED,
    languages: ["es", "en"],
    patterns: [
      /\binscripci[oó]n\b/i,
      /\bregistration\b/i,
      /\bregistro abierto\b/i,
      /\bregistration open\b/i,
    ],
  },
];

/**
 * Detect which publication trigger types appear in text.
 * @returns {{ triggerTypes: string[], hits: Array<{ triggerType: string, match: string }> }}
 */
export function detectPublicationTriggersInText(text = "", opts = {}) {
  const blob = String(text || "");
  const wanted = Array.isArray(opts.watchForTypes) ? new Set(opts.watchForTypes) : null;
  const hits = [];
  const seen = new Set();
  for (const group of PUBLICATION_TERM_GROUPS) {
    if (wanted && !wanted.has(group.triggerType)) continue;
    for (const re of group.patterns) {
      const m = blob.match(re);
      if (m) {
        if (!seen.has(group.triggerType)) {
          seen.add(group.triggerType);
          hits.push({ triggerType: group.triggerType, match: m[0] });
        }
        break;
      }
    }
  }
  return { triggerTypes: [...seen], hits };
}

/** Flat CSV-friendly rows for reporting. */
export function listMultilingualTriggerTermRows() {
  const rows = [];
  for (const g of PUBLICATION_TERM_GROUPS) {
    for (const re of g.patterns) {
      rows.push({
        triggerType: g.triggerType,
        languages: g.languages.join("|"),
        pattern: String(re),
      });
    }
  }
  return rows;
}
