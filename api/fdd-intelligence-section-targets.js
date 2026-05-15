/**
 * Jurisdiction-aware extraction targets for FDD Intelligence (US FTC + Mexico / LATAM Roman headings).
 * Term-specific fee logic stays in fdd-intelligence.js; this module is section labels + target inference only.
 */

export const EXTRACTION_TARGET_IDS = [
  "initial_fees",
  "other_fees",
  "estimated_initial_investment",
  "systems_training_assistance",
  "territory",
  "restrictions_suppliers",
  "franchisee_obligations",
  "renewal_transfer_termination",
  "financial_performance",
  "system_health",
  "general_terms",
];

/** US FTC Item number → internal target (catalog sections). */
export const US_ITEM_NUMBER_TO_EXTRACTION_TARGET = {
  "5": "initial_fees",
  "6": "other_fees",
  "7": "estimated_initial_investment",
  "8": "restrictions_suppliers",
  "9": "franchisee_obligations",
  "10": "general_terms",
  "11": "systems_training_assistance",
  "12": "territory",
  "17": "renewal_transfer_termination",
  "19": "financial_performance",
  "20": "system_health",
  EX: "general_terms",
};

export const FEE_EXTRACTION_TARGET_SET = new Set([
  "initial_fees",
  "other_fees",
  "estimated_initial_investment",
  "systems_training_assistance",
  "restrictions_suppliers",
  "renewal_transfer_termination",
]);

export const TERM_EXTRACTION_TARGET_SET = new Set([
  "territory",
  "restrictions_suppliers",
  "franchisee_obligations",
  "systems_training_assistance",
  "renewal_transfer_termination",
  "financial_performance",
  "system_health",
  "general_terms",
]);

const ROMAN_ORDERED = [
  "XIX",
  "XVIII",
  "XVII",
  "XVI",
  "XV",
  "XIV",
  "XIII",
  "XII",
  "XI",
  "X",
  "IX",
  "VIII",
  "VII",
  "VI",
  "V",
  "IV",
  "III",
  "II",
  "I",
];

const ROMAN_LINE_RE = new RegExp(
  `^\\s*(${ROMAN_ORDERED.join("|")})\\s*([.):\\-–]?)\\s*(.*)$`,
  "i"
);

function isTocLikeRomanLine(line) {
  const L = String(line || "").trim();
  if (!L) return true;
  if (/\.{4,}/.test(L)) return true;
  if (/\.{2,}\s*\d{1,4}\s*$/i.test(L)) return true;
  return false;
}

/**
 * Roman numeral major headings (Mexico / LATAM style), line-based.
 * Supports "V. TITLE" and "V." + title on following lines (up to 5 lines).
 * @returns {Array<{ index: number, label: string, heading: string, rawLine: string, candidateSourceType: string }>}
 */
export function collectRomanNumeralHeaderMatches(text) {
  const t = String(text || "").replace(/\r\n/g, "\n");
  const lines = t.split("\n");
  let offset = 0;
  const out = [];
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    const m = line.match(ROMAN_LINE_RE);
    if (!m) {
      offset += line.length + 1;
      continue;
    }
    const label = String(m[1] || "").toUpperCase();
    const sep = String(m[2] || "").trim();
    let heading = String(m[3] || "").trim();
    let candidateSourceType = "roman-heading-single-line";

    if (!heading && (sep === "." || sep === ")" || sep === ":" || sep === "-" || sep === "–" || sep === "")) {
      const parts = [];
      for (let k = i + 1; k < Math.min(i + 8, lines.length); k++) {
        const L = lines[k].trim();
        if (!L) break;
        if (ROMAN_LINE_RE.test(lines[k])) break;
        parts.push(L);
        if (parts.length >= 5) break;
      }
      heading = parts.join(" ");
      candidateSourceType = "roman-heading-multi-line";
    }

    const probe = `${line} ${heading}`.trim();
    if (isTocLikeRomanLine(probe)) {
      offset += line.length + 1;
      continue;
    }

    out.push({
      index: offset,
      label,
      romanLabel: label,
      heading: heading || line.trim(),
      rawLine: line.trim(),
      candidateSourceType,
    });
    offset += line.length + 1;
  }
  return out;
}

function scoreTarget(hay, rules) {
  let best = "general_terms";
  let bestScore = 0;
  for (const { target, weight, tests } of rules) {
    let s = 0;
    for (const x of tests) {
      if (x instanceof RegExp) {
        if (x.test(hay)) s += weight;
      } else if (typeof x === "string" && hay.includes(x)) {
        s += weight;
      }
    }
    if (s > bestScore) {
      bestScore = s;
      best = target;
    }
  }
  return bestScore > 0 ? best : "general_terms";
}

const HAY_RULES = [
  {
    target: "other_fees",
    weight: 22,
    tests: [/\bITEM\s+6\b/i, /\bOTHER\s+FEES\b/i, /\bOTRAS\s+CUOTAS\b/i, /\bCUOTAS\s+DISTINTAS\b/i],
  },
  {
    target: "initial_fees",
    weight: 20,
    tests: [
      /\bITEM\s+5\b/i,
      /\bINITIAL\s+FEES\b/i,
      /\bAPPLICATION\s+FEES\b/i,
      /\bAMOUNTS?\s+AND\s+TYPES?\s+OF\s+PAYMENT\b/i,
      /\bPAYMENTS?\s+TO\s+FRANCHISOR\b/i,
      /\bFEES?\s+PAYABLE\s+TO\s+FRANCHISOR\b/i,
      /\bCONTRAPRESTACIONES\b/i,
      /\bPAGOS?\s+AL\s+FRANQUICIANTE\b/i,
      /\bCUOTAS?\s+INICIALES\b/i,
      /\bFRANCHISE\s+APPLICATION\s+FEE\b/i,
      /\bAMOUNT\s+AND\s+TYPE\s+OF\s+PAYMENT\b/i,
    ],
  },
  {
    target: "estimated_initial_investment",
    weight: 20,
    tests: [
      /\bITEM\s+7\b/i,
      /\bESTIMATED\s+INITIAL\s+INVESTMENT\b/i,
      /\bESTIMATE\s+OF\s+COSTS\s+TO\s+ESTABLISH\b/i,
      /\bINITIAL\s+INVESTMENT\b/i,
      /\bESTIMATED\s+COSTS\b/i,
      /\bINVERSI[ÓO]N\s+INICIAL\b/i,
      /\bCOSTOS\s+ESTIMADOS\b/i,
      /\bCOSTOS\s+PARA\s+ESTABLECER\b/i,
    ],
  },
  {
    target: "financial_performance",
    weight: 20,
    tests: [/\bITEM\s+19\b/i, /\bFINANCIAL\s+PERFORMANCE\s+REPRESENTATIONS?\b/i],
  },
  {
    target: "system_health",
    weight: 20,
    tests: [/\bITEM\s+20\b/i, /\bOUTLETS?\b.*\bFRANCHISEE\s+INFORMATION\b/i, /\bSYSTEM\s+HEALTH\b/i],
  },
  {
    target: "restrictions_suppliers",
    weight: 18,
    tests: [/\bITEM\s+8\b/i, /\bRESTRICTIONS?\s+ON\s+SOURCES\b/i, /\bAPPROVED\s+SUPPLIERS?\b/i, /\bPROCUREMENT\b/i],
  },
  {
    target: "franchisee_obligations",
    weight: 18,
    tests: [/\bITEM\s+9\b/i, /\bFRANCHISEE['']?S\s+OBLIGATIONS\b/i, /\bOBLIGATIONS?\s+OF\s+THE\s+FRANCHISEE\b/i],
  },
  {
    target: "systems_training_assistance",
    weight: 16,
    tests: [
      /\bITEM\s+11\b/i,
      /\bFRANCHISOR['']?S\s+ASSISTANCE\b/i,
      /\bCOMPUTER\s+SYSTEMS?\b/i,
      /\bTRAINING\b/i,
      /\bDIGITAL\s+FLOOR\s+PLAN\b/i,
      /\bDIGITAL\s+KEY\s+SYSTEM\b/i,
      /\bOPENING\s+TRANSITION\b/i,
      /\bONQ\b/i,
      /\bREQUIRED\s+SYSTEMS?\b/i,
      /\bSISTEMAS?\b/i,
      /\bCAPACITACI[ÓO]N\b/i,
      /\bASISTENCIA\b/i,
    ],
  },
  {
    target: "territory",
    weight: 18,
    tests: [
      /\bITEM\s+12\b/i,
      /\bTERRITORY\b/i,
      /\bEXCLUSIVITY\b/i,
      /\bPROTECTED\s+AREA\b/i,
      /\bTERRITORIO\b/i,
      /\bEXCLUSIVIDAD\b/i,
      /\b[ÁA]REA\s+PROTEGIDA\b/i,
    ],
  },
  {
    target: "renewal_transfer_termination",
    weight: 18,
    tests: [
      /\bITEM\s+17\b/i,
      /\bRENEWAL\b.*\bTERMINATION\b/i,
      /\bTRANSFER\b/i,
      /\bCHANGE\s+OF\s+OWNERSHIP\b/i,
      /\bRE[-\s]?LICENSING\b/i,
      /\bTERMINACI[ÓO]N\b/i,
      /\bRENOVACI[ÓO]N\b/i,
      /\bTRANSFERENCIA\b/i,
      /\bCESI[ÓO]N\b/i,
      /\bCAMBIO\s+DE\s+CONTROL\b/i,
      /\bCAMBIO\s+DE\s+PROPIEDAD\b/i,
    ],
  },
];

/**
 * Map heading + first 1000 chars of body to a single extraction target.
 * @param {string} heading
 * @param {string} bodyPrefix
 */
export function inferExtractionTargetFromSection(heading, bodyPrefix) {
  const h = `${String(heading || "").trim()}\n${String(bodyPrefix || "").slice(0, 1000)}`;
  const hay = h.toUpperCase();
  return scoreTarget(hay, HAY_RULES);
}

/**
 * Merge ITEM and Roman markers by document index; ITEM wins at same/near index.
 * @param {Array<{ index: number, itemNumber: string, rawLine?: string, matchedTitleLine?: string }>} itemMatches
 * @param {Array<{ index: number, label: string, heading: string, rawLine: string }>} romanMatches
 */
export function mergeBoundaryMarkers(itemMatches, romanMatches) {
  const marks = [];
  for (const m of itemMatches || []) {
    marks.push({
      index: m.index,
      priority: 0,
      kind: "item",
      itemNumber: String(m.itemNumber),
      rawLine: m.rawLine || "",
      matchedTitleLine: m.matchedTitleLine || null,
    });
  }
  for (const r of romanMatches || []) {
    marks.push({
      index: r.index,
      priority: 1,
      kind: "roman",
      romanLabel: r.romanLabel || r.label,
      heading: r.heading,
      rawLine: r.rawLine,
    });
  }
  marks.sort((a, b) => a.index - b.index || a.priority - b.priority);
  const dedup = [];
  for (const m of marks) {
    if (!dedup.length) {
      dedup.push(m);
      continue;
    }
    const prev = dedup[dedup.length - 1];
    if (m.index === prev.index) {
      if (m.priority < prev.priority) dedup[dedup.length - 1] = m;
      continue;
    }
    if (Math.abs(m.index - prev.index) < 4 && m.kind === "item" && prev.kind === "roman") {
      dedup[dedup.length - 1] = m;
      continue;
    }
    if (Math.abs(m.index - prev.index) < 4 && m.kind === "roman" && prev.kind === "item") {
      continue;
    }
    dedup.push(m);
  }
  return dedup;
}

export function formatSectionPromptLabel(sec) {
  if (!sec) return "";
  const lab = String(sec.sourceSectionLabel || sec.itemNumber || "").trim();
  const h = String(sec.sourceSectionHeading || sec.itemTitle || "").trim();
  if (sec.sourceFormat === "us_fdd_item" && /^\d{1,2}$/.test(lab)) return `Item ${lab}: ${h}`.slice(0, 400);
  if (lab && h) return `${lab} · ${h}`.slice(0, 400);
  return (lab || h || "Section").slice(0, 400);
}

export function documentationReferenceFromSection(sec) {
  const lab = String(sec.sourceSectionLabel || sec.itemNumber || "").trim();
  const h = String(sec.sourceSectionHeading || sec.itemTitle || "").trim();
  if (lab && h) return `${lab} - ${h}`.slice(0, 500);
  return lab || h || "Unclear";
}
