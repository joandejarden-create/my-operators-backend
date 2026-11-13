/**
 * GDI Customer-Facing Opportunity Title Contract V1
 *
 * Permanent, centralized naming standard for every customer-visible GDI title
 * (tiles, lists, drawers, CSV, share). Idempotent — repeated application cannot
 * reintroduce stripped research suffixes.
 *
 * Keep:
 *   - Official event / programme / commercial activity name
 *   - Relevant event year when part of the name
 *   - Organization / brand clarity when it is the activity name itself
 *
 * Strip when appended as research qualifiers (em-dash / paren tails):
 *   - Date ranges and month-day fragments
 *   - City / state / hotel locations
 *   - Lodging / guestrooms / catering research descriptors
 *   - Early Watch / Monitor / Research Continuation / internal status
 *   - Confidence, competitor, and sales-action commentary
 *
 * Preserve words like Bethesda / Lodging / Training / month names when they are
 * genuinely part of the official activity name (left of the qualifier).
 *
 * Classification: REUSABLE_PRODUCT_CONTRACT
 */

export const GDI_CUSTOMER_FACING_OPPORTUNITY_TITLE_CONTRACT_V1 =
  "gdi-customer-facing-opportunity-title-contract-v1";

const MONTH =
  "(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)";

/** Trailing parenthetical research / status qualifiers. */
const PAREN_STATUS_RE = new RegExp(
  String.raw`\s*\(\s*(?:` +
    [
      "Early Watch",
      "Future Watch",
      "Overflow Watch",
      "Partnership Watch",
      "Monitor(?:ing)?",
      "Research Continuation",
      "Research",
      "Watch",
      "TBD",
      "destination TBD",
      "location TBD",
      "venue TBD",
      "hotel\\/venue not named",
      "hotel not yet named",
      "hotel not specified",
      "hotel not named",
      "room block coming soon",
      "housing pending",
      "Hotel & Travel open",
      "needs confirmation",
      "likely convention-scale[^)]*",
      "convention-scale(?:;[^)]*)?",
      "convention-scale overflow watch",
      "medical overflow watch",
      "overflow lodging potential",
      "primary host unfit",
    ].join("|") +
    String.raw`)\s*\)\s*$`,
  "i"
);

/**
 * Em-dash / en-dash / spaced-hyphen tails that are research qualifiers —
 * never part of the official activity name.
 */
const DASH_SUFFIX_PATTERNS = [
  // Date ranges: Sep 11–12 · Sep 11-12, 2027 · September 11–12
  new RegExp(
    String.raw`^(?:${MONTH})\.?\s+\d{1,2}(?:\s*[–—-]\s*\d{1,2})?(?:,?\s*20\d{2})?$`,
    "i"
  ),
  // Month + year (+ optional lodging / research words)
  new RegExp(
    String.raw`^(?:${MONTH})\.?\s+20\d{2}(?:\s+(?:lodging(?:\s+research)?|guestrooms?|catering|housing|research))?$`,
    "i"
  ),
  // Pure lodging / rooms / catering research descriptors
  /^(?:lodging(?:\s+research)?|guest\s*rooms?|room\s*block|catering(?:\s+research)?|housing(?:\s+research)?|overflow lodging(?:\s+research)?)$/i,
  // City, ST / City, State / City area (+ optional status paren already stripped)
  /^(?:[A-ZÁÉÍÓÚÑ][\w.'’ -]+),\s*(?:[A-Z]{2}|D\.?C\.?|[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)(?:\s+area)?(?:\s*\([^)]*\))?$/,
  // Washington DC / Ciudad de … / Greater|Metro labels (location-only tails)
  /^(?:Washington(?:\s*,?\s*D\.?C\.?)?|DMV)$/i,
  /^(?:Ciudad de|City of)\s+[\w.'’áéíóúñÁÉÍÓÚÑ -]+$/i,
  /^(?:Greater|Metro)\s+[\w.'’áéíóúñÁÉÍÓÚÑ -]+$/i,
  // Internal research / watch / continuation labels
  /^(?:Early Watch|Future Watch|Overflow Watch|Partnership Watch|Monitor(?:ing)?|Research Continuation|Local meetings research continuation|Civic meetings research continuation|Community meetings research continuation|Corporate meetings research continuation|Research|Watch|TBD|destination TBD|location TBD|venue TBD|housing pending|Hotel & Travel open|needs confirmation|already contracted at\b.*|monitoring for\b.*)$/i,
  // Hotel / venue not named + location hybrids
  /^(?:[\w.\s,'-]+)\s*\((?:hotel\/venue not named|destination TBD|location TBD|venue TBD|housing pending|overflow watch|convention-scale[^)]*|medical overflow watch|Early Watch|Future Watch)\)\s*$/i,
  // Competitor / confidence / sales-action commentary tails
  /^(?:competitor(?:\s+capture|\s+room\s+block)?|competitive(?:\s+intelligence)?|confidence\s+\d{1,3}%?|pursue now|contact buyer|build account|win room block|qualify opportunity|recommended action\b.*)$/i,
  // Demand-thesis / research-theme tails (not the official activity name)
  /^(?:scaling(?:\s+corporate)?(?:\s+account)?|preferred lodging partnership|stay[- ]?to[- ]?play(?:\s+weekend)?(?:\s+demand)?|stay[- ]?to[- ]?play(?:\s+housing)?(?:\s*\([^)]*\))?|multi[- ]?weekend\b.*demand|participant housing|overflow(?:\s+housing|\s+lodging|\s+play)?(?:\s+demand)?|housing demand|room block demand|weekend demand|research theme|corporate\s*\/\s*training demand\b.*|hotel\b.*(?:unconfirmed|sourcing|not named|block)\b.*|housing\b.*incomplete\b.*)$/i,
];

const ACRONYM_PRESERVE = Object.freeze({
  amwa: "AMWA",
  asae: "ASAE",
  pda: "PDA",
  tri: "TRI",
  nih: "NIH",
  nyc: "NYC",
  dmc: "DMC",
  rfp: "RFP",
  tbd: "TBD",
  mpi: "MPI",
  pco: "PCO",
  dc: "DC",
  plus: "PLUS",
  ife: "IFE",
  us: "U.S.",
  "u.s.": "U.S.",
  "u.s": "U.S.",
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function splitDashTail(title) {
  const raw = text(title);
  if (!raw) return { head: "", tail: "" };
  const m = raw.match(/^(.*?)(?:\s+[—–]\s+|\s+-\s+)(.+)$/);
  if (!m) return { head: raw, tail: "" };
  return { head: text(m[1]), tail: text(m[2]) };
}

function isRemovableDashSuffix(tail) {
  const t = text(tail);
  if (!t) return false;
  return DASH_SUFFIX_PATTERNS.some((re) => re.test(t));
}

function stripTrailingStatusParens(title) {
  let t = text(title);
  let guard = 0;
  while (guard < 6 && PAREN_STATUS_RE.test(t)) {
    t = t.replace(PAREN_STATUS_RE, "").trim();
    guard += 1;
  }
  return t;
}

function stripRemovableDashSuffixes(title) {
  let t = text(title);
  let guard = 0;
  while (guard < 8) {
    const { head, tail } = splitDashTail(t);
    if (!tail || !head) break;
    if (!isRemovableDashSuffix(tail)) break;
    t = head;
    guard += 1;
  }
  return t;
}

/**
 * Professional Title Case for customer display.
 * Preserves known acronyms and CamelCase / mid-cap brand tokens.
 */
export function applyCustomerFacingTitleCase(raw = "") {
  const s = text(raw);
  if (!s) return s;
  // Keep connective particles lowercase mid-title. Do NOT include lone "a"/"an"
  // — that mangles Spanish/place articles ("A Coruña") and brand fragments.
  const small =
    /^(?:and|or|of|the|for|to|in|on|at|&|de|del|la|las|los|y|e|do|da|dos|das|pola)$/i;
  return s
    .split(/(\s+|[-–—|:/,&()]+)/)
    .map((tok, idx) => {
      if (!/[A-Za-zÁÉÍÓÚÑáéíóúñ]/.test(tok)) return tok;
      const lower = tok.toLowerCase();
      if (ACRONYM_PRESERVE[lower]) return ACRONYM_PRESERVE[lower];
      // Preserve short/medium ALL-CAPS brands and acronyms (ADAPTATUR, NIH, ASAE).
      if (/^[A-ZÁÉÍÓÚÑ]{2,14}$/.test(tok)) return tok;
      if (/^[A-Z0-9]{2,14}$/.test(tok) && /[0-9]/.test(tok)) return tok;
      // Preserve CamelCase / brand tokens (BioCultura, iPhone).
      if (/[a-záéíóúñ]/.test(tok) && /[A-ZÁÉÍÓÚÑ]/.test(tok.slice(1))) return tok;
      if (small.test(tok) && idx !== 0) return lower;
      return tok.charAt(0).toUpperCase() + tok.slice(1).toLowerCase();
    })
    .join("");
}

/**
 * Core normalizer — idempotent.
 * @returns {{ displayTitle: string|null, originalSourceTitle: string|null, changed: boolean, strippedSuffixes: string[] }}
 */
export function normalizeCustomerFacingOpportunityTitle(raw, opts = {}) {
  const originalSourceTitle =
    text(opts.originalSourceTitle) || text(raw) || null;
  let t = text(raw);
  if (!t) {
    return {
      displayTitle: text(opts.fallback) || null,
      originalSourceTitle,
      changed: false,
      strippedSuffixes: [],
    };
  }

  const strippedSuffixes = [];
  let guard = 0;
  while (guard < 10) {
    guard += 1;
    const before = t;
    t = stripTrailingStatusParens(t);
    const { head, tail } = splitDashTail(t);
    if (tail && head && isRemovableDashSuffix(tail)) {
      strippedSuffixes.push(tail);
      t = head;
    } else {
      t = stripRemovableDashSuffixes(t);
    }
    t = text(t);
    if (t === before) break;
  }

  // Light cleanup of leftover " — " / double spaces
  t = t.replace(/\s*[—–]\s*$/g, "").replace(/\s{2,}/g, " ").trim();

  if (!t && opts.fallback) t = text(opts.fallback);
  if (!t) {
    return {
      displayTitle: null,
      originalSourceTitle,
      changed: Boolean(originalSourceTitle),
      strippedSuffixes,
    };
  }

  const beforeCase = t;
  if (opts.applyTitleCase !== false) {
    t = applyCustomerFacingTitleCase(t);
  }

  const displayTitle = t;
  // Structural change = stripped research suffixes. Pure cosmetic case folds of
  // already-clean titles are still normalized for display but not "corrections".
  const structurallyChanged =
    strippedSuffixes.length > 0 ||
    text(originalSourceTitle).replace(/\s+/g, " ") !== beforeCase;
  return {
    displayTitle,
    originalSourceTitle,
    changed: structurallyChanged || text(originalSourceTitle) !== displayTitle,
    structurallyChanged,
    strippedSuffixes,
  };
}

/** Convenience: return display string only. */
export function customerFacingOpportunityTitle(raw, opts = {}) {
  return normalizeCustomerFacingOpportunityTitle(raw, opts).displayTitle;
}

/**
 * Apply contract onto opportunity presentation fields without mutating
 * identity ids, qualification, contacts, or commercial theses.
 * Preserves unmodified source title on researchTitlePreserved / originalSourceTitle.
 */
export function applyCustomerFacingOpportunityTitleContract(opportunity = {}) {
  const o =
    opportunity && typeof opportunity === "object" ? { ...opportunity } : {};
  const prior =
    text(o.originalSourceTitle) ||
    text(o.researchTitlePreserved) ||
    text(o.title) ||
    text(o.displayTitle) ||
    text(o.drawerHeaderTitle) ||
    null;
  const candidate =
    text(o.displayTitle) ||
    text(o.drawerHeaderTitle) ||
    text(o.canonicalEventDisplayTitle) ||
    text(o.eventDisplayName) ||
    text(o.eventName) ||
    text(o.title) ||
    "";
  const { displayTitle, originalSourceTitle, changed } =
    normalizeCustomerFacingOpportunityTitle(candidate, {
      originalSourceTitle: prior,
      fallback: text(o.organizationName) || text(o.displayCompanyName) || null,
    });
  if (!displayTitle) return o;
  o.displayTitle = displayTitle;
  o.drawerHeaderTitle = displayTitle;
  if (prior && prior !== displayTitle) {
    o.researchTitlePreserved = o.researchTitlePreserved || prior;
    o.originalSourceTitle = o.originalSourceTitle || prior;
  } else if (originalSourceTitle) {
    o.originalSourceTitle = o.originalSourceTitle || originalSourceTitle;
  }
  if (changed) {
    o.customerFacingTitleContractV1 = {
      version: GDI_CUSTOMER_FACING_OPPORTUNITY_TITLE_CONTRACT_V1,
      applied: true,
    };
  }
  if (o.opportunityIdentity && typeof o.opportunityIdentity === "object") {
    o.opportunityIdentity = {
      ...o.opportunityIdentity,
      displayTitle,
      opportunityOverviewIdentity: displayTitle,
      researchTitlePreserved:
        o.opportunityIdentity.researchTitlePreserved ||
        o.researchTitlePreserved ||
        prior,
    };
  }
  if (o.eventsOpportunityOverview && typeof o.eventsOpportunityOverview === "object") {
    o.eventsOpportunityOverview = {
      ...o.eventsOpportunityOverview,
      drawerHeaderTitle: displayTitle,
      identityLine: displayTitle,
    };
  }
  return o;
}

export function isCustomerFacingTitleContractIdempotent(raw) {
  const once = customerFacingOpportunityTitle(raw);
  const twice = customerFacingOpportunityTitle(once);
  return once === twice;
}
