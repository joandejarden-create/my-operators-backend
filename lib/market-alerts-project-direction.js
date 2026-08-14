/**
 * Deterministic Project Direction for early-signal Market Alerts (internal).
 * Values: Advancing | Under Review | Challenged | Delayed | Rejected / Blocked
 */

export const PROJECT_DIRECTION_VALUES = [
  "Advancing",
  "Under Review",
  "Challenged",
  "Delayed",
  "Rejected / Blocked",
];

const APPEAL_REJECTED_RE =
  /\brejects? (?:an |the )?(?:appeal|objection)s?\b|\bappeal (?:was |is )?(?:rejected|denied|dismissed)\b/i;

const PROJECT_REJECTED_RE =
  /\b(?:planning|permit|zoning|application|proposal|project) (?:was |were )?(?:rejected|denied|refused|cancelled|canceled|abandoned)|(?:rejected|denied|refused) (?:the )?(?:planning|permit|zoning|application|proposal|hotel)|project (?:cancelled|canceled|abandoned|killed)\b/i;

const CHALLENGED_RE =
  /\b(lawsuit|sues?\b|sued\b|suo moto|environmental (?:challenge|case|scrutiny|hurdle)|regulator raises|council objections?|planning opposition|appeal filed|ngt\b|crz hurdle|faces? (?:a )?(?:legal|environmental) )\b/i;

const DELAYED_RE =
  /\b(project delayed|project postponed|approval postponed|construction postponed|financing delayed|hearing deferred|delayed indefinitely|postponed indefinitely)\b/i;

const UNDER_REVIEW_RE =
  /\b(public hearing|environmental (?:review|assessment|worksheet)|council reviewing|application pending|under review|signals? support|recommends? approval|many questions regarding)\b/i;

const ADVANCING_RE =
  /\b(planning approved|zoning approved|entitlement(?:s)? (?:approved|secured)|financing secured|construction (?:loan|financing)|credit approval|site acquired|breaks? ground|groundbreaking|moves? forward|development application approved|conditional-use approval|site plan approval|city council approves)\b/i;

const SUBMITTED_RE =
  /\b(application submitted|plans? submitted|planning (?:application|filed)|filed (?:a )?(?:planning|zoning))\b/i;

/**
 * @param {{ eventType?: string|null, title?: string, summary?: string }} input
 * @returns {string|null}
 */
export function inferProjectDirection(input = {}) {
  const eventType = input.eventType || null;
  const text = `${input.title || ""} ${input.summary || ""}`.trim();
  if (!text) return null;

  if (APPEAL_REJECTED_RE.test(text) && /\b(hotel|resort|proposed)\b/i.test(text)) {
    if (/\b(clears? the way|paving the way|allowing the (?:hotel|project)|project (?:can|may) proceed)\b/i.test(text)) {
      return "Advancing";
    }
    return "Under Review";
  }

  if (PROJECT_REJECTED_RE.test(text) && !APPEAL_REJECTED_RE.test(text)) {
    return "Rejected / Blocked";
  }

  if (CHALLENGED_RE.test(text)) return "Challenged";
  if (DELAYED_RE.test(text)) return "Delayed";

  if (UNDER_REVIEW_RE.test(text) && !/\b(approved|approval granted)\b/i.test(text)) {
    return "Under Review";
  }

  if (
    ADVANCING_RE.test(text) ||
    ["Planning Approval", "Construction Start", "Site Acquisition", "Financing"].includes(eventType)
  ) {
    return "Advancing";
  }

  if (SUBMITTED_RE.test(text) || eventType === "Planning Application") {
    return "Under Review";
  }

  if (["Development Proposal", "Adaptive Reuse Proposal", "New Development"].includes(eventType)) {
    return "Under Review";
  }

  return null;
}
