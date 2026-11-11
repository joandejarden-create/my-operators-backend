/**
 * GDI 2.0 Phase 2G — shared hotel-commercial responsibility classifier.
 *
 * Used by buyer-evidence-gate, shared eligibility alignment, and discovery strategies.
 * Generic leadership / probable relevance alone is NOT hotel-commercial.
 */

export const HOTEL_COMMERCIAL_RESPONSIBILITY_V1 = "gdi-hotel-commercial-responsibility-v1";

export const REMIT_CLASS = Object.freeze({
  HOTEL_COMMERCIAL_VERIFIED: "HOTEL_COMMERCIAL_VERIFIED",
  DEMAND_FUNCTION_RELEVANT: "DEMAND_FUNCTION_RELEVANT",
  LEADERSHIP_ONLY: "LEADERSHIP_ONLY",
  NON_HOTEL_COMMERCIAL: "NON_HOTEL_COMMERCIAL",
  ROLE_UNKNOWN: "ROLE_UNKNOWN",
  ROLE_SPAM: "ROLE_SPAM",
  NOT_A_PERSON: "NOT_A_PERSON",
});

const HOTEL_COMMERCIAL_ROLE_RE =
  /\b(meeting|meetings|event|events|conference|congress|convention|housing|hotel|venue|planner|planning|facilities|workplace|real estate|travel|procurement|sourcing|operations manager|office manager|administrative|executive assistant|gerente|directora de eventos|meetings director|event director|hospedaje|alojamiento|congreso)\b/i;

const DEMAND_FUNCTION_RE =
  /\b(training|learning|development|l&d|onboarding|deployment|project (?:operations |ops )?manager|program manager|implementation|site lead|branch manager|office lead|chief of staff|people ops|human resources|hr\b|talent|project operations)\b/i;

const JOB_TITLE_SHAPE_RE =
  /\b(director|manager|coordinator|planner|officer|head|lead|vp|vice president|gerente|directora|responsable|president|ceo|coo|chief)\b/i;

const NON_HOTEL_COMMERCIAL_RE =
  /\b(content strategy|content marketing|product\b|engineering|software|sales engineer|account executive\b|customer success|recruiter|recruiting|talent acquisition|people partner|hr business|media|press|pr\b|investor relations|speaker|keynote|panelist|communications)\b/i;

const LEADERSHIP_ONLY_RE =
  /\b(director|manager|vp|vice president|head of|lead|partner|principal|founder|ceo|coo|cfo|chro|cso|cto|cmo|chief\b|officer\b|presidente|gerente)\b/i;

const ROLE_SPAM_RE =
  /\b(contactan|regularmen|cookies|utilizamos|directory contact|expo guide|international fairs|fair,?\s*expo|página|informaci[oó]n|click here|read more)\b/i;

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

export function cleanBuyerRoleTitle(raw) {
  let t = text(raw);
  t = t.replace(/\s+We\s+(Inspire|Have Fun|Are|Believe|Value)\b[\s\S]*$/i, "");
  t = t.replace(/\s+IF\s+work\b[\s\S]*$/i, "");
  if (t.length > 80) t = t.slice(0, 80).trim();
  return t;
}

/**
 * Classify whether a role evidences hotel-buying / lodging-commercial remit.
 */
export function classifyHotelCommercialRemit(roleRaw = "", opts = {}) {
  const role = cleanBuyerRoleTitle(roleRaw);
  if (!role) {
    return {
      version: HOTEL_COMMERCIAL_RESPONSIBILITY_V1,
      class: REMIT_CLASS.ROLE_UNKNOWN,
      ok: false,
      hotelCommercial: false,
      reason: "role_unknown",
      role: null,
    };
  }
  if (ROLE_SPAM_RE.test(role) || role.length > 80) {
    return {
      version: HOTEL_COMMERCIAL_RESPONSIBILITY_V1,
      class: REMIT_CLASS.ROLE_SPAM,
      ok: false,
      hotelCommercial: false,
      reason: "role_looks_like_page_copy",
      role,
    };
  }
  if (NON_HOTEL_COMMERCIAL_RE.test(role) && !HOTEL_COMMERCIAL_ROLE_RE.test(role)) {
    return {
      version: HOTEL_COMMERCIAL_RESPONSIBILITY_V1,
      class: REMIT_CLASS.NON_HOTEL_COMMERCIAL,
      ok: false,
      hotelCommercial: false,
      reason: "role_not_hotel_commercially_relevant",
      role,
    };
  }
  if (HOTEL_COMMERCIAL_ROLE_RE.test(role)) {
    const strongLodgingRemit =
      /\b(housing|lodging|hospedaje|alojamiento|hotel|meetings?|events?|procurement|travel|venue|planner|planning)\b/i.test(
        role,
      );
    if (
      !JOB_TITLE_SHAPE_RE.test(role) &&
      opts.requireJobTitleShape !== false &&
      !strongLodgingRemit
    ) {
      return {
        version: HOTEL_COMMERCIAL_RESPONSIBILITY_V1,
        class: REMIT_CLASS.ROLE_SPAM,
        ok: false,
        hotelCommercial: false,
        reason: "event_keyword_without_buyer_job_title_shape",
        role,
      };
    }
    return {
      version: HOTEL_COMMERCIAL_RESPONSIBILITY_V1,
      class: REMIT_CLASS.HOTEL_COMMERCIAL_VERIFIED,
      ok: true,
      hotelCommercial: true,
      reason: strongLodgingRemit && !JOB_TITLE_SHAPE_RE.test(role)
        ? "hotel_commercial_remit_label_evidenced"
        : "hotel_commercial_role_evidenced",
      role,
    };
  }
  if (DEMAND_FUNCTION_RE.test(role) && JOB_TITLE_SHAPE_RE.test(role)) {
    return {
      version: HOTEL_COMMERCIAL_RESPONSIBILITY_V1,
      class: REMIT_CLASS.DEMAND_FUNCTION_RELEVANT,
      ok: true,
      hotelCommercial: true,
      reason: "demand_function_role_with_title_shape",
      role,
      note: "Training/L&D/project/ops roles may drive hotel demand; still require demand evidence separately.",
    };
  }
  if (LEADERSHIP_ONLY_RE.test(role)) {
    return {
      version: HOTEL_COMMERCIAL_RESPONSIBILITY_V1,
      class: REMIT_CLASS.LEADERSHIP_ONLY,
      ok: false,
      hotelCommercial: false,
      reason: "leadership_without_hotel_commercial_remit",
      role,
    };
  }
  return {
    version: HOTEL_COMMERCIAL_RESPONSIBILITY_V1,
    class: REMIT_CLASS.ROLE_UNKNOWN,
    ok: false,
    hotelCommercial: false,
    reason: "role_not_commercially_justified",
    role,
  };
}

/**
 * Probable/potential relevance labels never independently verify remit.
 */
export function probableRelevanceInsufficient(labels = "") {
  return /POTENTIALLY_RELEVANT|PROBABLE_RELEVANT|POTENTIAL_DECISION|INFLUENCER/i.test(
    String(labels || ""),
  );
}
