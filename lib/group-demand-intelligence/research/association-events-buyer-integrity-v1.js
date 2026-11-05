/**
 * Association / Event named-buyer integrity V1.
 *
 * Rejects false “people” that previously passed looksLikePersonName:
 * geographic regions, awards, organizational bodies, speaker-only labels,
 * date/filename phone pollution, and mismatched email ownership.
 *
 * Does NOT add new publication gates — it strengthens verification of the
 * existing “≥1 named relevant buyer + usable route” requirement.
 */

import { looksLikePersonName } from "../secondary-named-contact-v1.js";
import { assessBuyerOrgAndAccessibility } from "./commercial-accessibility-and-org-route-v1.js";

export const ASSOCIATION_EVENTS_BUYER_INTEGRITY_V1 =
  "gdi-association-events-buyer-integrity-v1";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

/**
 * Association-Events buyer wipe / Contact→congress-route rewrite applies only
 * to Association / Conference / Events inventories. Segments 4/9/12 (and other
 * non-Events demand families) must preserve evidenced named people even when
 * hotel-sourcing authority is unverified — display ≠ buyer eligibility.
 */
export function isAssociationEventsBuyerIntegritySegment(opportunity = {}) {
  const blob = [
    opportunity.demandSegmentId,
    opportunity.customerDemandSegmentId,
    opportunity.researchSegment,
    opportunity.demandSegmentRouting?.customerDemandSegmentId,
    opportunity.opportunityType,
    opportunity.demandFamily,
  ]
    .map((v) => String(v || "").toLowerCase())
    .join(" ");

  if (
    /local_catering|government_public_sector|relocation_extended|corporate_accounts_scaling|scaling_compan|corporate_meetings|sports_entertainment|education_scientific|projects_infrastructure|social_special|corporate_celebrations|travel_trade/.test(
      blob,
    )
  ) {
    return false;
  }
  if (
    opportunity.commercialIntelligenceBrief?.semanticContract ||
    opportunity.commercialIntelligenceBrief?.scalingBoilerplateExcluded === true ||
    opportunity.segments4912LiveWaveV1 === true
  ) {
    return false;
  }
  if (opportunity.associationEventsGoldBrief) return true;
  if (
    /associations_conferences|association_events|associations_government|medical_academic|exhibitor_sponsor|event_to_demand|conference.?group|association.?conference/.test(
      blob,
    )
  ) {
    return true;
  }
  return false;
}

/** Phone-only scrub for non-Events segments — keep named display contacts. */
function scrubPhonesPreserveNamedPeople(opportunity = {}) {
  const next = { ...opportunity };
  const scrubPhoneKeys = (obj) => {
    if (!obj || typeof obj !== "object") return obj;
    const out = { ...obj };
    for (const key of ["phone", "personPhone", "directPhone", "companyPhone", "organizationPhone"]) {
      if (out[key]) out[key] = scrubAssociationEventsPhone(out[key]);
    }
    return out;
  };
  if (next.contactPhone) {
    const scrubbed = scrubAssociationEventsPhone(next.contactPhone);
    if (!scrubbed) {
      next._rejectedContactPhone = text(next.contactPhone);
      next.contactPhone = null;
    } else next.contactPhone = scrubbed;
  }
  if (next.companyPhone) {
    const scrubbed = scrubAssociationEventsPhone(next.companyPhone);
    if (!scrubbed) {
      next._rejectedCompanyPhone = text(next.companyPhone);
      next.companyPhone = null;
    } else next.companyPhone = scrubbed;
  }
  if (next.primaryContact && typeof next.primaryContact === "object") {
    next.primaryContact = scrubPhoneKeys(next.primaryContact);
  }
  if (next.secondaryContact && typeof next.secondaryContact === "object") {
    next.secondaryContact = scrubPhoneKeys(next.secondaryContact);
  }
  if (next.contactChannelSeparation && typeof next.contactChannelSeparation === "object") {
    next.contactChannelSeparation = scrubPhoneKeys(next.contactChannelSeparation);
  }
  next._associationEventsBuyerScrubSkipped =
    "non_association_events_segment_preserve_named_display_contacts";
  return next;
}

/** Geographic / region labels mistaken for people (e.g. "Asia Pacific"). */
const GEO_REGION_RE =
  /^(asia pacific|latin america|north america|south america|central america|middle east|europe|africa|caribbean|global|americas|apac|emea|latam|usa|united states|mexico city|ciudad de m[eé]xico|canc[uú]n|catalunya|catalonia|paris)$/i;

/** City / country trailing tokens that mark directory labels, not people. */
const GEO_IN_NAME_RE =
  /\b(mexico city|ciudad de m[eé]xico|canc[uú]n|guadalajara|monterrey|united states|latin america|asia pacific)\b/i;

/** Trailing role words glued onto scraped names. */
const TRAILING_ROLE_RE =
  /\b(presidente|presidenta|director|directora|gerente|secretario|secretaria|coordinator|coordinador)\s*$/i;

/** Organizational bodies / generic roles, not named persons. */
const ORG_BODY_RE =
  /\b(consejo directivo|board of directors|comit[eé] organizador|organizing committee|secretariat|secretar[ií]a general|convenors?|enlace empresarial|c[aá]mara|association|asociaci[oó]n|society|sociedad|federation|directorio|staff|team|office|departamento|department|cap[ií]tulo|congreso|congress|visita|business expos?|expos?|summit|cumbre|colegio|institute|instituto)\b/i;

/** Awards, recognitions, program labels. */
const AWARD_OR_LABEL_RE =
  /\b(reconocimiento|award|premio|alebrije|trophy|medal|certificat|design week|speaker|ponente|keynote|moderador)\b/i;

/** Phone fields polluted by ISO dates, years, or reference ids. */
const DATE_AS_PHONE_RE =
  /^(?:19|20)\d{2}[-/.]\d{1,2}(?:[-/.]\d{1,2})?$|^(?:19|20)\d{2}\s*[-–]\s*\d{1,2}$|^\d{4}-\d{2}-\d{2}/;

const FILENAME_AS_PHONE_RE = /\.(pdf|docx?|html?|aspx?)$/i;

/**
 * True when the string is a credible personal name for association/event buyers.
 */
/** First-token role / adjective pollution (e.g. "Resilience Horacio…", "Administrativo Carlomagno…"). */
const LEADING_ROLE_TOKEN_RE =
  /^(administrativo|resilience|design|speaker|research|dr|dra|prof|ing|lic|mvz|enlace|reconocimiento|director|gerente|presidente|secretari[oa]|programs?|events?)\b/i;

/** Trailing profession / specialty glued onto a personal name (e.g. "Guillermo Torre Médico"). */
const TRAILING_PROFESSION_RE =
  /\b(m[eé]dico|medico|abogado|abogada|ingeniero|arquitecto|contador|licenciado|doctor|doctora)\s*$/i;

/** Org / practice shells mistaken for people (e.g. "SAI Derecho"). */
const ORG_SHELL_NAME_RE =
  /\b(derecho|consultores?|associates?|partners?|group|grupo|firm|despacho|servicios?|solutions?)\b/i;

export function isAssociationEventsValidPersonName(name) {
  const n = text(name);
  if (!n) return false;
  if (!looksLikePersonName(n)) return false;
  if (GEO_REGION_RE.test(n)) return false;
  if (GEO_IN_NAME_RE.test(n)) return false;
  // Place-name leading tokens (e.g. "Catalunya Josep Martí")
  if (/^(catalunya|catalonia|paris|madrid|barcelona|mexico|méxico)\b/i.test(n)) return false;
  if (ORG_BODY_RE.test(n)) return false;
  if (AWARD_OR_LABEL_RE.test(n)) return false;
  if (LEADING_ROLE_TOKEN_RE.test(n)) return false;
  if (TRAILING_ROLE_RE.test(n)) return false;
  if (TRAILING_PROFESSION_RE.test(n)) return false;
  if (ORG_SHELL_NAME_RE.test(n) && n.split(/\s+/).length <= 3) return false;
  // Roman / ordinal chapter labels ("Capítulo VI", "Chapter 3")
  if (/^(cap[ií]tulo|chapter)\b/i.test(n)) return false;
  if (/\b(cap[ií]tulo|chapter)\s+(vi|vii|viii|ix|x|\d+)\b/i.test(n)) return false;
  // Org+event mashups with no personal given name shape
  if (/^(business|future|human|medical|longevity|premium|general|structured|electo|mpi|centro|universidade|meeting|santa)\b/i.test(n)) {
    return false;
  }
  if (/\b(business forum|citibanamex|seap-iap|cruz alta|business expos?)\b/i.test(n)) return false;
  if (/\bDr\.?$/i.test(n) && n.split(/\s+/).length <= 2) return false;
  if (/^(tesorera|tesorero|inicio|eventos|pontificia|universidad)\b/i.test(n)) return false;
  // Functional labels mistaken for people
  if (/^(needs analysis|venue sourcing|room block|housing desk|organizing committee|speaker bureau)\b/i.test(n)) {
    return false;
  }
  // Security / nav / cookie scrape labels (e.g. "Scam Alert", "Skip to main content")
  if (/^(scam alert|security alert|cookie (?:notice|policy)|skip to(?: main)? content|accept cookies|privacy policy)\b/i.test(n)) {
    return false;
  }
  if (/\b(scam alert|skip to main content)\b/i.test(n)) return false;
  if (/^(venue|sourcing|housing|analysis|committee|secretariat)\b/i.test(n) && n.split(/\s+/).length <= 3) {
    return false;
  }
  if (/\b(universidad|javeriana|inicio eventos)\b/i.test(n)) return false;
  // Truncated scrape fragments ending with hyphen
  if (/[-–—]\s*$/.test(n) || /\s-\s*$/.test(n)) return false;
  // Specialty + city directory labels (e.g. "Endocrinología Mexico City")
  if (/olog[ií]a\b/i.test(n) && GEO_IN_NAME_RE.test(n)) return false;
  // Chamber / acronym + city shells (e.g. "CANACO CDMX")
  if (/^(canaco|concanaco|coparmex|amexcap|anade|andellac|smne|amrob)\b/i.test(n)) {
    return false;
  }
  // Two-token all-caps institutional labels
  if (/^[A-ZÁÉÍÓÚÑ]{3,}(?:\s+[A-ZÁÉÍÓÚÑ]{3,})+$/.test(n) && !/\s[A-Z][a-záéíóúñ]/.test(n)) {
    // Allow "JUAN PEREZ" style all-caps people (first token length usually < 12)
    const tokens = n.split(/\s+/);
    if (tokens.every((t) => t.length >= 4) && tokens.some((t) => /^(ASIA|LATIN|NORTH|SOUTH|MIDDLE|CONSEJO|DIRECTIVO|AMERICA|PACIFIC)$/i.test(t))) {
      return false;
    }
  }
  return true;
}

/**
 * Validate / scrub a phone candidate. Returns null when polluted.
 */
export function scrubAssociationEventsPhone(phone) {
  const p = text(phone);
  if (!p) return null;
  if (DATE_AS_PHONE_RE.test(p)) return null;
  if (FILENAME_AS_PHONE_RE.test(p)) return null;
  // Must contain enough digits to be a phone
  const digits = p.replace(/\D/g, "");
  if (digits.length < 8 || digits.length > 15) return null;
  // Reject pure year-month fragments like 20250717
  if (/^(19|20)\d{6,}$/.test(digits) && digits.length <= 8) return null;
  return p;
}

/** PR / agency / media brands that are not event accommodation buyers. */
const UNRELATED_AGENCY_OR_MEDIA_RE =
  /\b(edelman|fleishman|weber\s*shandwick|ketchum|hill\??\s*knowlton|bcw|ogilvy|publicis|havas|mma\b|pr\s*newswire|business\s*wire)\b/i;

/** Scraped sentence fragments mistaken for job titles. */
const TITLE_EXCERPT_POLLUTION_RE =
  /\b(comparti[oó]|hallaz|destac[oó]|afirm[oó]|señal[oó]|explic[oó]|durante\s+su\s+participaci|en\s+el\s+marco)\b/i;

/**
 * Assess whether a contact name is a commercially relevant named buyer candidate.
 * Speakers alone are not buyers unless accompanied by organizer/sourcing role.
 * PR-agency executives named in press coverage of an event are not event buyers.
 */
export function assessAssociationEventsBuyerCandidate(
  person = {},
  { eventBlob = "", organizerName = "", organizationName = "" } = {},
) {
  const name = text(person.name || person.fullName);
  const title = text(person.title || person.role || "");
  const orgHint = text(person.organization || person.company || person.employer || "");
  const reasons = [];
  if (!isAssociationEventsValidPersonName(name)) {
    return {
      ok: false,
      name: name || null,
      title: title || null,
      reason: "invalid_person_identity",
    };
  }
  const blob = `${title} ${orgHint} ${eventBlob}`.toLowerCase();
  const organizer = text(organizerName || organizationName);

  if (TITLE_EXCERPT_POLLUTION_RE.test(title) || title.length > 90) {
    reasons.push("title_is_webpage_excerpt_not_role");
  }
  if (/\*\*|#|nuevo gerente|^ceo\s*-/i.test(title)) {
    reasons.push("title_is_webpage_excerpt_not_role");
  }
  if (
    /^(organizing committee|comit[eé] organizador|speaker bureau|needs analysis|venue sourcing|housing desk|staff|team|secretariat)$/i.test(
      title,
    )
  ) {
    reasons.push("functional_label_not_person_role");
  }
  // Malformed scrape: "Programs & Events Keydy Espinoza Manager" or dual-name mashups
  if (/\b(programs?\s*&\s*events?|keydy|research\s+)/i.test(title) || /\bManager\b.+\bManager\b/i.test(title)) {
    reasons.push("malformed_combined_name_role");
  }
  if (/^presidente\s+sai\b/i.test(title) || /\bsai\s+derecho\b/i.test(`${title} ${orgHint}`)) {
    reasons.push("org_entity_presented_as_person_role");
  }
  if (
    UNRELATED_AGENCY_OR_MEDIA_RE.test(`${title} ${orgHint}`) &&
    organizer &&
    !UNRELATED_AGENCY_OR_MEDIA_RE.test(organizer) &&
    !new RegExp(organizer.slice(0, 24).replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i").test(
      `${title} ${orgHint}`,
    )
  ) {
    reasons.push("unrelated_agency_or_media_contact");
  }
  if (/\b(speaker|ponente|keynote|panelista|moderador)\b/i.test(title) &&
      !/\b(director|organizer|organizador|gerente|presidente|executive|sourcing|meetings|events|comercial)\b/i.test(title)) {
    reasons.push("speaker_only_not_buyer");
  }
  if (/\b(media|prensa|periodista|reporter|journalist|author)\b/i.test(title)) {
    reasons.push("media_contact_not_buyer");
  }
  const roleOk =
    !reasons.length ||
    (reasons.length === 1 &&
      reasons[0] === "speaker_only_not_buyer" &&
      /\b(director|executive|gerente|presidente|secretario|secretaria|organizer|organiser|pco|procurement|sourcing|meetings|events|comercial|ceo|coo)\b/i.test(
        title,
      ));
  // Hard reject agency / excerpt / functional / malformed pollution even when title contains "Manager"
  if (
    reasons.includes("unrelated_agency_or_media_contact") ||
    reasons.includes("title_is_webpage_excerpt_not_role") ||
    reasons.includes("functional_label_not_person_role") ||
    reasons.includes("malformed_combined_name_role") ||
    reasons.includes("org_entity_presented_as_person_role")
  ) {
    return {
      ok: false,
      name,
      title: title || null,
      reason: reasons[0],
      notes: reasons,
      eventContextHint: blob.slice(0, 80) || null,
    };
  }
  return {
    ok: roleOk && reasons.length === 0,
    name,
    title: title || null,
    reason: roleOk ? null : reasons[0] || "not_commercially_relevant",
    notes: reasons,
    eventContextHint: blob.slice(0, 80) || null,
  };
}

function clearPersonSlot(slot = {}) {
  if (!slot || typeof slot !== "object") return slot;
  return {
    ...slot,
    name: null,
    personName: null,
    fullName: null,
    title: null,
    role: null,
    email: null,
    phone: null,
    personPhone: null,
    companyPhone: scrubAssociationEventsPhone(slot.companyPhone) || null,
    url: slot.url || null,
  };
}

function personCommerciallyOk(person = {}, opportunity = {}) {
  const name = text(person.name || person.personName || person.fullName);
  if (!name) return { ok: false, reason: "empty_name" };
  if (!isAssociationEventsValidPersonName(name)) {
    return { ok: false, reason: "invalid_person_identity", name };
  }
  return assessAssociationEventsBuyerCandidate(
    {
      name,
      title: person.title || person.role || person.contactRole,
      organization: person.organization || person.company,
    },
    {
      organizerName: opportunity.organizationName || opportunity.organizer || "",
      organizationName: opportunity.organizationName || "",
      eventBlob: `${opportunity.eventName || ""} ${opportunity.title || ""}`,
    },
  );
}

/** Org-route sales action when named buyer is invalid / absent. */
export function buildAssociationEventsOrgRouteSalesAction(opportunity = {}) {
  // Prefer shared accessibility / org-role action (Stage 6) over generic Identify copy.
  const hotel =
    text(opportunity.hotelDisplayName || opportunity.hotelName) || "the hotel";
  const assessed = assessBuyerOrgAndAccessibility(opportunity, { hotelName: hotel });
  if (assessed?.recommendedAction) return assessed.recommendedAction;
  const title = text(opportunity.title || opportunity.organizationName) || "this activity";
  return `Identify the meetings / congress / housing owner for ${title} (not sponsorship alone) and introduce ${hotel}; confirm sourcing timetable and whether hotel selection remains open.`;
}

function actionMentionsRejectedName(actionText, rejectedNames = []) {
  const blob = text(actionText).toLowerCase();
  if (!blob) return false;
  return rejectedNames.some((n) => n && blob.includes(String(n).toLowerCase()));
}

function extractContactedPersonFromAction(actionText) {
  const m = text(actionText).match(/^Contact\s+([^:(]+)/i);
  return m ? text(m[1]) : "";
}

function demotePersonEvidenceIfInvalid(personEvidence, opportunity, rejected) {
  if (!personEvidence || typeof personEvidence !== "object") return personEvidence;
  const name = text(personEvidence.name);
  if (!name) return personEvidence;
  const pass = personCommerciallyOk(
    {
      name,
      title: personEvidence.role || personEvidence.title,
      organization: personEvidence.organization,
    },
    opportunity,
  );
  if (pass.ok) return personEvidence;
  rejected.push({
    field: "personEvidence",
    value: name,
    reason: pass.reason || "buyer_integrity_failed",
  });
  // Preserve research provenance URLs; remove actionable verified-buyer claim.
  return {
    ...personEvidence,
    verified: false,
    confidence: "LOW",
    personhood: "RESEARCH_CANDIDATE_REJECTED",
    roleOk: false,
    note: `Rejected as actionable buyer: ${pass.reason || "buyer_integrity_failed"}`,
    _rejectedBuyerName: name,
    _rejectedBuyerRole: text(personEvidence.role || personEvidence.title) || null,
    name: null,
    role: null,
    organization: personEvidence.organization || null,
  };
}

function scrubSalesBriefBuyerSurfaces(salesBrief, opportunity, rejected) {
  if (!salesBrief || typeof salesBrief !== "object") return salesBrief;
  const sb = { ...salesBrief };

  if (sb.whoToContact && typeof sb.whoToContact === "object") {
    const who = { ...sb.whoToContact };
    if (who.primary && !personCommerciallyOk(who.primary, opportunity).ok) {
      rejected.push({
        field: "salesBrief.whoToContact.primary",
        value: text(who.primary.name),
        reason: "invalid_or_unrelated_buyer",
      });
      who.primary = null;
    }
    if (who.secondary && !personCommerciallyOk(who.secondary, opportunity).ok) {
      rejected.push({
        field: "salesBrief.whoToContact.secondary",
        value: text(who.secondary?.name),
        reason: "invalid_or_unrelated_buyer",
      });
      who.secondary = null;
    }
    sb.whoToContact = who;
  }

  if (sb.qualification && typeof sb.qualification === "object") {
    const qual = { ...sb.qualification };
    if (Array.isArray(qual.namedBuyers)) {
      qual.namedBuyers = qual.namedBuyers.filter((b) => {
        const pass = personCommerciallyOk(b, opportunity);
        if (!pass.ok && text(b?.name)) {
          rejected.push({
            field: "salesBrief.qualification.namedBuyers",
            value: text(b.name),
            reason: pass.reason || "invalid_or_unrelated_buyer",
          });
        }
        return pass.ok;
      });
    }
    sb.qualification = qual;
  }

  // Corrupted approach objects (string spread into object) or stale Contact <rejected>
  const approach = sb.recommendedSalesApproach;
  if (approach != null) {
    let approachText = "";
    if (typeof approach === "string") approachText = approach;
    else if (typeof approach === "object") {
      approachText = text(
        approach.nextAction ||
          approach.openingApproach ||
          Object.keys(approach)
            .filter((k) => /^\d+$/.test(k))
            .sort((a, b) => Number(a) - Number(b))
            .map((k) => approach[k])
            .join(""),
      );
    }
    const contacted = extractContactedPersonFromAction(approachText);
    const contactedOk = contacted
      ? personCommerciallyOk(
          {
            name: contacted,
            title: opportunity.contactRole || opportunity._rejectedContactRole,
          },
          opportunity,
        ).ok
      : true;
    const rejectedNames = rejected.map((r) => text(r.value)).filter(Boolean);
    if (
      !contactedOk ||
      actionMentionsRejectedName(approachText, rejectedNames) ||
      (typeof approach === "object" &&
        Object.keys(approach).some((k) => /^\d+$/.test(k)))
    ) {
      rejected.push({
        field: "salesBrief.recommendedSalesApproach",
        value: contacted || approachText.slice(0, 80) || "corrupt_approach",
        reason: contactedOk ? "stale_or_corrupt_sales_approach" : "invalid_or_unrelated_buyer",
      });
      sb.recommendedSalesApproach = buildAssociationEventsOrgRouteSalesAction(opportunity);
    }
  }

  return sb;
}

function rewriteActionIfInvalidBuyer(actionText, opportunity, rejected, field) {
  const action = text(actionText);
  if (!action) return actionText ?? null;
  const contacted = extractContactedPersonFromAction(action);
  if (!contacted) {
    // Still scrub if action body mentions a rejected name from this pass
    const rejectedNames = rejected.map((r) => text(r.value)).filter(Boolean);
    if (actionMentionsRejectedName(action, rejectedNames)) {
      rejected.push({
        field,
        value: rejectedNames.find((n) => action.toLowerCase().includes(n.toLowerCase())),
        reason: "stale_action_mentions_rejected_buyer",
      });
      return buildAssociationEventsOrgRouteSalesAction(opportunity);
    }
    return actionText;
  }
  const titleFromAction = text((action.match(/\(([^)]{3,120})\)/) || [])[1] || "");
  const pass = personCommerciallyOk(
    {
      name: contacted,
      title:
        titleFromAction ||
        opportunity.contactRole ||
        opportunity._rejectedContactRole ||
        opportunity.personEvidence?.role ||
        opportunity.personEvidence?._rejectedBuyerRole ||
        opportunity.salesBrief?.whoToContact?.primary?.title,
    },
    opportunity,
  );
  // Also rewrite if this name was already rejected earlier in the same scrub pass
  const alreadyRejected = rejected.some(
    (r) => text(r.value).toLowerCase() === contacted.toLowerCase(),
  );
  if (pass.ok && !alreadyRejected) return actionText;
  rejected.push({
    field,
    value: contacted,
    reason: pass.ok ? "stale_action_mentions_rejected_buyer" : pass.reason || "invalid_or_unrelated_buyer",
  });
  return buildAssociationEventsOrgRouteSalesAction(opportunity);
}

function clearNestedRejectedBuyerName(node, opportunity, rejected, fieldPath) {
  if (!node || typeof node !== "object") return node;
  if (Array.isArray(node)) {
    return node
      .map((item, i) => clearNestedRejectedBuyerName(item, opportunity, rejected, `${fieldPath}[${i}]`))
      .filter((item) => {
        if (!item || typeof item !== "object") return true;
        // Drop named-buyer array rows that lost their name
        if ("name" in item && !text(item.name) && text(item._clearedRejectedBuyer)) return false;
        return true;
      });
  }
  const out = { ...node };
  const candidateName = text(out.name || out.personName || out.fullName || out.person);
  if (candidateName) {
    const pass = personCommerciallyOk(
      {
        name: candidateName,
        title: out.title || out.role || out.contactRole,
        organization: out.organization || out.company,
      },
      opportunity,
    );
    if (!pass.ok) {
      rejected.push({
        field: fieldPath,
        value: candidateName,
        reason: pass.reason || "invalid_or_unrelated_buyer",
      });
      if ("name" in out) out.name = null;
      if ("personName" in out) out.personName = null;
      if ("fullName" in out) out.fullName = null;
      if ("person" in out && typeof out.person === "string") out.person = null;
      if ("title" in out) out.title = null;
      if ("role" in out) out.role = null;
      // Boolean flag only — do not re-embed rejected name into active disposition JSON.
      out._clearedRejectedBuyer = true;
    }
  }
  for (const [k, v] of Object.entries(out)) {
    if (v && typeof v === "object") {
      out[k] = clearNestedRejectedBuyerName(v, opportunity, rejected, `${fieldPath}.${k}`);
    }
  }
  return out;
}

function scrubDispositionBuyerFields(disposition, opportunity, rejected) {
  if (!disposition || typeof disposition !== "object") return disposition;
  const next = { ...disposition };
  if (next.commercial && typeof next.commercial === "object") {
    next.commercial = clearNestedRejectedBuyerName(
      next.commercial,
      opportunity,
      rejected,
      "disposition.commercial",
    );
  }
  return next;
}

/** Strip already-rejected person names from nested presentation trees (string fields). */
function stripRejectedNamesFromTree(node, rejectedNames, rejected, fieldPath) {
  if (node == null) return node;
  if (typeof node === "string") {
    const lower = node.toLowerCase();
    const hit = rejectedNames.find((n) => n && lower.includes(n.toLowerCase()));
    if (hit && (lower === hit.toLowerCase() || /^[\wÀ-ÿ .'-]+$/i.test(node))) {
      rejected.push({
        field: fieldPath,
        value: hit,
        reason: "stale_nested_rejected_buyer_name",
      });
      return null;
    }
    return node;
  }
  if (Array.isArray(node)) {
    return node
      .map((item, i) =>
        stripRejectedNamesFromTree(item, rejectedNames, rejected, `${fieldPath}[${i}]`),
      )
      .filter((item) => {
        if (item == null) return false;
        if (typeof item === "object" && "name" in item && !text(item.name)) return false;
        return true;
      });
  }
  if (typeof node !== "object") return node;
  const out = { ...node };
  for (const [k, v] of Object.entries(out)) {
    // Preserve diagnostic rejection provenance keys; do not re-scan them.
    if (k.startsWith("_rejected") || k.startsWith("_cleared") || k === "contactAtomicScrubRejected") {
      continue;
    }
    out[k] = stripRejectedNamesFromTree(v, rejectedNames, rejected, `${fieldPath}.${k}`);
  }
  return out;
}

/**
 * Scrub opportunity contact fields atomically (returns new object).
 * run-050: clears nested/gold-brief/orphan roles together; never auto-promotes
 * a weak stakeholder merely to refill contactName.
 * run-052: also clears customer-presentation surfaces (salesBrief, actions,
 * personEvidence verified claim, disposition buyers) so rejected names cannot
 * reappear via UI/API fallbacks.
 */
export function scrubAssociationEventsContactFields(opportunity = {}) {
  if (!isAssociationEventsBuyerIntegritySegment(opportunity)) {
    return scrubPhonesPreserveNamedPeople(opportunity);
  }
  const next = { ...opportunity };
  const rejected = [];

  const top = personCommerciallyOk(
    { name: next.contactName, title: next.contactRole, organization: next.contactOrganization },
    next,
  );
  if (next.contactName && !top.ok) {
    rejected.push({ field: "contactName", value: text(next.contactName), reason: top.reason });
    next._rejectedContactName = text(next.contactName);
    next._rejectedContactRole = text(next.contactRole) || null;
    next.contactName = null;
    next.contactRole = null;
    next.contactEmail = null;
    next.contactOrganization = null;
  } else if (!next.contactName && text(next.contactRole)) {
    rejected.push({
      field: "contactRole",
      value: text(next.contactRole),
      reason: "orphan_role_without_person",
    });
    next._rejectedOrphanContactRole = text(next.contactRole);
    next.contactRole = null;
  }

  const scrubbedPhone = scrubAssociationEventsPhone(next.contactPhone);
  if (next.contactPhone && !scrubbedPhone) {
    rejected.push({ field: "contactPhone", value: text(next.contactPhone), reason: "polluted_phone" });
    next._rejectedContactPhone = text(next.contactPhone);
    next.contactPhone = null;
  } else {
    next.contactPhone = scrubbedPhone;
  }

  const scrubbedCompany = scrubAssociationEventsPhone(next.companyPhone);
  if (next.companyPhone && !scrubbedCompany) {
    next._rejectedCompanyPhone = text(next.companyPhone);
    next.companyPhone = null;
  } else if (scrubbedCompany) {
    next.companyPhone = scrubbedCompany;
  }

  if (next.primaryContact && typeof next.primaryContact === "object") {
    const pcPass = personCommerciallyOk(
      {
        name: next.primaryContact.name || next.primaryContact.personName,
        title: next.primaryContact.title || next.primaryContact.role,
      },
      next,
    );
    if ((next.primaryContact.name || next.primaryContact.personName) && !pcPass.ok) {
      rejected.push({
        field: "primaryContact",
        value: text(next.primaryContact.name || next.primaryContact.personName),
        reason: pcPass.reason,
      });
      next.primaryContact = clearPersonSlot(next.primaryContact);
    } else {
      const pc = { ...next.primaryContact };
      pc.phone = scrubAssociationEventsPhone(pc.phone);
      pc.companyPhone = scrubAssociationEventsPhone(pc.companyPhone);
      next.primaryContact = pc;
    }
  }

  if (next.contactChannelSeparation && typeof next.contactChannelSeparation === "object") {
    const sep = { ...next.contactChannelSeparation };
    sep.companyPhone = scrubAssociationEventsPhone(sep.companyPhone);
    sep.personSpecificPhone = scrubAssociationEventsPhone(sep.personSpecificPhone);
    next.contactChannelSeparation = sep;
  }

  if (next.scalingTwoContactPresentation && typeof next.scalingTwoContactPresentation === "object") {
    const stp = { ...next.scalingTwoContactPresentation };
    for (const slotKey of ["primary", "secondary"]) {
      const slot = stp[slotKey];
      if (!slot || typeof slot !== "object") continue;
      const pass = personCommerciallyOk(
        { name: slot.name || slot.personName, title: slot.title || slot.role },
        next,
      );
      if ((slot.name || slot.personName) && !pass.ok) {
        rejected.push({
          field: `scalingTwoContactPresentation.${slotKey}`,
          value: text(slot.name || slot.personName),
          reason: pass.reason,
        });
        stp[slotKey] = clearPersonSlot(slot);
      } else {
        stp[slotKey] = {
          ...slot,
          phone: scrubAssociationEventsPhone(slot.phone),
          personPhone: scrubAssociationEventsPhone(slot.personPhone),
          companyPhone: scrubAssociationEventsPhone(slot.companyPhone),
        };
      }
    }
    next.scalingTwoContactPresentation = stp;
  }

  if (Array.isArray(next.namedStakeholders)) {
    next.namedStakeholders = next.namedStakeholders.filter((s) => {
      const pass = personCommerciallyOk(s, next);
      if (!pass.ok && text(s?.name)) {
        rejected.push({ field: "namedStakeholders", value: text(s.name), reason: pass.reason });
      }
      return pass.ok;
    });
  }
  if (Array.isArray(next.secondaryContacts)) {
    next.secondaryContacts = next.secondaryContacts.filter((s) => personCommerciallyOk(s, next).ok);
  }

  if (next.associationEventsGoldBrief?.whoToContact) {
    const who = { ...next.associationEventsGoldBrief.whoToContact };
    if (who.primary && !personCommerciallyOk(who.primary, next).ok) {
      rejected.push({
        field: "goldBrief.whoToContact.primary",
        value: text(who.primary.name),
        reason: "invalid_or_unrelated_buyer",
      });
      who.primary = null;
    }
    if (who.secondary && !personCommerciallyOk(who.secondary, next).ok) {
      who.secondary = null;
    }
    who.routes = {
      ...(who.routes || {}),
      email: next.contactEmail || who.routes?.email || null,
      phone: next.contactPhone || null,
      url: next.contactUrl || who.routes?.url || null,
    };
    next.associationEventsGoldBrief = {
      ...next.associationEventsGoldBrief,
      whoToContact: who,
    };
  }

  // --- run-052 presentation-atomic surfaces ---
  // Keep personEvidence role available while rewriting Contact <name> actions,
  // then demote the verified-buyer claim (order matters for Edelman/excerpt titles).
  if (next.salesBrief && typeof next.salesBrief === "object") {
    next.salesBrief = scrubSalesBriefBuyerSurfaces(next.salesBrief, next, rejected);
  }

  next.salesActionRecommended = rewriteActionIfInvalidBuyer(
    next.salesActionRecommended,
    next,
    rejected,
    "salesActionRecommended",
  );
  next.recommendedAction = rewriteActionIfInvalidBuyer(
    next.recommendedAction,
    next,
    rejected,
    "recommendedAction",
  );
  next.suggestedAction = rewriteActionIfInvalidBuyer(
    next.suggestedAction,
    next,
    rejected,
    "suggestedAction",
  );
  // Keep suggestedAction aligned with cleaned recommendedAction when present.
  if (text(next.recommendedAction) && text(next.suggestedAction) !== text(next.recommendedAction)) {
    const sugPass = !/^Contact\s+/i.test(text(next.suggestedAction))
      ? { ok: true }
      : personCommerciallyOk(
          {
            name: extractContactedPersonFromAction(next.suggestedAction),
            title: text((String(next.suggestedAction).match(/\(([^)]{3,120})\)/) || [])[1] || ""),
          },
          next,
        );
    if (!sugPass.ok || actionMentionsRejectedName(
      next.suggestedAction,
      rejected.map((r) => text(r.value)).filter(Boolean),
    )) {
      next.suggestedAction = next.recommendedAction;
    }
  }

  next.personEvidence = demotePersonEvidenceIfInvalid(next.personEvidence, next, rejected);
  if (text(next.conversationOpener)) {
    const rejectedNames = rejected.map((r) => text(r.value)).filter(Boolean);
    if (actionMentionsRejectedName(next.conversationOpener, rejectedNames)) {
      rejected.push({
        field: "conversationOpener",
        value: rejectedNames.find((n) =>
          text(next.conversationOpener).toLowerCase().includes(n.toLowerCase()),
        ),
        reason: "stale_opener_mentions_rejected_buyer",
      });
      next.conversationOpener = null;
    }
  }

  if (next.gdi2ContinuationDispositionV1) {
    next.gdi2ContinuationDispositionV1 = scrubDispositionBuyerFields(
      next.gdi2ContinuationDispositionV1,
      next,
      rejected,
    );
    // Second pass: names already rejected this scrub (often without title on nested
    // disposition nodes) must not survive as attribution/route.surface.person strings.
    const rejectedNames = [
      ...new Set(rejected.map((r) => text(r.value)).filter(Boolean)),
    ];
    if (rejectedNames.length && next.gdi2ContinuationDispositionV1?.commercial) {
      next.gdi2ContinuationDispositionV1 = {
        ...next.gdi2ContinuationDispositionV1,
        commercial: stripRejectedNamesFromTree(
          next.gdi2ContinuationDispositionV1.commercial,
          rejectedNames,
          rejected,
          "disposition.commercial",
        ),
      };
    }
  }

  // Recompute verified-buyer flag from surviving commercial contact slots only.
  const survivingNamedBuyer =
    Boolean(text(next.contactName) && personCommerciallyOk({ name: next.contactName, title: next.contactRole }, next).ok) ||
    Boolean(
      text(next.primaryContact?.name || next.primaryContact?.personName) &&
        personCommerciallyOk(next.primaryContact, next).ok,
    ) ||
    Boolean(
      next.associationEventsGoldBrief?.whoToContact?.primary &&
        personCommerciallyOk(next.associationEventsGoldBrief.whoToContact.primary, next).ok,
    );
  if (!survivingNamedBuyer) {
    next.hasVerifiedNamedRelevantContact = false;
    if (next.personEvidence?.verified) {
      next.personEvidence = { ...next.personEvidence, verified: false };
    }
  }

  // Prefer gold-brief org-route approach when gold primary is null and actions were rewritten.
  // Never re-poison from a gold approach that still says Contact-<person> after scrub.
  let goldApproach = next.associationEventsGoldBrief?.recommendedSalesApproach;
  if (typeof goldApproach === "string" && /^Contact\s+/i.test(text(goldApproach))) {
    const cleanApproach = buildAssociationEventsOrgRouteSalesAction(next);
    next.associationEventsGoldBrief = {
      ...next.associationEventsGoldBrief,
      recommendedSalesApproach: cleanApproach,
    };
    goldApproach = cleanApproach;
  }
  if (
    typeof goldApproach === "string" &&
    text(goldApproach) &&
    !/^Contact\s+/i.test(text(goldApproach)) &&
    !next.associationEventsGoldBrief?.whoToContact?.primary &&
    (!text(next.recommendedAction) ||
      /^Identify the association meetings/i.test(text(next.recommendedAction)))
  ) {
    if (!text(next.recommendedAction) || /^Identify the association meetings/i.test(text(next.recommendedAction))) {
      next.recommendedAction = goldApproach;
    }
    if (
      !text(next.salesActionRecommended) ||
      /^Identify the association meetings/i.test(text(next.salesActionRecommended))
    ) {
      next.salesActionRecommended = goldApproach;
    }
  }

  // Intentionally NO auto-promote of namedStakeholders into contactName.
  next.contactAtomicScrubRejected = rejected;
  next.associationEventsBuyerIntegrityVersion = ASSOCIATION_EVENTS_BUYER_INTEGRITY_V1;
  next.associationEventsPresentationScrubVersion = "gdi-run-052-presentation-atomic-scrub-v1";
  return next;
}

/**
 * Assert a rejected name is absent from all contact AND customer-presentation surfaces.
 * run-052: includes salesBrief, actions, personEvidence actionable name, disposition.
 * Does not require absence from immutable evidence/source quote arrays.
 */
export function assertRejectedContactAbsent(opportunity = {}, rejectedName = "") {
  const needle = text(rejectedName).toLowerCase();
  if (!needle) return { ok: true, found: false };
  const pe = opportunity.personEvidence || {};
  const blob = JSON.stringify({
    contactName: opportunity.contactName,
    contactRole: opportunity.contactRole,
    primaryContact: opportunity.primaryContact,
    scalingTwoContactPresentation: opportunity.scalingTwoContactPresentation,
    namedStakeholders: opportunity.namedStakeholders,
    secondaryContacts: opportunity.secondaryContacts,
    whoToContact: opportunity.associationEventsGoldBrief?.whoToContact,
    salesBriefWhoToContact: opportunity.salesBrief?.whoToContact,
    salesBriefNamedBuyers: opportunity.salesBrief?.qualification?.namedBuyers,
    salesBriefRecommendedSalesApproach: opportunity.salesBrief?.recommendedSalesApproach,
    salesActionRecommended: opportunity.salesActionRecommended,
    recommendedAction: opportunity.recommendedAction,
    suggestedAction: opportunity.suggestedAction,
    conversationOpener: opportunity.conversationOpener,
    personEvidenceName: pe.name,
    personEvidenceVerifiedClaim:
      pe.verified === true ? { name: pe.name, role: pe.role } : null,
    dispositionCommercial: opportunity.gdi2ContinuationDispositionV1?.commercial || null,
  }).toLowerCase();
  return { ok: !blob.includes(needle), found: blob.includes(needle) };
}

/**
 * List integrity failures on an opportunity (diagnostic).
 */
export function diagnoseAssociationEventsBuyerIntegrity(opportunity = {}) {
  const failures = [];
  if (text(opportunity.contactName) && !isAssociationEventsValidPersonName(opportunity.contactName)) {
    failures.push({
      field: "contactName",
      value: text(opportunity.contactName),
      reason: "invalid_person_identity",
    });
  }
  if (opportunity.contactPhone && !scrubAssociationEventsPhone(opportunity.contactPhone)) {
    failures.push({
      field: "contactPhone",
      value: text(opportunity.contactPhone),
      reason: "polluted_phone",
    });
  }
  if (opportunity.companyPhone && !scrubAssociationEventsPhone(opportunity.companyPhone)) {
    failures.push({
      field: "companyPhone",
      value: text(opportunity.companyPhone),
      reason: "polluted_phone",
    });
  }
  for (const s of opportunity.namedStakeholders || []) {
    if (text(s?.name) && !isAssociationEventsValidPersonName(s.name)) {
      failures.push({
        field: "namedStakeholders",
        value: text(s.name),
        reason: "invalid_person_identity",
      });
    }
  }
  return {
    version: ASSOCIATION_EVENTS_BUYER_INTEGRITY_V1,
    ok: failures.length === 0 && isAssociationEventsValidPersonName(opportunity.contactName),
    failures,
  };
}
