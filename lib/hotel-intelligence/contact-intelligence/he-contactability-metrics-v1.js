/**
 * Hotel Explorer Contactability V1 — permanent quality metric.
 *
 * Contact identification ≠ contactability.
 * Direct contactability requires verified durable email or phone.
 * Company websites, switchboards, generic org mailboxes, and transient
 * Surfe session results do NOT count.
 */

export const HE_CONTACTABILITY_METRICS_V1 = "he-contactability-metrics-v1";

const EPHEMERAL_RE =
  /surfe|ephemeral|session[_-]?only|provider_returned|transient/i;

function text(v) {
  return String(v == null ? "" : v).trim();
}

function isEphemeralChannel(ch) {
  if (!ch || typeof ch !== "object") return false;
  if (ch.ephemeral === true || ch.surfeDerived === true || ch.sessionOnly === true) {
    return true;
  }
  const blob = [
    ch.source,
    ch.provenance,
    ch.provider,
    ch.verificationStatus,
    ch.verification_status,
    ch.note,
  ]
    .map(text)
    .join(" ");
  return EPHEMERAL_RE.test(blob);
}

function channelValue(ch) {
  return text(ch && (ch.value || ch.email || ch.phone));
}

function emailChannelList(person) {
  if (Array.isArray(person?.emailContacts) && person.emailContacts.length) {
    return person.emailContacts;
  }
  if (Array.isArray(person?.emails) && person.emails.length) {
    return person.emails;
  }
  if (Array.isArray(person?.channels)) {
    return person.channels.filter((c) =>
      /email/i.test(String(c?.kind || c?.type || "")),
    );
  }
  return [];
}

function phoneChannelList(person) {
  if (Array.isArray(person?.phoneContacts) && person.phoneContacts.length) {
    return person.phoneContacts;
  }
  if (Array.isArray(person?.phones) && person.phones.length) {
    return person.phones;
  }
  if (Array.isArray(person?.channels)) {
    return person.channels.filter((c) =>
      /phone|tel/i.test(String(c?.kind || c?.type || "")),
    );
  }
  return [];
}

function hasDurableDirectEmail(person) {
  return emailChannelList(person).some(
    (c) =>
      channelValue(c) &&
      !isEphemeralChannel(c) &&
      !isGenericMailbox(channelValue(c)),
  );
}

function hasDurableDirectPhone(person) {
  return phoneChannelList(person).some(
    (c) => channelValue(c) && !isEphemeralChannel(c),
  );
}

function hasVerifiedLinkedIn(person) {
  const url = text(person?.linkedinUrl || person?.linkedin || person?.profileUrl);
  if (/linkedin\.com\/in\//i.test(url)) return true;
  const channels = Array.isArray(person?.channels) ? person.channels : [];
  return channels.some(
    (c) =>
      /linkedin/i.test(String(c?.kind || c?.type || "")) &&
      /linkedin\.com\/in\//i.test(channelValue(c)) &&
      !isEphemeralChannel(c),
  );
}

function isGenericMailbox(email) {
  const local = String(email || "")
    .split("@")[0]
    .toLowerCase();
  return /^(info|contact|sales|reservations|front[_-]?desk|hello|admin|office|enquiry|inquiry)$/i.test(
    local,
  );
}

function isRelevantContact(person) {
  if (!person) return false;
  if (person.relevant === false || person.excludeFromContactability === true) return false;
  const name = text(person.name || person.fullName);
  if (!name || name.split(/\s+/).length < 2) return false;
  const role = text(
    person.businessPurpose ||
      person.purpose ||
      person.relationship ||
      person.relationshipToHotel ||
      person.title ||
      "",
  ).toLowerCase();
  if (/alternative[_ ]entry|org[_ ]entry|company[_ ]route|switchboard/i.test(role)) {
    return person.countAsDecisionMaker === true;
  }
  return true;
}

function isAlternativeEntryPoint(person) {
  const role = text(
    person?.businessPurpose || person?.purpose || person?.relationship || "",
  ).toLowerCase();
  return (
    person?.isAlternativeEntryPoint === true ||
    /alternative[_ ]entry|org[_ ]entry|entry[_ ]point/i.test(role)
  );
}

/**
 * @param {object[]} people
 * @returns {object} contactability metrics for one hotel
 */
export function computeContactabilityMetrics(people = []) {
  const list = Array.isArray(people) ? people.filter(Boolean) : [];
  const named = list.filter((p) => text(p.name || p.fullName).split(/\s+/).length >= 2);
  const relevant = named.filter(isRelevantContact);
  const alternatives = named.filter(isAlternativeEntryPoint);

  let verifiedNamed = 0;
  let emailCount = 0;
  let phoneCount = 0;
  let linkedinCount = 0;
  let reachable = 0;
  let unresolved = 0;

  for (const p of relevant) {
    const verified =
      p.verificationStatus === "VERIFIED" ||
      p.verified === true ||
      /VERIFIED|HIGH|PUBLIC/i.test(String(p.verificationStatus || p.confidence || ""));
    if (verified || text(p.title) || text(p.organization || p.organizationName)) {
      verifiedNamed += 1;
    }
    const email = hasDurableDirectEmail(p);
    const phone = hasDurableDirectPhone(p);
    const li = hasVerifiedLinkedIn(p);
    if (email) emailCount += 1;
    if (phone) phoneCount += 1;
    if (li) linkedinCount += 1;
    if (email || phone) reachable += 1;
    else unresolved += 1;
  }

  const denominator = relevant.length;
  const rate =
    denominator === 0
      ? null
      : Number((reachable / denominator).toFixed(4));

  return {
    version: HE_CONTACTABILITY_METRICS_V1,
    namedContactCount: named.length,
    verifiedNamedContacts: verifiedNamed,
    verifiedNamedContactCoverage: verifiedNamed,
    verifiedBusinessEmails: emailCount,
    verifiedDirectEmailCount: emailCount,
    verifiedDirectTelephones: phoneCount,
    verifiedDirectPhoneCount: phoneCount,
    verifiedLinkedInProfiles: linkedinCount,
    verifiedLinkedInCount: linkedinCount,
    directlyContactableProfessionals: reachable,
    verifiedDirectContactabilityRate: rate,
    directContactabilityStatus: denominator === 0 ? "NOT_MEASURABLE" : "MEASURED",
    relevantDecisionMakerCoverage: relevant.length,
    alternativeEntryPointCoverage: alternatives.length,
    unresolvedContactChannels: unresolved,
    directChannelsUnresolved: unresolved,
    /** Identification vs reachability — never conflate. */
    identificationComplete:
      relevant.length > 0 && verifiedNamed >= Math.min(1, relevant.length),
    contactabilityComplete: denominator > 0 && unresolved === 0,
  };
}

export function formatContactabilityFactoryFields(metrics) {
  const m = metrics || computeContactabilityMetrics([]);
  return {
    VerifiedPeople: m.verifiedNamedContactCoverage,
    VerifiedDirectEmails: m.verifiedDirectEmailCount,
    VerifiedDirectTelephones: m.verifiedDirectPhoneCount,
    VerifiedLinkedInProfiles: m.verifiedLinkedInCount,
    OrganizationEntryPoints: m.alternativeEntryPointCoverage,
    DirectChannelsUnresolved: m.directChannelsUnresolved,
    DirectContactabilityRate: m.verifiedDirectContactabilityRate,
    DirectContactabilityStatus: m.directContactabilityStatus,
    FindymailLookups: "NOT_RUN",
    FindymailCost: 0,
    SurfeUserInitiatedLookups: "N/A_UNATTENDED",
    CanonicalContactPersistence: "NOT_ASSESSED",
    ContactApiHydration: "NOT_ASSESSED",
    ContactUiRendering: "NOT_ASSESSED",
  };
}

export default {
  HE_CONTACTABILITY_METRICS_V1,
  computeContactabilityMetrics,
  formatContactabilityFactoryFields,
};
