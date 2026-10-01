/**
 * Pilot scorecard person-contact chain helpers.
 * Qualification for person A cannot combine with contact for person B.
 * Value-specific attribution: one attributable email does not validate another.
 */

export function nameKey(n) {
  return String(n || "")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .trim();
}

/**
 * Contact outcome for one complete person (or none).
 * @returns {"EMAIL_AND_PHONE"|"EMAIL_ONLY"|"PHONE_ONLY"|"NONE"}
 */
export function classifyPersonContactOutcome(perPerson) {
  const email = Boolean(perPerson?.email_attributable);
  const phone = Boolean(perPerson?.phone_attributable);
  if (email && phone) return "EMAIL_AND_PHONE";
  if (email) return "EMAIL_ONLY";
  if (phone) return "PHONE_ONLY";
  return "NONE";
}

/**
 * Find one qualified person who also carries attributable email/phone.
 * Case-level aggregate attributable flags must not complete the chain.
 * Channel paths require value-specific personAttributedChannels entries
 * (already filtered to exact values) plus affiliation evidence.
 */
export function findCompletePersonContactChain({
  qualifiedPeople = [],
  contactStatuses = {},
  personAttributedChannels = [],
} = {}) {
  const perPerson = contactStatuses.per_person || [];
  for (const p of qualifiedPeople) {
    const key = nameKey(p.display_name || p.name);
    const pp = perPerson.find((x) => nameKey(x.display_name) === key);
    if (
      pp &&
      pp.relevant_person &&
      pp.affiliation_evidenced &&
      (pp.email_attributable || pp.phone_attributable)
    ) {
      return {
        ok: true,
        person: p,
        per_person: pp,
        via: "contact_statuses.per_person",
        contact_outcome: classifyPersonContactOutcome(pp),
      };
    }
    const channelHit = (personAttributedChannels || []).some(
      (c) => nameKey(c.person_name) === key
    );
    if (
      channelHit &&
      pp &&
      pp.relevant_person === true &&
      pp.affiliation_evidenced === true
    ) {
      return {
        ok: true,
        person: p,
        per_person: pp,
        via: "person_attributed_channels",
        contact_outcome: classifyPersonContactOutcome(pp),
      };
    }
  }
  return {
    ok: false,
    person: null,
    per_person: null,
    via: null,
    contact_outcome: "NONE",
  };
}

/**
 * Evaluate complete_chain for a hotel scorecard row.
 * Does not use case-level email_attributable / phone_attributable aggregates.
 */
export function evaluatePilotCompleteChain({
  ownerOk,
  domainOk,
  qualifiedPeople,
  contactStatuses,
  personAttributedChannels,
}) {
  const chain = findCompletePersonContactChain({
    qualifiedPeople,
    contactStatuses,
    personAttributedChannels,
  });
  const emailOk = Boolean(chain.per_person?.email_attributable);
  const phoneOk = Boolean(chain.per_person?.phone_attributable);
  return {
    complete_chain: Boolean(ownerOk && domainOk && chain.ok),
    complete_person: chain.person,
    person_ok: (qualifiedPeople || []).length > 0,
    person_contact_ok: chain.ok,
    via: chain.via,
    contact_outcome: chain.contact_outcome,
    email_and_phone: Boolean(chain.ok && emailOk && phoneOk),
    email_only: Boolean(chain.ok && emailOk && !phoneOk),
    phone_only: Boolean(chain.ok && phoneOk && !emailOk),
  };
}
