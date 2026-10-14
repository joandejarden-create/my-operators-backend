/**
 * Contact channel policy for Parallel → GDI enrichment sequence.
 *
 * Sequence: saved evidence → local/official/public → Surfe fallback (opt-in).
 * Email OR phone is enough. Do not auto-buy a second channel.
 * Verification ≠ employment ≠ authority ≠ open sourcing.
 */

export const CONTACT_CHANNEL_POLICY_VERSION = "gdi-parallel-contact-channel-policy-v1";

export const CHANNEL_VERIFICATION = Object.freeze({
  OFFICIALLY_PUBLISHED: "officially_published",
  PROVIDER_VALIDATED: "provider_validated",
  LOCAL_PUBLIC: "local_public",
  NOT_FOUND: "not_found",
  AMBIGUOUS: "ambiguous",
  UNSUPPORTED: "unsupported",
  GENERATED_VARIANT: "generated_variant",
  UNKNOWN: "unknown",
});

export const ROUTE_CLASS = Object.freeze({
  USABLE_NAMED: "usable_named",
  USABLE_FUNCTIONAL: "usable_functional",
  ROUTING_ONLY: "routing_only",
  QUARANTINED: "quarantined",
  NONE: "none",
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function isEmail(v) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(text(v).split(";")[0]);
}

function isPhone(v) {
  const d = text(v).replace(/[^\d+]/g, "");
  return d.length >= 10;
}

const GENERIC_SWITCHBOARD = [
  /main\s*line/i,
  /switchboard/i,
  /front\s*desk/i,
  /general\s*inquiry/i,
  /event\s*help\s*desk/i,
  /help\s*desk/i,
];

const FUNCTIONAL_INBOX = [
  /info@/i,
  /contact@/i,
  /events?@/i,
  /meetings?@/i,
  /housing@/i,
  /registration@/i,
  /education@/i,
];

/**
 * Classify a single email/phone channel with its own verification status.
 */
export function classifyChannelValue(kind, value, meta = {}) {
  const raw = text(value);
  if (!raw) {
    return {
      kind,
      value: null,
      present: false,
      verificationStatus: CHANNEL_VERIFICATION.NOT_FOUND,
      usable: false,
    };
  }
  const verification =
    meta.verificationStatus ||
    meta.quality ||
    CHANNEL_VERIFICATION.UNKNOWN;
  const generated =
    verification === CHANNEL_VERIFICATION.GENERATED_VARIANT ||
    meta.generated === true;
  const unsupported =
    verification === CHANNEL_VERIFICATION.UNSUPPORTED ||
    verification === CHANNEL_VERIFICATION.NOT_FOUND ||
    verification === CHANNEL_VERIFICATION.AMBIGUOUS ||
    generated;

  let usable = false;
  if (kind === "email" && isEmail(raw) && !unsupported) usable = true;
  if (kind === "phone" && isPhone(raw) && !unsupported) {
    const phoneKind = text(meta.phoneKind || meta.kind);
    if (
      GENERIC_SWITCHBOARD.some((re) => re.test(phoneKind)) ||
      GENERIC_SWITCHBOARD.some((re) => re.test(raw))
    ) {
      usable = false;
    } else {
      usable = true;
    }
  }

  return {
    kind,
    value: raw,
    present: true,
    verificationStatus: verification,
    source: meta.source || null,
    verifiedAt: meta.verifiedAt || meta.verificationDate || null,
    method: meta.method || meta.source || null,
    usable,
    providerValidationOnly:
      verification === CHANNEL_VERIFICATION.PROVIDER_VALIDATED,
    neverOfficiallyPublished: generated || unsupported,
  };
}

/**
 * Evaluate contact surface for a card/opportunity.
 */
export function evaluateContactChannels(contact = {}, opts = {}) {
  const name = text(contact.name);
  const identityKind = text(contact.identityKind || opts.identityKind);
  const namedPerson =
    identityKind === "named_person" ||
    (name.split(/\s+/).length >= 2 &&
      !/^(general|info|staff|office|housing\s*center)/i.test(name));

  const emailsRaw = text(contact.email || contact.functionalEmail);
  const emailParts = emailsRaw
    ? emailsRaw.split(/[;,]/).map((s) => s.trim()).filter(Boolean)
    : [];
  const emails = emailParts.map((e) =>
    classifyChannelValue("email", e, {
      verificationStatus: contact.emailQuality || contact.verificationStatus,
      source: contact.source,
      verifiedAt: contact.verificationDate,
      method: contact.source,
    })
  );
  if (!emails.length && contact.functionalEmail) {
    emails.push(
      classifyChannelValue("email", contact.functionalEmail, {
        verificationStatus: contact.emailQuality,
        source: contact.source,
      })
    );
  }

  const phone = classifyChannelValue("phone", contact.phone || contact.switchboard, {
    verificationStatus: contact.phoneQuality,
    phoneKind: contact.phoneKind,
    source: contact.source,
  });

  const usableEmail = emails.find((e) => e.usable) || null;
  const usablePhone = phone.usable ? phone : null;
  const hasUsableChannel = Boolean(usableEmail || usablePhone);

  let routeClass = ROUTE_CLASS.NONE;
  if (hasUsableChannel && namedPerson) routeClass = ROUTE_CLASS.USABLE_NAMED;
  else if (hasUsableChannel) {
    const functional = emails.some((e) =>
      FUNCTIONAL_INBOX.some((re) => re.test(e.value || ""))
    );
    routeClass = functional
      ? ROUTE_CLASS.USABLE_FUNCTIONAL
      : ROUTE_CLASS.USABLE_NAMED;
  } else if (
    emails.some((e) => e.present) ||
    phone.present ||
    /housing\s*center|routing/i.test(name)
  ) {
    routeClass = ROUTE_CLASS.ROUTING_ONLY;
  }

  const quarantined = emails.filter(
    (e) =>
      e.present &&
      (e.verificationStatus === CHANNEL_VERIFICATION.NOT_FOUND ||
        e.verificationStatus === CHANNEL_VERIFICATION.AMBIGUOUS ||
        e.verificationStatus === CHANNEL_VERIFICATION.UNSUPPORTED ||
        e.verificationStatus === CHANNEL_VERIFICATION.GENERATED_VARIANT)
  );

  if (quarantined.length && !hasUsableChannel) {
    routeClass = ROUTE_CLASS.QUARANTINED;
  }

  /** Surfe: named person + org, neither usable email nor phone. */
  const orgOk = Boolean(text(opts.organizationName));
  const surfeEligible =
    namedPerson &&
    orgOk &&
    !hasUsableChannel &&
    routeClass !== ROUTE_CLASS.ROUTING_ONLY &&
    quarantined.length === 0;

  return {
    version: CONTACT_CHANNEL_POLICY_VERSION,
    namedPerson,
    organizationName: opts.organizationName || null,
    emails,
    phone,
    hasUsableEmail: Boolean(usableEmail),
    hasUsablePhone: Boolean(usablePhone),
    hasUsableChannel,
    /** One channel is enough — do not purchase the other automatically. */
    secondChannelPurchaseBlocked: true,
    routeClass,
    quarantinedEmails: quarantined,
    surfeEligible,
    surfeSkipReason: surfeEligible
      ? null
      : hasUsableChannel
        ? "usable_channel_already_present"
        : !namedPerson
          ? "no_verified_named_person"
          : !orgOk
            ? "organization_identity_insufficient"
            : routeClass === ROUTE_CLASS.ROUTING_ONLY
              ? "routing_only_not_surfe_target"
              : quarantined.length
                ? "unsupported_or_not_found_quarantine"
                : "not_eligible",
    dimensionsSeparated: {
      contactUsability: hasUsableChannel,
      currentEmployment: "UNKNOWN_NOT_INFERRED_FROM_CHANNEL",
      relevantRole: contact.role || null,
      purchasingAuthority: "NOT_PROVEN_BY_CONTACT",
      accountSpecificRelationship: "NOT_PROVEN_BY_CONTACT",
      openHotelSourcing: "NOT_PROVEN_BY_CONTACT",
    },
  };
}

/**
 * Build Surfe fallback queue from evaluated contacts (plan only).
 */
export function buildSurfeFallbackQueue(evaluatedRows = [], { maxCredits = 5 } = {}) {
  const queue = [];
  for (const row of evaluatedRows) {
    if (!row.surfeEligible) continue;
    queue.push({
      personName: row.personName || row.contact?.name,
      organizationName: row.organizationName,
      opportunityId: row.opportunityId || row.cardId || null,
      includeEmail: true,
      includeMobile: false,
      expectedEmailCreditsWorstCase: 1,
      expectedMobileCreditsWorstCase: 0,
      reason: "named_person_org_ok_no_usable_channel",
    });
    if (queue.length >= maxCredits) break;
  }
  return {
    version: CONTACT_CHANNEL_POLICY_VERSION,
    queue,
    expectedEmailCreditsWorstCase: queue.length,
    expectedMobileCreditsWorstCase: 0,
    note: "Plan only — do not execute Surfe without authorize + GDI_PARALLEL_SURFE_FALLBACK_ENABLED",
  };
}
