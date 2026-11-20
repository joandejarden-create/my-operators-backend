/**
 * Hotel Explorer — public contact channel hierarchy (founder directive).
 *
 * Aligns with GDI `gdi-public-contact-channel-hierarchy-v1` display + Surfe policy:
 * Public research first → canonical persistence after authorization →
 * Surfe only on explicit click and session-only (never persist Surfe PII).
 *
 * Do not invent emails from guessed formats. Do not mislabel org channels as direct.
 */

export const HE_PUBLIC_CONTACT_CHANNEL_HIERARCHY_V1 =
  "he-public-contact-channel-hierarchy-v1";

/** Shared with GDI founder directive labels — keep identical. */
export const CONTACT_DISPLAY_LABELS = Object.freeze({
  directEmail: "Direct Business Email",
  directPhone: "Direct Business Phone",
  orgEmail: "Organizational Contact Route",
  orgPhone: "Organizational Contact Route",
  findEmail: "Find Email",
  findPhone: "Find Phone",
  findBoth: "Find Email & Phone",
});

export const CONTACT_HIERARCHY_RANK = Object.freeze({
  PROPERTY_DECISION_MAKER: 1,
  OWNER_OR_OPERATOR_EXECUTIVE: 2,
  COMMERCIAL_INFLUENCER: 3,
  ORGANIZATIONAL_ROUTE: 4,
  PUBLISHED_BUSINESS_ROUTE: 5,
  ON_DEMAND_ENRICHMENT: 6,
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function present(v) {
  const s = text(v);
  return s !== "" && s !== "—" && s !== "-" && s.toLowerCase() !== "n/a";
}

export function isGenericMailbox(email) {
  const local = text(email).split("@")[0] || "";
  return /^(info|contact|hello|office|admin|support|sales|inquiries|enquiry|reception|mail|team|press|media)$/i.test(
    local
  );
}

/** Reject invented / pattern-guessed personal emails (HE-QL / GDI shared gate). */
export function looksLikeInventedEmail(email, personName) {
  const e = text(email).toLowerCase();
  if (!e || !e.includes("@")) return true;
  if (isGenericMailbox(e)) return false; // org mailboxes are allowed as org routes, not invented personal
  const local = e.split("@")[0] || "";
  const parts = text(personName)
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .split(/\s+/)
    .filter(Boolean);
  if (parts.length < 2) return false;
  const first = parts[0];
  const last = parts[parts.length - 1];
  const guesses = [
    `${first}.${last}`,
    `${first}${last}`,
    `${first[0]}${last}`,
    `${first}_${last}`,
    `${last}.${first}`,
  ];
  return guesses.includes(local);
}

function channelValue(ch) {
  return text(ch && (ch.value || ch.email || ch.phone));
}

function isOrgChannel(ch) {
  const blob = [
    ch?.classification,
    ch?.type,
    ch?.subject_type,
    ch?.subjectType,
    ch?.channelKind,
    ch?.kind,
    ch?.note,
    channelValue(ch),
  ]
    .map((x) => text(x).toLowerCase())
    .join(" ");
  return /official_org|org_general|organization|switchboard|info@|contact@|hello@|organizational/.test(
    blob
  );
}

function isPubliclyVerifiedChannel(ch) {
  const blob = [
    ch?.verification_status,
    ch?.verificationStatus,
    ch?.source,
    ch?.provenance,
    ch?.discovery_method,
  ]
    .map((x) => text(x).toUpperCase())
    .join(" ");
  return /VERIFIED_PUBLIC|PUBLICLY_VERIFIED|PUBLIC_SOURCE|COMPANY_WEBSITE|OFFICIAL/.test(blob);
}

export function selectDirectEmails(person = {}) {
  const list = person.emailContacts || person.emails || [];
  return list.filter((c) => {
    const v = channelValue(c);
    if (!v || !v.includes("@")) return false;
    if (isOrgChannel(c) || isGenericMailbox(v)) return false;
    if (c.surfeDerived === true || c.provider === "surfe" || c.ephemeral === true) return false;
    // Reject name-pattern guesses unless independently marked publicly verified.
    if (
      looksLikeInventedEmail(v, person.name || person.display_name) &&
      !isPubliclyVerifiedChannel(c)
    ) {
      return false;
    }
    return true;
  });
}

export function selectDirectPhones(person = {}) {
  const list = person.phoneContacts || person.phones || [];
  return list.filter((c) => {
    const v = channelValue(c);
    if (!v) return false;
    if (isOrgChannel(c)) return false;
    if (c.surfeDerived === true || c.provider === "surfe" || c.ephemeral === true) return false;
    return true;
  });
}

export function selectOrgRoutes(person = {}, orgFallback = null) {
  const out = [];
  const seen = new Set();
  function push(ch, kind) {
    const v = channelValue(ch);
    if (!v) return;
    const key = `${kind}:${v.toLowerCase()}`;
    if (seen.has(key)) return;
    seen.add(key);
    out.push({
      ...ch,
      kind,
      value: v,
      label: kind === "email" || kind === "web" ? CONTACT_DISPLAY_LABELS.orgEmail : CONTACT_DISPLAY_LABELS.orgPhone,
    });
  }
  for (const c of person.emailContacts || person.emails || []) {
    if (isOrgChannel(c) || isGenericMailbox(channelValue(c))) push(c, "email");
  }
  for (const c of person.phoneContacts || person.phones || []) {
    if (isOrgChannel(c)) push(c, "phone");
  }
  for (const c of person.organizationalChannels || person.orgChannels || []) {
    const v = channelValue(c);
    if (!v) continue;
    if (v.includes("@")) push(c, "email");
    else if (/^https?:/i.test(v)) push({ ...c, value: v }, "web");
    else push(c, "phone");
  }
  if (orgFallback) {
    for (const c of orgFallback.emails || []) push(c, "email");
    for (const c of orgFallback.phones || []) push(c, "phone");
    if (present(orgFallback.website)) {
      push({ value: orgFallback.website, classification: "official_org" }, "web");
    }
  }
  return out;
}

/**
 * Resolve Surfe CTA from missing direct channels only.
 * Returns null when no paid enrichment is appropriate.
 */
export function resolveEnrichAction(person = {}, opts = {}) {
  const name = text(person.name || person.display_name);
  const org = text(person.organization || person.organizationName);
  const hasName = name.split(/\s+/).filter(Boolean).length >= 2;
  const emails = selectDirectEmails(person);
  const phones = selectDirectPhones(person);
  const hasEmail = emails.length > 0;
  const hasPhone = phones.length > 0;

  if (!hasName || !org) {
    return {
      label: CONTACT_DISPLAY_LABELS.findBoth,
      enabled: false,
      channel: null,
      missing: [],
      reason: "Enrichment needs a full name and organization.",
      demoState: hasEmail && hasPhone ? "A" : hasEmail ? "B" : hasPhone ? "C" : "D",
    };
  }
  if (hasEmail && hasPhone) {
    return {
      label: null,
      enabled: false,
      channel: null,
      missing: [],
      reason: null,
      demoState: "A",
    };
  }

  let channel = "both";
  let label = CONTACT_DISPLAY_LABELS.findBoth;
  const missing = [];
  if (!hasEmail && hasPhone) {
    channel = "email";
    label = CONTACT_DISPLAY_LABELS.findEmail;
    missing.push("professional_email");
  } else if (hasEmail && !hasPhone) {
    channel = "phone";
    label = CONTACT_DISPLAY_LABELS.findPhone;
    missing.push("direct_business_phone");
  } else {
    missing.push("professional_email", "direct_business_phone");
  }

  const providerReady = opts.providerReady === true;
  const forcedOff = person.enrichAction && person.enrichAction.enabled === false;
  return {
    label,
    enabled: providerReady && !forcedOff,
    channel,
    missing,
    reason: providerReady
      ? null
      : (opts.userReason ||
        (person.enrichAction && person.enrichAction.reason) ||
        "Contact enrichment is not configured in this environment."),
    demoState: hasEmail ? "B" : hasPhone ? "C" : "D",
  };
}

export function classifyHeContactRole(person = {}) {
  const klass = text(person.contactClass || "").toUpperCase();
  const title = text(person.title || person.role);
  if (/OWNER_DECISION|PROPERTY_DECISION|DECISION_AUTHORITY/i.test(klass)) {
    return {
      rank: CONTACT_HIERARCHY_RANK.OWNER_OR_OPERATOR_EXECUTIVE,
      code: klass || "OWNER_DECISION_AUTHORITY",
      label: "Owner-side / operator-side executive",
      signingAuthorityClaimed: false,
    };
  }
  if (/RELEVANT_COMMERCIAL|ENTRY_POINT|INFLUENCER|REFERRAL/i.test(klass)) {
    return {
      rank: CONTACT_HIERARCHY_RANK.COMMERCIAL_INFLUENCER,
      code: klass || "RELEVANT_COMMERCIAL_ENTRY_POINT",
      label: "Relevant commercial influencer / referral",
      signingAuthorityClaimed: false,
    };
  }
  if (/asset management|investments|director|vp|president|ceo/i.test(title)) {
    return {
      rank: CONTACT_HIERARCHY_RANK.COMMERCIAL_INFLUENCER,
      code: "RELEVANT_COMMERCIAL_ENTRY_POINT",
      label: "Relevant commercial influencer / referral",
      signingAuthorityClaimed: false,
    };
  }
  return {
    rank: CONTACT_HIERARCHY_RANK.ORGANIZATIONAL_ROUTE,
    code: klass || "ORGANIZATIONAL_ROUTE",
    label: "Organizational / published business route",
    signingAuthorityClaimed: false,
  };
}

export function buildHePersonChannelDisplay(person = {}, orgFallback = null) {
  const emails = selectDirectEmails(person);
  const phones = selectDirectPhones(person);
  const orgRoutes = selectOrgRoutes(person, orgFallback);
  const enrich = resolveEnrichAction(person, { providerReady: true });
  const role = classifyHeContactRole(person);
  let demoState = enrich.demoState;
  if (role.code === "RELEVANT_COMMERCIAL_ENTRY_POINT" || /INFLUENCER/i.test(role.code)) {
    demoState = demoState === "A" || demoState === "B" || demoState === "C" ? demoState : "E";
    if (!emails.length && !phones.length) demoState = "E";
  }
  return {
    personId: person.personId || null,
    namedContact: text(person.name || person.display_name) || null,
    directEmails: emails,
    directPhones: phones,
    organizationalRoutes: orgRoutes,
    emailLabel: emails.length ? CONTACT_DISPLAY_LABELS.directEmail : null,
    phoneLabel: phones.length ? CONTACT_DISPLAY_LABELS.directPhone : null,
    enrichAction: enrich,
    roleClassification: role,
    demoState,
    surfeDerivedForbidden: true,
  };
}

/**
 * Proposed canonical public-contact upsert (dry-run only). Never includes Surfe fields.
 */
export function proposeHePublicContactUpsert(person = {}, opts = {}) {
  const display = buildHePersonChannelDisplay(person, opts.orgFallback || null);
  if (!display.namedContact) return null;
  const email = display.directEmails[0] ? channelValue(display.directEmails[0]) : null;
  const phone = display.directPhones[0] ? channelValue(display.directPhones[0]) : null;
  const orgEmail = display.organizationalRoutes.find((r) => r.kind === "email");
  const linkedIn = text(person.linkedinUrl);
  if (!email && !phone && !orgEmail && !linkedIn) return null;
  return {
    action: "UPSERT_PROPOSED_DRY_RUN",
    execute: false,
    applyAuthorized: false,
    target: "HI Org Contacts / HI Contact Channels (canonical — no duplicate person rows)",
    dedupeKey: [
      text(display.namedContact).toLowerCase(),
      text(person.organization || person.organizationName).toLowerCase(),
      linkedIn.toLowerCase(),
    ]
      .filter(Boolean)
      .join("|"),
    fields: {
      personName: display.namedContact,
      company: text(person.organization || person.organizationName),
      role: text(person.title || person.role),
      hotelId: opts.hotelId || null,
      hotelName: opts.hotelName || null,
      classification: display.roleClassification.code,
      publicBusinessEmail: email,
      publicBusinessPhone: phone,
      organizationalRouteEmail: orgEmail ? orgEmail.value : null,
      linkedInUrl: linkedIn || null,
      propertyResponsibility: person.propertyResponsibility || "UNKNOWN",
      routeType: email ? "DIRECT" : orgEmail ? "ORGANIZATIONAL" : linkedIn ? "IDENTITY" : "UNKNOWN",
      sourceUrls: [
        ...(person.profileUrls || []).map((p) => p.source_url || p.url).filter(Boolean),
        person.linkedinSourceUrl,
        opts.contactPageUrl || "https://mullencap.com/contact/",
      ].filter(Boolean),
      verificationStatus: "PUBLICLY_VERIFIED",
      lastVerifiedDate: person.lastVerified || new Date().toISOString().slice(0, 10),
      provenance: "PUBLIC_SOURCE",
      surfeDerived: false,
      channelDesignation: email || phone ? "DIRECT_VS_ORG_SEPARATED" : "ORG_OR_IDENTITY_ONLY",
    },
    overwritePolicy: "DO_NOT_OVERWRITE_NEWER_VERIFIED_WITH_STALE_OR_BLANK",
    note: "Phase 3 / HE production Airtable apply remains blocked until founder authorization.",
  };
}

export default {
  HE_PUBLIC_CONTACT_CHANNEL_HIERARCHY_V1,
  CONTACT_DISPLAY_LABELS,
  CONTACT_HIERARCHY_RANK,
  isGenericMailbox,
  looksLikeInventedEmail,
  selectDirectEmails,
  selectDirectPhones,
  selectOrgRoutes,
  resolveEnrichAction,
  classifyHeContactRole,
  buildHePersonChannelDisplay,
  proposeHePublicContactUpsert,
};
