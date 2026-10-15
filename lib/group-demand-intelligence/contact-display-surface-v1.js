/**
 * Customer-safe contact display surface for list cards + drawers.
 *
 * Hierarchy:
 *  1. Named person + person-specific email/phone
 *  2. Named person + separately labeled organization route
 *  3. Functional team/department email
 *  4. Verified company/office phone
 *  5. Form / intermediary URL
 *
 * Never attributes a generic inbox or switchboard to a named person.
 * Generic usable routes do not mark named-buyer enrichment complete.
 */

const GENERIC_LOCAL =
  /^(info|contact|hello|office|admin|administrator|admins?|support|sales|inquiries|enquiry|enquiries|reception|front.?desk|general|mail|team|hr|press|media|pr|federal|help|customerservice|customer.?service|membership|chapter|.*info|newbiz|events|meetings)$/i;

function text(v) {
  return String(v || "")
    .replace(/\s+/g, " ")
    .trim();
}

function scrubProviderBrandFromEvidence(value) {
  if (value == null || value === "") return value == null ? null : "";
  return String(value)
    .replace(/\bpreviously verified via Surfe\b/gi, "established from prior verified enrichment")
    .replace(/\bverified via Surfe\b/gi, "established from prior verified enrichment")
    .replace(/\bvia Surfe\b/gi, "from prior verified enrichment")
    .replace(/\bSurfe\b/gi, "enrichment provider")
    .replace(/\bWebhound\b/gi, "research provider")
    .replace(/\s+/g, " ")
    .trim();
}

export function isGenericOrganizationEmail(email) {
  const e = text(email).toLowerCase();
  if (!e || !e.includes("@")) return false;
  const local = e.split("@")[0] || "";
  if (GENERIC_LOCAL.test(local)) return true;
  if (/^(no-?reply|donotreply)/i.test(local)) return true;
  return false;
}

/**
 * First.Last@ or firstlast@ patterns that look person-specific (not authority proof).
 */
export function looksLikePersonSpecificEmail(email) {
  const e = text(email).toLowerCase();
  if (!e.includes("@") || isGenericOrganizationEmail(e)) return false;
  const local = e.split("@")[0] || "";
  // Require First.Last (or first_last) — single tokens like kelleninfo/info are not person-specific
  if (local.includes(".")) {
    const parts = local.split(".").filter(Boolean);
    if (parts.length >= 2 && parts.every((p) => /^[a-z][a-z'-]{1,30}$/i.test(p))) {
      return true;
    }
  }
  if (local.includes("_") || local.includes("-")) {
    const parts = local.split(/[_-]/).filter(Boolean);
    if (parts.length >= 2 && parts.every((p) => /^[a-z][a-z']{1,30}$/i.test(p))) {
      return true;
    }
  }
  return false;
}

export function displayNameFromPersonEmail(email) {
  const e = text(email);
  if (!e.includes("@") || isGenericOrganizationEmail(e)) return null;
  const local = e.split("@")[0] || "";
  if (local.includes(".")) {
    if (!looksLikePersonSpecificEmail(email)) return null;
    const parts = local.split(".").filter(Boolean);
    if (parts.length < 2) return null;
    return parts.map((p) => p.charAt(0).toUpperCase() + p.slice(1).toLowerCase()).join(" ");
  }
  // Single-token firstname@ when role/path evidence supports a named person
  if (/^[a-z][a-z'-]{1,24}$/i.test(local)) {
    return local.charAt(0).toUpperCase() + local.slice(1).toLowerCase();
  }
  return null;
}

/**
 * Firstname@ patterns are person-shaped when role / NAMED_DIRECT evidence exists.
 * Not authority proof — only mapping recovery for stored company-contact fallbacks.
 */
function looksLikeFirstnamePersonEmail(email) {
  const e = text(email).toLowerCase();
  if (!e.includes("@") || isGenericOrganizationEmail(e)) return false;
  if (looksLikePersonSpecificEmail(e)) return true;
  const local = e.split("@")[0] || "";
  return /^[a-z][a-z'-]{1,24}$/i.test(local);
}

function firstPhone(...candidates) {
  for (const c of candidates) {
    const t = text(c);
    if (t && t !== "—" && !/^not\s*established$/i.test(t)) return t;
  }
  return null;
}

function firstEmail(...candidates) {
  for (const c of candidates) {
    const t = text(c);
    if (t && t.includes("@")) return t;
  }
  return null;
}

/**
 * Build normalized contact surface from opportunity fields.
 */
export function buildContactDisplaySurface(opportunity = {}) {
  const o = opportunity || {};
  const existing =
    o.primaryContact && typeof o.primaryContact === "object" ? o.primaryContact : null;

  const personName = text(existing?.name || o.primaryContactName || o.contactName || "");
  const personNameOk = personName && personName !== "UNKNOWN" ? personName : null;

  const hasNamedPathEvidence =
    Boolean(text(o.contactRole) || text(o.contactResponsibility) || text(o.primaryContactRole)) ||
    o.commercialContactPath === "NAMED_DIRECT";

  const rawPersonEmail = firstEmail(
    existing?.emailOwnership === "organization_or_department" ? null : existing?.email,
    o.primaryContactEmail,
    // Top-level contactEmail when person-shaped (First.Last or firstname + role evidence)
    looksLikePersonSpecificEmail(o.contactEmail)
      ? o.contactEmail
      : looksLikeFirstnamePersonEmail(o.contactEmail) && (personNameOk || hasNamedPathEvidence)
        ? o.contactEmail
        : null
  );

  const orgEmail = firstEmail(
    o.functionalContactEmail,
    o.organizationContactEmail,
    o.contactChannelSeparation?.organizationRoute,
    isGenericOrganizationEmail(o.contactEmail) ? o.contactEmail : null,
    // If contactEmail is generic and was wrongly the only email
    existing?.emailOwnership === "organization_or_department" ? existing.email : null,
    isGenericOrganizationEmail(o.primaryContactEmail) ? o.primaryContactEmail : null,
    // Company-contact fallback: any remaining stored contactEmail not claimed as person
    o.contactEmail &&
      !looksLikePersonSpecificEmail(o.contactEmail) &&
      !(
        looksLikeFirstnamePersonEmail(o.contactEmail) &&
        (personNameOk || hasNamedPathEvidence)
      )
      ? o.contactEmail
      : null
  );

  // Person email must not equal org inbox
  let personEmail = rawPersonEmail;
  if (personEmail && orgEmail && personEmail.toLowerCase() === orgEmail.toLowerCase()) {
    personEmail = isGenericOrganizationEmail(personEmail) ? null : personEmail;
  }
  if (personEmail && isGenericOrganizationEmail(personEmail)) {
    // Misfiled org inbox — demote
    personEmail = null;
  }

  // Recover display name from person-shaped email when role evidence exists
  // (mapping recovery — not inventing an email pattern).
  let resolvedName = personNameOk;
  if (
    !resolvedName &&
    personEmail &&
    looksLikeFirstnamePersonEmail(personEmail) &&
    hasNamedPathEvidence
  ) {
    resolvedName = displayNameFromPersonEmail(personEmail);
  }

  // Person phone only when tied to a named person — never promote unlabeled company lines
  const personPhone = resolvedName ? firstPhone(existing?.phone, o.primaryContactPhone) : null;

  const companyPhone = firstPhone(
    o.companyPhone,
    o.businessPhone,
    o.officePhone,
    o.localOfficePhone,
    // Top-level contactPhone is company/office when no named person phone
    !personPhone ? o.contactPhone : null,
    !personPhone && existing?.phone && !resolvedName ? existing.phone : null,
    // If person exists but phone is unlabeled and different from person phone — treat as company
    resolvedName && o.contactPhone && o.contactPhone !== personPhone ? o.contactPhone : null
  );

  const contactUrl = text(
    o.contactUrl || o.publicContactPath || o.organizationContactUrl || o.officialContactPath || ""
  );
  const pathOk =
    contactUrl &&
    !/^https?:\/\/[^/]+\/?$/i.test(contactUrl) &&
    !/\/(en\/)?home\/?$/i.test(contactUrl);

  const role = text(
    existing?.role ||
      existing?.title ||
      o.primaryContactRole ||
      o.contactResponsibility ||
      o.contactRole ||
      o.buyerRole ||
      ""
  );

  const phoneTypeLabel =
    existing?.phoneTypeLabel ||
    o.primaryContactPhoneTypeLabel ||
    (companyPhone && !personPhone ? "Company / office phone" : null);

  const companyPhoneLabel = companyPhone
    ? text(o.companyPhoneLabel) ||
      text(o.officePhoneLabel) ||
      (text(o.localConnection) && /washington|dc|district/i.test(o.localConnection)
        ? "Washington office"
        : text(o.localConnection) && /san\s*diego/i.test(o.localConnection)
          ? "San Diego office"
          : "Company / office phone")
    : null;

  // Ask-to-be-directed guidance when only org routes exist OR named person is referral-only
  const suggestedRoles = Array.isArray(o.suggestedContactRoles) ? o.suggestedContactRoles : [];
  const referralHint =
    /intermediary|referral|not (a )?verified|not confirmed|lead to verify/i.test(
      `${role} ${text(o.contactResponsibilityClass)}`
    ) || o.contactResponsibilityClass === "REFERRAL_ENTRY";
  const askFor =
    text(o.contactChannelSeparation?.askToBeDirectedTo) ||
    text(existing?.askToBeDirectedTo) ||
    (resolvedName && referralHint
      ? "Ask to be directed to the person managing staff travel, workplace operations, or internal meetings."
      : null) ||
    (!resolvedName && (orgEmail || companyPhone)
      ? suggestedRoles.length
        ? `Ask for the person managing ${String(suggestedRoles[0]).replace(/_/g, " ")}.`
        : role
          ? `Ask for the person responsible for ${role}.`
          : "Ask for the person managing staff travel or internal meetings."
      : null);

  const primaryContact =
    resolvedName || personEmail || personPhone
      ? {
          name: resolvedName || null,
          role: role || null,
          title: role || null,
          email: personEmail || null,
          emailOwnership: personEmail ? "person_specific" : null,
          phone: personPhone || null,
          phoneTypeLabel:
            personPhone && existing?.phoneTypeLabel
              ? existing.phoneTypeLabel
              : personPhone
                ? "Direct / person phone"
                : null,
          relevanceEvidence: scrubProviderBrandFromEvidence(
            existing?.relevanceEvidence || existing?.whyThisContact || o.whoPrimaryReason || null
          ),
          suggestedContactLabel: resolvedName
            ? `Suggested contact: ${resolvedName}${role ? ` (${role})` : ""}`
            : null,
          askToBeDirectedTo: askFor,
        }
      : null;

  const hasNamedPerson = Boolean(resolvedName);
  const hasPersonChannel = Boolean(personEmail || personPhone);
  const hasOrgRoute = Boolean(orgEmail || companyPhone || pathOk);
  const hasUsableRoute = hasPersonChannel || hasOrgRoute;

  // Referral / intermediary leads are useful but do NOT complete named-buyer enrichment
  const roleBlob = `${role} ${text(o.contactResponsibilityClass)} ${text(o.recommendedAction)}`;
  const isReferralOrIntermediary =
    /intermediary|referral|not (a )?verified|not confirmed|lead to verify|not automatically the hotel buyer/i.test(
      roleBlob
    ) || o.contactResponsibilityClass === "REFERRAL_ENTRY";

  // Named enrichment complete only with person + person-specific email AND
  // not merely a senior/referral intermediary. Generic routes never complete this.
  const namedBuyerEnrichmentComplete = Boolean(
    hasNamedPerson &&
      personEmail &&
      !isReferralOrIntermediary &&
      !isGenericOrganizationEmail(personEmail)
  );

  const cardLines = [];
  if (hasNamedPerson) {
    cardLines.push({ kind: "name", text: resolvedName });
    if (role) cardLines.push({ kind: "role", text: role });
    if (personEmail) cardLines.push({ kind: "person_email", text: personEmail });
    if (personPhone) {
      cardLines.push({
        kind: "person_phone",
        text: personPhone,
        label: primaryContact?.phoneTypeLabel,
      });
    }
    if (orgEmail && orgEmail !== personEmail) {
      cardLines.push({
        kind: "org_email",
        text: orgEmail,
        label: "Organization route",
      });
    }
    if (companyPhone && companyPhone !== personPhone) {
      cardLines.push({
        kind: "company_phone",
        text: companyPhone,
        label: companyPhoneLabel || "Company / office phone",
      });
    }
  } else {
    // No named person — show useful fallbacks, never bare "No primary contact" alone
    if (!hasUsableRoute) {
      cardLines.push({
        kind: "empty",
        text: "Office operations contact not yet identified",
      });
    } else {
      cardLines.push({
        kind: "status",
        text: "Office operations contact not yet identified",
      });
      if (orgEmail) {
        cardLines.push({
          kind: "org_email",
          text: orgEmail,
          label: "Organization email",
        });
      }
      if (companyPhone) {
        cardLines.push({
          kind: "company_phone",
          text: companyPhone,
          label: companyPhoneLabel || "Company / office phone",
        });
      }
      if (pathOk) {
        cardLines.push({ kind: "url", text: contactUrl, label: "Contact page" });
      }
      if (askFor) {
        cardLines.push({ kind: "ask", text: askFor });
      }
    }
  }

  return {
    version: "gdi-contact-display-surface-v1",
    primaryContact,
    functionalContactEmail: orgEmail || null,
    organizationContactEmail: orgEmail || null,
    companyPhone: companyPhone || null,
    companyPhoneLabel: companyPhoneLabel || phoneTypeLabel || null,
    contactUrl: pathOk ? contactUrl : null,
    contactChannelSeparation: {
      suggestedPerson: resolvedName || null,
      personSpecificEmail: personEmail || null,
      personSpecificPhone: personPhone || null,
      organizationRoute: orgEmail || (pathOk ? contactUrl : null) || null,
      companyPhone: companyPhone || null,
      askToBeDirectedTo: askFor,
    },
    cardLines,
    hasNamedPerson,
    hasPersonChannel,
    hasOrgRoute,
    hasUsableRoute,
    namedBuyerEnrichmentComplete,
    // For persistence repair of misfiled top-level fields
    repairedTopLevel: {
      contactEmail: orgEmail || (personEmail && !resolvedName ? null : o.contactEmail) || null,
      // Keep company phone on contactPhone when no person phone
      contactPhone: companyPhone || personPhone || null,
      primaryContactName: resolvedName || null,
      primaryContactEmail: personEmail || null,
      primaryContactPhone: personPhone || null,
    },
  };
}

/**
 * Apply surface onto opportunity for DTO / optional persist repair.
 */
export function applyContactDisplaySurfaceToOpportunity(opportunity = {}, surface = null) {
  const s = surface || buildContactDisplaySurface(opportunity);
  const next = { ...opportunity };
  if (s.primaryContact) {
    next.primaryContact = {
      ...(next.primaryContact || {}),
      ...s.primaryContact,
    };
  }
  next.functionalContactEmail = s.functionalContactEmail || next.functionalContactEmail || null;
  next.organizationContactEmail =
    s.organizationContactEmail || next.organizationContactEmail || null;
  next.companyPhone = s.companyPhone || next.companyPhone || null;
  next.companyPhoneLabel = s.companyPhoneLabel || next.companyPhoneLabel || null;
  next.contactChannelSeparation = {
    ...(next.contactChannelSeparation || {}),
    ...s.contactChannelSeparation,
  };
  next.contactDisplaySurface = {
    version: s.version,
    cardLines: s.cardLines,
    hasNamedPerson: s.hasNamedPerson,
    hasUsableRoute: s.hasUsableRoute,
    namedBuyerEnrichmentComplete: s.namedBuyerEnrichmentComplete,
  };
  // Do not mark named enrichment complete from generic routes
  if (next.namedBuyerEnrichmentComplete === true && !s.namedBuyerEnrichmentComplete) {
    next.namedBuyerEnrichmentComplete = false;
  }
  if (s.namedBuyerEnrichmentComplete) {
    next.namedBuyerEnrichmentComplete = true;
  }
  return next;
}

export default {
  buildContactDisplaySurface,
  applyContactDisplaySurfaceToOpportunity,
  isGenericOrganizationEmail,
  looksLikePersonSpecificEmail,
  displayNameFromPersonEmail,
};
