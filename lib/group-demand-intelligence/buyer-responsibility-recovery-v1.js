/**
 * GDI 2.0 Phase 2D — offline buyer-responsibility evidence recovery.
 *
 * Recovers named buyer, affiliation, role, and organizational contact routes
 * from existing opportunity / contact research records. No Surfe/PDL/paid calls.
 */

import { looksLikePersonName } from "./secondary-named-contact-v1.js";
import {
  evaluateBuyerCommercialResponsibility,
  extractBuyerRoleFromOpportunity,
  resolveUsableContactRouteForEligibility,
} from "./commercial-eligibility-contract-v1.js";

export const BUYER_RESPONSIBILITY_RECOVERY_V1 =
  "gdi-buyer-responsibility-recovery-v1";

const NON_COMMERCIAL_ROLE_RE =
  /\b(speaker|keynote|panelist|attendee|delegate|guest|media|press|pr\b|investor relations|ir\b|photographer|emcee)\b/i;

const COMMERCIAL_ROLE_HINT_RE =
  /\b(meeting|meetings|event|events|conference|congress|convention|housing|hotel|venue|planner|planning|facilities|workplace|operations|procurement|sourcing|travel|director|manager|vp|head of|lead|organizer|gerente|directora|director)\b/i;

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function pickName(opportunity = {}) {
  return text(
    opportunity.primaryContact?.name ||
      opportunity.contactName ||
      opportunity.primaryContactName ||
      "",
  );
}

function looksLikeJobTitle(value) {
  const t = text(value);
  if (!t) return false;
  // Reject narrative sentences / evidence blurbs mistaken for titles.
  if (t.length > 80) return false;
  if ((t.match(/\s+/g) || []).length > 8) return false;
  if (/^(public source|extracted from|organization-linked|general organization)/i.test(t)) {
    return false;
  }
  if (/[.!?].+\s/.test(t) && t.length > 40) return false;
  return true;
}

function collectRoleCandidates(opportunity = {}) {
  const rows = [];
  const push = (field, value, confidence = "medium") => {
    const t = text(value);
    if (!looksLikeJobTitle(t)) return;
    rows.push({ field, role: t, confidence });
  };
  push("primaryContact.role", opportunity.primaryContact?.role, "high");
  push("primaryContact.title", opportunity.primaryContact?.title, "high");
  push("primaryContact.contactRole", opportunity.primaryContact?.contactRole, "high");
  push("contactRole", opportunity.contactRole, "high");
  push("contactResponsibility", opportunity.contactResponsibility, "high");
  push("buyerRole", opportunity.buyerRole, "medium");
  push("primaryContactRole", opportunity.primaryContactRole, "medium");
  push("salesBrief.primaryContactRole", opportunity.salesBrief?.primaryContactRole, "medium");
  // relationship / whyThisContact are provenance notes — not titles unless short+commercial
  push("relationshipToEvent", opportunity.relationshipToEvent, "low");
  push("primaryContact.relationshipToEvent", opportunity.primaryContact?.relationshipToEvent, "low");

  for (const c of opportunity.contacts || []) {
    push("contacts[].role", c?.role || c?.title || c?.contactRole, "medium");
  }
  for (const e of opportunity.evidence || []) {
    if (/^(role|title|responsibility|jobTitle|contactRole)$/i.test(String(e.field || ""))) {
      push(`evidence.${e.field}`, e.value || e.extractedText, "medium");
    }
  }
  return rows;
}

function collectRouteCandidates(opportunity = {}) {
  const rows = [];
  const push = (field, value, kind) => {
    const t = text(value);
    if (!t) return;
    rows.push({ field, value: t, kind });
  };
  push("primaryContact.email", opportunity.primaryContact?.email, "person_or_org");
  push("contactEmail", opportunity.contactEmail, "person_or_org");
  push("organizationContactEmail", opportunity.organizationContactEmail, "org_route");
  push("functionalContactEmail", opportunity.functionalContactEmail, "org_route");
  push("companyEmail", opportunity.companyEmail, "org_route");
  push("contactUrl", opportunity.contactUrl, "org_url");
  push("organizationContactUrl", opportunity.organizationContactUrl, "org_url");
  push("publicContactPath", opportunity.publicContactPath, "org_url");
  push("companyPhone", opportunity.companyPhone, "org_phone");
  push("primaryContact.sourceUrl", opportunity.primaryContact?.sourceUrl, "source");
  return rows;
}

/**
 * Recover buyer responsibility from saved evidence.
 */
export function recoverBuyerResponsibility(opportunity = {}) {
  const name = pickName(opportunity);
  const roles = collectRoleCandidates(opportunity);
  const routes = collectRouteCandidates(opportunity);
  const extractedRole = extractBuyerRoleFromOpportunity(
    opportunity.primaryContact || { name },
    opportunity,
  );

  const commercialRoles = roles.filter(
    (r) => COMMERCIAL_ROLE_HINT_RE.test(r.role) && !NON_COMMERCIAL_ROLE_RE.test(r.role),
  );
  const nonCommercial = roles.filter((r) => NON_COMMERCIAL_ROLE_RE.test(r.role));
  const bestRole =
    commercialRoles.find((r) => r.confidence === "high") ||
    commercialRoles[0] ||
    (extractedRole && !NON_COMMERCIAL_ROLE_RE.test(extractedRole)
      ? { field: "extractBuyerRoleFromOpportunity", role: extractedRole, confidence: "high" }
      : null);

  const affiliation = text(
    opportunity.primaryContact?.organization ||
      opportunity.organizationName ||
      opportunity.employer ||
      "",
  );

  const person = name
    ? {
        name,
        role: bestRole?.role || extractedRole || "",
        title: bestRole?.role || extractedRole || "",
        email: text(opportunity.primaryContact?.email || opportunity.contactEmail || ""),
      }
    : null;

  const before = person
    ? evaluateBuyerCommercialResponsibility(person, opportunity)
    : { ok: false, code: "MISSING_NAMED_RELEVANT_BUYER", reason: "no_named_buyer" };

  const overlay = {};
  let appliedMapping = false;

  if (person?.name && bestRole?.role) {
    if (!text(opportunity.primaryContact?.role) || !text(opportunity.contactRole)) {
      overlay.contactRole = bestRole.role;
      overlay.primaryContact = {
        ...(opportunity.primaryContact || {}),
        name: person.name,
        role: text(opportunity.primaryContact?.role) || bestRole.role,
        title: text(opportunity.primaryContact?.title) || bestRole.role,
        email: person.email || opportunity.primaryContact?.email || null,
      };
      appliedMapping = true;
    }
  }

  // Promote org-route fields when only functional mailbox exists on contactEmail
  const orgRoute =
    routes.find((r) => r.kind === "org_route") ||
    routes.find((r) => r.kind === "org_url") ||
    null;
  if (orgRoute && !text(opportunity.organizationContactEmail) && orgRoute.kind === "org_route") {
    overlay.organizationContactEmail = orgRoute.value;
    appliedMapping = true;
  }

  const merged = { ...opportunity, ...overlay };
  if (overlay.primaryContact) {
    merged.primaryContact = overlay.primaryContact;
  }

  const after = person
    ? evaluateBuyerCommercialResponsibility(
        {
          name: person.name,
          role: overlay.primaryContact?.role || person.role,
          title: overlay.primaryContact?.title || person.role,
          email: person.email,
        },
        merged,
      )
    : before;

  const routeBefore = resolveUsableContactRouteForEligibility(opportunity, person || {});
  const routeAfter = resolveUsableContactRouteForEligibility(merged, {
    ...(person || {}),
    role: after.extractedRole || person?.role,
  });

  const missing = [];
  if (!name || !looksLikePersonName(name)) missing.push("named_buyer");
  if (!after.ok) missing.push("buyer_responsibility");
  if (!routeAfter.ok) missing.push("contact_route");
  if (nonCommercial.length && !commercialRoles.length) {
    missing.push("only_non_commercial_roles_found");
  }

  let category = "C_NEW_RESEARCH_REQUIRED";
  if (before.ok && routeBefore.ok) category = "A_ALREADY_SUPPORTED";
  else if (after.ok && appliedMapping) category = "B_DETERMINISTIC_MAPPING_CORRECTED";
  else if (after.ok && !before.ok) category = "A_EXISTING_EVIDENCE_RECOVERED";
  else if (!name && !(opportunity.sources || []).length) {
    category = "E_UNABLE_TO_EVALUATE_MISSING_SOURCE_MATERIAL";
  } else if (nonCommercial.length && !commercialRoles.length) {
    category = "D_COMMERCIALLY_UNSUPPORTED";
  }

  const sourceRefs = [];
  if (opportunity.primaryContact?.sourceUrl) {
    sourceRefs.push({ kind: "contact_source", url: opportunity.primaryContact.sourceUrl });
  }
  for (const s of opportunity.sources || []) {
    const url = text(s?.url || s);
    if (/^https?:\/\//i.test(url)) sourceRefs.push({ kind: "source", url });
  }

  return {
    version: BUYER_RESPONSIBILITY_RECOVERY_V1,
    offlineOnly: true,
    doesNotCallPaidProviders: true,
    doesNotMutateStorage: true,
    secondBuyerRequired: false,
    namedBuyer: name && looksLikePersonName(name) ? name : null,
    // Redact full emails in exports — keep presence + domain only
    affiliation: affiliation || null,
    recoveredRole: bestRole?.role || null,
    recoveredRoleField: bestRole?.field || null,
    nonCommercialRolesFound: nonCommercial.map((r) => r.role).slice(0, 5),
    verificationConfidence: bestRole?.confidence || "low",
    beforeResponsibilityOk: before.ok === true,
    afterResponsibilityOk: after.ok === true,
    beforeRouteOk: routeBefore.ok === true,
    afterRouteOk: routeAfter.ok === true,
    routeReason: routeAfter.reason || routeBefore.reason || null,
    routeLabeling: routeAfter.labeling || null,
    category,
    missing,
    overlay: Object.keys(overlay).length ? overlay : null,
    sourceRefs: sourceRefs.slice(0, 5).map((s) => ({
      kind: s.kind,
      url: s.url,
    })),
    researchContinuationRecommendations: missing.map((m) => {
      if (m === "named_buyer") return "Recover commercially relevant named buyer from official org/event pages.";
      if (m === "buyer_responsibility") {
        return "Recover title/role evidence proving lodging, meetings, facilities, or sourcing responsibility.";
      }
      if (m === "contact_route") {
        return "Recover usable org route (functional mailbox / contact URL / phone) labeled for the named buyer.";
      }
      return `Investigate: ${m}`;
    }),
  };
}

export function applyBuyerResponsibilityRecoveryOverlay(opportunity = {}, recovery = null) {
  const rec = recovery || recoverBuyerResponsibility(opportunity);
  if (!rec.overlay) {
    return { opportunity: { ...opportunity }, recovery: rec, applied: false };
  }
  const next = { ...opportunity, ...rec.overlay };
  if (rec.overlay.primaryContact) {
    next.primaryContact = {
      ...(opportunity.primaryContact || {}),
      ...rec.overlay.primaryContact,
    };
  }
  return { opportunity: next, recovery: rec, applied: true };
}
