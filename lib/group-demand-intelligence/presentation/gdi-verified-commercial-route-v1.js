/**
 * Verified commercial contact-route gate V1.
 *
 * A source URL alone is NOT a verified commercial contact route.
 * Qualifying org routes need a real communication destination tied to
 * event sourcing, accommodation, partnerships, or the relevant commercial activity.
 */

export const GDI_VERIFIED_COMMERCIAL_ROUTE_V1 =
  "gdi-verified-commercial-route-v1";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

const CONTACT_PATH_RE =
  /contact|organiser|organizer|committee|housing|lodging|accommodation|planner|meetings?|groups?|sales|events?|partnerships?|bookings?|rfp|venue-services|destination-services/i;

const GENERIC_LABEL_RE =
  /^Official .+ organizing \/ lodging channel$/i;

/**
 * @param {{ label?: string, url?: string, organizationEmail?: string, organizationPhone?: string }} route
 * @returns {{ ok: boolean, code: string|null, reason: string }}
 */
export function evaluateVerifiedCommercialRoute(route = {}) {
  const label = text(route.label || route.approvedCommercialRoute);
  const url = text(route.url || route.commercialContactRoute || route.officialContactUrl);
  const email = text(route.organizationEmail || route.orgEmail);
  const phone = text(route.organizationPhone || route.companyPhone);

  if (email && /@/.test(email) && !/^info@|^hello@|^webmaster@/i.test(email)) {
    return {
      ok: true,
      code: "ORG_FUNCTIONAL_EMAIL",
      reason: "Verified functional organization email for commercial outreach",
    };
  }
  if (phone && /\d{7,}/.test(phone.replace(/\D/g, ""))) {
    return {
      ok: true,
      code: "ORG_FUNCTIONAL_PHONE",
      reason: "Verified organization phone for commercial outreach",
    };
  }

  if (!url || !/^https?:\/\//i.test(url)) {
    return {
      ok: false,
      code: "MISSING_COMMUNICATION_DESTINATION",
      reason: "No verified URL, email, or phone for organizational route",
    };
  }

  let pathname = "/";
  try {
    pathname = new URL(url).pathname.replace(/\/+$/, "") || "/";
  } catch {
    return {
      ok: false,
      code: "INVALID_URL",
      reason: "Organizational route URL is not parseable",
    };
  }

  const homepageOnly = pathname === "/" || pathname.length < 2;
  if (homepageOnly) {
    return {
      ok: false,
      code: "GENERIC_HOMEPAGE_REJECTED",
      reason: "Company/event homepage alone is not a verified commercial contact route",
    };
  }

  if (!CONTACT_PATH_RE.test(url) && !CONTACT_PATH_RE.test(label)) {
    return {
      ok: false,
      code: "UNRELATED_PAGE_REJECTED",
      reason:
        "URL/label does not evidence sourcing, accommodation, events, partnerships, or a relevant commercial function",
    };
  }

  if (GENERIC_LABEL_RE.test(label) && !CONTACT_PATH_RE.test(url)) {
    return {
      ok: false,
      code: "GENERIC_LABEL_WITHOUT_FUNCTIONAL_URL",
      reason: "Generic organizing/lodging channel label without a functional destination path",
    };
  }

  return {
    ok: true,
    code: "OFFICIAL_FUNCTIONAL_CHANNEL",
    reason: "Official page evidences a relevant commercial/organizing contact function",
  };
}

/**
 * Finalization contact completeness — permanent named-person standard.
 *
 * Customer-facing finalization (STAGED / CURRENT) requires a verified named
 * person. Verified org routes alone are RESEARCH_CONTINUATION unless the
 * caller explicitly sets allowOrgRouteOnly=true (internal research only).
 */
export function evaluateContactCompletenessForFinalization(opportunity = {}, opts = {}) {
  const looksLikePerson =
    typeof opts.looksLikePersonName === "function"
      ? opts.looksLikePersonName
      : (n) => {
          const s = text(n);
          return s.split(/\s+/).filter(Boolean).length >= 2 && s.length >= 5;
        };
  // Default false: org-route-only must not finalize customer packages.
  const allowOrgRouteOnly = opts.allowOrgRouteOnly === true;

  const name = text(
    opportunity.primaryContact?.name ||
      opportunity.contactName ||
      opportunity.namedStakeholders?.[0]?.name,
  );
  const gapLabel =
    /^(contact research required|buyer not yet identified|contact not yet identified|role not yet verified)$/i.test(
      name,
    );
  if (name && !gapLabel && looksLikePerson(name)) {
    return {
      complete: true,
      mode: "NAMED_PERSON",
      name,
      role: text(
        opportunity.primaryContact?.role ||
          opportunity.primaryContact?.title ||
          opportunity.primaryContact?.professionalTitle,
      ),
      organization: text(
        opportunity.primaryContact?.organization ||
          opportunity.organizationName,
      ),
    };
  }

  const routeEval = evaluateVerifiedCommercialRoute({
    label: opportunity.approvedCommercialRoute,
    url: opportunity.commercialContactRoute || opportunity.officialContactUrl,
    organizationEmail: opportunity.organizationEmail,
    organizationPhone: opportunity.organizationPhone,
  });
  if (routeEval.ok && allowOrgRouteOnly) {
    return {
      complete: true,
      mode: "VERIFIED_ORG_ROUTE",
      route: routeEval,
      note: "Internal research continuation only — not customer finalization",
    };
  }

  return {
    complete: false,
    mode: name && !gapLabel ? "CONTACT_INCOMPLETE" : "RESEARCH_CONTINUATION_NO_NAMED_PERSON",
    route: routeEval,
    name: name || null,
    reason: routeEval.ok
      ? "Verified org route is not sufficient for customer finalization without a named person"
      : "No verified named person and no verified commercial org route",
  };
}

export default {
  GDI_VERIFIED_COMMERCIAL_ROUTE_V1,
  evaluateVerifiedCommercialRoute,
  evaluateContactCompletenessForFinalization,
};
