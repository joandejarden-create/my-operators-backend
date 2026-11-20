/**
 * Surfe Search People readiness — non-chargeable account checks + schema notes.
 * Does NOT execute POST /v2/people/search (counts toward quota / may charge search credits).
 */
import {
  describeSurfeKeyPresence,
  getSurfeCredits,
  SURFE_PEOPLE_BASE_URL,
  SURFE_CREDIT_COSTS,
  searchSurfePeople,
} from "../../surfe/client.js";

export const GDI_SURFE_SEARCH_PEOPLE_READINESS_V1 =
  "gdi-surfe-search-people-readiness-v1";

/**
 * Documented Search People v2 capabilities (developers.surfe.com/public-009-search-people-v2).
 * Important: people filters have NO firstName/lastName — primarily org + role discovery.
 */
export const SURFE_SEARCH_PEOPLE_SCHEMA = Object.freeze({
  method: "POST",
  path: "/v2/people/search",
  baseUrl: SURFE_PEOPLE_BASE_URL,
  requestFields: {
    limit: "1–200 results per page",
    pageToken: "pagination",
    peoplePerCompany: "1–40 when searching by domain/name only",
    companies: [
      "domains",
      "names",
      "industries",
      "countries",
      "employeeCount",
      "keywords",
      "technologies",
    ],
    people: [
      "jobTitles",
      "exactJobTitles",
      "seniorities",
      "departments",
      "departmentClassifications",
      "countries",
      "previousCompanyDomains",
      "jobChangePeriodInDays",
    ],
    peopleFiltersMissing: [
      "firstName",
      "lastName",
      "fullName",
      "linkedInUrl",
      "email",
    ],
  },
  responsePersonFields: [
    "firstName",
    "lastName",
    "jobTitle",
    "companyName",
    "companyDomain",
    "linkedInUrl",
    "country",
    "seniorities",
    "departments",
    "externalID",
  ],
  matchingMode: "ORGANIZATION_AND_ROLE_DISCOVERY",
  exactPersonMatching:
    "NOT_SUPPORTED_VIA_SEARCH — use people/enrich with name+company(+domain|+verified LinkedIn)",
  credits: {
    withSearchCreditsEnabled:
      "ceil(results_returned / 10) ICP search credits per call",
    withoutSearchCredits: "daily people-search quota (docs: 200 results/day)",
    docs: SURFE_CREDIT_COSTS.docs.credits_quotas,
  },
  retentionAndUse:
    "Search returns identity/profile fields (incl. LinkedIn URL) without email/phone enrich. Email/phone require separate enrich. Surfe-derived PII remains session/provider-restricted per Dealality Surfe policy — do not persist as public-verified.",
});

/**
 * Non-chargeable readiness probe. Never calls search unless authorizePaidSearch=true
 * AND creditCeiling explicitly set (still not used by default Phase 2).
 */
export async function assessSurfeSearchPeopleReadiness(args = {}) {
  const key = describeSurfeKeyPresence();
  const out = {
    version: GDI_SURFE_SEARCH_PEOPLE_READINESS_V1,
    generatedAt: new Date().toISOString(),
    keyPresent: key.present === true,
    schema: SURFE_SEARCH_PEOPLE_SCHEMA,
    credits: null,
    searchExecuted: false,
    paidSearchAuthorized: false,
    recommendation: null,
  };

  if (!key.present) {
    out.recommendation =
      "SURFE_API_KEY missing — Search People unavailable until configured.";
    return out;
  }

  try {
    const credits = await getSurfeCredits();
    out.credits = {
      ok: credits.ok === true,
      http_status: credits.http_status,
      totalEmail: credits.payload?.totalEmail ?? null,
      totalMobile: credits.payload?.totalMobile ?? null,
      totalSearch: credits.payload?.totalSearch ?? null,
    };
  } catch (err) {
    out.credits = {
      ok: false,
      error: String(err?.message || err).slice(0, 160),
    };
  }

  const searchCredits = Number(out.credits?.totalSearch || 0);
  out.recommendation =
    searchCredits > 0
      ? "Search credits available — still require founder ceiling before any paid Search People calls. Prefer for org-role alternative discovery, not exact-person LinkedIn matching."
      : "totalSearch=0 — Search People uses daily quota (docs 200 results/day) when search credits disabled. Do not burn quota without founder ceiling. Exact-person matching remains Enrich + public LinkedIn discovery.";

  // Explicitly refuse unpaid/unauthorized live search in this readiness helper.
  if (args.authorizePaidSearch === true && Number(args.creditCeiling || 0) > 0) {
    out.paidSearchAuthorized = true;
    out.note =
      "Caller authorized a ceiling, but readiness helper still does not execute search — invoke searchSurfePeople from a dedicated gated script.";
  }

  out.searchFnAvailable = typeof searchSurfePeople === "function";
  return out;
}

export default {
  GDI_SURFE_SEARCH_PEOPLE_READINESS_V1,
  SURFE_SEARCH_PEOPLE_SCHEMA,
  assessSurfeSearchPeopleReadiness,
};
