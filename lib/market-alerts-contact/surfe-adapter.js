/**
 * Surfe V2 adapter for Market Alerts contact enrichment.
 * Wraps lib/surfe/client.js — never logs API keys or full emails/phones.
 */

import {
  searchSurfePeople,
  startSurfePeopleEnrichment,
  getSurfePeopleEnrichment,
  pollSurfePeopleEnrichment,
  normalizeSurfePerson,
  getSurfeApiKeyFromEnv,
  estimateWorstCaseCredits,
} from "../surfe/client.js";
import { CONTACT_PROVIDER_ID } from "./provider-interface.js";
import { maskEmail, maskPhone, logContactOp } from "./safe-log.js";

function splitName(fullName = "") {
  const parts = String(fullName || "")
    .trim()
    .split(/\s+/)
    .filter(Boolean);
  if (!parts.length) return { firstName: null, lastName: null };
  if (parts.length === 1) return { firstName: parts[0], lastName: null };
  return { firstName: parts[0], lastName: parts.slice(1).join(" ") };
}

function mapSearchPerson(raw) {
  const n = normalizeSurfePerson(raw);
  return {
    provider: CONTACT_PROVIDER_ID.SURFE,
    providerPersonId: n.external_id || null,
    personName: n.full_name,
    firstName: n.first_name,
    lastName: n.last_name,
    jobTitle: n.job_title,
    companyName: n.company_name,
    companyDomain: n.company_domain,
    linkedinUrl: n.linkedin_url,
    country: n.country,
  };
}

function pickWorkEmail(emails = []) {
  const list = Array.isArray(emails) ? emails : [];
  const work = list.find((e) => /work|business|professional/i.test(String(e.type || "")));
  const valid = list.find((e) => String(e.validation_status || "").toUpperCase() === "VALID");
  const chosen = work || valid || list[0] || null;
  if (!chosen?.email) return { email: null, emailStatus: null };
  return {
    email: chosen.email,
    emailStatus: chosen.validation_status || chosen.type || "provider_returned",
  };
}

function mapEnrichedPerson(raw) {
  const n = normalizeSurfePerson(raw);
  const { email, emailStatus } = pickWorkEmail(n.emails);
  const phone = n.mobile_phones?.[0]?.number || null;
  return {
    provider: CONTACT_PROVIDER_ID.SURFE,
    providerPersonId: n.external_id || null,
    personName: n.full_name,
    firstName: n.first_name,
    lastName: n.last_name,
    jobTitle: n.job_title,
    companyName: n.company_name,
    companyDomain: n.company_domain,
    linkedinUrl: n.linkedin_url,
    email,
    emailStatus,
    phone,
    phoneStatus: phone ? "provider_returned" : null,
    enrichmentStatus: n.status || "enriched",
    lastVerifiedAt: new Date().toISOString(),
  };
}

function providerErrorFromResponse(res) {
  if (!res) return { code: "UNKNOWN", message: "no_response" };
  if (res.insufficient_credits) return { code: "INSUFFICIENT_CREDITS", http: res.http_status };
  if (res.rate_limited) return { code: "RATE_LIMITED", http: 429 };
  if (res.http_status === 401) return { code: "AUTH", http: 401 };
  if (res.http_status === 402) return { code: "PAYMENT_REQUIRED", http: 402 };
  if (res.http_status === 403) return { code: "FORBIDDEN", http: 403 };
  if (res.http_status >= 500) return { code: "PROVIDER_5XX", http: res.http_status };
  return {
    code: "PROVIDER_ERROR",
    http: res.http_status,
    message: String(res.payload?.message || res.payload?.code || "error").slice(0, 120),
  };
}

/**
 * @returns {import('./provider-interface.js').ContactProvider}
 */
export function createSurfeContactProvider({ pollOnEnrich = false, maxPollWaitMs = 45000 } = {}) {
  return {
    id: CONTACT_PROVIDER_ID.SURFE,

    async findPeople(query = {}) {
      if (!getSurfeApiKeyFromEnv()) {
        return { ok: false, people: [], error: "MISSING_SURFE_API_KEY" };
      }
      const limit = Math.min(Math.max(Number(query.limit) || 3, 1), 10);
      const named = query.namedPerson || {};
      const nameParts = named.firstName
        ? { firstName: named.firstName, lastName: named.lastName }
        : splitName(named.fullName);

      /** Prefer named person + company over blind role search.
       * Surfe V2 requires companies.names[] and people.jobTitles[] (not .name / .jobTitle).
       */
      const companies = {};
      if (query.companyDomain) {
        companies.domains = [query.companyDomain];
      }
      if (query.companyName) {
        companies.names = [query.companyName];
      }

      const peopleFilters = {};
      if (nameParts.firstName) peopleFilters.firstName = nameParts.firstName;
      if (nameParts.lastName) peopleFilters.lastName = nameParts.lastName;
      if (named.linkedinUrl) peopleFilters.linkedInUrl = named.linkedinUrl;
      if (Array.isArray(query.roles) && query.roles.length && !nameParts.firstName) {
        peopleFilters.jobTitles = query.roles.slice(0, 8);
        peopleFilters.seniorities = ["Founder", "C-Level", "VP", "Director", "Head"];
      }

      const body = {
        limit,
        ...(Object.keys(companies).length ? { companies } : {}),
        ...(Object.keys(peopleFilters).length ? { people: peopleFilters } : {}),
        peoplePerCompany: Math.min(limit, 5),
      };

      logContactOp("surfe_findPeople", {
        company: query.companyName || null,
        hasNamed: Boolean(nameParts.firstName),
        roles: (query.roles || []).slice(0, 3),
        limit,
        searchShape: {
          hasNames: Boolean(companies.names),
          hasDomains: Boolean(companies.domains),
          hasJobTitles: Boolean(peopleFilters.jobTitles),
        },
      });

      try {
        let res = await searchSurfePeople(body);
        // Boutique / thin company-name index: one broader names-only search if filtered query returns empty.
        if (
          res.ok &&
          (!Array.isArray(res.payload?.people) || res.payload.people.length === 0) &&
          companies.names?.length &&
          peopleFilters.jobTitles
        ) {
          const broadBody = {
            limit,
            companies: { names: companies.names },
            peoplePerCompany: Math.min(limit, 5),
          };
          logContactOp("surfe_findPeople_broad_retry", { company: query.companyName || null });
          const broad = await searchSurfePeople(broadBody);
          if (broad.ok) res = broad;
        }
        if (!res.ok) {
          const detail = providerErrorFromResponse(res);
          return {
            ok: false,
            people: [],
            error: detail.code || "PROVIDER_ERROR",
            detail: {
              ...detail,
              httpStatus: res.http_status || res.status || null,
              validationTag: Array.isArray(res.payload?.errors)
                ? String(res.payload.errors[0]?.reason || "").slice(0, 80)
                : null,
            },
          };
        }
        const list = Array.isArray(res.payload?.people)
          ? res.payload.people
          : Array.isArray(res.payload?.results)
            ? res.payload.results
            : [];
        const mappedPeople = list.slice(0, limit).map(mapSearchPerson);
        return {
          ok: true,
          people: mappedPeople,
          creditsUsedEstimate: estimateWorstCaseCredits({ searchResults: mappedPeople.length }).search,
        };
      } catch (err) {
        logContactOp("surfe_findPeople_error", { code: err?.code || "THROW", message: String(err?.message || err).slice(0, 80) });
        return { ok: false, people: [], error: err?.code || "THROW", message: String(err?.message || err).slice(0, 120) };
      }
    },

    async enrichPeople(query = {}) {
      if (!getSurfeApiKeyFromEnv()) {
        return { ok: false, enrichmentId: null, people: [], error: "MISSING_SURFE_API_KEY" };
      }
      const peopleIn = Array.isArray(query.people) ? query.people : [];
      if (!peopleIn.length) {
        return { ok: true, enrichmentId: null, people: [], skipped: true, reason: "no_people" };
      }

      const includeEmail = query.includeEmail !== false;
      const includeMobile = query.includeMobile === true;
      const includeLinkedIn = query.includeLinkedIn !== false;

      const peopleBody = peopleIn.map((p) => {
        const parts = splitName(p.personName || p.fullName || `${p.firstName || ""} ${p.lastName || ""}`);
        return {
          ...(p.providerPersonId ? { externalID: p.providerPersonId } : {}),
          ...(parts.firstName ? { firstName: parts.firstName } : {}),
          ...(parts.lastName ? { lastName: parts.lastName } : {}),
          ...(p.companyName ? { companyName: p.companyName } : {}),
          ...(p.companyDomain ? { companyDomain: p.companyDomain } : {}),
          ...(p.linkedinUrl ? { linkedInUrl: p.linkedinUrl } : {}),
          ...(p.jobTitle ? { jobTitle: p.jobTitle } : {}),
        };
      });

      logContactOp("surfe_enrichPeople_start", {
        count: peopleBody.length,
        includeEmail,
        includeMobile,
        includeLinkedIn,
      });

      try {
        const res = await startSurfePeopleEnrichment({
          people: peopleBody,
          include: {
            email: includeEmail,
            mobile: includeMobile,
            linkedInUrl: includeLinkedIn,
          },
        });
        if (!res.ok) {
          return { ok: false, enrichmentId: null, people: [], error: providerErrorFromResponse(res).code, detail: providerErrorFromResponse(res) };
        }
        const enrichmentId =
          res.payload?.enrichmentID ||
          res.payload?.enrichmentId ||
          res.payload?.id ||
          null;

        if (!enrichmentId) {
          // Some responses may return people synchronously.
          const syncList = Array.isArray(res.payload?.people) ? res.payload.people : [];
          if (syncList.length) {
            return {
              ok: true,
              enrichmentId: null,
              status: "COMPLETED",
              pending: false,
              people: syncList.map(mapEnrichedPerson),
            };
          }
          return { ok: false, enrichmentId: null, people: [], error: "MISSING_ENRICHMENT_ID" };
        }

        if (!pollOnEnrich) {
          return {
            ok: true,
            enrichmentId,
            status: "PENDING",
            pending: true,
            people: [],
          };
        }

        const polled = await pollSurfePeopleEnrichment(enrichmentId, { maxWaitMs: maxPollWaitMs });
        if (polled.timed_out) {
          return { ok: true, enrichmentId, status: "PENDING", pending: true, people: [], timedOut: true };
        }
        if (!polled.ok) {
          return { ok: false, enrichmentId, people: [], error: providerErrorFromResponse(polled).code };
        }
        const status = String(polled.payload?.status || "").toUpperCase();
        const list = Array.isArray(polled.payload?.people) ? polled.payload.people : [];
        return {
          ok: status !== "FAILED",
          enrichmentId,
          status,
          pending: status === "PENDING" || status === "IN_PROGRESS",
          people: list.map(mapEnrichedPerson),
        };
      } catch (err) {
        logContactOp("surfe_enrichPeople_error", { code: err?.code || "THROW", message: String(err?.message || err).slice(0, 80) });
        return { ok: false, enrichmentId: null, people: [], error: err?.code || "THROW" };
      }
    },

    async getEnrichmentStatus(enrichmentId) {
      const res = await getSurfePeopleEnrichment(enrichmentId);
      if (!res.ok) {
        return { ok: false, enrichmentId, error: providerErrorFromResponse(res).code };
      }
      const status = String(res.payload?.status || "").toUpperCase();
      const list = Array.isArray(res.payload?.people) ? res.payload.people : [];
      return {
        ok: true,
        enrichmentId,
        status,
        pending: status === "PENDING" || status === "IN_PROGRESS",
        people: list.map(mapEnrichedPerson),
      };
    },
  };
}

export function summarizeContactForLog(c = {}) {
  return {
    name: c.personName || null,
    company: c.companyName || null,
    email: maskEmail(c.email),
    phone: maskPhone(c.phone),
    confidence: c.matchConfidence || c.confidence || null,
  };
}
