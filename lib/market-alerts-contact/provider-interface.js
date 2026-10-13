/**
 * Market Alerts contact provider interface (Phase A).
 * Surfe is one adapter; keep Market Alerts free of hard-coded Surfe calls.
 */

export const CONTACT_PROVIDER_ID = Object.freeze({
  SURFE: "surfe",
});

/**
 * @typedef {object} FindPeopleQuery
 * @property {string} companyName
 * @property {string} [companyDomain]
 * @property {string[]} [roles] — preferred job title strings
 * @property {string} [location]
 * @property {number} [limit]
 * @property {{ firstName?: string, lastName?: string, fullName?: string, linkedinUrl?: string }} [namedPerson]
 */

/**
 * @typedef {object} EnrichPeopleQuery
 * @property {object[]} people
 * @property {boolean} [includeEmail]
 * @property {boolean} [includeMobile]
 * @property {boolean} [includeLinkedIn]
 */

/**
 * @typedef {object} ContactProvider
 * @property {string} id
 * @property {(q: FindPeopleQuery) => Promise<{ ok: boolean, people: object[], error?: string, creditsUsedEstimate?: number }>} findPeople
 * @property {(q: EnrichPeopleQuery) => Promise<{ ok: boolean, enrichmentId?: string|null, status?: string, people?: object[], error?: string, pending?: boolean }>} enrichPeople
 * @property {(enrichmentId: string) => Promise<object>} [getEnrichmentStatus]
 */

export function createNoopContactProvider() {
  return {
    id: "noop",
    async findPeople() {
      return { ok: true, people: [], skipped: true, reason: "noop_provider" };
    },
    async enrichPeople() {
      return { ok: true, enrichmentId: null, people: [], skipped: true, reason: "noop_provider" };
    },
  };
}
