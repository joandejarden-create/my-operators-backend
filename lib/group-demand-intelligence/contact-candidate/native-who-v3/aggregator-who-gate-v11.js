/**
 * Aggregator / conference-listing WHO gate (V11).
 *
 * Aggregator pages may discover event existence / point to official sources.
 * They may NOT independently establish WHO without authoritative corroboration.
 *
 * No hotel/event/domain blacklists — structural host + domain-class rules.
 */

import {
  classifyDomain,
  hostnameOf,
  isOfficialClass,
  isSocialOrAggregator,
  DOMAIN_CLASS,
} from "./official-domain.js";

/**
 * Structural conference-listing / multi-event aggregator hosts.
 * Pattern-based (not a single-domain denylist).
 */
const CONFERENCE_AGGREGATOR_HOST =
  /(?:^|\.)(?:10times|eventbrite|allconferences?|conferencealerts?|conferenceseries|conferenceindex|clocate|waset|paper\.edu|academic-conferences)\b|(?:^|\.)[a-z0-9-]*conferences?\.(?:com|net|org)$/i;

/**
 * @param {string} url
 */
export function isConferenceListingAggregator(url) {
  const host = hostnameOf(url) || "";
  if (!host) return false;
  if (isSocialOrAggregator(url)) return true;
  return CONFERENCE_AGGREGATOR_HOST.test(host);
}

/**
 * Source-to-event relation for WHO confirmation.
 *
 * @param {{ sourceUrl?: string, domainClass?: string, onOfficialDomain?: boolean, officialCorroboration?: boolean, organization?: string, opportunityName?: string, eventSourceUrls?: string[] }} candidate
 * @param {{ organization?: string, opportunityName?: string, eventSourceUrls?: string[], officialHosts?: string[] }} ctx
 */
export function sourceToEventRelationGate(candidate = {}, ctx = {}) {
  const url = candidate.sourceUrl || "";
  const host = hostnameOf(url);
  const domainClass =
    candidate.domainClass ||
    classifyDomain(host, {
      organization: ctx.organization || candidate.organization,
      opportunityName: ctx.opportunityName || candidate.opportunityName,
      eventSourceUrls: ctx.eventSourceUrls || candidate.eventSourceUrls || [],
    });

  const aggregator = isConferenceListingAggregator(url);
  const official =
    candidate.onOfficialDomain === true ||
    isOfficialClass(domainClass) ||
    Boolean(candidate.officialCorroboration);

  // Official / operator / housing sources: pass
  if (official && !aggregator) {
    return {
      reject: false,
      gate: null,
      relation: "AUTHORITATIVE_SOURCE",
      domainClass,
      aggregator: false,
    };
  }

  // Aggregator may pass only with separate official corroboration
  if (aggregator) {
    if (candidate.officialCorroboration) {
      return {
        reject: false,
        gate: null,
        relation: "AGGREGATOR_WITH_OFFICIAL_CORROBORATION",
        domainClass,
        aggregator: true,
      };
    }
    return {
      reject: true,
      gate: "AGGREGATOR_WHO_WITHOUT_CORROBORATION",
      relation: "AGGREGATOR_ONLY",
      domainClass: domainClass || DOMAIN_CLASS.UNKNOWN,
      aggregator: true,
      reasons: [
        "aggregator_may_discover_event",
        "aggregator_cannot_independently_establish_who",
      ],
    };
  }

  // Unknown non-aggregator: soft allow (existing role/evidence gates decide)
  return {
    reject: false,
    gate: null,
    relation: "UNKNOWN_SOURCE",
    domainClass,
    aggregator: false,
  };
}

/**
 * Combined WHO admission after person-boundary: entity PERSON + source relation.
 */
export function aggregatorWhoGateV11(candidate = {}, ctx = {}) {
  return sourceToEventRelationGate(candidate, ctx);
}
