/**
 * Contact Intelligence V1.2 — commercial metrics (counts + percents).
 * Independent precision is always NOT_REVIEWED until human review.
 */

export const COMMERCIAL_METRICS_VERSION = "contact-commercial-metrics-v1.2";

function pct(num, den) {
  if (!den) return { count: num, total: den, percent: null };
  return { count: num, total: den, percent: Number(((num / den) * 100).toFixed(1)) };
}

/**
 * @param {object[]} rows — live discovery results
 */
export function computeCommercialMetrics(rows = []) {
  const n = rows.length;
  const owners = new Map();

  let officialWebsite = 0;
  let hotelPhone = 0;
  let hotelEmail = 0;
  let hotelContact = 0;
  let ownerResolved = 0;
  let ownerSurface = 0;
  let ownerCohortSeed = 0;
  let orgPhoneEmail = 0;
  let orgContactPage = 0;
  let relevantPerson = 0;
  let relevantPersonSeedOnly = 0;
  let personIndirect = 0;
  let personEmail = 0;
  let personPhone = 0;
  let deliverabilityChecked = 0;
  let censusJoined = 0;
  let actionableOwnerCost = 0;
  let actionableOwnerTime = 0;
  let actionableOwners = 0;

  for (const r of rows) {
    const f = r.flags || {};
    if (f.has_official_website_resolved) officialWebsite += 1;
    if (f.has_hotel_phone) hotelPhone += 1;
    if (f.has_hotel_email) hotelEmail += 1;
    if (f.has_hotel_phone_or_email || f.has_hotel_contact_page_fallback) hotelContact += 1;
    if (r.owner_entity_id) ownerResolved += 1;
    if (r.owner_resolution_provenance === "ownership_surface" || f.has_evidenced_current_owner) {
      ownerSurface += 1;
    }
    if (r.owner_resolution_provenance === "development_cohort_seed") ownerCohortSeed += 1;
    if (f.has_org_phone_or_email) orgPhoneEmail += 1;
    if (f.has_org_website_only || f.has_org_contact_page_fallback) orgContactPage += 1;
    if (f.has_relevant_person_evidenced || f.has_relevant_person) relevantPerson += 1;
    if (f.has_relevant_person_seed_only) relevantPersonSeedOnly += 1;
    if (f.has_person_with_indirect_corporate_route) personIndirect += 1;
    if (f.has_direct_person_email) personEmail += 1;
    if (f.has_direct_person_phone) personPhone += 1;
    if (f.deliverability_checked_email) deliverabilityChecked += 1;
    if (f.census_joined) censusJoined += 1;

    const oid = r.owner_entity_id || `unresolved:${r.hotel_id}`;
    if (!owners.has(oid)) {
      owners.set(oid, {
        owner_entity_id: r.owner_entity_id,
        hotels: 0,
        has_org_phone_email: false,
        has_person: false,
        cost_usd: 0,
        time_ms: 0,
      });
    }
    const o = owners.get(oid);
    o.hotels += 1;
    o.has_org_phone_email = o.has_org_phone_email || Boolean(f.has_org_phone_or_email);
    o.has_person =
      o.has_person || Boolean(f.has_relevant_person_evidenced || f.has_relevant_person);
    o.cost_usd += Number(r.cost?.serpapi_usd || 0);
    o.time_ms += Number(r.elapsed_ms || 0);
  }

  const uniqueOwners = [...owners.values()];
  const uniqueOwnerWithOrgRoute = uniqueOwners.filter((o) => o.has_org_phone_email).length;
  const newlyActionable = uniqueOwners.filter((o) => o.has_org_phone_email || o.has_person);
  for (const o of newlyActionable) {
    actionableOwners += 1;
    actionableOwnerCost += o.cost_usd;
    actionableOwnerTime += o.time_ms;
  }

  const hotelPhoneEmailOnly = rows.filter((r) => r.flags?.has_hotel_phone_or_email).length;

  return {
    version: COMMERCIAL_METRICS_VERSION,
    n_hotels: n,
    independent_precision: "NOT_REVIEWED",
    metrics: {
      official_property_website_resolved: pct(officialWebsite, n),
      hotel_phone: pct(hotelPhone, n),
      hotel_email: pct(hotelEmail, n),
      hotel_phone_email_or_contact_page: pct(hotelContact, n),
      hotel_phone_or_email_only: pct(hotelPhoneEmailOnly, n),
      evidenced_current_owner: pct(ownerSurface, n),
      owner_any_provenance: pct(ownerResolved, n),
      owner_cohort_seed_only: pct(ownerCohortSeed, n),
      owner_org_phone_or_email: pct(orgPhoneEmail, n),
      owner_contact_page_or_website_fallback: pct(orgContactPage, n),
      relevant_person_evidenced: pct(relevantPerson, n),
      relevant_person_seed_only: pct(relevantPersonSeedOnly, n),
      person_with_indirect_corporate_route: pct(personIndirect, n),
      direct_person_email: pct(personEmail, n),
      direct_person_phone: pct(personPhone, n),
      census_joined: pct(censusJoined, n),
      unique_owners: uniqueOwners.length,
      unique_owners_with_org_phone_email: uniqueOwnerWithOrgRoute,
      mean_cost_usd_per_newly_actionable_owner:
        actionableOwners > 0
          ? Number((actionableOwnerCost / actionableOwners).toFixed(4))
          : null,
      mean_research_time_ms_per_newly_actionable_owner:
        actionableOwners > 0 ? Math.round(actionableOwnerTime / actionableOwners) : null,
      deliverability_checked_email: pct(deliverabilityChecked, n),
    },
  };
}
