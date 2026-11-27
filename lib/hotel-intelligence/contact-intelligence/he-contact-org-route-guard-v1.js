/**
 * Contact Intelligence V3 — organization ↔ person route integrity.
 *
 * Never attach an economic-sponsor (or other) organization website/email/phone
 * to a person whose employer organization is different, unless independent
 * evidence on the person record establishes that route.
 */

export const HE_CONTACT_ORG_ROUTE_GUARD_V1 = "he-contact-org-route-guard-v1";

function text(v) {
  return String(v == null ? "" : v)
    .replace(/\s+/g, " ")
    .trim();
}

function normOrg(v) {
  return text(v)
    .toLowerCase()
    .replace(/[.,]/g, " ")
    .replace(/\b(inc|llc|ltd|corp|corporation|company|co|group|international|hotels?|hospitality)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function hostOfUrl(url) {
  const s = text(url);
  if (!s) return null;
  try {
    const u = new URL(/^https?:\/\//i.test(s) ? s : `https://${s}`);
    return u.hostname.replace(/^www\./i, "").toLowerCase() || null;
  } catch {
    return null;
  }
}

/** True when person employer matches the candidate organization name. */
export function personBelongsToOrganization(person = {}, organizationName) {
  const org = normOrg(organizationName);
  if (!org) return false;
  const personOrg = normOrg(
    person.organization || person.organizationName || person.employer || person.company
  );
  if (!personOrg) return false;
  return (
    personOrg === org ||
    personOrg.includes(org) ||
    org.includes(personOrg) ||
    // Marriott International ↔ Marriott Hotels soft match
    (personOrg.includes("marriott") && org.includes("marriott")) ||
    (personOrg.includes("mullen") && org.includes("mullen"))
  );
}

/**
 * Filter organizational routes so sponsor/owner fallback channels do not leak
 * onto people employed by a different organization.
 *
 * @param {object} person
 * @param {Array<object>} personOwnedRoutes — routes already on the person record
 * @param {object|null} ownerOrgFallback — contactableOwnerOrg / sponsor routes
 * @returns {Array<object>}
 */
export function filterOrgRoutesForPerson(person, personOwnedRoutes = [], ownerOrgFallback = null) {
  const owned = Array.isArray(personOwnedRoutes) ? personOwnedRoutes.slice() : [];
  if (!ownerOrgFallback) return owned;

  const ownerName = ownerOrgFallback.name || ownerOrgFallback.organizationName || null;
  if (!personBelongsToOrganization(person, ownerName)) {
    return owned;
  }

  const seen = new Set(
    owned.map((r) => `${r.kind || "web"}:${String(r.value || "").toLowerCase()}`)
  );
  const out = owned.slice();
  function push(val, kind, source) {
    const v = text(val);
    if (!v) return;
    const key = `${kind}:${v.toLowerCase()}`;
    if (seen.has(key)) return;
    seen.add(key);
    out.push({ value: v, kind, source: source || null, fromOwnerOrgFallback: true });
  }

  for (const c of ownerOrgFallback.emails || []) {
    push(c.value || c.email, "email", c.source_url || c.sourceUrl);
  }
  for (const c of ownerOrgFallback.phones || []) {
    push(c.value || c.phone, "phone", c.source_url || c.sourceUrl);
  }
  if (text(ownerOrgFallback.website)) {
    push(ownerOrgFallback.website, "web", ownerOrgFallback.website);
  }
  return out;
}

/**
 * Regression helper: Marriott employees must not receive mullencap.com routes
 * unless their employer is independently Mullen.
 */
export function assertNoCrossOrgWebsiteLeak(person, routes = []) {
  const personOrg = normOrg(person.organization || person.organizationName);
  const isMarriott = personOrg.includes("marriott");
  if (!isMarriott) return { ok: true };
  const leaks = (routes || []).filter((r) => {
    if (String(r.kind || "") !== "web" && !/^https?:/i.test(String(r.value || ""))) {
      return false;
    }
    const host = hostOfUrl(r.value);
    return host && host.includes("mullencap");
  });
  return {
    ok: leaks.length === 0,
    leaks,
    person: text(person.name),
    organization: text(person.organization || person.organizationName),
  };
}

export default {
  HE_CONTACT_ORG_ROUTE_GUARD_V1,
  hostOfUrl,
  personBelongsToOrganization,
  filterOrgRoutesForPerson,
  assertNoCrossOrgWebsiteLeak,
};
