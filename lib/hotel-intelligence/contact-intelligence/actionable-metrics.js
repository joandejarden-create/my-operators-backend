/**
 * Contact Intelligence V1 — commercial KPIs including ACTIONABLE_CONTACT_RATE.
 */

export const ACTIONABLE_METRICS_VERSION = "contact-actionable-metrics-v1";

function pct(num, den) {
  if (!den) return { count: num, total: den, percent: null };
  return { count: num, total: den, percent: Number(((num / den) * 100).toFixed(1)) };
}

/**
 * Per-hotel flags expected:
 * - usable_hotel_contact
 * - usable_owner_org_path
 * - usable_decision_maker_path
 * - decision_maker_email_verified (optional strict)
 */
export function computeActionableContactMetrics(rows = [], { strict_person_email = false } = {}) {
  const n = rows.length;
  let hotelOk = 0;
  let ownerOrgOk = 0;
  let dmOk = 0;
  let actionable = 0;
  let falsePerson = 0;
  let falseEmail = 0;
  let verifiedEmail = 0;
  let emailAttempts = 0;

  for (const r of rows) {
    const f = r.flags || r;
    const hotel = Boolean(f.usable_hotel_contact);
    const owner = Boolean(f.usable_owner_org_path);
    const dm = Boolean(f.usable_decision_maker_path);
    if (hotel) hotelOk += 1;
    if (owner) ownerOrgOk += 1;
    if (dm) dmOk += 1;

    const personEmailOk = strict_person_email
      ? Boolean(f.decision_maker_email_verified)
      : true;

    if (hotel && owner && personEmailOk) actionable += 1;
    if (f.false_person) falsePerson += 1;
    if (f.false_verified_email) falseEmail += 1;
    if (f.person_email_present) emailAttempts += 1;
    if (f.decision_maker_email_verified) verifiedEmail += 1;
  }

  return {
    version: ACTIONABLE_METRICS_VERSION,
    n_hotels: n,
    /**
     * Primary commercial KPI:
     * hotel has usable hotel contact AND usable owner/development contact path.
     * Variant strict_person_email also requires verified DM email.
     */
    ACTIONABLE_CONTACT_RATE: pct(actionable, n),
    OWNER_CONTACTABLE_RATE: pct(ownerOrgOk, n),
    DECISION_MAKER_CONTACTABLE_RATE: pct(dmOk, n),
    HOTEL_CONTACT_COVERAGE: pct(hotelOk, n),
    VERIFIED_EMAIL_RATE: pct(verifiedEmail, emailAttempts || n),
    FALSE_PERSON_RATE: pct(falsePerson, n),
    FALSE_EMAIL_RATE: pct(falseEmail, n),
    formula: {
      ACTIONABLE_CONTACT_RATE:
        "usable_hotel_contact AND usable_owner_org_path" +
        (strict_person_email ? " AND decision_maker_email_verified" : ""),
      OWNER_CONTACTABLE_RATE: "usable_owner_org_path / hotels",
      DECISION_MAKER_CONTACTABLE_RATE: "usable_decision_maker_path / hotels",
    },
    variants: {
      actionable_with_verified_dm_email: "strict_person_email=true",
      actionable_hotel_plus_org_only: "default (strict_person_email=false)",
    },
  };
}
