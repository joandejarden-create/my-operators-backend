/**
 * Detail-path contact parity with the approved Sheraton drawer comparison RIGHT.
 *
 * Read-time only: builds `scalingTwoContactPresentation` from stored, verified
 * contact fields already on the opportunity (package / inventory / Airtable /
 * prior research-pipeline writes). Missing channels stay explicitly missing.
 *
 * Never fetches opportunity source URLs.
 * Never calls Surfe.
 * Never invents contacts.
 * Never mutates publication bytes.
 * Never falls back to mock fixture channels.
 */
import { applyUnifiedWhoToContactStandard } from "../unified-who-to-contact-v1.js";

export const GDI_DRAWER_CONTACT_PARITY_PRESENTATION_V1 =
  "gdi-drawer-contact-parity-presentation-v1";

/** Deterministic mock Surfe fixture values — never customer-facing. */
const MOCK_FIXTURE_EMAIL = "alex.buyer@fixture.example";
const MOCK_FIXTURE_PHONE = "+1-202-555-0199";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function isMockFixtureEmail(email) {
  return text(email).toLowerCase() === MOCK_FIXTURE_EMAIL;
}

function isMockFixturePhone(phone) {
  const digits = text(phone).replace(/\D/g, "");
  return (
    text(phone) === MOCK_FIXTURE_PHONE ||
    digits === "12025550199" ||
    digits === "2025550199"
  );
}

/**
 * Strip mock Surfe fixture channels if they leaked onto a customer opportunity.
 * Leaves verified real channels untouched; blanks become explicit absences.
 */
export function stripMockFixtureContactChannels(opportunity = {}) {
  if (!opportunity || typeof opportunity !== "object") return opportunity;
  const next = { ...opportunity };
  const pc =
    next.primaryContact && typeof next.primaryContact === "object"
      ? { ...next.primaryContact }
      : null;
  let changed = false;

  if (pc) {
    if (isMockFixtureEmail(pc.email) || isMockFixtureEmail(pc.workEmail)) {
      pc.email = null;
      pc.workEmail = null;
      changed = true;
    }
    if (isMockFixturePhone(pc.phone) || isMockFixturePhone(pc.directPhone)) {
      pc.phone = null;
      pc.directPhone = null;
      changed = true;
    }
    next.primaryContact = pc;
  }
  if (isMockFixtureEmail(next.contactEmail)) {
    next.contactEmail = null;
    changed = true;
  }
  if (isMockFixturePhone(next.contactPhone)) {
    next.contactPhone = null;
    changed = true;
  }

  if (!changed) return opportunity;
  return {
    ...next,
    mockFixtureChannelsStripped: true,
  };
}

function channelPresence(opportunity = {}) {
  const email = text(
    opportunity.primaryContact?.email ||
      opportunity.primaryContact?.workEmail ||
      opportunity.contactEmail,
  );
  const phone = text(
    opportunity.primaryContact?.phone ||
      opportunity.primaryContact?.directPhone ||
      opportunity.contactPhone,
  );
  const orgEmail = text(
    opportunity.primaryContact?.organizationEmail ||
      opportunity.organizationContactEmail ||
      opportunity.functionalContactEmail,
  );
  const orgPhone = text(
    opportunity.primaryContact?.organizationPhone || opportunity.companyPhone,
  );
  return {
    hasDirectEmail: Boolean(email),
    hasDirectPhone: Boolean(phone),
    hasOrgEmail: Boolean(orgEmail),
    hasOrgPhone: Boolean(orgPhone),
  };
}

/**
 * @param {object} opportunity
 * @param {{ skipRecovery?: boolean }} [opts]
 *   `skipRecovery` retained for API compatibility; recovery is never performed
 *   on this customer detail path.
 * @returns {Promise<object>}
 */
export async function ensureDrawerContactParityPresentation(
  opportunity = {},
  opts = {},
) {
  if (!opportunity || typeof opportunity !== "object") return opportunity;

  // Customer detail path: never outbound-fetch research URLs (SSRF / latency).
  // Public-channel recovery belongs in the controlled research pipeline only.
  void opts;

  const cleaned = stripMockFixtureContactChannels(opportunity);
  const withTwo = applyUnifiedWhoToContactStandard(cleaned, { force: true });
  const presence = channelPresence(withTwo);

  return {
    ...withTwo,
    publicChannelRecoveryMeta: {
      version: GDI_DRAWER_CONTACT_PARITY_PRESENTATION_V1,
      reason: "STORED_DATA_ONLY",
      changed: false,
      outboundFetch: false,
      ...presence,
    },
  };
}
