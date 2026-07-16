/**
 * Record public Terms acceptance from signup into Commercial Acceptances.
 */
import Airtable from "airtable";
import { MAP_COMMERCIAL_ACCEPTANCE as F } from "./commercial-acceptance/field-map.js";

export const CURRENT_TERMS_VERSION =
  process.env.DEALALITY_TERMS_VERSION || "2026-07-16";

const TABLE_ID =
  process.env.COMMERCIAL_ACCEPTANCES_TABLE_ID || "tblznOWoTE0vF1dVG";

/**
 * @param {object} opts
 * @param {string} opts.email
 * @param {string} [opts.firstName]
 * @param {string} [opts.lastName]
 * @param {string} [opts.companyName]
 * @param {string} [opts.memberTypeHint] - company type from form
 * @param {string} [opts.acceptedAtIso]
 * @param {string} [opts.termsVersion]
 * @param {string} [opts.usersRecordId]
 */
export async function recordSignupTermsAcceptance(opts = {}) {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) {
    throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");
  }

  const email = typeof opts.email === "string" ? opts.email.trim().toLowerCase() : "";
  if (!email) throw new Error("email required for terms acceptance");

  const acceptedAt = opts.acceptedAtIso || new Date().toISOString();
  const termsVersion = opts.termsVersion || CURRENT_TERMS_VERSION;
  const firstName = (opts.firstName || "").trim();
  const lastName = (opts.lastName || "").trim();
  const companyName = (opts.companyName || "").trim() || email;
  const acceptedByName = [firstName, lastName].filter(Boolean).join(" ") || email;

  const acceptanceId = `SIGNUP-${acceptedAt.slice(0, 10)}-${email.slice(0, 24)}`.replace(
    /[^A-Za-z0-9\-@._]/g,
    "-"
  );

  const fields = {
    [F.acceptanceId]: acceptanceId.slice(0, 100),
    [F.memberLegalName]: companyName.slice(0, 200),
    [F.acceptanceType]: "Public Terms Only",
    [F.memberType]: mapMemberType(opts.memberTypeHint),
    [F.billingClass]: "non_billing",
    [F.participationLabel]: "Standard",
    [F.termsVersion]: termsVersion,
    [F.scheduleVersion]: "",
    [F.termsUrl]: "/terms",
    [F.acceptedByName]: acceptedByName,
    [F.acceptedByEmail]: email,
    [F.acceptedAt]: acceptedAt,
    [F.acceptanceMethod]: "In-platform click",
    [F.acceptanceEvidenceNotes]: "Signup form: Agree with Terms & Privacy Policy checkbox",
    [F.effectiveDate]: acceptedAt.slice(0, 10),
    [F.acceptanceStatus]: "Accepted",
    [F.platformAccessGranted]: false,
    [F.dealalityContactEmail]: "hello@aohospitalityadvisors.com",
    [F.memberRepresentativeEmail]: email,
    [F.internalNotes]: "Auto-created from /api/signup Terms checkbox",
  };

  if (opts.usersRecordId) {
    fields[F.users] = [opts.usersRecordId];
  }

  // Drop empty strings
  Object.keys(fields).forEach((k) => {
    if (fields[k] === "" || fields[k] == null) delete fields[k];
  });

  const base = new Airtable({ apiKey }).base(baseId);
  const created = await base(TABLE_ID).create([{ fields }], { typecast: true });
  return created[0];
}

function mapMemberType(hint) {
  const h = String(hint || "").toLowerCase();
  if (/owner|developer|investor|private equity|asset manager|broker/.test(h)) {
    return "Owner Member";
  }
  if (/brand|franchise/.test(h)) return "Brand Member";
  if (/management|operator/.test(h)) return "Operator Member";
  if (/advisor|consultant|legal|service|research|lender/.test(h)) return "Advisor";
  return "Other";
}
