/**
 * Seed example rows into Commercial Acceptances (Deal Capture MVP).
 *
 * Usage:
 *   node scripts/seed-commercial-acceptances-examples.mjs
 *   node scripts/seed-commercial-acceptances-examples.mjs --apply
 *
 * Idempotent by Acceptance ID — skips rows that already exist with the same ID.
 */
import "../load-env.js";
import Airtable from "airtable";
import { MAP_COMMERCIAL_ACCEPTANCE as F } from "../lib/commercial-acceptance/field-map.js";

const APPLY = process.argv.includes("--apply");
const TABLE_ID =
  process.env.COMMERCIAL_ACCEPTANCES_TABLE_ID || "tblznOWoTE0vF1dVG";

/** @type {Array<Record<string, unknown>>} */
const EXAMPLES = [
  {
    [F.acceptanceId]: "EXAMPLE-FOUND-001",
    [F.memberLegalName]: "Example Caribbean Hotels LLC",
    [F.memberAccountId]: "demo-owner-001",
    [F.acceptanceType]: "Founding Schedule",
    [F.memberType]: "Owner Member",
    [F.billingClass]: "founding_complimentary",
    [F.participationLabel]: "Founding Participant",
    [F.termsVersion]: "2026-07-16",
    [F.scheduleVersion]: "v1.0",
    [F.scheduleTemplate]: "founding_participant_prefilled",
    [F.termsUrl]: "http://localhost:8080/terms.html",
    [F.acceptedByName]: "Maria Rivera",
    [F.acceptedByEmail]: "maria.rivera@example-caribbean-hotels.com",
    [F.acceptedByTitle]: "Managing Director",
    [F.acceptedAt]: "2026-07-16T14:00:00.000Z",
    [F.acceptanceMethod]: "Email reply",
    [F.acceptanceEvidenceNotes]:
      "Email reply: \"I agree on behalf of Example Caribbean Hotels LLC. Maria Rivera, Managing Director.\"",
    [F.effectiveDate]: "2026-07-16",
    [F.initialTermEndDate]: "2027-07-16",
    [F.foundingEndDate]: "2027-07-16",
    [F.paidTransitionReviewDate]: "2027-06-16",
    [F.autoRenewal]: false,
    [F.nonRenewalNoticeDays]: 30,
    [F.listSubscriptionAnnualUsd]: 0,
    [F.subscriptionAnnualUsd]: 0,
    [F.successFeeWaived]: true,
    [F.upfrontSubmissionFeeUsd]: 0,
    [F.listPerKeyRateUsd]: 100,
    [F.perKeyRateUsd]: 0,
    [F.listMinimumSuccessFeeUsd]: 10000,
    [F.minimumSuccessFeeUsd]: 0,
    [F.loiCommitmentFeePct]: 0.8,
    [F.finalSuccessFeePct]: 0.2,
    [F.tailPeriodMonths]: 0,
    [F.discountApplied]: true,
    [F.discountType]: "Full waiver",
    [F.discountPercent]: 1,
    [F.discountAmountUsd]: 0,
    [F.discountAppliesTo]: ["Subscription", "Success Fee", "Per-key rate", "Minimum fee"],
    [F.discountDuration]: "Entire Initial Term",
    [F.discountCodeLabel]: "FOUNDING-100",
    [F.discountReason]: "Founding participant — complimentary access (demo example)",
    [F.discountApprovedBy]: "Joan Dejarden",
    [F.feeNotes]: "All fees waived during founding period. Demo / example row only.",
    [F.acceptanceStatus]: "Accepted",
    [F.platformAccessGranted]: true,
    [F.accessGrantedAt]: "2026-07-16T15:00:00.000Z",
    [F.grantedBy]: "Ops — Demo Seed",
    [F.internalNotes]: "EXAMPLE ROW — safe to delete. Founding owner path.",
    [F.dealalityContactEmail]: "hello@aohospitalityadvisors.com",
    [F.memberRepresentativeEmail]: "maria.rivera@example-caribbean-hotels.com",
  },
  {
    [F.acceptanceId]: "EXAMPLE-BRAND-DISC-002",
    [F.memberLegalName]: "Example Soft Brand Group Inc.",
    [F.memberAccountId]: "demo-brand-002",
    [F.acceptanceType]: "Standard Schedule",
    [F.memberType]: "Brand Member",
    [F.billingClass]: "standard_brand",
    [F.participationLabel]: "Discounted",
    [F.termsVersion]: "2026-07-16",
    [F.scheduleVersion]: "v1.0",
    [F.scheduleTemplate]: "standard_template",
    [F.termsUrl]: "http://localhost:8080/terms.html",
    [F.acceptedByName]: "James Chen",
    [F.acceptedByEmail]: "james.chen@example-softbrand.com",
    [F.acceptedByTitle]: "VP Development",
    [F.acceptedAt]: "2026-07-10T18:30:00.000Z",
    [F.acceptanceMethod]: "DocuSign",
    [F.acceptanceEvidenceNotes]: "DocuSign envelope ID: DEMO-ENV-002 (example)",
    [F.effectiveDate]: "2026-07-10",
    [F.initialTermEndDate]: "2027-07-10",
    [F.autoRenewal]: true,
    [F.nonRenewalNoticeDays]: 30,
    [F.listSubscriptionAnnualUsd]: 36000,
    [F.subscriptionAnnualUsd]: 27000,
    [F.successFeeWaived]: false,
    [F.discountApplied]: true,
    [F.discountType]: "Percent",
    [F.discountPercent]: 0.25,
    [F.discountAmountUsd]: 9000,
    [F.discountAppliesTo]: ["Subscription", "First year only"],
    [F.discountDuration]: "First year only",
    [F.discountValidThrough]: "2027-07-10",
    [F.discountCodeLabel]: "PILOT-25",
    [F.discountReason]: "Pilot brand — 25% off Growth plan first year (demo example)",
    [F.discountApprovedBy]: "Joan Dejarden",
    [F.feeNotes]:
      "List Growth plan $36,000/year. Net $27,000 year 1. Renewal at list unless new Schedule.",
    [F.acceptanceStatus]: "Accepted",
    [F.platformAccessGranted]: true,
    [F.accessGrantedAt]: "2026-07-10T19:00:00.000Z",
    [F.grantedBy]: "Ops — Demo Seed",
    [F.internalNotes]: "EXAMPLE ROW — safe to delete. Discounted brand subscription.",
    [F.dealalityContactEmail]: "hello@aohospitalityadvisors.com",
    [F.memberRepresentativeEmail]: "james.chen@example-softbrand.com",
  },
  {
    [F.acceptanceId]: "EXAMPLE-OP-STD-003",
    [F.memberLegalName]: "Example Operator Partners Ltd.",
    [F.memberAccountId]: "demo-operator-003",
    [F.acceptanceType]: "Standard Schedule",
    [F.memberType]: "Operator Member",
    [F.billingClass]: "standard_operator",
    [F.participationLabel]: "Standard",
    [F.termsVersion]: "2026-07-16",
    [F.scheduleVersion]: "v1.0",
    [F.scheduleTemplate]: "standard_template",
    [F.termsUrl]: "http://localhost:8080/terms.html",
    [F.acceptedByName]: "Sofia Alvarez",
    [F.acceptedByEmail]: "sofia.alvarez@example-operator.com",
    [F.acceptedByTitle]: "Chief Development Officer",
    [F.acceptedAt]: "2026-06-01T12:00:00.000Z",
    [F.acceptanceMethod]: "PDF signature",
    [F.acceptanceEvidenceNotes]: "Signed PDF Schedule stored in Drive (demo)",
    [F.effectiveDate]: "2026-06-01",
    [F.initialTermEndDate]: "2027-06-01",
    [F.autoRenewal]: true,
    [F.nonRenewalNoticeDays]: 30,
    [F.listSubscriptionAnnualUsd]: 24000,
    [F.subscriptionAnnualUsd]: 24000,
    [F.successFeeWaived]: false,
    [F.discountApplied]: false,
    [F.discountType]: "None",
    [F.discountPercent]: 0,
    [F.discountAppliesTo]: [],
    [F.feeNotes]: "Operator Growth plan — standard pricing, no discount (demo example)",
    [F.acceptanceStatus]: "Accepted",
    [F.platformAccessGranted]: true,
    [F.accessGrantedAt]: "2026-06-01T13:00:00.000Z",
    [F.grantedBy]: "Ops — Demo Seed",
    [F.internalNotes]: "EXAMPLE ROW — safe to delete. Standard operator subscription.",
    [F.dealalityContactEmail]: "hello@aohospitalityadvisors.com",
    [F.memberRepresentativeEmail]: "sofia.alvarez@example-operator.com",
  },
  {
    [F.acceptanceId]: "EXAMPLE-PENDING-004",
    [F.memberLegalName]: "Example Boutique Owner SA",
    [F.memberAccountId]: "demo-owner-pending-004",
    [F.acceptanceType]: "Founding Schedule",
    [F.memberType]: "Owner Member",
    [F.billingClass]: "founding_complimentary",
    [F.participationLabel]: "Founding Participant",
    [F.termsVersion]: "2026-07-16",
    [F.scheduleVersion]: "v1.0",
    [F.scheduleTemplate]: "founding_participant_prefilled",
    [F.termsUrl]: "http://localhost:8080/terms.html",
    [F.acceptedByName]: "Luis Ortega",
    [F.acceptedByEmail]: "luis.ortega@example-boutique.com",
    [F.acceptedByTitle]: "Owner",
    [F.acceptanceMethod]: "Email reply",
    [F.effectiveDate]: "2026-07-20",
    [F.foundingEndDate]: "2027-07-20",
    [F.subscriptionAnnualUsd]: 0,
    [F.successFeeWaived]: true,
    [F.discountApplied]: true,
    [F.discountType]: "Full waiver",
    [F.discountPercent]: 1,
    [F.discountCodeLabel]: "FOUNDING-100",
    [F.discountReason]: "Founding offer sent — awaiting acceptance (demo)",
    [F.acceptanceStatus]: "Pending",
    [F.platformAccessGranted]: false,
    [F.internalNotes]:
      "EXAMPLE ROW — safe to delete. Pending Schedule acceptance; Terms already accepted verbally.",
    [F.dealalityContactEmail]: "hello@aohospitalityadvisors.com",
    [F.memberRepresentativeEmail]: "luis.ortega@example-boutique.com",
  },
];

function scrubEmpty(fields) {
  const out = {};
  for (const [k, v] of Object.entries(fields)) {
    if (v === undefined || v === null) continue;
    if (Array.isArray(v) && v.length === 0) continue;
    out[k] = v;
  }
  return out;
}

async function listExistingIds(table) {
  const ids = new Set();
  await table
    .select({
      fields: [F.acceptanceId],
      pageSize: 100,
    })
    .eachPage((records, next) => {
      for (const r of records) {
        const id = r.get(F.acceptanceId);
        if (id) ids.add(String(id));
      }
      next();
    });
  return ids;
}

async function main() {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");

  const base = new Airtable({ apiKey }).base(baseId);
  const table = base(TABLE_ID);

  console.log(`Mode: ${APPLY ? "apply" : "dry-run"}`);
  console.log(`Table: ${TABLE_ID}`);

  const existing = await listExistingIds(table);
  const toCreate = [];
  const skipped = [];

  for (const row of EXAMPLES) {
    const id = String(row[F.acceptanceId]);
    if (existing.has(id)) {
      skipped.push(id);
      continue;
    }
    toCreate.push(scrubEmpty(row));
  }

  console.log(`Would create / create: ${toCreate.length}`);
  console.log(`Skipped (already exist): ${skipped.length}${skipped.length ? ` — ${skipped.join(", ")}` : ""}`);

  if (!APPLY) {
    for (const row of toCreate) {
      console.log(`  [dry-run] ${row[F.acceptanceId]} — ${row[F.memberLegalName]} (${row[F.acceptanceStatus]})`);
    }
    console.log("\nRe-run with --apply to write example records.");
    return;
  }

  // Airtable create allows max 10 records per request
  const created = [];
  for (let i = 0; i < toCreate.length; i += 10) {
    const chunk = toCreate.slice(i, i + 10).map((fields) => ({ fields }));
    const records = await table.create(chunk, { typecast: true });
    for (const r of records) {
      created.push({ id: r.id, acceptanceId: r.get(F.acceptanceId) });
      console.log(`Created ${r.get(F.acceptanceId)} → ${r.id}`);
    }
  }

  console.log(`\nDone. Created ${created.length} example row(s).`);
  console.log("Look for Acceptance IDs starting with EXAMPLE- in Airtable.");
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
