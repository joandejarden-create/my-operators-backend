/**
 * Unit tests for Pilot Target List mail-merge export logic (no Airtable writes).
 */
import assert from "node:assert/strict";
import {
  MAP_PILOT_TARGET_LIST,
} from "../lib/gtm-owner-target/pilot-target-list-field-map.js";
import {
  recordToMailMergeRow,
  splitContactName,
  rowsToCsv,
  buildFieldMappingReport,
} from "../lib/gtm-owner-target/pilot-target-list-outreach.js";

function testSplitName() {
  assert.deepEqual(splitContactName("Joan Delgado"), {
    firstName: "Joan",
    lastName: "Delgado",
    fullName: "Joan Delgado",
  });
  assert.deepEqual(splitContactName("Madonna"), {
    firstName: "Madonna",
    lastName: "",
    fullName: "Madonna",
  });
}

function approvedReadyFields(overrides = {}) {
  return {
    [MAP_PILOT_TARGET_LIST.name]: "Jane Owner",
    [MAP_PILOT_TARGET_LIST.email]: "jane@example.com",
    [MAP_PILOT_TARGET_LIST.emailSubject]: "Pilot invite",
    [MAP_PILOT_TARGET_LIST.finalApprovedEmail]: "Hello Jane,\n\nWould love your feedback.",
    [MAP_PILOT_TARGET_LIST.outreachStatus]: "Approved",
    [MAP_PILOT_TARGET_LIST.readyForMailMerge]: true,
    [MAP_PILOT_TARGET_LIST.doNotContact]: false,
    [MAP_PILOT_TARGET_LIST.sendChannel]: "Email",
    [MAP_PILOT_TARGET_LIST.mailMergeBatch]: "Pilot Wave 1",
    [MAP_PILOT_TARGET_LIST.role]: "CEO",
    [MAP_PILOT_TARGET_LIST.linkedInUrl]: "https://linkedin.com/in/jane",
    ...overrides,
  };
}

function testExportHappyPath() {
  const result = recordToMailMergeRow(
    "recTEST",
    approvedReadyFields(),
    new Map([["recCO", "Example Hotels"]]),
    { batch: "Pilot Wave 1", status: "Approved", channel: "Email" }
  );
  assert.equal(result.skip, false);
  assert.equal(result.row.email, "jane@example.com");
  assert.equal(result.row.subject, "Pilot invite");
  assert.match(result.row.message, /Would love your feedback/);
}

function testSkipsDoNotContact() {
  const result = recordToMailMergeRow(
    "recDNQ",
    approvedReadyFields({ [MAP_PILOT_TARGET_LIST.doNotContact]: true }),
    new Map(),
    { status: "Approved", channel: "Email" }
  );
  assert.equal(result.skip, true);
  assert.equal(result.reason, "do_not_contact");
}

function testSkipsNotReadyForMailMerge() {
  const result = recordToMailMergeRow(
    "recNR",
    approvedReadyFields({ [MAP_PILOT_TARGET_LIST.readyForMailMerge]: false }),
    new Map(),
    { status: "Approved", channel: "Email" }
  );
  assert.equal(result.skip, true);
  assert.equal(result.reason, "not_ready_for_mail_merge");
}

function testSkipsIncompleteApprovedRow() {
  const result = recordToMailMergeRow(
    "recBAD",
    approvedReadyFields({ [MAP_PILOT_TARGET_LIST.emailSubject]: "" }),
    new Map(),
    { status: "Approved", channel: "Email" }
  );
  assert.equal(result.skip, true);
  assert.equal(result.reason, "incomplete");
  assert.ok(result.warnings.includes("missing_email_subject"));
}

function testSkipsMissingEmailForEmailChannel() {
  const result = recordToMailMergeRow(
    "recNOEMAIL",
    approvedReadyFields({ [MAP_PILOT_TARGET_LIST.email]: "" }),
    new Map(),
    { status: "Approved", channel: "Email" }
  );
  assert.equal(result.skip, true);
  assert.ok(result.warnings.includes("missing_email"));
}

function testCsvHasHeaderAndRow() {
  const csv = rowsToCsv([
    {
      email: "a@b.com",
      first_name: "A",
      last_name: "B",
      full_name: "A B",
      company: "Co",
      role: "CEO",
      subject: "Hi",
      message: "Body",
      linkedin_url: "",
      send_channel: "Email",
      mail_merge_batch: "Pilot Wave 1",
      airtable_record_id: "rec1",
    },
  ]);
  assert.match(csv, /^email,first_name/);
  assert.match(csv, /a@b\.com/);
}

function testFieldMappingAvoidsDuplicates() {
  const existing = new Set([
    MAP_PILOT_TARGET_LIST.outreachMessageAngle,
    MAP_PILOT_TARGET_LIST.lastContactDate,
    MAP_PILOT_TARGET_LIST.priority,
  ]);
  const mapping = buildFieldMappingReport(existing);
  const messageAngle = mapping.find((r) => r.requiredField === "Message Angle (taxonomy)");
  assert.equal(messageAngle.addNewField, "No");
  const lastContact = mapping.find((r) => r.requiredField === "Last Contacted Date");
  assert.equal(lastContact.addNewField, "No");
}

function run() {
  testSplitName();
  testExportHappyPath();
  testSkipsDoNotContact();
  testSkipsNotReadyForMailMerge();
  testSkipsIncompleteApprovedRow();
  testSkipsMissingEmailForEmailChannel();
  testCsvHasHeaderAndRow();
  testFieldMappingAvoidsDuplicates();
  console.log("test-owner-targets-outreach-export: all passed");
}

run();
