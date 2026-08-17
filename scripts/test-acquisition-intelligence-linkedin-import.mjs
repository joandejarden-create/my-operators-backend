/**
 * Acquisition Intelligence Stage 1 — LinkedIn Connections CSV tests.
 *
 *   node scripts/test-acquisition-intelligence-linkedin-import.mjs
 *   npm run test:acquisition-intelligence-linkedin-import
 */
import test from "node:test";
import assert from "node:assert/strict";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  parseLinkedInConnectionsCsv,
  detectLinkedInConnectionsHeader,
  csvTextToMatrix,
  parseConnectedOn,
} from "../lib/acquisition-intelligence/linkedin-connections-parse.js";
import { buildLinkedInConnectionsPreview } from "../lib/acquisition-intelligence/linkedin-connections-preview.js";
import {
  normalizeLinkedInProfileUrl,
  buildRelationshipDedupeKey,
  buildPersonIdentityKey,
} from "../lib/acquisition-intelligence/linkedin-identity.js";
import {
  buildAcquisitionImportPlan,
  mergeNonBlankFields,
  buildRelationshipFieldsFromRow,
  buildContactFieldsFromLinkedInRow,
  CONTACT_LINKEDIN_REFRESH_FIELDS,
} from "../lib/acquisition-intelligence/import-plan.js";
import {
  buildAcquisitionRelationshipCoreFields,
  buildAcquisitionImportBatchCoreFields,
  getAcquisitionIntelligenceSchemaSummary,
  classifyFieldEnsureAction,
} from "../lib/acquisition-intelligence/schema-spec.js";
import {
  MAP_ACQUISITION_RELATIONSHIP as R,
  SOURCE_LINKEDIN_CONNECTIONS_EXPORT,
  VAL_RELATIONSHIP_STRENGTH,
} from "../lib/acquisition-intelligence/field-map.js";
import { MAP_GTM_CONTACT } from "../lib/gtm-owner-target/contact-field-map.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const FIXTURE = path.join(
  ROOT,
  "fixtures",
  "acquisition-intelligence",
  "linkedin-connections-synthetic.csv"
);
const REIMPORT = path.join(
  ROOT,
  "fixtures",
  "acquisition-intelligence",
  "linkedin-connections-reimport-changed.csv"
);

test("detects LinkedIn header below metadata notes rows", () => {
  const text = fs.readFileSync(FIXTURE, "utf8");
  const matrix = csvTextToMatrix(text);
  const detected = detectLinkedInConnectionsHeader(matrix);
  assert.ok(detected);
  assert.ok(detected.headerRowIndex >= 2, "header should sit below LinkedIn notes metadata");
  assert.ok(detected.headerMap["First Name"] != null);
  assert.ok(detected.headerMap["Last Name"] != null);
});

test("rejects non-LinkedIn CSV", () => {
  const parsed = parseLinkedInConnectionsCsv("Name,Email\nAda,ada@example.com\n");
  assert.equal(parsed.ok, false);
  assert.equal(parsed.error, "not_linkedin_connections_export");
});

test("parses synthetic fixture with missing optional fields and duplicates", () => {
  const text = fs.readFileSync(FIXTURE, "utf8");
  const parsed = parseLinkedInConnectionsCsv(text, { fileName: "Connections.csv" });
  assert.equal(parsed.ok, true);
  assert.ok(parsed.metadataRowCount >= 2);

  const unique = parsed.rows.filter((r) => !r.duplicateOfRow);
  const dups = parsed.rows.filter((r) => r.duplicateOfRow);
  assert.ok(unique.length >= 7);
  assert.ok(dups.length >= 1, "duplicate LinkedIn URL should be flagged");

  const missingEmail = unique.find((r) => r.displayName === "Ada Owner");
  assert.ok(missingEmail);
  assert.equal(missingEmail.email, "");
  assert.ok(missingEmail.linkedInUrl.includes("ada-owner"));

  const missingCompany = unique.find((r) => r.displayName === "Cara NoCompany");
  assert.equal(missingCompany.company, "");

  const missingPosition = unique.find((r) => r.displayName === "Dan NoTitle");
  assert.equal(missingPosition.position, "");

  const badDate = unique.find((r) => r.displayName === "Frank Collision" && r.company === "Same Name LLC");
  assert.equal(badDate.connectedOnInvalid, true);
  assert.equal(badDate.connectedOn, null);

  assert.ok(parsed.invalidRows.some((r) => r.reason === "missing_name"));
});

test("preview stats cover required Stage 1 metrics", () => {
  const text = fs.readFileSync(FIXTURE, "utf8");
  const parsed = parseLinkedInConnectionsCsv(text, { fileName: "Connections.csv" });
  const preview = buildLinkedInConnectionsPreview(parsed);
  assert.equal(preview.ok, true);
  assert.equal(preview.validation.pass, true);
  assert.equal(preview.stats.fileName, "Connections.csv");
  assert.ok(preview.stats.connectionsDetected > 0);
  assert.ok(preview.stats.recordsWithCompany > 0);
  assert.ok(preview.stats.recordsWithPosition > 0);
  assert.ok(preview.stats.recordsWithLinkedInUrl > 0);
  assert.ok(preview.stats.recordsWithEmail >= 1);
  assert.ok(preview.stats.potentialDuplicates >= 1);
  assert.ok(preview.stats.invalidRows >= 1);
  assert.ok(preview.stats.earliestConnection);
  assert.ok(preview.stats.latestConnection);
});

test("LinkedIn URL normalization collapses variants", () => {
  assert.equal(
    normalizeLinkedInProfileUrl("https://linkedin.com/in/hank-valid/"),
    "https://www.linkedin.com/in/hank-valid"
  );
  assert.equal(
    normalizeLinkedInProfileUrl("www.linkedin.com/in/Ada-Owner?trk=x"),
    "https://www.linkedin.com/in/ada-owner"
  );
  assert.equal(normalizeLinkedInProfileUrl("https://example.com/in/nope"), "");
});

test("parseConnectedOn handles LinkedIn formats and malformed values", () => {
  assert.equal(parseConnectedOn("15 Jan 2019").iso, "2019-01-15");
  assert.equal(parseConnectedOn("2020-02-01").iso, "2020-02-01");
  assert.equal(parseConnectedOn("not-a-date").invalid, true);
  assert.equal(parseConnectedOn("").iso, null);
});

test("relationship strength defaults to Unknown — never inferred from Connected On", () => {
  const fields = buildRelationshipFieldsFromRow(
    {
      firstName: "Ada",
      lastName: "Owner",
      displayName: "Ada Owner",
      linkedInUrl: "https://www.linkedin.com/in/ada-owner",
      company: "Harbor",
      position: "Partner",
      connectedOn: "2019-01-15",
    },
    "mem_test_user"
  );
  assert.equal(fields[R.relationshipStrength], "Unknown");
  assert.equal(fields[R.importSource], SOURCE_LINKEDIN_CONNECTIONS_EXPORT);
  assert.ok(VAL_RELATIONSHIP_STRENGTH.includes(fields[R.relationshipStrength]));
});

test("mergeNonBlankFields never overwrites with blanks", () => {
  const existing = {
    [MAP_GTM_CONTACT.title]: "Managing Partner",
    [MAP_GTM_CONTACT.company]: "Harbor Hotels Group",
    [MAP_GTM_CONTACT.email]: "kept@example.com",
  };
  const incoming = {
    [MAP_GTM_CONTACT.title]: "Principal",
    [MAP_GTM_CONTACT.company]: "",
    [MAP_GTM_CONTACT.email]: "",
  };
  const patch = mergeNonBlankFields(existing, incoming, CONTACT_LINKEDIN_REFRESH_FIELDS);
  assert.equal(patch[MAP_GTM_CONTACT.title], "Principal");
  assert.equal(patch[MAP_GTM_CONTACT.company], undefined);
  assert.equal(patch[MAP_GTM_CONTACT.email], undefined);
});

test("re-import updates LinkedIn-derived fields and preserves manual relationship strength", () => {
  const userId = "mem_joan_test";
  const firstText = fs.readFileSync(FIXTURE, "utf8");
  const firstParsed = parseLinkedInConnectionsCsv(firstText);
  const firstUnique = firstParsed.rows.filter((r) => !r.duplicateOfRow);
  const firstPlan = buildAcquisitionImportPlan(firstUnique, userId, {
    sourceFileName: "Connections.csv",
  });
  assert.ok(firstPlan.summary.createRelationships >= 7);

  // Simulate existing relationship for Ada with manual strength
  const ada = firstUnique.find((r) => r.displayName === "Ada Owner");
  const adaKey = buildRelationshipDedupeKey(userId, ada);
  const existingRelFields = buildRelationshipFieldsFromRow(ada, userId, {
    sourceFileName: "Connections.csv",
  });
  existingRelFields[R.relationshipStrength] = "4 — Know Well";
  existingRelFields[R.notes] = "Met at ALIS";

  const reText = fs.readFileSync(REIMPORT, "utf8");
  const reParsed = parseLinkedInConnectionsCsv(reText);
  const reUnique = reParsed.rows.filter((r) => !r.duplicateOfRow);

  const existingContactsByLinkedIn = new Map();
  const contactFields = buildContactFieldsFromLinkedInRow(ada, "Connections.csv");
  existingContactsByLinkedIn.set(ada.linkedInUrl, {
    id: "recContactAda",
    fields: contactFields,
  });

  const rePlan = buildAcquisitionImportPlan(reUnique, userId, {
    existingRelationshipsByDedupeKey: new Map([
      [adaKey, { id: "recRelAda", fields: existingRelFields }],
    ]),
    existingContactsByLinkedIn,
    existingContactsByDedupeKey: new Map(),
    sourceFileName: "Connections-reimport.csv",
  });

  const adaUpdate = rePlan.toUpdateRelationships.find((u) => u.id === "recRelAda");
  assert.ok(adaUpdate, "Ada should update on re-import");
  assert.equal(adaUpdate.patch[R.position], "Principal");
  assert.equal(adaUpdate.patch[R.company], "Harbor Hotels Group International");
  // Manual fields must not be in LinkedIn refresh patch keys as overwrites to Unknown
  assert.equal(adaUpdate.patch[R.relationshipStrength], undefined);
  assert.equal(adaUpdate.patch[R.notes], undefined);
});

test("user-scoped dedupe keys differ per user for same person", () => {
  const row = {
    linkedInUrl: "https://www.linkedin.com/in/ada-owner",
    firstName: "Ada",
    lastName: "Owner",
    company: "Harbor",
  };
  assert.notEqual(
    buildRelationshipDedupeKey("user_a", row),
    buildRelationshipDedupeKey("user_b", row)
  );
  assert.equal(buildPersonIdentityKey(row), "li:https://www.linkedin.com/in/ada-owner");
});

test("schema summary and field specs are Stage 1 scoped", () => {
  const summary = getAcquisitionIntelligenceSchemaSummary();
  assert.ok(summary.tables.includes("Acquisition Network Relationships"));
  assert.ok(summary.doesNotInclude.includes("automated_outreach"));
  assert.ok(summary.doesNotInclude.includes("linkedin_scraping"));
  assert.ok(buildAcquisitionRelationshipCoreFields().length > 10);
  assert.ok(buildAcquisitionImportBatchCoreFields().length > 8);

  const skip = classifyFieldEnsureAction(
    { type: "singleLineText" },
    { name: "X", type: "singleLineText" }
  );
  assert.equal(skip.action, "skip");
});
