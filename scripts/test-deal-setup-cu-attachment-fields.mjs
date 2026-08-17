/**
 * Verify Contact & Uploads attachment field mapping against live Airtable schema.
 */
import "../load-env.js";
import {
  CONTACT_UPLOADS_TABLE,
  CU_ATTACHMENT_AIRTABLE_FIELDS,
  CU_ATTACHMENT_FIELD,
  CU_ATTACHMENT_FORM_KEY,
  aggregateCuAttachmentsFromFields,
  isAirtableHostedAttachmentUrl,
  cuAttachmentFieldHasFilenames,
} from "../api/schemas/deal-setup-fields.js";
import { extractUploadResponseAttachments } from "../lib/dealality/airtable-upload-attachment.js";

const baseId = process.env.AIRTABLE_BASE_ID;
const apiKey = process.env.AIRTABLE_API_KEY;
if (!baseId || !apiKey) {
  console.error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY required");
  process.exit(1);
}

let passed = 0;
let failed = 0;

function ok(cond, msg) {
  if (cond) {
    passed += 1;
    console.log("ok:", msg);
  } else {
    failed += 1;
    console.error("FAIL:", msg);
  }
}

const metaRes = await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, {
  headers: { Authorization: `Bearer ${apiKey}` },
});
const meta = await metaRes.json();
if (!metaRes.ok) {
  console.error("Meta API failed:", meta.error || meta);
  process.exit(1);
}

const table = (meta.tables || []).find((t) => t.name === CONTACT_UPLOADS_TABLE);
ok(Boolean(table), `table "${CONTACT_UPLOADS_TABLE}" exists`);

const fieldNames = new Set((table?.fields || []).map((f) => f.name));
const fieldsByName = new Map((table?.fields || []).map((f) => [f.name, f]));
const attachmentFields = (table?.fields || [])
  .filter((f) => f.type === "multipleAttachments" || f.type === "singleAttachment")
  .map((f) => f.name);

ok(!fieldNames.has(CU_ATTACHMENT_FORM_KEY), `form key "${CU_ATTACHMENT_FORM_KEY}" is not an Airtable column`);

for (const name of CU_ATTACHMENT_AIRTABLE_FIELDS) {
  const field = fieldsByName.get(name);
  const isAttachment =
    field && (field.type === "multipleAttachments" || field.type === "singleAttachment");
  ok(isAttachment, `CU attachment column exists and is attachment type: ${JSON.stringify(name)}`);
}

for (const name of attachmentFields) {
  ok(
    CU_ATTACHMENT_AIRTABLE_FIELDS.includes(name),
    `Airtable attachment column included in read aggregate map: ${JSON.stringify(name)}`
  );
}

ok(
  CU_ATTACHMENT_AIRTABLE_FIELDS.length === attachmentFields.length,
  `read aggregate lists ${CU_ATTACHMENT_AIRTABLE_FIELDS.length} attachment column(s) (Airtable has ${attachmentFields.length})`
);

ok(fieldNames.has(CU_ATTACHMENT_FIELD), `generic write field exists: ${JSON.stringify(CU_ATTACHMENT_FIELD)}`);
ok(
  CU_ATTACHMENT_AIRTABLE_FIELDS.includes(CU_ATTACHMENT_FIELD),
  "generic write field is in CU_ATTACHMENT_AIRTABLE_FIELDS"
);

const sample = {};
for (const name of CU_ATTACHMENT_AIRTABLE_FIELDS) {
  sample[name] = [{ url: "https://example.com/" + encodeURIComponent(name) + ".pdf", filename: "a.pdf" }];
}
const aggregated = aggregateCuAttachmentsFromFields(sample);
ok(aggregated.length === CU_ATTACHMENT_AIRTABLE_FIELDS.length, "aggregateCuAttachmentsFromFields merges all columns");

ok(isAirtableHostedAttachmentUrl("https://v5.airtableusercontent.com/foo"), "isAirtableHostedAttachmentUrl accepts Airtable CDN");
ok(!isAirtableHostedAttachmentUrl("http://localhost:8080/api/my-deals/recX/attachments/a.pdf"), "isAirtableHostedAttachmentUrl rejects proxy URLs");

const uploadResponseSample = {
  fields: {
    fldTest123: [{ id: "att1", url: "https://v5.airtableusercontent.com/x", filename: "a.pdf" }],
  },
};
ok(
  extractUploadResponseAttachments(uploadResponseSample, CU_ATTACHMENT_FIELD)?.length === 1,
  "extractUploadResponseAttachments finds attachments keyed by field ID"
);

ok(cuAttachmentFieldHasFilenames([{ filename: "a.pdf" }], ["a.pdf"]), "cuAttachmentFieldHasFilenames matches filenames");

console.log(`\ntest-deal-setup-cu-attachment-fields: ${passed} passed, ${failed} failed`);
process.exit(failed > 0 ? 1 : 0);
