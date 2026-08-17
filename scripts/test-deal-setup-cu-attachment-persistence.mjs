/**
 * LIVE Airtable test: upload bytes via content API, verify re-fetch, cleanup.
 *
 * Requires AIRTABLE_BASE_ID + AIRTABLE_API_KEY.
 * Set LIVE_CU_ATTACHMENT_PERSISTENCE_TEST=1 to run (skipped otherwise).
 *
 * Optional env:
 *   DEAL_SETUP_ATTACHMENT_TEST_CU_RECORD_ID — CU record (default recR2r4ZkDM5vTuFY)
 */
import "../load-env.js";
import {
  CONTACT_UPLOADS_TABLE,
  CU_ATTACHMENT_FIELD,
  isAirtableHostedAttachmentUrl,
  cuAttachmentFieldHasFilenames,
  normalizeCuAttachmentItem,
} from "../api/schemas/deal-setup-fields.js";
import {
  uploadFileBytesToAirtable,
  MAX_AIRTABLE_ATTACHMENT_BYTES,
} from "../lib/dealality/airtable-upload-attachment.js";

if (process.env.LIVE_CU_ATTACHMENT_PERSISTENCE_TEST !== "1") {
  console.log(
    "skip: test-deal-setup-cu-attachment-persistence (set LIVE_CU_ATTACHMENT_PERSISTENCE_TEST=1 to run live Airtable upload)"
  );
  process.exit(0);
}

const baseId = process.env.AIRTABLE_BASE_ID;
const apiKey = process.env.AIRTABLE_API_KEY;
const cuRecordId =
  process.env.DEAL_SETUP_ATTACHMENT_TEST_CU_RECORD_ID || "recR2r4ZkDM5vTuFY";

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

const testFilename = `dc-persistence-test-${Date.now()}.pdf`;
const testBuffer = Buffer.from(
  "%PDF-1.4\n1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n2 0 obj<</Type/Pages/Kids[]/Count 0>>endobj\nxref\n0 3\ntrailer<</Size 3/Root 1 0 R>>\nstartxref\n0\n%%EOF\n",
  "utf8"
);

ok(testBuffer.length < MAX_AIRTABLE_ATTACHMENT_BYTES, "test file under Airtable size limit");

async function fetchCuFields() {
  const table = encodeURIComponent(CONTACT_UPLOADS_TABLE);
  const res = await fetch(
    `https://api.airtable.com/v0/${baseId}/${table}/${encodeURIComponent(cuRecordId)}`,
    { headers: { Authorization: `Bearer ${apiKey}` } }
  );
  const data = await res.json();
  if (!res.ok || data.error) {
    throw new Error(data.error?.message || `fetch CU ${res.status}`);
  }
  return data.fields || {};
}

async function patchCuFieldAttachments(attachments) {
  const table = encodeURIComponent(CONTACT_UPLOADS_TABLE);
  const res = await fetch(
    `https://api.airtable.com/v0/${baseId}/${table}/${encodeURIComponent(cuRecordId)}`,
    {
      method: "PATCH",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ fields: { [CU_ATTACHMENT_FIELD]: attachments }, typecast: true }),
    }
  );
  const data = await res.json();
  if (!res.ok || data.error) {
    throw new Error(data.error?.message || `patch CU ${res.status}`);
  }
  return data.fields?.[CU_ATTACHMENT_FIELD] || [];
}

try {
  const before = await fetchCuFields();
  const beforeField = before[CU_ATTACHMENT_FIELD] || [];

  await uploadFileBytesToAirtable({
    baseId,
    recordId: cuRecordId,
    fieldName: CU_ATTACHMENT_FIELD,
    buffer: testBuffer,
    contentType: "application/pdf",
    filename: testFilename,
    apiKey,
  });

  const after = await fetchCuFields();
  const afterField = after[CU_ATTACHMENT_FIELD] || [];

  ok(
    cuAttachmentFieldHasFilenames(afterField, [testFilename]),
    `re-fetch contains uploaded filename ${testFilename}`
  );

  const uploaded = afterField.find(
    (a) => String(a?.filename || "").trim() === testFilename
  );
  ok(Boolean(uploaded), "matched attachment object exists");
  ok(isAirtableHostedAttachmentUrl(uploaded?.url), "attachment URL is Airtable-hosted");

  const normalized = normalizeCuAttachmentItem(uploaded);
  ok(Boolean(normalized?.url && normalized?.filename), "normalizeCuAttachmentItem returns url + filename");
  ok(
    afterField.length >= beforeField.length + 1,
    "attachment count increased after upload"
  );

  const cleaned = afterField.filter((a) => String(a?.filename || "").trim() !== testFilename);
  await patchCuFieldAttachments(cleaned);

  const finalFields = await fetchCuFields();
  const finalField = finalFields[CU_ATTACHMENT_FIELD] || [];
  ok(
    !cuAttachmentFieldHasFilenames(finalField, [testFilename]),
    "test attachment removed after cleanup"
  );

  console.log(`\ntest-deal-setup-cu-attachment-persistence: ${passed} passed, ${failed} failed`);
  process.exit(failed > 0 ? 1 : 0);
} catch (err) {
  console.error("test-deal-setup-cu-attachment-persistence error:", err.message);
  process.exit(1);
}
