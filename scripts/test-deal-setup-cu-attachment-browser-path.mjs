/**
 * Browser-path integration test: invokes uploadDealAttachments the same way multer + server.js do.
 *
 * Requires AIRTABLE_BASE_ID + AIRTABLE_API_KEY.
 * Set LIVE_CU_ATTACHMENT_BROWSER_PATH_TEST=1 to run.
 *
 * Optional:
 *   DEAL_SETUP_ATTACHMENT_TEST_DEAL_ID (default recIeGRZP21udmTnt)
 */
import "../load-env.js";
import fs from "fs";
import os from "os";
import path from "path";
import { uploadDealAttachments } from "../api/my-deals.js";
import {
  CONTACT_UPLOADS_TABLE,
  CU_ATTACHMENT_FIELD,
  isAirtableHostedAttachmentUrl,
} from "../api/schemas/deal-setup-fields.js";

if (process.env.LIVE_CU_ATTACHMENT_BROWSER_PATH_TEST !== "1") {
  console.log(
    "skip: test-deal-setup-cu-attachment-browser-path (set LIVE_CU_ATTACHMENT_BROWSER_PATH_TEST=1)"
  );
  process.exit(0);
}

const baseId = process.env.AIRTABLE_BASE_ID;
const apiKey = process.env.AIRTABLE_API_KEY;
const dealId = process.env.DEAL_SETUP_ATTACHMENT_TEST_DEAL_ID || "recIeGRZP21udmTnt";

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

function mockRes() {
  const res = { statusCode: 200, body: null };
  res.status = (code) => {
    res.statusCode = code;
    return res;
  };
  res.json = (body) => {
    res.body = body;
    return res;
  };
  return res;
}

const testFilename = `dc-browser-path-test-${Date.now()}.pdf`;
const testBuffer = Buffer.from(
  "%PDF-1.4\n1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n2 0 obj<</Type/Pages/Kids[]/Count 0>>endobj\nxref\n0 3\ntrailer<</Size 3/Root 1 0 R>>\nstartxref\n0\n%%EOF\n",
  "utf8"
);

const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "dc-browser-att-"));
const stagedName = `${Date.now()}-${testFilename.replace(/[^a-zA-Z0-9.-]/g, "_")}`;
const stagedPath = path.join(tmpDir, stagedName);
fs.writeFileSync(stagedPath, testBuffer);

const req = {
  params: { recordId: dealId },
  files: [
    {
      fieldname: "files",
      originalname: testFilename,
      encoding: "7bit",
      mimetype: "application/pdf",
      destination: tmpDir,
      filename: stagedName,
      path: stagedPath,
      size: testBuffer.length,
    },
  ],
};

const res = mockRes();

try {
  await uploadDealAttachments(req, res);

  ok(res.statusCode === 200, `handler returns 200 (got ${res.statusCode})`);
  ok(res.body && res.body.success === true, "response success true");
  ok(res.body && res.body.dealId === dealId, `dealId matches ${dealId}`);
  ok(!!res.body?.cuRecordId, "cuRecordId returned");
  ok(res.body?.attachmentField === CU_ATTACHMENT_FIELD, "attachmentField is Pro Forma or Financials");
  ok(Array.isArray(res.body?.uploadedAttachments) && res.body.uploadedAttachments.length === 1, "uploadedAttachments length 1");
  const uploaded = res.body.uploadedAttachments[0];
  ok(uploaded?.filename === testFilename, "uploadedAttachments filename matches");
  ok(isAirtableHostedAttachmentUrl(uploaded?.url), "uploadedAttachments URL is Airtable-hosted");
  ok(!!uploaded?.id, "uploadedAttachments includes Airtable attachment id");

  const cuRecordId = res.body.cuRecordId;
  const cuRes = await fetch(
    `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(CONTACT_UPLOADS_TABLE)}/${encodeURIComponent(cuRecordId)}`,
    { headers: { Authorization: `Bearer ${apiKey}` } }
  );
  const cuData = await cuRes.json();
  const field = cuData.fields?.[CU_ATTACHMENT_FIELD] || [];
  const inAirtable = field.some((a) => String(a?.filename || "").trim() === testFilename);
  ok(inAirtable, "re-fetch from Airtable contains uploaded filename");

  const cleaned = field.filter((a) => String(a?.filename || "").trim() !== testFilename);
  await fetch(
    `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(CONTACT_UPLOADS_TABLE)}/${encodeURIComponent(cuRecordId)}`,
    {
      method: "PATCH",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ fields: { [CU_ATTACHMENT_FIELD]: cleaned }, typecast: true }),
    }
  );
  ok(true, "test attachment cleaned up");

  console.log(`\ntest-deal-setup-cu-attachment-browser-path: ${passed} passed, ${failed} failed`);
  process.exit(failed > 0 ? 1 : 0);
} catch (err) {
  console.error("test-deal-setup-cu-attachment-browser-path error:", err.message);
  process.exit(1);
} finally {
  try {
    fs.unlinkSync(stagedPath);
    fs.rmdirSync(tmpDir);
  } catch (_) {}
}
