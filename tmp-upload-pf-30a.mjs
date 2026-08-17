/**
 * Upload Features v20260730a CSS + JS to Webflow CDN, then print hosted URLs + SRI.
 */
import fs from "fs";
import crypto from "crypto";
import path from "path";
import { fileURLToPath } from "url";

const SITE = "68108c29063eeb5d1bd7ae4a";
const TOKEN = process.env.WEBFLOW_TOKEN || process.env.WEBFLOW_API_TOKEN;
if (!TOKEN) {
  console.error("Missing WEBFLOW_TOKEN / WEBFLOW_API_TOKEN");
  process.exit(1);
}

const files = [
  {
    local: "public/marketing/dealality-old-home-platform-features.v20260730a.css",
    name: "dealality-old-home-platform-features.v20260730a.css",
    contentType: "text/css",
  },
  {
    local: "public/marketing/dealality-old-home-platform-features.v20260730a.js",
    name: "dealality-old-home-platform-features.v20260730a.js",
    contentType: "application/javascript",
  },
];

async function createAsset(fileName, fileHash) {
  const res = await fetch(`https://api.webflow.com/v2/sites/${SITE}/assets`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${TOKEN}`,
      "Content-Type": "application/json",
      accept: "application/json",
    },
    body: JSON.stringify({ fileName, fileHash }),
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`createAsset ${fileName}: ${res.status} ${text}`);
  return JSON.parse(text);
}

async function uploadS3(uploadUrl, uploadDetails, bytes, contentType, fileName) {
  const form = new FormData();
  const order = [
    "acl",
    "bucket",
    "X-Amz-Algorithm",
    "X-Amz-Credential",
    "X-Amz-Date",
    "key",
    "Policy",
    "X-Amz-Signature",
    "success_action_status",
    "Content-Type",
    "Cache-Control",
  ];
  for (const k of order) {
    const map = {
      acl: uploadDetails.acl,
      bucket: uploadDetails.bucket,
      "X-Amz-Algorithm": uploadDetails.xAmzAlgorithm || uploadDetails["X-Amz-Algorithm"],
      "X-Amz-Credential": uploadDetails.xAmzCredential || uploadDetails["X-Amz-Credential"],
      "X-Amz-Date": uploadDetails.xAmzDate || uploadDetails["X-Amz-Date"],
      key: uploadDetails.key,
      Policy: uploadDetails.policy || uploadDetails.Policy,
      "X-Amz-Signature": uploadDetails.xAmzSignature || uploadDetails["X-Amz-Signature"],
      success_action_status: String(
        uploadDetails.successActionStatus || uploadDetails.success_action_status || "201"
      ),
      "Content-Type": uploadDetails.contentType || uploadDetails["Content-Type"] || contentType,
      "Cache-Control": uploadDetails.cacheControl || uploadDetails["Cache-Control"] || "max-age=31536000",
    };
    if (map[k] != null) form.append(k, map[k]);
  }
  // Also append any leftover uploadDetails keys not covered
  for (const [k, v] of Object.entries(uploadDetails)) {
    const aliases = {
      xAmzAlgorithm: "X-Amz-Algorithm",
      xAmzCredential: "X-Amz-Credential",
      xAmzDate: "X-Amz-Date",
      xAmzSignature: "X-Amz-Signature",
      policy: "Policy",
      successActionStatus: "success_action_status",
      contentType: "Content-Type",
      cacheControl: "Cache-Control",
    };
    const formKey = aliases[k] || k;
    if (!order.includes(formKey) && formKey !== "key" && typeof v === "string") {
      // skip unknowns already covered
    }
  }
  form.append("file", new Blob([bytes], { type: contentType }), fileName);
  const res = await fetch(uploadUrl, { method: "POST", body: form });
  const text = await res.text();
  return { status: res.status, text: text.slice(0, 400) };
}

function sriSha384(buf) {
  return "sha384-" + crypto.createHash("sha384").update(buf).digest("base64");
}
function sriSha256(buf) {
  return "sha256-" + crypto.createHash("sha256").update(buf).digest("base64");
}

const results = [];
for (const f of files) {
  const bytes = fs.readFileSync(f.local);
  const md5 = crypto.createHash("md5").update(bytes).digest("hex");
  console.log("create", f.name, "md5", md5, "bytes", bytes.length);
  const meta = await createAsset(f.name, md5);
  console.log("meta keys", Object.keys(meta));
  const uploadUrl = meta.uploadUrl || meta.upload?.url;
  const details = meta.uploadDetails || meta.upload?.details || meta.uploadDetails;
  if (!uploadUrl || !details) {
    console.log(JSON.stringify(meta, null, 2).slice(0, 1500));
    throw new Error("missing upload info");
  }
  const up = await uploadS3(uploadUrl, details, bytes, f.contentType, f.name);
  console.log("upload", f.name, up.status, up.text);
  if (up.status !== 201 && up.status !== 204 && up.status !== 200) {
    process.exit(1);
  }
  const hosted =
    meta.hostedUrl ||
    meta.asset?.hostedUrl ||
    `https://cdn.prod.website-files.com/${SITE}/${details.key.split("/").pop()}`;
  results.push({
    name: f.name,
    local: f.local,
    assetId: meta.id || meta.asset?.id,
    hostedUrl: hosted,
    key: details.key,
    sha384: sriSha384(bytes),
    sha256: sriSha256(bytes),
  });
}

fs.writeFileSync("tmp-pf-30a-upload.json", JSON.stringify(results, null, 2));
console.log(JSON.stringify(results, null, 2));
