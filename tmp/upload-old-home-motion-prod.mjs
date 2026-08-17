/**
 * Upload Old Home motion prod assets to Webflow CDN.
 */
import fs from "fs";
import crypto from "crypto";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, "..");

function loadEnv() {
  try {
    const raw = fs.readFileSync(path.join(root, ".env"), "utf8");
    for (const line of raw.split(/\r?\n/)) {
      const m = line.match(/^([^#=]+)=(.*)$/);
      if (!m) continue;
      const k = m[1].trim();
      let v = m[2].trim();
      if (
        (v.startsWith('"') && v.endsWith('"')) ||
        (v.startsWith("'") && v.endsWith("'"))
      ) {
        v = v.slice(1, -1);
      }
      if (!process.env[k]) process.env[k] = v;
    }
  } catch (_) {}
}
loadEnv();

const SITE = "68108c29063eeb5d1bd7ae4a";
const TOKEN = process.env.WEBFLOW_TOKEN || process.env.WEBFLOW_API_TOKEN;
if (!TOKEN) {
  console.error("Missing WEBFLOW_TOKEN");
  process.exit(1);
}

const files = [
  {
    local: "public/marketing/old-home-motion.prod.v20260801a.css",
    name: "old-home-motion.prod.v20260801a.css",
    contentType: "text/css",
  },
  {
    local: "public/marketing/old-home-motion.prod.v20260801a.js",
    name: "old-home-motion.prod.v20260801a.js",
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
    "Cache-Control":
      uploadDetails.cacheControl || uploadDetails["Cache-Control"] || "max-age=31536000",
  };
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
    if (map[k] != null) form.append(k, map[k]);
  }
  form.append("file", new Blob([bytes], { type: contentType }), fileName);
  const res = await fetch(uploadUrl, { method: "POST", body: form });
  const text = await res.text();
  return { status: res.status, text: text.slice(0, 400) };
}

function sriSha384(buf) {
  return "sha384-" + crypto.createHash("sha384").update(buf).digest("base64");
}

const results = [];
for (const f of files) {
  const bytes = fs.readFileSync(path.join(root, f.local));
  const md5 = crypto.createHash("md5").update(bytes).digest("hex");
  console.log("create", f.name, "md5", md5, "bytes", bytes.length);
  const meta = await createAsset(f.name, md5);
  const uploadUrl = meta.uploadUrl || meta.upload?.url;
  const details = meta.uploadDetails || meta.upload?.details || meta.uploadDetails;
  if (!uploadUrl || !details) {
    console.log(JSON.stringify(meta, null, 2).slice(0, 1500));
    throw new Error("missing upload info");
  }
  const up = await uploadS3(uploadUrl, details, bytes, f.contentType, f.name);
  console.log("upload", f.name, up.status);
  const hostedUrl = meta.hostedUrl || meta.assetUrl || meta.url;
  const out = {
    name: f.name,
    hostedUrl,
    id: meta.id || meta.assetId,
    sri: sriSha384(bytes),
    md5,
    bytes: bytes.length,
    uploadStatus: up.status,
  };
  results.push(out);
  fs.writeFileSync(
    path.join(root, "tmp", f.name.replace(/\./g, "-") + "-meta.json"),
    JSON.stringify(out, null, 2)
  );
}

fs.writeFileSync(
  path.join(root, "tmp/old-home-motion-prod-v20260801a-upload.json"),
  JSON.stringify(results, null, 2)
);
console.log(JSON.stringify(results, null, 2));
