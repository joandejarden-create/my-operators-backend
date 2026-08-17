/**
 * Upload a file to Webflow Assets via create_asset + S3 POST.
 * Usage: node scripts/upload-webflow-asset.mjs <filePath> [displayName]
 * Prints JSON: { fileName, fileHash, ... } — then expects env WEBFLOW_UPLOAD_META from MCP create_asset.
 *
 * This script only computes MD5 and can complete S3 upload when given meta JSON:
 * node scripts/upload-webflow-asset.mjs --complete meta.json filePath
 */
import fs from "fs";
import crypto from "crypto";
import path from "path";

const args = process.argv.slice(2);
if (args[0] === "--hash") {
  const filePath = args[1];
  const buf = fs.readFileSync(filePath);
  const md5 = crypto.createHash("md5").update(buf).digest("hex");
  console.log(
    JSON.stringify({
      filePath,
      fileName: path.basename(filePath),
      fileHash: md5,
      size: buf.length,
      contentType: filePath.endsWith(".css")
        ? "text/css"
        : filePath.endsWith(".js")
          ? "application/javascript"
          : "application/octet-stream",
    })
  );
  process.exit(0);
}

if (args[0] === "--complete") {
  const metaPath = args[1];
  const filePath = args[2];
  const raw = fs.readFileSync(metaPath, "utf8").replace(/^\uFEFF/, "");
  const meta = JSON.parse(raw);
  const uploadUrl = meta.uploadUrl || meta.upload_url;
  const details = meta.uploadDetails || meta.upload_details || meta;
  const buf = fs.readFileSync(filePath);
  const form = new FormData();
  const map = {
    acl: details.acl,
    bucket: details.bucket,
    "X-Amz-Algorithm": details.xAmzAlgorithm || details["X-Amz-Algorithm"],
    "X-Amz-Credential": details.xAmzCredential || details["X-Amz-Credential"],
    "X-Amz-Date": details.xAmzDate || details["X-Amz-Date"],
    key: details.key,
    Policy: details.policy || details.Policy,
    "X-Amz-Signature": details.xAmzSignature || details["X-Amz-Signature"],
    success_action_status:
      details.successActionStatus || details.success_action_status || "201",
    "Content-Type": details.contentType || details["Content-Type"],
    "Cache-Control": details.cacheControl || details["Cache-Control"] || "public, max-age=31536000",
  };
  for (const [k, v] of Object.entries(map)) {
    if (v != null) form.append(k, String(v));
  }
  const fileName = path.basename(filePath);
  form.append("file", new Blob([buf], { type: map["Content-Type"] }), fileName);
  const res = await fetch(uploadUrl, { method: "POST", body: form });
  const text = await res.text();
  console.log(JSON.stringify({ status: res.status, body: text.slice(0, 500), hostedUrlHint: details.key }));
  process.exit(res.status === 201 ? 0 : 1);
}

console.error("Usage: --hash <file> | --complete <meta.json> <file>");
process.exit(1);
