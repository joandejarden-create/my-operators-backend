import fs from "fs";
import path from "path";
import crypto from "crypto";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, "..");
const filePath = path.join(root, "public/marketing/old-home-fouc-gate.v20260729b.js");
const meta = JSON.parse(fs.readFileSync(path.join(__dirname, "tmp-wf-upload-fouc29b.json"), "utf8"));
const bytes = fs.readFileSync(filePath);
const u = meta.uploadDetails;

const form = new FormData();
form.append("acl", u.acl);
form.append("bucket", u.bucket);
form.append("X-Amz-Algorithm", u.xAmzAlgorithm);
form.append("X-Amz-Credential", u.xAmzCredential);
form.append("X-Amz-Date", u.xAmzDate);
form.append("key", u.key);
form.append("Policy", u.policy);
form.append("X-Amz-Signature", u.xAmzSignature);
form.append("success_action_status", u.successActionStatus);
form.append("Content-Type", u.contentType);
form.append("Cache-Control", u.cacheControl);
form.append("file", new Blob([bytes], { type: u.contentType }), "old-home-fouc-gate.v20260729b.js");

const res = await fetch(meta.uploadUrl, { method: "POST", body: form });
const sha384 = crypto.createHash("sha384").update(bytes).digest("base64");
console.log(JSON.stringify({
  status: res.status,
  hostedUrl: meta.hostedUrl,
  integrity: `sha384-${sha384}`,
  md5: crypto.createHash("md5").update(bytes).digest("hex"),
  bytes: bytes.length,
}, null, 2));
if (res.status !== 201) {
  console.error(await res.text());
  process.exit(1);
}
