import fs from "fs";
import path from "path";

const payload = JSON.parse(fs.readFileSync("tmp-pricing-uploads.json", "utf8"));

async function uploadOne(entry, label) {
  const filePath = entry.file;
  const fileBytes = fs.readFileSync(filePath);
  const form = new FormData();
  const d = entry.uploadDetails;
  form.append("acl", d.acl);
  form.append("bucket", d.bucket);
  form.append("X-Amz-Algorithm", d.xAmzAlgorithm);
  form.append("X-Amz-Credential", d.xAmzCredential);
  form.append("X-Amz-Date", d.xAmzDate);
  form.append("key", d.key);
  form.append("Policy", d.policy);
  form.append("X-Amz-Signature", d.xAmzSignature);
  form.append("success_action_status", d.successActionStatus);
  form.append("Content-Type", d.contentType);
  form.append("Cache-Control", d.cacheControl);
  form.append(
    "file",
    new Blob([fileBytes], { type: d.contentType }),
    path.basename(filePath)
  );
  const res = await fetch(entry.uploadUrl, { method: "POST", body: form });
  console.log(label, res.status, entry.hostedUrl);
  if (res.status !== 201) {
    const text = await res.text();
    throw new Error(label + " upload failed: " + res.status + " " + text.slice(0, 400));
  }
}

await uploadOne(payload.css, "css");
await uploadOne(payload.js, "js");
console.log("ok");
