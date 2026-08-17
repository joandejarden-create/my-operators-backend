import fs from "fs";
import path from "path";

// Exact uploadDetails from Webflow create_asset response (do not hand-edit policy)
const raw = fs.readFileSync("tmp-footer-e-upload-details.json", "utf8");
const d = JSON.parse(raw);
console.log("policy ok", Buffer.from(d.policy, "base64").toString("utf8").includes("application/javascript"));

const file = "public/marketing/old-home-footer-oh.v20260729e.js";
const fileBytes = fs.readFileSync(file);
const form = new FormData();
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
form.append("file", new Blob([fileBytes], { type: d.contentType }), path.basename(file));

const res = await fetch("https://webflow-prod-assets.s3.amazonaws.com/", {
  method: "POST",
  body: form,
});
console.log(res.status, await res.text());
