import fs from "fs";
import { Blob } from "buffer";

const uploadUrl = "https://webflow-prod-assets.s3.amazonaws.com/";
const d = JSON.parse(fs.readFileSync("tmp-wordmark-upload-details.json", "utf8"));
const bytes = fs.readFileSync("tmp-wordmark-crop.png");

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
form.append(
  "file",
  new Blob([bytes], { type: "image/png" }),
  "dealality-wordmark-nav.png"
);

const res = await fetch(uploadUrl, { method: "POST", body: form });
console.log(res.status);
console.log((await res.text()).slice(0, 400));
