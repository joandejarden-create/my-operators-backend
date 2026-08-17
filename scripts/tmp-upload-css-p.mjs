import fs from "fs";
import axios from "axios";
import FormData from "form-data";

const details = JSON.parse(fs.readFileSync("tmp-css-upload-details-p.json", "utf8"));
const d = details.uploadDetails;
const fileData = fs.readFileSync("tmp-old-home-dark.v20260728p.css");
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
form.append("file", fileData, {
  filename: "dealality-old-home-dark.v20260728p.css",
  contentType: d.contentType,
});

const res = await axios.post(details.uploadUrl, form, {
  headers: form.getHeaders(),
  maxBodyLength: Infinity,
  validateStatus: () => true,
});
console.log(JSON.stringify({ status: res.status }));
if (res.status !== 201) process.exit(1);
