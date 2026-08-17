import fs from "fs";
import axios from "axios";
import FormData from "form-data";

const details = JSON.parse(fs.readFileSync("tmp-css-upload-details.json", "utf8"));
const uploadUrl = details.uploadUrl;
const d = details.uploadDetails;
const fileName = "dealality-old-home-dark.v20260728o.css";
const fileData = fs.readFileSync("tmp-old-home-dark.v20260728o.css");

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
form.append("file", fileData, { filename: fileName, contentType: d.contentType });

const res = await axios.post(uploadUrl, form, {
  headers: form.getHeaders(),
  maxBodyLength: Infinity,
  validateStatus: () => true,
});
console.log(JSON.stringify({ status: res.status, data: typeof res.data === "string" ? res.data.slice(0, 200) : res.data }));
if (res.status !== 201) process.exit(1);
