import fs from "fs";
import axios from "axios";
import FormData from "form-data";

const uploadUrl = process.env.WF_UPLOAD_URL;
const details = JSON.parse(fs.readFileSync(process.env.WF_UPLOAD_DETAILS_FILE, "utf8"));
const fileName = process.env.WF_FILE_NAME;
const filePath = process.env.WF_FILE_PATH;
const fileData = fs.readFileSync(filePath);

const form = new FormData();
form.append("acl", details.acl);
form.append("bucket", details.bucket);
form.append("X-Amz-Algorithm", details.xAmzAlgorithm);
form.append("X-Amz-Credential", details.xAmzCredential);
form.append("X-Amz-Date", details.xAmzDate);
form.append("key", details.key);
form.append("Policy", details.policy);
form.append("X-Amz-Signature", details.xAmzSignature);
form.append("success_action_status", details.successActionStatus);
form.append("Content-Type", details.contentType);
form.append("Cache-Control", details.cacheControl);
form.append("file", fileData, { filename: fileName, contentType: details.contentType });

const res = await axios.post(uploadUrl, form, {
  headers: form.getHeaders(),
  maxBodyLength: Infinity,
  validateStatus: () => true,
});
console.log(JSON.stringify({ status: res.status, data: typeof res.data === "string" ? res.data.slice(0, 200) : res.data }));
if (res.status !== 201) process.exit(1);
