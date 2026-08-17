import fs from "fs";
import crypto from "crypto";
import axios from "axios";
import FormData from "form-data";

const siteId = "68108c29063eeb5d1bd7ae4a";
const filePath = "public/marketing/dealality-old-home-quote-tiles.v20260729a.css";
const fileName = "dealality-old-home-quote-tiles.v20260729a.css";
const fileData = fs.readFileSync(filePath);
const fileHash = crypto.createHash("md5").update(fileData).digest("hex");

const token = process.env.WEBFLOW_API_TOKEN || process.env.WF_TOKEN;
if (!token) {
  console.error("Missing WEBFLOW_API_TOKEN");
  process.exit(1);
}

const createRes = await axios.post(
  `https://api.webflow.com/v2/sites/${siteId}/assets`,
  { fileName, fileHash },
  {
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      accept: "application/json",
    },
    validateStatus: () => true,
  }
);
console.log("create", createRes.status, JSON.stringify(createRes.data).slice(0, 500));
if (createRes.status >= 300) process.exit(1);

const { uploadUrl, uploadDetails, hostedUrl, id } = createRes.data;
const form = new FormData();
for (const [k, v] of Object.entries(uploadDetails)) {
  form.append(k, v);
}
form.append("file", fileData, { filename: fileName, contentType: uploadDetails["Content-Type"] || "text/css" });

const up = await axios.post(uploadUrl, form, {
  headers: form.getHeaders(),
  maxBodyLength: Infinity,
  validateStatus: () => true,
});
console.log(JSON.stringify({ uploadStatus: up.status, id, hostedUrl, fileHash }));
if (up.status !== 201) process.exit(1);
