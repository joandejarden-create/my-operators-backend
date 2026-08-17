import fs from "fs";
import axios from "axios";
import FormData from "form-data";

const uploadUrl = "https://webflow-prod-assets.s3.amazonaws.com/";
const uploadDetails = {
  acl: "public-read",
  bucket: "webflow-prod-assets",
  xAmzAlgorithm: "AWS4-HMAC-SHA256",
  xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260728/us-east-1/s3/aws4_request",
  xAmzDate: "20260728T215442Z",
  key: "68108c29063eeb5d1bd7ae4a/6a6925228176a9babc95f12d_dealality-old-home-hero-video.v20260728.js",
  policy:
    "eyJleHBpcmF0aW9uIjoiMjAyNi0wNy0yOFQyMjo1NDo0MloiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoiYXBwbGljYXRpb24vamF2YXNjcmlwdCJ9LHsic3VjY2Vzc19hY3Rpb25fc3RhdHVzIjoiMjAxIn0sWyJzdGFydHMtd2l0aCIsIiRDb250ZW50LVR5cGUiLCJhcHBsaWNhdGlvbi9qYXZhc2NyaXB0Il0sWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsMCwzMTQ1NzI4MF0seyJhY2wiOiJwdWJsaWMtcmVhZCJ9LHsiYnVja2V0Ijoid2ViZmxvdy1wcm9kLWFzc2V0cyJ9LHsiWC1BbXotQWxnb3JpdGhtIjoiQVdTNC1ITUFDLVNIQTI1NiJ9LHsiWC1BbXotQ3JlZGVudGlhbCI6IkFLSUFRTExIV0Q2TUVKR0VUTFNULzIwMjYwNzI4L3VzLWVhc3QtMS9zMy9hd3M0X3JlcXVlc3QifSx7IlgtQW16LURhdGUiOiIyMDI2MDcyOFQyMTU0NDJaIn0seyJrZXkiOiI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvNmE2OTI1MjI4MTc2YTliYWJjOTVmMTJkX2RlYWxhbGl0eS1vbGQtaG9tZS1oZXJvLXZpZGVvLnYyMDI2MDcyOC5qcyJ9XX0=",
  xAmzSignature:
    "d5684c296fe339ba191fe5237c4e663126fadec132dc1aad0ffced5fe8874f32",
  successActionStatus: "201",
  contentType: "application/javascript",
  cacheControl: "max-age=31536000",
};
const fileName = "dealality-old-home-hero-video.v20260728.js";
const fileData = fs.readFileSync(
  "public/marketing/dealality-old-home-hero-video.js"
);

const form = new FormData();
form.append("acl", uploadDetails.acl);
form.append("bucket", uploadDetails.bucket);
form.append("X-Amz-Algorithm", uploadDetails.xAmzAlgorithm);
form.append("X-Amz-Credential", uploadDetails.xAmzCredential);
form.append("X-Amz-Date", uploadDetails.xAmzDate);
form.append("key", uploadDetails.key);
form.append("Policy", uploadDetails.policy);
form.append("X-Amz-Signature", uploadDetails.xAmzSignature);
form.append("success_action_status", uploadDetails.successActionStatus);
form.append("Content-Type", uploadDetails.contentType);
form.append("Cache-Control", uploadDetails.cacheControl);
form.append("file", fileData, {
  filename: fileName,
  contentType: uploadDetails.contentType,
});

const uploadResponse = await axios.post(uploadUrl, form, {
  headers: form.getHeaders(),
  maxBodyLength: Infinity,
  validateStatus: () => true,
});
console.log("status", uploadResponse.status);
if (uploadResponse.status !== 201) {
  console.log(String(uploadResponse.data).slice(0, 500));
  process.exit(1);
}
console.log("ok");
