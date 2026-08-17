import fs from "fs";
import path from "path";

const item = {
  file: "public/marketing/dealality-old-home-platform-features.v20260730a.js",
  uploadUrl: "https://webflow-prod-assets.s3.amazonaws.com/",
  uploadDetails: {
    acl: "public-read",
    bucket: "webflow-prod-assets",
    xAmzAlgorithm: "AWS4-HMAC-SHA256",
    xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260729/us-east-1/s3/aws4_request",
    xAmzDate: "20260729T230715Z",
    key: "68108c29063eeb5d1bd7ae4a/6a6a87a362cddcf7dd1152a4_dealality-old-home-platform-features.v20260730a.js",
    policy:
      "eyJleHBpcmF0aW9uIjoiMjAyNi0wNy0zMFQwMDowNzoxNVoiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoiYXBwbGljYXRpb24vamF2YXNjcmlwdCJ9LHsic3VjY2Vzc19hY3Rpb25fc3RhdHVzIjoiMjAxIn0sWyJzdGFydHMtd2l0aCIsIiRDb250ZW50LVR5cGUiLCJhcHBsaWNhdGlvbi9qYXZhc2NyaXB0Il0sWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsMCwzMTQ1NzI4MF0seyJhY2wiOiJwdWJsaWMtcmVhZCJ9LHsiYnVja2V0Ijoid2ViZmxvdy1wcm9kLWFzc2V0cyJ9LHsiWC1BbXotQWxnb3JpdGhtIjoiQVdTNC1ITUFDLVNIQTI1NiJ9LHsiWC1BbXotQ3JlZGVudGlhbCI6IkFLSUFRTExIV0Q2TUVKR0VUTFNULzIwMjYwNzI5L3VzLWVhc3QtMS9zMy9hd3M0X3JlcXVlc3QifSx7IlgtQW16LURhdGUiOiIyMDI2MDcyOVQyMzA3MTVaIn0seyJrZXkiOiI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvNmE2YTg3YTM2MmNkZGNmN2RkMTE1MmE0X2RlYWxhbGl0eS1vbGQtaG9tZS1wbGF0Zm9ybS1mZWF0dXJlcy52MjAyNjA3MzBhLmpzIn1dfQ==",
    xAmzSignature: "c86a6be9674589efd609dfd9f87b467bf8edbdbadbb635a4e5c43542c1622b7f",
    successActionStatus: "201",
    contentType: "application/javascript",
    cacheControl: "max-age=31536000",
  },
};

const fileBytes = fs.readFileSync(item.file);
const form = new FormData();
const d = item.uploadDetails;
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
form.append("file", new Blob([fileBytes], { type: d.contentType }), path.basename(item.file));
const res = await fetch(item.uploadUrl, { method: "POST", body: form });
const text = await res.text();
console.log(path.basename(item.file), res.status, text.slice(0, 300));
if (res.status !== 201) process.exit(1);
console.log("js ok");
