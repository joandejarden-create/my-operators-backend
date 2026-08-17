import fs from "fs";
import path from "path";

const details = {
  acl: "public-read",
  bucket: "webflow-prod-assets",
  xAmzAlgorithm: "AWS4-HMAC-SHA256",
  xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260729/us-east-1/s3/aws4_request",
  xAmzDate: "20260729T163601Z",
  key: "68108c29063eeb5d1bd7ae4a/6a6a2bf1702e0bc4103b0cf8_old-home-problem-v2.v20260729c.js",
  policy:
    "eyJleHBpcmF0aW9uIjoiMjAyNi0wNy0yOVQxNzozNjowMVoiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoiYXBwbGljYXRpb24vamF2YXNjcmlwdCJ9LHsic3VjY2Vzc19hY3Rpb25fc3RhdHVzIjoiMjAxIn0sWyJzdGFydHMtd2l0aCIsIiRDb250ZW50LVR5cGUiLCJhcHBsaWNhdGlvbi9qYXZhc2NyaXB0Il0sWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsMCwzMTQ1NzI4MF0seyJhY2wiOiJwdWJsaWMtcmVhZCJ9LHsiYnVja2V0Ijoid2ViZmxvdy1wcm9kLWFzc2V0cyJ9LHsiWC1BbXotQWxnb3JpdGhtIjoiQVdTNC1ITUFDLVNIQTI1NiJ9LHsiWC1BbXotQ3JlZGVudGlhbCI6IkFLSUFRTExIV0Q2TUVKR0VUTFNULzIwMjYwNzI5L3VzLWVhc3QtMS9zMy9hd3M0X3JlcXVlc3QifSx7IlgtQW16LURhdGUiOiIyMDI2MDcyOVQxNjM2MDFaIn0seyJrZXkiOiI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvNmE2YTJiZjE3MDJlMGJjNDEwM2IwY2Y4X29sZC1ob21lLXByb2JsZW0tdjIudjIwMjYwNzI5Yy5qcyJ9XX0=",
  xAmzSignature: "581dedfde8ebb7463bf8d0d05187c3a5ee0f9d75a66671c4ee7b8c19195b06de",
  successActionStatus: "201",
  contentType: "application/javascript",
  cacheControl: "max-age=31536000",
};

const file = path.resolve("public/marketing/old-home-problem-v2.v20260729c.js");
const fileBytes = fs.readFileSync(file);
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
form.append("file", new Blob([fileBytes], { type: details.contentType }), path.basename(file));
const res = await fetch("https://webflow-prod-assets.s3.amazonaws.com/", {
  method: "POST",
  body: form,
});
console.log(res.status, (await res.text()).slice(0, 180));
