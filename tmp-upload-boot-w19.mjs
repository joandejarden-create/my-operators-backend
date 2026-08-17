import fs from "fs";
import path from "path";

const file = path.resolve("public/marketing/old-home-asset-boot.v20260729w19.js");
const fileBytes = fs.readFileSync(file);
const d = {
  acl: "public-read",
  bucket: "webflow-prod-assets",
  xAmzAlgorithm: "AWS4-HMAC-SHA256",
  xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260729/us-east-1/s3/aws4_request",
  xAmzDate: "20260729T155700Z",
  key: "68108c29063eeb5d1bd7ae4a/6a6a22ccbd88580f5eef5167_old-home-asset-boot.v20260729w19.js",
  policy:
    "eyJleHBpcmF0aW9uIjoiMjAyNi0wNy0yOVQxNjo1NzowMFoiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoiYXBwbGljYXRpb24vamF2YXNjcmlwdCJ9LHsic3VjY2Vzc19hY3Rpb25fc3RhdHVzIjoiMjAxIn0sWyJzdGFydHMtd2l0aCIsIiRDb250ZW50LVR5cGUiLCJhcHBsaWNhdGlvbi9qYXZhc2NyaXB0Il0sWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsMCwzMTQ1NzI4MF0seyJhY2wiOiJwdWJsaWMtcmVhZCJ9LHsiYnVja2V0Ijoid2ViZmxvdy1wcm9kLWFzc2V0cyJ9LHsiWC1BbXotQWxnb3JpdGhtIjoiQVdTNC1ITUFDLVNIQTI1NiJ9LHsiWC1BbXotQ3JlZGVudGlhbCI6IkFLSUFRTExIV0Q2TUVKR0VUTFNULzIwMjYwNzI5L3VzLWVhc3QtMS9zMy9hd3M0X3JlcXVlc3QifSx7IlgtQW16LURhdGUiOiIyMDI2MDcyOVQxNTU3MDBaIn0seyJrZXkiOiI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvNmE2YTIyY2NiZDg4NTgwZjVlZWY1MTY3X29sZC1ob21lLWFzc2V0LWJvb3QudjIwMjYwNzI5dzE5LmpzIn1dfQ==",
  xAmzSignature: "7b06e728c292f1b0f26e85a7392557a78635ae4c759b7dfb450f2a1aae22ba81",
  successActionStatus: "201",
  contentType: "application/javascript",
  cacheControl: "max-age=31536000",
};

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
