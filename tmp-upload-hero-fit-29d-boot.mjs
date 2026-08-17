import fs from "fs";
import path from "path";
import crypto from "crypto";

const details = {
  acl: "public-read",
  bucket: "webflow-prod-assets",
  xAmzAlgorithm: "AWS4-HMAC-SHA256",
  xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260729/us-east-1/s3/aws4_request",
  xAmzDate: "20260729T210925Z",
  key: "68108c29063eeb5d1bd7ae4a/6a6a6c050ab86c971afcef84_old-home-hero-fit-boot.v20260729d.js",
  policy:
    "eyJleHBpcmF0aW9uIjoiMjAyNi0wNy0yOVQyMjowOToyNVoiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoiYXBwbGljYXRpb24vamF2YXNjcmlwdCJ9LHsic3VjY2Vzc19hY3Rpb25fc3RhdHVzIjoiMjAxIn0sWyJzdGFydHMtd2l0aCIsIiRDb250ZW50LVR5cGUiLCJhcHBsaWNhdGlvbi9qYXZhc2NyaXB0Il0sWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsMCwzMTQ1NzI4MF0seyJhY2wiOiJwdWJsaWMtcmVhZCJ9LHsiYnVja2V0Ijoid2ViZmxvdy1wcm9kLWFzc2V0cyJ9LHsiWC1BbXotQWxnb3JpdGhtIjoiQVdTNC1ITUFDLVNIQTI1NiJ9LHsiWC1BbXotQ3JlZGVudGlhbCI6IkFLSUFRTExIV0Q2TUVKR0VUTFNULzIwMjYwNzI5L3VzLWVhc3QtMS9zMy9hd3M0X3JlcXVlc3QifSx7IlgtQW16LURhdGUiOiIyMDI2MDcyOVQyMTA5MjVaIn0seyJrZXkiOiI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvNmE2YTZjMDUwYWI4NmM5NzFhZmNlZjg0X29sZC1ob21lLWhlcm8tZml0LWJvb3QudjIwMjYwNzI5ZC5qcyJ9XX0=",
  xAmzSignature: "f1b6ac83b57c29443024b8263c107746bab83b7475456f1ab0181bfe80c62049",
  successActionStatus: "201",
  contentType: "application/javascript",
  cacheControl: "max-age=31536000",
};

const file = path.resolve("public/marketing/old-home-hero-fit-boot.v20260729d.js");
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
form.append(
  "file",
  new Blob([fileBytes], { type: details.contentType }),
  path.basename(file)
);
const res = await fetch("https://webflow-prod-assets.s3.amazonaws.com/", {
  method: "POST",
  body: form,
});
const sha384 = crypto.createHash("sha384").update(fileBytes).digest("base64");
console.log("upload", res.status, (await res.text()).slice(0, 120));
console.log("sri", `sha384-${sha384}`);
console.log(
  "hosted",
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6a6c050ab86c971afcef84_old-home-hero-fit-boot.v20260729d.js"
);
