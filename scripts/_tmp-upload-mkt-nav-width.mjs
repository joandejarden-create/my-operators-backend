import fs from "fs";
import path from "path";

const filePath = path.resolve(
  "C:/Dev/deal-capture-proxy/public/marketing/dealality-marketing-nav-width.v20260801a.js"
);
const fileBuf = fs.readFileSync(filePath);
const uploadUrl = "https://webflow-prod-assets.s3.amazonaws.com/";
const uploadDetails = {
  acl: "public-read",
  bucket: "webflow-prod-assets",
  "X-Amz-Algorithm": "AWS4-HMAC-SHA256",
  "X-Amz-Credential": "AKIAQLLHWD6MEJGETLST/20260801/us-east-1/s3/aws4_request",
  "X-Amz-Date": "20260801T195348Z",
  key: "68108c29063eeb5d1bd7ae4a/6a6e4ecc93bf55bda98a5a1f_dealality-marketing-nav-width.v20260801a.js",
  Policy:
    "eyJleHBpcmF0aW9uIjoiMjAyNi0wOC0wMVQyMDo1Mzo0OFoiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoiYXBwbGljYXRpb24vamF2YXNjcmlwdCJ9LHsic3VjY2Vzc19hY3Rpb25fc3RhdHVzIjoiMjAxIn0sWyJzdGFydHMtd2l0aCIsIiRDb250ZW50LVR5cGUiLCJhcHBsaWNhdGlvbi9qYXZhc2NyaXB0Il0sWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsMCwzMTQ1NzI4MF0seyJhY2wiOiJwdWJsaWMtcmVhZCJ9LHsiYnVja2V0Ijoid2ViZmxvdy1wcm9kLWFzc2V0cyJ9LHsiWC1BbXotQWxnb3JpdGhtIjoiQVdTNC1ITUFDLVNIQTI1NiJ9LHsiWC1BbXotQ3JlZGVudGlhbCI6IkFLSUFRTExIV0Q2TUVKR0VUTFNULzIwMjYwODAxL3VzLWVhc3QtMS9zMy9hd3M0X3JlcXVlc3QifSx7IlgtQW16LURhdGUiOiIyMDI2MDgwMVQxOTUzNDhaIn0seyJrZXkiOiI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvNmE2ZTRlY2M5M2JmNTViZGE5OGE1YTFmX2RlYWxhbGl0eS1tYXJrZXRpbmctbmF2LXdpZHRoLnYyMDI2MDgwMWEuanMifV19",
  "X-Amz-Signature": "397d1370c662ad70f89b00b0e4fce629a0a69977cf4c09acd0a3ed9555690e32",
  success_action_status: "201",
  "Content-Type": "application/javascript",
  "Cache-Control": "max-age=31536000",
};

const form = new FormData();
for (const [k, v] of Object.entries(uploadDetails)) form.append(k, v);
form.append(
  "file",
  new Blob([fileBuf], { type: "application/javascript" }),
  path.basename(filePath)
);
const res = await fetch(uploadUrl, { method: "POST", body: form });
console.log(
  JSON.stringify({
    status: res.status,
    ok: res.status === 201,
    hostedUrl:
      "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6e4ecc93bf55bda98a5a1f_dealality-marketing-nav-width.v20260801a.js",
    body: (await res.text()).slice(0, 180),
  })
);
