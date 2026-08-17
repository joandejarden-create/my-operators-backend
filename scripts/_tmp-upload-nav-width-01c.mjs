import fs from "fs";
import path from "path";

const filePath = path.resolve(
  "C:/Dev/deal-capture-proxy/public/marketing/dealality-marketing-nav-width.v20260801c.js"
);
const fileBuf = fs.readFileSync(filePath);
const form = new FormData();
const details = {
  acl: "public-read",
  bucket: "webflow-prod-assets",
  "X-Amz-Algorithm": "AWS4-HMAC-SHA256",
  "X-Amz-Credential": "AKIAQLLHWD6MEJGETLST/20260801/us-east-1/s3/aws4_request",
  "X-Amz-Date": "20260801T200122Z",
  key: "68108c29063eeb5d1bd7ae4a/6a6e5092c766e9957260e969_dealality-marketing-nav-width.v20260801c.js",
  Policy:
    "eyJleHBpcmF0aW9uIjoiMjAyNi0wOC0wMVQyMTowMToyMloiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoiYXBwbGljYXRpb24vamF2YXNjcmlwdCJ9LHsic3VjY2Vzc19hY3Rpb25fc3RhdHVzIjoiMjAxIn0sWyJzdGFydHMtd2l0aCIsIiRDb250ZW50LVR5cGUiLCJhcHBsaWNhdGlvbi9qYXZhc2NyaXB0Il0sWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsMCwzMTQ1NzI4MF0seyJhY2wiOiJwdWJsaWMtcmVhZCJ9LHsiYnVja2V0Ijoid2ViZmxvdy1wcm9kLWFzc2V0cyJ9LHsiWC1BbXotQWxnb3JpdGhtIjoiQVdTNC1ITUFDLVNIQTI1NiJ9LHsiWC1BbXotQ3JlZGVudGlhbCI6IkFLSUFRTExIV0Q2TUVKR0VUTFNULzIwMjYwODAxL3VzLWVhc3QtMS9zMy9hd3M0X3JlcXVlc3QifSx7IlgtQW16LURhdGUiOiIyMDI2MDgwMVQyMDAxMjJaIn0seyJrZXkiOiI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvNmE2ZTUwOTJjNzY2ZTk5NTcyNjBlOTY5X2RlYWxhbGl0eS1tYXJrZXRpbmctbmF2LXdpZHRoLnYyMDI2MDgwMWMuanMifV19",
  "X-Amz-Signature": "28879246e78d6a18937be5f9fa0f30fb347c78e08377fada49deec67c032ad1e",
  success_action_status: "201",
  "Content-Type": "application/javascript",
  "Cache-Control": "max-age=31536000",
};
for (const [k, v] of Object.entries(details)) form.append(k, v);
form.append(
  "file",
  new Blob([fileBuf], { type: "application/javascript" }),
  path.basename(filePath)
);
const res = await fetch("https://webflow-prod-assets.s3.amazonaws.com/", {
  method: "POST",
  body: form,
});
console.log(JSON.stringify({ status: res.status, ok: res.status === 201 }));
