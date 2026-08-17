import fs from "fs";
import path from "path";

const file = "C:/Dev/deal-capture-proxy/public/marketing/dealality-old-home-nav-scroll.v20260801a.js";
const buf = fs.readFileSync(file);
const details = {
  acl: "public-read",
  bucket: "webflow-prod-assets",
  "X-Amz-Algorithm": "AWS4-HMAC-SHA256",
  "X-Amz-Credential": "AKIAQLLHWD6MEJGETLST/20260801/us-east-1/s3/aws4_request",
  "X-Amz-Date": "20260801T190621Z",
  key: "68108c29063eeb5d1bd7ae4a/6a6e43ac556592892627d31e_dealality-old-home-nav-scroll.v20260801a.js",
  policy:
    "eyJleHBpcmF0aW9uIjoiMjAyNi0wOC0wMVQyMDowNjoyMVoiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoiYXBwbGljYXRpb24vamF2YXNjcmlwdCJ9LHsic3VjY2Vzc19hY3Rpb25fc3RhdHVzIjoiMjAxIn0sWyJzdGFydHMtd2l0aCIsIiRDb250ZW50LVR5cGUiLCJhcHBsaWNhdGlvbi9qYXZhc2NyaXB0Il0sWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsMCwzMTQ1NzI4MF0seyJhY2wiOiJwdWJsaWMtcmVhZCJ9LHsiYnVja2V0Ijoid2ViZmxvdy1wcm9kLWFzc2V0cyJ9LHsiWC1BbXotQWxnb3JpdGhtIjoiQVdTNC1ITUFDLVNIQTI1NiJ9LHsiWC1BbXotQ3JlZGVudGlhbCI6IkFLSUFRTExIV0Q2TUVKR0VUTFNULzIwMjYwODAxL3VzLWVhc3QtMS9zMy9hd3M0X3JlcXVlc3QifSx7IlgtQW16LURhdGUiOiIyMDI2MDgwMVQxOTA2MjFaIn0seyJrZXkiOiI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvNmE2ZTQzYWM1NTY1OTI4OTI2MjdkMzFlX2RlYWxhbGl0eS1vbGQtaG9tZS1uYXYtc2Nyb2xsLnYyMDI2MDgwMWEuanMifV19",
  "X-Amz-Signature": "15bd9c638b11fbc4509b6ef13534d95aaf82bf07e1542ee9c86f2d5a81e0a796",
  success_action_status: "201",
  "Content-Type": "application/javascript",
  "Cache-Control": "max-age=31536000",
};

const form = new FormData();
for (const [k, v] of Object.entries(details)) form.append(k, v);
form.append("file", new Blob([buf], { type: "application/javascript" }), path.basename(file));

const res = await fetch("https://webflow-prod-assets.s3.amazonaws.com/", {
  method: "POST",
  body: form,
});
const text = await res.text();
console.log(JSON.stringify({ status: res.status, body: text.slice(0, 280) }));
if (res.status !== 201) process.exit(1);
