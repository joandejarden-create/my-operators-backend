import fs from "fs";
import path from "path";

const file =
  "C:/Dev/deal-capture-proxy/public/marketing/dealality-old-home-nav-scroll.v20260801b.js";
const buf = fs.readFileSync(file);
const details = {
  acl: "public-read",
  bucket: "webflow-prod-assets",
  "X-Amz-Algorithm": "AWS4-HMAC-SHA256",
  "X-Amz-Credential": "AKIAQLLHWD6MEJGETLST/20260801/us-east-1/s3/aws4_request",
  "X-Amz-Date": "20260801T191433Z",
  key: "68108c29063eeb5d1bd7ae4a/6a6e45995b605a3355bed5e9_dealality-old-home-nav-scroll.v20260801b.js",
  policy:
    "eyJleHBpcmF0aW9uIjoiMjAyNi0wOC0wMVQyMDoxNDozM1oiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoiYXBwbGljYXRpb24vamF2YXNjcmlwdCJ9LHsic3VjY2Vzc19hY3Rpb25fc3RhdHVzIjoiMjAxIn0sWyJzdGFydHMtd2l0aCIsIiRDb250ZW50LVR5cGUiLCJhcHBsaWNhdGlvbi9qYXZhc2NyaXB0Il0sWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsMCwzMTQ1NzI4MF0seyJhY2wiOiJwdWJsaWMtcmVhZCJ9LHsiYnVja2V0Ijoid2ViZmxvdy1wcm9kLWFzc2V0cyJ9LHsiWC1BbXotQWxnb3JpdGhtIjoiQVdTNC1ITUFDLVNIQTI1NiJ9LHsiWC1BbXotQ3JlZGVudGlhbCI6IkFLSUFRTExIV0Q2TUVKR0VUTFNULzIwMjYwODAxL3VzLWVhc3QtMS9zMy9hd3M0X3JlcXVlc3QifSx7IlgtQW16LURhdGUiOiIyMDI2MDgwMVQxOTE0MzNaIn0seyJrZXkiOiI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvNmE2ZTQ1OTk1YjYwNWEzMzU1YmVkNWU5X2RlYWxhbGl0eS1vbGQtaG9tZS1uYXYtc2Nyb2xsLnYyMDI2MDgwMWIuanMifV19",
  "X-Amz-Signature": "ee787c04775e33dc3fec7bc8afd018fa0fb041c5eac3c50bb518a42e62bf9b40",
  success_action_status: "201",
  "Content-Type": "application/javascript",
  "Cache-Control": "max-age=31536000",
};

const form = new FormData();
for (const [k, v] of Object.entries(details)) form.append(k, v);
form.append(
  "file",
  new Blob([buf], { type: "application/javascript" }),
  path.basename(file)
);

const res = await fetch("https://webflow-prod-assets.s3.amazonaws.com/", {
  method: "POST",
  body: form,
});
const text = await res.text();
console.log(JSON.stringify({ status: res.status, body: text.slice(0, 280) }));
if (res.status !== 201) process.exit(1);
