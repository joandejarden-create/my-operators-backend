import fs from "fs";

const details = {
  acl: "public-read",
  bucket: "webflow-prod-assets",
  "X-Amz-Algorithm": "AWS4-HMAC-SHA256",
  "X-Amz-Credential": "AKIAQLLHWD6MEJGETLST/20260731/us-east-1/s3/aws4_request",
  "X-Amz-Date": "20260731T201022Z",
  key: "68108c29063eeb5d1bd7ae4a/6a6d012e6090178c80db722f_dealality-old-home-testimonials.v20260731ac.js",
  Policy:
    "eyJleHBpcmF0aW9uIjoiMjAyNi0wNy0zMVQyMToxMDoyMloiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoiYXBwbGljYXRpb24vamF2YXNjcmlwdCJ9LHsic3VjY2Vzc19hY3Rpb25fc3RhdHVzIjoiMjAxIn0sWyJzdGFydHMtd2l0aCIsIiRDb250ZW50LVR5cGUiLCJhcHBsaWNhdGlvbi9qYXZhc2NyaXB0Il0sWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsMCwzMTQ1NzI4MF0seyJhY2wiOiJwdWJsaWMtcmVhZCJ9LHsiYnVja2V0Ijoid2ViZmxvdy1wcm9kLWFzc2V0cyJ9LHsiWC1BbXotQWxnb3JpdGhtIjoiQVdTNC1ITUFDLVNIQTI1NiJ9LHsiWC1BbXotQ3JlZGVudGlhbCI6IkFLSUFRTExIV0Q2TUVKR0VUTFNULzIwMjYwNzMxL3VzLWVhc3QtMS9zMy9hd3M0X3JlcXVlc3QifSx7IlgtQW16LURhdGUiOiIyMDI2MDczMVQyMDEwMjJaIn0seyJrZXkiOiI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvNmE2ZDAxMmU2MDkwMTc4YzgwZGI3MjJmX2RlYWxhbGl0eS1vbGQtaG9tZS10ZXN0aW1vbmlhbHMudjIwMjYwNzMxYWMuanMifV19",
  "X-Amz-Signature":
    "2e0c16def458d2e08363c20da1ae4826989566f1a9059fcc46d0b9f1605244c4",
  success_action_status: "201",
  "Content-Type": "application/javascript",
  "Cache-Control": "max-age=31536000",
};

const form = new FormData();
for (const [k, v] of Object.entries(details)) form.append(k, v);
const bytes = fs.readFileSync(
  "public/marketing/dealality-old-home-testimonials.v20260731ac.js"
);
form.append(
  "file",
  new Blob([bytes], { type: "application/javascript" }),
  "dealality-old-home-testimonials.v20260731ac.js"
);
const r = await fetch("https://webflow-prod-assets.s3.amazonaws.com/", {
  method: "POST",
  body: form,
});
console.log(r.status, await r.text());
