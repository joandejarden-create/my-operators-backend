/**
 * Upload boot-guard 01v to Webflow S3, then print update payload.
 */
import fs from "fs";
import path from "path";

const filePath = path.resolve(
  "C:/Dev/deal-capture-proxy/public/marketing/old-home-boot-guard.v20260801v.js"
);
const fileBuf = fs.readFileSync(filePath);

const uploadUrl = "https://webflow-prod-assets.s3.amazonaws.com/";
const uploadDetails = {
  acl: "public-read",
  bucket: "webflow-prod-assets",
  "X-Amz-Algorithm": "AWS4-HMAC-SHA256",
  "X-Amz-Credential": "AKIAQLLHWD6MEJGETLST/20260801/us-east-1/s3/aws4_request",
  "X-Amz-Date": "20260801T193829Z",
  key: "68108c29063eeb5d1bd7ae4a/6a6e4b355a6294f460fc7fec_old-home-boot-guard.v20260801v.js",
  Policy:
    "eyJleHBpcmF0aW9uIjoiMjAyNi0wOC0wMVQyMDozODoyOVoiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoiYXBwbGljYXRpb24vamF2YXNjcmlwdCJ9LHsic3VjY2Vzc19hY3Rpb25fc3RhdHVzIjoiMjAxIn0sWyJzdGFydHMtd2l0aCIsIiRDb250ZW50LVR5cGUiLCJhcHBsaWNhdGlvbi9qYXZhc2NyaXB0Il0sWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsMCwzMTQ1NzI4MF0seyJhY2wiOiJwdWJsaWMtcmVhZCJ9LHsiYnVja2V0Ijoid2ViZmxvdy1wcm9kLWFzc2V0cyJ9LHsiWC1BbXotQWxnb3JpdGhtIjoiQVdTNC1ITUFDLVNIQTI1NiJ9LHsiWC1BbXotQ3JlZGVudGlhbCI6IkFLSUFRTExIV0Q2TUVKR0VUTFNULzIwMjYwODAxL3VzLWVhc3QtMS9zMy9hd3M0X3JlcXVlc3QifSx7IlgtQW16LURhdGUiOiIyMDI2MDgwMVQxOTM4MjlaIn0seyJrZXkiOiI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvNmE2ZTRiMzU1YTYyOTRmNDYwZmM3ZmVjX29sZC1ob21lLWJvb3QtZ3VhcmQudjIwMjYwODAxdi5qcyJ9XX0=",
  "X-Amz-Signature": "792b147ab8e8e3d70f1473cce1f55a11a46dcea9cdb3a2e179c264d29793c0f4",
  success_action_status: "201",
  "Content-Type": "application/javascript",
  "Cache-Control": "max-age=31536000",
};

const form = new FormData();
for (const [k, v] of Object.entries(uploadDetails)) {
  form.append(k, v);
}
form.append(
  "file",
  new Blob([fileBuf], { type: "application/javascript" }),
  path.basename(filePath)
);

const res = await fetch(uploadUrl, { method: "POST", body: form });
const body = await res.text();
const hostedUrl =
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6e4b355a6294f460fc7fec_old-home-boot-guard.v20260801v.js";
console.log(
  JSON.stringify({
    status: res.status,
    ok: res.status === 201,
    hostedUrl,
    integrityHash: "sha256-bsLTiL/9XOQFHb3KkR2nDEABUfWBkk5MadTFJ1dVdb4=",
    body: body.slice(0, 200),
  })
);
