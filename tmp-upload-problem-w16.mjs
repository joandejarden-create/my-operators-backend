import fs from "fs";
import path from "path";
import crypto from "crypto";

async function upload(fileRel, details, hostedUrl) {
  const file = path.resolve(fileRel);
  const fileBytes = fs.readFileSync(file);
  const md5 = crypto.createHash("md5").update(fileBytes).digest("hex");
  console.log(path.basename(file), "bytes", fileBytes.length, "md5", md5);

  const form = new FormData();
  const d = details;
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
  const text = await res.text();
  console.log("upload", path.basename(file), res.status, text.slice(0, 200));
  if (res.status !== 201) throw new Error("upload failed " + path.basename(file));

  const check = await fetch(hostedUrl, { method: "GET" });
  const body = await check.text();
  console.log("cdn", hostedUrl.split("/").pop(), check.status, "len", body.length, "Hard?", body.includes("Hard to Compare"), "problem?", body.includes("data-oh-problem-v2"));
}

const cssDetails = {
  acl: "public-read",
  bucket: "webflow-prod-assets",
  xAmzAlgorithm: "AWS4-HMAC-SHA256",
  xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260729/us-east-1/s3/aws4_request",
  xAmzDate: "20260729T145647Z",
  key: "68108c29063eeb5d1bd7ae4a/6a6a14af8da6137b340675c6_dealality-old-home-freeform-head.v20260729w16.css",
  policy:
    "eyJleHBpcmF0aW9uIjoiMjAyNi0wNy0yOVQxNTo1Njo0N1oiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoidGV4dC9jc3MifSx7InN1Y2Nlc3NfYWN0aW9uX3N0YXR1cyI6IjIwMSJ9LFsic3RhcnRzLXdpdGgiLCIkQ29udGVudC1UeXBlIiwidGV4dC9jc3MiXSxbImNvbnRlbnQtbGVuZ3RoLXJhbmdlIiwwLDMxNDU3MjgwXSx7ImFjbCI6InB1YmxpYy1yZWFkIn0seyJidWNrZXQiOiJ3ZWJmbG93LXByb2QtYXNzZXRzIn0seyJYLUFtei1BbGdvcml0aG0iOiJBV1M0LUhNQUMtU0hBMjU2In0seyJYLUFtei1DcmVkZW50aWFsIjoiQUtJQVFMTEhXRDZNRUpHRVRMU1QvMjAyNjA3MjkvdXMtZWFzdC0xL3MzL2F3czRfcmVxdWVzdCJ9LHsiWC1BbXotRGF0ZSI6IjIwMjYwNzI5VDE0NTY0N1oifSx7ImtleSI6IjY4MTA4YzI5MDYzZWViNWQxYmQ3YWU0YS82YTZhMTRhZjhkYTYxMzdiMzQwNjc1YzZfZGVhbGFsaXR5LW9sZC1ob21lLWZyZWVmb3JtLWhlYWQudjIwMjYwNzI5dzE2LmNzcyJ9XX0=",
  xAmzSignature: "e2c9e8caddedb3b7475b274e113ab340c148e585740b270d1a901d1e396af397",
  successActionStatus: "201",
  contentType: "text/css",
  cacheControl: "max-age=31536000",
};

const jsDetails = {
  acl: "public-read",
  bucket: "webflow-prod-assets",
  xAmzAlgorithm: "AWS4-HMAC-SHA256",
  xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260729/us-east-1/s3/aws4_request",
  xAmzDate: "20260729T145648Z",
  key: "68108c29063eeb5d1bd7ae4a/6a6a14b095a809ffc9b2eaa3_old-home-footer-oh.v20260729e.js",
  policy:
    "eyJleHBpcmF0aW9uIjoiMjAyNi0wNy0yOVQxNTo1Njo0OFoiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoiYXBwbGljYXRpb24vamF2YXNjcmlwdCJ9LHsic3VjY2Vzc19hY3Rpb25fc3RhdHVzIjoiMjAxIn0sWyJzdGFydHMtd2l0aCIsIiRDb250ZW50LVR5cGUiLCJhcHBsaWNhdGlvbi9qYXZhc2NyaXB0Il0sWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsMCwzMTQ1NzI4MF0seyJhY2wiOiJwdWJsaWMtcmVhZCJ9LHsiYnVja2V0Ijoid2ViZmxvdy1wcm9kLWFzc2V0cyJ9LHsiWC1BbXotQWxnb3JpdGhtIjoiQVdTNC1ITUFDLVNIQTI1NiJ9LHsiWC1BbXotQ3JlZGVudGlhbCI6IkFLSUFRTExIV0Q2TUVKR0VUTFNULzIwMjYwNzI5L3VzLWVhc3QtMS9zMy9hd3M0X3JlcXVlc3QifSx7IlgtQW16LURhdGUiOiIyMDI2MDcyOVQxNDU2NDhaIn0seyJrZXkiOiI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvNmE2YTE0YjA5NWE4MDlmZmM5YjJlYWEzX29sZC1ob21lLWZvb3Rlci1vaC52MjAyNjA3MjllLmpzIn1dfQ==",
  xAmzSignature: "017bb1b3a5c335c754388a48094c5f6fd3f913adef65ee0852871fcf5680a982",
  successActionStatus: "201",
  contentType: "application/javascript",
  cacheControl: "max-age=31536000",
};

// Re-assert CSS content right before upload in case OneDrive reverted
const { spawnSync } = await import("child_process");
spawnSync("node", ["tmp-write-w16-problem.mjs"], { stdio: "inherit" });
const cssBytes = fs.readFileSync("public/marketing/dealality-old-home-freeform-head.v20260729w16.css");
const cssMd5 = crypto.createHash("md5").update(cssBytes).digest("hex");
if (cssMd5 !== "24f7f0314d2c900feb1327dbfb79d072") {
  console.warn("WARNING: CSS md5 changed to", cssMd5, "- may fail S3 ETag check");
}

await upload(
  "public/marketing/dealality-old-home-freeform-head.v20260729w16.css",
  cssDetails,
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6a14af8da6137b340675c6_dealality-old-home-freeform-head.v20260729w16.css"
);
await upload(
  "public/marketing/old-home-footer-oh.v20260729e.js",
  jsDetails,
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6a14b095a809ffc9b2eaa3_old-home-footer-oh.v20260729e.js"
);
