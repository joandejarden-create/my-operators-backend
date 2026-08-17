/**
 * Upload freeform-head 01f CSS to Webflow S3.
 */
import fs from "fs";
import path from "path";

const filePath = path.resolve(
  "C:/Dev/deal-capture-proxy/public/marketing/dealality-old-home-freeform-head.v20260801f.css"
);
const fileBuf = fs.readFileSync(filePath);

const uploadUrl = "https://webflow-prod-assets.s3.amazonaws.com/";
const uploadDetails = {
  acl: "public-read",
  bucket: "webflow-prod-assets",
  "X-Amz-Algorithm": "AWS4-HMAC-SHA256",
  "X-Amz-Credential": "AKIAQLLHWD6MEJGETLST/20260801/us-east-1/s3/aws4_request",
  "X-Amz-Date": "20260801T193147Z",
  key: "68108c29063eeb5d1bd7ae4a/6a6e49a3ab735f9c93cd83c8_dealality-old-home-freeform-head.v20260801f.css",
  Policy:
    "eyJleHBpcmF0aW9uIjoiMjAyNi0wOC0wMVQyMDozMTo0N1oiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoidGV4dC9jc3MifSx7InN1Y2Nlc3NfYWN0aW9uX3N0YXR1cyI6IjIwMSJ9LFsic3RhcnRzLXdpdGgiLCIkQ29udGVudC1UeXBlIiwidGV4dC9jc3MiXSxbImNvbnRlbnQtbGVuZ3RoLXJhbmdlIiwwLDMxNDU3MjgwXSx7ImFjbCI6InB1YmxpYy1yZWFkIn0seyJidWNrZXQiOiJ3ZWJmbG93LXByb2QtYXNzZXRzIn0seyJYLUFtei1BbGdvcml0aG0iOiJBV1M0LUhNQUMtU0hBMjU2In0seyJYLUFtei1DcmVkZW50aWFsIjoiQUtJQVFMTEhXRDZNRUpHRVRMU1QvMjAyNjA4MDEvdXMtZWFzdC0xL3MzL2F3czRfcmVxdWVzdCJ9LHsiWC1BbXotRGF0ZSI6IjIwMjYwODAxVDE5MzE0N1oifSx7ImtleSI6IjY4MTA4YzI5MDYzZWViNWQxYmQ3YWU0YS82YTZlNDlhM2FiNzM1ZjljOTNjZDgzYzhfZGVhbGFsaXR5LW9sZC1ob21lLWZyZWVmb3JtLWhlYWQudjIwMjYwODAxZi5jc3MifV19",
  "X-Amz-Signature": "b9173c9fcea805f81dc158cc548b55483df03b599aab78ae857425686ed1d125",
  success_action_status: "201",
  "Content-Type": "text/css",
  "Cache-Control": "max-age=31536000",
};

const form = new FormData();
for (const [k, v] of Object.entries(uploadDetails)) {
  form.append(k, v);
}
form.append("file", new Blob([fileBuf], { type: "text/css" }), path.basename(filePath));

const res = await fetch(uploadUrl, { method: "POST", body: form });
const body = await res.text();
console.log(
  JSON.stringify({
    status: res.status,
    ok: res.status === 201,
    hostedUrl:
      "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6e49a3ab735f9c93cd83c8_dealality-old-home-freeform-head.v20260801f.css",
    body: body.slice(0, 300),
  })
);
