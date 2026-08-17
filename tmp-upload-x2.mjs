import fs from "fs";
import path from "path";

const file = "public/marketing/dealality-old-home-platform-features.v20260729x2.css";
const uploadUrl = "https://webflow-prod-assets.s3.amazonaws.com/";
const d = {
  acl: "public-read",
  bucket: "webflow-prod-assets",
  xAmzAlgorithm: "AWS4-HMAC-SHA256",
  xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260729/us-east-1/s3/aws4_request",
  xAmzDate: "20260729T152215Z",
  key: "68108c29063eeb5d1bd7ae4a/6a6a1aa77e93121d8fecb49d_dealality-old-home-platform-features.v20260729x2.css",
  policy:
    "eyJleHBpcmF0aW9uIjoiMjAyNi0wNy0yOVQxNjoyMjoxNVoiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoidGV4dC9jc3MifSx7InN1Y2Nlc3NfYWN0aW9uX3N0YXR1cyI6IjIwMSJ9LFsic3RhcnRzLXdpdGgiLCIkQ29udGVudC1UeXBlIiwidGV4dC9jc3MiXSxbImNvbnRlbnQtbGVuZ3RoLXJhbmdlIiwwLDMxNDU3MjgwXSx7ImFjbCI6InB1YmxpYy1yZWFkIn0seyJidWNrZXQiOiJ3ZWJmbG93LXByb2QtYXNzZXRzIn0seyJYLUFtei1BbGdvcml0aG0iOiJBV1M0LUhNQUMtU0hBMjU2In0seyJYLUFtei1DcmVkZW50aWFsIjoiQUtJQVFMTEhXRDZNRUpHRVRMU1QvMjAyNjA3MjkvdXMtZWFzdC0xL3MzL2F3czRfcmVxdWVzdCJ9LHsiWC1BbXotRGF0ZSI6IjIwMjYwNzI5VDE1MjIxNVoifSx7ImtleSI6IjY4MTA4YzI5MDYzZWViNWQxYmQ3YWU0YS82YTZhMWFhNzdlOTMxMjFkOGZlY2I0OWRfZGVhbGFsaXR5LW9sZC1ob21lLXBsYXRmb3JtLWZlYXR1cmVzLnYyMDI2MDcyOXgyLmNzcyJ9XX0=",
  xAmzSignature: "d1b65a210b67e809b1e00306f848e020f6e008b6b4580ca35ec47c96468d65da",
  successActionStatus: "201",
  contentType: "text/css",
  cacheControl: "max-age=31536000",
};

const fileBytes = fs.readFileSync(file);
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

const res = await fetch(uploadUrl, { method: "POST", body: form });
const text = await res.text();
console.log(path.basename(file), res.status, text.slice(0, 400));
