import fs from "fs";
import axios from "axios";
import FormData from "form-data";

const uploadDetails = {
  acl: "public-read",
  bucket: "webflow-prod-assets",
  xAmzAlgorithm: "AWS4-HMAC-SHA256",
  xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260728/us-east-1/s3/aws4_request",
  xAmzDate: "20260728T222019Z",
  key: "68108c29063eeb5d1bd7ae4a/6a692b236485017a5aaebb61_dealality-old-home-freeform-head.v20260729.css",
  policy:
    "eyJleHBpcmF0aW9uIjoiMjAyNi0wNy0yOFQyMzoyMDoxOVoiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoidGV4dC9jc3MifSx7InN1Y2Nlc3NfYWN0aW9uX3N0YXR1cyI6IjIwMSJ9LFsic3RhcnRzLXdpdGgiLCIkQ29udGVudC1UeXBlIiwidGV4dC9jc3MiXSxbImNvbnRlbnQtbGVuZ3RoLXJhbmdlIiwwLDMxNDU3MjgwXSx7ImFjbCI6InB1YmxpYy1yZWFkIn0seyJidWNrZXQiOiJ3ZWJmbG93LXByb2QtYXNzZXRzIn0seyJYLUFtei1BbGdvcml0aG0iOiJBV1M0LUhNQUMtU0hBMjU2In0seyJYLUFtei1DcmVkZW50aWFsIjoiQUtJQVFMTEhXRDZNRUpHRVRMU1QvMjAyNjA3MjgvdXMtZWFzdC0xL3MzL2F3czRfcmVxdWVzdCJ9LHsiWC1BbXotRGF0ZSI6IjIwMjYwNzI4VDIyMjAxOVoifSx7ImtleSI6IjY4MTA4YzI5MDYzZWViNWQxYmQ3YWU0YS82YTY5MmIyMzY0ODUwMTdhNWFhZWJiNjFfZGVhbGFsaXR5LW9sZC1ob21lLWZyZWVmb3JtLWhlYWQudjIwMjYwNzI5LmNzcyJ9XX0=",
  xAmzSignature:
    "1439f4fc0fa8f3a453a0b4b45fd0eb106c2a01cc5d7f9d1c647211908eae9c86",
  successActionStatus: "201",
  contentType: "text/css",
  cacheControl: "max-age=31536000",
};

const fileData = fs.readFileSync(
  "public/marketing/dealality-old-home-freeform-head.v20260729.css"
);
const form = new FormData();
form.append("acl", uploadDetails.acl);
form.append("bucket", uploadDetails.bucket);
form.append("X-Amz-Algorithm", uploadDetails.xAmzAlgorithm);
form.append("X-Amz-Credential", uploadDetails.xAmzCredential);
form.append("X-Amz-Date", uploadDetails.xAmzDate);
form.append("key", uploadDetails.key);
form.append("Policy", uploadDetails.policy);
form.append("X-Amz-Signature", uploadDetails.xAmzSignature);
form.append("success_action_status", uploadDetails.successActionStatus);
form.append("Content-Type", uploadDetails.contentType);
form.append("Cache-Control", uploadDetails.cacheControl);
form.append("file", fileData, {
  filename: "dealality-old-home-freeform-head.v20260729.css",
  contentType: "text/css",
});

const res = await axios.post(
  "https://webflow-prod-assets.s3.amazonaws.com/",
  form,
  { headers: form.getHeaders(), maxBodyLength: Infinity, validateStatus: () => true }
);
console.log("status", res.status);
if (res.status !== 201) {
  console.log(String(res.data).slice(0, 400));
  process.exit(1);
}

const hosted =
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a692b236485017a5aaebb61_dealality-old-home-freeform-head.v20260729.css";
const head = `<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter+Tight:wght@400;500;600;700;800&family=Plus+Jakarta+Sans:wght@400;500;600;700;800&family=Lora:ital,wght@0,400;1,400&display=swap" rel="stylesheet">
<link rel="stylesheet" href="https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a68c28696192b91c48d1768_dealality-old-home-dark.v20260728ag.css">
<link rel="stylesheet" href="https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a68f96d1f20a4a06d72162c_dealality-old-home-freeform.v20260728benefits.css">
<link rel="stylesheet" href="https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6906d02cfa3b13446a3236_dealality-old-home-benefits-tabs.v20260728b.css">
<link rel="stylesheet" href="https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a69179b0ce72c9fded41454_dealality-old-home-perspectives.v20260728.css">
<link rel="stylesheet" href="${hosted}">
`;
fs.writeFileSync("tmp-old-home-head-links-only.txt", head);
console.log("head bytes", head.length);
console.log(head);
