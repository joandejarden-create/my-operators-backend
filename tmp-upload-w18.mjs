import fs from "fs";
import path from "path";

const details = {
  acl: "public-read",
  bucket: "webflow-prod-assets",
  xAmzAlgorithm: "AWS4-HMAC-SHA256",
  xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260729/us-east-1/s3/aws4_request",
  xAmzDate: "20260729T151810Z",
  key: "68108c29063eeb5d1bd7ae4a/6a6a19b23c529c0bd6a9e33f_dealality-old-home-freeform-head.v20260729w18.css",
  policy:
    "eyJleHBpcmF0aW9uIjoiMjAyNi0wNy0yOVQxNjoxODoxMFoiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoidGV4dC9jc3MifSx7InN1Y2Nlc3NfYWN0aW9uX3N0YXR1cyI6IjIwMSJ9LFsic3RhcnRzLXdpdGgiLCIkQ29udGVudC1UeXBlIiwidGV4dC9jc3MiXSxbImNvbnRlbnQtbGVuZ3RoLXJhbmdlIiwwLDMxNDU3MjgwXSx7ImFjbCI6InB1YmxpYy1yZWFkIn0seyJidWNrZXQiOiJ3ZWJmbG93LXByb2QtYXNzZXRzIn0seyJYLUFtei1BbGdvcml0aG0iOiJBV1M0LUhNQUMtU0hBMjU2In0seyJYLUFtei1DcmVkZW50aWFsIjoiQUtJQVFMTEhXRDZNRUpHRVRMU1QvMjAyNjA3MjkvdXMtZWFzdC0xL3MzL2F3czRfcmVxdWVzdCJ9LHsiWC1BbXotRGF0ZSI6IjIwMjYwNzI5VDE1MTgxMFoifSx7ImtleSI6IjY4MTA4YzI5MDYzZWViNWQxYmQ3YWU0YS82YTZhMTliMjNjNTI5YzBiZDZhOWUzM2ZfZGVhbGFsaXR5LW9sZC1ob21lLWZyZWVmb3JtLWhlYWQudjIwMjYwNzI5dzE4LmNzcyJ9XX0=",
  xAmzSignature: "eb8e7a8ca8b9eb30ab5ef9ade1dae5ca0a52abde7d0c6ba95c78f83c9a32e76f",
  successActionStatus: "201",
  contentType: "text/css",
  cacheControl: "max-age=31536000",
};

const file = path.resolve("public/marketing/dealality-old-home-freeform-head.v20260729w18.css");
const fileBytes = fs.readFileSync(file);
const form = new FormData();
form.append("acl", details.acl);
form.append("bucket", details.bucket);
form.append("X-Amz-Algorithm", details.xAmzAlgorithm);
form.append("X-Amz-Credential", details.xAmzCredential);
form.append("X-Amz-Date", details.xAmzDate);
form.append("key", details.key);
form.append("Policy", details.policy);
form.append("X-Amz-Signature", details.xAmzSignature);
form.append("success_action_status", details.successActionStatus);
form.append("Content-Type", details.contentType);
form.append("Cache-Control", details.cacheControl);
form.append("file", new Blob([fileBytes], { type: details.contentType }), path.basename(file));

const res = await fetch("https://webflow-prod-assets.s3.amazonaws.com/", { method: "POST", body: form });
console.log("upload", res.status, (await res.text()).slice(0, 180));

const hosted =
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6a19b23c529c0bd6a9e33f_dealality-old-home-freeform-head.v20260729w18.css";
const cdn = await (await fetch(hosted)).text();
console.log("cdn Hard", cdn.includes("Hard to Compare"), "gutter", cdn.includes("Left-aligned hero copy"), "len", cdn.length);

const head = fs.readFileSync("tmp-freeform-head-w16.html", "utf8").replace(
  "6a6a14af8da6137b340675c6_dealality-old-home-freeform-head.v20260729w16.css",
  "6a6a19b23c529c0bd6a9e33f_dealality-old-home-freeform-head.v20260729w18.css"
);
fs.writeFileSync("tmp-freeform-head-w18.html", head);
const footer = `<script src="https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6a14b095a809ffc9b2eaa3_old-home-footer-oh.v20260729e.js"></script>
<!-- ohOpenOpportunityReview: hero Explore opens opportunity-review iframe modal -->
<!-- ohProblemV2: The Problem cards + fragmented evaluation animation -->
`;
fs.writeFileSync("tmp-freeform-footer-e.html", footer);
console.log("prepared freeform head chars", head.length);
