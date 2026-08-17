import fs from "fs";
import path from "path";
import crypto from "crypto";

const w19Url =
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6a2ae57222e6141191d651_dealality-old-home-freeform-head.v20260729w19.css";

let js = fs.readFileSync("public/marketing/old-home-problem-v2.v20260729a.js", "utf8");
js = js
  .replace(/https:\/\/cdn\.prod\.website-files\.com\/68108c29063eeb5d1bd7ae4a\/[^"']+freeform-head\.v20260729w\d+\.css/g, w19Url)
  .replace("PLACEHOLDER_w19.css", "6a6a2ae57222e6141191d651_dealality-old-home-freeform-head.v20260729w19.css");
const out = "public/marketing/old-home-problem-v2.v20260729b.js";
fs.writeFileSync(out, js);
const buf = fs.readFileSync(out);
console.log("js md5", crypto.createHash("md5").update(buf).digest("hex"), buf.length);
console.log("points w19", js.includes("w19.css"));

const cssDetails = {
  acl: "public-read",
  bucket: "webflow-prod-assets",
  xAmzAlgorithm: "AWS4-HMAC-SHA256",
  xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260729/us-east-1/s3/aws4_request",
  xAmzDate: "20260729T163133Z",
  key: "68108c29063eeb5d1bd7ae4a/6a6a2ae57222e6141191d651_dealality-old-home-freeform-head.v20260729w19.css",
  policy:
    "eyJleHBpcmF0aW9uIjoiMjAyNi0wNy0yOVQxNzozMTozM1oiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoidGV4dC9jc3MifSx7InN1Y2Nlc3NfYWN0aW9uX3N0YXR1cyI6IjIwMSJ9LFsic3RhcnRzLXdpdGgiLCIkQ29udGVudC1UeXBlIiwidGV4dC9jc3MiXSxbImNvbnRlbnQtbGVuZ3RoLXJhbmdlIiwwLDMxNDU3MjgwXSx7ImFjbCI6InB1YmxpYy1yZWFkIn0seyJidWNrZXQiOiJ3ZWJmbG93LXByb2QtYXNzZXRzIn0seyJYLUFtei1BbGdvcml0aG0iOiJBV1M0LUhNQUMtU0hBMjU2In0seyJYLUFtei1DcmVkZW50aWFsIjoiQUtJQVFMTEhXRDZNRUpHRVRMU1QvMjAyNjA3MjkvdXMtZWFzdC0xL3MzL2F3czRfcmVxdWVzdCJ9LHsiWC1BbXotRGF0ZSI6IjIwMjYwNzI5VDE2MzEzM1oifSx7ImtleSI6IjY4MTA4YzI5MDYzZWViNWQxYmQ3YWU0YS82YTZhMmFlNTcyMjJlNjE0MTE5MWQ2NTFfZGVhbGFsaXR5LW9sZC1ob21lLWZyZWVmb3JtLWhlYWQudjIwMjYwNzI5dzE5LmNzcyJ9XX0=",
  xAmzSignature: "f2387835bba0f583a0ccedbe54804339d39f839f52c431fbd0825eec0e33d884",
  successActionStatus: "201",
  contentType: "text/css",
  cacheControl: "max-age=31536000",
};

async function upload(fileRel, details) {
  const file = path.resolve(fileRel);
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
  console.log(path.basename(file), res.status);
}

await upload("public/marketing/dealality-old-home-freeform-head.v20260729w19.css", cssDetails);
console.log("css md5 used", crypto.createHash("md5").update(fs.readFileSync("public/marketing/dealality-old-home-freeform-head.v20260729w19.css")).digest("hex"));
