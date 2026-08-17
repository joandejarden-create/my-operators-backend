import fs from "fs";
import path from "path";

const uploads = [
  {
    file: "public/marketing/dealality-old-home-freeform-head.v20260729w16.css",
    uploadUrl: "https://webflow-prod-assets.s3.amazonaws.com/",
    uploadDetails: {
      acl: "public-read",
      bucket: "webflow-prod-assets",
      xAmzAlgorithm: "AWS4-HMAC-SHA256",
      xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260729/us-east-1/s3/aws4_request",
      xAmzDate: "20260729T151449Z",
      key: "68108c29063eeb5d1bd7ae4a/6a6a18e9e0ff4ffc45c68246_dealality-old-home-freeform-head.v20260729w16.css",
      policy:
        "eyJleHBpcmF0aW9uIjoiMjAyNi0wNy0yOVQxNjoxNDo0OVoiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoidGV4dC9jc3MifSx7InN1Y2Nlc3NfYWN0aW9uX3N0YXR1cyI6IjIwMSJ9LFsic3RhcnRzLXdpdGgiLCIkQ29udGVudC1UeXBlIiwidGV4dC9jc3MiXSxbImNvbnRlbnQtbGVuZ3RoLXJhbmdlIiwwLDMxNDU3MjgwXSx7ImFjbCI6InB1YmxpYy1yZWFkIn0seyJidWNrZXQiOiJ3ZWJmbG93LXByb2QtYXNzZXRzIn0seyJYLUFtei1BbGdvcml0aG0iOiJBV1M0LUhNQUMtU0hBMjU2In0seyJYLUFtei1DcmVkZW50aWFsIjoiQUtJQVFMTEhXRDZNRUpHRVRMU1QvMjAyNjA3MjkvdXMtZWFzdC0xL3MzL2F3czRfcmVxdWVzdCJ9LHsiWC1BbXotRGF0ZSI6IjIwMjYwNzI5VDE1MTQ0OVoifSx7ImtleSI6IjY4MTA4YzI5MDYzZWViNWQxYmQ3YWU0YS82YTZhMThlOWUwZmY0ZmZjNDVjNjgyNDZfZGVhbGFsaXR5LW9sZC1ob21lLWZyZWVmb3JtLWhlYWQudjIwMjYwNzI5dzE2LmNzcyJ9XX0=",
      xAmzSignature: "20f7651f39efd147442fc6a850bf17aa6842e7ebf62f96b386787359d3991b68",
      successActionStatus: "201",
      contentType: "text/css",
      cacheControl: "max-age=31536000",
    },
  },
  {
    file: "public/marketing/old-home-footer-oh.v20260729e.js",
    uploadUrl: "https://webflow-prod-assets.s3.amazonaws.com/",
    uploadDetails: {
      acl: "public-read",
      bucket: "webflow-prod-assets",
      xAmzAlgorithm: "AWS4-HMAC-SHA256",
      xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260729/us-east-1/s3/aws4_request",
      xAmzDate: "20260729T151449Z",
      key: "68108c29063eeb5d1bd7ae4a/6a6a18e98c3961137df0aa97_old-home-footer-oh.v20260729e.js",
      policy:
        "eyJleHBpcmF0aW9uIjoiMjAyNi0wNy0yOVQxNjoxNDo0OVoiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoiYXBwbGljYXRpb24vamF2YXNjcmlwdCJ9LHsic3VjY2Nlc3NfYWN0aW9uX3N0YXR1cyI6IjIwMSJ9LFsic3RhcnRzLXdpdGgiLCIkQ29udGVudC1UeXBlIiwiYXBwbGljYXRpb24vamF2YXNjcmlwdCJdLFsiY29udGVudC1sZW5ndGgtcmFuZ2UiLDAsMzE0NTcyODBdLHsiYWNsIjoicHVibGljLXJlYWQifSx7ImJ1Y2tldCI6IndlYmZsb3ctcHJvZC1hc3NldHMifSx7IlgtQW16LUFsZ29yaXRobSI6IkFXUzQtSE1BQy1TSEEyNTYifSx7IlgtQW16LUNyZWRlbnRpYWwiOiJBS0lBUUxMSFdENk1FSkdFVExTVC8yMDI2MDcyOS91cy1lYXN0LTMvczMvYXdzNF9yZXF1ZXN0In0seyJYLUFtei1EYXRlIjoiMjAyNjA3MjlUMTUxNDQ5WiJ9LHsia2V5IjoiNjgxMDhjMjkwNjNlZWI1ZDFiZDdhZTRhLzZhNmExOGU5OGMzOTYxMTM3ZGYwYWE5N19vbGQtaG9tZS1mb290ZXItb2gudjIwMjYwNzI5ZS5qcyJ9XX0=",
      xAmzSignature: "88cbd4cf9d38151d731595a7d7c818a0cdd6c723b3cf9d6574d7cce4039b619c",
      successActionStatus: "201",
      contentType: "application/javascript",
      cacheControl: "max-age=31536000",
    },
  },
];

for (const u of uploads) {
  const fileBytes = fs.readFileSync(u.file);
  const d = u.uploadDetails;
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
  form.append(
    "file",
    new Blob([fileBytes], { type: d.contentType }),
    path.basename(u.file)
  );
  const res = await fetch(u.uploadUrl, { method: "POST", body: form });
  const text = await res.text();
  console.log(path.basename(u.file), res.status, text.slice(0, 300));
}
