import fs from "fs";
import path from "path";

const uploads = [
  {
    file: "public/marketing/old-home-manual-process.v20260731j.html",
    uploadUrl: "https://webflow-prod-assets.s3.amazonaws.com/",
    uploadDetails: {
      acl: "public-read",
      bucket: "webflow-prod-assets",
      xAmzAlgorithm: "AWS4-HMAC-SHA256",
      xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260731/us-east-1/s3/aws4_request",
      xAmzDate: "20260731T222426Z",
      key: "68108c29063eeb5d1bd7ae4a/6a6d209a0e72bb4b73b584e0_old-home-manual-process.v20260731j.html",
      policy:
        "eyJleHBpcmF0aW9uIjoiMjAyNi0wNy0zMVQyMzoyNDoyNloiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoidGV4dC9odG1sIn0seyJzdWNjZXNzX2FjdGlvbl9zdGF0dXMiOiIyMDEifSxbInN0YXJ0cy13aXRoIiwiJENvbnRlbnQtVHlwZSIsInRleHQvaHRtbCJdLFsiY29udGVudC1sZW5ndGgtcmFuZ2UiLDAsMzE0NTcyODBdLHsiYWNsIjoicHVibGljLXJlYWQifSx7ImJ1Y2tldCI6IndlYmZsb3ctcHJvZC1hc3NldHMifSx7IlgtQW16LUFsZ29yaXRobSI6IkFXUzQtSE1BQy1TSEEyNTYifSx7IlgtQW16LUNyZWRlbnRpYWwiOiJBS0lBUUxMSFdENk1FSkdFVExTVC8yMDI2MDczMS91cy1lYXN0LTEvczMvYXdzNF9yZXF1ZXN0In0seyJYLUFtei1EYXRlIjoiMjAyNjA3MzFUMjIyNDI2WiJ9LHsia2V5IjoiNjgxMDhjMjkwNjNlZWI1ZDFiZDdhZTRhLzZhNmQyMDlhMGU3MmJiNGI3M2I1ODRlMF9vbGQtaG9tZS1tYW51YWwtcHJvY2Vzcy52MjAyNjA3MzFqLmh0bWwifV19",
      xAmzSignature: "54813aa17fd1ca9b5927f372a6d811c6b397cb93ffc3387a90f6997b99c3e3ce",
      successActionStatus: "201",
      contentType: "text/html",
      cacheControl: "max-age=31536000",
    },
  },
  {
    file: "public/marketing/old-home-manual-process.v20260731k.css",
    uploadUrl: "https://webflow-prod-assets.s3.amazonaws.com/",
    uploadDetails: {
      acl: "public-read",
      bucket: "webflow-prod-assets",
      xAmzAlgorithm: "AWS4-HMAC-SHA256",
      xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260731/us-east-1/s3/aws4_request",
      xAmzDate: "20260731T222426Z",
      key: "68108c29063eeb5d1bd7ae4a/6a6d209a3cdf91ccf55e43f0_old-home-manual-process.v20260731k.css",
      policy:
        "eyJleHBpcmF0aW9uIjoiMjAyNi0wNy0zMVQyMzoyNDoyNloiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoidGV4dC9jc3MifSx7InN1Y2Nlc3NfYWN0aW9uX3N0YXR1cyI6IjIwMSJ9LFsic3RhcnRzLXdpdGgiLCIkQ29udGVudC1UeXBlIiwidGV4dC9jc3MiXSxbImNvbnRlbnQtbGVuZ3RoLXJhbmdlIiwwLDMxNDU3MjgwXSx7ImFjbCI6InB1YmxpYy1yZWFkIn0seyJidWNrZXQiOiJ3ZWJmbG93LXByb2QtYXNzZXRzIn0seyJYLUFtei1BbGdvcml0aG0iOiJBV1M0LUhNQUMtU0hBMjU2In0seyJYLUFtei1DcmVkZW50aWFsIjoiQUtJQVFMTEhXRDZNRUpHRVRMU1QvMjAyNjA3MzEvdXMtZWFzdC0xL3MzL2F3czRfcmVxdWVzdCJ9LHsiWC1BbXotRGF0ZSI6IjIwMjYwNzMxVDIyMjQyNloifSx7ImtleSI6IjY4MTA4YzI5MDYzZWViNWQxYmQ3YWU0YS82YTZkMjA5YTNjZGY5MWNjZjU1ZTQzZjBfb2xkLWhvbWUtbWFudWFsLXByb2Nlc3MudjIwMjYwNzMxay5jc3MifV19",
      xAmzSignature: "4211445ad872f252b9d14d5c5508de6cbf63058643190249aa1c014fca5b5d2d",
      successActionStatus: "201",
      contentType: "text/css",
      cacheControl: "max-age=31536000",
    },
  },
];

for (const meta of uploads) {
  const file = fs.readFileSync(meta.file);
  const d = meta.uploadDetails;
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
  form.append("file", new Blob([file], { type: d.contentType }), path.basename(meta.file));
  const res = await fetch(meta.uploadUrl, { method: "POST", body: form });
  console.log(path.basename(meta.file), res.status);
}
