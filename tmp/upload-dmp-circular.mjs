import fs from "fs";
import path from "path";

const uploads = [
  {
    file: "public/marketing/old-home-manual-process.v20260731k.html",
    uploadDetails: {
      acl: "public-read",
      bucket: "webflow-prod-assets",
      xAmzAlgorithm: "AWS4-HMAC-SHA256",
      xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260731/us-east-1/s3/aws4_request",
      xAmzDate: "20260731T223708Z",
      key: "68108c29063eeb5d1bd7ae4a/6a6d2394deb6d514bfbdf4b7_old-home-manual-process.v20260731k.html",
      policy:
        "eyJleHBpcmF0aW9uIjoiMjAyNi0wNy0zMVQyMzozNzowOFoiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoidGV4dC9odG1sIn0seyJzdWNjZXNzX2FjdGlvbl9zdGF0dXMiOiIyMDEifSxbInN0YXJ0cy13aXRoIiwiJENvbnRlbnQtVHlwZSIsInRleHQvaHRtbCJdLFsiY29udGVudC1sZW5ndGgtcmFuZ2UiLDAsMzE0NTcyODBdLHsiYWNsIjoicHVibGljLXJlYWQifSx7ImJ1Y2tldCI6IndlYmZsb3ctcHJvZC1hc3NldHMifSx7IlgtQW16LUFsZ29yaXRobSI6IkFXUzQtSE1BQy1TSEEyNTYifSx7IlgtQW16LUNyZWRlbnRpYWwiOiJBS0lBUUxMSFdENk1FSkdFVExTVC8yMDI2MDczMS91cy1lYXN0LTEvczMvYXdzNF9yZXF1ZXN0In0seyJYLUFtei1EYXRlIjoiMjAyNjA3MzFUMjIzNzA4WiJ9LHsia2V5IjoiNjgxMDhjMjkwNjNlZWI1ZDFiZDdhZTRhLzZhNmQyMzk0ZGViNmQ1MTRiZmJkZjRiN19vbGQtaG9tZS1tYW51YWwtcHJvY2Vzcy52MjAyNjA3MzFrLmh0bWwifV19",
      xAmzSignature: "edf9bc35090cb7f8b990b69f1aac2ba7e7c365521bdce1124abfe222c384d3c0",
      successActionStatus: "201",
      contentType: "text/html",
      cacheControl: "max-age=31536000",
    },
  },
  {
    file: "public/marketing/old-home-manual-process.v20260731l.css",
    uploadDetails: {
      acl: "public-read",
      bucket: "webflow-prod-assets",
      xAmzAlgorithm: "AWS4-HMAC-SHA256",
      xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260731/us-east-1/s3/aws4_request",
      xAmzDate: "20260731T223709Z",
      key: "68108c29063eeb5d1bd7ae4a/6a6d2395e8d817b263df5cd0_old-home-manual-process.v20260731l.css",
      policy:
        "eyJleHBpcmF0aW9uIjoiMjAyNi0wNy0zMVQyMzozNzowOVoiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoidGV4dC9jc3MifSx7InN1Y2Nlc3NfYWN0aW9uX3N0YXR1cyI6IjIwMSJ9LFsic3RhcnRzLXdpdGgiLCIkQ29udGVudC1UeXBlIiwidGV4dC9jc3MiXSxbImNvbnRlbnQtbGVuZ3RoLXJhbmdlIiwwLDMxNDU3MjgwXSx7ImFjbCI6InB1YmxpYy1yZWFkIn0seyJidWNrZXQiOiJ3ZWJmbG93LXByb2QtYXNzZXRzIn0seyJYLUFtei1BbGdvcml0aG0iOiJBV1M0LUhNQUMtU0hBMjU2In0seyJYLUFtei1DcmVkZW50aWFsIjoiQUtJQVFMTEhXRDZNRUpHRVRMU1QvMjAyNjA3MzEvdXMtZWFzdC0xL3MzL2F3czRfcmVxdWVzdCJ9LHsiWC1BbXotRGF0ZSI6IjIwMjYwNzMxVDIyMzcwOVoifSx7ImtleSI6IjY4MTA4YzI5MDYzZWViNWQxYmQ3YWU0YS82YTZkMjM5NWU4ZDgxN2IyNjNkZjVjZDBfb2xkLWhvbWUtbWFudWFsLXByb2Nlc3MudjIwMjYwNzMxbC5jc3MifV19",
      xAmzSignature: "c68b17db961110f31f87f9e4a5614a826d87d2b5a68c3a96f057e9240787e500",
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
  const res = await fetch("https://webflow-prod-assets.s3.amazonaws.com/", {
    method: "POST",
    body: form,
  });
  console.log(path.basename(meta.file), res.status);
}
