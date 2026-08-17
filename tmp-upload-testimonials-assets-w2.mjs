import fs from "fs";
import axios from "axios";
import FormData from "form-data";

const uploads = [
  {
    file: "public/marketing/dealality-old-home-freeform-head.v20260729w.css",
    uploadUrl: "https://webflow-prod-assets.s3.amazonaws.com/",
    uploadDetails: {
      acl: "public-read",
      bucket: "webflow-prod-assets",
      xAmzAlgorithm: "AWS4-HMAC-SHA256",
      xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260729/us-east-1/s3/aws4_request",
      xAmzDate: "20260729T074243Z",
      key: "68108c29063eeb5d1bd7ae4a/6a69aef357bb4e4a3eaaadb3_dealality-old-home-freeform-head.v20260729w2.css",
      policy:
        "eyJleHBpcmF0aW9uIjoiMjAyNi0wNy0yOVQwODo0Mjo0M1oiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoidGV4dC9jc3MifSx7InN1Y2Nlc3NfYWN0aW9uX3N0YXR1cyI6IjIwMSJ9LFsic3RhcnRzLXdpdGgiLCIkQ29udGVudC1UeXBlIiwidGV4dC9jc3MiXSxbImNvbnRlbnQtbGVuZ3RoLXJhbmdlIiwwLDMxNDU3MjgwXSx7ImFjbCI6InB1YmxpYy1yZWFkIn0seyJidWNrZXQiOiJ3ZWJmbG93LXByb2QtYXNzZXRzIn0seyJYLUFtei1BbGdvcml0aG0iOiJBV1M0LUhNQUMtU0hBMjU2In0seyJYLUFtei1DcmVkZW50aWFsIjoiQUtJQVFMTEhXRDZNRUpHRVRMU1QvMjAyNjA3MjkvdXMtZWFzdC0xL3MzL2F3czRfcmVxdWVzdCJ9LHsiWC1BbXotRGF0ZSI6IjIwMjYwNzI5VDA3NDI0M1oifSx7ImtleSI6IjY4MTA4YzI5MDYzZWViNWQxYmQ3YWU0YS82YTY5YWVmMzU3YmI0ZTRhM2VhYWFkYjNfZGVhbGFsaXR5LW9sZC1ob21lLWZyZWVmb3JtLWhlYWQudjIwMjYwNzI5dzIuY3NzIn1dfQ==",
      xAmzSignature: "6eb548c2fcca0a4bae98006d3c288803acbe9225604284cf1b6261566f4bfa84",
      successActionStatus: "201",
      contentType: "text/css",
      cacheControl: "max-age=31536000",
    },
  },
  {
    file: "public/marketing/dealality-old-home-testimonials.js",
    uploadUrl: "https://webflow-prod-assets.s3.amazonaws.com/",
    uploadDetails: {
      acl: "public-read",
      bucket: "webflow-prod-assets",
      xAmzAlgorithm: "AWS4-HMAC-SHA256",
      xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260729/us-east-1/s3/aws4_request",
      xAmzDate: "20260729T074244Z",
      key: "68108c29063eeb5d1bd7ae4a/6a69aef48c6beb6b7c28c051_dealality-old-home-testimonials.v20260729w3.js",
      policy:
        "eyJleHBpcmF0aW9uIjoiMjAyNi0wNy0yOVQwODo0Mjo0NFoiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoiYXBwbGljYXRpb24vamF2YXNjcmlwdCJ9LHsic3VjY2Vzc19hY3Rpb25fc3RhdHVzIjoiMjAxIn0sWyJzdGFydHMtd2l0aCIsIiRDb250ZW50LVR5cGUiLCJhcHBsaWNhdGlvbi9qYXZhc2NyaXB0Il0sWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsMCwzMTQ1NzI4MF0seyJhY2wiOiJwdWJsaWMtcmVhZCJ9LHsiYnVja2V0Ijoid2ViZmxvdy1wcm9kLWFzc2V0cyJ9LHsiWC1BbXotQWxnb3JpdGhtIjoiQVdTNC1ITUFDLVNIQTI1NiJ9LHsiWC1BbXotQ3JlZGVudGlhbCI6IkFLSUFRTExIV0Q2TUVKR0VUTFNULzIwMjYwNzI5L3VzLWVhc3QtMS9zMy9hd3M0X3JlcXVlc3QifSx7IlgtQW16LURhdGUiOiIyMDI2MDcyOVQwNzQyNDRaIn0seyJrZXkiOiI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvNmE2OWFlZjQ4YzZiZWI2YjdjMjhjMDUxX2RlYWxhbGl0eS1vbGQtaG9tZS10ZXN0aW1vbmlhbHMudjIwMjYwNzI5dzMuanMifV19",
      xAmzSignature: "afab3db18e45520f66e12487eb3051bfdecb2607647d3d0a868c5f8bfea671c8",
      successActionStatus: "201",
      contentType: "application/javascript",
      cacheControl: "max-age=31536000",
    },
  },
];

for (const item of uploads) {
  const fileData = fs.readFileSync(item.file);
  const form = new FormData();
  const d = item.uploadDetails;
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
  form.append("file", fileData, {
    filename: item.file.split("/").pop(),
    contentType: d.contentType,
  });
  const res = await axios.post(item.uploadUrl, form, {
    headers: form.getHeaders(),
    maxBodyLength: Infinity,
    validateStatus: () => true,
  });
  console.log(item.file, res.status);
  if (res.status !== 201) process.exit(1);
}

console.log("ok");
