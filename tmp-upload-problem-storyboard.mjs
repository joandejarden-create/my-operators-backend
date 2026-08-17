import fs from "fs";
import FormData from "form-data";
import axios from "axios";

async function upload(filePath, fileName, details) {
  const fileData = fs.readFileSync(filePath);
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
  form.append("file", fileData, {
    filename: fileName,
    contentType: details.contentType,
  });
  const res = await axios.post(
    "https://webflow-prod-assets.s3.amazonaws.com/",
    form,
    {
      headers: form.getHeaders(),
      maxBodyLength: Infinity,
      validateStatus: () => true,
    }
  );
  console.log(fileName, res.status);
  if (res.status !== 201) {
    console.log(String(res.data).slice(0, 400));
    process.exit(1);
  }
}

const cssDetails = {
  acl: "public-read",
  bucket: "webflow-prod-assets",
  xAmzAlgorithm: "AWS4-HMAC-SHA256",
  xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260729/us-east-1/s3/aws4_request",
  xAmzDate: "20260729T211322Z",
  key: "68108c29063eeb5d1bd7ae4a/6a6a6cf2fdc1b486e861e6c9_old-home-problem-storyboard.v20260729a.css",
  policy:
    "eyJleHBpcmF0aW9uIjoiMjAyNi0wNy0yOVQyMjoxMzoyMloiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoidGV4dC9jc3MifSx7InN1Y2Nlc3NfYWN0aW9uX3N0YXR1cyI6IjIwMSJ9LFsic3RhcnRzLXdpdGgiLCIkQ29udGVudC1UeXBlIiwidGV4dC9jc3MiXSxbImNvbnRlbnQtbGVuZ3RoLXJhbmdlIiwwLDMxNDU3MjgwXSx7ImFjbCI6InB1YmxpYy1yZWFkIn0seyJidWNrZXQiOiJ3ZWJmbG93LXByb2QtYXNzZXRzIn0seyJYLUFtei1BbGdvcml0aG0iOiJBV1M0LUhNQUMtU0hBMjU2In0seyJYLUFtei1DcmVkZW50aWFsIjoiQUtJQVFMTEhXRDZNRUpHRVRMU1QvMjAyNjA3MjkvdXMtZWFzdC0xL3MzL2F3czRfcmVxdWVzdCJ9LHsiWC1BbXotRGF0ZSI6IjIwMjYwNzI5VDIxMTMyMloifSx7ImtleSI6IjY4MTA4YzI5MDYzZWViNWQxYmQ3YWU0YS82YTZhNmNmMmZkYzFiNDg2ZTg2MWU2Yzlfb2xkLWhvbWUtcHJvYmxlbS1zdG9yeWJvYXJkLnYyMDI2MDcyOWEuY3NzIn1dfQ==",
  xAmzSignature: "e997be7245c4fae37e51bd5ec617f0aa6bac83a8bc98d4d8b2ac302a154e7c7c",
  successActionStatus: "201",
  contentType: "text/css",
  cacheControl: "max-age=31536000",
};

const jsDetails = {
  acl: "public-read",
  bucket: "webflow-prod-assets",
  xAmzAlgorithm: "AWS4-HMAC-SHA256",
  xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260729/us-east-1/s3/aws4_request",
  xAmzDate: "20260729T211952Z",
  key: "68108c29063eeb5d1bd7ae4a/6a6a6e7838eb4605a5bcdfbb_old-home-problem-storyboard.v20260729a.js",
  policy:
    "eyJleHBpcmF0aW9uIjoiMjAyNi0wNy0yOVQyMjoxOTo1MloiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoiYXBwbGljYXRpb24vamF2YXNjcmlwdCJ9LHsic3VjY2Vzc19hY3Rpb25fc3RhdHVzIjoiMjAxIn0sWyJzdGFydHMtd2l0aCIsIiRDb250ZW50LVR5cGUiLCJhcHBsaWNhdGlvbi9qYXZhc2NyaXB0Il0sWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsMCwzMTQ1NzI4MF0seyJhY2wiOiJwdWJsaWMtcmVhZCJ9LHsiYnVja2V0Ijoid2ViZmxvdy1wcm9kLWFzc2V0cyJ9LHsiWC1BbXotQWxnb3JpdGhtIjoiQVdTNC1ITUFDLVNIQTI1NiJ9LHsiWC1BbXotQ3JlZGVudGlhbCI6IkFLSUFRTExIV0Q2TUVKR0VUTFNULzIwMjYwNzI5L3VzLWVhc3QtMS9zMy9hd3M0X3JlcXVlc3QifSx7IlgtQW16LURhdGUiOiIyMDI2MDcyOVQyMTE5NTJaIn0seyJrZXkiOiI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvNmE2YTZlNzgzOGViNDYwNWE1YmNkZmJiX29sZC1ob21lLXByb2JsZW0tc3Rvcnlib2FyZC52MjAyNjA3MjlhLmpzIn1dfQ==",
  xAmzSignature: "a3b21c72fbc21081d6e1e9d7cfaa7ff9c8068d6ef0aa9161ecd13f70c8054b09",
  successActionStatus: "201",
  contentType: "application/javascript",
  cacheControl: "max-age=31536000",
};

await upload(
  "public/marketing/old-home-problem-storyboard.v20260729a.css",
  "old-home-problem-storyboard.v20260729a.css",
  cssDetails
);
await upload(
  "public/marketing/old-home-problem-storyboard.v20260729a.js",
  "old-home-problem-storyboard.v20260729a.js",
  jsDetails
);
console.log("OK");
