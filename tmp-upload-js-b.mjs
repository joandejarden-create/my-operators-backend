import fs from "fs";
import FormData from "form-data";
import axios from "axios";

const details = {
  acl: "public-read",
  bucket: "webflow-prod-assets",
  xAmzAlgorithm: "AWS4-HMAC-SHA256",
  xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260729/us-east-1/s3/aws4_request",
  xAmzDate: "20260729T220257Z",
  key: "68108c29063eeb5d1bd7ae4a/6a6a78910fc7ccd49a1c9617_old-home-problem-storyboard.v20260729b.js",
  policy:
    "eyJleHBpcmF0aW9uIjoiMjAyNi0wNy0yOVQyMzowMjo1N1oiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoiYXBwbGljYXRpb24vamF2YXNjcmlwdCJ9LHsic3VjY2Vzc19hY3Rpb25fc3RhdHVzIjoiMjAxIn0sWyJzdGFydHMtd2l0aCIsIiRDb250ZW50LVR5cGUiLCJhcHBsaWNhdGlvbi9qYXZhc2NyaXB0Il0sWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsMCwzMTQ1NzI4MF0seyJhY2wiOiJwdWJsaWMtcmVhZCJ9LHsiYnVja2V0Ijoid2ViZmxvdy1wcm9kLWFzc2V0cyJ9LHsiWC1BbXotQWxnb3JpdGhtIjoiQVdTNC1ITUFDLVNIQTI1NiJ9LHsiWC1BbXotQ3JlZGVudGlhbCI6IkFLSUFRTExIV0Q2TUVKR0VUTFNULzIwMjYwNzI5L3VzLWVhc3QtMS9zMy9hd3M0X3JlcXVlc3QifSx7IlgtQW16LURhdGUiOiIyMDI2MDcyOVQyMjAyNTdaIn0seyJrZXkiOiI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvNmE2YTc4OTEwZmM3Y2NkNDlhMWM5NjE3X29sZC1ob21lLXByb2JsZW0tc3Rvcnlib2FyZC52MjAyNjA3MjliLmpzIn1dfQ==",
  xAmzSignature: "fb96e44a165ba45d70b83ef9ab5c267f5c0ac50403645a797523cdb5b1b09382",
  successActionStatus: "201",
  contentType: "application/javascript",
  cacheControl: "max-age=31536000",
};

const fileData = fs.readFileSync(
  "public/marketing/old-home-problem-storyboard.v20260729b.js"
);
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
  filename: "old-home-problem-storyboard.v20260729b.js",
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
console.log(res.status);
if (res.status !== 201) {
  console.log(String(res.data).slice(0, 400));
  process.exit(1);
}
console.log(
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6a78910fc7ccd49a1c9617_old-home-problem-storyboard.v20260729b.js"
);
