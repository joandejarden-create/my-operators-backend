import fs from "fs";
import path from "path";

const details = {
  acl: "public-read",
  bucket: "webflow-prod-assets",
  xAmzAlgorithm: "AWS4-HMAC-SHA256",
  xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260729/us-east-1/s3/aws4_request",
  xAmzDate: "20260729T154827Z",
  key: "68108c29063eeb5d1bd7ae4a/6a6a20cbfb1bb5c13b407f85_dealality-site-footer-auth.v20260729a.js",
  policy:
    "eyJleHBpcmF0aW9uIjoiMjAyNi0wNy0yOVQxNjo0ODoyN1oiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoiYXBwbGljYXRpb24vamF2YXNjcmlwdCJ9LHsic3VjY2Vzc19hY3Rpb25fc3RhdHVzIjoiMjAxIn0sWyJzdGFydHMtd2l0aCIsIiRDb250ZW50LVR5cGUiLCJhcHBsaWNhdGlvbi9qYXZhc2NyaXB0Il0sWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsMCwzMTQ1NzI4MF0seyJhY2wiOiJwdWJsaWMtcmVhZCJ9LHsiYnVja2V0Ijoid2ViZmxvdy1wcm9kLWFzc2V0cyJ9LHsiWC1BbXotQWxnb3JpdGhtIjoiQVdTNC1ITUFDLVNIQTI1NiJ9LHsiWC1BbXotQ3JlZGVudGlhbCI6IkFLSUFRTExIV0Q2TUVKR0VUTFNULzIwMjYwNzI5L3VzLWVhc3QtMS9zMy9hd3M0X3JlcXVlc3QifSx7IlgtQW16LURhdGUiOiIyMDI2MDcyOVQxNTQ4MjdaIn0seyJrZXkiOiI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvNmE2YTIwY2JmYjFiYjVjMTNiNDA3Zjg1X2RlYWxhbGl0eS1zaXRlLWZvb3Rlci1hdXRoLnYyMDI2MDcyOWEuanMifV19",
  xAmzSignature: "48bcd2a2175f3611a42233b60ed773ba56facda9fd703320a6c457a8bd69641b",
  successActionStatus: "201",
  contentType: "application/javascript",
  cacheControl: "max-age=31536000",
};

const file = path.resolve("public/marketing/dealality-site-footer-auth.v20260729a.js");
const bytes = fs.readFileSync(file);
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
form.append("file", new Blob([bytes], { type: details.contentType }), path.basename(file));
const res = await fetch("https://webflow-prod-assets.s3.amazonaws.com/", { method: "POST", body: form });
console.log("upload", res.status, (await res.text()).slice(0, 120));
