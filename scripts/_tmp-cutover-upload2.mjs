import fs from "fs";
import path from "path";

const ROOT = "C:/Dev/deal-capture-proxy/public/marketing";
const ASSETS = [
  {
    file: "old-home-section-order.v20260801a.js",
    id: "6a6e2c86d2843e12337cb105",
    uploadDetails: {
      acl: "public-read",
      bucket: "webflow-prod-assets",
      xAmzAlgorithm: "AWS4-HMAC-SHA256",
      xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260801/us-east-1/s3/aws4_request",
      xAmzDate: "20260801T172734Z",
      key: "68108c29063eeb5d1bd7ae4a/6a6e2c86d2843e12337cb105_old-home-section-order.v20260801a.js",
      policy:
        "eyJleHBpcmF0aW9uIjoiMjAyNi0wOC0wMVQxODoyNzozNFoiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoiYXBwbGljYXRpb24vamF2YXNjcmlwdCJ9LHsic3VjY2Vzc19hY3Rpb25fc3RhdHVzIjoiMjAxIn0sWyJzdGFydHMtd2l0aCIsIiRDb250ZW50LVR5cGUiLCJhcHBsaWNhdGlvbi9qYXZhc2NyaXB0Il0sWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsMCwzMTQ1NzI4MF0seyJhY2wiOiJwdWJsaWMtcmVhZCJ9LHsiYnVja2V0Ijoid2ViZmxvdy1wcm9kLWFzc2V0cyJ9LHsiWC1BbXotQWxnb3JpdGhtIjoiQVdTNC1ITUFDLVNIQTI1NiJ9LHsiWC1BbXotQ3JlZGVudGlhbCI6IkFLSUFRTExIV0Q2TUVKR0VUTFNULzIwMjYwODAxL3VzLWVhc3QtMS9zMy9hd3M0X3JlcXVlc3QifSx7IlgtQW16LURhdGUiOiIyMDI2MDgwMVQxNzI3MzRaIn0seyJrZXkiOiI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvNmE2ZTJjODZkMjg0M2UxMjMzN2NiMTA1X29sZC1ob21lLXNlY3Rpb24tb3JkZXIudjIwMjYwODAxYS5qcyJ9XX0=",
      xAmzSignature: "f3c22c84decebfc1b03efc1df04876ac221ce0fc560c75dd7c980c2ef36de3a1",
      successActionStatus: "201",
      contentType: "application/javascript",
      cacheControl: "max-age=31536000",
    },
  },
  {
    file: "old-home-problem-storyboard.v20260801a.js",
    id: "6a6e2c86fa76208fc4c12077",
    uploadDetails: {
      acl: "public-read",
      bucket: "webflow-prod-assets",
      xAmzAlgorithm: "AWS4-HMAC-SHA256",
      xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260801/us-east-1/s3/aws4_request",
      xAmzDate: "20260801T172734Z",
      key: "68108c29063eeb5d1bd7ae4a/6a6e2c86fa76208fc4c12077_old-home-problem-storyboard.v20260801a.js",
      policy:
        "eyJleHBpcmF0aW9uIjoiMjAyNi0wOC0wMVQxODoyNzozNFoiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoiYXBwbGljYXRpb24vamF2YXNjcmlwdCJ9LHsic3VjY2Vzc19hY3Rpb25fc3RhdHVzIjoiMjAxIn0sWyJzdGFydHMtd2l0aCIsIiRDb250ZW50LVR5cGUiLCJhcHBsaWNhdGlvbi9qYXZhc2NyaXB0Il0sWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsMCwzMTQ1NzI4MF0seyJhY2wiOiJwdWJsaWMtcmVhZCJ9LHsiYnVja2V0Ijoid2ViZmxvdy1wcm9kLWFzc2V0cyJ9LHsiWC1BbXotQWxnb3JpdGhtIjoiQVdTNC1ITUFDLVNIQTI1NiJ9LHsiWC1BbXotQ3JlZGVudGlhbCI6IkFLSUFRTExIV0Q2TUVKR0VUTFNULzIwMjYwODAxL3VzLWVhc3QtMS9zMy9hd3M0X3JlcXVlc3QifSx7IlgtQW16LURhdGUiOiIyMDI2MDgwMVQxNzI3MzRaIn0seyJrZXkiOiI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvNmE2ZTJjODZmYTc2MjA4ZmM0YzEyMDc3X29sZC1ob21lLXByb2JsZW0tc3Rvcnlib2FyZC52MjAyNjA4MDFhLmpzIn1dfQ==",
      xAmzSignature: "f707653eafdf964e85180d769439ab9b52e0141002bcb5938590a7fd9a8f16bc",
      successActionStatus: "201",
      contentType: "application/javascript",
      cacheControl: "max-age=31536000",
    },
  },
  {
    file: "old-home-modules-icons.v20260801a.js",
    id: "6a6e2c86291ad17ff6f4df65",
    uploadDetails: {
      acl: "public-read",
      bucket: "webflow-prod-assets",
      xAmzAlgorithm: "AWS4-HMAC-SHA256",
      xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260801/us-east-1/s3/aws4_request",
      xAmzDate: "20260801T172734Z",
      key: "68108c29063eeb5d1bd7ae4a/6a6e2c86291ad17ff6f4df65_old-home-modules-icons.v20260801a.js",
      policy:
        "eyJleHBpcmF0aW9uIjoiMjAyNi0wOC0wMVQxODoyNzozNFoiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoiYXBwbGljYXRpb24vamF2YXNjcmlwdCJ9LHsic3VjY2Vzc19hY3Rpb25fc3RhdHVzIjoiMjAxIn0sWyJzdGFydHMtd2l0aCIsIiRDb250ZW50LVR5cGUiLCJhcHBsaWNhdGlvbi9qYXZhc2NyaXB0Il0sWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsMCwzMTQ1NzI4MF0seyJhY2wiOiJwdWJsaWMtcmVhZCJ9LHsiYnVja2V0Ijoid2ViZmxvdy1wcm9kLWFzc2V0cyJ9LHsiWC1BbXotQWxnb3JpdGhtIjoiQVdTNC1ITUFDLVNIQTI1NiJ9LHsiWC1BbXotQ3JlZGVudGlhbCI6IkFLSUFRTExIV0Q2TUVKR0VUTFNULzIwMjYwODAxL3VzLWVhc3QtMS9zMy9hd3M0X3JlcXVlc3QifSx7IlgtQW16LURhdGUiOiIyMDI2MDgwMVQxNzI3MzRaIn0seyJrZXkiOiI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvNmE2ZTJjODYyOTFhZDE3ZmY2ZjRkZjY1X29sZC1ob21lLW1vZHVsZXMtaWNvbnMudjIwMjYwODAxYS5qcyJ9XX0=",
      xAmzSignature: "b6d2e3d43daa6d591fdcd0eb1248d161b102796258b2f7f8752dcb512a171fb0",
      successActionStatus: "201",
      contentType: "application/javascript",
      cacheControl: "max-age=31536000",
    },
  },
  {
    file: "old-home-modules-copy.v20260801a.js",
    id: "6a6e2c87183fbba14fc80533",
    uploadDetails: {
      acl: "public-read",
      bucket: "webflow-prod-assets",
      xAmzAlgorithm: "AWS4-HMAC-SHA256",
      xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260801/us-east-1/s3/aws4_request",
      xAmzDate: "20260801T172735Z",
      key: "68108c29063eeb5d1bd7ae4a/6a6e2c87183fbba14fc80533_old-home-modules-copy.v20260801a.js",
      policy:
        "eyJleHBpcmF0aW9uIjoiMjAyNi0wOC0wMVQxODoyNzozNVoiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoiYXBwbGljYXRpb24vamF2YXNjcmlwdCJ9LHsic3VjY2Vzc19hY3Rpb25fc3RhdHVzIjoiMjAxIn0sWyJzdGFydHMtd2l0aCIsIiRDb250ZW50LVR5cGUiLCJhcHBsaWNhdGlvbi9qYXZhc2NyaXB0Il0sWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsMCwzMTQ1NzI4MF0seyJhY2wiOiJwdWJsaWMtcmVhZCJ9LHsiYnVja2V0Ijoid2ViZmxvdy1wcm9kLWFzc2V0cyJ9LHsiWC1BbXotQWxnb3JpdGhtIjoiQVdTNC1ITUFDLVNIQTI1NiJ9LHsiWC1BbXotQ3JlZGVudGlhbCI6IkFLSUFRTExIV0Q2TUVKR0VUTFNULzIwMjYwODAxL3VzLWVhc3QtMS9zMy9hd3M0X3JlcXVlc3QifSx7IlgtQW16LURhdGUiOiIyMDI2MDgwMVQxNzI3MzVaIn0seyJrZXkiOiI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvNmE2ZTJjODcxODNmYmJhMTRmYzgwNTMzX29sZC1ob21lLW1vZHVsZXMtY29weS52MjAyNjA4MDFhLmpzIn1dfQ==",
      xAmzSignature: "0fca29b28dab2535493920b72ef19169b4e608f1315c3daf7f5356aa41370e0c",
      successActionStatus: "201",
      contentType: "application/javascript",
      cacheControl: "max-age=31536000",
    },
  },
];

for (const asset of ASSETS) {
  const buf = fs.readFileSync(path.join(ROOT, asset.file));
  const d = asset.uploadDetails;
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
  form.append("file", new Blob([buf], { type: d.contentType }), asset.file);
  const res = await fetch("https://webflow-prod-assets.s3.amazonaws.com/", {
    method: "POST",
    body: form,
  });
  console.log(res.status, asset.file, asset.id);
}
