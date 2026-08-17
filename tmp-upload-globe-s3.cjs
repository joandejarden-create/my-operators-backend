const fs = require("fs");
const path = require("path");

const uploadUrl = "https://webflow-prod-assets.s3.amazonaws.com/";
const uploadDetails = {
  acl: "public-read",
  bucket: "webflow-prod-assets",
  xAmzAlgorithm: "AWS4-HMAC-SHA256",
  xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260728/us-east-1/s3/aws4_request",
  xAmzDate: "20260728T212535Z",
  key: "68108c29063eeb5d1bd7ae4a/6a691e4f3b0bf638b1052fc6_dealality-globe-texture.jpg",
  policy:
    "eyJleHBpcmF0aW9uIjoiMjAyNi0wNy0yOFQyMjoyNTozNVoiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoiaW1hZ2UvanBlZyJ9LHsic3VjY2Vzc19hY3Rpb25fc3RhdHVzIjoiMjAxIn0sWyJzdGFydHMtd2l0aCIsIiRDb250ZW50LVR5cGUiLCJpbWFnZS9qcGVnIl0sWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsMCwzMTQ1NzI4MF0seyJhY2wiOiJwdWJsaWMtcmVhZCJ9LHsiYnVja2V0Ijoid2ViZmxvdy1wcm9kLWFzc2V0cyJ9LHsiWC1BbXotQWxnb3JpdGhtIjoiQVdTNC1ITUFDLVNIQTI1NiJ9LHsiWC1BbXotQ3JlZGVudGlhbCI6IkFLSUFRTExIV0Q2TUVKR0VUTFNULzIwMjYwNzI4L3VzLWVhc3QtMS9zMy9hd3M0X3JlcXVlc3QifSx7IlgtQW16LURhdGUiOiIyMDI2MDcyOFQyMTI1MzVaIn0seyJrZXkiOiI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvNmE2OTFlNGYzYjBiZjYzOGIxMDUyZmM2X2RlYWxhbGl0eS1nbG9iZS10ZXh0dXJlLmpwZyJ9XX0=",
  xAmzSignature: "7c34234a5f218eaf9c7717e62359b5da8cbfbb5a621decf863de5b5306cc672c",
  successActionStatus: "201",
  contentType: "image/jpeg",
  cacheControl: "max-age=31536000",
};

const filePath = path.join(
  "public",
  "marketing",
  "assets",
  "dealality-globe-texture.jpg"
);
const fileData = fs.readFileSync(filePath);

(async () => {
  const form = new FormData();
  form.append("acl", uploadDetails.acl);
  form.append("bucket", uploadDetails.bucket);
  form.append("X-Amz-Algorithm", uploadDetails.xAmzAlgorithm);
  form.append("X-Amz-Credential", uploadDetails.xAmzCredential);
  form.append("X-Amz-Date", uploadDetails.xAmzDate);
  form.append("key", uploadDetails.key);
  form.append("Policy", uploadDetails.policy);
  form.append("X-Amz-Signature", uploadDetails.xAmzSignature);
  form.append("success_action_status", uploadDetails.successActionStatus);
  form.append("Content-Type", uploadDetails.contentType);
  form.append("Cache-Control", uploadDetails.cacheControl);
  form.append(
    "file",
    new Blob([fileData], { type: "image/jpeg" }),
    "dealality-globe-texture.jpg"
  );

  const res = await fetch(uploadUrl, { method: "POST", body: form });
  console.log("status", res.status);
  const text = await res.text();
  console.log(text.slice(0, 300));
})();
