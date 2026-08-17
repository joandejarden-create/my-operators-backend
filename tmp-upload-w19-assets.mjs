import fs from "fs";
import path from "path";

const uploads = [
  {
    file: "public/marketing/dealality-old-home-freeform-head.v20260729w19.css",
    details: {
      acl: "public-read",
      bucket: "webflow-prod-assets",
      xAmzAlgorithm: "AWS4-HMAC-SHA256",
      xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260729/us-east-1/s3/aws4_request",
      xAmzDate: "20260729T153434Z",
      key: "68108c29063eeb5d1bd7ae4a/6a6a1d8ac5f82dea4aac9efb_dealality-old-home-freeform-head.v20260729w19.css",
      policy:
        "eyJleHBpcmF0aW9uIjoiMjAyNi0wNy0yOVQxNjozNDozNFoiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoidGV4dC9jc3MifSx7InN1Y2Nlc3NfYWN0aW9uX3N0YXR1cyI6IjIwMSJ9LFsic3RhcnRzLXdpdGgiLCIkQ29udGVudC1UeXBlIiwidGV4dC9jc3MiXSxbImNvbnRlbnQtbGVuZ3RoLXJhbmdlIiwwLDMxNDU3MjgwXSx7ImFjbCI6InB1YmxpYy1yZWFkIn0seyJidWNrZXQiOiJ3ZWJmbG93LXByb2QtYXNzZXRzIn0seyJYLUFtei1BbGdvcml0aG0iOiJBV1M0LUhNQUMtU0hBMjU2In0seyJYLUFtei1DcmVkZW50aWFsIjoiQUtJQVFMTEhXRDZNRUpHRVRMU1QvMjAyNjA3MjkvdXMtZWFzdC0xL3MzL2F3czRfcmVxdWVzdCJ9LHsiWC1BbXotRGF0ZSI6IjIwMjYwNzI5VDE1MzQzNFoifSx7ImtleSI6IjY4MTA4YzI5MDYzZWViNWQxYmQ3YWU0YS82YTZhMWQ4YWM1ZjgyZGVhNGFhYzllZmJfZGVhbGFsaXR5LW9sZC1ob21lLWZyZWVmb3JtLWhlYWQudjIwMjYwNzI5dzE5LmNzcyJ9XX0=",
      xAmzSignature: "6789e6a2a75dac847a883485c7da72860ff83d98938372d60c94e0fdc804df5a",
      successActionStatus: "201",
      contentType: "text/css",
      cacheControl: "max-age=31536000",
    },
  },
  {
    file: "public/marketing/dealality-old-home-testimonials.v20260729w4.js",
    details: {
      acl: "public-read",
      bucket: "webflow-prod-assets",
      xAmzAlgorithm: "AWS4-HMAC-SHA256",
      xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260729/us-east-1/s3/aws4_request",
      xAmzDate: "20260729T153434Z",
      key: "68108c29063eeb5d1bd7ae4a/6a6a1d8a108b8e3379c5463e_dealality-old-home-testimonials.v20260729w4.js",
      policy:
        "eyJleHBpcmF0aW9uIjoiMjAyNi0wNy0yOVQxNjozNDozNFoiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoiYXBwbGljYXRpb24vamF2YXNjcmlwdCJ9LHsic3VjY2Vzc19hY3Rpb25fc3RhdHVzIjoiMjAxIn0sWyJzdGFydHMtd2l0aCIsIiRDb250ZW50LVR5cGUiLCJhcHBsaWNhdGlvbi9qYXZhc2NyaXB0Il0sWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsMCwzMTQ1NzI4MF0seyJhY2wiOiJwdWJsaWMtcmVhZCJ9LHsiYnVja2V0Ijoid2ViZmxvdy1wcm9kLWFzc2V0cyJ9LHsiWC1BbXotQWxnb3JpdGhtIjoiQVdTNC1ITUFDLVNIQTI1NiJ9LHsiWC1BbXotQ3JlZGVudGlhbCI6IkFLSUFRTExIV0Q2TUVKR0VUTFNULzIwMjYwNzI5L3VzLWVhc3QtMS9zMy9hd3M0X3JlcXVlc3QifSx7IlgtQW16LURhdGUiOiIyMDI2MDcyOVQxNTM0MzRaIn0seyJrZXkiOiI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvNmE2YTFkOGExMDhiOGUzMzc5YzU0NjNlX2RlYWxhbGl0eS1vbGQtaG9tZS10ZXN0aW1vbmlhbHMudjIwMjYwNzI5dzQuanMifV19",
      xAmzSignature: "6efb790be8d9bcca5fbd470d093c6fb15cd1b515365aeeca0a00bf0d8bf55cb0",
      successActionStatus: "201",
      contentType: "application/javascript",
      cacheControl: "max-age=31536000",
    },
  },
];

for (const item of uploads) {
  const fileBytes = fs.readFileSync(path.resolve(item.file));
  const d = item.details;
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
    path.basename(item.file)
  );
  const res = await fetch("https://webflow-prod-assets.s3.amazonaws.com/", {
    method: "POST",
    body: form,
  });
  console.log(item.file, res.status, await res.text());
}
