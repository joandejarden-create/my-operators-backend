import fs from "fs";
import FormData from "form-data";

const uploads = [
  {
    file: "public/marketing/old-home-problem-deal-desk.v1.css",
    name: "oh-deal-desk-cinematic-v1-phaseB.css",
    uploadUrl: "https://webflow-prod-assets.s3.amazonaws.com/",
    uploadDetails: {
      acl: "public-read",
      bucket: "webflow-prod-assets",
      xAmzAlgorithm: "AWS4-HMAC-SHA256",
      xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260731/us-east-1/s3/aws4_request",
      xAmzDate: "20260731T070401Z",
      key: "68108c29063eeb5d1bd7ae4a/6a6c48e126a07d130ad33932_oh-deal-desk-cinematic-v1-phaseB.css",
      policy:
        "eyJleHBpcmF0aW9uIjoiMjAyNi0wNy0zMVQwODowNDowMVoiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoidGV4dC9jc3MifSx7InN1Y2Nlc3NfYWN0aW9uX3N0YXR1cyI6IjIwMSJ9LFsic3RhcnRzLXdpdGgiLCIkQ29udGVudC1UeXBlIiwidGV4dC9jc3MiXSxbImNvbnRlbnQtbGVuZ3RoLXJhbmdlIiwwLDMxNDU3MjgwXSx7ImFjbCI6InB1YmxpYy1yZWFkIn0seyJidWNrZXQiOiJ3ZWJmbG93LXByb2QtYXNzZXRzIn0seyJYLUFtei1BbGdvcml0aG0iOiJBV1M0LUhNQUMtU0hBMjU2In0seyJYLUFtei1DcmVkZW50aWFsIjoiQUtJQVFMTEhXRDZNRUpHRVRMU1QvMjAyNjA3MzEvdXMtZWFzdC0xL3MzL2F3czRfcmVxdWVzdCJ9LHsiWC1BbXotRGF0ZSI6IjIwMjYwNzMxVDA3MDQwMVoifSx7ImtleSI6IjY4MTA4YzI5MDYzZWViNWQxYmQ3YWU0YS82YTZjNDhlMTI2YTA3ZDEzMGFkMzM5MzJfb2gtZGVhbC1kZXNrLWNpbmVtYXRpYy12MS1waGFzZUIuY3NzIn1dfQ==",
      xAmzSignature: "94cb60009541609c4ee1ef32655881bf1e8cff7c6b49ab6615894d5184eb150b",
      successActionStatus: "201",
      contentType: "text/css",
      cacheControl: "max-age=31536000",
    },
  },
  {
    file: "public/marketing/old-home-problem-deal-desk.v1.js",
    name: "old-home-problem-deal-desk.v1.js",
    uploadUrl: "https://webflow-prod-assets.s3.amazonaws.com/",
    uploadDetails: {
      acl: "public-read",
      bucket: "webflow-prod-assets",
      xAmzAlgorithm: "AWS4-HMAC-SHA256",
      xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260731/us-east-1/s3/aws4_request",
      xAmzDate: "20260731T070401Z",
      key: "68108c29063eeb5d1bd7ae4a/6a6c48e19cbfe5ed843b0866_old-home-problem-deal-desk.v1.js",
      policy:
        "eyJleHBpcmF0aW9uIjoiMjAyNi0wNy0zMVQwODowNDowMVoiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoiYXBwbGljYXRpb24vamF2YXNjcmlwdCJ9LHsic3VjY2Vzc19hY3Rpb25fc3RhdHVzIjoiMjAxIn0sWyJzdGFydHMtd2l0aCIsIiRDb250ZW50LVR5cGUiLCJhcHBsaWNhdGlvbi9qYXZhc2NyaXB0Il0sWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsMCwzMTQ1NzI4MF0seyJhY2wiOiJwdWJsaWMtcmVhZCJ9LHsiYnVja2V0Ijoid2ViZmxvdy1wcm9kLWFzc2V0cyJ9LHsiWC1BbXotQWxnb3JpdGhtIjoiQVdTNC1ITUFDLVNIQTI1NiJ9LHsiWC1BbXotQ3JlZGVudGlhbCI6IkFLSUFRTExIV0Q2TUVKR0VUTFNULzIwMjYwNzMxL3VzLWVhc3QtMS9zMy9hd3M0X3JlcXVlc3QifSx7IlgtQW16LURhdGUiOiIyMDI2MDczMVQwNzA0MDFaIn0seyJrZXkiOiI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvNmE2YzQ4ZTE5Y2JmZTVlZDg0M2IwODY2X29sZC1ob21lLXByb2JsZW0tZGVhbC1kZXNrLnYxLmpzIn1dfQ==",
      xAmzSignature: "a76be440743e13f620bab3e4b0349af88d18b1ada5d384abadca1f4a6d3de109",
      successActionStatus: "201",
      contentType: "application/javascript",
      cacheControl: "max-age=31536000",
    },
  },
  {
    file: "public/marketing/dealality-old-home-platform-video-launcher.js",
    name: "dealality-old-home-platform-video-launcher.v20260731a.js",
    uploadUrl: "https://webflow-prod-assets.s3.amazonaws.com/",
    uploadDetails: {
      acl: "public-read",
      bucket: "webflow-prod-assets",
      xAmzAlgorithm: "AWS4-HMAC-SHA256",
      xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260731/us-east-1/s3/aws4_request",
      xAmzDate: "20260731T070402Z",
      key: "68108c29063eeb5d1bd7ae4a/6a6c48e226a07d130ad339c3_dealality-old-home-platform-video-launcher.v20260731a.js",
      policy:
        "eyJleHBpcmF0aW9uIjoiMjAyNi0wNy0zMVQwODowNDowMloiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoiYXBwbGljYXRpb24vamF2YXNjcmlwdCJ9LHsic3VjY2Vzc19hY3Rpb25fc3RhdHVzIjoiMjAxIn0sWyJzdGFydHMtd2l0aCIsIiRDb250ZW50LVR5cGUiLCJhcHBsaWNhdGlvbi9qYXZhc2NyaXB0Il0sWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsMCwzMTQ1NzI4MF0seyJhY2wiOiJwdWJsaWMtcmVhZCJ9LHsiYnVja2V0Ijoid2ViZmxvdy1wcm9kLWFzc2V0cyJ9LHsiWC1BbXotQWxnb3JpdGhtIjoiQVdTNC1ITUFDLVNIQTI1NiJ9LHsiWC1BbXotQ3JlZGVudGlhbCI6IkFLSUFRTExIV0Q2TUVKR0VUTFNULzIwMjYwNzMxL3VzLWVhc3QtMS9zMy9hd3M0X3JlcXVlc3QifSx7IlgtQW16LURhdGUiOiIyMDI2MDczMVQwNzA0MDJaIn0seyJrZXkiOiI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvNmE2YzQ4ZTIyNmEwN2QxMzBhZDMzOWMzX2RlYWxhbGl0eS1vbGQtaG9tZS1wbGF0Zm9ybS12aWRlby1sYXVuY2hlci52MjAyNjA3MzFhLmpzIn1dfQ==",
      xAmzSignature: "af9baa318391f40427cb3850e4ecc7e4ed65943f6266d444a0bd113c6ac542a4",
      successActionStatus: "201",
      contentType: "application/javascript",
      cacheControl: "max-age=31536000",
    },
  },
];

function uploadOne(item) {
  return new Promise((resolve, reject) => {
    const fileData = fs.readFileSync(item.file);
    const d = item.uploadDetails;
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
    form.append("file", fileData, {
      filename: item.name,
      contentType: d.contentType,
    });
    form.submit(item.uploadUrl, (err, res) => {
      if (err) return reject(err);
      let body = "";
      res.on("data", (c) => (body += c));
      res.on("end", () => {
        resolve({ name: item.name, status: res.statusCode, body: body.slice(0, 120) });
      });
    });
  });
}

for (const item of uploads) {
  const result = await uploadOne(item);
  console.log(JSON.stringify(result));
  if (result.status !== 201) process.exit(1);
}
