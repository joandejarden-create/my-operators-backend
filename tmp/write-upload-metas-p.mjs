import fs from "fs";

const metas = {
  html: {
    uploadUrl: "https://webflow-prod-assets.s3.amazonaws.com/",
    uploadDetails: {
      acl: "public-read",
      bucket: "webflow-prod-assets",
      xAmzAlgorithm: "AWS4-HMAC-SHA256",
      xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260731/us-east-1/s3/aws4_request",
      xAmzDate: "20260731T232009Z",
      key: "68108c29063eeb5d1bd7ae4a/6a6d2da9ee3f05f9a86185ff_old-home-manual-process.v20260731p.html",
      policy:
        "eyJleHBpcmF0aW9uIjoiMjAyNi0wOC0wMVQwMDoyMDowOVoiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoidGV4dC9odG1sIn0seyJzdWNjZXNzX2FjdGlvbl9zdGF0dXMiOiIyMDEifSxbInN0YXJ0cy13aXRoIiwiJENvbnRlbnQtVHlwZSIsInRleHQvaHRtbCJdLFsiY29udGVudC1sZW5ndGgtcmFuZ2UiLDAsMzE0NTcyODBdLHsiYWNsIjoicHVibGljLXJlYWQifSx7ImJ1Y2tldCI6IndlYmZsb3ctcHJvZC1hc3NldHMifSx7IlgtQW16LUFsZ29yaXRobSI6IkFXUzQtSE1BQy1TSEEyNTYifSx7IlgtQW16LUNyZWRlbnRpYWwiOiJBS0lBUUxMSFdENk1FSkdFVExTVC8yMDI2MDczMS91cy1lYXN0LTEvczMvYXdzNF9yZXF1ZXN0In0seyJYLUFtei1EYXRlIjoiMjAyNjA3MzFUMjMyMDA5WiJ9LHsia2V5IjoiNjgxMDhjMjkwNjNlZWI1ZDFiZDdhZTRhLzZhNmQyZGE5ZWUzZjA1ZjlhODYxODVmZl9vbGQtaG9tZS1tYW51YWwtcHJvY2Vzcy52MjAyNjA3MzFwLmh0bWwifV19",
      xAmzSignature: "1765c138980db36214c0630038278133018b573407f6b75411ede19c8948a85b",
      successActionStatus: "201",
      contentType: "text/html",
      cacheControl: "max-age=31536000",
    },
  },
  css: {
    uploadUrl: "https://webflow-prod-assets.s3.amazonaws.com/",
    uploadDetails: {
      acl: "public-read",
      bucket: "webflow-prod-assets",
      xAmzAlgorithm: "AWS4-HMAC-SHA256",
      xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260731/us-east-1/s3/aws4_request",
      xAmzDate: "20260731T232009Z",
      key: "68108c29063eeb5d1bd7ae4a/6a6d2da9f18627be39046e85_old-home-manual-process.v20260731p.css",
      policy:
        "eyJleHBpcmF0aW9uIjoiMjAyNi0wOC0wMVQwMDoyMDowOVoiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoidGV4dC9jc3MifSx7InN1Y2Nlc3NfYWN0aW9uX3N0YXR1cyI6IjIwMSJ9LFsic3RhcnRzLXdpdGgiLCIkQ29udGVudC1UeXBlIiwidGV4dC9jc3MiXSxbImNvbnRlbnQtbGVuZ3RoLXJhbmdlIiwwLDMxNDU3MjgwXSx7ImFjbCI6InB1YmxpYy1yZWFkIn0seyJidWNrZXQiOiJ3ZWJmbG93LXByb2QtYXNzZXRzIn0seyJYLUFtei1BbGdvcml0aG0iOiJBV1M0LUhNQUMtU0hBMjU2In0seyJYLUFtei1DcmVkZW50aWFsIjoiQUtJQVFMTEhXRDZNRUpHRVRMU1QvMjAyNjA3MzEvdXMtZWFzdC0xL3MzL2F3czRfcmVxdWVzdCJ9LHsiWC1BbXotRGF0ZSI6IjIwMjYwNzMxVDIzMjAwOVoifSx7ImtleSI6IjY4MTA4YzI5MDYzZWViNWQxYmQ3YWU0YS82YTZkMmRhOWYxODYyN2JlMzkwNDZlODVfb2xkLWhvbWUtbWFudWFsLXByb2Nlc3MudjIwMjYwNzMxcC5jc3MifV19",
      xAmzSignature: "5e6852a11aa2479dfb37bfeba81870e2130a58bd30244a6f806a8badeec822cd",
      successActionStatus: "201",
      contentType: "text/css",
      cacheControl: "max-age=31536000",
    },
  },
  js: {
    uploadUrl: "https://webflow-prod-assets.s3.amazonaws.com/",
    uploadDetails: {
      acl: "public-read",
      bucket: "webflow-prod-assets",
      xAmzAlgorithm: "AWS4-HMAC-SHA256",
      xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260731/us-east-1/s3/aws4_request",
      xAmzDate: "20260731T232009Z",
      key: "68108c29063eeb5d1bd7ae4a/6a6d2da968c3b501f8010aa5_old-home-manual-process.v20260731p.js",
      policy:
        "eyJleHBpcmF0aW9uIjoiMjAyNi0wOC0wMVQwMDoyMDowOVoiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoiYXBwbGljYXRpb24vamF2YXNjcmlwdCJ9LHsic3VjY2Vzc19hY3Rpb25fc3RhdHVzIjoiMjAxIn0sWyJzdGFydHMtd2l0aCIsIiRDb250ZW50LVR5cGUiLCJhcHBsaWNhdGlvbi9qYXZhc2NyaXB0Il0sWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsMCwzMTQ1NzI4MF0seyJhY2wiOiJwdWJsaWMtcmVhZCJ9LHsiYnVja2V0Ijoid2ViZmxvdy1wcm9kLWFzc2V0cyJ9LHsiWC1BbXotQWxnb3JpdGhtIjoiQVdTNC1ITUFDLVNIQTI1NiJ9LHsiWC1BbXotQ3JlZGVudGlhbCI6IkFLSUFRTExIV0Q2TUVKR0VUTFNULzIwMjYwNzMxL3VzLWVhc3QtMS9zMy9hd3M0X3JlcXVlc3QifSx7IlgtQW16LURhdGUiOiIyMDI2MDczMVQyMzIwMDlaIn0seyJrZXkiOiI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvNmE2ZDJkYTk2OGMzYjUwMWY4MDEwYWE1X29sZC1ob21lLW1hbnVhbC1wcm9jZXNzLnYyMDI2MDczMXAuanMifV19",
      xAmzSignature: "b78824396e5ca4b754234807bb7836715480486161cc3fabf021f0cfad880e31",
      successActionStatus: "201",
      contentType: "application/javascript",
      cacheControl: "max-age=31536000",
    },
  },
};

for (const [k, v] of Object.entries(metas)) {
  fs.writeFileSync(`tmp/wf-upload-${k}-p.json`, JSON.stringify(v));
}
console.log("wrote metas");
