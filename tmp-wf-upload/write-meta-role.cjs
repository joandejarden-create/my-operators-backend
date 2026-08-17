const fs = require("fs");
const metas = [
  {
    name: "pricing",
    file: "public/marketing/dealality-old-home-pricing.v20260801a.js",
    uploadUrl: "https://webflow-prod-assets.s3.amazonaws.com/",
    uploadDetails: {
      acl: "public-read",
      bucket: "webflow-prod-assets",
      xAmzAlgorithm: "AWS4-HMAC-SHA256",
      xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260801/us-east-1/s3/aws4_request",
      xAmzDate: "20260801T142218Z",
      key: "68108c29063eeb5d1bd7ae4a/6a6e011a7be4790556f2607b_dealality-old-home-pricing.v20260801a.js",
      policy:
        "eyJleHBpcmF0aW9uIjoiMjAyNi0wOC0wMVQxNToyMjoxOFoiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoiYXBwbGljYXRpb24vamF2YXNjcmlwdCJ9LHsic3VjY2Vzc19hY3Rpb25fc3RhdHVzIjoiMjAxIn0sWyJzdGFydHMtd2l0aCIsIiRDb250ZW50LVR5cGUiLCJhcHBsaWNhdGlvbi9qYXZhc2NyaXB0Il0sWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsMCwzMTQ1NzI4MF0seyJhY2wiOiJwdWJsaWMtcmVhZCJ9LHsiYnVja2V0Ijoid2ViZmxvdy1wcm9kLWFzc2V0cyJ9LHsiWC1BbXotQWxnb3JpdGhtIjoiQVdTNC1ITUFDLVNIQTI1NiJ9LHsiWC1BbXotQ3JlZGVudGlhbCI6IkFLSUFRTExIV0Q2TUVKR0VUTFNULzIwMjYwODAxL3VzLWVhc3QtMS9zMy9hd3M0X3JlcXVlc3QifSx7IlgtQW16LURhdGUiOiIyMDI2MDgwMVQxNDIyMThaIn0seyJrZXkiOiI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvNmE2ZTAxMWE3YmU0NzkwNTU2ZjI2MDdiX2RlYWxhbGl0eS1vbGQtaG9tZS1wcmljaW5nLnYyMDI2MDgwMWEuanMifV19",
      xAmzSignature:
        "b17872a3f29f9db5cb887bed6b5ce5645904522db0c7f4093997abdcdcef7196",
      successActionStatus: "201",
      contentType: "application/javascript",
      cacheControl: "max-age=31536000",
    },
  },
  {
    name: "explore",
    file: "public/marketing/old-home-explore-cta.v20260801d.js",
    uploadUrl: "https://webflow-prod-assets.s3.amazonaws.com/",
    uploadDetails: {
      acl: "public-read",
      bucket: "webflow-prod-assets",
      xAmzAlgorithm: "AWS4-HMAC-SHA256",
      xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260801/us-east-1/s3/aws4_request",
      xAmzDate: "20260801T142218Z",
      key: "68108c29063eeb5d1bd7ae4a/6a6e011a78d1ce1e62697d4b_old-home-explore-cta.v20260801d.js",
      policy:
        "eyJleHBpcmF0aW9uIjoiMjAyNi0wOC0wMVQxNToyMjoxOFoiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoiYXBwbGljYXRpb24vamF2YXNjcmlwdCJ9LHsic3VjY2Vzc19hY3Rpb25fc3RhdHVzIjoiMjAxIn0sWyJzdGFydHMtd2l0aCIsIiRDb250ZW50LVR5cGUiLCJhcHBsaWNhdGlvbi9qYXZhc2NyaXB0Il0sWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsMCwzMTQ1NzI4MF0seyJhY2wiOiJwdWJsaWMtcmVhZCJ9LHsiYnVja2V0Ijoid2ViZmxvdy1wcm9kLWFzc2V0cyJ9LHsiWC1BbXotQWxnb3JpdGhtIjoiQVdTNC1ITUFDLVNIQTI1NiJ9LHsiWC1BbXotQ3JlZGVudGlhbCI6IkFLSUFRTExIV0Q2TUVKR0VUTFNULzIwMjYwODAxL3VzLWVhc3QtMS9zMy9hd3M0X3JlcXVlc3QifSx7IlgtQW16LURhdGUiOiIyMDI2MDgwMVQxNDIyMThaIn0seyJrZXkiOiI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvNmE2ZTAxMWE3OGQxY2UxZTYyNjk3ZDRiX29sZC1ob21lLWV4cGxvcmUtY3RhLnYyMDI2MDgwMWQuanMifV19",
      xAmzSignature:
        "bb8c6dfb56eee36b4cbc4872f61b2bc91d24b94f5af6c55a389a9ac9eb3e544f",
      successActionStatus: "201",
      contentType: "application/javascript",
      cacheControl: "max-age=31536000",
    },
  },
  {
    name: "boot",
    file: "public/marketing/old-home-boot-guard.v20260801m.js",
    uploadUrl: "https://webflow-prod-assets.s3.amazonaws.com/",
    uploadDetails: {
      acl: "public-read",
      bucket: "webflow-prod-assets",
      xAmzAlgorithm: "AWS4-HMAC-SHA256",
      xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260801/us-east-1/s3/aws4_request",
      xAmzDate: "20260801T142218Z",
      key: "68108c29063eeb5d1bd7ae4a/6a6e011a590d5f9032065d16_old-home-boot-guard.v20260801m.js",
      policy:
        "eyJleHBpcmF0aW9uIjoiMjAyNi0wOC0wMVQxNToyMjoxOFoiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoiYXBwbGljYXRpb24vamF2YXNjcmlwdCJ9LHsic3VjY2Vzc19hY3Rpb25fc3RhdHVzIjoiMjAxIn0sWyJzdGFydHMtd2l0aCIsIiRDb250ZW50LVR5cGUiLCJhcHBsaWNhdGlvbi9qYXZhc2NyaXB0Il0sWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsMCwzMTQ1NzI4MF0seyJhY2wiOiJwdWJsaWMtcmVhZCJ9LHsiYnVja2V0Ijoid2ViZmxvdy1wcm9kLWFzc2V0cyJ9LHsiWC1BbXotQWxnb3JpdGhtIjoiQVdTNC1ITUFDLVNIQTI1NiJ9LHsiWC1BbXotQ3JlZGVudGlhbCI6IkFLSUFRTExIV0Q2TUVKR0VUTFNULzIwMjYwODAxL3VzLWVhc3QtMS9zMy9hd3M0X3JlcXVlc3QifSx7IlgtQW16LURhdGUiOiIyMDI2MDgwMVQxNDIyMThaIn0seyJrZXkiOiI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvNmE2ZTAxMWE1OTBkNWY5MDMyMDY1ZDE2X29sZC1ob21lLWJvb3QtZ3VhcmQudjIwMjYwODAxbS5qcyJ9XX0=",
      xAmzSignature:
        "cd7ab138d5827c0279659012179f97f1fdc9cd8562af2e37ff0344c56743403c",
      successActionStatus: "201",
      contentType: "application/javascript",
      cacheControl: "max-age=31536000",
    },
  },
];
for (const m of metas) {
  fs.writeFileSync("tmp-wf-upload/" + m.name + ".json", JSON.stringify(m));
}
console.log("wrote", metas.length);
