import fs from "fs";
import path from "path";

const uploads = [
  {
    file: "tmp-insights-imgs/ins-1.jpg",
    uploadUrl: "https://webflow-prod-assets.s3.amazonaws.com/",
    uploadDetails: {
      acl: "public-read",
      bucket: "webflow-prod-assets",
      xAmzAlgorithm: "AWS4-HMAC-SHA256",
      xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260728/us-east-1/s3/aws4_request",
      xAmzDate: "20260728T070432Z",
      key: "68108c29063eeb5d1bd7ae4a/6a685480c65a1dc00fdfad2b_insights-reflagging.jpg",
      policy:
        "eyJleHBpcmF0aW9uIjoiMjAyNi0wNy0yOFQwODowNDozMloiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoiaW1hZ2UvanBlZyJ9LHsic3VjY2Vzc19hY3Rpb25fc3RhdHVzIjoiMjAxIn0sWyJzdGFydHMtd2l0aCIsIiRDb250ZW50LVR5cGUiLCJpbWFnZS9qcGVnIl0sWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsMCwzMTQ1NzI4MF0seyJhY2wiOiJwdWJsaWMtcmVhZCJ9LHsiYnVja2V0Ijoid2ViZmxvdy1wcm9kLWFzc2V0cyJ9LHsiWC1BbXotQWxnb3JpdGhtIjoiQVdTNC1ITUFDLVNIQTI1NiJ9LHsiWC1BbXotQ3JlZGVudGlhbCI6IkFLSUFRTExIV0Q2TUVKR0VUTFNULzIwMjYwNzI4L3VzLWVhc3QtMS9zMy9hd3M0X3JlcXVlc3QifSx7IlgtQW16LURhdGUiOiIyMDI2MDcyOFQwNzA0MzJaIn0seyJrZXkiOiI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvNmE2ODU0ODBjNjVhMWRjMDBmZGZhZDJiX2luc2lnaHRzLXJlZmxhZ2dpbmcuanBnIn1dfQ==",
      xAmzSignature: "1b4549b0d40a324d3a302303ebffdeb3afd4f25f1f39cb84e77d81f458e54adb",
      successActionStatus: "201",
      contentType: "image/jpeg",
      cacheControl: "max-age=31536000",
    },
  },
  {
    file: "tmp-insights-imgs/ins-2.jpg",
    uploadUrl: "https://webflow-prod-assets.s3.amazonaws.com/",
    uploadDetails: {
      acl: "public-read",
      bucket: "webflow-prod-assets",
      xAmzAlgorithm: "AWS4-HMAC-SHA256",
      xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260728/us-east-1/s3/aws4_request",
      xAmzDate: "20260728T070433Z",
      key: "68108c29063eeb5d1bd7ae4a/6a6854810f807fa204a739f2_insights-soft-hard-brands.jpg",
      policy:
        "eyJleHBpcmF0aW9uIjoiMjAyNi0wNy0yOFQwODowNDozM1oiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoiaW1hZ2UvanBlZyJ9LHsic3VjY2Vzc19hY3Rpb25fc3RhdHVzIjoiMjAxIn0sWyJzdGFydHMtd2l0aCIsIiRDb250ZW50LVR5cGUiLCJpbWFnZS9qcGVnIl0sWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsMCwzMTQ1NzI4MF0seyJhY2wiOiJwdWJsaWMtcmVhZCJ9LHsiYnVja2V0Ijoid2ViZmxvdy1wcm9kLWFzc2V0cyJ9LHsiWC1BbXotQWxnb3JpdGhtIjoiQVdTNC1ITUFDLVNIQTI1NiJ9LHsiWC1BbXotQ3JlZGVudGlhbCI6IkFLSUFRTExIV0Q2TUVKR0VUTFNULzIwMjYwNzI4L3VzLWVhc3QtMS9zMy9hd3M0X3JlcXVlc3QifSx7IlgtQW16LURhdGUiOiIyMDI2MDcyOFQwNzA0MzNaIn0seyJrZXkiOiI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvNmE2ODU0ODEwZjgwN2ZhMjA0YTczOWYyX2luc2lnaHRzLXNvZnQtaGFyZC1icmFuZHMuanBnIn1dfQ==",
      xAmzSignature: "c366f0065ff8d2835d089e388872e345c7c3af66c390ae7d3dc7d46306719645",
      successActionStatus: "201",
      contentType: "image/jpeg",
      cacheControl: "max-age=31536000",
    },
  },
  {
    file: "tmp-insights-imgs/ins-3.jpg",
    uploadUrl: "https://webflow-prod-assets.s3.amazonaws.com/",
    uploadDetails: {
      acl: "public-read",
      bucket: "webflow-prod-assets",
      xAmzAlgorithm: "AWS4-HMAC-SHA256",
      xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260728/us-east-1/s3/aws4_request",
      xAmzDate: "20260728T070433Z",
      key: "68108c29063eeb5d1bd7ae4a/6a68548108565a6f6e6640c3_insights-brand-development.jpg",
      policy:
        "eyJleHBpcmF0aW9uIjoiMjAyNi0wNy0yOFQwODowNDozM1oiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoiaW1hZ2UvanBlZyJ9LHsic3VjY2Vzc19hY3Rpb25fc3RhdHVzIjoiMjAxIn0sWyJzdGFydHMtd2l0aCIsIiRDb250ZW50LVR5cGUiLCJpbWFnZS9qcGVnIl0sWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsMCwzMTQ1NzI4MF0seyJhY2wiOiJwdWJsaWMtcmVhZCJ9LHsiYnVja2V0Ijoid2ViZmxvdy1wcm9kLWFzc2V0cyJ9LHsiWC1BbXotQWxnb3JpdGhtIjoiQVdTNC1ITUFDLVNIQTI1NiJ9LHsiWC1BbXotQ3JlZGVudGlhbCI6IkFLSUFRTExIV0Q2TUVKR0VUTFNULzIwMjYwNzI4L3VzLWVhc3QtMS9zMy9hd3M0X3JlcXVlc3QifSx7IlgtQW16LURhdGUiOiIyMDI2MDcyOFQwNzA0MzNaIn0seyJrZXkiOiI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvNmE2ODU0ODEwODU2NWE2ZjZlNjY0MGMzX2luc2lnaHRzLWJyYW5kLWRldmVsb3BtZW50LmpwZyJ9XX0=",
      xAmzSignature: "f8fc58220de9ee7db6a5f97035d0fe9f950631226de8d5e8a75636045296255f",
      successActionStatus: "201",
      contentType: "image/jpeg",
      cacheControl: "max-age=31536000",
    },
  },
];

for (const item of uploads) {
  const fileBytes = fs.readFileSync(item.file);
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
  form.append("file", new Blob([fileBytes], { type: d.contentType }), path.basename(item.file));
  const res = await fetch(item.uploadUrl, { method: "POST", body: form });
  console.log(path.basename(item.file), res.status, await res.text());
}
