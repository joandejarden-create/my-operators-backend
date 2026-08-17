import fs from "fs";
import path from "path";

const details = {
  acl: "public-read",
  bucket: "webflow-prod-assets",
  xAmzAlgorithm: "AWS4-HMAC-SHA256",
  xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260729/us-east-1/s3/aws4_request",
  xAmzDate: "20260729T153336Z",
  key: "68108c29063eeb5d1bd7ae4a/6a6a1d50cef0212b9997fc62_old-home-problem-v2.v20260729a.js",
  policy:
    "eyJleHBpcmF0aW9uIjoiMjAyNi0wNy0yOVQxNjozMzozNloiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoiYXBwbGljYXRpb24vamF2YXNjcmlwdCJ9LHsic3VjY2Vzc19hY3Rpb25fc3RhdHVzIjoiMjAxIn0sWyJzdGFydHMtd2l0aCIsIiRDb250ZW50LVR5cGUiLCJhcHBsaWNhdGlvbi9qYXZhc2NyaXB0Il0sWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsMCwzMTQ1NzI4MF0seyJhY2wiOiJwdWJsaWMtcmVhZCJ9LHsiYnVja2V0Ijoid2ViZmxvdy1wcm9kLWFzc2V0cyJ9LHsiWC1BbXotQWxnb3JpdGhtIjoiQVdTNC1ITUFDLVNIQTI1NiJ9LHsiWC1BbXotQ3JlZGVudGlhbCI6IkFLSUFRTExIV0Q2TUVKR0VUTFNULzIwMjYwNzI5L3VzLWVhc3QtMS9zMy9hd3M0X3JlcXVlc3QifSx7IlgtQW16LURhdGUiOiIyMDI2MDcyOVQxNTMzMzZaIn0seyJrZXkiOiI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvNmE2YTFkNTBjZWYwMjEyYjk5OTdmYzYyX29sZC1ob21lLXByb2JsZW0tdjIudjIwMjYwNzI5YS5qcyJ9XX0=",
  xAmzSignature: "49cd824cbae4739e325f50a63ba6fbf1148c7c9dc00b8d3cce95aebae2e1c456",
  successActionStatus: "201",
  contentType: "application/javascript",
  cacheControl: "max-age=31536000",
};

const file = path.resolve("public/marketing/old-home-problem-v2.v20260729a.js");
const fileBytes = fs.readFileSync(file);
const form = new FormData();
for (const [k, v] of Object.entries({
  acl: details.acl,
  bucket: details.bucket,
  "X-Amz-Algorithm": details.xAmzAlgorithm,
  "X-Amz-Credential": details.xAmzCredential,
  "X-Amz-Date": details.xAmzDate,
  key: details.key,
  Policy: details.policy,
  "X-Amz-Signature": details.xAmzSignature,
  success_action_status: details.successActionStatus,
  "Content-Type": details.contentType,
  "Cache-Control": details.cacheControl,
})) {
  form.append(k, v);
}
form.append("file", new Blob([fileBytes], { type: details.contentType }), path.basename(file));
const res = await fetch("https://webflow-prod-assets.s3.amazonaws.com/", { method: "POST", body: form });
console.log("upload", res.status);

const hosted =
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6a1d50cef0212b9997fc62_old-home-problem-v2.v20260729a.js";
const body = await (await fetch(hosted)).text();
console.log("cdn problem", body.includes("data-oh-problem-v2"), body.length);

const restore = fs.readFileSync("tmp-restore-site-footer.html", "utf8").trimEnd();
const footer =
  restore +
  `\n<!-- ohProblemV2: Old Home The Problem rebuild (page freeform write returned 406) -->\n<script src="${hosted}"></script>\n`;
fs.writeFileSync("tmp-site-footer-restored-with-problem.txt", footer);
console.log("footer chars", footer.length);
