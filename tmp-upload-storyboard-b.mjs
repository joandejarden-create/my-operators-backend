import fs from "fs";
import FormData from "form-data";
import axios from "axios";
import crypto from "crypto";

async function upload(filePath, fileName, details) {
  const fileData = fs.readFileSync(filePath);
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
  form.append("file", fileData, { filename: fileName, contentType: details.contentType });
  const res = await axios.post("https://webflow-prod-assets.s3.amazonaws.com/", form, {
    headers: form.getHeaders(),
    maxBodyLength: Infinity,
    validateStatus: () => true,
  });
  console.log(fileName, res.status);
  if (res.status !== 201) {
    console.log(String(res.data).slice(0, 300));
    process.exit(1);
  }
}

const cssDetails = {
  acl: "public-read",
  bucket: "webflow-prod-assets",
  xAmzAlgorithm: "AWS4-HMAC-SHA256",
  xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260729/us-east-1/s3/aws4_request",
  xAmzDate: "20260729T215733Z",
  key: "68108c29063eeb5d1bd7ae4a/6a6a774df6520a2f78161f69_old-home-problem-storyboard.v20260729b.css",
  policy:
    "eyJleHBpcmF0aW9uIjoiMjAyNi0wNy0yOVQyMjo1NzozM1oiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoidGV4dC9jc3MifSx7InN1Y2Nlc3NfYWN0aW9uX3N0YXR1cyI6IjIwMSJ9LFsic3RhcnRzLXdpdGgiLCIkQ29udGVudC1UeXBlIiwidGV4dC9jc3MiXSxbImNvbnRlbnQtbGVuZ3RoLXJhbmdlIiwwLDMxNDU3MjgwXSx7ImFjbCI6InB1YmxpYy1yZWFkIn0seyJidWNrZXQiOiJ3ZWJmbG93LXByb2QtYXNzZXRzIn0seyJYLUFtei1BbGdvcml0aG0iOiJBV1M0LUhNQUMtU0hBMjU2In0seyJYLUFtei1DcmVkZW50aWFsIjoiQUtJQVFMTEhXRDZNRUpHRVRMU1QvMjAyNjA3MjkvdXMtZWFzdC0xL3MzL2F3czRfcmVxdWVzdCJ9LHsiWC1BbXotRGF0ZSI6IjIwMjYwNzI5VDIxNTczM1oifSx7ImtleSI6IjY4MTA4YzI5MDYzZWViNWQxYmQ3YWU0YS82YTZhNzc0ZGY2NTIwYTJmNzgxNjFmNjlfb2xkLWhvbWUtcHJvYmxlbS1zdG9yeWJvYXJkLnYyMDI2MDcyOWIuY3NzIn1dfQ==",
  xAmzSignature: "d1036aaba908df9b2053128c2ef1714b2d4e8c1c120d5e87d514d47fd4fa1ca6",
  successActionStatus: "201",
  contentType: "text/css",
  cacheControl: "max-age=31536000",
};

await upload(
  "public/marketing/old-home-problem-storyboard.v20260729b.css",
  "old-home-problem-storyboard.v20260729b.css",
  cssDetails
);

const jsPath = "public/marketing/old-home-problem-storyboard.v20260729b.js";
const jsHash = crypto.createHash("md5").update(fs.readFileSync(jsPath)).digest("hex");
console.log("jsHash", jsHash);
fs.writeFileSync("tmp-js-hash.txt", jsHash);
