import fs from "fs";
import axios from "axios";
import FormData from "form-data";

const item = {
  file: "public/marketing/dealality-old-home-freeform-head.v20260729w2.css",
  uploadUrl: "https://webflow-prod-assets.s3.amazonaws.com/",
  uploadDetails: {
    acl: "public-read",
    bucket: "webflow-prod-assets",
    xAmzAlgorithm: "AWS4-HMAC-SHA256",
    xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260729/us-east-1/s3/aws4_request",
    xAmzDate: "20260729T083530Z",
    key: "68108c29063eeb5d1bd7ae4a/6a69bb5256a71837bacda5c5_dealality-old-home-freeform-head.v20260729w3.css",
    policy: "eyJleHBpcmF0aW9uIjoiMjAyNi0wNy0yOVQwOTozNTozMFoiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoidGV4dC9jc3MifSx7InN1Y2Nlc3NfYWN0aW9uX3N0YXR1cyI6IjIwMSJ9LFsic3RhcnRzLXdpdGgiLCIkQ29udGVudC1UeXBlIiwidGV4dC9jc3MiXSxbImNvbnRlbnQtbGVuZ3RoLXJhbmdlIiwwLDMxNDU3MjgwXSx7ImFjbCI6InB1YmxpYy1yZWFkIn0seyJidWNrZXQiOiJ3ZWJmbG93LXByb2QtYXNzZXRzIn0seyJYLUFtei1BbGdvcml0aG0iOiJBV1M0LUhNQUMtU0hBMjU2In0seyJYLUFtei1DcmVkZW50aWFsIjoiQUtJQVFMTEhXRDZNRUpHRVRMU1QvMjAyNjA3MjkvdXMtZWFzdC0xL3MzL2F3czRfcmVxdWVzdCJ9LHsiWC1BbXotRGF0ZSI6IjIwMjYwNzI5VDA4MzUzMFoifSx7ImtleSI6IjY4MTA4YzI5MDYzZWViNWQxYmQ3YWU0YS82YTY5YmI1MjU2YTcxODM3YmFjZGE1YzVfZGVhbGFsaXR5LW9sZC1ob21lLWZyZWVmb3JtLWhlYWQudjIwMjYwNzI5dzMuY3NzIn1dfQ==",
    xAmzSignature: "ec8ba8ace81dc3ce83a4798050f55d0768e09c9a08beb38ff467b68b1132c71d",
    successActionStatus: "201",
    contentType: "text/css",
    cacheControl: "max-age=31536000",
  },
};

const fileData = fs.readFileSync(item.file);
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
form.append("file", fileData, {
  filename: "dealality-old-home-freeform-head.v20260729w3.css",
  contentType: d.contentType,
});
const res = await axios.post(item.uploadUrl, form, {
  headers: form.getHeaders(),
  maxBodyLength: Infinity,
  validateStatus: () => true,
});
console.log(res.status);
if (res.status !== 201) { console.log(res.data); process.exit(1); }
console.log("ok");
