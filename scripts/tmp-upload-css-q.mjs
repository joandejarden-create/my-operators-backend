import fs from "fs";
import axios from "axios";
import FormData from "form-data";

const details = {
  uploadUrl: "https://webflow-prod-assets.s3.amazonaws.com/",
  uploadDetails: {
    acl: "public-read",
    bucket: "webflow-prod-assets",
    xAmzAlgorithm: "AWS4-HMAC-SHA256",
    xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260728/us-east-1/s3/aws4_request",
    xAmzDate: "20260728T122859Z",
    key: "68108c29063eeb5d1bd7ae4a/6a68a08b4977ae6a5c060594_dealality-old-home-dark.v20260728q.css",
    policy:
      "eyJleHBpcmF0aW9uIjoiMjAyNi0wNy0yOFQxMzoyODo1OVoiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoidGV4dC9jc3MifSx7InN1Y2Nlc3NfYWN0aW9uX3N0YXR1cyI6IjIwMSJ9LFsic3RhcnRzLXdpdGgiLCIkQ29udGVudC1UeXBlIiwidGV4dC9jc3MiXSxbImNvbnRlbnQtbGVuZ3RoLXJhbmdlIiwwLDMxNDU3MjgwXSx7ImFjbCI6InB1YmxpYy1yZWFkIn0seyJidWNrZXQiOiJ3ZWJmbG93LXByb2QtYXNzZXRzIn0seyJYLUFtei1BbGdvcml0aG0iOiJBV1M0LUhNQUMtU0hBMjU2In0seyJYLUFtei1DcmVkZW50aWFsIjoiQUtJQVFMTEhXRDZNRUpHRVRMU1QvMjAyNjA3MjgvdXMtZWFzdC0xL3MzL2F3czRfcmVxdWVzdCJ9LHsiWC1BbXotRGF0ZSI6IjIwMjYwNzI4VDEyMjg1OVoifSx7ImtleSI6IjY4MTA4YzI5MDYzZWViNWQxYmQ3YWU0YS82YTY4YTA4YjQ5NzdhZTZhNWMwNjA1OTRfZGVhbGFsaXR5LW9sZC1ob21lLWRhcmsudjIwMjYwNzI4cS5jc3MifV19",
    xAmzSignature: "afe8913146a5a142807d600ba0d061a733b0490d0679404fa86e33053236fe3d",
    successActionStatus: "201",
    contentType: "text/css",
    cacheControl: "max-age=31536000",
  },
};

const d = details.uploadDetails;
const fileData = fs.readFileSync("tmp-old-home-dark.v20260728q.css");
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
  filename: "dealality-old-home-dark.v20260728q.css",
  contentType: d.contentType,
});

const res = await axios.post(details.uploadUrl, form, {
  headers: form.getHeaders(),
  maxBodyLength: Infinity,
  validateStatus: () => true,
});
console.log(JSON.stringify({ status: res.status }));
if (res.status !== 201) process.exit(1);
