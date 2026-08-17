import fs from "fs";
import { spawnSync } from "child_process";

const meta = {
  uploadDetails: {
    acl: "public-read",
    bucket: "webflow-prod-assets",
    xAmzAlgorithm: "AWS4-HMAC-SHA256",
    xAmzCredential: "AKIAQLLHWD6MEJGETLST/20260801/us-east-1/s3/aws4_request",
    xAmzDate: "20260801T032613Z",
    key: "68108c29063eeb5d1bd7ae4a/6a6d675501f2a1c97e016821_old-home-manual-process.v20260801f12.css",
    policy:
      "eyJleHBpcmF0aW9uIjoiMjAyNi0wOC0wMVQwNDoyNjoxM1oiLCJjb25kaXRpb25zIjpbWyJzdGFydHMtd2l0aCIsIiRrZXkiLCI2ODEwOGMyOTA2M2VlYjVkMWJkN2FlNGEvIl0seyJjYWNoZS1jb250cm9sIjoibWF4LWFnZT0zMTUzNjAwMCJ9LHsiQ29udGVudC1UeXBlIjoidGV4dC9jc3MifSx7InN1Y2Nlc3NfYWN0aW9uX3N0YXR1cyI6IjIwMSJ9LFsic3RhcnRzLXdpdGgiLCIkQ29udGVudC1UeXBlIiwidGV4dC9jc3MiXSxbImNvbnRlbnQtbGVuZ3RoLXJhbmdlIiwwLDMxNDU3MjgwXSx7ImFjbCI6InB1YmxpYy1yZWFkIn0seyJidWNrZXQiOiJ3ZWJmbG93LXByb2QtYXNzZXRzIn0seyJYLUFtei1BbGdvcml0aG0iOiJBV1M0LUhNQUMtU0hBMjU2In0seyJYLUFtei1DcmVkZW50aWFsIjoiQUtJQVFMTEhXRDZNRUpHRVRMU1QvMjAyNjA4MDEvdXMtZWFzdC0xL3MzL2F3czRfcmVxdWVzdCJ9LHsiWC1BbXotRGF0ZSI6IjIwMjYwODAxVDAzMjYxM1oifSx7ImtleSI6IjY4MTA4YzI5MDYzZWViNWQxYmQ3YWU0YS82YTZkNjc1NTAxZjJhMWM5N2UwMTY4MjFfb2xkLWhvbWUtbWFudWFsLXByb2Nlc3MudjIwMjYwODAxZjEyLmNzcyJ9XX0=",
    xAmzSignature: "ab5b53a8b65e46959312778001253385413e1fdf7461b1e64b2d290ec2462d75",
    successActionStatus: "201",
    contentType: "text/css",
    cacheControl: "max-age=31536000",
  },
  uploadUrl: "https://webflow-prod-assets.s3.amazonaws.com/",
};

fs.writeFileSync("tmp-dmp-f12-css.json", JSON.stringify(meta));
const up = spawnSync(
  "node",
  [
    "scripts/upload-webflow-asset.mjs",
    "--complete",
    "tmp-dmp-f12-css.json",
    "public/marketing/old-home-manual-process.v20260801f.css",
  ],
  { encoding: "utf8" }
);
process.stdout.write(up.stdout || "");
process.stderr.write(up.stderr || "");
if (up.status) process.exit(up.status || 1);

const html = fs
  .readFileSync("public/marketing/old-home-manual-process.v20260801f.html", "utf8")
  .trim();
const shell =
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6cdb367404e90afdfdb29a_old-home-manual-process.shell.v20260731a.css";
const css =
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6d675501f2a1c97e016821_old-home-manual-process.v20260801f12.css";
const boot =
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6d621e55fe07fb96c584ed_old-home-manual-process.boot.v20260801f10.js";

const embed =
  `<link rel='stylesheet' href='${shell}' />` +
  `<link rel='stylesheet' href='${css}' />` +
  html +
  `<script src='${boot}' defer></script>`;

fs.writeFileSync("docs/_dmp_embed_inline.html", embed);
console.log(
  JSON.stringify({
    chars: embed.length,
    v132: /1\.1\.32/.test(embed),
    css12: /f12\.css/.test(embed),
  })
);
