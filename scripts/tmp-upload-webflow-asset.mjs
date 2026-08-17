import fs from "fs";
import crypto from "crypto";
import https from "https";
import { URL } from "url";

const filePath = process.argv[2];
const detailsPath = process.argv[3];
let uploadUrl = process.env.WF_UPLOAD_URL;
let d = process.env.WF_UPLOAD_DETAILS ? JSON.parse(process.env.WF_UPLOAD_DETAILS) : null;
let contentType = process.env.WF_CONTENT_TYPE || null;

if (detailsPath) {
  const meta = JSON.parse(fs.readFileSync(detailsPath, "utf8"));
  uploadUrl = meta.uploadUrl || uploadUrl;
  d = meta.uploadDetails || d;
  contentType = meta.contentType || contentType;
}
contentType = contentType || d?.contentType || "application/octet-stream";

if (!filePath || !uploadUrl || !d) {
  console.error(
    "Usage: node scripts/tmp-upload-webflow-asset.mjs <file> [details.json]"
  );
  process.exit(1);
}

const file = fs.readFileSync(filePath);
const fileName = filePath.split(/[/\\]/).pop();
const boundary = "----WebKitFormBoundary" + crypto.randomBytes(16).toString("hex");
const parts = [];

function addField(name, val) {
  parts.push(
    Buffer.from(
      `--${boundary}\r\nContent-Disposition: form-data; name="${name}"\r\n\r\n${val}\r\n`
    )
  );
}

addField("acl", d.acl);
addField("bucket", d.bucket);
addField("X-Amz-Algorithm", d.xAmzAlgorithm);
addField("X-Amz-Credential", d.xAmzCredential);
addField("X-Amz-Date", d.xAmzDate);
addField("key", d.key);
addField("Policy", d.policy);
addField("X-Amz-Signature", d.xAmzSignature);
addField("success_action_status", d.successActionStatus);
addField("Content-Type", contentType);
addField("Cache-Control", d.cacheControl);
parts.push(
  Buffer.from(
    `--${boundary}\r\nContent-Disposition: form-data; name="file"; filename="${fileName}"\r\nContent-Type: ${contentType}\r\n\r\n`
  )
);
parts.push(file);
parts.push(Buffer.from(`\r\n--${boundary}--\r\n`));
const body = Buffer.concat(parts);

const u = new URL(uploadUrl);
const req = https.request(
  {
    hostname: u.hostname,
    path: u.pathname,
    method: "POST",
    headers: {
      "Content-Type": `multipart/form-data; boundary=${boundary}`,
      "Content-Length": body.length,
    },
  },
  (res) => {
    let data = "";
    res.on("data", (c) => (data += c));
    res.on("end", () => {
      console.log("status", res.statusCode);
      if (data) console.log(data.slice(0, 500));
      process.exit(res.statusCode === 201 ? 0 : 1);
    });
  }
);
req.on("error", (e) => {
  console.error(e);
  process.exit(1);
});
req.write(body);
req.end();
