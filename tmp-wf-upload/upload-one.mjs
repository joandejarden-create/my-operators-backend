import fs from "fs";
import path from "path";
import https from "https";
import { URL } from "url";

const metaPath = process.argv[2];
if (!metaPath) {
  console.error("usage: node upload-one.mjs <meta.json>");
  process.exit(1);
}

const meta = JSON.parse(fs.readFileSync(metaPath, "utf8"));
const fileBuf = fs.readFileSync(meta.file);
const d = meta.uploadDetails;
const boundary = "----WebKitFormBoundary" + Math.random().toString(16).slice(2);
const parts = [];

function addField(name, value) {
  parts.push(
    Buffer.from(
      `--${boundary}\r\nContent-Disposition: form-data; name="${name}"\r\n\r\n${value}\r\n`
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
addField("Content-Type", d.contentType);
addField("Cache-Control", d.cacheControl);

const fileName = path.basename(meta.file);
parts.push(
  Buffer.from(
    `--${boundary}\r\nContent-Disposition: form-data; name="file"; filename="${fileName}"\r\nContent-Type: ${d.contentType}\r\n\r\n`
  )
);
parts.push(fileBuf);
parts.push(Buffer.from(`\r\n--${boundary}--\r\n`));
const body = Buffer.concat(parts);

const u = new URL(meta.uploadUrl);
const opts = {
  method: "POST",
  hostname: u.hostname,
  path: u.pathname,
  headers: {
    "Content-Type": `multipart/form-data; boundary=${boundary}`,
    "Content-Length": body.length,
  },
};

const status = await new Promise((resolve, reject) => {
  const req = https.request(opts, (res) => {
    let data = "";
    res.on("data", (c) => (data += c));
    res.on("end", () =>
      resolve({ status: res.statusCode, body: data.slice(0, 300) })
    );
  });
  req.on("error", reject);
  req.write(body);
  req.end();
});

console.log(meta.name || fileName, status.status, status.body);
if (status.status !== 201) process.exit(1);
