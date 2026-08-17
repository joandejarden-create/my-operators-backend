import fs from "fs";
import crypto from "crypto";

const p = "public/marketing/old-home-problem-v2.v20260729e.js";
const b = fs.readFileSync(p);
console.log("md5", crypto.createHash("md5").update(b).digest("hex"));
console.log("bytes", b.length);
console.log("has w21", b.toString().includes("v20260729w21.css"));
