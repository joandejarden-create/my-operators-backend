import fs from "fs";

const env = fs.readFileSync(".env", "utf8");
const keys = (env.match(/^[A-Z0-9_]*(WEBFLOW|WF)[A-Z0-9_]*=/gm) || []).slice(0, 30);
console.log(keys.join("\n"));
