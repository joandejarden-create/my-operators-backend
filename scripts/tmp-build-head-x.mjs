import fs from "fs";

const head = `<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter+Tight:wght@400;500;600;700;800&family=Plus+Jakarta+Sans:wght@400;500;600;700;800&family=Lora:ital,wght@0,400;1,400&display=swap" rel="stylesheet">
<link rel="stylesheet" href="https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a68b18e603bd344f7a9c64a_dealality-old-home-dark.v20260728x.css">
`;

// Keep existing inline block by reading from stdin file written separately
const inline = fs.readFileSync("tmp-old-home-head-inline.txt", "utf8");
fs.writeFileSync("tmp-old-home-head-full.txt", head + inline);
console.log("wrote", head.length + inline.length);
