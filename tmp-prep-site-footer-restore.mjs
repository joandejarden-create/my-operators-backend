import fs from "fs";

const restore = fs.readFileSync("tmp-restore-site-footer.html", "utf8").trimEnd();
const injector = `
<!-- ohProblemV2: Old Home The Problem rebuild (page freeform write returned 406) -->
<script src="https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/PLACEHOLDER_old-home-problem-v2.v20260729a.js"></script>
`;
fs.writeFileSync("tmp-site-footer-restored-with-problem.txt", restore + "\n" + injector);
console.log("chars", (restore + injector).length);
