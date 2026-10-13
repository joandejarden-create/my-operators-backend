import fs from "node:fs";
import { listPursuits } from "../lib/group-demand-intelligence/pursuit/pursuit-store-v1.js";

const ui = fs.readFileSync(
  "public/js/group-demand-intelligence/dealality-gdi-ui.js",
  "utf8"
);
const start = ui.indexOf("function workflowPresetHtml");
const chunk = ui.slice(start, start + 1200);
const out = {
  workflowChunkHasOnlyThree:
    /label: "All"/.test(chunk) &&
    /label: "Ready"/.test(chunk) &&
    /label: "Watching"/.test(chunk) &&
    !/Active Pursuits/.test(chunk) &&
    !/Follow-Up Due/.test(chunk) &&
    !/Hotel Selection/.test(chunk) &&
    !/label: "Closed"/.test(chunk),
  startPursuit: /Start Pursuit/.test(ui),
  viewPursuit: /View Pursuit/.test(ui),
  pursuitPanel: /function pursuitPanelHtml/.test(ui),
  pursuitRecords:
    listPursuits("rec2PVBDavppGpenm").length +
    listPursuits("recUOyzOXn2Zdp98I").length,
};
console.log(JSON.stringify(out, null, 2));
if (!out.workflowChunkHasOnlyThree || out.pursuitRecords !== 5) process.exit(1);
