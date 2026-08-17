import fs from "fs";

const t = fs.readFileSync(
  "C:/Users/joand/.cursor/projects/c-Dev-deal-capture-proxy/agent-tools/553be4bb-6c30-4acd-8854-90a3c881994b.txt",
  "utf8"
);
const j = JSON.parse(t);
function findCode(node, path = []) {
  if (!node || typeof node !== "object") return null;
  if (typeof node.value === "string" && node.value.includes("dealality-problem-desk")) {
    return { path: path.join("."), value: node.value };
  }
  if (typeof node.code === "string" && node.code.includes("dealality-problem-desk")) {
    return { path: path.join(".") + ".code", value: node.code };
  }
  for (const [k, v] of Object.entries(node)) {
    const hit = findCode(v, path.concat(k));
    if (hit) return hit;
  }
  return null;
}
const hit = findCode(j);
if (!hit) {
  console.log(JSON.stringify({ ok: false, keys: Object.keys(j), sample: t.slice(0, 500) }, null, 2));
  process.exit(1);
}
const s = hit.value;
console.log(
  JSON.stringify(
    {
      path: hit.path,
      len: s.length,
      hasProbe: s.includes("polish-v2 probe"),
      hasProbeOnly: />probe<\/div>/.test(s) && !s.includes("cinematic-v1"),
      hasCinematic: s.includes('data-visual="cinematic-v1"'),
      hasHotel: s.includes("6a6bde85c014ee4e80e65c24"),
      hasImport: s.includes("@import"),
      hasStyleId: s.includes('id="oh-deal-desk"'),
      hasScript: /<script/i.test(s),
      hasAnimation: /@keyframes|animation\s*:/.test(s),
      hasStrip: s.includes("dpd-strip"),
      defaultState: (s.match(/data-story-state="([^"]+)"/) || [])[1],
      head: s.slice(0, 180),
    },
    null,
    2
  )
);
