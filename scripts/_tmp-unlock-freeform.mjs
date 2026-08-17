import fs from "fs";

function unlock(src, dst, replacements) {
  let t = fs.readFileSync(src, "utf8");
  for (const [a, b] of replacements) t = t.split(a).join(b);
  fs.writeFileSync(dst, t);
  console.log(
    dst,
    [...t.matchAll(/if \([^)]*path[^)]*\) return;/gi)].map((m) => m[0])
  );
}

unlock("old-home-section-order.v20260731c.js", "old-home-section-order.v20260801a.js", [
  ["Path-gated to /old-home.", "Path-gated to / and /old-home (homepage cutover)."],
  [
    'var path = (location.pathname || "").replace(/\\/+$/, "").toLowerCase();',
    'var path = (location.pathname || "").replace(/\\/+$/, "").toLowerCase() || "/";',
  ],
  ['if (path !== "/old-home") return;', 'if (path !== "/" && path !== "/old-home") return;'],
]);

unlock(
  "old-home-problem-storyboard.v20260729b.js",
  "old-home-problem-storyboard.v20260801a.js",
  [
    ["Path-gated to /old-home.", "Path-gated to / and /old-home (homepage cutover)."],
    ['if (PATH !== "/old-home") return;', 'if (PATH !== "/" && PATH !== "/old-home") return;'],
  ]
);
