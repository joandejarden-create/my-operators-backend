import fs from "fs";

const raw = fs.readFileSync(
  "C:/Users/joand/.cursor/projects/c-Dev-deal-capture-proxy/agent-tools/3a764595-f812-475e-bc44-1c217ebc8e4a.txt",
  "utf8"
);
for (const line of raw.trim().split(/\n/)) {
  try {
    const j = JSON.parse(line);
    const regs = j.result?.registeredScripts;
    if (regs) {
      const hits = regs.filter((r) =>
        /modules|copy|benefit/i.test(
          [r.id, r.displayName, r.hostedLocation].join(" ")
        )
      );
      console.log(
        JSON.stringify(
          hits.map((h) => ({
            id: h.id,
            loc: h.hostedLocation,
            ver: h.version,
          })),
          null,
          2
        )
      );
    }
    if (j.label === "site_footer") {
      const c = j.result?.content || "";
      console.log("footer has modules-copy", /modules-copy/.test(c));
      const m = c.match(/old-home-modules-copy[^"'\s<>]*/g);
      console.log("matches", m);
    }
  } catch {
    // ignore non-json lines
  }
}
