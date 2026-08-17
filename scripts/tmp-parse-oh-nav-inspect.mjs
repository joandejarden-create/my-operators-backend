import fs from "fs";

const raw = fs.readFileSync(
  "C:/Users/joand/.cursor/projects/c-Dev-deal-capture-proxy/agent-tools/1ebb43e8-8123-4211-8c3e-cf57f3651b55.txt",
  "utf8"
);

// File may be multiple JSON objects (one per line or concatenated). Split carefully.
const chunks = [];
let depth = 0;
let start = -1;
for (let i = 0; i < raw.length; i++) {
  const ch = raw[i];
  if (ch === "{") {
    if (depth === 0) start = i;
    depth++;
  } else if (ch === "}") {
    depth--;
    if (depth === 0 && start >= 0) {
      chunks.push(raw.slice(start, i + 1));
      start = -1;
    }
  }
}

const out = { sections: {}, nav: [], mnav: [], footer: [], howLinks: [] };

function linkLabel(m) {
  const c = (m.children || []).find((x) => x.type === "String");
  return c?.textContent || "(no text)";
}

for (const chunk of chunks) {
  let obj;
  try {
    obj = JSON.parse(chunk);
  } catch {
    continue;
  }
  const results = obj?.result || [];
  for (const r of results) {
    for (const q of r.data || []) {
      if (
        [
          "about",
          "how",
          "modules",
          "pricing",
          "faq",
          "insights",
          "trust",
          "nav",
          "mnav",
        ].includes(q.label)
      ) {
        out.sections[q.label] = {
          total: q.total_matches,
          matches: (q.matches || []).map((m) => ({
            id: m.id,
            type: m.type,
            attrs: m.attributes,
            styles: m.styleNames,
            text: (m.children || [])
              .filter((c) => c.type === "String")
              .map((c) => c.textContent)
              .join("|"),
          })),
        };
      }
      if (q.label === "all-links") {
        for (const m of q.matches || []) {
          const label = linkLabel(m);
          const href = m.attributes?.href || m.settings?.link?.href || null;
          const linkType = m.settings?.link?.linkType || null;
          const pageSectionId = m.settings?.link?.pageSectionId || null;
          const styles = m.styleNames || [];
          const row = {
            label,
            href,
            linkType,
            pageSectionId,
            styles,
            id: m.id,
            attrId: m.attributes?.id || null,
          };
          if (styles.includes("oh-nav-link") || styles.includes("oh-nav-signin") || styles.includes("oh-nav-cta") || styles.includes("oh-nav-logo")) {
            out.nav.push(row);
          }
          if (styles.includes("oh-mnav-link")) out.mnav.push(row);
          if (
            label === "About" ||
            label === "Case Studies" ||
            label === "Insights" ||
            label === "How It Works" ||
            label === "FAQs" ||
            label === "The Dealality Method" ||
            label === "Strategic Paths" ||
            label === "Proposal Comparison" ||
            (m.attributes?.id || "").startsWith("footer")
          ) {
            // footer-ish: collect by known footer ids / labels without nav styles
            if (!styles.includes("oh-nav-link") && !styles.includes("oh-mnav-link")) {
              out.footer.push(row);
            }
          }
          if (label === "How It Works") out.howLinks.push(row);
        }
      }
    }
  }
}

console.log(JSON.stringify(out, null, 2));
