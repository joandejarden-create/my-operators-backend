import { readFileSync, writeFileSync } from "fs";

const html = readFileSync("public/brand-development-dashboard.html", "utf8");
const m = html.match(/<style>([\s\S]*?)<\/style>/);
if (!m) throw new Error("no style block in brand-development-dashboard.html");

const dedented = m[1]
  .split("\n")
  .map((line) => line.replace(/^        /, ""))
  .join("\n")
  .trim();

const embedExtras = `
/* App shell embed (operator + brand when iframed) */
.embed-mode .dashboard-main-section { margin-left: 0 !important; }
.embed-mode .bdd-product-note { display: none; }
.embed-mode .sidebar-wrapper { display: none !important; }

.bdd-setup-banner {
  display: flex;
  align-items: flex-start;
  gap: 12px;
  padding: 12px 16px;
  margin-bottom: 16px;
  border-radius: 8px;
  border: 0.6px solid var(--neutral--600);
  background: rgba(16, 25, 53, 0.55);
  font-size: 13px;
  color: var(--neutral--400);
}
.bdd-setup-banner__label {
  flex-shrink: 0;
  font-size: 10px;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.06em;
  color: var(--accent--primary-1);
}
.bdd-inline-error {
  padding: 12px 16px;
  margin-bottom: 16px;
  border-radius: 8px;
  border: 1px solid rgba(255, 90, 101, 0.45);
  background: rgba(255, 90, 101, 0.08);
  color: var(--neutral--200);
  font-size: 13px;
  line-height: 1.5;
}
.bdd-results-count {
  font-size: 13px;
  color: var(--neutral--500);
}
.bdd-opportunity-title {
  color: var(--neutral--100);
  font-weight: 600;
  font-size: 12px;
  display: block;
}
.bdd-opportunity-sub {
  display: block;
  font-size: 11px;
  color: var(--neutral--500);
  margin-top: 4px;
  line-height: 1.35;
}
.bdd-status-text-plain {
  font-size: 12px;
  color: var(--neutral--300);
}
.bdd-row-actions {
  display: flex;
  flex-direction: column;
  gap: 6px;
  align-items: stretch;
  min-width: 120px;
}
#oddModalBody label {
  display: block;
  margin-bottom: 12px;
  font-size: 12px;
  color: var(--neutral--400);
}
#oddModalBody textarea,
#oddModalBody input[type="text"],
#oddModalBody input[type="date"] {
  width: 100%;
  margin-top: 6px;
  background: var(--secondary--color-1);
  border: 1px solid rgba(255, 255, 255, 0.18);
  color: var(--neutral--100);
  padding: 10px 14px;
  border-radius: 8px;
  font-size: 0.875rem;
  font-family: inherit;
  box-sizing: border-box;
}
`;

const header =
  "/**\n * Workspace shell styles — full extract from brand-development-dashboard.html\n * Regenerate: node scripts/extract-deal-workspace-shell-css.mjs\n */\n\n";

writeFileSync("public/css/deal-workspace-shell.css", header + dedented + "\n" + embedExtras);
console.log("Wrote public/css/deal-workspace-shell.css (" + dedented.length + " chars)");
