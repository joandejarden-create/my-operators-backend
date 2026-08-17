/**
 * Initialize company folder under Brand Reference Material.
 *
 *   npm run partner-reference:init-folder -- --company "Marriott International"
 *   npm run partner-reference:init-folder -- --company "Arbor Lodging" --apply
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  REFERENCE_SUBFOLDERS,
  resolveReferenceRoot,
  sanitizeFolderName,
  writeCaptureReadme,
} from "../lib/partner-intelligence/reference-material-paths.js";
import { getPortalByFolder } from "../api/lib/partner-development-portal-registry.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

function arg(name) {
  const i = process.argv.indexOf(name);
  return i >= 0 ? process.argv[i + 1] : null;
}

const APPLY = process.argv.includes("--apply");
const company = arg("--company");

async function main() {
  if (!company) {
    console.error("Usage: --company \"Parent Company or Operator Name\" [--apply]");
    process.exit(1);
  }

  const folder = sanitizeFolderName(company);
  const root = resolveReferenceRoot();
  const companyDir = path.join(root, folder);
  const subdirs = Object.values(REFERENCE_SUBFOLDERS);

  const portal = getPortalByFolder(folder);

  console.log("Reference root:", root);
  console.log("Company folder:", companyDir);
  if (portal) {
    console.log("Development portal:", portal.developmentPortal);
    if (portal.downloadsPage) console.log("Downloads:", portal.downloadsPage);
  }

  if (!APPLY) {
    console.log("\nWould create:", subdirs.map((s) => path.join(folder, s)).join(", "));
    console.log("Dry run — add --apply to create folders.");
    process.exit(0);
  }

  fs.mkdirSync(companyDir, { recursive: true });
  for (const sub of subdirs) {
    fs.mkdirSync(path.join(companyDir, sub), { recursive: true });
  }
  const readme = writeCaptureReadme(folder, companyDir);

  if (portal) {
    const portalNote = path.join(companyDir, "PORTAL-LINKS.md");
    fs.writeFileSync(
      portalNote,
      `# ${folder} — Official development links

- **Portal:** ${portal.developmentPortal}
${portal.downloadsPage ? `- **Downloads:** ${portal.downloadsPage}\n` : ""}${portal.fddNotes ? `- **FDD:** ${portal.fddNotes}\n` : ""}${portal.regionalNotes ? `- **Regional:** ${portal.regionalNotes}\n` : ""}

## Search patterns

${portal.searchPatterns.map((p) => `- \`${p}\``).join("\n")}
`,
      "utf8"
    );
    console.log("Wrote", portalNote);
  }

  console.log("Created", companyDir);
  console.log("README:", readme);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
