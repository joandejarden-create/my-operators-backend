/**
 * Validate that drawer-comparison AFTER slots carry recovered public channels
 * for every Sheraton opportunity (general path — not EUROGUSS-only).
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  buildSheratonDrawerComparisonInventory,
  projectDrawerComparisonPair,
} from "../../lib/group-demand-intelligence/presentation/gdi-sheraton-drawer-comparison-v1.js";
import { proposeAirtablePublicContactUpsert } from "../../lib/group-demand-intelligence/presentation/gdi-public-contact-channel-hierarchy-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const inv = buildSheratonDrawerComparisonInventory();
const rows = [];
const concurrency = 4;
let idx = 0;

async function one(item) {
  const pair = await projectDrawerComparisonPair(item.raw, {
    hotelId: "recCAGqkERSXgiku4",
  });
  const before = pair.before?.scalingTwoContactPresentation?.primary || {};
  const after = pair.after?.scalingTwoContactPresentation?.primary || {};
  const meta = pair.after?.publicChannelRecoveryMeta || null;
  const proposal = proposeAirtablePublicContactUpsert(pair.after);
  return {
    id: item.id,
    title: item.raw?.title || null,
    before: {
      email: before.email || null,
      phone: before.phone || null,
      org: before.organizationEmail || null,
      surfe: before.surfeEnrichmentChannel || null,
    },
    after: {
      email: after.email || null,
      phone: after.phone || null,
      org: after.organizationEmail || null,
      surfe: after.surfeEnrichmentChannel || null,
      emailLabel: after.emailLabel || null,
      phoneLabel: after.phoneLabel || null,
      orgLabel: after.organizationEmailLabel || null,
      officialContactUrl: after.officialContactUrl || null,
    },
    recovery: meta,
    upsertProposed: Boolean(
      proposal && (after.email || after.phone || after.organizationEmail),
    ),
    channelsAppeared:
      Boolean(after.email || after.phone || after.organizationEmail) &&
      !(after.email && after.surfeEnrichmentChannel === "both") &&
      !(after.email && after.phone && after.surfeEnrichmentChannel),
  };
}

const queue = [...inv.items];
await Promise.all(
  Array.from({ length: concurrency }, async () => {
    while (idx < queue.length) {
      const i = idx++;
      rows.push(await one(queue[i]));
    }
  }),
);

rows.sort((a, b) => String(a.id).localeCompare(String(b.id)));
const euroguss = rows.find((r) => r.id === "paid3_565ce4588e6d");
const recovered = rows.filter(
  (r) => r.recovery?.changed || r.after.email || r.after.phone || r.after.org,
);
const summary = {
  total: rows.length,
  afterWithDirectEmail: rows.filter((r) => r.after.email).length,
  afterWithDirectPhone: rows.filter((r) => r.after.phone).length,
  afterWithOrgRoute: rows.filter((r) => r.after.org).length,
  recoveryChanged: rows.filter((r) => r.recovery?.changed).length,
  upsertsProposed: rows.filter((r) => r.upsertProposed).length,
  eurogussPass: Boolean(
    euroguss?.after?.email === "gissella.palma@nmna.us" &&
      /618-5833/.test(String(euroguss?.after?.phone || "")) &&
      euroguss?.after?.org === "info@euroguss-mexico.com" &&
      !euroguss?.after?.surfe &&
      !euroguss?.before?.email &&
      !euroguss?.before?.phone,
  ),
  euroguss,
};

const out = path.join(__dirname, "CONTACT_CHANNELS_LIVE_VALIDATION.json");
fs.writeFileSync(out, JSON.stringify({ summary, rows }, null, 2));
console.log(JSON.stringify(summary, null, 2));
console.log("Wrote", out);
