import {
  resolveExternalClientLinks,
  fingerprintBethesdaGdiContractToken,
  BETHESDA_GDI_CONTRACT_TOKEN_ID,
} from "../lib/admin/report-external-client-links-v1.js";
import fs from "node:fs";

const tokens = JSON.parse(
  fs.readFileSync("config/client-share/gdi-share-registry/active-tokens.json", "utf8")
).tokens;
const active = Object.values(tokens).filter(
  (t) => t.status === "ACTIVE" && !t.revokedAt
);
const find = (re) => active.find((t) => re.test(String(t.label || "")));
const want = [
  { name: "Bethesda", hotelId: "recLuxvwwxID7U2B8" },
  { name: "Hilton New York Times Square", re: /Hilton.*Times Square/i },
  { name: "Renaissance New York Times Square", re: /Renaissance.*Times Square/i },
  { name: "Waterstone", re: /Waterstone/i },
  { name: "Cambridge Beaches", re: /Cambridge Beaches/i },
  { name: "NOW NOW NOHO", re: /NOW NOW NOHO|NOHO/i },
];
const shares = [];
for (const w of want) {
  let hotelId = w.hotelId;
  if (!hotelId) hotelId = find(w.re)?.hotelId || null;
  if (!hotelId) {
    shares.push({ name: w.name, present: false });
    continue;
  }
  const r = resolveExternalClientLinks(hotelId, { createIfMissing: false });
  shares.push({
    name: w.name,
    hotelId,
    present: !!r?.gdi?.url,
    tokenId: r?.gdi?.tokenId || null,
  });
}
const fp = fingerprintBethesdaGdiContractToken();
const out = {
  bethesdaTokenUnchanged:
    fp.tokenId === BETHESDA_GDI_CONTRACT_TOKEN_ID && fp.present,
  fp,
  shares,
};
fs.mkdirSync("reports/gdi-external-restore-v1", { recursive: true });
fs.writeFileSync(
  "reports/gdi-external-restore-v1/other-shares-resolve.json",
  JSON.stringify(out, null, 2)
);
console.log(JSON.stringify(out, null, 2));
