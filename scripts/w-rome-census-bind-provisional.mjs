/**
 * Bind provisional gdi_hotel_w_rome → canonical HPC rece0or38cxo3Fymb.
 * Updates alias map, ADP census link, GDI hotel config (redirect + canonical file).
 * Does NOT create duplicate Fits/Targets — only identity linkage.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  loadAdpGdiAliasMap,
  saveAdpGdiAliasMap,
  resolveCanonicalHotelId,
} from "../lib/hotel-census/adp-gdi-canonical-identity.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const CANONICAL = "rece0or38cxo3Fymb";
const PROVISIONAL = "gdi_hotel_w_rome";
const ADP = "adp_w_rome";
const OUT = path.join(
  ROOT,
  "reports/group-demand-intelligence/w-rome-census-stewardship-v1"
);

function main() {
  fs.mkdirSync(OUT, { recursive: true });

  // 1) Alias map
  const map = loadAdpGdiAliasMap({ force: true });
  map.aliases[CANONICAL] = {
    canonicalHotelId: CANONICAL,
    displayName: "W Rome",
    identityKey: "ind_marriott_it_romwv",
    adpPropertyId: ADP,
    provisionalGdiHotelId: PROVISIONAL,
    adp: true,
    gdi: true,
    linkStatus: "canonical",
  };
  map.aliases[PROVISIONAL] = {
    canonicalHotelId: CANONICAL,
    displayName: "W Rome",
    identityKey: "ind_marriott_it_romwv",
    adpPropertyId: ADP,
    provisionalGdiHotelId: PROVISIONAL,
    adp: true,
    gdi: true,
    linkStatus: "canonical",
    aliasOf: CANONICAL,
  };
  map.aliases[ADP] = {
    canonicalHotelId: CANONICAL,
    aliasOf: CANONICAL,
    provisionalGdiHotelId: PROVISIONAL,
    linkStatus: "canonical",
  };
  saveAdpGdiAliasMap(map);

  // 2) ADP census-links registry
  const linksPath = path.join(ROOT, "fixtures/ai-demand-positioning/census-links-v1.json");
  const links = JSON.parse(fs.readFileSync(linksPath, "utf8"));
  links.links = links.links || {};
  links.links[ADP] = {
    censusRecordId: CANONICAL,
    displayName: "W Rome",
    identityKey: "ind_marriott_it_romwv",
    linkStatus: "canonical",
    linkedAt: new Date().toISOString(),
    note: "Bound after W Rome census stewardship V1 insert",
  };
  fs.writeFileSync(linksPath, JSON.stringify(links, null, 2) + "\n");

  // 3) Canonical GDI hotel config
  const provisionalPath = path.join(
    ROOT,
    "config/group-demand-intelligence/hotels",
    `${PROVISIONAL}.json`
  );
  const provisional = JSON.parse(fs.readFileSync(provisionalPath, "utf8"));
  const canonicalCfg = {
    ...provisional,
    hotelId: CANONICAL,
    displayName: "W Rome",
    country: "Italy",
    city: "Rome",
    aliases: {
      adpPropertyId: ADP,
      marriottPropertyCode: "ROMWV",
      censusRecordId: CANONICAL,
      provisionalGdiHotelId: PROVISIONAL,
      note: "Canonical hotelId is HPC record. Provisional GDI key retained as alias only.",
    },
    geo: {
      latitude: 41.906211,
      longitude: 12.488489,
    },
    gdiOnboarding: {
      status: "CANONICAL",
      censusLinkStatus: "canonical",
      blocking: [],
      officialWebsite: "https://www.marriott.com/en-us/hotels/romwv-w-rome/overview/",
      marsha: "ROMWV",
      boundFromProvisional: PROVISIONAL,
      boundAt: new Date().toISOString(),
    },
  };
  delete canonicalCfg.redirectTo;
  fs.writeFileSync(
    path.join(ROOT, "config/group-demand-intelligence/hotels", `${CANONICAL}.json`),
    JSON.stringify(canonicalCfg, null, 2) + "\n"
  );

  // 4) Provisional becomes redirect-only
  fs.writeFileSync(
    provisionalPath,
    JSON.stringify(
      {
        hotelId: PROVISIONAL,
        deprecatedProvisionalId: PROVISIONAL,
        redirectTo: CANONICAL,
        aliases: {
          adpPropertyId: ADP,
          marriottPropertyCode: "ROMWV",
          censusRecordId: CANONICAL,
          note: "Canonical hotelId is HPC record. Provisional GDI key retained as alias only.",
          provisionalGdiHotelId: PROVISIONAL,
        },
        note: "DEPRECATED provisional config — use canonical HPC hotelId file.",
      },
      null,
      2
    ) + "\n"
  );

  // 5) ADP fixture census hint
  const adpFixPath = path.join(ROOT, "fixtures/ai-demand-positioning/w-rome-property-profile.json");
  const adpFix = JSON.parse(fs.readFileSync(adpFixPath, "utf8"));
  adpFix.censusRecordId = CANONICAL;
  adpFix.geography = {
    ...(adpFix.geography || {}),
    latitude: 41.906211,
    longitude: 12.488489,
  };
  adpFix.stewardship = {
    censusLinkStatus: "canonical",
    peerPackStatus: "STEWARDSHIP_REQUIRED",
    note: "Census linked. Peer set and ADP baseline still require human stewardship. Do not invent peers.",
  };
  fs.writeFileSync(adpFixPath, JSON.stringify(adpFix, null, 2) + "\n");

  const resolved = resolveCanonicalHotelId(PROVISIONAL);
  const report = {
    from: PROVISIONAL,
    to: CANONICAL,
    resolveProvisional: resolved,
    resolveCanonical: resolveCanonicalHotelId(CANONICAL),
    resolveAdp: resolveCanonicalHotelId(ADP),
    status: resolved === CANONICAL ? "PASS" : "FAIL",
    duplicatedFits: 0,
    duplicatedTargets: 0,
    note: "Identity bind only — Fit/Target apply uses canonical hotelId (no duplicate seed rows from provisional).",
  };
  fs.writeFileSync(path.join(OUT, "PROVISIONAL_BIND.json"), JSON.stringify(report, null, 2));
  console.log(JSON.stringify(report, null, 2));
  if (report.status !== "PASS") process.exit(1);
}

main();
