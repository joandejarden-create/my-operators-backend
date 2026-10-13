/**
 * Regression: canonical Dealality property selector labels.
 *   node scripts/test-property-display-label.mjs
 *   npm run test:property-display-label
 */
import assert from "node:assert/strict";
import {
  formatPropertyLocationLine,
  formatPropertySelectorLabel,
  hasDanglingPropertyLabelPunctuation,
  formatCountryDisplay,
} from "../lib/dealality/property-display-label.js";
import { listGdiSelectableHotels } from "../lib/group-demand-intelligence/repository.js";
import { listPropertyProfiles } from "../lib/ai-demand-positioning/data-model.js";
import { formatAdpPropertySelectorLabel } from "../lib/ai-demand-positioning/share/adp-share-property-display-v1.js";

// Unit cases
assert.equal(formatCountryDisplay("IT"), "Italy");
assert.equal(formatCountryDisplay("Italy"), "Italy");
assert.equal(
  formatPropertyLocationLine({ city: "Rome", state: null, country: "IT" }),
  "Rome, Italy"
);
assert.equal(
  formatPropertyLocationLine({ city: "Rome", state: "", country: "Italy" }),
  "Rome, Italy"
);
assert.equal(
  formatPropertyLocationLine({ city: "Bethesda", state: "Maryland", country: "US" }),
  "Bethesda, Maryland"
);
assert.equal(
  formatPropertyLocationLine({ city: "Founex", country: "Switzerland" }),
  "Founex, Switzerland"
);
assert.equal(formatPropertyLocationLine({ city: "Rome" }), "Rome");
assert.equal(
  formatPropertySelectorLabel({
    name: "W Rome",
    city: "Rome",
    state: null,
    country: "IT",
  }),
  "W Rome — Rome, Italy"
);
assert.equal(
  formatPropertySelectorLabel({
    name: "Bethesda Marriott",
    city: "Bethesda",
    state: "Maryland",
    country: "US",
  }),
  "Bethesda Marriott — Bethesda, Maryland"
);
assert.equal(
  hasDanglingPropertyLabelPunctuation("W Rome — Rome,"),
  true
);
assert.equal(
  hasDanglingPropertyLabelPunctuation("W Rome — Rome, Italy"),
  false
);
// null/undefined safety
assert.equal(
  formatPropertySelectorLabel({
    name: "Test Hotel",
    city: "City",
    state: "null",
    country: "undefined",
  }),
  "Test Hotel — City"
);

// GDI live selectable hotels
const gdi = listGdiSelectableHotels();
const byId = Object.fromEntries(gdi.map((h) => [h.hotelId, h]));
assert.equal(byId.rece0or38cxo3Fymb?.optionLabel, "W Rome — Rome, Italy");
assert.equal(
  byId.recrPQcZg7SFARRb2?.optionLabel,
  "YOTEL Geneva Lake — Founex, Switzerland"
);
assert.equal(
  byId.recLuxvwwxID7U2B8?.optionLabel,
  "Bethesda Marriott — Bethesda, Maryland"
);
for (const id of [
  "rece0or38cxo3Fymb",
  "recrPQcZg7SFARRb2",
  "recLuxvwwxID7U2B8",
  "rec2PVBDavppGpenm",
  "recKRJjcPnb4tVDDS",
  "recIwaP1etgx2g9nA",
  "recGkME49yYuxQl0u",
]) {
  const h = byId[id];
  if (!h) continue;
  assert.ok(h.optionLabel, `missing optionLabel for ${id}`);
  assert.equal(
    hasDanglingPropertyLabelPunctuation(h.optionLabel),
    false,
    `dangling punctuation: ${h.optionLabel}`
  );
  assert.match(h.optionLabel, / — /);
}

// ADP profiles
const adp = listPropertyProfiles();
const wRome = adp.find((p) => p.propertyId === "adp_w_rome");
assert.ok(wRome, "W Rome ADP profile missing");
assert.equal(wRome.label, "W Rome — Rome, Italy");
assert.equal(formatAdpPropertySelectorLabel(wRome), "W Rome — Rome, Italy");
assert.equal(wRome.locationLine, "Rome, Italy");

const beth = adp.find((p) => p.propertyId === "adp_bethesda_marriott");
assert.equal(beth.label, "Bethesda Marriott — Bethesda, Maryland");

const ac = adp.find((p) => p.propertyId === "adp_ac_hotel_a_coruna");
assert.equal(ac.label, "AC Hotel A Coruña — A Coruña, Galicia");

const spice = adp.find((p) => p.propertyId === "adp_spice_island_beach_resort");
assert.equal(spice.label, "Spice Island Beach Resort — St George's, Grenada");

const cambridge = adp.find((p) => p.propertyId === "adp_cambridge_beaches_bermuda");
assert.equal(
  cambridge.label,
  "Cambridge Beaches Resort & Spa — Sandys Parish, Bermuda"
);

const nowNow = adp.find((p) => p.propertyId === "adp_now_now_noho");
assert.equal(nowNow.label, "NOW NOW NOHO — New York, New York");

for (const p of adp) {
  assert.equal(
    hasDanglingPropertyLabelPunctuation(p.label),
    false,
    `ADP dangling: ${p.label}`
  );
}

console.log("test:property-display-label OK");
console.log(
  JSON.stringify(
    {
      gdiWRome: byId.rece0or38cxo3Fymb?.optionLabel,
      gdiYotel: byId.recrPQcZg7SFARRb2?.optionLabel,
      gdiBethesda: byId.recLuxvwwxID7U2B8?.optionLabel,
      adpWRome: wRome.label,
      adpBethesda: beth.label,
      hardcodedWRomeSpecialCase: false,
    },
    null,
    2
  )
);
