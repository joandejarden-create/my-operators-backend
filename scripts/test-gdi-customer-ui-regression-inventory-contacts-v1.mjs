/**
 * GDI customer UI regression — inventory empty states + contact name visibility.
 *
 *   npm run test:gdi-customer-ui-regression-inventory-contacts-v1
 *   node scripts/test-gdi-customer-ui-regression-inventory-contacts-v1.mjs
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildCanonicalCustomerPresentationContract,
  resolveCanonicalContactNameAndKind,
} from "../lib/group-demand-intelligence/presentation/gdi-canonical-customer-presentation-contract-v1.js";
import {
  attachUniversalTileContactPreview,
  selectUniversalTileContactPreview,
  classifyCustomerContactDisplayAuth,
  CUSTOMER_CONTACT_DISPLAY_AUTH,
  TILE_NO_NAMED_CONTACT_LABEL,
} from "../lib/group-demand-intelligence/universal-tile-contact-preview-v1.js";
import { inspectFounderPreviewPublicationAvailability } from "../lib/group-demand-intelligence/publication-package/gdi-founder-preview-availability-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const CACHE_BUST_UI = "gdi-sortable-list-v1";
const CACHE_BUST_APP = "gdi-sortable-list-v1";

let failed = 0;
function check(name, fn) {
  try {
    fn();
    console.log(`PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`FAIL ${name}: ${err.message}`);
  }
}

const appJs = fs.readFileSync(
  path.join(ROOT, "public/js/group-demand-intelligence/app.js"),
  "utf8",
);
const uiJs = fs.readFileSync(
  path.join(ROOT, "public/js/group-demand-intelligence/dealality-gdi-ui.js"),
  "utf8",
);
const apiJs = fs.readFileSync(
  path.join(ROOT, "api/group-demand-intelligence.js"),
  "utf8",
);
const html = fs.readFileSync(
  path.join(ROOT, "public/group-demand-intelligence.html"),
  "utf8",
);

check("empty_state_no_internal_staged_version_jargon", () => {
  assert.doesNotMatch(appJs, /No staged version available for this hotel/);
  assert.doesNotMatch(apiJs, /No staged version available for this hotel/);
  assert.doesNotMatch(appJs, /No staged inventory for this property/);
  assert.match(appJs, /customerFacingMissingInventoryMessage/);
  assert.match(
    appJs,
    /No customer-ready opportunities are available for this property yet/,
  );
  assert.match(appJs, /data-gdi-recovery="view-research"/);
});

check("cambridge_has_research_inventory_but_no_publication", () => {
  const hotelId = "recIwaP1etgx2g9nA";
  const avail = inspectFounderPreviewPublicationAvailability({ hotelId });
  assert.equal(avail.hasStagedPublication, false);
  assert.equal(avail.hasCurrentPublication, false);
  const oppPath = path.join(
    ROOT,
    "data/group-demand-intelligence/hotels",
    hotelId,
    "opportunities.json",
  );
  assert.ok(fs.existsSync(oppPath), "Cambridge research opportunities exist");
  const doc = JSON.parse(fs.readFileSync(oppPath, "utf8"));
  assert.ok((doc.opportunities || []).length >= 1, "non-empty research inventory");
});

check("named_contact_survives_without_email", () => {
  const opp = attachUniversalTileContactPreview({
    id: "t_named_no_email",
    primaryContact: {
      name: "Teddy Oberts",
      title: "Senior Procurement Manager – Corporate Travel, Meetings & Events",
      email: null,
      phone: null,
      customerFacingNamed: true,
    },
  });
  const preview = selectUniversalTileContactPreview(opp);
  assert.equal(preview.kind, "named");
  assert.equal(preview.name, "Teddy Oberts");
  const contract = buildCanonicalCustomerPresentationContract(opp);
  assert.equal(contract.contact.kind, "named");
  assert.equal(contract.contact.name, "Teddy Oberts");
  const auth = classifyCustomerContactDisplayAuth({
    name: "Teddy Oberts",
    customerFacingNamed: true,
    email: null,
  });
  assert.equal(auth.auth, CUSTOMER_CONTACT_DISPLAY_AUTH.VERIFIED_NAME_NO_CHANNEL);
  assert.equal(auth.displayName, true);
});

check("gap_label_does_not_overwrite_primary_contact_name", () => {
  const polluted = {
    id: "t_polluted",
    primaryContact: {
      name: "Daniel Roy",
      title: "Senior Strategic Sourcing Partner",
    },
    tileContactPreview: {
      kind: "none",
      name: null,
      displayName: TILE_NO_NAMED_CONTACT_LABEL,
      displayRole: null,
    },
    tileContactName: TILE_NO_NAMED_CONTACT_LABEL,
  };
  const resolved = resolveCanonicalContactNameAndKind(polluted);
  assert.equal(resolved.kind, "named");
  assert.equal(resolved.name, "Daniel Roy");
  const contract = buildCanonicalCustomerPresentationContract(polluted);
  assert.equal(contract.contact.kind, "named");
  assert.equal(contract.contact.name, "Daniel Roy");
  assert.notEqual(contract.contact.name, TILE_NO_NAMED_CONTACT_LABEL);
});

check("authorized_slot_named_without_email_displays", () => {
  const preview = selectUniversalTileContactPreview({
    id: "t_slot_named",
    scalingTwoContactPresentation: {
      primary: {
        name: "Alex Rivera",
        title: "Event Manager",
        customerFacingNamed: true,
        email: null,
        phone: null,
      },
    },
  });
  assert.equal(preview.kind, "named");
  assert.equal(preview.name, "Alex Rivera");
});

check("explicit_restricted_name_is_suppressed", () => {
  const restricted = {
    id: "t_restricted",
    primaryContact: {
      name: "Hidden Restricted Person",
      title: "VP Procurement",
      customerFacingNamed: false,
      suppressCustomerName: true,
      customerVisibility: "RESTRICTED",
    },
    scalingTwoContactPresentation: {
      primary: {
        name: "Hidden Restricted Person",
        title: "VP Procurement",
        customerFacingNamed: false,
        suppressCustomerName: true,
        customerVisibility: "RESTRICTED",
      },
    },
  };
  const auth = classifyCustomerContactDisplayAuth(restricted.primaryContact);
  assert.equal(auth.auth, CUSTOMER_CONTACT_DISPLAY_AUTH.RESTRICTED);
  assert.equal(auth.displayName, false);
  assert.equal(auth.preserveInternally, true);
  const preview = selectUniversalTileContactPreview(restricted);
  assert.equal(preview.kind, "none");
  assert.equal(preview.displayName, TILE_NO_NAMED_CONTACT_LABEL);
  const resolved = resolveCanonicalContactNameAndKind(restricted);
  assert.equal(resolved.kind, "none");
  assert.equal(resolved.name, null);
  const contract = buildCanonicalCustomerPresentationContract(restricted);
  assert.equal(contract.contact.name, null);
});

check("incomplete_verification_not_customer_displayed", () => {
  const auth = classifyCustomerContactDisplayAuth({
    name: "Internal Only Person",
    title: "Coordinator",
    customerFacingNamed: false,
  });
  assert.equal(auth.auth, CUSTOMER_CONTACT_DISPLAY_AUTH.INTERNAL_ONLY);
  assert.equal(auth.displayName, false);
  assert.equal(auth.preserveInternally, true);
  const preview = selectUniversalTileContactPreview({
    id: "t_internal",
    scalingTwoContactPresentation: {
      primary: {
        name: "Internal Only Person",
        title: "Coordinator",
        customerFacingNamed: false,
      },
    },
  });
  assert.equal(preview.kind, "none");
});

check("missing_contact_stays_research_required", () => {
  const opp = attachUniversalTileContactPreview({
    id: "t_none",
    title: "Some Event",
  });
  const preview = selectUniversalTileContactPreview(opp);
  assert.equal(preview.kind, "none");
  assert.equal(preview.displayName, TILE_NO_NAMED_CONTACT_LABEL);
  const contract = buildCanonicalCustomerPresentationContract(opp);
  assert.equal(contract.contact.kind, "none");
  assert.equal(contract.contact.name, null);
});

check("ui_rejects_gap_label_as_named_person", () => {
  assert.match(uiJs, /isContactGapLabel/);
  assert.match(uiJs, /scrubMalformedParentheticals/);
  assert.match(uiJs, /seenIds/);
});

check("malformed_parenthetical_scrubber_present", () => {
  assert.match(uiJs, /function scrubMalformedParentheticals/);
  assert.match(uiJs, /empty \/ malformed parentheticals/);
});

check("sales_ready_and_market_scope_unchanged_markers", () => {
  assert.match(uiJs, /MARKET_SCOPE/);
  assert.match(uiJs, /matchesMarketScopeFilter/);
  assert.match(uiJs, /INVENTORY_POPULATION_VALUES/);
});

check("cache_bust_updated", () => {
  assert.match(html, new RegExp(`app\\.js\\?v=${CACHE_BUST_APP}`));
  assert.match(html, new RegExp(`dealality-gdi-ui\\.js\\?v=${CACHE_BUST_UI}`));
  const shareHtml = fs.readFileSync(
    path.join(ROOT, "public/group-demand-intelligence-share.html"),
    "utf8",
  );
  assert.match(
    shareHtml,
    new RegExp(`dealality-gdi-ui\\.js\\?v=${CACHE_BUST_UI}`),
  );
});

check("card_dedupe_is_by_opportunity_id_not_title", () => {
  // Safe card dedupe by stable opportunity id — not by event/org title collapse.
  assert.match(uiJs, /var seenIds = \{\}/);
  assert.match(uiJs, /if \(seenIds\[id\]\) return false/);
  assert.match(uiJs, /Do not collapse on event\/organization name alone/);
});

if (failed) {
  console.error(`\n${failed} failure(s)`);
  process.exit(1);
}
console.log("\nOK test:gdi-customer-ui-regression-inventory-contacts-v1");
