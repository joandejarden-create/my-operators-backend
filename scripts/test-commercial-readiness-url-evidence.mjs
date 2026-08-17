import { extractCommercialReadinessUrlEvidence } from "../lib/commercial-readiness-url-evidence.js";
import { buildCommercialReadinessSnapshot } from "../lib/commercial-readiness-snapshot-build.js";

function assert(condition, message) {
  if (!condition) {
    console.error("FAIL:", message);
    process.exitCode = 1;
    return;
  }
  console.log("PASS:", message);
}

function mockResponse({ ok = true, status = 200, contentType = "text/html", body = "" }) {
  return {
    ok,
    status,
    headers: { get: (k) => (k.toLowerCase() === "content-type" ? contentType : "") },
    text: async () => body,
  };
}

function buildInputs(overrides = {}) {
  return {
    hotelWebsiteUrl: "https://example-hotel.com",
    bookingComUrl: "https://booking.example.com/property",
    expediaUrl: "https://expedia.example.com/property",
    googleBusinessProfileUrl: "https://google.com/maps/place/x",
    currentBrandStatus: "Branded",
    currentOperatorStatus: "Brand-managed",
    estimatedOtaShare: "Moderate",
    estimatedDirectBookingShare: "Moderate",
    bookingEngineProvider: "IHG booking engine",
    crmGuestEmailCapture: "Yes",
    mainCommercialConcern: "Poor conversion",
    ownerGoal: "Improve direct bookings",
    estimatedOtaCommission: "18%",
    ...overrides,
  };
}

const hotelHtml = `
<html><head><title>Inter Example Hotel | Official Site</title><meta name="description" content="Book direct for best rate guarantee and exclusive offers."></head>
<body>
<h1>Book Direct and Save</h1>
<h2>Rooms and Suites</h2>
<p>Best rate guarantee and flexible cancellation policy.</p>
<p>Join loyalty for member rate and exclusive benefits.</p>
<p>Business and leisure guests enjoy beachfront location near the old city.</p>
<p>Subscribe to our newsletter and WhatsApp concierge.</p>
</body></html>`;

const otaHtml = `
<html><head><title>Inter Example Hotel - Booking.com</title></head>
<body>
<h1>Inter Example Hotel</h1>
<p>Guests rated location highly. Review score 8.9 based on 2400 reviews.</p>
<p>Free cancellation available on selected rooms. Reserve now, pay later.</p>
<p>Compare room types and guest ratings.</p>
</body></html>`;

async function run() {
  console.log("\n=== Commercial Readiness URL Evidence Tests ===\n");

  // 1. Invalid URL rejected.
  {
    const inputs = buildInputs({ hotelWebsiteUrl: "not-a-url", bookingComUrl: "", expediaUrl: "", googleBusinessProfileUrl: "" });
    const evidence = await extractCommercialReadinessUrlEvidence(inputs, {
      skipDns: true,
      fetchImpl: async () => mockResponse({}),
    });
    assert(evidence.sources.hotelWebsite.status === "failed", "Invalid URL is rejected");
    assert(evidence.sources.hotelWebsite.reason === "invalid_url", "Invalid URL reason captured");
  }

  // 2. Localhost/private blocked.
  {
    const inputs = buildInputs({ hotelWebsiteUrl: "http://127.0.0.1:8080/private", bookingComUrl: "", expediaUrl: "", googleBusinessProfileUrl: "" });
    const evidence = await extractCommercialReadinessUrlEvidence(inputs, {
      skipDns: true,
      fetchImpl: async () => mockResponse({}),
    });
    assert(evidence.sources.hotelWebsite.status === "failed", "Private URL blocked");
    assert(evidence.sources.hotelWebsite.reason === "blocked_private_ip", "Private URL block reason captured");
  }

  // 3/5/6/7. Successful mock extraction populates evidence and comparison.
  {
    const inputs = buildInputs();
    const fetchImpl = async (url) => {
      if (String(url).includes("example-hotel")) return mockResponse({ body: hotelHtml });
      return mockResponse({ body: otaHtml });
    };
    const evidence = await extractCommercialReadinessUrlEvidence(inputs, { skipDns: true, fetchImpl });
    assert(evidence.sources.hotelWebsite.status === "extracted", "Hotel evidence extracted");
    assert(evidence.sources.bookingCom.status === "extracted", "Booking.com evidence extracted");
    assert(evidence.sources.expedia.status === "extracted", "Expedia evidence extracted");
    assert(
      evidence.ownedVsOtaComparison.assessment !== "Insufficient extracted evidence",
      "Owned-vs-OTA comparison becomes evidence-based when extraction succeeds"
    );

    const result = buildCommercialReadinessSnapshot(inputs, { urlEvidence: evidence });
    assert(
      result.snapshot.ownedChannelVsOtaContentGap.assessment !== "Insufficient extracted evidence",
      "Snapshot uses evidence-based owned-vs-OTA assessment"
    );
  }

  // 4/8. Partial extraction falls back to directional low confidence language.
  {
    const inputs = buildInputs();
    const fetchImpl = async (url) => {
      if (String(url).includes("example-hotel")) return mockResponse({ body: hotelHtml });
      return mockResponse({ ok: false, status: 403 });
    };
    const evidence = await extractCommercialReadinessUrlEvidence(inputs, { skipDns: true, fetchImpl });
    assert(evidence.sources.hotelWebsite.status === "extracted", "Hotel extracted in partial case");
    assert(evidence.sources.bookingCom.status === "blocked" || evidence.sources.expedia.status === "blocked", "OTA blocked status captured");
    assert(
      evidence.ownedVsOtaComparison.confidence === "Low",
      "Partial extraction produces low-confidence directional comparison"
    );
  }

  // 9. InterContinental-like case still works with OTA blocks.
  {
    const inputs = buildInputs({
      hotelWebsiteUrl: "https://www.intercartagena.com/en",
      bookingComUrl: "https://www.booking.com/hotel/co/intercontinental-cartagena.en-gb.html",
      expediaUrl: "https://www.expedia.com/es/Cartagena-Hoteles-Intercontinental-Cartagena-De-Indias.h7985087.Informacion-Hotel",
      googleBusinessProfileUrl: "https://www.google.com/maps/search/InterContinental+Cartagena+de+Indias",
    });
    const fetchImpl = async (url) => {
      if (String(url).includes("intercartagena")) return mockResponse({ body: hotelHtml });
      return mockResponse({ ok: false, status: 403 });
    };
    const evidence = await extractCommercialReadinessUrlEvidence(inputs, { skipDns: true, fetchImpl });
    const result = buildCommercialReadinessSnapshot(inputs, { urlEvidence: evidence });
    assert(result.labels.readinessLevel === "Developing", "InterContinental deterministic label remains Developing");
    assert(result.labels.otaRisk === "Potential moderate risk indicated", "InterContinental OTA label unchanged");
    assert(
      result.labels.directCapability === "Developing — infrastructure present, conversion unconfirmed",
      "InterContinental direct capability label unchanged"
    );
  }

  console.log("\n=== Done ===\n");
  if (process.exitCode) process.exit(process.exitCode);
}

run();

