/**
 * Freeze published PDFs, sync V3 draft pointers, verify Admin channel parity.
 *
 * Does NOT publish V3, overwrite published bytes, rotate shares, or deploy.
 *
 *   node scripts/ava-v3-pdf-version-resolution-fix-v1.mjs
 */
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  ensurePublishedPdfFrozen,
  syncDraftPdfPointersFromDurable,
  describeAvaPdfChannels,
  resolveAvaPdfByChannel,
  pdfTextLooksLikeCustomerReportV3,
  AVA_PDF_CHANNEL,
} from "../lib/ai-visibility-audit-action-plan/ava-pdf-channels-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const REPORT = path.join(
  ROOT,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1"
);

const HOTELS = [
  {
    propertyId: "adp_waterstone_boca_raton",
    slug: "waterstone-boca-raton",
    expectDraftPages: 20,
    founderOldSha:
      "8173ca19ace9add954c442bcecb48cade0fb7394de77ea9b5c468b34391e38c8",
  },
  {
    propertyId: "adp_ac_hotel_a_coruna",
    slug: "ac-hotel-a-coruna",
    expectDraftPages: 19,
  },
  {
    propertyId: "adp_castillo_hotel_son_vida",
    slug: "castillo-hotel-son-vida",
    expectDraftPages: 20,
  },
];

function sha256(buf) {
  return crypto.createHash("sha256").update(buf).digest("hex");
}

async function pdfMeta(abs) {
  const buf = fs.readFileSync(abs);
  const { getDocument } = await import("pdfjs-dist/legacy/build/pdf.mjs");
  const doc = await getDocument({
    data: new Uint8Array(buf),
    useSystemFonts: true,
  }).promise;
  let text = "";
  for (let i = 1; i <= doc.numPages; i++) {
    const page = await doc.getPage(i);
    const content = await page.getTextContent();
    text += content.items.map((it) => it.str).join(" ") + "\n";
  }
  return { pages: doc.numPages, sha256: sha256(buf), text, bytes: buf.length };
}

async function main() {
  // Prefer durable for sync writes into active store; also verify mode=off reads.
  process.env.AVA_STORAGE_MODE = process.env.AVA_STORAGE_MODE || "local";

  const rows = [];
  for (const hotel of HOTELS) {
    const frozen = ensurePublishedPdfFrozen(hotel.propertyId);
    const synced = syncDraftPdfPointersFromDurable(hotel.propertyId);

    const channels = describeAvaPdfChannels(hotel.propertyId);
    const published = resolveAvaPdfByChannel({
      propertyId: hotel.propertyId,
      channel: AVA_PDF_CHANNEL.PUBLISHED,
    });
    const draft = resolveAvaPdfByChannel({
      propertyId: hotel.propertyId,
      channel: AVA_PDF_CHANNEL.DRAFT,
    });

    // Preview/Download parity = same resolver, same bytes
    const draftAgain = resolveAvaPdfByChannel({
      propertyId: hotel.propertyId,
      channel: AVA_PDF_CHANNEL.DRAFT,
    });

    let publishedMeta = null;
    let draftMeta = null;
    let v3 = null;
    if (published.ok) publishedMeta = await pdfMeta(published.abs);
    if (draft.ok) {
      draftMeta = await pdfMeta(draft.abs);
      v3 = pdfTextLooksLikeCustomerReportV3(draftMeta.text);
    }

    const missingDraftFailsClosed = resolveAvaPdfByChannel({
      propertyId: hotel.propertyId,
      channel: AVA_PDF_CHANNEL.HISTORY,
      versionId: "ava_v1_DOES_NOT_EXIST_MISSING_ARTIFACT",
    });

    const pass =
      frozen.ok &&
      synced.ok &&
      published.ok &&
      draft.ok &&
      published.sha256 !== draft.sha256 &&
      draft.sha256 === draftAgain.sha256 &&
      draftMeta?.pages === hotel.expectDraftPages &&
      v3?.hasExecutive &&
      v3?.hasImplementationByOwner &&
      v3?.hasPlatformAssessment &&
      !missingDraftFailsClosed.ok &&
      missingDraftFailsClosed.error === "VERSION_NOT_FOUND" &&
      (!hotel.founderOldSha || published.sha256 === hotel.founderOldSha);

    rows.push({
      propertyId: hotel.propertyId,
      slug: hotel.slug,
      pass,
      frozen,
      synced,
      published: published.ok
        ? {
            sha256: published.sha256,
            pages: publishedMeta?.pages,
            mtime: published.mtime,
            versionId: published.versionId,
          }
        : { error: published.error },
      draft: draft.ok
        ? {
            sha256: draft.sha256,
            pages: draftMeta?.pages,
            mtime: draft.mtime,
            versionId: draft.versionId,
            previewShaEqualsDownloadSha: draft.sha256 === draftAgain.sha256,
            v3Sections: v3,
          }
        : { error: draft.error },
      channelsSummary: {
        publishedAvailable: channels.published.available,
        draftAvailable: channels.draft.available,
        differsFromPublished: channels.draft.differsFromPublished,
      },
      historyMissingFailsClosed: {
        ok: missingDraftFailsClosed.ok,
        error: missingDraftFailsClosed.error,
      },
      founderOldShaMatch:
        hotel.founderOldSha != null
          ? published.ok && published.sha256 === hotel.founderOldSha
          : null,
    });
  }

  // Mode=off must still resolve draft via durable overlay (no silent archive fallback)
  const prevMode = process.env.AVA_STORAGE_MODE;
  delete process.env.AVA_STORAGE_MODE;
  const offDraft = resolveAvaPdfByChannel({
    propertyId: "adp_waterstone_boca_raton",
    channel: AVA_PDF_CHANNEL.DRAFT,
  });
  const offPublished = resolveAvaPdfByChannel({
    propertyId: "adp_waterstone_boca_raton",
    channel: AVA_PDF_CHANNEL.PUBLISHED,
  });
  process.env.AVA_STORAGE_MODE = prevMode;

  const out = {
    contract: "AVA_V3_PDF_VERSION_RESOLUTION_RESULTS_V1",
    generatedAt: new Date().toISOString(),
    publicationAuthorized: false,
    deploymentAuthorized: false,
    adpMeasurementsChanged: false,
    hotels: rows,
    modeOffStillResolvesDraft:
      offDraft.ok === true &&
      offDraft.sha256 === rows[0]?.draft?.sha256,
    modeOffPublishedUnchanged:
      offPublished.ok === true &&
      offPublished.sha256 === rows[0]?.published?.sha256,
    passCount: rows.filter((r) => r.pass).length,
    failCount: rows.filter((r) => !r.pass).length,
  };

  fs.mkdirSync(REPORT, { recursive: true });
  fs.writeFileSync(
    path.join(REPORT, "AVA_V3_PDF_VERSION_RESOLUTION_RESULTS.json"),
    `${JSON.stringify(out, null, 2)}\n`
  );

  console.log(
    JSON.stringify(
      {
        passCount: out.passCount,
        failCount: out.failCount,
        modeOffStillResolvesDraft: out.modeOffStillResolvesDraft,
        hotels: rows.map((r) => ({
          slug: r.slug,
          pass: r.pass,
          publishedSha: r.published.sha256?.slice(0, 12),
          publishedPages: r.published.pages,
          draftSha: r.draft.sha256?.slice(0, 12),
          draftPages: r.draft.pages,
        })),
      },
      null,
      2
    )
  );

  if (out.failCount || !out.modeOffStillResolvesDraft) process.exit(1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
