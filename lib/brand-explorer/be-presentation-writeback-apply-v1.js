/**
 * Controlled Brand Explorer Presentation writeback (Curio / Ascend).
 *
 * Writes ONLY "Brand Setup - Brand Explorer Presentation" for one Brand Basics id.
 * Proposed payload = cumulative assembler (CURRENT + staged Phase 2H candidate).
 *
 * Contract: BE_PRESENTATION_WRITEBACK_APPLY_V1
 */
import { createHash } from "crypto";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "fs";
import { join } from "path";
import Airtable from "airtable";
import {
  BE_PRESENTATION_TABLE,
  BE_WRITE_CLASSES,
} from "./be-airtable-integration-sot-v1.js";
import { assembleCumulativeProposal } from "./be-cumulative-proposal-assembler-v1.js";
import { hashPresentationBlocks } from "./be-founder-approval-publication-v1.js";

export const BE_PRESENTATION_WRITEBACK_APPLY_V1 = "BE_PRESENTATION_WRITEBACK_APPLY_V1";

const LINK_FIELD_CANDIDATES = [
  "Brand",
  "Brand_Basic_ID",
  "Brand Setup - Brand Basics",
  "Brand Basics",
];

const FOUNDATION_TOKEN =
  "FOUNDER_AUTHORIZE_BE_CURIO_ASCEND_PRESENTATION_WRITEBACK_20261120";

export function requireFounderWriteToken(argvToken) {
  const envTok = String(process.env.BE_FOUNDER_PRESENTATION_WRITEBACK_TOKEN || "").trim();
  const argTok = String(argvToken || "").trim();
  return argTok === FOUNDATION_TOKEN || envTok === FOUNDATION_TOKEN;
}

export function expectedFounderToken() {
  return FOUNDATION_TOKEN;
}

function airtableBase() {
  const key = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!key || !baseId) {
    throw new Error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required");
  }
  return new Airtable({ apiKey: key }).base(baseId);
}

function slotKeyOf(rec) {
  return String(rec.get?.("Slot Key") || rec.fields?.["Slot Key"] || rec.slotKey || "").trim();
}

function blockFromRecord(rec) {
  const f = rec.fields || {};
  const img = Array.isArray(f.Image) && f.Image[0] ? f.Image[0].url || null : null;
  return {
    recordId: rec.id,
    slotKey: slotKeyOf(rec),
    title: String(f.Title || "").trim(),
    body: String(f.Body || "").trim(),
    sort: typeof f["Sort Order"] === "number" ? f["Sort Order"] : Number(f["Sort Order"]) || 0,
    imageUrl: img,
    active: f.Active === false ? false : true,
  };
}

export async function fetchPresentationBlocksForBrand(brandId, brandName) {
  const base = airtableBase();
  const escapedId = String(brandId).replace(/"/g, '\\"');
  const escapedName = String(brandName || "").replace(/"/g, '\\"');
  const merged = [];
  const seen = new Set();
  const pushAll = (records) => {
    for (const r of records) {
      if (!seen.has(r.id)) {
        seen.add(r.id);
        merged.push(r);
      }
    }
  };

  if (escapedName) {
    try {
      pushAll(
        await base(BE_PRESENTATION_TABLE)
          .select({ filterByFormula: `{Brand Name} = "${escapedName}"`, maxRecords: 500 })
          .all()
      );
    } catch {
      /* optional */
    }
    try {
      pushAll(
        await base(BE_PRESENTATION_TABLE)
          .select({ filterByFormula: `{Brand} = "${escapedName}"`, maxRecords: 500 })
          .all()
      );
    } catch {
      /* schema */
    }
  }
  if (!merged.length) {
    for (const linkField of LINK_FIELD_CANDIDATES) {
      try {
        const formula = `FIND("${escapedId}", ARRAYJOIN({${linkField}})) > 0`;
        const records = await base(BE_PRESENTATION_TABLE).select({ filterByFormula: formula }).all();
        if (records.length) {
          pushAll(records);
          break;
        }
      } catch {
        /* wrong field */
      }
    }
  }

  const blocks = merged.map(blockFromRecord).filter((b) => b.slotKey);
  blocks.sort((a, b) => a.slotKey.localeCompare(b.slotKey) || a.sort - b.sort);
  return { records: merged, blocks, contentHash: hashPresentationBlocks(blocks) };
}

function groupBySlot(blocks) {
  const map = new Map();
  for (const b of blocks) {
    const k = String(b.slotKey || "").trim();
    if (!k) continue;
    if (!map.has(k)) map.set(k, []);
    map.get(k).push(b);
  }
  return map;
}

function fieldsForUpdate(block) {
  const fields = {
    Title: String(block.title || ""),
    Body: String(block.body || ""),
    Active: block.active === false ? false : true,
  };
  if (block.sort != null && Number.isFinite(Number(block.sort))) {
    fields["Sort Order"] = Number(block.sort);
  }
  const imageUrl = String(block.imageUrl || "").trim();
  if (imageUrl && /^https?:\/\//i.test(imageUrl)) {
    fields.Image = [{ url: imageUrl }];
  }
  return fields;
}

function fieldsForCreate(brandId, brandName, block, linkField) {
  const fields = {
    [linkField]: [brandId],
    "Slot Key": String(block.slotKey || "").trim(),
    ...fieldsForUpdate(block),
  };
  if (brandName) fields["Brand Name"] = brandName;
  return fields;
}

/**
 * Build ordered ops preserving multi-row slot cardinality.
 */
export function buildCardinalityAwareWritePlan({ brandId, brandName, currentBlocks, proposedBlocks }) {
  const currentBySlot = groupBySlot(currentBlocks);
  const proposedBySlot = groupBySlot(proposedBlocks);
  const allSlots = new Set([...currentBySlot.keys(), ...proposedBySlot.keys()]);
  const operations = [];

  for (const slotKey of [...allSlots].sort()) {
    const curList = currentBySlot.get(slotKey) || [];
    const propList = proposedBySlot.get(slotKey) || [];
    const n = Math.max(curList.length, propList.length);
    for (let i = 0; i < n; i++) {
      const cur = curList[i] || null;
      const prop = propList[i] || null;
      const before = cur
        ? { title: cur.title || "", body: cur.body || "", sort: cur.sort ?? null, imageUrl: cur.imageUrl || null }
        : null;
      const after = prop
        ? { title: prop.title || "", body: prop.body || "", sort: prop.sort ?? null, imageUrl: prop.imageUrl || null }
        : null;
      const same = JSON.stringify(before) === JSON.stringify(after);
      if (same && cur && prop) continue;

      if (prop && cur?.recordId) {
        operations.push({
          op: "UPDATE",
          table: BE_PRESENTATION_TABLE,
          recordId: cur.recordId,
          brandBasicsId: brandId,
          slotKey,
          index: i,
          before,
          after,
          fields: fieldsForUpdate(prop),
          writeClass: BE_WRITE_CLASSES.FOUNDER_PUBLISH,
        });
      } else if (prop && !cur) {
        operations.push({
          op: "CREATE",
          table: BE_PRESENTATION_TABLE,
          recordId: null,
          brandBasicsId: brandId,
          brandName: brandName || null,
          slotKey,
          index: i,
          before: null,
          after,
          fields: null, // filled at apply with resolved link field
          block: prop,
          writeClass: BE_WRITE_CLASSES.FOUNDER_PUBLISH,
        });
      } else if (!prop && cur?.recordId) {
        operations.push({
          op: "DEACTIVATE",
          table: BE_PRESENTATION_TABLE,
          recordId: cur.recordId,
          brandBasicsId: brandId,
          slotKey,
          index: i,
          before,
          after: null,
          fields: { Active: false },
          writeClass: BE_WRITE_CLASSES.FOUNDER_PUBLISH,
        });
      }
    }
  }

  return {
    contract: BE_PRESENTATION_WRITEBACK_APPLY_V1,
    brandBasicsId: brandId,
    targetTable: BE_PRESENTATION_TABLE,
    operations,
    counts: {
      update: operations.filter((o) => o.op === "UPDATE").length,
      create: operations.filter((o) => o.op === "CREATE").length,
      deactivate: operations.filter((o) => o.op === "DEACTIVATE").length,
      unchangedSkipped: 0,
    },
    idempotencyKey: createHash("sha256")
      .update(JSON.stringify(operations.map((o) => [o.op, o.slotKey, o.recordId, o.index])))
      .digest("hex"),
  };
}

export function loadPhase2hCandidate(root, relativePath) {
  const abs = join(root, "public", relativePath.replace(/^\/+/, ""));
  if (!existsSync(abs)) throw new Error(`Candidate missing: ${abs}`);
  return JSON.parse(readFileSync(abs, "utf8"));
}

/** Title/body/sort only — Airtable Image CDN URLs often differ after attach. */
export function hashPresentationText(blocks) {
  const normalized = (Array.isArray(blocks) ? blocks : [])
    .map((b) => ({
      slotKey: String(b.slotKey || "").trim(),
      title: String(b.title || "").trim(),
      body: String(b.body || "").trim(),
      sort: typeof b.sort === "number" ? b.sort : Number(b.sort) || 0,
      active: b.active === false ? false : true,
    }))
    .filter((b) => b.slotKey && b.active !== false)
    .sort((a, b) => a.slotKey.localeCompare(b.slotKey) || a.sort - b.sort);
  return createHash("sha256").update(JSON.stringify(normalized)).digest("hex");
}

export function assembleProposedBlocks(currentBlocks, candidate) {
  const candidateBlocks =
    (candidate.brandExplorer && candidate.brandExplorer.blocks) || candidate.blocks || [];

  // Frozen approval payload is already CURRENT+overlay assembled — write exactly these blocks.
  const contract = String(candidate?.contract || "");
  if (/FROZEN_APPROVAL_PAYLOAD/i.test(contract)) {
    const blocks = candidateBlocks.slice();
    const contentHash = hashPresentationBlocks(blocks);
    const expected = String(candidate.approvalContentHash || "").trim();
    if (expected && expected !== contentHash) {
      throw new Error(
        `Frozen approval payload hash mismatch: file=${expected} computed=${contentHash}`
      );
    }
    return {
      blocks,
      assembly: {
        summary: {
          frozenApprovalPayload: true,
          assembledBlockCount: blocks.length,
          approvalContentHash: expected || contentHash,
        },
      },
      contentHash: expected || contentHash,
      textHash: hashPresentationText(blocks),
      candidateBlockCount: blocks.length,
      frozenApprovalPayload: true,
    };
  }

  const assembled = assembleCumulativeProposal(currentBlocks, candidateBlocks, {
    note: "Founder-authorized Presentation writeback",
  });
  return {
    blocks: assembled.blocks,
    assembly: assembled,
    contentHash: hashPresentationBlocks(assembled.blocks),
    textHash: hashPresentationText(assembled.blocks),
    candidateBlockCount: candidateBlocks.length,
  };
}

async function resolveLinkField(base, brandId, brandName) {
  // Prefer Brand when create probe isn't possible; try known candidates by reading schema via a dry create attempt on first apply.
  for (const linkField of LINK_FIELD_CANDIDATES) {
    try {
      // Probe: filter using link field — if formula parses, field exists.
      await base(BE_PRESENTATION_TABLE)
        .select({
          filterByFormula: `FIND("${String(brandId).replace(/"/g, '\\"')}", ARRAYJOIN({${linkField}})) > 0`,
          maxRecords: 1,
        })
        .firstPage();
      return linkField;
    } catch {
      /* try next */
    }
  }
  return "Brand";
}

export async function applyPresentationWritePlan({ writePlan, dryRun = true, brandName = "" }) {
  const base = airtableBase();
  const linkField = await resolveLinkField(base, writePlan.brandBasicsId, brandName);
  let airtableWrites = 0;
  const results = [];

  const updates = writePlan.operations.filter((o) => o.op === "UPDATE" || o.op === "DEACTIVATE");
  const creates = writePlan.operations.filter((o) => o.op === "CREATE");

  for (let i = 0; i < updates.length; i += 10) {
    const chunk = updates.slice(i, i + 10);
    if (dryRun) {
      for (const o of chunk) {
        results.push({ op: o.op, slotKey: o.slotKey, recordId: o.recordId, dryRun: true });
      }
      continue;
    }
    const payload = chunk.map((o) => ({ id: o.recordId, fields: o.fields }));
    await base(BE_PRESENTATION_TABLE).update(payload);
    airtableWrites += payload.length;
    for (const o of chunk) {
      results.push({ op: o.op, slotKey: o.slotKey, recordId: o.recordId, applied: true });
    }
  }

  for (let i = 0; i < creates.length; i += 10) {
    const chunk = creates.slice(i, i + 10);
    if (dryRun) {
      for (const o of chunk) {
        results.push({
          op: "CREATE",
          slotKey: o.slotKey,
          dryRun: true,
          fieldsPreview: fieldsForCreate(writePlan.brandBasicsId, brandName, o.block, linkField),
        });
      }
      continue;
    }
    const payload = chunk.map((o) => ({
      fields: fieldsForCreate(writePlan.brandBasicsId, brandName, o.block, linkField),
    }));
    const created = await base(BE_PRESENTATION_TABLE).create(payload);
    airtableWrites += created.length;
    for (let j = 0; j < created.length; j++) {
      results.push({
        op: "CREATE",
        slotKey: chunk[j].slotKey,
        recordId: created[j].id,
        applied: true,
      });
    }
  }

  return {
    dryRun: Boolean(dryRun),
    linkField,
    airtableWrites,
    results,
    counts: writePlan.counts,
  };
}

export function persistWritebackReport(root, report) {
  const dir = join(root, "reports", "brand-explorer", "presentation-writeback-v1");
  mkdirSync(dir, { recursive: true });
  const stamp = String(report.generatedAt || new Date().toISOString()).replace(/[:.]/g, "-");
  const brand = String(report.brandSlug || report.brandId || "brand").replace(/[^a-zA-Z0-9_-]/g, "_");
  const path = join(dir, `${brand}-${report.mode || "run"}-${stamp}.json`);
  writeFileSync(path, JSON.stringify(report, null, 2));
  return path;
}
