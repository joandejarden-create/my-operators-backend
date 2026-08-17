import "dotenv/config";
import { listPresentationRowsLight } from "../lib/partner-intelligence/brand-explorer-lane2-common.js";

const { rows } = await listPresentationRowsLight("recTJdPlr4mDs9app", "SO/ Hotels & Resorts");
const pos = rows.filter((r) => /positioning/i.test(r.slotKey || "") || /positioning|audience|psychographic/i.test(r.title || ""));
console.log(
  pos.map((r) => ({
    id: r.recordId,
    slot: r.slotKey,
    title: r.title,
    body: String(r.body || "").slice(0, 240),
    words: String(r.body || "")
      .trim()
      .split(/\s+/)
      .filter(Boolean).length,
  }))
);

const fairmont = await listPresentationRowsLight("recJhPaDVU3YUDQUt", "Fairmont");
const sf = fairmont.rows.filter((r) => /san francisco/i.test(r.title || ""));
console.log(
  "fairmont SF",
  sf.map((r) => ({
    id: r.recordId,
    slot: r.slotKey,
    title: r.title,
    eds: r.externalDisplayStatus,
    active: r.active,
    hasImage: !!r.imageUrl,
  }))
);
