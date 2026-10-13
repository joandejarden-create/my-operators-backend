/**
 * Local Research Target bag companion (filesystem) when Airtable upsert is skipped/fails.
 */
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "fs";
import { join } from "path";

export function loadLocalResearchTargets(hotelId) {
  const p = join(
    process.cwd(),
    "data/group-demand-intelligence/hotels",
    hotelId,
    "research-targets.json"
  );
  if (!existsSync(p)) {
    return { hotelId, targets: [], updatedAt: null };
  }
  return JSON.parse(readFileSync(p, "utf8"));
}

export function upsertLocalResearchTarget(hotelId, target) {
  const doc = loadLocalResearchTargets(hotelId);
  const id = target.targetId;
  const idx = (doc.targets || []).findIndex((t) => t.targetId === id);
  if (idx >= 0) doc.targets[idx] = { ...doc.targets[idx], ...target };
  else doc.targets.push(target);
  doc.updatedAt = new Date().toISOString();
  const dir = join(process.cwd(), "data/group-demand-intelligence/hotels", hotelId);
  mkdirSync(dir, { recursive: true });
  writeFileSync(
    join(dir, "research-targets.json"),
    JSON.stringify(doc, null, 2)
  );
  return { created: idx < 0, target };
}
