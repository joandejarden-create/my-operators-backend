import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DEFAULT_CACHE = path.resolve(
  __dirname,
  "../../../../fixtures/gdi/nextgen-discovery/first-wave-cache-v1.json"
);

export function loadFirstWaveCache(rootDir = process.cwd()) {
  const candidates = [
    path.join(rootDir, "fixtures/gdi/nextgen-discovery/first-wave-cache-v1.json"),
    DEFAULT_CACHE,
  ];
  for (const p of candidates) {
    if (fs.existsSync(p)) {
      return JSON.parse(fs.readFileSync(p, "utf8"));
    }
  }
  return { schema: "missing", byHotel: {}, rejectionFixtures: [] };
}

export function loadHunterCacheDiscoveries({
  hunterId,
  hotelId,
  rootDir = process.cwd(),
  includeRejectionFixtures = false,
} = {}) {
  const cache = loadFirstWaveCache(rootDir);
  const hotel = cache.byHotel?.[hotelId] || null;
  const discoveries = hotel?.hunters?.[hunterId]
    ? [...hotel.hunters[hunterId]]
    : [];
  if (includeRejectionFixtures) {
    for (const r of cache.rejectionFixtures || []) {
      discoveries.push({ ...r, exclude: false });
    }
  }
  return {
    discoveries,
    territory: hotel?.territory || "",
    hotelName: hotel?.hotelName || "",
    cacheSchema: cache.schema,
    paid: false,
    source: "fixtures/gdi/nextgen-discovery/first-wave-cache-v1.json",
  };
}
