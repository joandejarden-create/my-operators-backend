/**
 * ADP_PORTFOLIO_INVENTORY_V1 — discover every hotel with any ADP footprint.
 * Do not rely on frozen 31/37 lists.
 */

import {
  existsSync,
  readdirSync,
  readFileSync,
  statSync,
} from "fs";
import { join } from "path";
import { loadPropertyProfile } from "../data-model.js";
import {
  listStagedPropertyIds,
  loadStagedOwnerPayload,
} from "../review/adp-staged-report-store-v1.js";
import { listPublishedPropertyIds } from "../published-snapshot.js";
import { listLoyaltyBppStagedPropertyIds } from "../brand-portfolio/loyalty-bpp-staged-pack-store-v1.js";
import { listAllPublicationRecords } from "../review/adp-publication-registry-v1.js";
import { listOperationalAdpPropertyIds } from "../governance/adp-operational-registry-hygiene-v1.js";
import { listAllAdpFixtureProfiles } from "../review/list-adp-reviewer-properties-v1.js";
import { discoverAdpPropertyIds } from "../multi-property-governed-audit-v2.js";
import { readShareRegistry } from "../share/adp-signed-share-capability-v1.js";

export const ADP_PORTFOLIO_INVENTORY_V1 = "ADP_PORTFOLIO_INVENTORY_V1";

function safeList(fn, fallback = []) {
  try {
    const v = fn();
    return Array.isArray(v) ? v : fallback;
  } catch {
    return fallback;
  }
}

function shareRegistryPropertyIds() {
  try {
    const reg = readShareRegistry();
    const tokenMap =
      reg?.tokens && typeof reg.tokens === "object" && !Array.isArray(reg.tokens)
        ? reg.tokens
        : {};
    const ids = new Set();
    for (const t of Object.values(tokenMap)) {
      const pid = t?.propertyId || t?.hotelId || t?.adpPropertyId;
      if (pid) ids.add(String(pid));
    }
    return [...ids];
  } catch {
    return [];
  }
}

function listDirs(dir) {
  if (!existsSync(dir)) return [];
  return readdirSync(dir).filter((name) => {
    try {
      return statSync(join(dir, name)).isDirectory();
    } catch {
      return false;
    }
  });
}

function listFixturePropertyIds(cwd) {
  const dir = join(cwd, "fixtures/ai-demand-positioning");
  if (!existsSync(dir)) return [];
  const ids = [];
  for (const f of readdirSync(dir).filter((x) => x.endsWith("-property-profile.json"))) {
    try {
      const data = JSON.parse(readFileSync(join(dir, f), "utf8"));
      if (data?.propertyId) ids.push(data.propertyId);
    } catch {
      /* ignore */
    }
  }
  return ids;
}

function listRuntimePropertyIds(cwd) {
  const dir = join(cwd, "data/ai-demand-positioning/runtime");
  if (!existsSync(dir)) return [];
  const ids = new Set();
  for (const f of readdirSync(dir).filter(
    (x) => x.startsWith("adp_period_") && x.endsWith(".json")
  )) {
    const m = f.match(/^adp_period_(adp_[a-z0-9_]+?)_\d{14}_/);
    if (m) ids.add(m[1]);
    else {
      // draft_ peers etc.
      const m2 = f.match(/^adp_period_((?:adp_|draft_)[a-z0-9_]+?)_\d{14}_/);
      if (m2) ids.add(m2[1]);
    }
  }
  return [...ids];
}

function latestCorePeriodMeta(propertyId, cwd) {
  const dir = join(cwd, "data/ai-demand-positioning/runtime");
  if (!existsSync(dir)) return null;
  const prefix = `adp_period_${propertyId}_`;
  const files = readdirSync(dir)
    .filter((f) => f.startsWith(prefix) && f.endsWith(".json"))
    .sort();
  if (!files.length) return null;
  const latest = files.at(-1);
  try {
    const per = JSON.parse(readFileSync(join(dir, latest), "utf8"));
    return {
      periodId: per.periodId || latest.replace(/\.json$/, ""),
      observationCount: (per.observations || []).length,
      scenarioCount:
        per.scenarioUniverse?.scenarioCount ||
        per.scenarioCount ||
        new Set((per.observations || []).map((o) => o.scenarioId)).size,
      measurementFamily: per.measurementFamily || "CORE_ADP",
      methodologyId:
        per.methodologyId ||
        per.scenarioUniverse?.independentUnpromptedAudit?.standard ||
        null,
    };
  } catch {
    return { periodId: latest.replace(/\.json$/, ""), observationCount: null };
  }
}

function publishedMeta(propertyId, cwd) {
  const dir = join(cwd, "data/ai-demand-positioning/published", propertyId);
  if (!existsSync(dir)) return { published: false };
  const manPath = join(dir, "manifest.json");
  if (!existsSync(manPath)) {
    return { published: true, note: "published_dir_without_manifest" };
  }
  try {
    const man = JSON.parse(readFileSync(manPath, "utf8"));
    return {
      published: true,
      publishedVersionId: man.latestPublishedVersionId || man.versionId || null,
      periodId: man.periodId || null,
      customerPublished: man.customerPublished !== false,
    };
  } catch {
    return { published: true, note: "manifest_unreadable" };
  }
}

function bppMeta(propertyId, cwd) {
  const dir = join(cwd, "data/ai-demand-positioning/bpp-loyalty-staged", propertyId);
  if (!existsSync(dir)) return { bppStaged: false };
  const manPath = join(dir, "manifest.json");
  if (!existsSync(manPath)) return { bppStaged: true };
  try {
    const man = JSON.parse(readFileSync(manPath, "utf8"));
    return {
      bppStaged: true,
      bppVersionId: man.latestVersionId || man.versionId || null,
      bppPeriodId: man.periodId || null,
      bppCertification: man.certification || null,
    };
  } catch {
    return { bppStaged: true };
  }
}

function candidateMeta(propertyId, cwd) {
  const dir = join(cwd, "data/ai-demand-positioning/publication-candidates");
  if (!existsSync(dir)) return { candidates: 0 };
  const files = readdirSync(dir).filter((f) => f.includes(propertyId));
  return { candidates: files.length, candidateFiles: files.slice(0, 5) };
}

/**
 * Build complete ADP inventory from all local footprints.
 * Union follows explore guidance: operational ∪ fixtures ∪ staged ∪ published ∪
 * BPP-staged ∪ publication registry ∪ candidates ∪ share registry ∪ runtime discovery.
 */
export function buildAdpPortfolioInventory({ cwd = process.cwd() } = {}) {
  const fixtureIds = listFixturePropertyIds(cwd);
  const fixtureProfileIds = safeList(() =>
    listAllAdpFixtureProfiles(cwd).map((p) => p.propertyId).filter(Boolean)
  );
  const stagedIds = safeList(() => listStagedPropertyIds(cwd), listDirs(join(cwd, "data/ai-demand-positioning/staged")));
  const publishedIds = safeList(() => listPublishedPropertyIds({}), listDirs(join(cwd, "data/ai-demand-positioning/published")));
  const bppIds = safeList(
    () => listLoyaltyBppStagedPropertyIds(cwd),
    listDirs(join(cwd, "data/ai-demand-positioning/bpp-loyalty-staged"))
  );
  const operationalIds = safeList(() => listOperationalAdpPropertyIds());
  const publicationRecordIds = safeList(() =>
    listAllPublicationRecords(cwd)
      .map((r) => r?.propertyId)
      .filter(Boolean)
  );
  const shareIds = shareRegistryPropertyIds();
  const discoveredRuntimeIds = safeList(() => discoverAdpPropertyIds());
  const runtimeIds = listRuntimePropertyIds(cwd);
  const registryIds = listDirs(
    join(cwd, "data/ai-demand-positioning/competitor-entity-registries")
  );

  // Candidates: parse propertyId from cand_<propertyId>_staged_*.json
  const candidateDir = join(cwd, "data/ai-demand-positioning/publication-candidates");
  const candidatePropertyIds = [];
  if (existsSync(candidateDir)) {
    for (const f of readdirSync(candidateDir).filter((x) => x.startsWith("cand_") && x.endsWith(".json"))) {
      const m = f.match(/^cand_(adp_[a-z0-9_]+?)_staged_/);
      if (m) candidatePropertyIds.push(m[1]);
    }
  }

  const allIds = [
    ...new Set([
      ...fixtureIds,
      ...fixtureProfileIds,
      ...stagedIds,
      ...publishedIds,
      ...bppIds,
      ...operationalIds,
      ...publicationRecordIds,
      ...shareIds,
      ...candidatePropertyIds,
      ...discoveredRuntimeIds.filter((id) => String(id).startsWith("adp_")),
      ...runtimeIds.filter((id) => id.startsWith("adp_")),
      ...registryIds,
    ]),
  ].sort();

  const hotels = [];
  for (const propertyId of allIds) {
    const profile = loadPropertyProfile(propertyId);
    const staged = loadStagedOwnerPayload(propertyId);
    const period = latestCorePeriodMeta(propertyId, cwd);
    const published = publishedMeta(propertyId, cwd);
    const bpp = bppMeta(propertyId, cwd);
    const cand = candidateMeta(propertyId, cwd);
    const payload = staged?.payload || null;
    const bppSection = payload?.brandPortfolioPosition || null;

    const isDraftPeer = String(propertyId).startsWith("draft_");
    const isSubjectHotel = String(propertyId).startsWith("adp_");

    hotels.push({
      propertyId,
      name: profile?.name || propertyId,
      hpcHotelId: profile?.hpcHotelId || profile?.censusRecordId || null,
      city: profile?.city || null,
      market: profile?.market || null,
      greenfield: Boolean(profile?.greenfield),
      customerDropdownVisible: profile?.customerDropdownVisible !== false,
      inventoryRole: isSubjectHotel
        ? "ADP_SUBJECT"
        : isDraftPeer
          ? "DRAFT_PEER_FIXTURE"
          : "OTHER",
      sources: {
        fixture: fixtureIds.includes(propertyId) || fixtureProfileIds.includes(propertyId),
        staged: stagedIds.includes(propertyId),
        published: publishedIds.includes(propertyId),
        bppLoyaltyStaged: bppIds.includes(propertyId),
        runtimePeriod: runtimeIds.includes(propertyId) || discoveredRuntimeIds.includes(propertyId),
        competitorRegistry: registryIds.includes(propertyId),
        operational: operationalIds.includes(propertyId),
        publicationRegistry: publicationRecordIds.includes(propertyId),
        externalShare: shareIds.includes(propertyId),
        publicationCandidate: candidatePropertyIds.includes(propertyId),
      },
      monitoring: period,
      staged: staged
        ? {
            versionId: staged.versionId || staged.stagedVersionId || null,
            periodId: staged.periodId || payload?.period?.periodId || null,
            publicationStatus: staged.publicationStatus || payload?.publicationStatus || null,
            considerationRate: payload?.executiveMetrics?.considerationRate?.rate ?? null,
            scenarioPresenceRate: payload?.executiveMetrics?.scenarioPresence?.rate ?? null,
            bppStatus: bppSection?.status || bppSection?.customerState || null,
          }
        : null,
      published,
      bpp,
      publicationCandidates: cand,
      lastCertificationHint:
        staged?.certification?.status ||
        payload?._adpAtomicCertification?.status ||
        (published.published ? "PUBLISHED_PRESENT" : null),
    });
  }

  return {
    contract: ADP_PORTFOLIO_INVENTORY_V1,
    generatedAt: new Date().toISOString(),
    sourceUnion: [
      "listAllAdpFixtureProfiles",
      "listOperationalAdpPropertyIds",
      "listStagedPropertyIds",
      "listPublishedPropertyIds",
      "listLoyaltyBppStagedPropertyIds",
      "listAllPublicationRecords",
      "readShareRegistry",
      "publication-candidates/",
      "discoverAdpPropertyIds",
      "runtime adp_period_*",
      "competitor-entity-registries/",
    ],
    counts: {
      totalDiscovered: hotels.length,
      adpSubjectHotels: hotels.filter((h) => h.inventoryRole === "ADP_SUBJECT").length,
      draftPeerFixtures: hotels.filter((h) => h.inventoryRole === "DRAFT_PEER_FIXTURE")
        .length,
      withFixture: hotels.filter((h) => h.sources.fixture).length,
      withStaged: hotels.filter((h) => h.sources.staged).length,
      withPublished: hotels.filter((h) => h.sources.published).length,
      withBppStaged: hotels.filter((h) => h.sources.bppLoyaltyStaged).length,
      withRuntime: hotels.filter((h) => h.sources.runtimePeriod).length,
      withCompetitorRegistry: hotels.filter((h) => h.sources.competitorRegistry).length,
      withOperational: hotels.filter((h) => h.sources.operational).length,
      withPublicationRegistry: hotels.filter((h) => h.sources.publicationRegistry).length,
      withExternalShare: hotels.filter((h) => h.sources.externalShare).length,
      withPublicationCandidate: hotels.filter((h) => h.sources.publicationCandidate).length,
      draftOrPeerOnly: runtimeIds.filter((id) => id.startsWith("draft_")).length,
    },
    draftPeerRuntimeIds: runtimeIds.filter((id) => id.startsWith("draft_")).sort(),
    hotels,
    subjectHotels: hotels.filter((h) => h.inventoryRole === "ADP_SUBJECT"),
    draftPeerHotels: hotels.filter((h) => h.inventoryRole === "DRAFT_PEER_FIXTURE"),
  };
}
