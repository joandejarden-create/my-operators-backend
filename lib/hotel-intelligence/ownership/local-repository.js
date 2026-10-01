/**
 * Local file OwnershipRepository (P0 persistence).
 * Hidden behind OwnershipRepository — services must not import paths from here.
 *
 * Layout under {root}/ownership/:
 *   meta.json, entities.json, aliases.json, relationships.json,
 *   evidence.jsonl, research-runs.json, observations.jsonl, dossiers.json
 */

import fs from "node:fs";
import path from "node:path";
import { resolveDataRoot, ensureDir, readJsonFile, writeJsonFile } from "../local-store.js";
import { createMemoryOwnershipRepository } from "./memory-repository.js";
import { OWNERSHIP_REPOSITORY_VERSION } from "./repository.js";

export const LOCAL_OWNERSHIP_REPOSITORY_VERSION =
  "ownership-local-repository-v1";

const SCHEMA_VERSION = 1;

/**
 * @param {object} [opts]
 * @param {string} [opts.root] — hotel-intelligence data root (ownership/ created under it)
 */
export function createLocalOwnershipRepository(opts = {}) {
  const dataRoot = opts.root || resolveDataRoot(opts.env || process.env);
  const ownershipRoot = path.join(dataRoot, "ownership");
  ensureDir(ownershipRoot);

  const paths = {
    root: ownershipRoot,
    meta: path.join(ownershipRoot, "meta.json"),
    entities: path.join(ownershipRoot, "entities.json"),
    aliases: path.join(ownershipRoot, "aliases.json"),
    relationships: path.join(ownershipRoot, "relationships.json"),
    evidence: path.join(ownershipRoot, "evidence.jsonl"),
    runs: path.join(ownershipRoot, "research-runs.json"),
    observations: path.join(ownershipRoot, "observations.jsonl"),
    dossiers: path.join(ownershipRoot, "dossiers.json"),
  };

  /** Simple per-instance write lock (P0; single-process MCP). */
  let writeChain = Promise.resolve();
  function withWriteLock(fn) {
    const next = writeChain.then(fn, fn);
    writeChain = next.catch(() => {});
    return next;
  }

  function loadMaps() {
    const entitiesDoc = readJsonFile(paths.entities, {
      version: SCHEMA_VERSION,
      by_id: {},
    });
    const aliasesDoc = readJsonFile(paths.aliases, {
      version: SCHEMA_VERSION,
      by_id: {},
    });
    const relDoc = readJsonFile(paths.relationships, {
      version: SCHEMA_VERSION,
      by_id: {},
    });
    const runsDoc = readJsonFile(paths.runs, {
      version: SCHEMA_VERSION,
      by_id: {},
    });
    const dossiersDoc = readJsonFile(paths.dossiers, {
      version: SCHEMA_VERSION,
      by_id: {},
    });

    const entities = new Map(Object.entries(entitiesDoc.by_id || {}));
    const aliases = new Map(Object.entries(aliasesDoc.by_id || {}));
    const relationships = new Map(Object.entries(relDoc.by_id || {}));
    const runs = new Map(Object.entries(runsDoc.by_id || {}));
    const dossiers = new Map(Object.entries(dossiersDoc.by_id || {}));
    const evidenceByRel = loadEvidenceJsonl(paths.evidence);
    const observations = loadObservationsJsonl(paths.observations);

    return { entities, aliases, relationships, runs, evidenceByRel, observations, dossiers };
  }

  function persistMaps(maps) {
    ensureMeta();
    writeJsonFile(paths.entities, {
      version: SCHEMA_VERSION,
      by_id: Object.fromEntries(maps.entities),
    });
    writeJsonFile(paths.aliases, {
      version: SCHEMA_VERSION,
      by_id: Object.fromEntries(maps.aliases),
    });
    writeJsonFile(paths.relationships, {
      version: SCHEMA_VERSION,
      by_id: Object.fromEntries(maps.relationships),
    });
    writeJsonFile(paths.runs, {
      version: SCHEMA_VERSION,
      by_id: Object.fromEntries(maps.runs),
    });
    writeJsonFile(paths.dossiers, {
      version: SCHEMA_VERSION,
      by_id: Object.fromEntries(maps.dossiers),
    });
    rewriteEvidenceJsonl(paths.evidence, maps.evidenceByRel);
    rewriteObservationsJsonl(paths.observations, maps.observations);
  }

  function ensureMeta() {
    if (!fs.existsSync(paths.meta)) {
      writeJsonFile(paths.meta, {
        schema_version: SCHEMA_VERSION,
        repository_version: LOCAL_OWNERSHIP_REPOSITORY_VERSION,
        created_at: new Date().toISOString(),
      });
    }
  }

  ensureMeta();
  const maps = loadMaps();
  const memory = createMemoryOwnershipRepository(maps);

  async function persistAfter(promise) {
    const result = await promise;
    persistMaps(maps);
    return result;
  }

  return {
    version: OWNERSHIP_REPOSITORY_VERSION,
    kind: "local",
    local_version: LOCAL_OWNERSHIP_REPOSITORY_VERSION,
    /** @internal tests only */
    _paths: paths,

    getEntity: (id) => memory.getEntity(id),
    listAliases: (id) => memory.listAliases(id),
    findByIdentifier: (k, v) => memory.findByIdentifier(k, v),
    findByNormalizedName: (n) => memory.findByNormalizedName(n),
    getRelationship: (id) => memory.getRelationship(id),
    listRelationshipsForHotel: (id, o) =>
      memory.listRelationshipsForHotel(id, o),
    listRelationshipsForEntity: (id, o) =>
      memory.listRelationshipsForEntity(id, o),
    listEvidence: (id) => memory.listEvidence(id),
    getResearchRun: (id) => memory.getResearchRun(id),

    upsertEntity: (entity) =>
      withWriteLock(() => persistAfter(memory.upsertEntity(entity))),
    addAlias: (alias) =>
      withWriteLock(() => persistAfter(memory.addAlias(alias))),
    upsertRelationship: (rel) =>
      withWriteLock(() => persistAfter(memory.upsertRelationship(rel))),
    addEvidence: (ev) =>
      withWriteLock(() => persistAfter(memory.addEvidence(ev))),
    createResearchRun: (run) =>
      withWriteLock(() => persistAfter(memory.createResearchRun(run))),
    updateResearchRun: (run) =>
      withWriteLock(() => persistAfter(memory.updateResearchRun(run))),

    addObservation: (obs) =>
      withWriteLock(() => persistAfter(memory.addObservation(obs))),
    listObservationsForHotel: (id) => memory.listObservationsForHotel(id),
    listObservationsForSubject: (t, id) => memory.listObservationsForSubject(t, id),
    upsertResearchDossier: (d) =>
      withWriteLock(() => persistAfter(memory.upsertResearchDossier(d))),
    getResearchDossier: (id) => memory.getResearchDossier(id),
    listResearchDossiersForHotel: (id) => memory.listResearchDossiersForHotel(id),

    /** Reload from disk into the in-memory maps (tests / multi-handle). */
    reload() {
      const fresh = loadMaps();
      maps.entities.clear();
      maps.aliases.clear();
      maps.relationships.clear();
      maps.runs.clear();
      maps.evidenceByRel.clear();
      maps.observations.clear();
      maps.dossiers.clear();
      for (const [k, v] of fresh.entities) maps.entities.set(k, v);
      for (const [k, v] of fresh.aliases) maps.aliases.set(k, v);
      for (const [k, v] of fresh.relationships) maps.relationships.set(k, v);
      for (const [k, v] of fresh.runs) maps.runs.set(k, v);
      for (const [k, v] of fresh.evidenceByRel) maps.evidenceByRel.set(k, v);
      for (const [k, v] of fresh.observations) maps.observations.set(k, v);
      for (const [k, v] of fresh.dossiers) maps.dossiers.set(k, v);
    },
  };
}

/**
 * @param {string} filePath
 * @returns {Map<string, object[]>}
 */
function loadEvidenceJsonl(filePath) {
  const byRel = new Map();
  if (!fs.existsSync(filePath)) return byRel;
  const text = fs.readFileSync(filePath, "utf8");
  for (const line of text.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    try {
      const row = JSON.parse(trimmed);
      const rid = String(row.relationship_id || "").trim();
      if (!rid) continue;
      if (!byRel.has(rid)) byRel.set(rid, []);
      byRel.get(rid).push(row);
    } catch {
      /* skip corrupt line */
    }
  }
  return byRel;
}

/**
 * @param {string} filePath
 * @param {Map<string, object[]>} evidenceByRel
 */
function rewriteObservationsJsonl(filePath, observations) {
  ensureDir(path.dirname(filePath));
  const lines = [];
  for (const row of observations.values()) {
    lines.push(JSON.stringify(row));
  }
  const tmp = `${filePath}.${process.pid}.${Date.now()}.tmp`;
  fs.writeFileSync(tmp, lines.length ? `${lines.join("\n")}\n` : "", "utf8");
  fs.copyFileSync(tmp, filePath);
  try {
    fs.unlinkSync(tmp);
  } catch {
    /* ignore */
  }
}

function loadObservationsJsonl(filePath) {
  const byId = new Map();
  if (!fs.existsSync(filePath)) return byId;
  const text = fs.readFileSync(filePath, "utf8");
  for (const line of text.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    try {
      const row = JSON.parse(trimmed);
      const id = String(row.observation_id || "").trim();
      if (!id) continue;
      byId.set(id, row);
    } catch {
      /* skip corrupt line */
    }
  }
  return byId;
}

function rewriteEvidenceJsonl(filePath, evidenceByRel) {
  ensureDir(path.dirname(filePath));
  const lines = [];
  for (const rows of evidenceByRel.values()) {
    for (const row of rows) {
      lines.push(JSON.stringify(row));
    }
  }
  const tmp = `${filePath}.${process.pid}.${Date.now()}.tmp`;
  fs.writeFileSync(tmp, lines.length ? `${lines.join("\n")}\n` : "", "utf8");
  fs.copyFileSync(tmp, filePath);
  try {
    fs.unlinkSync(tmp);
  } catch {
    /* ignore */
  }
}
