/**
 * Packet 2.8B-2 — OrganizationEvidenceCorpus (L1).
 * Indexes existing org-level evidence for reuse — no new Webhound.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  GSF_OWNER_ENTITY_ID,
} from "./compilers/gsf-from-evidence.js";
import { DOVETAIL_ENTITY_ID } from "./compilers/cambridge-from-evidence.js";
import { HNF_ENTITY_ID, ALLIANCE_ENTITY_ID } from "./compilers/mexico-from-evidence.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../../../..");

export const ORGANIZATION_EVIDENCE_CORPUS_VERSION = "organization-evidence-corpus-v1";

const CORPUS = Object.freeze({
  [GSF_OWNER_ENTITY_ID]: {
    owner_entity_id: GSF_OWNER_ENTITY_ID,
    documents: [
      {
        id: "doc_kgpv_deep",
        path: "fixtures/golden-demo/krystal-grand-pv-deep-research-v1.json",
        class: "deep_research_compile",
      },
      {
        id: "doc_gsf_cohort",
        path: "fixtures/golden-demo/gsf-mexico-ownership-cohort-v1.json",
        class: "relationship_cohort",
      },
    ],
  },
  [DOVETAIL_ENTITY_ID]: {
    owner_entity_id: DOVETAIL_ENTITY_ID,
    documents: [
      {
        id: "doc_cambridge_deep",
        path: "fixtures/golden-demo/cambridge-beaches-deep-research-v1.json",
        class: "deep_research_compile",
      },
    ],
  },
  [HNF_ENTITY_ID]: {
    owner_entity_id: HNF_ENTITY_ID,
    documents: [
      {
        id: "doc_sheraton_deep",
        path: "fixtures/golden-demo/sheraton-gdl-expo-deep-research-v1.json",
        class: "deep_research_compile",
      },
    ],
  },
  [ALLIANCE_ENTITY_ID]: {
    owner_entity_id: ALLIANCE_ENTITY_ID,
    documents: [
      {
        id: "doc_voco_deep",
        path: "fixtures/golden-demo/real-inn-cancun-deep-research-v1.json",
        class: "deep_research_compile",
      },
    ],
  },
});

export function getOrganizationEvidenceCorpus(ownerEntityId) {
  const entry = CORPUS[String(ownerEntityId || "").trim()];
  if (!entry) {
    return {
      version: ORGANIZATION_EVIDENCE_CORPUS_VERSION,
      owner_entity_id: ownerEntityId,
      documents: [],
      status: "EMPTY",
    };
  }
  const documents = entry.documents.map((d) => {
    const abs = path.join(ROOT, d.path);
    return {
      ...d,
      exists: fs.existsSync(abs),
    };
  });
  return {
    version: ORGANIZATION_EVIDENCE_CORPUS_VERSION,
    owner_entity_id: entry.owner_entity_id,
    documents,
    status: documents.some((d) => d.exists) ? "AVAILABLE" : "MISSING_FILES",
    research_ladder: ["L0_graph", "L1_corpus", "L2_playbooks", "L3_native", "L4_advanced", "L5_webhound_blocked"],
    l5_webhound: "DISABLED_THIS_PACKET",
  };
}
