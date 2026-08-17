#!/usr/bin/env node
/**
 * Populate Leadership Team Members profile-detail fields (sample CALA-realistic values).
 * Matches Operator Setup → Profile detail (Explorer card) fields.
 *
 * Usage:
 *   node scripts/seed-operator-leadership-member-profile-data.mjs --operator-id recXXX
 *   node scripts/seed-operator-leadership-member-profile-data.mjs --operator-id recXXX --dry-run
 */
import "../load-env.js";
import Airtable from "airtable";
import { MAP_LEADERSHIP_MEMBER } from "../api/lib/operator-leadership-member-map.js";
import {
  NEW_BASE_LEADERSHIP_TABLE,
  fetchRecordsLinkedToMaster,
} from "../api/lib/operator-setup-new-base-read.js";

/** Sample profile rows keyed by normalized last name token (lowercase). */
const SAMPLES_BY_NAME = {
  santos: {
    [MAP_LEADERSHIP_MEMBER.hospitalityExperienceYears]: 25,
    [MAP_LEADERSHIP_MEMBER.companyTenureYears]: 12,
    [MAP_LEADERSHIP_MEMBER.priorBackground]: "Marriott divisional operator — Caribbean & island markets",
    [MAP_LEADERSHIP_MEMBER.languages]: ["English", "Spanish"],
    [MAP_LEADERSHIP_MEMBER.marketExperience]: ["Puerto Rico", "Dominican Republic", "Caribbean"],
    [MAP_LEADERSHIP_MEMBER.coreExpertise]: ["Operations", "Owner Relations", "Brand Compliance"],
    [MAP_LEADERSHIP_MEMBER.relevantAssetTypes]: ["Resort", "Full-Service", "Branded"],
  },
  fernandez: {
    [MAP_LEADERSHIP_MEMBER.hospitalityExperienceYears]: 25,
    [MAP_LEADERSHIP_MEMBER.companyTenureYears]: 8,
    [MAP_LEADERSHIP_MEMBER.priorBackground]: "Major branded flags — resort & full-service operations",
    [MAP_LEADERSHIP_MEMBER.languages]: ["English", "Spanish"],
    [MAP_LEADERSHIP_MEMBER.marketExperience]: ["Dominican Republic", "Puerto Rico", "Caribbean"],
    [MAP_LEADERSHIP_MEMBER.coreExpertise]: ["Operations", "Brand Compliance"],
    [MAP_LEADERSHIP_MEMBER.relevantAssetTypes]: ["Resort", "Full-Service"],
  },
  ohara: {
    [MAP_LEADERSHIP_MEMBER.hospitalityExperienceYears]: 22,
    [MAP_LEADERSHIP_MEMBER.companyTenureYears]: 5,
    [MAP_LEADERSHIP_MEMBER.priorBackground]: "Listed hospitality REIT finance — US GAAP consolidation",
    [MAP_LEADERSHIP_MEMBER.languages]: ["English"],
    [MAP_LEADERSHIP_MEMBER.marketExperience]: ["United States", "Puerto Rico", "Dominican Republic"],
    [MAP_LEADERSHIP_MEMBER.coreExpertise]: ["Finance & Owner Reporting", "Legal / Compliance"],
    [MAP_LEADERSHIP_MEMBER.relevantAssetTypes]: ["Full-Service", "Branded", "Mixed-Use"],
  },
  reyes: {
    [MAP_LEADERSHIP_MEMBER.hospitalityExperienceYears]: 18,
    [MAP_LEADERSHIP_MEMBER.companyTenureYears]: 7,
    [MAP_LEADERSHIP_MEMBER.priorBackground]: "Caribbean hospitality labor — union & contract properties",
    [MAP_LEADERSHIP_MEMBER.languages]: ["English", "Spanish"],
    [MAP_LEADERSHIP_MEMBER.marketExperience]: ["Puerto Rico", "Dominican Republic", "Caribbean"],
    [MAP_LEADERSHIP_MEMBER.coreExpertise]: ["HR / Talent", "Operations"],
    [MAP_LEADERSHIP_MEMBER.relevantAssetTypes]: ["Resort", "Full-Service", "Independent"],
  },
  alvarez: {
    [MAP_LEADERSHIP_MEMBER.hospitalityExperienceYears]: 20,
    [MAP_LEADERSHIP_MEMBER.companyTenureYears]: 12,
    [MAP_LEADERSHIP_MEMBER.priorBackground]: "LATAM hospitality platforms — growth & governance",
    [MAP_LEADERSHIP_MEMBER.languages]: ["English", "Spanish", "Portuguese"],
    [MAP_LEADERSHIP_MEMBER.marketExperience]: ["Mexico", "CALA — Regional", "Brazil"],
    [MAP_LEADERSHIP_MEMBER.coreExpertise]: ["Development", "Owner Relations"],
    [MAP_LEADERSHIP_MEMBER.relevantAssetTypes]: ["Lifestyle", "Branded", "Mixed-Use"],
  },
  chen: {
    [MAP_LEADERSHIP_MEMBER.hospitalityExperienceYears]: 22,
    [MAP_LEADERSHIP_MEMBER.companyTenureYears]: 6,
    [MAP_LEADERSHIP_MEMBER.priorBackground]: "Marriott regional operations — brand-managed full-service",
    [MAP_LEADERSHIP_MEMBER.languages]: ["English"],
    [MAP_LEADERSHIP_MEMBER.marketExperience]: ["United States", "Mexico", "Caribbean"],
    [MAP_LEADERSHIP_MEMBER.coreExpertise]: ["Operations", "Pre-Opening / Transitions"],
    [MAP_LEADERSHIP_MEMBER.relevantAssetTypes]: ["Full-Service", "Select-Service", "Urban"],
  },
};

function parseArgs() {
  const args = process.argv.slice(2);
  let operatorId = "";
  let dryRun = false;
  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--operator-id" && args[i + 1]) operatorId = args[++i];
    if (args[i] === "--dry-run") dryRun = true;
  }
  return { operatorId, dryRun };
}

function nameKey(name) {
  const parts = String(name || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[''\u2018\u2019`]/g, "")
    .split(/\s+/)
    .filter(Boolean);
  return parts[parts.length - 1] || "";
}

function sampleForName(name) {
  const key = nameKey(name);
  return SAMPLES_BY_NAME[key] || null;
}

async function main() {
  const { operatorId, dryRun } = parseArgs();
  if (!operatorId) {
    console.error("Usage: --operator-id recXXXXXXXX [--dry-run]");
    process.exit(1);
  }

  const rows = await fetchRecordsLinkedToMaster(NEW_BASE_LEADERSHIP_TABLE, operatorId);

  if (!rows.length) {
    console.error("No leadership rows linked to master", operatorId);
    process.exit(1);
  }

  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);

  let updated = 0;
  for (const rec of rows) {
    const name = rec.fields?.name || rec.fields?.Name || "";
    const sample = sampleForName(name);
    if (!sample) {
      console.warn("No sample for", name, "— skip");
      continue;
    }
    console.log(dryRun ? "[dry-run] would patch" : "Patching", name, rec.id);
    if (!dryRun) {
      await base(NEW_BASE_LEADERSHIP_TABLE).update(rec.id, sample, { typecast: true });
    }
    updated += 1;
  }

  console.log(
    dryRun ? `Dry run: ${updated} row(s) would update.` : `Updated ${updated} leadership profile row(s).`
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
