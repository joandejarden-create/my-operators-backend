import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import {
  VAL_PILOT_FIT,
  VAL_PILOT_OUTREACH_MESSAGE_ANGLE,
  VAL_PILOT_OUTREACH_SEGMENT,
  VAL_PILOT_OUTREACH_STATUS,
  VAL_PILOT_REGION,
  VAL_PILOT_RELATIONSHIP_STRENGTH,
  VAL_PILOT_RELEVANCE,
  VAL_PILOT_SEND_CHANNEL,
  VAL_PILOT_PRIORITY,
} from "../lib/gtm-owner-target/pilot-target-list-field-map.js";

const requiredSets = [
  ["VAL_PILOT_REGION", VAL_PILOT_REGION],
  ["VAL_PILOT_OUTREACH_SEGMENT", VAL_PILOT_OUTREACH_SEGMENT],
  ["VAL_PILOT_FIT", VAL_PILOT_FIT],
  ["VAL_PILOT_PRIORITY", VAL_PILOT_PRIORITY],
  ["VAL_PILOT_OUTREACH_STATUS", VAL_PILOT_OUTREACH_STATUS],
  ["VAL_PILOT_SEND_CHANNEL", VAL_PILOT_SEND_CHANNEL],
  ["VAL_PILOT_OUTREACH_MESSAGE_ANGLE", VAL_PILOT_OUTREACH_MESSAGE_ANGLE],
  ["VAL_PILOT_RELATIONSHIP_STRENGTH", VAL_PILOT_RELATIONSHIP_STRENGTH],
  ["VAL_PILOT_RELEVANCE", VAL_PILOT_RELEVANCE],
];

const migrationMap = {
  pilotFit: {
    Strong: "Strong Pilot Candidate",
    Possible: "Possible Pilot Candidate",
    Weak: "Weak Fit",
    "Not a Fit": "Not A Fit",
  },
  relationshipStrength: {
    Strong: "Strong Warm Relationship",
    Warm: "Known Contact",
    Light: "LinkedIn / Light Connection",
    LinkedIn: "LinkedIn / Light Connection",
    "Cold Outreach": "Cold",
    Cold: "Cold",
  },
  outreachStatus: {
    "Converted to Pilot": "Converted To Pilot",
  },
  outreachMessageAngle: {
    "Operator Profile": "Operator Perspective",
    "Brand Profile": "Brand Criteria Input",
    "Referral Ask": "Warm Intro / Referral",
    "Feedback Ask": "Feedback / Perspective",
    "Feedback / Profile Input / Referral Only If Owner Opts In": "Owner-Opt-In Referral Only",
  },
};

function assertNoDuplicates(name, list) {
  const unique = new Set(list);
  assert.equal(unique.size, list.length, `${name} contains duplicate options`);
}

function run() {
  for (const [name, set] of requiredSets) {
    assert.ok(Array.isArray(set) && set.length > 0, `${name} should be non-empty`);
    assertNoDuplicates(name, set);
  }

  assert.ok(
    VAL_PILOT_OUTREACH_MESSAGE_ANGLE.includes("Owner-Opt-In Referral Only"),
    "Owner-Opt-In Referral Only must exist"
  );
  assert.ok(
    !VAL_PILOT_OUTREACH_MESSAGE_ANGLE.includes("Owner Leads"),
    "Owner Leads must not exist"
  );

  for (const target of Object.values(migrationMap.pilotFit)) {
    assert.ok(VAL_PILOT_FIT.includes(target), `pilotFit migration target missing option: ${target}`);
  }
  for (const target of Object.values(migrationMap.relationshipStrength)) {
    assert.ok(
      VAL_PILOT_RELATIONSHIP_STRENGTH.includes(target),
      `relationshipStrength migration target missing option: ${target}`
    );
  }
  for (const target of Object.values(migrationMap.outreachStatus)) {
    assert.ok(
      VAL_PILOT_OUTREACH_STATUS.includes(target),
      `outreachStatus migration target missing option: ${target}`
    );
  }
  for (const target of Object.values(migrationMap.outreachMessageAngle)) {
    assert.ok(
      VAL_PILOT_OUTREACH_MESSAGE_ANGLE.includes(target),
      `outreachMessageAngle migration target missing option: ${target}`
    );
  }

  // Dry-run safety smoke check: option schema should not mutate.
  const baseId = process.env.AIRTABLE_GTM_BASE_ID;
  const token = process.env.AIRTABLE_GTM_API_KEY || process.env.AIRTABLE_PAT || process.env.AIRTABLE_API_KEY;
  if (baseId && token) {
    const before = execFileSync(
      process.execPath,
      [
        "--input-type=module",
        "-e",
        "const baseId=process.env.AIRTABLE_GTM_BASE_ID; const token=process.env.AIRTABLE_GTM_API_KEY||process.env.AIRTABLE_PAT||process.env.AIRTABLE_API_KEY; const r=await fetch('https://api.airtable.com/v0/meta/bases/'+encodeURIComponent(baseId)+'/tables',{headers:{Authorization:'Bearer '+token}}); const j=await r.json(); const t=(j.tables||[]).find(x=>x.id==='tblgsKWuI25MWohAP'||x.name==='Pilot Target List'); const names=['Outreach Segment','Pilot Fit','Priority','Outreach Status','Send Channel','Outreach Message Angle','Relationship Strength','Pilot Relevance']; const shape=Object.fromEntries((t.fields||[]).filter(f=>names.includes(f.name)).map(f=>[f.name,(f.options?.choices||[]).map(c=>c.name)])); console.log(JSON.stringify(shape));",
      ],
      { encoding: "utf8", env: process.env }
    ).trim();

    execFileSync(process.execPath, ["scripts/setup-pilot-target-list-dropdown-options.mjs", "--dry-run"], {
      encoding: "utf8",
      env: process.env,
    });

    const after = execFileSync(
      process.execPath,
      [
        "--input-type=module",
        "-e",
        "const baseId=process.env.AIRTABLE_GTM_BASE_ID; const token=process.env.AIRTABLE_GTM_API_KEY||process.env.AIRTABLE_PAT||process.env.AIRTABLE_API_KEY; const r=await fetch('https://api.airtable.com/v0/meta/bases/'+encodeURIComponent(baseId)+'/tables',{headers:{Authorization:'Bearer '+token}}); const j=await r.json(); const t=(j.tables||[]).find(x=>x.id==='tblgsKWuI25MWohAP'||x.name==='Pilot Target List'); const names=['Outreach Segment','Pilot Fit','Priority','Outreach Status','Send Channel','Outreach Message Angle','Relationship Strength','Pilot Relevance']; const shape=Object.fromEntries((t.fields||[]).filter(f=>names.includes(f.name)).map(f=>[f.name,(f.options?.choices||[]).map(c=>c.name)])); console.log(JSON.stringify(shape));",
      ],
      { encoding: "utf8", env: process.env }
    ).trim();
    assert.equal(after, before, "Dry-run must not mutate Airtable schema options");
  }

  console.log("test-pilot-target-list-dropdown-options: all passed");
}

run();

