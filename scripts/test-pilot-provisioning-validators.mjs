#!/usr/bin/env node
/**
 * Unit tests for pilot provisioning validators (no Airtable).
 */
import {
  validateMemberstackIdPair,
  validateWorkspaceAccessSource,
  validateAccountStatus,
  classifyDealAccessPath,
  isTestMemberstackId,
} from "../lib/pilot-provisioning/pilot-validators.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");

let passed = 0;
let failed = 0;

function ok(cond, msg) {
  if (cond) {
    passed += 1;
    console.log("ok:", msg);
  } else {
    failed += 1;
    console.error("FAIL:", msg);
  }
}

{
  const r = validateMemberstackIdPair(
    { primary: "mem_live123", mirror: "mem_live123" },
    { allowTestId: false }
  );
  ok(r.ok, "matching live mem_ ids pass");
}

{
  const r = validateMemberstackIdPair(
    { primary: "mem_sb_test", mirror: "mem_sb_test" },
    { allowTestId: false }
  );
  ok(r.problems.includes("test_memberstack_id_on_production_users_row"), "mem_sb_ rejected by default");
}

{
  const r = validateMemberstackIdPair(
    { primary: "mem_live", mirror: "mem_other" },
    { allowTestId: false }
  );
  ok(r.problems.includes("memberstack_id_slug_mismatch"), "mismatch detected");
}

{
  const r = validateWorkspaceAccessSource(
    { "Workspace Access": ["Owner"] },
    { "Workspace Access": ["Admin", "Owner"] }
  );
  ok(
    !r.warnings.includes("users_row_has_workspace_access_populated"),
    "no Users Workspace Access warnings (field not on Users in current base)"
  );
  ok(r.companyWs.includes("Admin") && r.companyWs.includes("Owner"), "reads CP Workspace Access values");
  ok(r.workspaceAccessSource === "Company Profile → Workspace Access", "workspace access source label");
}

{
  const r = validateWorkspaceAccessSource({}, { "Workspace Access": ["Owner"] });
  ok(r.problems.length === 0 && r.companyWs.includes("Owner"), "CP Owner passes");
}

{
  const r = validateAccountStatus({ fieldName: "Account Status", value: "Active" });
  ok(r.problems.length === 0, "Active status passes");
}

{
  const r = validateAccountStatus({ fieldName: "Account Status", value: "Pending" });
  ok(r.problems.some((p) => p.startsWith("account_status_pending")), "Pending fails without allow");
}

{
  const r = validateAccountStatus(
    { fieldName: "Account Status", value: "Pending" },
    { allowPending: true }
  );
  ok(r.problems.length === 0, "Pending allowed with flag");
}

{
  const r = validateAccountStatus({ fieldName: "Account Status", value: "Disabled" });
  ok(r.problems.some((p) => p.startsWith("account_status_inactive")), "Disabled fails");
}

{
  const paths = classifyDealAccessPath(
    { "Company Profile": ["recCP"], User_ID: ["recUser"] },
    "recUser",
    ["recCP"]
  );
  ok(paths.viaCompany && paths.viaUser, "deal via both CP and User_ID");
}

{
  const paths = classifyDealAccessPath({ User_ID: ["recUser"] }, "recUser", ["recCP"]);
  ok(paths.viaUser && !paths.viaCompany, "user-only deal path");
}

ok(isTestMemberstackId("mem_sb_abc"), "isTestMemberstackId true for mem_sb_");

{
  const { readMemberstackIdsFromUserFields } = await import("../lib/pilot-provisioning/pilot-validators.js");
  const ids = readMemberstackIdsFromUserFields({
    Unique_Webflow_ID: "mem_live123",
    Slug: "mem_live123",
  });
  ok(ids.primary === "mem_live123" && ids.mirror === "mem_live123", "reads Unique_Webflow_ID underscore alias");
}

const linkScript = fs.readFileSync(path.join(root, "scripts/link-airtable-user-memberstack.mjs"), "utf8");
ok(linkScript.includes("--allow-test-memberstack-id"), "link script has allow-test flag");
ok(linkScript.includes("mem_sb_"), "link script documents mem_sb_ rejection");

console.log(`\ntest-pilot-provisioning-validators: ${passed} passed, ${failed} failed`);
process.exit(failed > 0 ? 1 : 0);
