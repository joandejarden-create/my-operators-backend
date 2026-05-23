#!/usr/bin/env node
/**
 * Print Memberstack custom field keys for a member (discover API field IDs).
 *
 *   node scripts/inspect-memberstack-custom-fields.mjs --email joandejarden@gmail.com
 */
import "dotenv/config";
import axios from "axios";
import { MS_CF } from "../lib/memberstack/memberstack-custom-fields.js";

const BASE = (process.env.MEMBERSTACK_BASE_URL || "https://admin.memberstack.com").replace(/\/$/, "");

function parseArgs() {
  const emailIdx = process.argv.indexOf("--email");
  const idIdx = process.argv.indexOf("--memberstack-id");
  const email = emailIdx >= 0 ? process.argv[emailIdx + 1] : null;
  const memberstackId = idIdx >= 0 ? process.argv[idIdx + 1] : null;
  if (!email && !memberstackId) {
    console.error(
      "Usage: node scripts/inspect-memberstack-custom-fields.mjs --email <email>\n" +
        "   or: node scripts/inspect-memberstack-custom-fields.mjs --memberstack-id mem_sb_..."
    );
    process.exit(1);
  }
  return {
    email: email ? email.trim().toLowerCase() : null,
    memberstackId: memberstackId ? memberstackId.trim() : null,
  };
}

async function main() {
  const key = (process.env.MEMBERSTACK_SECRET_KEY || "").trim();
  if (!key) {
    console.error("Set MEMBERSTACK_SECRET_KEY in .env");
    process.exit(1);
  }
  const { email, memberstackId } = parseArgs();
  const headers = { "X-API-KEY": key, "Content-Type": "application/json" };
  const path = memberstackId || encodeURIComponent(email);
  const res = await axios.get(`${BASE}/members/${path}`, {
    headers,
    validateStatus: () => true,
  });
  if (res.status !== 200) {
    console.error("GET member failed:", res.status, res.data);
    process.exit(1);
  }
  const member = res.data?.data || res.data?.member || res.data;
  const cf = member?.customFields || {};
  console.log("Member:", member?.id, member?.email);
  console.log("\nConfigured MS_CF mapping (env overrides defaults):");
  console.log(JSON.stringify(MS_CF, null, 2));
  console.log("\nActual customFields on member:");
  console.log(JSON.stringify(cf, null, 2));
  console.log("\nKeys on member:", Object.keys(cf).join(", ") || "(none)");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
