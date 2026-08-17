import dotenv from "dotenv";
dotenv.config();
import { listPresentationRowsLight } from "../lib/partner-intelligence/brand-explorer-lane2-common.js";
import { auditPresentationRowExternalOwner } from "../lib/partner-intelligence/brand-explorer-external-owner-content-governance.js";

const { rows } = await listPresentationRowsLight("recwXZ5gVZ8ZH8ekA", "BW Premier Collection");
const s1 = rows.find((r) => r.slotKey === "overview.scenario.1");
console.log(JSON.stringify(s1, null, 2));
if (s1) console.log("audit", auditPresentationRowExternalOwner(s1));
