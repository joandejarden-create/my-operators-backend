/**
 * One-shot: strip webmaster desk personas + clean polluted role strings.
 */
import "../load-env.js";
import {
  loadOpportunitiesCanonical,
  saveOpportunitiesCanonical,
} from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { hasNamedPerson } from "../lib/group-demand-intelligence/contact-resolution.js";
import { isRejectedDeskPersona } from "../lib/group-demand-intelligence/contact-desk-persona-gate.js";
import { classifyContactTier } from "../lib/group-demand-intelligence/contact-tiers-v1-2.js";
import { attachCompletenessFields } from "../lib/group-demand-intelligence/contact-completeness-v1.js";

const HOTEL = "recLuxvwwxID7U2B8";

const doc = await loadOpportunitiesCanonical(HOTEL);
let fixed = 0;
const next = (doc.opportunities || []).map((o) => {
  const name = o.primaryContactName || o.primaryContact?.name;
  const email = o.primaryContactEmail || o.primaryContact?.email;
  const role = o.primaryContactRole || o.primaryContact?.role;
  const c = { name, email, role };
  const desk = isRejectedDeskPersona(c);
  const namedOk = hasNamedPerson(c);

  if (desk.reject || (name && !namedOk && /webmaster/i.test(String(name)))) {
    fixed += 1;
    const cleaned = {
      ...o,
      primaryContact: null,
      primaryContactName: null,
      primaryContactRole: null,
      primaryContactEmail: null,
      primaryContactPhone: null,
      commercialContactPathLabel:
        o.commercialContactPathLabel ||
        "Official program / organization contact path",
      contactOfficialUrl: o.contactOfficialUrl || o.officialSource || null,
    };
    cleaned.contactTier = classifyContactTier(cleaned);
    return attachCompletenessFields(cleaned, {
      publicContactCeilingReason: "NO_PUBLIC_STAFF",
      unresolvedContactReason: "EVENT_OWNER_NOT_OBSERVABLE",
    });
  }

  if (role && name && String(role).includes(String(name))) {
    fixed += 1;
    const cleanedRole = String(role)
      .replace(new RegExp(`,?\\s*${String(name).replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}.*$`, "i"), "")
      .replace(/,\s*at\s*$/i, "")
      .trim();
    const cleaned = {
      ...o,
      primaryContactRole: cleanedRole || role,
      primaryContact: {
        ...(o.primaryContact || {}),
        role: cleanedRole || role,
      },
    };
    return attachCompletenessFields(cleaned);
  }

  return o;
});

await saveOpportunitiesCanonical(HOTEL, { ...doc, opportunities: next });
const nci = next.find((o) => o.id === "gdi_opp_2027_nci_rna_biology_symposium_7");
const loudoun = next.find((o) => o.id === "gdi_opp_loudoun_college_showcase_2027");
console.log(
  JSON.stringify(
    {
      fixed,
      nciName: nci?.primaryContactName,
      nciTier: nci?.contactTier,
      loudounRole: loudoun?.primaryContactRole,
    },
    null,
    2
  )
);
