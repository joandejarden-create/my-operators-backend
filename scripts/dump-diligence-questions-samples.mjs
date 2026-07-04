import "../load-env.js";
import { NEW_BASE_DILIGENCE_TABLE, fetchAllRecordsRest } from "../api/lib/operator-setup-new-base-read.js";

const rows = await fetchAllRecordsRest(NEW_BASE_DILIGENCE_TABLE);
const byKey = new Map();

for (const r of rows) {
    const f = r.fields || {};
    const cat = String(f.category || "").trim();
    const q = String(f.question || "").trim();
    const ans = String(f.answer || "").trim();
    if (!q) continue;
    const k = cat + "\t" + q;
    if (!byKey.has(k)) {
        byKey.set(k, { category: cat, question: q, samples: [] });
    }
    const o = byKey.get(k);
    if (ans && o.samples.length < 4) {
        o.samples.push(ans.slice(0, 280).replace(/\s+/g, " "));
    }
}

const list = [...byKey.values()].sort((a, b) =>
    a.category.localeCompare(b.category) || a.question.localeCompare(b.question)
);

for (const x of list) {
    console.log("\n---\n[" + x.category + "]\nQ: " + x.question);
    if (x.samples.length) {
        console.log("Sample answer snippets (from table):");
        x.samples.forEach((s, i) => console.log("  (" + (i + 1) + ") " + s + (s.length >= 280 ? "…" : "")));
    } else {
        console.log("(no non-empty answers in table for this question)");
    }
}

console.log("\nTotal unique Q:", list.length, "total rows:", rows.length);
