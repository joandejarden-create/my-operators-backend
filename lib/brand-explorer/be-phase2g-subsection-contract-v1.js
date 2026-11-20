/**
 * Phase 2G — subsection hierarchy + parity helpers.
 * Contract: BE_SUBSECTION_PARITY_CONTRACT_V1
 */
import { readFileSync } from "fs";
import { join } from "path";
import { repoRoot } from "./canonical/product-shell.js";

export const BE_SUBSECTION_PARITY_CONTRACT_V1 = "BE_SUBSECTION_PARITY_CONTRACT_V1";

let _contract = null;

export function loadSubsectionParityContract({ reload = false } = {}) {
  if (_contract && !reload) return _contract;
  _contract = JSON.parse(
    readFileSync(join(repoRoot(), "configs/brand-explorer-subsection-parity-contract-v1.json"), "utf8"),
  );
  return _contract;
}

export function nz(s) {
  return s == null ? "" : String(s).trim();
}

export function listSubsections(tabId = null) {
  const c = loadSubsectionParityContract();
  const tabs = tabId ? c.tabs.filter((t) => t.tabId === tabId) : c.tabs;
  const out = [];
  for (const tab of tabs) {
    for (const sub of tab.subsections || []) {
      out.push({ ...sub, tabId: tab.tabId, tabName: tab.tabName });
    }
  }
  return out;
}

export function slotMatchesSubsection(slotKey, subsection) {
  const key = nz(slotKey);
  return (subsection.slotPatterns || []).some((p) => new RegExp(p).test(key));
}

export function subsectionForSlot(slotKey) {
  for (const sub of listSubsections()) {
    if (slotMatchesSubsection(slotKey, sub)) return sub;
  }
  return null;
}

export function groupBlocksBySubsection(blocks) {
  const list = Array.isArray(blocks) ? blocks : [];
  const map = new Map();
  for (const sub of listSubsections()) {
    map.set(sub.id, { subsection: sub, items: [] });
  }
  const unmatched = [];
  for (const b of list) {
    const sub = subsectionForSlot(b.slotKey || b.slot);
    if (!sub) {
      unmatched.push(b);
      continue;
    }
    map.get(sub.id).items.push(b);
  }
  return { byId: map, unmatched, order: listSubsections() };
}

/** Match collection items by title/body signature — never array index alone. */
export function matchContentItems(leftItems, rightItems) {
  const L = leftItems || [];
  const R = rightItems || [];
  const used = new Set();
  const pairs = [];
  for (const left of L) {
    let best = -1;
    let bestScore = 0;
    R.forEach((right, i) => {
      if (used.has(i)) return;
      let score = 0;
      const lt = nz(left.title).toLowerCase();
      const rt = nz(right.title).toLowerCase();
      const ls = nz(left.slotKey);
      const rs = nz(right.slotKey);
      if (ls && rs && ls === rs) score += 2;
      if (lt && rt && lt === rt) score += 4;
      else if (lt && rt && (lt.includes(rt) || rt.includes(lt))) score += 2;
      if (nz(left.imageUrl) && nz(left.imageUrl) === nz(right.imageUrl)) score += 1;
      if (nz(left.body).slice(0, 40) && nz(left.body).slice(0, 40) === nz(right.body).slice(0, 40)) score += 2;
      if (score > bestScore) {
        bestScore = score;
        best = i;
      }
    });
    if (best >= 0 && bestScore > 0) {
      used.add(best);
      pairs.push({ left, right: R[best], status: "matched" });
    } else {
      pairs.push({ left, right: null, status: "missing_right" });
    }
  }
  R.forEach((right, i) => {
    if (!used.has(i)) pairs.push({ left: null, right, status: "missing_left" });
  });
  return pairs;
}
