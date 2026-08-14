#!/usr/bin/env node
import { sanitizeMarketAlertPlainText } from "../lib/market-alerts-plain-text.js";

const dirtyEntity =
  '&lt;span class="field field--name-title field--type-string field--label-hidden"&gt;Former Full-Service Hotel in Downtown Springfield, IL for Sale&lt;/span&gt;&lt;div class="clearfix text-formatted field field--name-body field--type-text-with-summary field--label-hidden field__item"&gt;&lt;p data-pm-slice="1 1 []"&gt;The Hilco Global Real Estate Practice is managing the bankruptcy sale of the former Wyndham City Centre hotel.&lt;/p&gt;&lt;/div&gt;';

const dirtyRaw =
  '<span class="field field--name-title">Former Full-Service Hotel in Downtown Springfield, IL for Sale</span><div class="clearfix text-formatted field field--name-body field__item"><p data-pm-slice="1 1 []">The Hilco Global Real Estate Practice is managing the bankruptcy sale of the former Wyndham City Centre hotel.</p></div>';

for (const [label, input] of [
  ["entity", dirtyEntity],
  ["raw", dirtyRaw],
]) {
  const out = sanitizeMarketAlertPlainText(input, { preserveWhitespace: true });
  const ok = !out.includes("<") && out.includes("Hilco Global") && out.includes("Wyndham");
  console.log(ok ? "OK" : "FAIL", label, "=>", out);
  if (!ok) process.exitCode = 1;
}
