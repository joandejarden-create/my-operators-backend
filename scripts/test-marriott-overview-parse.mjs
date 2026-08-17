#!/usr/bin/env node
import { readFileSync } from "node:fs";
import { parseMarriottOverviewHtml } from "../lib/marriott-hotel-content-fetch.js";

const html = readFileSync("fixtures/marriott-overview-poplc-sample.html", "utf8");
const parsed = parseMarriottOverviewHtml(html);
console.log(JSON.stringify(parsed, null, 2));
console.log("\namenity count", parsed.amenities.length);
console.log("has overview", parsed.description.includes("15-minute drive"));
