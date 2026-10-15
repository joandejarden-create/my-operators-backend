#!/usr/bin/env node
/**
 * Lesson registry inspector. Does not invent lessons.
 */
import { lessonRegistrySummary, activeLessons } from "../lib/brand-explorer/canonical/lessons.js";

if (process.argv.includes("--apply")) {
  console.error("Lesson upgrade refuses --apply. Active lessons require a validator and a mutation test already in the registry.");
  process.exit(2);
}
const summary = lessonRegistrySummary();
console.log(JSON.stringify({ ...summary, activeIds: activeLessons().map((l) => l.lessonId), writes: 0 }, null, 2));
process.exit(summary.executable ? 0 : 1);
