#!/usr/bin/env node
import { runMutationSuite } from "../lib/brand-explorer/canonical/mutations.js";
import { lessonRegistrySummary } from "../lib/brand-explorer/canonical/lessons.js";

const suite = runMutationSuite();
const lessons = lessonRegistrySummary();
console.log(JSON.stringify({ suite, lessons, writes: 0 }, null, 2));
process.exit(suite.pass && lessons.executable ? 0 : 1);
