# Third-party notice — ownership iterative research loop

## Upstream

- Repository: https://github.com/dzhng/deep-research
- Pinned commit: `1f8f3e285bbc23e80b98a66a64effab9069f3ad4`
- License file (MIT): confirmed at that commit (`LICENSE`)
- `package.json` `license` field at that commit: `ISC` (file prevails as MIT for NOTICE purposes; do not redistribute Firecrawl-dependent scripts as Dealality runtime)

## What was adapted into Dealality

Location: `lib/hotel-intelligence/contact-intelligence/ownership-iterative-research-loop.js`

Adapted ideas (rewritten for Dealality contracts — not a vendored copy of `src/deep-research.ts`):

1. Recursive depth / breadth narrowing after each SERP round
2. Query objects carrying a research goal / follow-up direction
3. Follow-up questions derived from prior findings
4. Deduplicating visited URLs and repeated findings across concurrent branches
5. Concurrency limit before parallel search work

## Explicitly not adapted / not installed

- `@mendable/firecrawl-js` — mapped to Context.dev `search` + `scrape_markdown`
- `ai` / `generateObject` as primary claim extractor — Dealality uses deterministic + existing structured document reader
- `writeFinalReport` / `writeFinalAnswer` — Dealality returns the existing ownership/contact handoff contract
- `lodash-es`, `zod`, `p-limit` as new runtime deps — concurrency implemented locally

## MIT license text (upstream LICENSE at pinned commit)

```
MIT License

Copyright (c) 2025 David Zhang

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```
