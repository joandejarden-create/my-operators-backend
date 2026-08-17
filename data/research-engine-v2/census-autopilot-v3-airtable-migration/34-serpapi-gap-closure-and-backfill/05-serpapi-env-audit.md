# SerpApi env audit

## Canonical name
`SERPAPI_KEY`

## Findings
1. Provider client (`lib/research-engine-v2/providers/serpapi-google-hotels/client.js`) historically required `SERPAPI_KEY`.
2. V3.0.2 deep research gated on `SERPAPI_API_KEY` only — **naming mismatch**.
3. `.env.example` documents `SERPAPI_KEY=`.
4. Fixed: client resolves `SERPAPI_KEY ?? SERPAPI_API_KEY`; V3.0.2 research check updated; canonical remains `SERPAPI_KEY`.

## Availability (boolean only)
`serpapi_key_available = true`
