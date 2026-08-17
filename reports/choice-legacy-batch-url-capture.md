# Choice Legacy Mini-Batch URL Capture v1

Generated: 2026-07-06T21:58:36.486Z
Mode: **apply**
Airtable modified: **yes**

## Executive summary

| Metric | Count |
|--------|------:|
| Total URLs planned | 6 |
| Ready to capture | 6 |
| Captured | 6 |
| Skipped duplicates | 0 |
| Failed | 0 |

### Warnings (aggregate)

- browser_user_agent_fallback_used

### Batch apply command

```bash
npm run choice-legacy-batch-url-capture -- --apply --approve-choice-legacy-batch-url-capture
```

### Next recommended command

```bash
npm run choice-legacy-batch-source-stewardship -- --dry-run
```

## URLs

| Brand | Slot | URL | Status | HTTP | Bytes | Text len | Duplicate | Source ID |
|-------|------|-----|--------|------|------:|---------:|-----------|-----------|
| Comfort Inn & Suites | consumer_page | https://www.choicehotels.com/comfort-hotels | captured | 200 | 3027571 | 7373 | no | `recxm2Jxqvi2n2I8K` |
| | warnings: browser_user_agent_fallback_used | | | | | | | |
| Comfort Inn & Suites | press_kit | https://media.choicehotels.com/comfort-press-kit | captured | 200 | 132820 | 6651 | no | `recRbi8CjS8BVt4Z3` |
| Everhome Suites | consumer_page | https://www.choicehotels.com/everhome-suites | captured | 200 | 3153443 | 14128 | no | `rec28KQ9ubpynVfTq` |
| | warnings: browser_user_agent_fallback_used | | | | | | | |
| Everhome Suites | press_kit | https://media.choicehotels.com/everhome-suites | captured | 200 | 138824 | 6168 | no | `rechbWISi8BQwTqGb` |
| Quality Inn | consumer_page | https://www.choicehotels.com/quality-inn | captured | 200 | 3079649 | 9239 | no | `recpsFcGtpvib16s0` |
| | warnings: browser_user_agent_fallback_used | | | | | | | |
| Quality Inn | press_kit | https://media.choicehotels.com/quality-press-kit | captured | 200 | 129518 | 5658 | no | `recfh3rpBaKo0U0H1` |

## Per-URL detail

### Comfort Inn & Suites — consumer_page

- Brand record: `recOzH5iAE1xEjyD0`
- Source URL: https://www.choicehotels.com/comfort-hotels
- Source title: **Comfort Inn & Suites — Choice consumer brand page**
- Type: **Brand Page**
- Status: **captured**
- Final URL: https://www.choicehotels.com/comfort-hotels
- HTTP status: 200
- Content type: text/html; charset=utf-8
- Bytes: 3027571
- Readable text length: 7373
- Duplicate: no
- Source Library row: `recxm2Jxqvi2n2I8K`
- Local file path: `Choice Hotels International/website/Comfort Inn & Suites — Choice consumer brand page.html`
- Warnings: browser_user_agent_fallback_used

### Comfort Inn & Suites — press_kit

- Brand record: `recOzH5iAE1xEjyD0`
- Source URL: https://media.choicehotels.com/comfort-press-kit
- Source title: **Comfort Inn & Suites — Choice press kit / media center**
- Type: **Press Release**
- Status: **captured**
- Final URL: https://media.choicehotels.com/comfort-press-kit
- HTTP status: 200
- Content type: text/html; charset=utf-8
- Bytes: 132820
- Readable text length: 6651
- Duplicate: no
- Source Library row: `recRbi8CjS8BVt4Z3`
- Local file path: `Choice Hotels International/press/Comfort Inn & Suites — Choice press kit _ media center.html`

### Everhome Suites — consumer_page

- Brand record: `recqkkrsevi4r9ibj`
- Source URL: https://www.choicehotels.com/everhome-suites
- Source title: **Everhome Suites — Choice consumer brand page**
- Type: **Brand Page**
- Status: **captured**
- Final URL: https://www.choicehotels.com/everhome-suites
- HTTP status: 200
- Content type: text/html; charset=utf-8
- Bytes: 3153443
- Readable text length: 14128
- Duplicate: no
- Source Library row: `rec28KQ9ubpynVfTq`
- Local file path: `Choice Hotels International/website/Everhome Suites — Choice consumer brand page.html`
- Warnings: browser_user_agent_fallback_used

### Everhome Suites — press_kit

- Brand record: `recqkkrsevi4r9ibj`
- Source URL: https://media.choicehotels.com/everhome-suites
- Source title: **Everhome Suites — Choice press kit / media center**
- Type: **Press Release**
- Status: **captured**
- Final URL: https://media.choicehotels.com/everhome-suites
- HTTP status: 200
- Content type: text/html; charset=utf-8
- Bytes: 138824
- Readable text length: 6168
- Duplicate: no
- Source Library row: `rechbWISi8BQwTqGb`
- Local file path: `Choice Hotels International/press/Everhome Suites — Choice press kit _ media center.html`

### Quality Inn — consumer_page

- Brand record: `recd8o4k1JddhkRWW`
- Source URL: https://www.choicehotels.com/quality-inn
- Source title: **Quality Inn — Choice consumer brand page**
- Type: **Brand Page**
- Status: **captured**
- Final URL: https://www.choicehotels.com/quality-inn
- HTTP status: 200
- Content type: text/html; charset=utf-8
- Bytes: 3079649
- Readable text length: 9239
- Duplicate: no
- Source Library row: `recpsFcGtpvib16s0`
- Local file path: `Choice Hotels International/website/Quality Inn — Choice consumer brand page.html`
- Warnings: browser_user_agent_fallback_used

### Quality Inn — press_kit

- Brand record: `recd8o4k1JddhkRWW`
- Source URL: https://media.choicehotels.com/quality-press-kit
- Source title: **Quality Inn — Choice press kit / media center**
- Type: **Press Release**
- Status: **captured**
- Final URL: https://media.choicehotels.com/quality-press-kit
- HTTP status: 200
- Content type: text/html; charset=utf-8
- Bytes: 129518
- Readable text length: 5658
- Duplicate: no
- Source Library row: `recfh3rpBaKo0U0H1`
- Local file path: `Choice Hotels International/press/Quality Inn — Choice press kit _ media center.html`

## Apply result

- Captured: **6**
- Skipped duplicates: **0**
- Failed: **0**

## Does not do

- Rebuild Brand Explorer content or overwrite Brand Setup fields
- Capture development URLs (JS-shell provenance only — excluded from v1)
- Extract or approve facts
- Publish governance or set Company Validated / Company Validation Date
- Auto-approve URL sources (Explorer Use and Extraction remain No)
- Change UI, scoring, BAS, OAS, OCS, Deal Readiness, or schema
