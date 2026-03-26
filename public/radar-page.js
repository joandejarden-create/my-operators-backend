// Radar with Ranked List page logic (Webflow handoff)
// Preserve required IDs/classes in HTML; this file relies on existing DOM hooks.

// Webflow runs under its own domain; rewrite relative /api/* calls to backend when configured.
(function () {
    try {
        var base = (window.DEAL_CAPTURE_API_BASE_URL || '').replace(/\/$/, '');
        if (!base || typeof window.fetch !== 'function') return;
        var origFetch = window.fetch;
        window.fetch = function (input, init) {
            if (typeof input === 'string' && input.indexOf('/api/') === 0) {
                return origFetch(base + input, init);
            }
            return origFetch(input, init);
        };
    } catch (e) { /* no-op */ }
})();

(function () {
    var rlLastData = null, rlSortColumn = null, rlSortDir = 'desc', rlAbort = null;
    var sortIndicatorHtml = '<span class="sort-indicator"><span class="sort-indicator-arrow sort-indicator-arrow-up"></span><span class="sort-indicator-arrow sort-indicator-arrow-down"></span></span>';
    function rlEsc(s) { var d = document.createElement('div'); d.textContent = s; return d.innerHTML; }
    function rlTh(label, dataSort) { return '<th data-sort="' + dataSort + '"><span style="display:inline-flex;align-items:center;">' + label + sortIndicatorHtml + '</span></th>'; }
    function rlGetRankBy() { return (document.getElementById('rlRankBy') && document.getElementById('rlRankBy').value) || 'operator'; }
    function rlPct(n, d) { if (!d) return '0%'; var v = (n || 0) / d; if (!Number.isFinite(v)) return '0%'; return (v * 100).toFixed(v >= 0.1 ? 1 : 2) + '%'; }
    function rlTotals(list) { return (list || []).reduce(function (acc, it) { acc.units += (it.hotel_count || 0); acc.keys += (it.total_keys || 0); return acc; }, { units: 0, keys: 0 }); }
    var rlLastList = [];
    function toKeys(p) { var v = p.rooms != null ? p.rooms : p.keys; return parseInt(v, 10) || 0; }
    function normStatus(s) { if (s == null || String(s).trim() === '') return 'Unknown'; var t = String(s).trim().toLowerCase(); if (t === 'open') return 'Open'; if (t === 'pipeline') return 'Pipeline'; if (t === 'candidate') return 'Candidate'; if (t.indexOf('open') !== -1) return 'Open'; if (t.indexOf('pipeline') !== -1) return 'Pipeline'; if (t.indexOf('candidate') !== -1) return 'Candidate'; return 'Unknown'; }
    function normChainScale(s) { if (s == null || String(s).trim() === '') return 'Unknown'; var t = String(s).trim().toLowerCase(); if (t.indexOf('luxury') !== -1) return 'Luxury'; if (t.indexOf('upper upscale') !== -1) return 'Upper Upscale'; if (t.indexOf('upscale') !== -1) return 'Upscale'; if (t.indexOf('upper midscale') !== -1) return 'Upper Midscale'; if (t.indexOf('midscale') !== -1) return 'Midscale'; if (t.indexOf('economy') !== -1) return 'Economy'; if (t.indexOf('independent') !== -1) return 'Independent'; return s.trim(); }
    function normMarket(p) { var c = (p.country != null && String(p.country).trim() !== '') ? String(p.country).trim() : null; if (c) return c; var r = (p.region != null && String(p.region).trim() !== '') ? String(p.region).trim() : null; return r || 'Unknown'; }
    function computeExpansionSummaries(props) {
        var list = props || [];
        var totalUnits = list.length;
        var totalKeys = list.reduce(function (sum, p) { return sum + toKeys(p); }, 0);
        var byStatus = {};
        list.forEach(function (p) { var s = normStatus(p.status); if (!byStatus[s]) byStatus[s] = { units: 0, keys: 0 }; byStatus[s].units += 1; byStatus[s].keys += toKeys(p); });
        var statusOrder = ['Open', 'Pipeline', 'Candidate', 'Unknown'];
        var pipelineMix = statusOrder.filter(function (s) { var o = byStatus[s]; return o && (o.units > 0 || o.keys > 0); }).map(function (s) {
            var o = byStatus[s];
            return { status: s, units: o.units, keys: o.keys, unitsPct: totalUnits ? (o.units / totalUnits) * 100 : 0, keysPct: totalKeys ? (o.keys / totalKeys) * 100 : 0 };
        });
        var byScale = {};
        list.forEach(function (p) { var sc = normChainScale(p.propertyType || p.chain_scale); if (!byScale[sc]) byScale[sc] = { units: 0, keys: 0 }; byScale[sc].units += 1; byScale[sc].keys += toKeys(p); });
        var scaleRows = Object.keys(byScale).map(function (n) { return { name: n, units: byScale[n].units, keys: byScale[n].keys, keysPct: totalKeys ? (byScale[n].keys / totalKeys) * 100 : 0 }; }).sort(function (a, b) { return b.keys - a.keys; });
        var maxScaleShow = 7;
        var chainScaleMix = scaleRows.length <= maxScaleShow ? scaleRows : scaleRows.slice(0, maxScaleShow - 1).concat([{ name: 'Other', units: scaleRows.slice(maxScaleShow - 1).reduce(function (a, r) { return a + r.units; }, 0), keys: scaleRows.slice(maxScaleShow - 1).reduce(function (a, r) { return a + r.keys; }, 0), keysPct: scaleRows.slice(maxScaleShow - 1).reduce(function (a, r) { return a + r.keysPct; }, 0) }]);
        return { pipelineMix: pipelineMix, chainScaleMix: chainScaleMix, totalUnits: totalUnits, totalKeys: totalKeys };
    }
    function renderExpansionSummaryBlocks(summaries) {
        var s = summaries;
        var pipelineKeysPct = 0;
        s.pipelineMix.forEach(function (m) { if (m.status === 'Pipeline') pipelineKeysPct = m.keysPct; });
        var pipelineLine = 'Pipeline share (keys): ' + (pipelineKeysPct >= 0.1 ? pipelineKeysPct.toFixed(1) : pipelineKeysPct.toFixed(0)) + '%';
        var pipelineRows = s.pipelineMix.map(function (r) { return '<tr><td class="breakdown-name">' + rlEsc(r.status) + '</td><td class="breakdown-units">' + Number(r.units || 0).toLocaleString() + '</td><td class="breakdown-units">' + rlPct(r.units, s.totalUnits) + '</td><td class="breakdown-keys">' + (r.keys || 0).toLocaleString() + '</td><td class="breakdown-keys">' + rlPct(r.keys, s.totalKeys) + '</td></tr>'; }).join('');
        var scaleRows = s.chainScaleMix.map(function (r) { return '<tr><td class="breakdown-name">' + rlEsc(r.name) + '</td><td class="breakdown-units">' + Number(r.units || 0).toLocaleString() + '</td><td class="breakdown-units">' + rlPct(r.units, s.totalUnits) + '</td><td class="breakdown-keys">' + (r.keys || 0).toLocaleString() + '</td><td class="breakdown-keys">' + rlPct(r.keys, s.totalKeys) + '</td></tr>'; }).join('');
        return '<div class="expansion-summary-blocks"><div class="expansion-summary-header">Summary</div><div class="expansion-summary-cards">' +
            '<div class="expansion-summary-card"><div class="affiliation-breakdown-title">Pipeline Mix</div><table class="affiliation-breakdown-table"><thead><tr><th>Status</th><th>Units</th><th>% Units</th><th>Keys</th><th>% Keys</th></tr></thead><tbody>' + pipelineRows + '</tbody></table><p class="expansion-pipeline-line">' + rlEsc(pipelineLine) + '</p></div>' +
            '<div class="expansion-summary-card"><div class="affiliation-breakdown-title">Chain Scale Mix</div><table class="affiliation-breakdown-table"><thead><tr><th>Chain Scale</th><th>Units</th><th>% Units</th><th>Keys</th><th>% Keys</th></tr></thead><tbody>' + scaleRows + '</tbody></table></div>' +
            '</div></div>';
    }
    function rlIsBlankRow(item) {
        var n = (item.operator_name !== undefined ? item.operator_name : item.name);
        if (n == null || String(n).trim() === '') return true;
        var s = String(n).trim();
        return s === '(no operator)' || s === '(no brand)' || s === '(no parent company)' || s === '(no chain scale)' || s === '(no status)';
    }
    function rlGetRankedList() {
        var rankBy = rlGetRankBy(), list = [], label = 'operators';
        if (!rlLastData) return { list: [], label: label };
        if (rankBy === 'operator') { list = (rlLastData.operators || []).slice(); label = 'operators'; }
        else if (rankBy === 'brand') { list = (rlLastData.brands || []).slice(); label = 'brands'; }
        else if (rankBy === 'parent_company') { list = (rlLastData.parent_companies || []).slice(); label = 'parent companies'; }
        else if (rankBy === 'region') { list = (rlLastData.regions || []).slice(); label = 'regions'; }
        else if (rankBy === 'chain_scale') { list = (rlLastData.chain_scales || []).slice(); label = 'chain scales'; }
        else if (rankBy === 'status') { list = (rlLastData.statuses || []).slice(); label = 'statuses'; }
        var blank = [], nonBlank = [];
        list.forEach(function (item) { if (rlIsBlankRow(item)) blank.push(item); else nonBlank.push(item); });
        nonBlank.sort(function (a, b) { var ka = a.total_keys || 0, kb = b.total_keys || 0; if (kb !== ka) return kb - ka; return (b.hotel_count || 0) - (a.hotel_count || 0); });
        list = nonBlank.concat(blank);
        return { list: list, label: label };
    }

    var RL_MAX_BREAKDOWN_ROWS = 25;
    function rlBuildCountry(properties) {
        var by = {};
        (properties || []).forEach(function (p) { var c = (p.country || '').trim() || '(no country)'; if (!by[c]) by[c] = { units: 0, keys: 0 }; by[c].units += 1; by[c].keys += toKeys(p); });
        var rows = Object.keys(by).map(function (n) { return { name: n, units: by[n].units, keys: by[n].keys }; }).sort(function (a, b) { return b.keys - a.keys; });
        if (!rows.length) return '<div class="breakdown-panel"><div class="country-breakdown-title">Units &amp; keys by country</div><p style="color:#888;font-size:12px;">No data</p></div>';
        var totals = rows.reduce(function (acc, r) { acc.units += r.units; acc.keys += (r.keys || 0); return acc; }, { units: 0, keys: 0 });
        var show = rows.length <= RL_MAX_BREAKDOWN_ROWS ? rows : rows.slice(0, RL_MAX_BREAKDOWN_ROWS - 1);
        var rest = rows.length > RL_MAX_BREAKDOWN_ROWS ? rows.slice(RL_MAX_BREAKDOWN_ROWS - 1) : [];
        if (rest.length) {
            var otherUnits = rest.reduce(function (a, r) { return a + r.units; }, 0);
            var otherKeys = rest.reduce(function (a, r) { return a + (r.keys || 0); }, 0);
            show.push({ name: 'Other (' + rest.length + ' more)', units: otherUnits, keys: otherKeys });
        }
        var tr = show.map(function (r) { return '<tr><td class="breakdown-name">' + rlEsc(r.name) + '</td><td class="breakdown-units">' + Number(r.units || 0).toLocaleString() + '</td><td class="breakdown-units">' + rlPct(r.units, totals.units) + '</td><td class="breakdown-keys">' + (r.keys || 0).toLocaleString() + '</td><td class="breakdown-keys">' + rlPct((r.keys || 0), totals.keys) + '</td></tr>'; }).join('');
        return '<div class="breakdown-panel"><div class="country-breakdown-title">Units &amp; keys by country</div><table class="country-breakdown-table"><thead><tr><th>Country</th><th>Units</th><th>% Units</th><th>Keys</th><th>% Keys</th></tr></thead><tbody>' + tr + '</tbody></table></div>';
    }
    function rlBuildOperator(properties) {
        var by = {};
        (properties || []).forEach(function (p) { var o = (p.operator_name || '').trim() || '(no operator)'; if (!by[o]) by[o] = { units: 0, keys: 0 }; by[o].units += 1; by[o].keys += toKeys(p); });
        var rows = Object.keys(by).map(function (n) { return { name: n, units: by[n].units, keys: by[n].keys }; }).sort(function (a, b) { return b.keys - a.keys; });
        if (!rows.length) return '<div class="breakdown-panel"><div class="affiliation-breakdown-title">Units &amp; keys by operator</div><p style="color:#888;font-size:12px;">No data</p></div>';
        var totals = rows.reduce(function (acc, r) { acc.units += r.units; acc.keys += (r.keys || 0); return acc; }, { units: 0, keys: 0 });
        var show = rows.length <= RL_MAX_BREAKDOWN_ROWS ? rows : rows.slice(0, RL_MAX_BREAKDOWN_ROWS - 1);
        var rest = rows.length > RL_MAX_BREAKDOWN_ROWS ? rows.slice(RL_MAX_BREAKDOWN_ROWS - 1) : [];
        if (rest.length) {
            var otherUnits = rest.reduce(function (a, r) { return a + r.units; }, 0);
            var otherKeys = rest.reduce(function (a, r) { return a + (r.keys || 0); }, 0);
            show.push({ name: 'Other (' + rest.length + ' more)', units: otherUnits, keys: otherKeys });
        }
        var tr = show.map(function (r) { return '<tr><td class="breakdown-name">' + rlEsc(r.name) + '</td><td class="breakdown-units">' + Number(r.units || 0).toLocaleString() + '</td><td class="breakdown-units">' + rlPct(r.units, totals.units) + '</td><td class="breakdown-keys">' + (r.keys || 0).toLocaleString() + '</td><td class="breakdown-keys">' + rlPct((r.keys || 0), totals.keys) + '</td></tr>'; }).join('');
        return '<div class="breakdown-panel"><div class="affiliation-breakdown-title">Units &amp; keys by operator</div><table class="affiliation-breakdown-table"><thead><tr><th>Operator</th><th>Units</th><th>% Units</th><th>Keys</th><th>% Keys</th></tr></thead><tbody>' + tr + '</tbody></table></div>';
    }
    function rlBuildChainFranchise(properties) {
        var list = properties || [];
        var chain = list.filter(function (p) { var ot = (p.operation_type || '').toString().trim().toLowerCase(); return ot === 'chain management'; });
        var franchise = list.filter(function (p) { var ot = (p.operation_type || '').toString().trim().toLowerCase(); return ot === 'franchise'; });
        var other = list.filter(function (p) { var ot = (p.operation_type || '').toString().trim().toLowerCase(); return ot !== 'chain management' && ot !== 'franchise'; });
        function panel(title, props, col) {
            var by = {};
            props.forEach(function (p) { var o = (p.operator_name || '').trim() || '(no operator)'; if (!by[o]) by[o] = { units: 0, keys: 0 }; by[o].units += 1; by[o].keys += toKeys(p); });
            var rows = Object.keys(by).map(function (n) { return { name: n, units: by[n].units, keys: by[n].keys }; }).sort(function (a, b) { return b.keys - a.keys; });
            if (!rows.length) return '<div class="breakdown-panel"><div class="affiliation-breakdown-title">' + rlEsc(title) + '</div><p style="color:#888;font-size:12px;">No data</p></div>';
            var totals = rows.reduce(function (acc, r) { acc.units += r.units; acc.keys += (r.keys || 0); return acc; }, { units: 0, keys: 0 });
            var show = rows.length <= RL_MAX_BREAKDOWN_ROWS ? rows : rows.slice(0, RL_MAX_BREAKDOWN_ROWS - 1);
            var rest = rows.length > RL_MAX_BREAKDOWN_ROWS ? rows.slice(RL_MAX_BREAKDOWN_ROWS - 1) : [];
            if (rest.length) {
                var otherUnits = rest.reduce(function (a, r) { return a + r.units; }, 0);
                var otherKeys = rest.reduce(function (a, r) { return a + (r.keys || 0); }, 0);
                show.push({ name: 'Other (' + rest.length + ' more)', units: otherUnits, keys: otherKeys });
            }
            var tr = show.map(function (r) { return '<tr><td class="breakdown-name">' + rlEsc(r.name) + '</td><td class="breakdown-units">' + Number(r.units || 0).toLocaleString() + '</td><td class="breakdown-units">' + rlPct(r.units, totals.units) + '</td><td class="breakdown-keys">' + (r.keys || 0).toLocaleString() + '</td><td class="breakdown-keys">' + rlPct((r.keys || 0), totals.keys) + '</td></tr>'; }).join('');
            return '<div class="breakdown-panel"><div class="affiliation-breakdown-title">' + rlEsc(title) + '</div><table class="affiliation-breakdown-table"><thead><tr><th>' + rlEsc(col) + '</th><th>Units</th><th>% Units</th><th>Keys</th><th>% Keys</th></tr></thead><tbody>' + tr + '</tbody></table></div>';
        }
        return panel('Chain Management', chain, 'Operator') + panel('Franchise', franchise, 'Operator') + panel('Independent', other, 'Operator');
    }
    function rlExpandContent(properties, rankBy) {
        if (!properties || !properties.length) return '<div class="breakdown-row-content"><div class="breakdown-panel"><p style="color:#888;font-size:12px;">No data</p></div></div>';
        var summaries = computeExpansionSummaries(properties);
        var summaryHtml = renderExpansionSummaryBlocks(summaries);
        var breakdownHtml = '';
        if (rankBy === 'brand') breakdownHtml = rlBuildOperator(properties) + rlBuildCountry(properties);
        else if (rankBy === 'parent_company') breakdownHtml = rlBuildChainFranchise(properties);
        else breakdownHtml = rlBuildOperator(properties) + rlBuildCountry(properties);
        return '<div class="breakdown-row-content">' + summaryHtml + breakdownHtml + '</div>';
    }
    function rlSetHeaders() {
        var head = document.getElementById('rlTableHead');
        if (!head || !head.querySelector('tr')) return;
        var rankBy = rlGetRankBy();
        var tr = head.querySelector('tr');
        var names = { brand: 'Brand / Affiliation', parent_company: 'Parent Company', region: 'Region', chain_scale: 'Chain Scale', status: 'Status' };
        var nameLabel = names[rankBy] || 'Name';
        if (rankBy === 'operator') tr.innerHTML = '<th class="rank-col" data-sort="total_keys"><span style="display:inline-flex;align-items:center;">#' + sortIndicatorHtml + '</span></th>' + rlTh('Operator', 'operator_name') + rlTh('Units', 'hotel_count') + rlTh('% Units', 'pct_units') + rlTh('Keys', 'total_keys') + rlTh('% Keys', 'pct_keys');
        else tr.innerHTML = '<th class="rank-col" data-sort="total_keys"><span style="display:inline-flex;align-items:center;">#' + sortIndicatorHtml + '</span></th>' + rlTh(nameLabel, 'name') + rlTh('Units', 'hotel_count') + rlTh('% Units', 'pct_units') + rlTh('Keys', 'total_keys') + rlTh('% Keys', 'pct_keys');
        tr.querySelectorAll('th[data-sort]').forEach(function (th) {
            th.classList.remove('sort-asc', 'sort-desc');
            if (th.getAttribute('data-sort') === rlSortColumn) th.classList.add(rlSortDir === 'asc' ? 'sort-asc' : 'sort-desc');
            th.onclick = function () { rlHandleSort(th.getAttribute('data-sort')); };
        });
    }
    function rlSortList(list, col, dir) {
        var blank = [], nonBlank = [];
        list.forEach(function (item) { if (rlIsBlankRow(item)) blank.push(item); else nonBlank.push(item); });
        var asc = dir === 'asc';
        nonBlank.sort(function (a, b) {
            var cmp = 0;
            if (col === 'operator_name' || col === 'name') cmp = (a.operator_name || a.name || '').toString().localeCompare((b.operator_name || b.name || '').toString());
            else if (col === 'hotel_count') cmp = (a.hotel_count || 0) - (b.hotel_count || 0);
            else if (col === 'total_keys') cmp = (a.total_keys || 0) - (b.total_keys || 0);
            else if (col === 'pct_units') cmp = (a.hotel_count || 0) - (b.hotel_count || 0);
            else if (col === 'pct_keys') cmp = (a.total_keys || 0) - (b.total_keys || 0);
            if (cmp === 0) cmp = (a.operator_name || a.name || '').toString().localeCompare((b.operator_name || b.name || '').toString());
            return asc ? cmp : -cmp;
        });
        return nonBlank.concat(blank);
    }
    function rlHandleSort(column) { rlSortColumn = column; rlSortDir = (column === 'operator_name' || column === 'name') ? 'asc' : 'desc'; var ranked = rlGetRankedList(); rlSetHeaders(); rlRenderList(rlSortList(ranked.list, column, rlSortDir)); }
    function rlRenderList(list) {
        rlLastList = list;
        var rankBy = rlGetRankBy(), body = document.getElementById('rlTableBody');
        if (!body) return;
        var totals = rlTotals(list);
        body.innerHTML = list.map(function (item, i) {
            var rank = i + 1, rawName = (item.name != null ? item.name : item.operator_name);
            var name = (rawName == null || String(rawName).trim() === '' || rlIsBlankRow(item)) ? '(blank)' : String(rawName).trim();
            var units = item.hotel_count != null ? item.hotel_count : 0, keys = item.total_keys != null ? item.total_keys : 0;
            var pctUnits = rlPct(units, totals.units);
            var pctKeys = rlPct(keys, totals.keys);
            var initial = name.charAt(0).toUpperCase(), nameCell = '<div class="operator-name-cell"><span class="operator-initial-thumb">' + rlEsc(initial) + '</span><span class="operator-name-text">' + rlEsc(name) + '</span></div>';
            var placeholder = '<div class="breakdown-row-content rl-expand-placeholder" data-expand-index="' + i + '" data-not-built="1"></div>';
            return '<tr class="simple-row-expand" data-row-index="' + i + '"><td class="rank-col"><span class="rank-number">' + rank + '</span><span class="row-expand-icon row-expand-icon-closed">▶</span><span class="row-expand-icon row-expand-icon-open">▼</span></td><td>' + nameCell + '</td><td>' + Number(units || 0).toLocaleString() + '</td><td>' + pctUnits + '</td><td>' + keys.toLocaleString() + '</td><td>' + pctKeys + '</td></tr><tr class="similar-row"><td colspan="6">' + placeholder + '</td></tr>';
        }).join('');
        body.querySelectorAll('.similar-row').forEach(function (row) { row.classList.add('rl-hidden'); });
        body.querySelectorAll('.simple-row-expand').forEach(function (row) {
            row.addEventListener('click', function () {
                var next = row.nextElementSibling;
                if (!next || !next.classList.contains('similar-row')) return;
                var td = next.querySelector('td');
                var placeholder = td && td.querySelector('[data-not-built]');
                var isCurrentlyHidden = next.classList.contains('rl-hidden');
                if (placeholder) {
                    var idx = parseInt(placeholder.getAttribute('data-expand-index'), 10);
                    next.classList.remove('rl-hidden');
                    row.classList.add('expanded');
                    var item = rlLastList[idx];
                    var rankByVal = rlGetRankBy();
                    setTimeout(function () {
                        if (!td.parentNode) return;
                        var expandHtml = item && item.properties ? rlExpandContent(item.properties, rankByVal) : '<div class="breakdown-row-content"><div class="breakdown-panel"><p style="color:#888;font-size:12px;">No data</p></div></div>';
                        td.innerHTML = expandHtml;
                    }, 0);
                } else {
                    if (isCurrentlyHidden) return;
                    var idx = parseInt(row.getAttribute('data-row-index'), 10);
                    next.classList.add('rl-hidden');
                    row.classList.remove('expanded');
                    if (!isNaN(idx)) {
                        var placeholderHtml = '<div class="breakdown-row-content rl-expand-placeholder" data-expand-index="' + idx + '" data-not-built="1"></div>';
                        var tdRef = td;
                        setTimeout(function () {
                            if (tdRef.parentNode) tdRef.innerHTML = placeholderHtml;
                        }, 0);
                    }
                }
            });
        });
    }
    function rlApplyView() {
        if (!rlLastData) return;
        var propNum = (rlLastData.total_properties != null ? rlLastData.total_properties : (rlLastData.properties || []).length);
        var rc = document.getElementById('rlPropertyCount'); if (rc) rc.textContent = Number(propNum || 0).toLocaleString();
        var rw = document.getElementById('rlFullDataset'); if (rw) rw.textContent = (rlLastData.total_in_census != null) ? ' (of ' + Number(rlLastData.total_in_census).toLocaleString() + ' in full dataset)' : '';
        var ranked = rlGetRankedList();
        var rk = document.getElementById('rlRankCount'); var rl = document.getElementById('rlRankLabel'); if (rk) rk.textContent = Number(ranked.list.length || 0).toLocaleString(); if (rl) rl.textContent = ranked.label;
        rlSetHeaders(); rlRenderList(ranked.list);
    }
    var rlCache = { paramsKey: '', data: null };
    window.runRankedList = function () {
        if (rlAbort) rlAbort.abort();
        rlAbort = new AbortController();
        var searchEl = document.getElementById('locationSearch');
        var pcEl = document.getElementById('parentCompanyFilter');
        var brandEl = document.getElementById('brandFilter');
        var statusEl = document.getElementById('statusFilter');
        var chainEl = document.getElementById('propertyTypeFilter');
        var regionEl = document.getElementById('regionFilter');
        var locTypeEl = document.getElementById('locationTypeFilter');
        var searchVal = (searchEl && searchEl.value) ? searchEl.value.trim() : '';
        var parentCompanyVal = (pcEl && pcEl.value) ? pcEl.value.trim() : '';
        var brandVal = (brandEl && brandEl.value) ? brandEl.value.trim() : '';
        var statusVal = (statusEl && statusEl.value) ? statusEl.value.trim() : '';
        var chainVal = (chainEl && chainEl.value) ? chainEl.value.trim() : '';
        var regionVal = (regionEl && regionEl.value) ? regionEl.value.trim() : '';
        var locTypeVal = (locTypeEl && locTypeEl.value) ? locTypeEl.value.trim() : '';

        var params = new URLSearchParams({
            search: searchVal,
            parentCompany: parentCompanyVal,
            brand: brandVal,
            status: statusVal,
            chainScale: chainVal,
            region: regionVal,
            locationType: locTypeVal,
            operationType: ''
        });
        var paramsKey = params.toString();
        if (rlCache.paramsKey === paramsKey && rlCache.data) {
            rlLastData = rlCache.data;
            document.getElementById('rlLoading').classList.add('rl-hidden');
            rlApplyView();
            document.getElementById('rlTableWrap').classList.remove('rl-hidden');
            var listLen = rlGetRankedList().list.length;
            if (!listLen) { document.getElementById('rlTableWrap').classList.add('rl-hidden'); document.getElementById('rlEmpty').classList.remove('rl-hidden'); document.getElementById('rlEmpty').innerHTML = '<p>No data. Adjust filters and try again.</p>'; } else document.getElementById('rlEmpty').classList.add('rl-hidden');
            return;
        }
        document.getElementById('rlLoading').classList.remove('rl-hidden');
        document.getElementById('rlTableWrap').classList.add('rl-hidden');
        document.getElementById('rlEmpty').classList.add('rl-hidden');
        var apiBase = (window.DEAL_CAPTURE_API_BASE_URL || '').replace(/\/$/, '');
        var opsUrl = (apiBase ? apiBase : '') + '/api/operators-by-brand-region?' + paramsKey;
        fetch(opsUrl, { signal: rlAbort.signal }).then(function (r) { return r.ok ? r.json() : r.json().then(function (j) { throw new Error(j.error || j.details || 'Request failed'); }); }).then(function (data) {
            rlLastData = data;
            rlCache = { paramsKey: paramsKey, data: data };
            document.getElementById('rlLoading').classList.add('rl-hidden');
            rlApplyView();
            document.getElementById('rlTableWrap').classList.remove('rl-hidden');
            var listLen = rlGetRankedList().list.length;
            if (!listLen) { document.getElementById('rlTableWrap').classList.add('rl-hidden'); document.getElementById('rlEmpty').classList.remove('rl-hidden'); document.getElementById('rlEmpty').innerHTML = '<p>No data. Adjust filters and try again.</p>'; } else document.getElementById('rlEmpty').classList.add('rl-hidden');
        }).catch(function (err) { if (err.name === 'AbortError') return; document.getElementById('rlLoading').classList.add('rl-hidden'); document.getElementById('rlEmpty').classList.remove('rl-hidden'); document.getElementById('rlEmpty').innerHTML = '<p>' + rlEsc(err.message || 'Failed to load') + '</p>'; });
        rlAbort = null;
    };
    document.getElementById('rlRankBy').addEventListener('change', function () { rlSortColumn = null; if (rlLastData) rlApplyView(); });
    function rlOnMainFilterChange() { rlLastData = null; if (document.querySelector('.radar-main-tab[data-tab="rankedlist"]') && document.querySelector('.radar-main-tab[data-tab="rankedlist"]').classList.contains('active')) runRankedList(); }
    ['locationSearch'].forEach(function (id) { var el = document.getElementById(id); if (el) el.addEventListener('input', (function () { var t; return function () { clearTimeout(t); t = setTimeout(rlOnMainFilterChange, 400); }; })()); });
    ['parentCompanyFilter', 'brandFilter', 'statusFilter', 'propertyTypeFilter', 'regionFilter', 'locationTypeFilter'].forEach(function (id) { var el = document.getElementById(id); if (el) el.addEventListener('change', rlOnMainFilterChange); });

    document.querySelectorAll('.radar-main-tab').forEach(function (tab) {
        tab.addEventListener('click', function () {
            document.querySelectorAll('.radar-main-tab').forEach(function (t) { t.classList.remove('active'); });
            tab.classList.add('active');
            var which = tab.getAttribute('data-tab');
            var radarPanel = document.getElementById('radarTabPanel');
            var listPanel = document.getElementById('rankedListTabPanel');
            if (which === 'radar') {
                radarPanel.classList.remove('rl-hidden');
                radarPanel.style.display = 'flex';
                listPanel.classList.add('rl-hidden');
            } else {
                radarPanel.classList.add('rl-hidden');
                radarPanel.style.display = 'none';
                listPanel.classList.remove('rl-hidden');
                if (rlLastData === null) runRankedList();
            }
        });
    });
})();

(function () {
    function moveRadarToggles() {
        var mount = document.getElementById('radarTogglesMount');
        var headerActions = document.querySelector('.mapping-header .header-actions');
        if (!mount || !headerActions) return;
        var controls = headerActions.querySelector('.overlay-controls');
        if (controls) mount.appendChild(controls);
        headerActions.remove();
    }
    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', moveRadarToggles);
    else moveRadarToggles();
})();

(function () {
    function setMapControlOffset() {
        var header = document.querySelector('.mapping-header');
        var mapEl = document.getElementById('map');
        if (!header || !mapEl) return;
        var headerOffset = Math.max(12, (header.offsetHeight || 0) + 12);
        var r = mapEl.getBoundingClientRect();
        var top = Math.max(headerOffset, Math.round(r.top) + 12);
        var left = Math.round(r.left) + 12;
        mapEl.style.setProperty('--radar-map-zoom-top', top + 'px');
        mapEl.style.setProperty('--radar-map-zoom-left', left + 'px');
    }
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', function () {
            setMapControlOffset();
            window.addEventListener('resize', setMapControlOffset);
            window.addEventListener('scroll', setMapControlOffset, { passive: true });
        });
    } else {
        setMapControlOffset();
        window.addEventListener('resize', setMapControlOffset);
        window.addEventListener('scroll', setMapControlOffset, { passive: true });
    }
})();

// resetView wrapper depends on brand-presence-mapping.js; attach after full page load.
window.addEventListener('load', function () {
    var orig = window.resetView;
    if (orig && !orig.__radarRankedWrapped) {
        var wrapped = function () {
            orig();
            var rankedTab = document.querySelector('.radar-main-tab[data-tab="rankedlist"]');
            if (rankedTab && rankedTab.classList.contains('active') && window.runRankedList) window.runRankedList();
        };
        wrapped.__radarRankedWrapped = true;
        window.resetView = wrapped;
    }
});
