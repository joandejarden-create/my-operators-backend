/**
 * User Management page – list, add, edit, delete users (Airtable User Management table).
 *
 * API: Frontend calls the app proxy (API_BASE). The server uses CONFIG (env) and forwards to
 *   https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${USER_MANAGEMENT_TABLE_ID}/...
 * Credentials (AIRTABLE_API_KEY, AIRTABLE_BASE_ID) are server-side only; never expose in this file.
 */
(function () {
    const API_BASE = '/api/user-management';
    const REGION_COLORS = { MEA: 'um-tag--orange', CALA: 'um-tag--green', EU: 'um-tag--purple', EUROPE: 'um-tag--purple', AP: 'um-tag--blue', AMERICAS: 'um-tag--amber', GLOBAL: 'um-tag--blue' };
    const CONTACT_VISIBILITY_OPTIONS = ['Show Contact', 'Hide Contact', 'Visible on Match', 'Admin Controlled'];
    const COUNTRY_OPTIONS_TOP = ['United States', 'Canada', 'United Kingdom', 'Mexico', 'Australia'];
    const COUNTRY_OPTIONS_ALL = [
        'Afghanistan', 'Albania', 'Algeria', 'Andorra', 'Angola', 'Antigua and Barbuda', 'Argentina', 'Armenia', 'Australia', 'Austria', 'Azerbaijan',
        'Bahamas', 'Bahrain', 'Bangladesh', 'Barbados', 'Belarus', 'Belgium', 'Belize', 'Benin', 'Bhutan', 'Bolivia', 'Bosnia and Herzegovina', 'Botswana', 'Brazil', 'Brunei', 'Bulgaria', 'Burkina Faso', 'Burundi',
        'Cambodia', 'Cameroon', 'Canada', 'Cape Verde', 'Central African Republic', 'Chad', 'Chile', 'China', 'Colombia', 'Comoros', 'Congo', 'Costa Rica', 'Croatia', 'Cuba', 'Cyprus', 'Czech Republic', 'Czechia',
        'Democratic Republic of the Congo', 'Denmark', 'Djibouti', 'Dominica', 'Dominican Republic',
        'East Timor', 'Ecuador', 'Egypt', 'El Salvador', 'Equatorial Guinea', 'Eritrea', 'Estonia', 'Eswatini', 'Ethiopia',
        'Fiji', 'Finland', 'France',
        'Gabon', 'Gambia', 'Georgia', 'Germany', 'Ghana', 'Greece', 'Grenada', 'Guatemala', 'Guinea', 'Guinea-Bissau', 'Guyana',
        'Haiti', 'Honduras', 'Hungary',
        'Iceland', 'India', 'Indonesia', 'Iran', 'Iraq', 'Ireland', 'Israel', 'Italy', 'Ivory Coast',
        'Jamaica', 'Japan', 'Jordan',
        'Kazakhstan', 'Kenya', 'Kiribati', 'Kosovo', 'Kuwait', 'Kyrgyzstan',
        'Laos', 'Latvia', 'Lebanon', 'Lesotho', 'Liberia', 'Libya', 'Liechtenstein', 'Lithuania', 'Luxembourg',
        'Madagascar', 'Malawi', 'Malaysia', 'Maldives', 'Mali', 'Malta', 'Marshall Islands', 'Mauritania', 'Mauritius', 'Mexico', 'Micronesia', 'Moldova', 'Monaco', 'Mongolia', 'Montenegro', 'Morocco', 'Mozambique', 'Myanmar',
        'Namibia', 'Nauru', 'Nepal', 'Netherlands', 'New Zealand', 'Nicaragua', 'Niger', 'Nigeria', 'North Korea', 'North Macedonia', 'Norway',
        'Oman',
        'Pakistan', 'Palau', 'Palestine', 'Panama', 'Papua New Guinea', 'Paraguay', 'Peru', 'Philippines', 'Poland', 'Portugal', 'Puerto Rico',
        'Qatar',
        'Republic of the Congo', 'Romania', 'Russia', 'Rwanda',
        'Saint Kitts and Nevis', 'Saint Lucia', 'Saint Vincent and the Grenadines', 'Samoa', 'San Marino', 'Sao Tome and Principe', 'Saudi Arabia', 'Senegal', 'Serbia', 'Seychelles', 'Sierra Leone', 'Singapore', 'Slovakia', 'Slovenia', 'Solomon Islands', 'Somalia', 'South Africa', 'South Korea', 'South Sudan', 'Spain', 'Sri Lanka', 'Sudan', 'Suriname', 'Sweden', 'Switzerland', 'Syria',
        'Taiwan', 'Tajikistan', 'Tanzania', 'Thailand', 'Togo', 'Tonga', 'Trinidad and Tobago', 'Tunisia', 'Turkey', 'Turkmenistan', 'Tuvalu',
        'Uganda', 'Ukraine', 'United Arab Emirates', 'United Kingdom', 'United States', 'Uruguay', 'Uzbekistan',
        'Vanuatu', 'Vatican City', 'Venezuela', 'Vietnam',
        'Yemen',
        'Zambia', 'Zimbabwe'
    ];
    const COUNTRY_OPTIONS = [...COUNTRY_OPTIONS_TOP, ...COUNTRY_OPTIONS_ALL.filter(function (c) { return COUNTRY_OPTIONS_TOP.indexOf(c) === -1; })];
    // Map Partner Directory long names to same codes used in region filter (Partner Directory)
    function regionToPartnerDirectoryCode(r) {
        const raw = (typeof r === 'string' ? r : (r && typeof r === 'object' && r.name != null ? r.name : String(r || '')));
        const u = raw.trim().replace(/\s+/g, ' ').toUpperCase();
        if (!u) return '';
        if (u.indexOf('GLOBAL') >= 0) return 'GLOBAL';
        if (u.indexOf('CARIBBEAN') >= 0 || u.indexOf('LATIN') >= 0 || u === 'CALA') return 'CALA';
        if (u.indexOf('EUROPE') >= 0 || u === 'EU') return 'EUROPE';
        if (u.indexOf('MIDDLE') >= 0 && u.indexOf('EAST') >= 0 || u.indexOf('MEA') >= 0 || u.indexOf('AFRICA') >= 0) return 'MEA';
        if (u.indexOf('ASIA') >= 0 && u.indexOf('PACIFIC') >= 0 || u === 'AP') return 'AP';
        if (u.indexOf('AMERICAS') >= 0 || (u.indexOf('AMERICA') >= 0 && u.indexOf('LATIN') < 0 && u.indexOf('CARIBBEAN') < 0)) return 'AMERICAS';
        return '';
    }
    // Display labels for table (same as Partner Directory filter labels)
    const REGION_DISPLAY_LABELS = { GLOBAL: 'Global', AMERICAS: 'Americas', CALA: 'CALA', EUROPE: 'Europe', MEA: 'MEA', AP: 'AP' };
    const ALL_FIVE_REGION_CODES = ['AMERICAS', 'CALA', 'EUROPE', 'MEA', 'AP'];

    let users = [];
    let filteredUsers = [];
    let companies = [];
    let sortColumn = null;
    let sortDirection = 'asc';

    const el = (id) => document.getElementById(id);
    const tableBody = el('umTableBody');
    const addUserBtn = el('addUserBtn');
    const bulkActionsBtn = el('bulkActionsBtn');
    const bulkActionsMenu = el('bulkActionsMenu');
    const selectAll = el('selectAll');
    const userModal = el('userModal');
    const userModalTitle = el('userModalTitle');
    const userModalClose = el('userModalClose');
    const userModalCancel = el('userModalCancel');
    const userForm = el('userForm');
    const userRecordId = el('userRecordId');
    const userFormSubmit = el('userFormSubmit');
    const umError = el('umError');
    const successMessage = el('successMessage');
    const errorMessage = el('errorMessage');

    let toastHideTimeout = null;
    let toastHideTimeout2 = null;
    const TOAST_DURATION_MS = 5000;
    const TOAST_SLIDE_MS = 300;

    function showToast(message, type) {
        const el = type === 'error' ? errorMessage : successMessage;
        if (!el) return;
        const msgNode = el.querySelector('.toast-message');
        if (msgNode) msgNode.textContent = message || '';
        if (toastHideTimeout) { clearTimeout(toastHideTimeout); toastHideTimeout = null; }
        if (toastHideTimeout2) { clearTimeout(toastHideTimeout2); toastHideTimeout2 = null; }
        const other = el === successMessage ? errorMessage : successMessage;
        if (other) { other.classList.remove('show'); other.style.display = 'none'; }
        if (el === successMessage) {
            const bar = successMessage.querySelector('.toast-progress-bar');
            if (bar) { bar.style.width = '0%'; bar.classList.remove('animate'); }
        }
        el.style.display = 'block';
        el.classList.remove('show');
        el.offsetHeight;
        setTimeout(function () {
            el.classList.add('show');
            if (el === successMessage) {
                const bar = successMessage.querySelector('.toast-progress-bar');
                if (bar) { bar.offsetHeight; bar.classList.add('animate'); }
            }
        }, 20);
        toastHideTimeout = setTimeout(function () {
            el.classList.remove('show');
            toastHideTimeout = null;
            if (el === successMessage) {
                const bar = successMessage.querySelector('.toast-progress-bar');
                if (bar) { bar.classList.remove('animate'); bar.style.width = '0%'; }
            }
            toastHideTimeout2 = setTimeout(function () {
                el.style.display = 'none';
                toastHideTimeout2 = null;
            }, TOAST_SLIDE_MS);
        }, TOAST_DURATION_MS);
    }

    function showError(msg) {
        if (!umError) return;
        umError.textContent = msg;
        umError.style.display = msg ? 'block' : 'none';
    }

    function getSelectedIds() {
        return Array.from(document.querySelectorAll('.um-row-check:checked')).map(cb => cb.value);
    }

    function regionTagClass(region) {
        const r = (region || '').toUpperCase().trim();
        return REGION_COLORS[r] || 'um-tag--blue';
    }

    function renderRegionFocus(regionFocus) {
        if (!regionFocus || (Array.isArray(regionFocus) && regionFocus.length === 0)) return '—';
        const arr = Array.isArray(regionFocus) ? regionFocus : [regionFocus];
        const codes = [];
        const seen = new Set();
        for (let i = 0; i < arr.length; i++) {
            const code = regionToPartnerDirectoryCode(arr[i]);
            if (code && !seen.has(code)) { seen.add(code); codes.push(code); }
        }
        if (codes.length === 0) return '—';
        if (codes.indexOf('GLOBAL') >= 0) {
            return '<span class="um-tag ' + regionTagClass('GLOBAL') + '">' + escapeHtml('Global') + '</span>';
        }
        const fiveSet = new Set(ALL_FIVE_REGION_CODES);
        const hasAllFive = fiveSet.size === codes.length && codes.every(function (c) { return fiveSet.has(c); });
        if (hasAllFive) {
            return '<span class="um-tag ' + regionTagClass('GLOBAL') + '">' + escapeHtml('Global') + '</span>';
        }
        return codes.map(function (code) {
            const label = REGION_DISPLAY_LABELS[code] || code;
            return '<span class="um-tag ' + regionTagClass(code) + '">' + escapeHtml(label) + '</span>';
        }).join(' ');
    }

    function escapeHtml(s) {
        const div = document.createElement('div');
        div.textContent = s;
        return div.innerHTML;
    }

    function userHasRegion(u, regionCode) {
        const arr = Array.isArray(u.regionFocus) ? u.regionFocus : (typeof u.regionFocus === 'string' ? (u.regionFocus || '').split(',').map(s => s.trim()).filter(Boolean) : []);
        const normalized = arr.map(r => regionToPartnerDirectoryCode(r));
        return normalized.indexOf(regionCode) !== -1;
    }

    function getFilteredUsers() {
        const search = (el('umSearchInput') && el('umSearchInput').value || '').trim().toLowerCase();
        const role = (el('umRoleFilter') && el('umRoleFilter').value || '').trim();
        const region = (el('umRegionFilter') && el('umRegionFilter').value || '').trim();
        const visibility = (el('umVisibilityFilter') && el('umVisibilityFilter').value || '').trim();
        let list = users;
        if (search) {
            list = list.filter(u => {
                const text = [u.firstName, u.lastName, u.companyTitle, u.companyEmail, u.platformRole, u.contactVisibility].filter(Boolean).join(' ').toLowerCase();
                return text.indexOf(search) !== -1;
            });
        }
        if (role) list = list.filter(u => (u.platformRole || '').trim() === role);
        if (region) list = list.filter(u => userHasRegion(u, region));
        if (visibility) {
            const visLower = visibility.toLowerCase();
            list = list.filter(u => (u.contactVisibility || '').trim().toLowerCase() === visLower);
        }
        return list;
    }

    function applyFilters() {
        filteredUsers = getFilteredUsers();
        if (sortColumn) {
            filteredUsers.sort((a, b) => {
                const aVal = (a[sortColumn] || '').toString().trim().toLowerCase();
                const bVal = (b[sortColumn] || '').toString().trim().toLowerCase();
                if (aVal < bVal) return sortDirection === 'asc' ? -1 : 1;
                if (aVal > bVal) return sortDirection === 'asc' ? 1 : -1;
                return 0;
            });
        }
        const search = (el('umSearchInput') && el('umSearchInput').value || '').trim();
        const role = (el('umRoleFilter') && el('umRoleFilter').value || '').trim();
        const region = (el('umRegionFilter') && el('umRegionFilter').value || '').trim();
        const visibility = (el('umVisibilityFilter') && el('umVisibilityFilter').value || '').trim();
        const activeCount = (search ? 1 : 0) + (role ? 1 : 0) + (region ? 1 : 0) + (visibility ? 1 : 0);
        const badge = el('umFilterCountBadge');
        if (badge) {
            badge.textContent = activeCount;
            badge.style.display = activeCount > 0 ? 'inline-flex' : 'none';
        }
        renderTable();
        updateSortHeaders();
    }

    function updateSortHeaders() {
        const headers = document.querySelectorAll('.um-table th[data-sort]');
        headers.forEach(th => {
            th.classList.remove('sort-asc', 'sort-desc');
            if (th.getAttribute('data-sort') === sortColumn) th.classList.add(sortDirection === 'asc' ? 'sort-asc' : 'sort-desc');
        });
    }

    function renderTable() {
        if (!tableBody) return;
        if (users.length === 0) {
            tableBody.innerHTML = '<tr><td colspan="11" class="um-empty">No users yet. Click Add User to add teammates.</td></tr>';
            if (bulkActionsBtn) bulkActionsBtn.disabled = true;
            return;
        }
        if (filteredUsers.length === 0) {
            tableBody.innerHTML = '<tr><td colspan="11" class="um-empty">No users match the current filters. Try clearing filters or a different search.</td></tr>';
            if (bulkActionsBtn) bulkActionsBtn.disabled = true;
            return;
        }
        tableBody.innerHTML = filteredUsers.map(u => {
            const fullName = [u.firstName, u.lastName].filter(Boolean).join(' ') || '—';
            const role = u.platformRole || '—';
            const visibility = (u.contactVisibility || '').trim() || 'Show Contact';
            const visOpts = CONTACT_VISIBILITY_OPTIONS.map(opt => {
                const sel = (opt === visibility) ? ' selected' : '';
                return '<option value="' + escapeHtml(opt) + '"' + sel + '>' + escapeHtml(opt) + '</option>';
            }).join('');
            const visibilityCell = '<select class="visibility-select" data-record-id="' + escapeHtml(u.id) + '" data-user-name="' + escapeHtml(fullName) + '" aria-label="Contact visibility for ' + escapeHtml(fullName) + '">' + visOpts + '</select>';
            return `
<tr data-record-id="${escapeHtml(u.id)}">
  <td class="col-check"><input type="checkbox" class="um-row-check" value="${escapeHtml(u.id)}" aria-label="Select ${escapeHtml(fullName)}"></td>
  <td>${escapeHtml(u.firstName || '')}</td>
  <td>${escapeHtml(u.lastName || '')}</td>
  <td>${escapeHtml(u.companyTitle || '')}</td>
  <td>${escapeHtml(u.phoneNumber || '')}</td>
  <td>${escapeHtml(u.companyEmail || '')}</td>
  <td>${escapeHtml(u.country || '')}</td>
  <td>${escapeHtml(role)}</td>
  <td>${renderRegionFocus(u.regionFocus)}</td>
  <td>${visibilityCell}</td>
  <td class="um-actions-cell">
    <button type="button" class="um-edit" data-record-id="${escapeHtml(u.id)}" title="Edit">✎</button>
    <button type="button" class="um-delete" data-record-id="${escapeHtml(u.id)}" title="Remove">🗑</button>
  </td>
</tr>`;
        }).join('');
        bulkActionsBtn.disabled = false;
        selectAll.checked = false;
        selectAll.indeterminate = false;
        tableBody.querySelectorAll('.um-edit').forEach(btn => btn.addEventListener('click', () => openEdit(btn.getAttribute('data-record-id'))));
        tableBody.querySelectorAll('.um-delete').forEach(btn => btn.addEventListener('click', () => confirmDelete(btn.getAttribute('data-record-id'))));
        tableBody.querySelectorAll('.um-row-check').forEach(cb => cb.addEventListener('change', updateBulkState));
        tableBody.querySelectorAll('.visibility-select').forEach(sel => sel.addEventListener('change', onVisibilityChange));
        if (el('selectAll')) el('selectAll').onchange = toggleSelectAll;
    }

    async function onVisibilityChange(e) {
        const sel = e.target.closest('.visibility-select');
        if (!sel) return;
        const recordId = sel.getAttribute('data-record-id');
        const userName = sel.getAttribute('data-user-name') || 'User';
        const newVisibility = (sel.value || '').trim();
        if (!recordId || !newVisibility) return;
        try {
            const res = await fetch(API_BASE + '/' + encodeURIComponent(recordId), {
                method: 'PATCH',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ contactVisibility: newVisibility }),
            });
            const data = await res.json().catch(() => ({}));
            if (!res.ok) throw new Error(data.details || data.error || res.statusText);
            const u = users.find(x => x.id === recordId);
            if (u) u.contactVisibility = newVisibility;
            applyFilters();
            showToast('Contact visibility updated for ' + userName + '.');
        } catch (err) {
            showToast(err.message || 'Could not update visibility.', 'error');
            applyFilters();
        }
    }

    function updateBulkState() {
        const ids = getSelectedIds();
        const all = tableBody.querySelectorAll('.um-row-check');
        if (!selectAll) return;
        selectAll.checked = all.length > 0 && ids.length === all.length;
        selectAll.indeterminate = ids.length > 0 && ids.length < all.length;
        bulkActionsBtn.disabled = ids.length === 0;
    }

    function toggleSelectAll() {
        const checkboxes = tableBody.querySelectorAll('.um-row-check');
        checkboxes.forEach(cb => { cb.checked = selectAll.checked; });
        bulkActionsBtn.disabled = !selectAll.checked;
        selectAll.indeterminate = false;
    }

    function openAdd() {
        userRecordId.value = '';
        userModalTitle.textContent = 'Add User';
        userForm.reset();
        userForm.querySelector('[name="platformRole"]').value = 'Company Admin';
        userForm.querySelector('[name="contactVisibility"]').value = 'Show Contact';
        userModal.classList.add('open');
    }

    function openEdit(recordId) {
        const u = users.find(x => x.id === recordId);
        if (!u) return;
        userRecordId.value = u.id;
        userModalTitle.textContent = 'Edit User';
        el('userFirstName').value = u.firstName || '';
        el('userLastName').value = u.lastName || '';
        el('userCompanyTitle').value = u.companyTitle || '';
        el('userPhone').value = u.phoneNumber || '';
        el('userEmail').value = u.companyEmail || '';
        el('userCompany').value = u.companyProfileId || '';
        el('userPlatformRole').value = u.platformRole || 'Company Admin';
        const regionSel = el('userRegionFocus');
        if (regionSel && regionSel.multiple) {
            const raw = Array.isArray(u.regionFocus) ? u.regionFocus : (typeof u.regionFocus === 'string' ? u.regionFocus.split(',').map(s => s.trim()).filter(Boolean) : []);
            const codes = [...new Set(raw.map(regionToPartnerDirectoryCode).filter(Boolean))];
            const isGlobal = codes.indexOf('GLOBAL') >= 0 || (codes.length >= 5 && ALL_FIVE_REGION_CODES.every(function (r) { return codes.indexOf(r) >= 0; }));
            Array.from(regionSel.options).forEach(function (opt) {
                opt.selected = isGlobal ? (opt.value === 'GLOBAL') : (codes.indexOf(opt.value) >= 0);
            });
        } else if (regionSel) {
            regionSel.value = Array.isArray(u.regionFocus) ? u.regionFocus.join(', ') : (u.regionFocus || '');
        }
        el('userContactVisibility').value = u.contactVisibility || 'Show Contact';
        const dealVal = (u.dealAccess || 'Full').trim();
        const docVal = (u.documentAccess || 'Full').trim();
        el('userDealAccess').value = (dealVal === 'View only' ? 'View Only' : dealVal) || 'Full';
        el('userDocumentAccess').value = (docVal === 'View only' ? 'View Only' : docVal) || 'Full';
        const countrySel = el('userCountry');
        if (countrySel) {
            const countryVal = (u.country || '').trim();
            if (countryVal && !COUNTRY_OPTIONS.includes(countryVal)) {
                const opt = document.createElement('option');
                opt.value = countryVal;
                opt.textContent = countryVal;
                countrySel.appendChild(opt);
            }
            countrySel.value = countryVal;
        }
        userModal.classList.add('open');
    }

    function closeModal() {
        userModal.classList.remove('open');
    }

    async function loadUsers() {
        showError('');
        tableBody.innerHTML = '<tr><td colspan="10" class="um-loading">Loading users…</td></tr>';
        try {
            const res = await fetch(API_BASE);
            if (!res.ok) throw new Error(res.statusText || 'Failed to load users');
            const data = await res.json();
            users = data.users || [];
            applyFilters();
        } catch (e) {
            console.error(e);
            showError('Could not load users. ' + (e.message || 'Please try again.'));
            tableBody.innerHTML = '<tr><td colspan="11" class="um-empty">Error loading users.</td></tr>';
        }
    }

    async function loadCompanies() {
        try {
            const res = await fetch(API_BASE + '/companies');
            if (!res.ok) return;
            const data = await res.json();
            companies = data.companies || [];
            const sel = el('userCompany');
            if (!sel) return;
            const firstOpt = sel.querySelector('option');
            sel.innerHTML = '';
            if (firstOpt) sel.appendChild(firstOpt);
            companies.forEach(c => {
                const opt = document.createElement('option');
                opt.value = c.id;
                opt.textContent = c.name || c.id;
                sel.appendChild(opt);
            });
        } catch (e) {
            console.warn('Could not load companies:', e);
        }
    }

    function getFormPayload() {
        const form = userForm;
        let regionFocus = [];
        if (form.regionFocus && form.regionFocus.multiple) {
            regionFocus = Array.from(form.regionFocus.selectedOptions || []).map(o => o.value).filter(Boolean);
        } else if (form.regionFocus) {
            const v = (form.regionFocus.value || '').trim();
            if (v) regionFocus = v.split(',').map(s => s.trim()).filter(Boolean);
        }
        if (regionFocus.indexOf('GLOBAL') >= 0) regionFocus = ['GLOBAL'];
        else if (regionFocus.length >= 5) {
            const set = new Set(regionFocus);
            if (ALL_FIVE_REGION_CODES.every(function (r) { return set.has(r); })) regionFocus = ['GLOBAL'];
        }
        return {
            firstName: (form.firstName && form.firstName.value || '').trim(),
            lastName: (form.lastName && form.lastName.value || '').trim(),
            companyTitle: (form.companyTitle && form.companyTitle.value || '').trim(),
            phoneNumber: (form.phoneNumber && form.phoneNumber.value || '').trim(),
            companyEmail: (form.companyEmail && form.companyEmail.value || '').trim(),
            companyProfileId: (form.companyProfileId && form.companyProfileId.value || '').trim() || undefined,
            platformRole: (form.platformRole && form.platformRole.value || '').trim(),
            contactVisibility: (form.contactVisibility && form.contactVisibility.value || '').trim(),
            regionFocus: regionFocus.length ? regionFocus : undefined,
            dealAccess: (form.dealAccess && form.dealAccess.value || '').trim() || undefined,
            documentAccess: (form.documentAccess && form.documentAccess.value || '').trim() || undefined,
            country: (form.country && form.country.value || '').trim(),
        };
    }

    async function submitForm(e) {
        e.preventDefault();
        const id = userRecordId.value.trim();
        const payload = getFormPayload();
        if (!payload.firstName || !payload.lastName || !payload.companyEmail) {
            showToast('First name, last name, and company email are required.', 'error');
            return;
        }
        if (!payload.companyProfileId) {
            showToast('Company is required.', 'error');
            return;
        }
        if (!payload.country) {
            showToast('Based (Country) is required.', 'error');
            return;
        }
        userFormSubmit.disabled = true;
        try {
            if (id) {
                const res = await fetch(API_BASE + '/' + encodeURIComponent(id), {
                    method: 'PATCH',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(payload),
                });
                const data = await res.json().catch(() => ({}));
                if (!res.ok) throw new Error(data.details || data.error || res.statusText);
                const idx = users.findIndex(x => x.id === id);
                if (idx !== -1) users[idx] = { ...users[idx], ...data.user };
                showToast('User updated successfully.');
            } else {
                const res = await fetch(API_BASE, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(payload),
                });
                const data = await res.json().catch(() => ({}));
                if (!res.ok) throw new Error(data.details || data.error || res.statusText);
                if (data.user) users.push(data.user);
                showToast('User added successfully.');
            }
            applyFilters();
            closeModal();
        } catch (err) {
            showToast(err.message || 'Request failed.', 'error');
        } finally {
            userFormSubmit.disabled = false;
        }
    }

    function confirmDelete(recordId) {
        const u = users.find(x => x.id === recordId);
        const name = u ? [u.firstName, u.lastName].filter(Boolean).join(' ') || 'User' : 'User';
        if (!confirm(`Remove ${name} from User Management? This cannot be undone.`)) return;
        doDelete(recordId);
    }

    async function doDelete(recordId) {
        try {
            const res = await fetch(API_BASE + '/' + encodeURIComponent(recordId), { method: 'DELETE' });
            const data = await res.json().catch(() => ({}));
            if (!res.ok) throw new Error(data.details || data.error || res.statusText);
            users = users.filter(x => x.id !== recordId);
            applyFilters();
            showToast('User removed.');
        } catch (err) {
            showToast(err.message || 'Could not remove user.', 'error');
        }
    }

    async function bulkDelete() {
        const ids = getSelectedIds();
        if (ids.length === 0) return;
        if (!confirm(`Remove ${ids.length} selected user(s)? This cannot be undone.`)) return;
        try {
            const res = await fetch(API_BASE + '/bulk-delete', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ recordIds: ids }),
            });
            const data = await res.json().catch(() => ({}));
            if (!res.ok) throw new Error(data.error || data.details || res.statusText);
            users = users.filter(x => !ids.includes(x.id));
            applyFilters();
            showToast(data.deleted + ' user(s) removed.');
        } catch (err) {
            showToast(err.message || 'Bulk remove failed.', 'error');
        }
    }

    function setupBulkDropdown() {
        if (!bulkActionsBtn || !bulkActionsMenu) return;
        bulkActionsBtn.addEventListener('click', (e) => {
            e.stopPropagation();
            bulkActionsMenu.classList.toggle('open');
        });
        document.addEventListener('click', () => bulkActionsMenu.classList.remove('open'));
        bulkActionsMenu.querySelector('[data-action="bulk-delete"]').addEventListener('click', () => {
            bulkActionsMenu.classList.remove('open');
            bulkDelete();
        });
    }

    function handleSort(column) {
        if (sortColumn === column) sortDirection = sortDirection === 'asc' ? 'desc' : 'asc';
        else { sortColumn = column; sortDirection = 'asc'; }
        applyFilters();
    }

    function setupSort() {
        document.querySelectorAll('.um-table th[data-sort]').forEach(th => {
            th.removeEventListener('click', th._sortHandler);
            th._sortHandler = () => handleSort(th.getAttribute('data-sort'));
            th.addEventListener('click', th._sortHandler);
        });
        updateSortHeaders();
    }

    function setupFilters() {
        const searchInput = el('umSearchInput');
        const roleFilter = el('umRoleFilter');
        const regionFilter = el('umRegionFilter');
        const visibilityFilter = el('umVisibilityFilter');
        const clearBtn = el('umClearFilters');
        if (searchInput) searchInput.addEventListener('input', applyFilters);
        if (roleFilter) roleFilter.addEventListener('change', applyFilters);
        if (regionFilter) regionFilter.addEventListener('change', applyFilters);
        if (visibilityFilter) visibilityFilter.addEventListener('change', applyFilters);
        if (clearBtn) {
            clearBtn.addEventListener('click', () => {
                if (searchInput) searchInput.value = '';
                if (roleFilter) roleFilter.value = '';
                if (regionFilter) regionFilter.value = '';
                if (visibilityFilter) visibilityFilter.value = '';
                applyFilters();
            });
        }
    }

    function syncRegionGlobalExclusive() {
        const regionSel = el('userRegionFocus');
        if (!regionSel || !regionSel.multiple) return;
        const selected = Array.from(regionSel.selectedOptions || []).map(function (o) { return o.value; });
        const hasGlobal = selected.indexOf('GLOBAL') >= 0;
        if (hasGlobal && selected.length > 1) {
            Array.from(regionSel.options).forEach(function (opt) { opt.selected = opt.value !== 'GLOBAL'; });
        } else if (hasGlobal) {
            Array.from(regionSel.options).forEach(function (opt) { opt.selected = opt.value === 'GLOBAL'; });
        }
    }

    userForm.addEventListener('submit', submitForm);
    addUserBtn.addEventListener('click', openAdd);
    userModalClose.addEventListener('click', closeModal);
    userModalCancel.addEventListener('click', closeModal);
    userModal.addEventListener('click', (e) => { if (e.target === userModal) closeModal(); });
    if (el('userRegionFocus')) el('userRegionFocus').addEventListener('change', syncRegionGlobalExclusive);

    function populateCountrySelect() {
        const sel = el('userCountry');
        if (!sel || sel.options.length > 1) return;
        COUNTRY_OPTIONS.forEach(function (name) {
            const opt = document.createElement('option');
            opt.value = name;
            opt.textContent = name;
            sel.appendChild(opt);
        });
    }

    setupBulkDropdown();
    setupFilters();
    populateCountrySelect();
    loadCompanies();
    loadUsers().then(() => setupSort());
})();
