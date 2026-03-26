// Brand Review Dashboard JavaScript
class BrandReviewDashboard {
    constructor() {
        this.deals = [];
        this.filteredDeals = [];
        this.currentPage = 1;
        this.itemsPerPage = 10;
        this.currentSort = { field: 'submitDate', direction: 'desc' };
        this.selectedDeals = new Set();
        this.filters = {};
        
        this.init();
    }

    async init() {
        await this.loadDeals();
        this.setupEventListeners();
        this.renderDeals();
        this.updateTabCounts();
    }

    async loadDeals() {
        try {
            showLoading(true);
            
            // For now, we'll use mock data. In production, this would call your API
            const response = await fetch('/api/brand-review/deals', {
                method: 'GET',
                headers: {
                    'Content-Type': 'application/json',
                    // Add authentication headers here
                }
            });

            if (response.ok) {
                const data = await response.json();
                this.deals = data.deals || [];
            } else {
                // Fallback to mock data for development
                this.deals = this.getMockDeals();
            }
            
            this.applyFilters();
        } catch (error) {
            console.error('Error loading deals:', error);
            // Use mock data if API fails
            this.deals = this.getMockDeals();
            this.applyFilters();
        } finally {
            showLoading(false);
        }
    }

    getMockDeals() {
        return [
            {
                id: '1',
                propertyName: '90-key resort in Medellín seeking upper upscale flag.',
                brandMatch: 'Autograph Collection',
                ownerName: 'Andrea Serrano',
                ownerTitle: 'Director of Acquisitions',
                ownerCompany: 'Serrano Hotel Group',
                ownerEmail: 'andrea.serrano@serranohotels.com',
                ownerPhone: '+57-4-555-0123',
                propertyType: 'Upper Upscale',
                rooms: 90,
                budget: '$25,000,000',
                country: 'Colombia',
                city: 'Medellín',
                stage: 'Pre-Construction',
                submitDate: '2024-01-15',
                status: 'new',
                matchScore: 92,
                respondTime: 'Very Fast - Frequently',
                respondTimeColor: 'green',
                description: 'Modern convention center hotel in downtown Chicago seeking upscale brand partnership.',
                timeline: 'Q3 2024 Opening',
                brandExperience: 'Previous experience with Marriott and Hilton brands',
                specialConsiderations: 'Green building certification required'
            },
            {
                id: '2',
                propertyName: '155-key Airport Hotel in Lima seeking upscale flag.',
                brandMatch: 'Tribute Collection',
                ownerName: 'Carlos Mendes',
                ownerTitle: 'Managing Partner',
                ownerCompany: 'Altura Hospitality Capital',
                ownerEmail: 'carlos.mendes@alturahospitality.com',
                ownerPhone: '+51-1-555-1234',
                propertyType: 'Upscale',
                rooms: 155,
                budget: '$35,000,000',
                country: 'Peru',
                city: 'Lima',
                stage: 'Planning',
                submitDate: '2024-01-14',
                status: 'new',
                matchScore: 85,
                respondTime: 'Very Fast - Frequently',
                respondTimeColor: 'green',
                description: 'Luxury beachfront resort in Manuel Antonio with spa and wellness facilities.',
                timeline: 'Q4 2024 Opening',
                brandExperience: 'First hotel project, strong local partnerships',
                specialConsiderations: 'Eco-friendly design and sustainability focus'
            },
            {
                id: '3',
                propertyName: '210 key All-inclusive in Dom. Republic seeking Upper Upscale flag.',
                brandMatch: 'W Hotels',
                ownerName: 'Melissa Tran',
                ownerTitle: 'Partner, Hotel Investments',
                ownerCompany: 'Tran Capital Group',
                ownerEmail: 'melissa.tran@trancapital.com',
                ownerPhone: '+1-555-0456',
                propertyType: 'Upper Upscale',
                rooms: 210,
                budget: '$45,000,000',
                country: 'Dominican Republic',
                city: 'Punta Cana',
                stage: 'Construction',
                submitDate: '2024-01-13',
                status: 'deal-received',
                matchScore: 81,
                respondTime: 'Responsive - Occasionally',
                respondTimeColor: 'orange',
                description: 'Modern business hotel adjacent to Hartsfield-Jackson Airport.',
                timeline: 'Q2 2024 Opening',
                brandExperience: '10+ years in hospitality development',
                specialConsiderations: '24/7 operations, conference facilities'
            },
            {
                id: '4',
                propertyName: '130-key full-service in Bogotá seeking Luxury flag.',
                brandMatch: 'AC by Marriott',
                ownerName: 'Daniel Navarro',
                ownerTitle: 'Real Estate Director',
                ownerCompany: 'Blue Arch Capital',
                ownerEmail: 'daniel.navarro@bluearchcapital.com',
                ownerPhone: '+57-1-555-5678',
                propertyType: 'Luxury',
                rooms: 130,
                budget: '$40,000,000',
                country: 'Colombia',
                city: 'Bogotá',
                stage: 'Renovation',
                submitDate: '2024-01-12',
                status: 'viewed-by-brand',
                matchScore: 75,
                respondTime: 'Stalled - Rarely',
                respondTimeColor: 'red',
                description: 'Historic building conversion to luxury boutique hotel in Edinburgh Old Town.',
                timeline: 'Q1 2025 Opening',
                brandExperience: 'Specialized in heritage property conversions',
                specialConsiderations: 'Historic preservation requirements, premium positioning'
            },
            {
                id: '5',
                propertyName: '100 key select-service in Playa del Carmen seeking midscale flag.',
                brandMatch: 'Renissance Hotels',
                ownerName: 'David Rosenthal',
                ownerTitle: 'Principal',
                ownerCompany: 'Rosenthal Lodging Group',
                ownerEmail: 'david.rosenthal@rosenthallodging.com',
                ownerPhone: '+52-984-555-0789',
                propertyType: 'Midscale',
                rooms: 100,
                budget: '$18,000,000',
                country: 'Mexico',
                city: 'Playa del Carmen',
                stage: 'Pre-Construction',
                submitDate: '2024-01-11',
                status: 'deal-received',
                matchScore: 73,
                respondTime: 'Lightning Fast - Occasionally',
                respondTimeColor: 'green',
                description: 'Extended stay hotel targeting corporate relocations and long-term guests.',
                timeline: 'Q4 2024 Opening',
                brandExperience: 'Focus on extended stay and corporate housing',
                specialConsiderations: 'Full kitchen amenities, business center, fitness facility'
            },
            {
                id: '6',
                propertyName: '70 key Lifestyle / Boutique in Mexico seeking upscale flag.',
                brandMatch: 'Tribute Collection',
                ownerName: 'Thomas "TJ" Bryant',
                ownerTitle: 'Co-Founder & CEO',
                ownerCompany: 'Elevate Stay Ventures',
                ownerEmail: 'tj.bryant@elevatestay.com',
                ownerPhone: '+52-55-555-0321',
                propertyType: 'Upscale',
                rooms: 70,
                budget: '$15,000,000',
                country: 'Mexico',
                city: 'Mexico City',
                stage: 'Concept',
                submitDate: '2024-01-10',
                status: 'deal-received',
                matchScore: 45,
                respondTime: 'Unresponsive - Rarely',
                respondTimeColor: 'red',
                description: 'Luxury mountain lodge with ski-in/ski-out access and spa facilities.',
                timeline: 'Q2 2025 Opening',
                brandExperience: 'Resort development in mountain markets',
                specialConsiderations: 'Seasonal operations, high-end amenities, location premium'
            }
        ];
    }

    setupEventListeners() {
        // Tab navigation
        document.querySelectorAll('.nav-tab').forEach(tab => {
            tab.addEventListener('click', (e) => {
                this.switchTab(e.currentTarget.dataset.tab);
            });
        });

        // Search functionality
        document.getElementById('dealSearch').addEventListener('input', (e) => {
            this.searchDeals(e.target.value);
        });

        // Table sorting
        document.querySelectorAll('.sortable').forEach(header => {
            header.addEventListener('click', (e) => {
                this.sortDeals(e.currentTarget.dataset.sort);
            });
        });

        // Filter changes
        document.querySelectorAll('.filter-group select').forEach(select => {
            select.addEventListener('change', () => {
                this.applyFilters();
            });
        });

        // Bulk action select
        document.getElementById('bulkActionSelect').addEventListener('change', (e) => {
            if (e.target.value) {
                this.applyBulkAction(e.target.value);
                e.target.value = '';
            }
        });
    }

    switchTab(tab) {
        // Update active tab
        document.querySelectorAll('.nav-tab').forEach(t => t.classList.remove('active'));
        document.querySelector(`[data-tab="${tab}"]`).classList.add('active');

        // Filter deals by status
        if (tab === 'new') {
            this.filters.status = 'new';
        } else if (tab === 'active') {
            this.filters.status = ['deal-received', 'viewed-by-brand'];
        } else if (tab === 'archives') {
            this.filters.status = 'archived';
        } else if (tab === 'deal-log') {
            this.filters.status = 'logged';
        } else {
            this.filters.status = '';
        }

        this.applyFilters();
        this.currentPage = 1;
        this.renderDeals();
    }

    searchDeals(query) {
        this.filters.search = query.toLowerCase();
        this.applyFilters();
        this.currentPage = 1;
        this.renderDeals();
    }

    applyFilters() {
        this.filteredDeals = this.deals.filter(deal => {
            // Status filter
            if (this.filters.status) {
                if (Array.isArray(this.filters.status)) {
                    if (!this.filters.status.includes(deal.status)) {
                        return false;
                    }
                } else if (deal.status !== this.filters.status) {
                    return false;
                }
            }

            // Search filter
            if (this.filters.search) {
                const searchFields = [
                    deal.propertyName,
                    deal.ownerName,
                    deal.ownerCompany,
                    deal.city,
                    deal.country
                ].join(' ').toLowerCase();
                
                if (!searchFields.includes(this.filters.search)) {
                    return false;
                }
            }

            // Property type filter
            if (this.filters.propertyType && deal.propertyType !== this.filters.propertyType) {
                return false;
            }

            // Room count filter
            if (this.filters.roomCount) {
                const rooms = deal.rooms;
                const [min, max] = this.filters.roomCount.split('-').map(Number);
                if (max && (rooms < min || rooms > max)) return false;
                if (!max && rooms < min) return false; // 300+ case
            }

            // Budget filter
            if (this.filters.budget) {
                const budget = parseFloat(deal.budget.replace(/[$,]/g, ''));
                const [min, max] = this.filters.budget.split('-').map(s => 
                    parseFloat(s.replace(/[$,]/g, '')) * 1000000
                );
                if (max && (budget < min || budget > max)) return false;
                if (!max && budget < min) return false; // 50M+ case
            }

            // Country filter
            if (this.filters.country && deal.country !== this.filters.country) {
                return false;
            }

            return true;
        });

        this.sortDeals(this.currentSort.field, this.currentSort.direction);
        this.renderDeals();
        this.updateTabCounts();
    }

    sortDeals(field, direction = null) {
        if (field === this.currentSort.field) {
            this.currentSort.direction = direction || (this.currentSort.direction === 'asc' ? 'desc' : 'asc');
        } else {
            this.currentSort.field = field;
            this.currentSort.direction = direction || 'asc';
        }

        this.filteredDeals.sort((a, b) => {
            let aVal = a[field];
            let bVal = b[field];

            // Handle special sorting cases
            if (field === 'budget') {
                aVal = parseFloat(aVal.replace(/[$,]/g, ''));
                bVal = parseFloat(bVal.replace(/[$,]/g, ''));
            } else if (field === 'submitDate') {
                aVal = new Date(aVal);
                bVal = new Date(bVal);
            }

            if (aVal < bVal) return this.currentSort.direction === 'asc' ? -1 : 1;
            if (aVal > bVal) return this.currentSort.direction === 'asc' ? 1 : -1;
            return 0;
        });

        this.renderDeals();
    }

    renderDeals() {
        const tbody = document.getElementById('dealsTableBody');
        const startIndex = (this.currentPage - 1) * this.itemsPerPage;
        const endIndex = startIndex + this.itemsPerPage;
        const pageDeals = this.filteredDeals.slice(startIndex, endIndex);

        tbody.innerHTML = pageDeals.map(deal => `
            <tr class="deal-row" data-deal-id="${deal.id}">
                <td class="select-col">
                    <input type="checkbox" class="deal-select" data-deal-id="${deal.id}" 
                           onchange="toggleDealSelection('${deal.id}')"
                           ${this.selectedDeals.has(deal.id) ? 'checked' : ''}>
                </td>
                <td class="status-col">
                    <span class="status-badge status-${deal.status}">
                        ${this.getStatusLabel(deal.status)}
                    </span>
                </td>
                <td class="brand-match-col">
                    <div class="brand-match">${deal.brandMatch}</div>
                </td>
                <td class="score-col">
                    <div class="match-score score-${this.getScoreColor(deal.matchScore)}">
                        ${deal.matchScore}
                    </div>
                </td>
                <td class="headline-col">
                    <div class="deal-headline">${deal.propertyName}</div>
                </td>
                <td class="contact-col">
                    <div class="deal-contact">
                        <div class="contact-info">
                            <div class="contact-name">${deal.ownerName}</div>
                            <div class="contact-title">${deal.ownerTitle}, ${deal.ownerCompany}</div>
                        </div>
                        <div class="contact-avatar">
                            <img src="https://via.placeholder.com/32x32/667eea/ffffff?text=${deal.ownerName.split(' ').map(n => n[0]).join('')}" alt="${deal.ownerName}">
                        </div>
                    </div>
                </td>
                <td class="respond-time-col">
                    <div class="respond-time respond-${deal.respondTimeColor}">
                        ${deal.respondTime}
                    </div>
                </td>
                <td class="deal-details-col">
                    <button class="learn-more-btn" onclick="viewDealDetails('${deal.id}')">
                        LEARN MORE
                    </button>
                </td>
                <td class="actions-col">
                    <div class="action-buttons">
                        <button class="action-btn chat-btn" onclick="startChat('${deal.id}')" title="Chat">
                            <i class="fas fa-comment"></i>
                        </button>
                        <button class="action-btn email-btn" onclick="sendEmail('${deal.id}')" title="Email">
                            <i class="fas fa-envelope"></i>
                        </button>
                        <button class="action-btn more-btn" onclick="showDealMenu('${deal.id}')" title="More Actions">
                            <i class="fas fa-ellipsis-v"></i>
                        </button>
                    </div>
                </td>
            </tr>
        `).join('');

        this.updatePagination();
    }

    getStatusLabel(status) {
        const labels = {
            'new': 'NEW',
            'deal-received': 'Deal Received',
            'viewed-by-brand': 'Viewed by Brand',
            'under-review': 'UNDER REVIEW',
            'approved': 'APPROVED',
            'declined': 'DECLINED',
            'request-info': 'MORE INFO NEEDED'
        };
        return labels[status] || status.toUpperCase();
    }

    getScoreColor(score) {
        if (score >= 85) return 'high';
        if (score >= 70) return 'medium';
        return 'low';
    }

    formatDate(dateString) {
        const date = new Date(dateString);
        return date.toLocaleDateString('en-US', { 
            month: 'short', 
            day: 'numeric', 
            year: 'numeric' 
        });
    }

    updateTabCounts() {
        const counts = {
            new: this.deals.filter(d => d.status === 'new').length,
            active: this.deals.filter(d => ['deal-received', 'viewed-by-brand'].includes(d.status)).length,
            archives: 80, // Mock count from image
            'deal-log': 40 // Mock count from image
        };

        Object.entries(counts).forEach(([status, count]) => {
            const element = document.getElementById(`${status}DealsCount`);
            if (element) element.textContent = count;
        });
    }

    updatePagination() {
        const totalPages = Math.ceil(this.filteredDeals.length / this.itemsPerPage);
        const startItem = (this.currentPage - 1) * this.itemsPerPage + 1;
        const endItem = Math.min(this.currentPage * this.itemsPerPage, this.filteredDeals.length);

        // Update pagination info
        document.getElementById('paginationInfo').textContent = 
            `Showing ${startItem}-${endItem} of ${this.filteredDeals.length} deals`;

        // Update pagination controls
        document.getElementById('prevPage').disabled = this.currentPage === 1;
        document.getElementById('nextPage').disabled = this.currentPage === totalPages;

        // Update page numbers
        const pageNumbers = document.getElementById('pageNumbers');
        pageNumbers.innerHTML = '';
        
        for (let i = 1; i <= Math.min(totalPages, 5); i++) {
            const button = document.createElement('button');
            button.className = `page-number ${i === this.currentPage ? 'active' : ''}`;
            button.textContent = i;
            button.onclick = () => this.goToPage(i);
            pageNumbers.appendChild(button);
        }
    }

    goToPage(page) {
        this.currentPage = page;
        this.renderDeals();
    }

    async updateDealStatus(dealId, status, notes = '') {
        try {
            showLoading(true);

            const response = await fetch('/api/brand-review/update-status', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    // Add authentication headers here
                },
                body: JSON.stringify({
                    dealId,
                    status,
                    notes
                })
            });

            if (response.ok) {
                // Update local data
                const deal = this.deals.find(d => d.id === dealId);
                if (deal) {
                    deal.status = status;
                    deal.lastUpdated = new Date().toISOString();
                }

                this.applyFilters();
                this.updateTabCounts();
                
                showNotification(`Deal status updated to ${status}`, 'success');
            } else {
                throw new Error('Failed to update deal status');
            }
        } catch (error) {
            console.error('Error updating deal status:', error);
            showNotification('Failed to update deal status', 'error');
        } finally {
            showLoading(false);
        }
    }
}

// Global functions for HTML event handlers
let dashboard;

function toggleFilters() {
    const panel = document.getElementById('filtersPanel');
    panel.style.display = panel.style.display === 'none' ? 'block' : 'none';
}

function applyFilters() {
    dashboard.filters = {
        propertyType: document.getElementById('filterPropertyType').value,
        roomCount: document.getElementById('filterRoomCount').value,
        budget: document.getElementById('filterBudget').value,
        country: document.getElementById('filterCountry').value
    };
    dashboard.applyFilters();
}

function clearFilters() {
    document.getElementById('filterPropertyType').value = '';
    document.getElementById('filterRoomCount').value = '';
    document.getElementById('filterBudget').value = '';
    document.getElementById('filterCountry').value = '';
    dashboard.filters = {};
    dashboard.applyFilters();
}

function toggleSelectAll() {
    const selectAll = document.getElementById('selectAll');
    const checkboxes = document.querySelectorAll('.deal-select');
    
    checkboxes.forEach(checkbox => {
        checkbox.checked = selectAll.checked;
        if (selectAll.checked) {
            dashboard.selectedDeals.add(checkbox.dataset.dealId);
        } else {
            dashboard.selectedDeals.delete(checkbox.dataset.dealId);
        }
    });
}

function toggleDealSelection(dealId) {
    if (dashboard.selectedDeals.has(dealId)) {
        dashboard.selectedDeals.delete(dealId);
    } else {
        dashboard.selectedDeals.add(dealId);
    }
}

function applyBulkAction(action) {
    if (dashboard.selectedDeals.size === 0) {
        showNotification('Please select deals first', 'warning');
        return;
    }

    const confirmMessage = `Are you sure you want to ${action} ${dashboard.selectedDeals.size} selected deal(s)?`;
    if (confirm(confirmMessage)) {
        dashboard.selectedDeals.forEach(dealId => {
            dashboard.updateDealStatus(dealId, action);
        });
        dashboard.selectedDeals.clear();
        document.getElementById('selectAll').checked = false;
    }
}

function viewDealDetails(dealId) {
    const deal = dashboard.deals.find(d => d.id === dealId);
    if (!deal) return;

    const modal = document.getElementById('dealDetailModal');
    const content = document.getElementById('dealDetailContent');
    const title = document.getElementById('modalTitle');

    title.textContent = deal.propertyName;
    content.innerHTML = `
        <div class="deal-detail-grid">
            <div class="detail-section">
                <h4>Property Information</h4>
                <div class="detail-item">
                    <label>Property Name:</label>
                    <span>${deal.propertyName}</span>
                </div>
                <div class="detail-item">
                    <label>Property Type:</label>
                    <span>${deal.propertyType}</span>
                </div>
                <div class="detail-item">
                    <label>Location:</label>
                    <span>${deal.city}, ${deal.country}</span>
                </div>
                <div class="detail-item">
                    <label>Number of Rooms:</label>
                    <span>${deal.rooms}</span>
                </div>
                <div class="detail-item">
                    <label>Budget:</label>
                    <span>${deal.budget}</span>
                </div>
                <div class="detail-item">
                    <label>Stage:</label>
                    <span>${deal.stage}</span>
                </div>
                <div class="detail-item">
                    <label>Timeline:</label>
                    <span>${deal.timeline}</span>
                </div>
            </div>

            <div class="detail-section">
                <h4>Owner Information</h4>
                <div class="detail-item">
                    <label>Name:</label>
                    <span>${deal.ownerName}</span>
                </div>
                <div class="detail-item">
                    <label>Company:</label>
                    <span>${deal.ownerCompany}</span>
                </div>
                <div class="detail-item">
                    <label>Email:</label>
                    <span><a href="mailto:${deal.ownerEmail}">${deal.ownerEmail}</a></span>
                </div>
                <div class="detail-item">
                    <label>Phone:</label>
                    <span><a href="tel:${deal.ownerPhone}">${deal.ownerPhone}</a></span>
                </div>
                <div class="detail-item">
                    <label>Brand Experience:</label>
                    <span>${deal.brandExperience}</span>
                </div>
            </div>

            <div class="detail-section full-width">
                <h4>Project Description</h4>
                <p>${deal.description}</p>
            </div>

            <div class="detail-section full-width">
                <h4>Special Considerations</h4>
                <p>${deal.specialConsiderations}</p>
            </div>
        </div>
    `;

    modal.style.display = 'block';
}

function closeDealModal() {
    document.getElementById('dealDetailModal').style.display = 'none';
}

function updateDealStatus(status) {
    // This would be called from the modal
    const dealId = dashboard.currentDealId; // You'd need to track this
    dashboard.updateDealStatus(dealId, status);
    closeDealModal();
}

function quickApprove(dealId) {
    if (confirm('Are you sure you want to approve this deal?')) {
        dashboard.updateDealStatus(dealId, 'approved');
    }
}

function quickDecline(dealId) {
    if (confirm('Are you sure you want to decline this deal?')) {
        dashboard.updateDealStatus(dealId, 'declined');
    }
}

function showDealMenu(dealId) {
    // Implementation for dropdown menu
    console.log('Show menu for deal:', dealId);
}

function startChat(dealId) {
    console.log('Start chat for deal:', dealId);
    showNotification('Chat feature coming soon!', 'info');
}

function sendEmail(dealId) {
    console.log('Send email for deal:', dealId);
    showNotification('Email feature coming soon!', 'info');
}

function changePage(direction) {
    const totalPages = Math.ceil(dashboard.filteredDeals.length / dashboard.itemsPerPage);
    const newPage = dashboard.currentPage + direction;
    
    if (newPage >= 1 && newPage <= totalPages) {
        dashboard.goToPage(newPage);
    }
}

// Utility functions
function showLoading(show) {
    document.getElementById('loadingOverlay').style.display = show ? 'flex' : 'none';
}

function showNotification(message, type = 'info') {
    // Simple notification system - you could enhance this
    const notification = document.createElement('div');
    notification.className = `notification notification-${type}`;
    notification.textContent = message;
    
    document.body.appendChild(notification);
    
    setTimeout(() => {
        notification.remove();
    }, 3000);
}

// Initialize dashboard when page loads
document.addEventListener('DOMContentLoaded', () => {
    dashboard = new BrandReviewDashboard();
});
