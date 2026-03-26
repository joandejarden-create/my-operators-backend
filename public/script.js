// Modal Functions
function openDealForm() {
    document.getElementById('dealModal').style.display = 'block';
    document.body.style.overflow = 'hidden';
}

function closeDealForm() {
    document.getElementById('dealModal').style.display = 'none';
    document.body.style.overflow = 'auto';
}

function openBrandForm() {
    document.getElementById('brandModal').style.display = 'block';
    document.body.style.overflow = 'hidden';
}

function closeBrandForm() {
    document.getElementById('brandModal').style.display = 'none';
    document.body.style.overflow = 'auto';
}

function openRequestForm() {
    document.getElementById('requestModal').style.display = 'block';
    document.body.style.overflow = 'hidden';
}

function closeRequestForm() {
    document.getElementById('requestModal').style.display = 'none';
    document.body.style.overflow = 'auto';
}

// Close modal when clicking outside of it
window.onclick = function(event) {
    const dealModal = document.getElementById('dealModal');
    const brandModal = document.getElementById('brandModal');
    const requestModal = document.getElementById('requestModal');
    
    if (event.target === dealModal) {
        closeDealForm();
    } else if (event.target === brandModal) {
        closeBrandForm();
    } else if (event.target === requestModal) {
        closeRequestForm();
    }
}

// Deal Form Submission
async function submitDeal(event) {
    event.preventDefault();
    
    const form = document.getElementById('dealForm');
    const formData = new FormData(form);
    const data = Object.fromEntries(formData);
    
    // Show loading state
    const submitBtn = form.querySelector('.submit-btn');
    const originalText = submitBtn.textContent;
    submitBtn.textContent = 'Submitting Deal...';
    submitBtn.disabled = true;
    
    try {
        const response = await fetch('/api/intake/deal', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'x-intake-secret': 'your-secret-here' // This should match your INTAKE_SHARED_SECRET
            },
            body: JSON.stringify({
                projectName: data.projectName,
                email: data.ownerEmail,
                firstName: data.ownerFirstName,
                lastName: data.ownerLastName,
                country: data.country,
                memberstackId: 'temp-id' // This would come from Memberstack in production
            })
        });
        
        const result = await response.json();
        
        if (response.ok) {
            alert('Deal submitted successfully! We\'ll review your project and get back to you soon.');
            closeDealForm();
            form.reset();
        } else {
            throw new Error(result.error || 'Failed to submit deal');
        }
        
    } catch (error) {
        console.error('Error submitting deal:', error);
        alert('There was an error submitting your deal. Please try again.');
    } finally {
        submitBtn.textContent = originalText;
        submitBtn.disabled = false;
    }
}

// Brand Form Submission
async function submitBrand(event) {
    event.preventDefault();
    
    const form = document.getElementById('brandForm');
    const formData = new FormData(form);
    const data = Object.fromEntries(formData);
    
    // Show loading state
    const submitBtn = form.querySelector('.submit-btn');
    const originalText = submitBtn.textContent;
    submitBtn.textContent = 'Creating Profile...';
    submitBtn.disabled = true;
    
    try {
        const response = await fetch('/api/intake/brand', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'x-intake-secret': 'your-secret-here' // This should match your INTAKE_SHARED_SECRET
            },
            body: JSON.stringify({
                brandName: data.brandName,
                brandType: data.brandType,
                brandEmail: data.brandEmail,
                brandPhone: data.brandPhone,
                contactFirstName: data.contactFirstName,
                contactLastName: data.contactLastName,
                contactTitle: data.contactTitle,
                targetRegions: data.targetRegions,
                minRooms: data.minRooms,
                maxRooms: data.maxRooms,
                budgetRange: data.budgetRange,
                dealCriteria: data.dealCriteria,
                trackRecord: data.trackRecord
            })
        });
        
        const result = await response.json();
        
        if (response.ok) {
            alert('Brand profile created successfully! We\'ll review your profile and start sending you relevant deals.');
            closeBrandForm();
            form.reset();
        } else {
            throw new Error(result.error || 'Failed to create brand profile');
        }
        
    } catch (error) {
        console.error('Error creating brand profile:', error);
        alert('There was an error creating your brand profile. Please try again.');
    } finally {
        submitBtn.textContent = originalText;
        submitBtn.disabled = false;
    }
}

// Request Information Form Submission
async function submitRequest(event) {
    event.preventDefault();
    
    const form = document.getElementById('requestForm');
    const formData = new FormData(form);
    const data = Object.fromEntries(formData);
    
    // Show loading state
    const submitBtn = form.querySelector('.submit-btn');
    const originalText = submitBtn.textContent;
    submitBtn.textContent = 'Submitting...';
    submitBtn.disabled = true;
    
    try {
        const response = await fetch('/api/request-info', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(data)
        });
        
        const result = await response.json();
        
        if (response.ok) {
            alert(result.message || 'Thank you for your interest! We\'ll be in touch soon.');
            closeRequestForm();
            form.reset();
        } else {
            throw new Error(result.error || 'Failed to submit request');
        }
        
    } catch (error) {
        console.error('Error submitting form:', error);
        alert('There was an error submitting your request. Please try again.');
    } finally {
        submitBtn.textContent = originalText;
        submitBtn.disabled = false;
    }
}

// Dashboard navigation
function goToDashboard() {
    // This would typically redirect to your dashboard
    // For now, we'll show an alert
    alert('Dashboard functionality coming soon!');
}

// Test mode counter (for demonstration) — only runs when .test-count exists
let testCount = 0;
function updateTestCount() {
    const el = document.querySelector('.test-count');
    if (el) {
        testCount++;
        el.textContent = testCount;
    }
}
const testCountEl = document.querySelector('.test-count');
if (testCountEl) {
    setInterval(updateTestCount, 5000);
}

// Add some subtle animations on page load — only when .main-content exists
document.addEventListener('DOMContentLoaded', function() {
    const mainContent = document.querySelector('.main-content');
    if (!mainContent) return;
    mainContent.style.opacity = '0';
    mainContent.style.transform = 'translateY(20px)';
    setTimeout(() => {
        mainContent.style.transition = 'opacity 0.8s ease, transform 0.8s ease';
        mainContent.style.opacity = '1';
        mainContent.style.transform = 'translateY(0)';
    }, 100);
});
