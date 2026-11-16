# ADMIN_SURFACE_TRACE

| Tab | Frontend | API | Backend | Source |
|-----|----------|-----|---------|--------|
| AI Demand Reviews | admin-ai-demand-reviews.js | GET /api/admin/ai-demand-positioning/monthly-reviews | getAdminMonthlyReviews | buildPublishedAdpReviewCoverageV1 + archive |
| ADP Action Plan | admin-adp-action-plan.js | GET .../action-plan-catalog | getAdminActionPlanCatalog | same coverage; requires reviewId |
| Report Archive | admin-report-archive.js | GET /api/admin/report-archive | getAdminReportArchiveCatalog | dealality-report-archive |
| AI Visibility Audit | admin-ai-visibility-audit.js | GET /api/admin/ai-visibility-audit/hotels | getAdminAvaHotels | resolveAvaAdminHotelRow / AUDIT_META |
