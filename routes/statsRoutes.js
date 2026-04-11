// statsRoutes maps statistics endpoints to backend stats handlers.
const express = require('express');
const router  = express.Router();
const statsController = require('../controllers/statsController');
const authController  = require('../controllers/authController');

// ── Public endpoints ─────────────────────────────────────────────────────────

// Overview / dashboard
router.get('/overview',           statsController.getOverview);

// Author impact — uses stored procedure GetAuthorImpact()
router.get('/authors',            statsController.getAuthorStats);
router.get('/author-track',       statsController.getAuthorTrackRecord);
router.get('/author-track/:name', statsController.getAuthorTrackRecord);

// Journal stats — direct journals table listing with sorting/search
router.get('/journals',           statsController.getJournalStats);

// Papers per year — MongoDB aggregation
router.get('/papers-per-year',    statsController.getPapersPerYear);

// Architecture & query-performance docs
router.get('/database-info',      statsController.getDatabaseInfo);
router.get('/query-performance',  statsController.getQueryPerformance);

// ── New endpoints (procedures / triggers) ────────────────────────────────────

// GetTrendingPapers(year, limit) — papers from ?year ordered by author_count
router.get('/trending',           statsController.getTrendingPapers);

// GetIncompletePapers() — papers missing abstract / journal / publish_year
router.get('/incomplete-papers',  statsController.getIncompletePapers);

// GetActiveUsers() — users ordered by last_login (trg_update_last_login)
router.get('/active-users',       authController.authenticate, statsController.getActiveUsers);

// Important papers — flagged by trigger trg_mark_important_paper (author_count >= 5)
router.get('/important-papers',   statsController.getImportantPapers);

// Journal popularity — paper_count column maintained by trg_update_journal_count
router.get('/journal-popularity', statsController.getJournalPopularity);

module.exports = router;
