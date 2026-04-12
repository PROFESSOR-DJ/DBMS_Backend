// graphRoutes maps graph analytics endpoints to backend graph handlers.
const express        = require('express');
const router         = express.Router();
const neo4jCtrl      = require('../controllers/neo4jController');
const { authenticate } = require('../controllers/authController');


router.get('/health',                  neo4jCtrl.getHealth);
router.get('/stats',                   neo4jCtrl.getGraphStats);
router.get('/top-authors',             neo4jCtrl.getTopAuthors);
router.get('/top-journals',            neo4jCtrl.getTopJournals);
router.get('/papers-by-year',          neo4jCtrl.getPapersByYear);
router.get('/papers-by-source',        neo4jCtrl.getPapersBySource);
router.get('/search-authors',          neo4jCtrl.searchAuthors);
router.post('/conflict-check',         authenticate, neo4jCtrl.checkConflictOfInterest);


router.get('/author-network/:name',    neo4jCtrl.getAuthorNetwork);
router.get('/author-papers/:name',     neo4jCtrl.getAuthorPapers);
router.get('/journal-authors/:journal', neo4jCtrl.getJournalAuthors);

// ── New routes using CO_AUTHORED and FIRST_AUTHORED ──────────────────────────

// Research leadership — authors ranked by first-authored papers
router.get('/top-first-authors', neo4jCtrl.getTopFirstAuthors);

// First-authored papers for a specific author
router.get('/author-first-papers/:name', neo4jCtrl.getAuthorFirstAuthoredPapers);

// Direct collaboration strength between two authors using CO_AUTHORED
router.get('/collaboration-strength', neo4jCtrl.getAuthorCollaborationStrength);

// Shortest collaboration path between two researchers
router.get('/research-path', neo4jCtrl.findResearchPath);

// Collaboration leaderboard — most connected researchers
router.get('/collaboration-leaderboard', neo4jCtrl.getCollaborationLeaderboard);

// Journal impact network — cross-journal patterns
router.get('/journal-network/:journal', neo4jCtrl.getJournalImpactNetwork);

// Source distribution with year range
router.get('/source-distribution', neo4jCtrl.getSourceDistribution);

module.exports = router;
