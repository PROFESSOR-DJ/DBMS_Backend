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

module.exports = router;
