const express = require('express');
const router = express.Router();
const statsController = require('../controllers/statsController');
const authController = require('../controllers/authController');

// All stats routes are protected
router.use(authController.authenticate);

router.get('/overview', statsController.getOverview);
router.get('/authors', statsController.getAuthorStats);
router.get('/journals', statsController.getJournalStats);
router.get('/papers-per-year', statsController.getPapersPerYear);
router.get('/database-info', statsController.getDatabaseInfo);
router.get('/query-performance', statsController.getQueryPerformance);

module.exports = router;