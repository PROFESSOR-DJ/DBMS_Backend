const express = require('express');
const router = express.Router();
const hybridController = require('../controllers/hybridController');
const authController = require('../controllers/authController');

// Protected hybrid queries
router.use(authController.authenticate);

// Cross-database queries
router.get('/paper-details/:id', hybridController.getPaperDetailsHybrid);
router.get('/author-network/:name', hybridController.getAuthorNetwork);
router.get('/journal-analysis/:journal', hybridController.getJournalAnalysis);
router.get('/search-hybrid', hybridController.searchHybrid);

// Database synchronization
router.post('/sync/mysql-to-mongo', hybridController.syncMySQLToMongo);
router.post('/sync/mongo-to-mysql', hybridController.syncMongoToMySQL);
router.get('/sync/status', hybridController.getSyncStatus);

module.exports = router;