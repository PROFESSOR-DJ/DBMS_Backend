// journalRoutes maps journal endpoints to Scimago-style handlers.
const express = require('express');
const router = express.Router();
const journalController = require('../controllers/journalController');

router.get('/search', journalController.searchJournals);
router.get('/:id', journalController.getJournalDetails);

module.exports = router;
