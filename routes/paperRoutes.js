const express = require('express');
const router = express.Router();
const paperController = require('../controllers/paperController');
const authController = require('../controllers/authController');

// Public routes
router.get('/', paperController.getAllPapers);
router.get('/search', paperController.searchPapers);
router.get('/:id', paperController.getPaperById);
router.get('/year/:year', paperController.getPapersByYear);
router.get('/journal/:journal', paperController.getPapersByJournal);
router.get('/author/:author', paperController.getPapersByAuthor);

// Protected routes (admin functionality)
router.post('/', authController.authenticate, paperController.createPaper);
router.put('/:id', authController.authenticate, paperController.updatePaper);
router.delete('/:id', authController.authenticate, paperController.deletePaper);
router.post('/bulk', authController.authenticate, paperController.addPapersBulk);

module.exports = router;