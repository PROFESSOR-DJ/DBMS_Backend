const express = require('express');
const router = express.Router();
const authorController = require('../controllers/authorController');
const authController = require('../controllers/authController');

// Public routes
router.get('/', authorController.getAllAuthors);
router.get('/search', authorController.searchAuthors);

// Protected routes
router.post('/', authController.authenticate, authorController.createAuthor);
router.put('/:id', authController.authenticate, authorController.updateAuthor);
router.delete('/:id', authController.authenticate, authorController.deleteAuthor);

module.exports = router;
