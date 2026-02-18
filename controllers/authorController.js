const AuthorModel = require('../models/mysql/authorModel');

// Get all authors
const getAllAuthors = async (req, res) => {
    try {
        const limit = parseInt(req.query.limit, 10) || 100;
        const offset = parseInt(req.query.offset, 10) || 0;
        const authors = await AuthorModel.findAll(limit, offset);
        res.json({ authors });
    } catch (error) {
        console.error('Get all authors error:', error);
        res.status(500).json({ error: 'Internal server error' });
    }
};

// Search authors
const searchAuthors = async (req, res) => {
    try {
        const { q } = req.query;
        if (!q) return res.status(400).json({ error: 'Query parameter required' });
        const authors = await AuthorModel.searchByName(q);
        res.json({ authors });
    } catch (error) {
        console.error('Search authors error:', error);
        res.status(500).json({ error: 'Internal server error' });
    }
};

// Create author
const createAuthor = async (req, res) => {
    try {
        const { name, paper_id } = req.body;
        if (!name) return res.status(400).json({ error: 'Author name required' });
        if (!paper_id) return res.status(400).json({ error: 'Linking to a paper is required for new authors' });

        const result = await AuthorModel.create({ author_name: name, paper_id });
        res.status(201).json({ message: 'Author created and linked to paper', author_id: result.insertId, name });
    } catch (error) {
        console.error('Create author error:', error);
        res.status(500).json({ error: 'Internal server error' });
    }
};

// Update author
const updateAuthor = async (req, res) => {
    try {
        const { id } = req.params;
        const { name } = req.body;
        if (!name) return res.status(400).json({ error: 'Author name required' }); // Assuming only name update for now
        await AuthorModel.update(id, { author_name: name });
        res.json({ message: 'Author updated', author_id: id, name });
    } catch (error) {
        console.error('Update author error:', error);
        res.status(500).json({ error: 'Internal server error' });
    }
};

// Delete author
const deleteAuthor = async (req, res) => {
    try {
        const { id } = req.params;
        await AuthorModel.delete(id);
        res.json({ message: 'Author deleted', author_id: id });
    } catch (error) {
        console.error('Delete author error:', error);
        res.status(500).json({ error: 'Internal server error' });
    }
};

module.exports = {
    getAllAuthors,
    searchAuthors,
    createAuthor,
    updateAuthor,
    deleteAuthor
};
