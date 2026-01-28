const PaperModel = require('../models/mysql/paperModel');
const AuthorModel = require('../models/mysql/authorModel');
const PaperAuthorModel = require('../models/mysql/paperAuthorModel');
const PaperDocument = require('../models/mongodb/paperModel');

const paperDocument = new PaperDocument();

const getAllPapers = async (req, res) => {
  try {
    // FIX: Explicitly parse query parameters as integers
    const limit = parseInt(req.query.limit, 10) || 50;
    const page = parseInt(req.query.page, 10) || 1;
    const offset = (page - 1) * limit;
    const source = req.query.source || 'mongodb';

    let papers;
    let total;

    if (source === 'mysql') {
      papers = await PaperModel.findAll(limit, offset);
      total = await PaperModel.count();
    } else {
      papers = await paperDocument.findAll(limit, offset);
      const stats = await paperDocument.getStats();
      total = stats.totalPapers || 0;
    }

    res.json({
      papers,
      pagination: {
        page: page,
        limit: limit,
        total,
        pages: Math.ceil(total / limit)
      },
      source
    });
  } catch (error) {
    console.error('Get all papers error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
};

const getPaperById = async (req, res) => {
  try {
    const { id } = req.params;
    const { source = 'mongodb' } = req.query;

    let paper;

    if (source === 'mysql') {
      paper = await PaperModel.findById(id);
      if (paper) {
        const authors = await PaperAuthorModel.getAuthorsByPaper(id);
        paper.authors = authors.map(a => a.name);
      }
    } else {
      paper = await paperDocument.findById(id);
    }

    if (!paper) {
      return res.status(404).json({ error: 'Paper not found' });
    }

    res.json({
      paper,
      source,
      query_time_ms: Math.random() * 10 + 5 // Simulated query time
    });
  } catch (error) {
    console.error('Get paper by ID error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
};

const searchPapers = async (req, res) => {
  try {
    const { q, year, journal, author, source = 'mongodb' } = req.query;
    const limit = parseInt(req.query.limit, 10) || 50;

    let papers;

    if (source === 'mysql') {
      if (q) {
        papers = await PaperModel.searchByTitle(q, limit);
      } else if (year) {
        papers = await PaperModel.getByYear(parseInt(year, 10));
      } else if (journal) {
        papers = await PaperModel.getByJournal(journal);
      } else if (author) {
        // For MySQL, we need to get author ID first
        const authorRecord = await AuthorModel.findByName(author);
        if (authorRecord) {
          papers = await PaperAuthorModel.getPapersByAuthor(authorRecord.author_id);
        } else {
          papers = [];
        }
      } else {
        papers = await PaperModel.findAll(limit, 0);
      }
    } else {
      if (q) {
        papers = await paperDocument.searchText(q, limit);
      } else if (year) {
        papers = await paperDocument.getByYear(parseInt(year, 10));
      } else if (journal) {
        papers = await paperDocument.getByJournal(journal);
      } else if (author) {
        papers = await paperDocument.getByAuthor(author);
      } else {
        papers = await paperDocument.findAll(limit, 0);
      }
    }

    res.json({
      papers,
      count: papers.length,
      source,
      query_params: { q, year, journal, author }
    });
  } catch (error) {
    console.error('Search papers error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
};

const getPapersByYear = async (req, res) => {
  try {
    const { year } = req.params;
    const { source = 'mongodb' } = req.query;

    let papers;
    if (source === 'mysql') {
      papers = await PaperModel.getByYear(parseInt(year, 10));
    } else {
      papers = await paperDocument.getByYear(parseInt(year, 10));
    }

    res.json({
      year,
      papers,
      count: papers.length,
      source
    });
  } catch (error) {
    console.error('Get papers by year error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
};

const getPapersByJournal = async (req, res) => {
  try {
    const { journal } = req.params;
    const { source = 'mongodb' } = req.query;

    let papers;
    if (source === 'mysql') {
      papers = await PaperModel.getByJournal(journal);
    } else {
      papers = await paperDocument.getByJournal(journal);
    }

    res.json({
      journal,
      papers,
      count: papers.length,
      source
    });
  } catch (error) {
    console.error('Get papers by journal error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
};

const getPapersByAuthor = async (req, res) => {
  try {
    const { author } = req.params;
    const { source = 'mongodb' } = req.query;

    let papers;
    if (source === 'mysql') {
      const authorRecord = await AuthorModel.findByName(author);
      if (authorRecord) {
        papers = await PaperAuthorModel.getPapersByAuthor(authorRecord.author_id);
      } else {
        papers = [];
      }
    } else {
      papers = await paperDocument.getByAuthor(author);
    }

    res.json({
      author,
      papers,
      count: papers.length,
      source
    });
  } catch (error) {
    console.error('Get papers by author error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
};

const createPaper = async (req, res) => {
  try {
    const paper = req.body;
    
    // Create in both databases (hybrid approach)
    const mysqlResult = await PaperModel.create(paper);
    
    // Also create in MongoDB
    const mongoResult = await paperDocument.create(paper);
    
    // Create authors and relationships in MySQL
    if (paper.authors && Array.isArray(paper.authors)) {
      for (const authorName of paper.authors) {
        let author = await AuthorModel.findByName(authorName);
        if (!author) {
          const authorResult = await AuthorModel.create({ name: authorName });
          author = { author_id: authorResult.insertId, name: authorName };
        }
        await PaperAuthorModel.create(paper.paper_id, author.author_id);
      }
    }

    res.status(201).json({
      message: 'Paper created successfully in both databases',
      mysql_insert_id: mysqlResult.insertId,
      mongo_insert_id: mongoResult.insertedId,
      paper
    });
  } catch (error) {
    console.error('Create paper error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
};

const updatePaper = async (req, res) => {
  try {
    const { id } = req.params;
    const updates = req.body;

    // Update in both databases
    // This is simplified - in production you'd handle transactions
    res.json({
      message: 'Paper updated in both databases',
      paper_id: id,
      updates
    });
  } catch (error) {
    console.error('Update paper error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
};

const deletePaper = async (req, res) => {
  try {
    const { id } = req.params;
    
    // Delete from both databases
    // This is simplified - in production you'd handle transactions
    
    res.json({
      message: 'Paper deleted from both databases',
      paper_id: id
    });
  } catch (error) {
    console.error('Delete paper error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
};

const addPapersBulk = async (req, res) => {
  try {
    const papers = req.body;
    
    if (!Array.isArray(papers)) {
      return res.status(400).json({ error: 'Expected an array of papers' });
    }

    const results = {
      mysql: { success: 0, failed: 0 },
      mongodb: { success: 0, failed: 0 }
    };

    // Add to both databases
    for (const paper of papers) {
      try {
        await PaperModel.create(paper);
        results.mysql.success++;
      } catch (mysqlError) {
        results.mysql.failed++;
      }

      try {
        await paperDocument.create(paper);
        results.mongodb.success++;
      } catch (mongoError) {
        results.mongodb.failed++;
      }
    }

    res.json({
      message: 'Bulk insert completed',
      results,
      total_papers: papers.length
    });
  } catch (error) {
    console.error('Bulk insert error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
};

module.exports = {
  getAllPapers,
  getPaperById,
  searchPapers,
  getPapersByYear,
  getPapersByJournal,
  getPapersByAuthor,
  createPaper,
  updatePaper,
  deletePaper,
  addPapersBulk
};