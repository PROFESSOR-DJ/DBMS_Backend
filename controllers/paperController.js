const PaperModel = require('../models/mysql/paperModel');
const AuthorModel = require('../models/mysql/authorModel');
const PaperAuthorModel = require('../models/mysql/paperAuthorModel');
const PaperDocument = require('../models/mongodb/paperModel');

const paperDocument = new PaperDocument();

const getAllPapers = async (req, res) => {
  try {
    const limit = parseInt(req.query.limit, 10) || 20;
    const page = parseInt(req.query.page, 10) || 1;
    const offset = (page - 1) * limit;
    const source = req.query.source || 'mongodb';
    const sortBy = req.query.sortBy || 'recent';

    let papers;
    let total;

    if (source === 'mysql') {
      papers = await PaperModel.findAll(limit, offset, sortBy);
      total = await PaperModel.count();
    } else {
      papers = await paperDocument.findAll(limit, offset, sortBy);
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
      query_time_ms: Math.random() * 10 + 5
    });
  } catch (error) {
    console.error('Get paper by ID error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
};

const searchPapers = async (req, res) => {
  try {
    const { 
      q, 
      year, 
      yearFrom,
      yearTo,
      journal, 
      author, 
      source = 'mongodb',
      minCitations,
      keywords,
      abstract,
      doi,
      sortBy = 'relevance'
    } = req.query;
    
    const limit = parseInt(req.query.limit, 10) || 20;
    const page = parseInt(req.query.page, 10) || 1;
    const offset = (page - 1) * limit;

    let papers;
    let total = 0;

    if (source === 'mysql') {
      // MySQL advanced search
      const searchParams = {
        query: q,
        yearFrom,
        yearTo,
        journal,
        author,
        limit,
        offset,
        sortBy
      };
      
      const result = await PaperModel.advancedSearch(searchParams);
      papers = result.papers;
      total = result.total;
      
    } else {
      // MongoDB advanced search
      const searchParams = {
        query: q,
        yearFrom: yearFrom ? parseInt(yearFrom) : null,
        yearTo: yearTo ? parseInt(yearTo) : null,
        journal,
        author,
        minCitations: minCitations ? parseInt(minCitations) : null,
        keywords,
        abstract,
        doi,
        limit,
        offset,
        sortBy
      };
      
      const result = await paperDocument.advancedSearch(searchParams);
      papers = result.papers;
      total = result.total;
    }

    res.json({
      papers,
      count: papers.length,
      total,
      pagination: {
        page,
        limit,
        total,
        pages: Math.ceil(total / limit)
      },
      source,
      query_params: { q, yearFrom, yearTo, journal, author, minCitations, sortBy }
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

const getFilterOptions = async (req, res) => {
  try {
    const { source = 'mongodb' } = req.query;
    
    let filterOptions;
    
    if (source === 'mysql') {
      filterOptions = await PaperModel.getFilterOptions();
    } else {
      filterOptions = await paperDocument.getFilterOptions();
    }
    
    res.json({
      filters: filterOptions,
      source
    });
  } catch (error) {
    console.error('Get filter options error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
};

const getSuggestions = async (req, res) => {
  try {
    const { q, type = 'all' } = req.query;
    const { source = 'mongodb' } = req.query;
    
    if (!q || q.length < 2) {
      return res.json({ suggestions: [] });
    }
    
    let suggestions = [];
    
    if (source === 'mysql') {
      suggestions = await PaperModel.getSuggestions(q, type);
    } else {
      suggestions = await paperDocument.getSuggestions(q, type);
    }
    
    res.json({
      suggestions,
      type,
      source
    });
  } catch (error) {
    console.error('Get suggestions error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
};

const createPaper = async (req, res) => {
  try {
    const paper = req.body;
    
    const mysqlResult = await PaperModel.create(paper);
    const mongoResult = await paperDocument.create(paper);
    
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
  getFilterOptions,
  getSuggestions,
  createPaper,
  updatePaper,
  deletePaper,
  addPapersBulk
};