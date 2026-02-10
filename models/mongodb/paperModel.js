const { getMongoDB } = require('../../config/database');

class PaperDocument {
  getCollection() {
    try {
      const db = getMongoDB();
      return db.collection('papers');
    } catch (error) {
      throw new Error('MongoDB not available: ' + error.message);
    }
  }

  // Create paper document
  async create(paper) {
    const collection = this.getCollection();
    
    const paperDoc = {
      paper_id: paper.paper_id || `paper_${Date.now()}`,
      title: paper.title || '',
      abstract: paper.abstract || '',
      authors: Array.isArray(paper.authors) ? paper.authors : [],
      doi: paper.doi || '',
      has_full_text: Boolean(paper.has_full_text),
      is_covid19: Boolean(paper.is_covid19),
      journal: paper.journal || '',
      sha: paper.sha || '',
      source: paper.source || 'manual',
      year: typeof paper.year === 'number' ? paper.year : new Date().getFullYear(),
      ...(paper.citation_count && { citation_count: paper.citation_count }),
      ...(paper.keywords && { keywords: paper.keywords }),
      ...(paper.created_at && { created_at: paper.created_at }),
      ...(paper.updated_at && { updated_at: paper.updated_at })
    };
    
    const result = await collection.insertOne(paperDoc);
    return result;
  }

  // Get all papers with sorting
  async findAll(limit = 100, skip = 0, sortBy = 'recent') {
    const collection = this.getCollection();
    
    let sortOption = { year: -1, title: 1 };
    
    switch (sortBy) {
      case 'recent':
        sortOption = { year: -1, title: 1 };
        break;
      case 'oldest':
        sortOption = { year: 1, title: 1 };
        break;
      case 'title':
        sortOption = { title: 1 };
        break;
      case 'citations':
        sortOption = { citation_count: -1, year: -1 };
        break;
      case 'journal':
        sortOption = { journal: 1, year: -1 };
        break;
      default:
        sortOption = { year: -1, title: 1 };
    }
    
    const cursor = collection.find().sort(sortOption).skip(skip).limit(limit);
    return cursor.toArray();
  }

  // Get paper by ID
  async findById(paper_id) {
    const collection = this.getCollection();
    return collection.findOne({ paper_id: paper_id });
  }

  // Advanced search with multiple filters
  async advancedSearch(params) {
    const collection = this.getCollection();
    const {
      query,
      yearFrom,
      yearTo,
      journal,
      author,
      minCitations,
      keywords,
      abstract,
      doi,
      limit = 20,
      offset = 0,
      sortBy = 'relevance'
    } = params;

    // Build query filter
    const filter = {};
    const textSearchFields = [];

    // Text search
    if (query) {
      // Try text index search first
      filter.$text = { $search: query };
    }

    // Year range filter
    if (yearFrom || yearTo) {
      filter.year = {};
      if (yearFrom) filter.year.$gte = parseInt(yearFrom);
      if (yearTo) filter.year.$lte = parseInt(yearTo);
    }

    // Journal filter
    if (journal) {
      filter.journal = { $regex: journal, $options: 'i' };
    }

    // Author filter
    if (author) {
      filter.authors = { $regex: author, $options: 'i' };
    }

    // Citation filter
    if (minCitations) {
      filter.citation_count = { $gte: parseInt(minCitations) };
    }

    // Keywords filter
    if (keywords) {
      filter.keywords = { $regex: keywords, $options: 'i' };
    }

    // Abstract filter
    if (abstract) {
      filter.abstract = { $regex: abstract, $options: 'i' };
    }

    // DOI filter
    if (doi) {
      filter.doi = doi;
    }

    // Get total count
    const total = await collection.countDocuments(filter);

    // Sorting
    let sortOption = { year: -1, title: 1 };
    
    switch (sortBy) {
      case 'recent':
        sortOption = { year: -1, title: 1 };
        break;
      case 'oldest':
        sortOption = { year: 1, title: 1 };
        break;
      case 'title':
        sortOption = { title: 1 };
        break;
      case 'citations':
        sortOption = { citation_count: -1, year: -1 };
        break;
      case 'relevance':
        if (query && filter.$text) {
          sortOption = { score: { $meta: "textScore" }, year: -1 };
        } else {
          sortOption = { year: -1, title: 1 };
        }
        break;
      default:
        sortOption = { year: -1, title: 1 };
    }

    // Execute query
    let cursor;
    if (query && filter.$text && sortBy === 'relevance') {
      cursor = collection
        .find(filter, { score: { $meta: "textScore" } })
        .sort(sortOption)
        .skip(offset)
        .limit(limit);
    } else {
      cursor = collection
        .find(filter)
        .sort(sortOption)
        .skip(offset)
        .limit(limit);
    }

    const papers = await cursor.toArray();

    return {
      papers,
      total
    };
  }

  // Search papers by text (using MongoDB text index)
  async searchText(query, limit = 50) {
    const collection = this.getCollection();
    
    try {
      const cursor = collection.find(
        { $text: { $search: query } },
        { score: { $meta: "textScore" } }
      ).sort({ score: { $meta: "textScore" } }).limit(limit);
      return cursor.toArray();
    } catch (error) {
      console.log('Text search failed, falling back to regex:', error.message);
      const cursor = collection.find({
        $or: [
          { title: { $regex: query, $options: 'i' } },
          { abstract: { $regex: query, $options: 'i' } }
        ]
      }).limit(limit);
      return cursor.toArray();
    }
  }

  // Get papers by year
  async getByYear(year) {
    const collection = this.getCollection();
    const cursor = collection.find({ year: parseInt(year) }).sort({ title: 1 });
    return cursor.toArray();
  }

  // Get papers by year range
  async getByYearRange(yearFrom, yearTo) {
    const collection = this.getCollection();
    const cursor = collection.find({
      year: {
        $gte: parseInt(yearFrom),
        $lte: parseInt(yearTo)
      }
    }).sort({ year: -1, title: 1 });
    return cursor.toArray();
  }

  // Get papers by journal
  async getByJournal(journal) {
    const collection = this.getCollection();
    const cursor = collection.find({ 
      journal: { $regex: journal, $options: 'i' } 
    }).sort({ year: -1 });
    return cursor.toArray();
  }

  // Get papers by author
  async getByAuthor(authorName) {
    const collection = this.getCollection();
    const cursor = collection.find({ 
      authors: { $regex: authorName, $options: 'i' } 
    }).sort({ year: -1 });
    return cursor.toArray();
  }

  // Get COVID-19 papers
  async getCovid19Papers(limit = 50) {
    const collection = this.getCollection();
    const cursor = collection.find({ is_covid19: true }).sort({ year: -1 }).limit(limit);
    return cursor.toArray();
  }

  // Get filter options
  async getFilterOptions() {
    const collection = this.getCollection();

    const [yearStats, journalStats, keywordStats] = await Promise.all([
      // Get available years
      collection.distinct('year').then(years => 
        years.filter(y => y != null).sort((a, b) => b - a)
      ),
      
      // Get top journals
      collection.aggregate([
        { $group: { _id: '$journal', count: { $sum: 1 } } },
        { $sort: { count: -1 } },
        { $limit: 50 }
      ]).toArray(),
      
      // Get top keywords
      collection.aggregate([
        { $unwind: '$keywords' },
        { $group: { _id: '$keywords', count: { $sum: 1 } } },
        { $sort: { count: -1 } },
        { $limit: 50 }
      ]).toArray()
    ]);

    // Get year range
    const yearRange = await collection.aggregate([
      {
        $group: {
          _id: null,
          minYear: { $min: '$year' },
          maxYear: { $max: '$year' }
        }
      }
    ]).toArray();

    return {
      years: yearStats,
      journals: journalStats.map(j => ({ name: j._id, count: j.count })),
      keywords: keywordStats.map(k => ({ name: k._id, count: k.count })),
      yearRange: yearRange[0] || { minYear: 2000, maxYear: 2024 }
    };
  }

  // Get search suggestions
  async getSuggestions(query, type = 'all') {
    const collection = this.getCollection();
    const suggestions = [];

    if (type === 'all' || type === 'title') {
      const titles = await collection
        .find(
          { title: { $regex: `^${query}`, $options: 'i' } },
          { projection: { title: 1 } }
        )
        .limit(5)
        .toArray();
      
      suggestions.push(...titles.map(t => ({ type: 'title', value: t.title })));
    }

    if (type === 'all' || type === 'journal') {
      const journals = await collection.distinct('journal', {
        journal: { $regex: `^${query}`, $options: 'i' }
      });
      
      suggestions.push(...journals.slice(0, 5).map(j => ({ type: 'journal', value: j })));
    }

    if (type === 'all' || type === 'author') {
      const authors = await collection.aggregate([
        { $unwind: '$authors' },
        { $match: { authors: { $regex: `^${query}`, $options: 'i' } } },
        { $group: { _id: '$authors' } },
        { $limit: 5 }
      ]).toArray();
      
      suggestions.push(...authors.map(a => ({ type: 'author', value: a._id })));
    }

    if (type === 'all' || type === 'keyword') {
      const keywords = await collection.aggregate([
        { $unwind: '$keywords' },
        { $match: { keywords: { $regex: `^${query}`, $options: 'i' } } },
        { $group: { _id: '$keywords' } },
        { $limit: 5 }
      ]).toArray();
      
      suggestions.push(...keywords.map(k => ({ type: 'keyword', value: k._id })));
    }

    return suggestions;
  }

  // Get aggregated statistics
  async getStats() {
    const collection = this.getCollection();
    
    const pipeline = [
      {
        $facet: {
          totalStats: [
            {
              $group: {
                _id: null,
                totalPapers: { $sum: 1 },
                covid19Papers: { $sum: { $cond: ["$is_covid19", 1, 0] } },
                papersWithFullText: { $sum: { $cond: ["$has_full_text", 1, 0] } },
                avgCitationCount: { $avg: { $ifNull: ["$citation_count", 0] } }
              }
            }
          ],
          uniqueJournals: [
            { $group: { _id: "$journal" } },
            { $count: "count" }
          ],
          uniqueAuthors: [
            { $unwind: "$authors" },
            { $group: { _id: "$authors" } },
            { $count: "count" }
          ]
        }
      }
    ];
    
    const result = await collection.aggregate(pipeline).toArray();
    const stats = result[0] || {};
    
    return {
      totalPapers: stats.totalStats?.[0]?.totalPapers || 0,
      covid19Papers: stats.totalStats?.[0]?.covid19Papers || 0,
      papersWithFullText: stats.totalStats?.[0]?.papersWithFullText || 0,
      avgCitations: Math.round((stats.totalStats?.[0]?.avgCitationCount || 0) * 100) / 100,
      uniqueJournalCount: stats.uniqueJournals?.[0]?.count || 0,
      uniqueAuthorCount: stats.uniqueAuthors?.[0]?.count || 0
    };
  }

  // Get papers per year
  async getPapersPerYear() {
    const collection = this.getCollection();
    
    const pipeline = [
      {
        $group: {
          _id: "$year",
          count: { $sum: 1 },
          covidCount: { $sum: { $cond: ["$is_covid19", 1, 0] } }
        }
      },
      {
        $sort: { _id: 1 }
      }
    ];
    return collection.aggregate(pipeline).toArray();
  }

  // Get top journals
  async getTopJournals(limit = 10) {
    const collection = this.getCollection();
    
    const pipeline = [
      {
        $group: {
          _id: "$journal",
          count: { $sum: 1 },
          avgCitations: { $avg: { $ifNull: ["$citation_count", 0] } }
        }
      },
      {
        $sort: { count: -1 }
      },
      {
        $limit: limit
      },
      {
        $project: {
          journal: "$_id",
          count: 1,
          avgCitations: { $round: ["$avgCitations", 2] }
        }
      }
    ];
    return collection.aggregate(pipeline).toArray();
  }

  // Get top authors
  async getTopAuthors(limit = 10) {
    const collection = this.getCollection();
    
    const pipeline = [
      {
        $unwind: "$authors"
      },
      {
        $group: {
          _id: "$authors",
          count: { $sum: 1 },
          totalCitations: { $sum: { $ifNull: ["$citation_count", 0] } }
        }
      },
      {
        $sort: { count: -1 }
      },
      {
        $limit: limit
      },
      {
        $project: {
          author: "$_id",
          count: 1,
          avgCitations: { 
            $round: [
              { $cond: [
                { $eq: ["$count", 0] },
                0,
                { $divide: ["$totalCitations", "$count"] }
              ]},
              2
            ]
          }
        }
      }
    ];
    return collection.aggregate(pipeline).toArray();
  }

  // Get distinct values for a field
  async getDistinct(field) {
    const collection = this.getCollection();
    return collection.distinct(field);
  }

  // Get COVID-19 research statistics
  async getCovid19Stats() {
    const collection = this.getCollection();
    
    const pipeline = [
      {
        $match: { is_covid19: true }
      },
      {
        $facet: {
          byYear: [
            {
              $group: {
                _id: "$year",
                count: { $sum: 1 }
              }
            },
            { $sort: { _id: 1 } }
          ],
          byJournal: [
            {
              $group: {
                _id: "$journal",
                count: { $sum: 1 }
              }
            },
            { $sort: { count: -1 } },
            { $limit: 10 }
          ],
          bySource: [
            {
              $group: {
                _id: "$source",
                count: { $sum: 1 }
              }
            },
            { $sort: { count: -1 } }
          ]
        }
      }
    ];
    
    const result = await collection.aggregate(pipeline).toArray();
    return result[0] || {};
  }

  // Get papers by multiple criteria
  async getByMultipleCriteria(criteria) {
    const collection = this.getCollection();
    const { years, journals, authors, keywords, limit = 100, skip = 0 } = criteria;
    
    const filter = {};
    
    if (years && years.length > 0) {
      filter.year = { $in: years.map(y => parseInt(y)) };
    }
    
    if (journals && journals.length > 0) {
      filter.journal = { $in: journals };
    }
    
    if (authors && authors.length > 0) {
      filter.authors = { $in: authors };
    }
    
    if (keywords && keywords.length > 0) {
      filter.keywords = { $in: keywords };
    }
    
    const cursor = collection
      .find(filter)
      .sort({ year: -1, title: 1 })
      .skip(skip)
      .limit(limit);
    
    return cursor.toArray();
  }
}

module.exports = PaperDocument;