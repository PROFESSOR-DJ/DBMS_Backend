// paperModel defines the backend MongoDB schema and operations for papers.
const { getMongoDB } = require('../../config/database');
const { ObjectId } = require('mongodb');

const escapeRegex = (value) => String(value || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

class PaperDocument {
  getCollection() {
    try {
      const db = getMongoDB();
      return db.collection('papers');
    } catch (error) {
      throw new Error('MongoDB not available: ' + error.message);
    }
  }

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
      is_important: Boolean(paper.is_important),
      author_count: parseInt(paper.author_count, 10) || 0,
      ...(paper.citation_count && { citation_count: paper.citation_count }),
      ...(paper.keywords && { keywords: paper.keywords }),
      ...(paper.created_at && { created_at: paper.created_at }),
      ...(paper.updated_at && { updated_at: paper.updated_at })
    };
    
    const result = await collection.insertOne(paperDoc);
    return result;
  }

  async findAll(limit = 100, skip = 0, sortBy = 'recent') {
    const collection = this.getCollection();
    
    let sortOption = { year: -1, title: 1 };
    
    switch (sortBy) {
      case 'recent':   sortOption = { year: -1, title: 1 }; break;
      case 'oldest':   sortOption = { year: 1, title: 1 }; break;
      case 'title':    sortOption = { title: 1 }; break;
      case 'citations': sortOption = { citation_count: -1, year: -1 }; break;
      case 'journal':  sortOption = { journal: 1, year: -1 }; break;
      default:         sortOption = { year: -1, title: 1 };
    }
    
    const cursor = collection.find().sort(sortOption).skip(skip).limit(limit);
    return cursor.toArray();
  }

  async findById(id) {
    const collection = this.getCollection();

    let paper = await collection.findOne({ paper_id: id });
    if (paper) return paper;

    if (ObjectId.isValid(id)) {
      paper = await collection.findOne({ _id: new ObjectId(id) });
      if (paper) return paper;
    }

    paper = await collection.findOne({ paper_id: { $regex: `^${id}$`, $options: 'i' } });
    return paper || null;
  }

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
      sortBy = 'relevance',
      highlyCollaborative = false
    } = params;

    const filter = {};

    if (highlyCollaborative) {
      filter.is_important = true;
    }

    if (yearFrom || yearTo) {
      filter.year = {};
      if (yearFrom) filter.year.$gte = parseInt(yearFrom);
      if (yearTo) filter.year.$lte = parseInt(yearTo);
    }

    if (journal)      filter.journal   = { $regex: journal, $options: 'i' };
    if (author)       filter.authors   = { $regex: author, $options: 'i' };
    if (minCitations) filter.citation_count = { $gte: parseInt(minCitations) };
    if (keywords)     filter.keywords  = { $regex: keywords, $options: 'i' };
    if (abstract)     filter.abstract  = { $regex: abstract, $options: 'i' };
    if (doi)          filter.doi       = doi;

    if (query) {
      return this.broadSearch({
        query,
        baseFilter: filter,
        limit,
        offset,
        sortBy,
      });
    }

    const total = await collection.countDocuments(filter);

    let sortOption = { year: -1, title: 1 };
    switch (sortBy) {
      case 'recent':    sortOption = { year: -1, title: 1 }; break;
      case 'oldest':    sortOption = { year: 1, title: 1 }; break;
      case 'title':     sortOption = { title: 1 }; break;
      case 'citations': sortOption = { citation_count: -1, year: -1 }; break;
      case 'relevance':
        sortOption = (query && filter.$text)
          ? { score: { $meta: 'textScore' }, year: -1 }
          : { year: -1, title: 1 };
        break;
      default: sortOption = { year: -1, title: 1 };
    }

    let cursor;
    if (query && filter.$text && sortBy === 'relevance') {
      cursor = collection
        .find(filter, { score: { $meta: 'textScore' } })
        .sort(sortOption).skip(offset).limit(limit);
    } else {
      cursor = collection.find(filter).sort(sortOption).skip(offset).limit(limit);
    }

    const papers = await cursor.toArray();
    return { papers, total };
  }

  async broadSearch({ query, baseFilter = {}, limit = 20, offset = 0, sortBy = 'relevance' }) {
    const collection = this.getCollection();
    const safeLimit = Math.max(1, Math.min(parseInt(limit, 10) || 20, 100));
    const safeOffset = Math.max(0, parseInt(offset, 10) || 0);
    const escapedQuery = escapeRegex(query);
    const textFilter = { ...baseFilter, $text: { $search: query } };

    try {
      const textCursor = collection
        .find(
          textFilter,
          {
            projection: {
              paper_id: 1,
              title: 1,
              abstract: 1,
              authors: 1,
              doi: 1,
              has_full_text: 1,
              is_covid19: 1,
              journal: 1,
              sha: 1,
              source: 1,
              year: 1,
              citation_count: 1,
              keywords: 1,
              score: { $meta: 'textScore' },
            },
          }
        )
        .sort({ score: { $meta: 'textScore' }, year: -1 })
        .skip(safeOffset)
        .limit(safeLimit);

      const [papers, total] = await Promise.all([
        textCursor.toArray(),
        collection.countDocuments(textFilter),
      ]);

      if (papers.length > 0) {
        return { papers, total };
      }
    } catch {
      // Fall through to regex search if the text index is missing or invalid.
    }

    const regexFilter = {
      ...baseFilter,
      $or: [
        { title: { $regex: escapedQuery, $options: 'i' } },
        { abstract: { $regex: escapedQuery, $options: 'i' } },
        { authors: { $regex: escapedQuery, $options: 'i' } },
        { journal: { $regex: escapedQuery, $options: 'i' } },
        { keywords: { $regex: escapedQuery, $options: 'i' } },
        { doi: { $regex: escapedQuery, $options: 'i' } },
      ],
    };

    const papers = await collection
      .find(regexFilter)
      .sort(sortBy === 'title' ? { title: 1 } : { year: -1, title: 1 })
      .skip(safeOffset)
      .limit(safeLimit)
      .maxTimeMS(15000)
      .toArray();

    return { papers, total: safeOffset + papers.length + (papers.length === safeLimit ? 1 : 0) };
  }

  async searchText(query, limit = 50) {
    const collection = this.getCollection();
    try {
      const cursor = collection
        .find({ $text: { $search: query } }, { score: { $meta: 'textScore' } })
        .sort({ score: { $meta: 'textScore' } })
        .limit(limit);
      return cursor.toArray();
    } catch (error) {
      const cursor = collection.find({
        $or: [
          { title: { $regex: query, $options: 'i' } },
          { abstract: { $regex: query, $options: 'i' } }
        ]
      }).limit(limit);
      return cursor.toArray();
    }
  }

  async getByYear(year) {
    const collection = this.getCollection();
    return collection.find({ year: parseInt(year) }).sort({ title: 1 }).toArray();
  }

  async getByYearRange(yearFrom, yearTo) {
    const collection = this.getCollection();
    return collection.find({
      year: { $gte: parseInt(yearFrom), $lte: parseInt(yearTo) }
    }).sort({ year: -1, title: 1 }).toArray();
  }

  async getByJournal(journal) {
    const collection = this.getCollection();
    return collection.find({ journal: { $regex: journal, $options: 'i' } })
      .sort({ year: -1 }).toArray();
  }

  async getByAuthor(authorName) {
    const collection = this.getCollection();
    return collection.find({ authors: { $regex: authorName, $options: 'i' } })
      .sort({ year: -1 }).toArray();
  }

  async getCovid19Papers(limit = 50) {
    const collection = this.getCollection();
    return collection.find({ is_covid19: true }).sort({ year: -1 }).limit(limit).toArray();
  }

  async getFilterOptions() {
    const collection = this.getCollection();

    const [yearStats, journalStats, keywordStats] = await Promise.all([
      collection.distinct('year').then(years =>
        years.filter(y => y != null).sort((a, b) => b - a)
      ),
      collection.aggregate([
        { $group: { _id: '$journal', count: { $sum: 1 } } },
        { $sort: { count: -1 } },
        { $limit: 50 }
      ]).toArray(),
      collection.aggregate([
        { $unwind: '$keywords' },
        { $group: { _id: '$keywords', count: { $sum: 1 } } },
        { $sort: { count: -1 } },
        { $limit: 50 }
      ]).toArray()
    ]);

    const yearRange = await collection.aggregate([
      { $group: { _id: null, minYear: { $min: '$year' }, maxYear: { $max: '$year' } } }
    ]).toArray();

    return {
      years: yearStats,
      journals: journalStats.map(j => ({ name: j._id, count: j.count })),
      keywords: keywordStats.map(k => ({ name: k._id, count: k.count })),
      yearRange: yearRange[0] || { minYear: 2000, maxYear: 2024 }
    };
  }

  async getSuggestions(query, type = 'all') {
    const collection = this.getCollection();
    const suggestions = [];

    if (type === 'all' || type === 'title') {
      const titles = await collection
        .find({ title: { $regex: `^${query}`, $options: 'i' } }, { projection: { title: 1 } })
        .limit(5).toArray();
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
      const kws = await collection.aggregate([
        { $unwind: '$keywords' },
        { $match: { keywords: { $regex: `^${query}`, $options: 'i' } } },
        { $group: { _id: '$keywords' } },
        { $limit: 5 }
      ]).toArray();
      suggestions.push(...kws.map(k => ({ type: 'keyword', value: k._id })));
    }

    return suggestions;
  }

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
                covid19Papers: { $sum: { $cond: ['$is_covid19', 1, 0] } },
                papersWithFullText: { $sum: { $cond: ['$has_full_text', 1, 0] } },
                avgCitationCount: { $avg: { $ifNull: ['$citation_count', 0] } }
              }
            }
          ],
          uniqueJournals: [
            { $group: { _id: '$journal' } },
            { $count: 'count' }
          ],
          uniqueAuthors: [
            { $unwind: '$authors' },
            { $group: { _id: '$authors' } },
            { $count: 'count' }
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

  async getPapersPerYear() {
    const collection = this.getCollection();
    return collection.aggregate([
      { $group: { _id: '$year', count: { $sum: 1 }, covidCount: { $sum: { $cond: ['$is_covid19', 1, 0] } } } },
      { $sort: { _id: 1 } }
    ]).toArray();
  }

  async getTopJournals(limit = 10) {
    const collection = this.getCollection();
    return collection.aggregate([
      { $group: { _id: '$journal', count: { $sum: 1 }, avgCitations: { $avg: { $ifNull: ['$citation_count', 0] } } } },
      { $sort: { count: -1 } },
      { $limit: limit },
      { $project: { journal: '$_id', count: 1, avgCitations: { $round: ['$avgCitations', 2] } } }
    ]).toArray();
  }

  async getTopAuthors(limit = 10) {
    const collection = this.getCollection();
    return collection.aggregate([
      { $unwind: '$authors' },
      { $group: { _id: '$authors', count: { $sum: 1 }, totalCitations: { $sum: { $ifNull: ['$citation_count', 0] } } } },
      { $sort: { count: -1 } },
      { $limit: limit },
      {
        $project: {
          author: '$_id', count: 1,
          avgCitations: {
            $round: [{ $cond: [{ $eq: ['$count', 0] }, 0, { $divide: ['$totalCitations', '$count'] }] }, 2]
          }
        }
      }
    ]).toArray();
  }

  async getAuthorInsights(authorName, limit = 5) {
    const collection = this.getCollection();
    const safeLimit = Math.max(1, Math.min(parseInt(limit, 10) || 5, 20));
    const escaped = authorName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const authorRegex = new RegExp(`^${escaped}$`, 'i');

    const [summary] = await collection.aggregate([
      { $match: { authors: authorRegex } },
      {
        $facet: {
          totals: [
            {
              $group: {
                _id: null,
                paper_count: { $sum: 1 },
                total_citations: { $sum: { $ifNull: ['$citation_count', 0] } },
                avg_citations: { $avg: { $ifNull: ['$citation_count', 0] } },
                first_year: { $min: '$year' },
                latest_year: { $max: '$year' },
              },
            },
          ],
          top_keywords: [
            { $unwind: '$keywords' },
            { $group: { _id: '$keywords', count: { $sum: 1 } } },
            { $sort: { count: -1, _id: 1 } },
            { $limit: safeLimit },
          ],
          top_journals: [
            { $match: { journal: { $nin: [null, ''] } } },
            { $group: { _id: '$journal', count: { $sum: 1 } } },
            { $sort: { count: -1, _id: 1 } },
            { $limit: safeLimit },
          ],
          recent_papers: [
            { $sort: { year: -1, citation_count: -1, title: 1 } },
            { $limit: safeLimit },
            {
              $project: {
                _id: 0,
                paper_id: 1,
                title: 1,
                journal: 1,
                year: 1,
                citation_count: 1,
                keywords: { $slice: [{ $ifNull: ['$keywords', []] }, 5] },
              },
            },
          ],
        },
      },
    ]).toArray();

    const totals = summary?.totals?.[0] || {};
    return {
      paper_count: totals.paper_count || 0,
      total_citations: totals.total_citations || 0,
      avg_citations: Math.round((totals.avg_citations || 0) * 100) / 100,
      first_year: totals.first_year || null,
      latest_year: totals.latest_year || null,
      top_keywords: (summary?.top_keywords || []).map(item => ({ keyword: item._id, count: item.count })),
      top_journals: (summary?.top_journals || []).map(item => ({ journal: item._id, count: item.count })),
      recent_papers: summary?.recent_papers || [],
    };
  }

  async getDistinct(field) {
    const collection = this.getCollection();
    return collection.distinct(field);
  }

  async getCovid19Stats() {
    const collection = this.getCollection();
    return collection.aggregate([
      { $match: { is_covid19: true } },
      {
        $facet: {
          byYear: [{ $group: { _id: '$year', count: { $sum: 1 } } }, { $sort: { _id: 1 } }],
          byJournal: [{ $group: { _id: '$journal', count: { $sum: 1 } } }, { $sort: { count: -1 } }, { $limit: 10 }],
          bySource: [{ $group: { _id: '$source', count: { $sum: 1 } } }, { $sort: { count: -1 } }]
        }
      }
    ]).toArray().then(r => r[0] || {});
  }

  async getByMultipleCriteria(criteria) {
    const collection = this.getCollection();
    const { years, journals, authors, keywords, limit = 100, skip = 0 } = criteria;
    const filter = {};
    if (years?.length)    filter.year    = { $in: years.map(y => parseInt(y)) };
    if (journals?.length) filter.journal = { $in: journals };
    if (authors?.length)  filter.authors = { $in: authors };
    if (keywords?.length) filter.keywords = { $in: keywords };
    return collection.find(filter).sort({ year: -1, title: 1 }).skip(skip).limit(limit).toArray();
  }

  async update(paper_id, updates) {
    const collection = this.getCollection();
    return collection.updateOne({ paper_id }, { $set: { ...updates, updated_at: new Date() } });
  }

  async delete(paper_id) {
    const collection = this.getCollection();
    return collection.deleteOne({ paper_id });
  }
}

module.exports = PaperDocument;
