const { getMongoDB } = require('../../config/database');

class PaperDocument {
  // Use lazy initialization - don't try to get collection in constructor
  getCollection() {
    try {
      const db = getMongoDB();
      return db.collection('papers');
    } catch (error) {
      throw new Error('MongoDB not available: ' + error.message);
    }
  }

  // Create paper document matching your schema
  async create(paper) {
    const collection = this.getCollection();
    
    // Ensure all required fields from your schema
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
      // Optional fields
      ...(paper.citation_count && { citation_count: paper.citation_count }),
      ...(paper.keywords && { keywords: paper.keywords }),
      ...(paper.created_at && { created_at: paper.created_at }),
      ...(paper.updated_at && { updated_at: paper.updated_at })
    };
    
    const result = await collection.insertOne(paperDoc);
    return result;
  }

  // Get all paper documents
  async findAll(limit = 100, skip = 0) {
    const collection = this.getCollection();
    const cursor = collection.find().sort({ year: -1 }).skip(skip).limit(limit);
    return cursor.toArray();
  }

  // Get paper by ID
  async findById(paper_id) {
    const collection = this.getCollection();
    return collection.findOne({ paper_id: paper_id });
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
      // Fallback to regex search if text index is not available
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

  // Get papers by journal
  async getByJournal(journal) {
    const collection = this.getCollection();
    const cursor = collection.find({ journal: journal }).sort({ year: -1 });
    return cursor.toArray();
  }

  // Get papers by author
  async getByAuthor(authorName) {
    const collection = this.getCollection();
    const cursor = collection.find({ authors: authorName }).sort({ year: -1 });
    return cursor.toArray();
  }

  // Get papers by COVID-19 flag
  async getCovid19Papers(limit = 50) {
    const collection = this.getCollection();
    const cursor = collection.find({ is_covid19: true }).sort({ year: -1 }).limit(limit);
    return cursor.toArray();
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
}

module.exports = PaperDocument;