const { getMongoDB } = require('../../config/database');

class PaperDocument {
  constructor() {
    this.collection = getMongoDB().collection('papers');
  }

  // Create paper document matching your schema
  async create(paper) {
    // Ensure all required fields from your schema
    const paperDoc = {
      paper_id: paper.paper_id || `paper_${Date.now()}`,
      title: paper.title || '',
      abstract: paper.abstract || '',
      authors: paper.authors || [],
      doi: paper.doi || '',
      has_full_text: paper.has_full_text || false,
      is_covid19: paper.is_covid19 || false,
      journal: paper.journal || '',
      sha: paper.sha || '',
      source: paper.source || 'manual',
      year: paper.year || new Date().getFullYear(),
      created_at: new Date(),
      updated_at: new Date(),
      // Optional fields that might exist
      citation_count: paper.citation_count || 0,
      keywords: paper.keywords || []
    };
    
    const result = await this.collection.insertOne(paperDoc);
    return result;
  }

  // Get all paper documents
  async findAll(limit = 100, skip = 0) {
    const cursor = this.collection.find().sort({ year: -1 }).skip(skip).limit(limit);
    return cursor.toArray();
  }

  // Get paper by ID
  async findById(paper_id) {
    return this.collection.findOne({ paper_id: paper_id });
  }

  // Search papers by text (using MongoDB text index)
  async searchText(query, limit = 50) {
    try {
      const cursor = this.collection.find(
        { $text: { $search: query } },
        { score: { $meta: "textScore" } }
      ).sort({ score: { $meta: "textScore" } }).limit(limit);
      return cursor.toArray();
    } catch (error) {
      // Fallback to regex search if text index is not available
      console.log('Text search failed, falling back to regex:', error.message);
      const cursor = this.collection.find({
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
    const cursor = this.collection.find({ year: parseInt(year) }).sort({ title: 1 });
    return cursor.toArray();
  }

  // Get papers by journal
  async getByJournal(journal) {
    const cursor = this.collection.find({ journal: journal }).sort({ year: -1 });
    return cursor.toArray();
  }

  // Get papers by author
  async getByAuthor(authorName) {
    const cursor = this.collection.find({ authors: authorName }).sort({ year: -1 });
    return cursor.toArray();
  }

  // Get papers by COVID-19 flag
  async getCovid19Papers(limit = 50) {
    const cursor = this.collection.find({ is_covid19: true }).sort({ year: -1 }).limit(limit);
    return cursor.toArray();
  }

  // Get aggregated statistics
  async getStats() {
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
                avgCitationCount: { $avg: "$citation_count" }
              }
            }
          ],
          yearStats: [
            {
              $group: {
                _id: "$year",
                count: { $sum: 1 }
              }
            },
            { $sort: { _id: 1 } }
          ],
          journalStats: [
            {
              $group: {
                _id: "$journal",
                count: { $sum: 1 }
              }
            },
            { $sort: { count: -1 } },
            { $limit: 10 }
          ],
          authorStats: [
            { $unwind: "$authors" },
            {
              $group: {
                _id: "$authors",
                count: { $sum: 1 }
              }
            },
            { $sort: { count: -1 } },
            { $limit: 10 }
          ]
        }
      }
    ];
    
    const result = await this.collection.aggregate(pipeline).toArray();
    const stats = result[0] || {};
    
    return {
      totalPapers: stats.totalStats?.[0]?.totalPapers || 0,
      covid19Papers: stats.totalStats?.[0]?.covid19Papers || 0,
      papersWithFullText: stats.totalStats?.[0]?.papersWithFullText || 0,
      avgCitationCount: stats.totalStats?.[0]?.avgCitationCount || 0,
      papersPerYear: stats.yearStats || [],
      topJournals: stats.journalStats || [],
      topAuthors: stats.authorStats || []
    };
  }

  // Get papers per year
  async getPapersPerYear() {
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
    return this.collection.aggregate(pipeline).toArray();
  }

  // Get top journals
  async getTopJournals(limit = 10) {
    const pipeline = [
      {
        $group: {
          _id: "$journal",
          count: { $sum: 1 },
          avgCitations: { $avg: "$citation_count" }
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
    return this.collection.aggregate(pipeline).toArray();
  }

  // Get top authors
  async getTopAuthors(limit = 10) {
    const pipeline = [
      {
        $unwind: "$authors"
      },
      {
        $group: {
          _id: "$authors",
          count: { $sum: 1 },
          totalCitations: { $sum: "$citation_count" }
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
          avgCitations: { $round: [{ $divide: ["$totalCitations", "$count"] }, 2] }
        }
      }
    ];
    return this.collection.aggregate(pipeline).toArray();
  }

  // Get distinct values for a field
  async getDistinct(field) {
    return this.collection.distinct(field);
  }

  // Get COVID-19 research statistics
  async getCovid19Stats() {
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
    
    const result = await this.collection.aggregate(pipeline).toArray();
    return result[0] || {};
  }
}

module.exports = PaperDocument;