const { getMySQL } = require('../../config/database');

class PaperModel {
  // Create paper
  static async create(paper) {
    const { paper_id, title, year, journal } = paper;
    const query = 'INSERT INTO paper (paper_id, title, year, journal) VALUES (?, ?, ?, ?)';
    const [result] = await (await getMySQL()).execute(query, [paper_id, title, year, journal]);
    return result;
  }

  // Get all papers
  static async findAll(limit = 100, offset = 0) {
    const query = 'SELECT * FROM paper ORDER BY year DESC LIMIT ? OFFSET ?';
    const [rows] = await (await getMySQL()).execute(query, [limit, offset]);
    return rows;
  }

  // Get paper by ID
  static async findById(paper_id) {
    const query = 'SELECT * FROM paper WHERE paper_id = ?';
    const [rows] = await (await getMySQL()).execute(query, [paper_id]);
    return rows[0];
  }

  // Search papers by title
  static async searchByTitle(title, limit = 50) {
    const query = 'SELECT * FROM paper WHERE title LIKE ? ORDER BY year DESC LIMIT ?';
    const [rows] = await (await getMySQL()).execute(query, [`%${title}%`, limit]);
    return rows;
  }

  // Get papers by year
  static async getByYear(year) {
    const query = 'SELECT * FROM paper WHERE year = ? ORDER BY title';
    const [rows] = await (await getMySQL()).execute(query, [year]);
    return rows;
  }

  // Get papers by journal
  static async getByJournal(journal) {
    const query = 'SELECT * FROM paper WHERE journal = ? ORDER BY year DESC';
    const [rows] = await (await getMySQL()).execute(query, [journal]);
    return rows;
  }

  // Get total count
  static async count() {
    const query = 'SELECT COUNT(*) as count FROM paper';
    const [rows] = await (await getMySQL()).execute(query);
    return rows[0].count;
  }

  // Get years with paper count
  static async getYearStats() {
    const query = 'SELECT year, COUNT(*) as count FROM paper GROUP BY year ORDER BY year DESC';
    const [rows] = await (await getMySQL()).execute(query);
    return rows;
  }
}

module.exports = PaperModel;