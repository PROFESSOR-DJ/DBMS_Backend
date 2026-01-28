const { getMySQL } = require('../../config/database');

class PaperAuthorModel {
  // Create paper-author relationship
  static async create(paper_id, author_id) {
    const query = 'INSERT INTO paper_author (paper_id, author_id) VALUES (?, ?)';
    const [result] = await (await getMySQL()).execute(query, [paper_id, author_id]);
    return result;
  }

  // Get authors of a paper
  static async getAuthorsByPaper(paper_id) {
    const query = `
      SELECT a.author_id, a.name
      FROM author a
      JOIN paper_author pa ON a.author_id = pa.author_id
      WHERE pa.paper_id = ?
    `;
    const [rows] = await (await getMySQL()).execute(query, [paper_id]);
    return rows;
  }

  // Get papers by an author
  static async getPapersByAuthor(author_id) {
    const query = `
      SELECT p.paper_id, p.title, p.year, p.journal
      FROM paper p
      JOIN paper_author pa ON p.paper_id = pa.paper_id
      WHERE pa.author_id = ?
      ORDER BY p.year DESC
    `;
    const [rows] = await (await getMySQL()).execute(query, [author_id]);
    return rows;
  }

  // Check if relationship exists
  static async exists(paper_id, author_id) {
    const query = 'SELECT 1 FROM paper_author WHERE paper_id = ? AND author_id = ?';
    const [rows] = await (await getMySQL()).execute(query, [paper_id, author_id]);
    return rows.length > 0;
  }
}

module.exports = PaperAuthorModel;