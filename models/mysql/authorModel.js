const { getMySQL } = require('../../config/database');

class AuthorModel {
  // Create author
  static async create(author) {
    const { name } = author;
    const query = 'INSERT INTO author (name) VALUES (?)';
    const [result] = await (await getMySQL()).execute(query, [name]);
    return result;
  }

  // Get all authors
  static async findAll(limit = 100, offset = 0) {
    const query = 'SELECT * FROM author ORDER BY name LIMIT ? OFFSET ?';
    const [rows] = await (await getMySQL()).execute(query, [limit, offset]);
    return rows;
  }

  // Get author by ID
  static async findById(author_id) {
    const query = 'SELECT * FROM author WHERE author_id = ?';
    const [rows] = await (await getMySQL()).execute(query, [author_id]);
    return rows[0];
  }

  // Get author by name
  static async findByName(name) {
    const query = 'SELECT * FROM author WHERE name = ?';
    const [rows] = await (await getMySQL()).execute(query, [name]);
    return rows[0];
  }

  // Search authors by name
  static async searchByName(name, limit = 50) {
    const query = 'SELECT * FROM author WHERE name LIKE ? ORDER BY name LIMIT ?';
    const [rows] = await (await getMySQL()).execute(query, [`%${name}%`, limit]);
    return rows;
  }

  // Get top authors by paper count
  static async getTopAuthors(limit = 10) {
    const query = `
      SELECT a.author_id, a.name, COUNT(pa.paper_id) as paper_count
      FROM author a
      LEFT JOIN paper_author pa ON a.author_id = pa.author_id
      GROUP BY a.author_id, a.name
      ORDER BY paper_count DESC
      LIMIT ?
    `;
    const [rows] = await (await getMySQL()).execute(query, [limit]);
    return rows;
  }

  // Get total count
  static async count() {
    const query = 'SELECT COUNT(*) as count FROM author';
    const [rows] = await (await getMySQL()).execute(query);
    return rows[0].count;
  }
}

module.exports = AuthorModel;