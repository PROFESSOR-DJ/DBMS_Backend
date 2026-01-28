const { getMySQL } = require('../../config/database');

class AuthorModel {
  // Create author
  static async create(author) {
    const { name } = author;
    const query = 'INSERT INTO author (name) VALUES (?)';
    const [result] = await (await getMySQL()).execute(query, [name]);
    return result;
  }

  // Get all authors - FIX: Convert limit and offset to integers
  static async findAll(limit = 100, offset = 0) {
    const query = 'SELECT * FROM author ORDER BY name LIMIT ? OFFSET ?';
    const limitInt = parseInt(limit, 10);
    const offsetInt = parseInt(offset, 10);
    const [rows] = await (await getMySQL()).execute(query, [limitInt, offsetInt]);
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

  // Search authors by name - FIX: Convert limit to integer
  static async searchByName(name, limit = 50) {
    const query = 'SELECT * FROM author WHERE name LIKE ? ORDER BY name LIMIT ?';
    const limitInt = parseInt(limit, 10);
    const [rows] = await (await getMySQL()).execute(query, [`%${name}%`, limitInt]);
    return rows;
  }

  // Get top authors by paper count - FIX: Convert limit to integer
  static async getTopAuthors(limit = 10) {
    const query = `
      SELECT a.author_id, a.name, COUNT(pa.paper_id) as paper_count
      FROM author a
      LEFT JOIN paper_author pa ON a.author_id = pa.author_id
      GROUP BY a.author_id, a.name
      ORDER BY paper_count DESC
      LIMIT ?
    `;
    const limitInt = parseInt(limit, 10);
    const [rows] = await (await getMySQL()).execute(query, [limitInt]);
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