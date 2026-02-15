const { getMySQL } = require('../../config/database');

class AuthorModel {
  // Create author
  static async create(author) {
    const { author_name } = author;
    const query = 'INSERT INTO authors (author_name) VALUES (?)';
    const [result] = await (await getMySQL()).execute(query, [author_name]);
    return result;
  }

  // Get all authors
  static async findAll(limit = 100, offset = 0) {
    const limitInt = parseInt(limit, 10);
    const offsetInt = parseInt(offset, 10);

    if (!Number.isInteger(limitInt) || !Number.isInteger(offsetInt)) {
      throw new Error('Invalid pagination parameters');
    }

    const sql = `
      SELECT * FROM authors 
      ORDER BY author_name 
      LIMIT ${limitInt} OFFSET ${offsetInt}
    `;
    const [rows] = await (await getMySQL()).query(sql);
    return rows;
  }

  // Get author by ID
  static async findById(author_id) {
    const query = 'SELECT * FROM authors WHERE author_id = ?';
    const [rows] = await (await getMySQL()).execute(query, [author_id]);
    return rows[0];
  }

  // Get author by name
  static async findByName(author_name) {
    const query = 'SELECT * FROM authors WHERE author_name = ?';
    const [rows] = await (await getMySQL()).execute(query, [author_name]);
    return rows[0];
  }

  // Search authors by name
  static async searchByName(author_name, limit = 50) {
    const limitInt = parseInt(limit, 10);

    if (!Number.isInteger(limitInt)) {
      throw new Error('Invalid limit parameter');
    }

    const sql = `
      SELECT * FROM authors 
      WHERE author_name LIKE ? 
      ORDER BY author_name 
      LIMIT ${limitInt}
    `;
    const [rows] = await (await getMySQL()).query(sql, [`%${author_name}%`]);
    return rows;
  }

  // Get top authors by paper count
  static async getTopAuthors(limit = 10) {
    const limitInt = parseInt(limit, 10);

    if (!Number.isInteger(limitInt)) {
      throw new Error('Invalid limit parameter');
    }

    const sql = `
      SELECT a.author_id, a.author_name as name, COUNT(pa.paper_id) as paper_count
      FROM authors a
      LEFT JOIN paper_authors pa ON a.author_id = pa.author_id
      GROUP BY a.author_id, a.author_name
      ORDER BY paper_count DESC
      LIMIT ${limitInt}
    `;
    const [rows] = await (await getMySQL()).query(sql);
    return rows;
  }

  // Get total count
  static async count() {
    const query = 'SELECT COUNT(*) as count FROM authors';
    const [rows] = await (await getMySQL()).execute(query);
    return rows[0].count;
  }
}

module.exports = AuthorModel;