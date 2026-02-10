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
  // ✅ FIX: Use query() with inline numbers for LIMIT/OFFSET
  static async findAll(limit = 100, offset = 0) {
    if (!Number.isInteger(limit) || !Number.isInteger(offset)) {
      throw new Error('Invalid pagination parameters');
    }
    
    const sql = `
      SELECT * FROM author 
      ORDER BY name 
      LIMIT ${limit} OFFSET ${offset}
    `;
    const [rows] = await (await getMySQL()).query(sql);
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
  // ✅ FIX: Use query() with inline limit
  static async searchByName(name, limit = 50) {
    if (!Number.isInteger(limit)) {
      throw new Error('Invalid limit parameter');
    }
    
    const sql = `
      SELECT * FROM author 
      WHERE name LIKE ? 
      ORDER BY name 
      LIMIT ${limit}
    `;
    const [rows] = await (await getMySQL()).query(sql, [`%${name}%`]);
    return rows;
  }

  // Get top authors by paper count
  // ✅ FIX: Use query() with inline limit - this is the critical one causing your error!
  static async getTopAuthors(limit = 10) {
    if (!Number.isInteger(limit)) {
      throw new Error('Invalid limit parameter');
    }
    
    // Inline the limit - safe because we validated it's an integer
    const sql = `
      SELECT a.author_id, a.name, COUNT(pa.paper_id) as paper_count
      FROM author a
      LEFT JOIN paper_author pa ON a.author_id = pa.author_id
      GROUP BY a.author_id, a.name
      ORDER BY paper_count DESC
      LIMIT ${limit}
    `;
    const [rows] = await (await getMySQL()).query(sql);
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