const { getMySQL } = require('../../config/database');

class AuthorModel {
  // Create author
  // Create author with optional paper link
  static async create(author) {
    const { author_name, paper_id } = author;
    const pool = getMySQL();
    const connection = await pool.getConnection();

    try {
      if (paper_id) {
        await connection.beginTransaction();
        try {
          // 1. Create Author
          const [result] = await connection.execute('INSERT INTO authors (author_name) VALUES (?)', [author_name]);
          const author_id = result.insertId;

          // 2. Link object to Paper
          // Get next author_order
          const [rows] = await connection.execute('SELECT MAX(author_order) as max_order FROM paper_authors WHERE paper_id = ?', [paper_id]);
          const nextOrder = (rows[0].max_order || 0) + 1;

          await connection.execute('INSERT INTO paper_authors (paper_id, author_id, author_order) VALUES (?, ?, ?)',
            [paper_id, author_id, nextOrder]);

          await connection.commit();
          return result;
        } catch (error) {
          await connection.rollback();
          throw error;
        }
      } else {
        const query = 'INSERT INTO authors (author_name) VALUES (?)';
        const [result] = await connection.execute(query, [author_name]);
        return result;
      }
    } finally {
      connection.release();
    }
  }

  // Update author
  static async update(author_id, updates) {
    const { author_name } = updates;
    const query = 'UPDATE authors SET author_name = ? WHERE author_id = ?';
    const [result] = await (await getMySQL()).execute(query, [author_name, author_id]);
    return result;
  }

  // Delete author
  static async delete(author_id) {
    const query = 'DELETE FROM authors WHERE author_id = ?';
    const [result] = await (await getMySQL()).execute(query, [author_id]);
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
      SELECT a.*, COUNT(pa.paper_id) as paper_count
      FROM authors a
      LEFT JOIN paper_authors pa ON a.author_id = pa.author_id
      WHERE a.author_name LIKE ? 
      GROUP BY a.author_id
      ORDER BY paper_count DESC, a.author_name ASC
      LIMIT ${limitInt}
    `;
    const [rows] = await (await getMySQL()).query(sql, [`%${author_name}%`]);
    return rows;
  }

  // Get papers of an author
  static async getPapersByAuthor(author_name) {
    const sql = `
        SELECT p.title, p.paper_id, p.publish_year
        FROM paper_authors pa
        JOIN papers p ON pa.paper_id = p.paper_id
        JOIN authors a ON pa.author_id = a.author_id
        WHERE a.author_name = ?
        ORDER BY p.publish_year DESC
      `;
    const [rows] = await (await getMySQL()).execute(sql, [author_name]);
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