// authorModel manages backend MySQL author records.
// Active triggers relevant to this model:
//   trg_before_author_delete — BEFORE DELETE on authors:
//       raises SQLSTATE 45000 if author still has paper links.
//       The old manual paper-count check in JS is removed; the trigger handles it.

const { getMySQL } = require('../../config/database');

class AuthorModel {

  // ── CREATE ────────────────────────────────────────────────────────────────
  // When paper_id is supplied:
  //   1. Inserts the author row.
  //   2. Links author to the paper via paper_authors.
  //   3. trg_after_paper_authors_insert fires → increments paper_metrics.author_count.
  //   4. If author_count reaches 5, trg_mark_important_paper sets papers.is_important=TRUE.
  static async create(author) {
    const { author_name, paper_id } = author;
    const pool       = getMySQL();
    const connection = await pool.getConnection();

    try {
      if (paper_id) {
        await connection.beginTransaction();
        try {
          const [result] = await connection.execute(
            'INSERT INTO authors (author_name) VALUES (?)',
            [author_name]
          );
          const author_id = result.insertId;

          const [rows] = await connection.execute(
            'SELECT MAX(author_order) AS max_order FROM paper_authors WHERE paper_id = ?',
            [paper_id]
          );
          const nextOrder = (rows[0].max_order || 0) + 1;

          // trg_after_paper_authors_insert fires here
          await connection.execute(
            'INSERT INTO paper_authors (paper_id, author_id, author_order) VALUES (?, ?, ?)',
            [paper_id, author_id, nextOrder]
          );

          await connection.commit();
          return result;
        } catch (error) {
          await connection.rollback();
          throw error;
        }
      } else {
        const [result] = await connection.execute(
          'INSERT INTO authors (author_name) VALUES (?)',
          [author_name]
        );
        return result;
      }
    } finally {
      connection.release();
    }
  }

  // ── UPDATE ────────────────────────────────────────────────────────────────
  // trg_update_last_login is on the users table, not authors — no trigger here.
  static async update(author_id, updates) {
    const { author_name } = updates;
    const [result] = await (await getMySQL()).execute(
      'UPDATE authors SET author_name = ? WHERE author_id = ?',
      [author_name, author_id]
    );
    return result;
  }

  // ── DELETE ────────────────────────────────────────────────────────────────
  // The old JS pre-check (manual COUNT of paper_authors) has been removed.
  // trg_before_author_delete handles the guard and raises a descriptive error
  // (SQLSTATE 45000) which propagates to the error handler as BUSINESS_RULE_VIOLATION.
  static async delete(author_id) {
    const [result] = await (await getMySQL()).execute(
      'DELETE FROM authors WHERE author_id = ?',
      [author_id]
    );
    return result;
  }

  // ── FIND ALL ──────────────────────────────────────────────────────────────
  static async findAll(limit = 100, offset = 0, sortBy = 'recent') {
    const limitInt  = parseInt(limit,  10);
    const offsetInt = parseInt(offset, 10);
    if (!Number.isInteger(limitInt) || !Number.isInteger(offsetInt)) {
      throw new Error('Invalid pagination parameters');
    }

    let orderBy = 'a.created_at DESC, a.author_id DESC';
    switch (sortBy) {
      case 'name':
        orderBy = 'a.author_name ASC';
        break;
      case 'papers':
        orderBy = 'paper_count DESC, a.author_name ASC';
        break;
      case 'recent':
      default:
        orderBy = 'a.created_at DESC, a.author_id DESC';
        break;
    }

    const sql = `
      SELECT a.*, COUNT(pa.paper_id) AS paper_count
      FROM   authors a
      LEFT JOIN paper_authors pa ON pa.author_id = a.author_id
      GROUP BY a.author_id
      ORDER BY ${orderBy}
      LIMIT ${limitInt} OFFSET ${offsetInt}
    `;
    const [rows] = await (await getMySQL()).query(sql);
    return rows;
  }

  // ── FIND BY ID ────────────────────────────────────────────────────────────
  static async findById(author_id) {
    const [rows] = await (await getMySQL()).execute(
      'SELECT * FROM authors WHERE author_id = ?',
      [author_id]
    );
    return rows[0];
  }

  // ── FIND BY NAME (exact) ──────────────────────────────────────────────────
  static async findByName(author_name) {
    const [rows] = await (await getMySQL()).execute(
      'SELECT * FROM authors WHERE author_name = ?',
      [author_name]
    );
    return rows[0];
  }

  // ── SEARCH BY NAME ────────────────────────────────────────────────────────
  static async searchByName(author_name, limit = 50) {
    const limitInt = parseInt(limit, 10);
    if (!Number.isInteger(limitInt)) throw new Error('Invalid limit parameter');

    const sql = `
      SELECT a.*, COUNT(pa.paper_id) AS paper_count
      FROM   authors a
      LEFT JOIN paper_authors pa ON a.author_id = pa.author_id
      WHERE  a.author_name LIKE ?
      GROUP BY a.author_id
      ORDER BY paper_count DESC, a.author_name ASC
      LIMIT ${limitInt}
    `;
    const [rows] = await (await getMySQL()).query(sql, [`%${author_name}%`]);
    return rows;
  }

  // ── GET PAPERS BY AUTHOR ──────────────────────────────────────────────────
  static async getPapersByAuthor(author_name) {
    const sql = `
      SELECT p.title, p.paper_id, p.publish_year
      FROM   paper_authors pa
      JOIN   papers  p ON pa.paper_id  = p.paper_id
      JOIN   authors a ON pa.author_id = a.author_id
      WHERE  a.author_name = ?
      ORDER BY p.publish_year DESC
    `;
    const [rows] = await (await getMySQL()).execute(sql, [author_name]);
    return rows;
  }

  // ── GET TOP AUTHORS ───────────────────────────────────────────────────────
  // Uses the same heuristic as before: aggregate on the junction table first,
  // then JOIN to authors for names. This mirrors GetAuthorImpact but lets the
  // model honour the limit parameter without altering the stored procedure.
  static async getTopAuthors(limit = 10) {
    const limitInt = parseInt(limit, 10);
    if (!Number.isInteger(limitInt)) throw new Error('Invalid limit parameter');

    const sql = `
      SELECT a.author_id, a.author_name AS name, COUNT(pa.paper_id) AS paper_count
      FROM   authors a
      LEFT JOIN paper_authors pa ON a.author_id = pa.author_id
      GROUP BY a.author_id, a.author_name
      ORDER BY paper_count DESC
      LIMIT ${limitInt}
    `;
    const [rows] = await (await getMySQL()).query(sql);
    return rows;
  }

  // ── COUNT ─────────────────────────────────────────────────────────────────
  static async count() {
    const [rows] = await (await getMySQL()).execute('SELECT COUNT(*) AS count FROM authors');
    return rows[0].count;
  }
}

module.exports = AuthorModel;
