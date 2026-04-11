// journalModel manages journal search/detail with Scimago-like ranking metadata.
const { getMySQL } = require('../../config/database');

class JournalModel {
  static async searchJournals({ q = '', country, oa, quartile, sortBy = 'rank', limit = 20, offset = 0 }) {
    const connection = await getMySQL();
    const params = [];
    const filters = [];

    if (q && q.trim()) {
      filters.push('(jr.title LIKE ? OR jr.publisher LIKE ? OR jr.best_subject_area LIKE ?)');
      const pattern = `%${q.trim()}%`;
      params.push(pattern, pattern, pattern);
    }

    if (country && country.trim()) {
      filters.push('jr.country = ?');
      params.push(country.trim());
    }

    if (oa !== undefined) {
      filters.push('jr.oa = ?');
      params.push(oa ? 1 : 0);
    }

    if (quartile && quartile.trim()) {
      filters.push('jr.best_quartile = ?');
      params.push(quartile.trim().toUpperCase());
    }

    const whereClause = filters.length ? `WHERE ${filters.join(' AND ')}` : '';

    let orderBy = 'jr.sjr_rank ASC';
    if (sortBy === 'sjr') orderBy = 'jr.sjr_index DESC';
    if (sortBy === 'citescore') orderBy = 'jr.citescore DESC';
    if (sortBy === 'hindex') orderBy = 'jr.h_index DESC';
    if (sortBy === 'title') orderBy = 'jr.title ASC';

    const limitInt = Number.parseInt(limit, 10);
    const offsetInt = Number.parseInt(offset, 10);

    const sql = `
      SELECT
        jr.journal_ranking_id,
        jr.sjr_rank AS journal_rank,
        jr.title,
        jr.oa,
        jr.country,
        jr.sjr_index,
        jr.citescore,
        jr.h_index,
        jr.best_quartile,
        jr.best_subject_area,
        jr.publisher,
        COALESCE(p.paper_count, 0) AS local_paper_count
      FROM journal_rankings jr
      LEFT JOIN (
        SELECT j.journal_name, COUNT(p.paper_id) AS paper_count
        FROM journals j
        LEFT JOIN papers p ON p.journal_id = j.journal_id
        GROUP BY j.journal_name
      ) p ON LOWER(p.journal_name) = LOWER(jr.title)
      ${whereClause}
      ORDER BY ${orderBy}
      LIMIT ${limitInt} OFFSET ${offsetInt}
    `;

    const [rows] = await connection.query(sql, params);
    return rows.map((row) => ({
      ...row,
      rank: row.journal_rank,
    }));
  }

  static async countSearch({ q = '', country, oa, quartile }) {
    const connection = await getMySQL();
    const params = [];
    const filters = [];

    if (q && q.trim()) {
      filters.push('(title LIKE ? OR publisher LIKE ? OR best_subject_area LIKE ?)');
      const pattern = `%${q.trim()}%`;
      params.push(pattern, pattern, pattern);
    }
    if (country && country.trim()) {
      filters.push('country = ?');
      params.push(country.trim());
    }
    if (oa !== undefined) {
      filters.push('oa = ?');
      params.push(oa ? 1 : 0);
    }
    if (quartile && quartile.trim()) {
      filters.push('best_quartile = ?');
      params.push(quartile.trim().toUpperCase());
    }

    const whereClause = filters.length ? `WHERE ${filters.join(' AND ')}` : '';
    const [rows] = await connection.query(
      `SELECT COUNT(*) AS total FROM journal_rankings ${whereClause}`,
      params
    );
    return rows[0].total;
  }

  static async getJournalProfile(journalId) {
    const connection = await getMySQL();

    const [journalRows] = await connection.execute(
      'SELECT * FROM journal_rankings WHERE journal_ranking_id = ? OR title = ? LIMIT 1',
      [journalId, journalId]
    );

    if (!journalRows.length) {
      return null;
    }

    const journal = journalRows[0];

    const [papers] = await connection.execute(
      `
      SELECT
        p.paper_id,
        p.title,
        p.publish_year AS year,
        j.journal_name,
        GROUP_CONCAT(a.author_name ORDER BY pa.author_order SEPARATOR ', ') AS authors
      FROM papers p
      LEFT JOIN journals j ON j.journal_id = p.journal_id
      LEFT JOIN paper_authors pa ON pa.paper_id = p.paper_id
      LEFT JOIN authors a ON a.author_id = pa.author_id
      WHERE LOWER(j.journal_name) = LOWER(?)
      GROUP BY p.paper_id, p.title, p.publish_year, j.journal_name
      ORDER BY p.publish_year DESC, p.title ASC
      LIMIT 100
      `,
      [journal.title]
    );

    const [topAuthors] = await connection.execute(
      `
      SELECT
        a.author_id,
        a.author_name,
        COUNT(pa.paper_id) AS paper_count
      FROM authors a
      JOIN paper_authors pa ON pa.author_id = a.author_id
      JOIN papers p ON p.paper_id = pa.paper_id
      JOIN journals j ON j.journal_id = p.journal_id
      WHERE LOWER(j.journal_name) = LOWER(?)
      GROUP BY a.author_id, a.author_name
      ORDER BY paper_count DESC, a.author_name ASC
      LIMIT 20
      `,
      [journal.title]
    );

    return { journal, papers, topAuthors };
  }
}

module.exports = JournalModel;
