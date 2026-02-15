const { getMySQL } = require('../../config/database');

class PaperModel {
  // Create paper
  static async create(paper) {
    const connection = await getMySQL();
    const { paper_id, title, year, journal } = paper;

    // Lookup journal_id
    let journal_id = null;
    if (journal) {
      const [rows] = await connection.execute('SELECT journal_id FROM journals WHERE journal_name = ?', [journal]);
      if (rows.length > 0) journal_id = rows[0].journal_id;
    }

    const query = 'INSERT INTO papers (paper_id, title, publish_year, journal_id) VALUES (?, ?, ?, ?)';
    const [result] = await connection.execute(query, [paper_id, title, year, journal_id]);
    return result;
  }

  // Get all papers with sorting - OPTIMIZED
  static async findAll(limit = 100, offset = 0, sortBy = 'recent') {
    const connection = await getMySQL();

    // ✅ FIX: Validate and convert to integers
    const limitInt = parseInt(limit, 10);
    const offsetInt = parseInt(offset, 10);

    if (!Number.isInteger(limitInt) || !Number.isInteger(offsetInt)) {
      throw new Error('Invalid pagination parameters');
    }

    let orderBy = 'p.publish_year DESC, p.title ASC';

    switch (sortBy) {
      case 'recent':
        orderBy = 'p.publish_year DESC, p.title ASC';
        break;
      case 'oldest':
        orderBy = 'p.publish_year ASC, p.title ASC';
        break;
      case 'title':
        orderBy = 'p.title ASC';
        break;
      case 'journal':
        orderBy = 'j.journal_name ASC, p.publish_year DESC';
        break;
      default:
        orderBy = 'p.publish_year DESC, p.title ASC';
    }

    // ✅ FIX: Use query() with inline integers instead of execute()
    // Join with journals and sources to get names
    const sql = `
      SELECT p.*, p.publish_year as year, j.journal_name as journal, s.source_name as source,
             GROUP_CONCAT(a.author_name ORDER BY pa.author_order SEPARATOR ', ') as authors
      FROM papers p
      LEFT JOIN journals j ON p.journal_id = j.journal_id
      LEFT JOIN sources s ON p.source_id = s.source_id
      LEFT JOIN paper_authors pa ON p.paper_id = pa.paper_id
      LEFT JOIN authors a ON pa.author_id = a.author_id
      GROUP BY p.paper_id
      ORDER BY ${orderBy} 
      LIMIT ${limitInt} OFFSET ${offsetInt}
    `;
    const [rows] = await connection.query(sql);
    return rows;
  }

  // Get paper by ID
  static async findById(paper_id) {
    const connection = await getMySQL();
    const query = `
      SELECT p.*, p.publish_year as year, j.journal_name as journal, s.source_name as source,
             GROUP_CONCAT(a.author_name ORDER BY pa.author_order SEPARATOR ', ') as authors
      FROM papers p
      LEFT JOIN journals j ON p.journal_id = j.journal_id
      LEFT JOIN sources s ON p.source_id = s.source_id
      LEFT JOIN paper_authors pa ON p.paper_id = pa.paper_id
      LEFT JOIN authors a ON pa.author_id = a.author_id
      WHERE p.paper_id = ?
      GROUP BY p.paper_id
    `;
    const [rows] = await connection.execute(query, [paper_id]);
    return rows[0];
  }

  // Advanced search with multiple filters - OPTIMIZED
  static async advancedSearch(params) {
    const connection = await getMySQL();
    const {
      query,
      yearFrom,
      yearTo,
      journal,
      author,
      limit = 20,
      offset = 0,
      sortBy = 'relevance'
    } = params;

    let sql = `
      SELECT p.*, p.publish_year as year, j.journal_name as journal, s.source_name as source,
             GROUP_CONCAT(a.author_name ORDER BY pa.author_order SEPARATOR ', ') as authors
      FROM papers p
      LEFT JOIN journals j ON p.journal_id = j.journal_id
      LEFT JOIN sources s ON p.source_id = s.source_id
      LEFT JOIN paper_authors pa ON p.paper_id = pa.paper_id
      LEFT JOIN authors a ON pa.author_id = a.author_id
    `;
    let conditions = [];
    let queryParams = [];

    // Note: Joins are already added above for selection, we don't need to add them again for filtering
    // but the logic below conditionally added them. 
    // Since we now ALWAYS join for selection, we can remove the conditional join logic or ensure we don't duplicate.
    // However, if we group by paper_id, we need a.author_name usage in filtering to work correctly (it does)

    /*
    // Join with author tables if author filter is applied
    if (author) {
       // already joined
    }
    */

    sql += ' WHERE 1=1';

    // Text search on title
    if (query) {
      conditions.push('p.title LIKE ?');
      queryParams.push(`%${query}%`);
    }

    // Year range filter
    if (yearFrom) {
      conditions.push('p.publish_year >= ?');
      queryParams.push(parseInt(yearFrom, 10));
    }

    if (yearTo) {
      conditions.push('p.publish_year <= ?');
      queryParams.push(parseInt(yearTo, 10));
    }

    // Journal filter
    if (journal) {
      conditions.push('j.journal_name LIKE ?');
      queryParams.push(`%${journal}%`);
    }

    // Author filter
    if (author) {
      conditions.push('a.author_name LIKE ?'); // Updated to author_name
      queryParams.push(`%${author}%`);
    }

    // Add conditions to query
    if (conditions.length > 0) {
      sql += ' AND ' + conditions.join(' AND ');
    }

    // Get total count
    const countSql = sql.replace(/SELECT DISTINCT .* FROM/, 'SELECT COUNT(DISTINCT p.paper_id) as total FROM');
    // Note: complex replace might be needed if sql structure varies, but here it's consistent

    // For safer count, we can wrap or just rebuild:
    const sqlCount = `
      SELECT COUNT(DISTINCT p.paper_id) as total 
      FROM papers p
      LEFT JOIN journals j ON p.journal_id = j.journal_id
      LEFT JOIN sources s ON p.source_id = s.source_id
      ${author ? 'LEFT JOIN paper_authors pa ON p.paper_id = pa.paper_id LEFT JOIN authors a ON pa.author_id = a.author_id' : ''}
      WHERE 1=1 
      ${conditions.length > 0 ? 'AND ' + conditions.join(' AND ') : ''}
    `;

    const [countResult] = await connection.execute(sqlCount, queryParams);
    const total = countResult[0].total;

    // Sorting
    let orderBy = 'p.publish_year DESC, p.title ASC';
    switch (sortBy) {
      case 'recent':
        orderBy = 'p.publish_year DESC, p.title ASC';
        break;
      case 'oldest':
        orderBy = 'p.publish_year ASC, p.title ASC';
        break;
      case 'title':
        orderBy = 'p.title ASC';
        break;
      case 'citations':
        orderBy = 'p.publish_year DESC'; // MySQL doesn't have citation_count
        break;
      case 'relevance':
        orderBy = 'p.publish_year DESC, p.title ASC';
        break;
      default:
        orderBy = 'p.publish_year DESC, p.title ASC';
    }

    // Add sorting and pagination - OPTIMIZED
    const limitInt = parseInt(limit, 10);
    const offsetInt = parseInt(offset, 10);

    if (!Number.isInteger(limitInt) || !Number.isInteger(offsetInt)) {
      throw new Error('Invalid pagination parameters');
    }

    sql += ` GROUP BY p.paper_id ORDER BY ${orderBy} LIMIT ${limitInt} OFFSET ${offsetInt}`;
    const [rows] = await connection.query(sql, queryParams);

    return {
      papers: rows,
      total
    };
  }

  // Search papers by title - OPTIMIZED
  static async searchByTitle(title, limit = 50) {
    const connection = await getMySQL();
    const limitInt = parseInt(limit, 10);

    if (!Number.isInteger(limitInt)) {
      throw new Error('Invalid limit parameter');
    }

    const sql = `
      SELECT p.*, p.publish_year as year, j.journal_name as journal, s.source_name as source,
             GROUP_CONCAT(a.author_name ORDER BY pa.author_order SEPARATOR ', ') as authors
      FROM papers p
      LEFT JOIN journals j ON p.journal_id = j.journal_id
      LEFT JOIN sources s ON p.source_id = s.source_id
      LEFT JOIN paper_authors pa ON p.paper_id = pa.paper_id
      LEFT JOIN authors a ON pa.author_id = a.author_id
      WHERE p.title LIKE ? 
      GROUP BY p.paper_id
      ORDER BY p.publish_year DESC 
      LIMIT ${limitInt}
    `;
    const [rows] = await connection.query(sql, [`%${title}%`]);
    return rows;
  }

  // Get papers by year
  static async getByYear(year) {
    const connection = await getMySQL();
    const query = `
      SELECT p.*, p.publish_year as year, j.journal_name as journal, s.source_name as source,
             GROUP_CONCAT(a.author_name ORDER BY pa.author_order SEPARATOR ', ') as authors
      FROM papers p
      LEFT JOIN journals j ON p.journal_id = j.journal_id
      LEFT JOIN sources s ON p.source_id = s.source_id
      LEFT JOIN paper_authors pa ON p.paper_id = pa.paper_id
      LEFT JOIN authors a ON pa.author_id = a.author_id
      WHERE p.publish_year = ? 
      GROUP BY p.paper_id
      ORDER BY p.title
    `;
    const [rows] = await connection.execute(query, [year]);
    return rows;
  }

  // Get papers by year range
  static async getByYearRange(yearFrom, yearTo) {
    const connection = await getMySQL();
    const query = `
      SELECT p.*, j.journal_name as journal, s.source_name as source
      FROM papers p
      LEFT JOIN journals j ON p.journal_id = j.journal_id
      LEFT JOIN sources s ON p.source_id = s.source_id
      WHERE p.publish_year BETWEEN ? AND ? 
      ORDER BY p.publish_year DESC, p.title ASC
    `;
    const [rows] = await connection.execute(query, [
      parseInt(yearFrom, 10),
      parseInt(yearTo, 10)
    ]);
    return rows;
  }

  // Get papers by journal
  static async getByJournal(journal) {
    const connection = await getMySQL();
    const query = `
      SELECT p.*, p.publish_year as year, j.journal_name as journal, s.source_name as source,
             GROUP_CONCAT(a.author_name ORDER BY pa.author_order SEPARATOR ', ') as authors
      FROM papers p
      LEFT JOIN journals j ON p.journal_id = j.journal_id
      LEFT JOIN sources s ON p.source_id = s.source_id
      LEFT JOIN paper_authors pa ON p.paper_id = pa.paper_id
      LEFT JOIN authors a ON pa.author_id = a.author_id
      WHERE j.journal_name LIKE ? 
      GROUP BY p.paper_id
      ORDER BY p.publish_year DESC
    `;
    const [rows] = await connection.execute(query, [`%${journal}%`]);
    return rows;
  }

  // Get filter options (for dropdowns) - OPTIMIZED
  static async getFilterOptions() {
    const connection = await getMySQL();

    // Run all queries in parallel for optimization
    const [years, journals, yearRange] = await Promise.all([
      // Get available years
      connection.execute(
        'SELECT DISTINCT publish_year FROM papers WHERE publish_year IS NOT NULL ORDER BY publish_year DESC'
      ).then(([rows]) => rows),

      // Get available journals (limited to top 100)
      connection.execute(
        'SELECT DISTINCT j.journal_name FROM journals j WHERE j.journal_name IS NOT NULL ORDER BY j.journal_name ASC LIMIT 100'
      ).then(([rows]) => rows),

      // Get year range
      connection.execute(
        'SELECT MIN(publish_year) as min_year, MAX(publish_year) as max_year FROM papers'
      ).then(([rows]) => rows)
    ]);

    return {
      years: years.map(y => y.publish_year),
      journals: journals.map(j => j.journal_name),
      yearRange: yearRange[0]
    };
  }

  // Get search suggestions - OPTIMIZED
  static async getSuggestions(query, type = 'all') {
    const connection = await getMySQL();
    const suggestions = [];

    if (type === 'all' || type === 'title') {
      const [titles] = await connection.execute(
        'SELECT DISTINCT title FROM papers WHERE title LIKE ? ORDER BY title LIMIT 5',
        [`${query}%`]
      );
      suggestions.push(...titles.map(t => ({ type: 'title', value: t.title })));
    }

    if (type === 'all' || type === 'journal') {
      const [journals] = await connection.execute(
        'SELECT DISTINCT journal_name FROM journals WHERE journal_name LIKE ? ORDER BY journal_name LIMIT 5',
        [`${query}%`]
      );
      suggestions.push(...journals.map(j => ({ type: 'journal', value: j.journal_name })));
    }

    if (type === 'all' || type === 'author') {
      const [authors] = await connection.execute(
        'SELECT DISTINCT author_name FROM authors WHERE author_name LIKE ? ORDER BY author_name LIMIT 5',
        [`${query}%`]
      );
      suggestions.push(...authors.map(a => ({ type: 'author', value: a.author_name })));
    }

    return suggestions;
  }

  // Get total count
  static async count() {
    const connection = await getMySQL();
    const query = 'SELECT COUNT(*) as count FROM papers';
    const [rows] = await connection.execute(query);
    return rows[0].count;
  }

  // Get years with paper count - OPTIMIZED with GROUP BY
  static async getYearStats() {
    const connection = await getMySQL();
    const query = `
      SELECT publish_year, COUNT(*) as count 
      FROM papers 
      WHERE publish_year IS NOT NULL
      GROUP BY publish_year 
      ORDER BY publish_year DESC
    `;
    const [rows] = await connection.execute(query);
    return rows;
  }

  // Get top journals - OPTIMIZED
  static async getTopJournals(limit = 10) {
    const connection = await getMySQL();
    const limitInt = parseInt(limit, 10);
    const sql = `
      SELECT j.journal_name as journal, COUNT(p.paper_id) as count
      FROM papers p
      JOIN journals j ON p.journal_id = j.journal_id
      GROUP BY j.journal_id, j.journal_name
      ORDER BY count DESC
      LIMIT ${limitInt}
    `;
    const [rows] = await connection.query(sql);
    return rows;
  }

  // Get recent papers - OPTIMIZED
  static async getRecent(limit = 20) {
    const connection = await getMySQL();
    const limitInt = parseInt(limit, 10);

    if (!Number.isInteger(limitInt)) {
      throw new Error('Invalid limit parameter');
    }

    const sql = `
      SELECT p.*, p.publish_year as year, j.journal_name as journal, s.source_name as source,
             GROUP_CONCAT(a.author_name ORDER BY pa.author_order SEPARATOR ', ') as authors
      FROM papers p
      LEFT JOIN journals j ON p.journal_id = j.journal_id
      LEFT JOIN sources s ON p.source_id = s.source_id
      LEFT JOIN paper_authors pa ON p.paper_id = pa.paper_id
      LEFT JOIN authors a ON pa.author_id = a.author_id
      GROUP BY p.paper_id
      ORDER BY p.publish_year DESC, p.title ASC 
      LIMIT ${limitInt}
    `;
    const [rows] = await connection.query(sql);
    return rows;
  }

  // Get papers by multiple criteria - OPTIMIZED
  static async getByMultipleCriteria(criteria) {
    const connection = await getMySQL();
    const { years, journals, authors, limit = 100, offset = 0 } = criteria;

    let sql = `
      SELECT p.*, p.publish_year as year, j.journal_name as journal, s.source_name as source,
             GROUP_CONCAT(a.author_name ORDER BY pa.author_order SEPARATOR ', ') as authors
      FROM papers p
      LEFT JOIN journals j ON p.journal_id = j.journal_id
      LEFT JOIN sources s ON p.source_id = s.source_id
      LEFT JOIN paper_authors pa ON p.paper_id = pa.paper_id
      LEFT JOIN authors a ON pa.author_id = a.author_id
    `;
    let conditions = [];
    let params = [];

    // Authors join already added
    /*
    if (authors && authors.length > 0) {
      // ...
    }
    */

    if (authors && authors.length > 0) {
      const authorConditions = authors.map(() => 'a.author_name = ?').join(' OR '); // Updated to author_name
      conditions.push(`(${authorConditions})`);
      params.push(...authors);
    }

    if (years && years.length > 0) {
      const yearConditions = years.map(() => 'p.publish_year = ?').join(' OR ');
      conditions.push(`(${yearConditions})`);
      params.push(...years.map(y => parseInt(y, 10)));
    }

    if (journals && journals.length > 0) {
      const journalConditions = journals.map(() => 'j.journal_name = ?').join(' OR ');
      conditions.push(`(${journalConditions})`);
      params.push(...journals);
    }

    if (conditions.length > 0) {
      sql += ' WHERE ' + conditions.join(' AND ');
    }

    const limitInt = parseInt(limit, 10);
    const offsetInt = parseInt(offset, 10);

    if (!Number.isInteger(limitInt) || !Number.isInteger(offsetInt)) {
      throw new Error('Invalid pagination parameters');
    }

    sql += ` GROUP BY p.paper_id ORDER BY p.publish_year DESC, p.title ASC LIMIT ${limitInt} OFFSET ${offsetInt}`;
    const [rows] = await connection.query(sql, params);
    return rows;
  }

  // OPTIMIZED: Update paper (only update changed fields)
  static async update(paper_id, updates) {
    const connection = await getMySQL();
    const fields = [];
    const values = [];

    if (updates.title !== undefined) {
      fields.push('title = ?');
      values.push(updates.title);
    }
    if (updates.year !== undefined) {
      fields.push('publish_year = ?');
      values.push(parseInt(updates.year, 10));
    }
    if (updates.journal !== undefined) {
      // We'd need to lookup journal_id here too, but for now assuming simple update not supported or complicated
      // fields.push('journal_id = ?'); 
    }

    if (fields.length === 0) {
      return { affectedRows: 0 };
    }

    values.push(paper_id);
    const query = `UPDATE papers SET ${fields.join(', ')} WHERE paper_id = ?`;
    const [result] = await connection.execute(query, values);
    return result;
  }

  // OPTIMIZED: Delete paper (cascade handled by FK constraints)
  static async delete(paper_id) {
    const connection = await getMySQL();
    const query = 'DELETE FROM papers WHERE paper_id = ?';
    const [result] = await connection.execute(query, [paper_id]);
    return result;
  }
}

module.exports = PaperModel;