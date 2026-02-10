const { getMySQL } = require('../../config/database');

class PaperModel {
  // Create paper
  static async create(paper) {
    const connection = await getMySQL();
    const { paper_id, title, year, journal } = paper;
    const query = 'INSERT INTO paper (paper_id, title, year, journal) VALUES (?, ?, ?, ?)';
    const [result] = await connection.execute(query, [paper_id, title, year, journal]);
    return result;
  }

  // Get all papers with sorting
  static async findAll(limit = 100, offset = 0, sortBy = 'recent') {
    const connection = await getMySQL();
    
    // ✅ FIX: Validate and convert to integers
    const limitInt = parseInt(limit, 10);
    const offsetInt = parseInt(offset, 10);
    
    if (!Number.isInteger(limitInt) || !Number.isInteger(offsetInt)) {
      throw new Error('Invalid pagination parameters');
    }
    
    let orderBy = 'year DESC, title ASC';
    
    switch (sortBy) {
      case 'recent':
        orderBy = 'year DESC, title ASC';
        break;
      case 'oldest':
        orderBy = 'year ASC, title ASC';
        break;
      case 'title':
        orderBy = 'title ASC';
        break;
      case 'journal':
        orderBy = 'journal ASC, year DESC';
        break;
      default:
        orderBy = 'year DESC, title ASC';
    }
    
    // ✅ FIX: Use query() with inline integers instead of execute()
    const sql = `SELECT * FROM paper ORDER BY ${orderBy} LIMIT ${limitInt} OFFSET ${offsetInt}`;
    const [rows] = await connection.query(sql);
    return rows;
  }

  // Get paper by ID
  static async findById(paper_id) {
    const connection = await getMySQL();
    const query = 'SELECT * FROM paper WHERE paper_id = ?';
    const [rows] = await connection.execute(query, [paper_id]);
    return rows[0];
  }

  // Advanced search with multiple filters
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

    let sql = 'SELECT DISTINCT p.* FROM paper p';
    let conditions = [];
    let queryParams = [];

    // Join with author tables if author filter is applied
    if (author) {
      sql += ` 
        LEFT JOIN paper_author pa ON p.paper_id = pa.paper_id
        LEFT JOIN author a ON pa.author_id = a.author_id
      `;
    }

    sql += ' WHERE 1=1';

    // Text search on title
    if (query) {
      conditions.push('p.title LIKE ?');
      queryParams.push(`%${query}%`);
    }

    // Year range filter
    if (yearFrom) {
      conditions.push('p.year >= ?');
      queryParams.push(parseInt(yearFrom, 10));
    }

    if (yearTo) {
      conditions.push('p.year <= ?');
      queryParams.push(parseInt(yearTo, 10));
    }

    // Journal filter
    if (journal) {
      conditions.push('p.journal LIKE ?');
      queryParams.push(`%${journal}%`);
    }

    // Author filter
    if (author) {
      conditions.push('a.name LIKE ?');
      queryParams.push(`%${author}%`);
    }

    // Add conditions to query
    if (conditions.length > 0) {
      sql += ' AND ' + conditions.join(' AND ');
    }

    // Sorting
    let orderBy = 'p.year DESC, p.title ASC';
    switch (sortBy) {
      case 'recent':
        orderBy = 'p.year DESC, p.title ASC';
        break;
      case 'oldest':
        orderBy = 'p.year ASC, p.title ASC';
        break;
      case 'title':
        orderBy = 'p.title ASC';
        break;
      case 'citations':
        orderBy = 'p.year DESC'; // MySQL doesn't have citation_count
        break;
      case 'relevance':
        if (query) {
          orderBy = 'p.year DESC, p.title ASC';
        } else {
          orderBy = 'p.year DESC, p.title ASC';
        }
        break;
      default:
        orderBy = 'p.year DESC, p.title ASC';
    }

    // Get total count
    const countSql = sql.replace('SELECT DISTINCT p.*', 'SELECT COUNT(DISTINCT p.paper_id) as total');
    const [countResult] = await connection.execute(countSql, queryParams);
    const total = countResult[0].total;

    // Add sorting and pagination
    sql += ` ORDER BY ${orderBy} LIMIT ? OFFSET ?`;
    queryParams.push(parseInt(limit, 10));
    queryParams.push(parseInt(offset, 10));

    const [rows] = await connection.execute(sql, queryParams);

    return {
      papers: rows,
      total
    };
  }

  // Search papers by title
  static async searchByTitle(title, limit = 50) {
    const connection = await getMySQL();
    const query = 'SELECT * FROM paper WHERE title LIKE ? ORDER BY year DESC LIMIT ?';
    const limitInt = parseInt(limit, 10);
    const [rows] = await connection.execute(query, [`%${title}%`, limitInt]);
    return rows;
  }

  // Get papers by year
  static async getByYear(year) {
    const connection = await getMySQL();
    const query = 'SELECT * FROM paper WHERE year = ? ORDER BY title';
    const [rows] = await connection.execute(query, [year]);
    return rows;
  }

  // Get papers by journal
  static async getByJournal(journal) {
    const connection = await getMySQL();
    const query = 'SELECT * FROM paper WHERE journal LIKE ? ORDER BY year DESC';
    const [rows] = await connection.execute(query, [`%${journal}%`]);
    return rows;
  }

  // Get filter options (for dropdowns)
  static async getFilterOptions() {
    const connection = await getMySQL();

    // Get available years
    const [years] = await connection.execute(
      'SELECT DISTINCT year FROM paper WHERE year IS NOT NULL ORDER BY year DESC'
    );

    // Get available journals
    const [journals] = await connection.execute(
      'SELECT DISTINCT journal FROM paper WHERE journal IS NOT NULL ORDER BY journal ASC LIMIT 100'
    );

    // Get year range
    const [yearRange] = await connection.execute(
      'SELECT MIN(year) as min_year, MAX(year) as max_year FROM paper'
    );

    return {
      years: years.map(y => y.year),
      journals: journals.map(j => j.journal),
      yearRange: yearRange[0]
    };
  }

  // Get search suggestions
  static async getSuggestions(query, type = 'all') {
    const connection = await getMySQL();
    const suggestions = [];

    if (type === 'all' || type === 'title') {
      const [titles] = await connection.execute(
        'SELECT DISTINCT title FROM paper WHERE title LIKE ? ORDER BY title LIMIT 5',
        [`${query}%`]
      );
      suggestions.push(...titles.map(t => ({ type: 'title', value: t.title })));
    }

    if (type === 'all' || type === 'journal') {
      const [journals] = await connection.execute(
        'SELECT DISTINCT journal FROM paper WHERE journal LIKE ? ORDER BY journal LIMIT 5',
        [`${query}%`]
      );
      suggestions.push(...journals.map(j => ({ type: 'journal', value: j.journal })));
    }

    if (type === 'all' || type === 'author') {
      const [authors] = await connection.execute(
        'SELECT DISTINCT name FROM author WHERE name LIKE ? ORDER BY name LIMIT 5',
        [`${query}%`]
      );
      suggestions.push(...authors.map(a => ({ type: 'author', value: a.name })));
    }

    return suggestions;
  }

  // Get total count
  static async count() {
    const connection = await getMySQL();
    const query = 'SELECT COUNT(*) as count FROM paper';
    const [rows] = await connection.execute(query);
    return rows[0].count;
  }

  // Get years with paper count
  static async getYearStats() {
    const connection = await getMySQL();
    const query = 'SELECT year, COUNT(*) as count FROM paper GROUP BY year ORDER BY year DESC';
    const [rows] = await connection.execute(query);
    return rows;
  }

  // Get papers by year range
  static async getByYearRange(yearFrom, yearTo) {
    const connection = await getMySQL();
    const query = 'SELECT * FROM paper WHERE year BETWEEN ? AND ? ORDER BY year DESC, title ASC';
    const [rows] = await connection.execute(query, [
      parseInt(yearFrom, 10),
      parseInt(yearTo, 10)
    ]);
    return rows;
  }

  // Get recent papers
  static async getRecent(limit = 20) {
    const connection = await getMySQL();
    const query = 'SELECT * FROM paper ORDER BY year DESC, title ASC LIMIT ?';
    const [rows] = await connection.execute(query, [parseInt(limit, 10)]);
    return rows;
  }

  // Get papers by multiple criteria
  static async getByMultipleCriteria(criteria) {
    const connection = await getMySQL();
    const { years, journals, authors, limit = 100, offset = 0 } = criteria;
    
    let sql = 'SELECT DISTINCT p.* FROM paper p';
    let conditions = [];
    let params = [];

    if (authors && authors.length > 0) {
      sql += ' LEFT JOIN paper_author pa ON p.paper_id = pa.paper_id';
      sql += ' LEFT JOIN author a ON pa.author_id = a.author_id';
      
      const authorConditions = authors.map(() => 'a.name = ?').join(' OR ');
      conditions.push(`(${authorConditions})`);
      params.push(...authors);
    }

    if (years && years.length > 0) {
      const yearConditions = years.map(() => 'p.year = ?').join(' OR ');
      conditions.push(`(${yearConditions})`);
      params.push(...years.map(y => parseInt(y, 10)));
    }

    if (journals && journals.length > 0) {
      const journalConditions = journals.map(() => 'p.journal = ?').join(' OR ');
      conditions.push(`(${journalConditions})`);
      params.push(...journals);
    }

    if (conditions.length > 0) {
      sql += ' WHERE ' + conditions.join(' AND ');
    }

    sql += ' ORDER BY p.year DESC, p.title ASC LIMIT ? OFFSET ?';
    params.push(parseInt(limit, 10), parseInt(offset, 10));

    const [rows] = await connection.execute(sql, params);
    return rows;
  }
}

module.exports = PaperModel;