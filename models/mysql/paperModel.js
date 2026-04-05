// paperModel manages backend MySQL paper records.
// Active triggers on this model's tables:
//   trg_validate_paper        — BEFORE INSERT on papers: rejects title < 5 chars (raises SQLSTATE 45000)
//   trg_update_journal_count  — AFTER INSERT on papers: increments journals.paper_count automatically
//   trg_after_paper_insert    — AFTER INSERT on papers: inserts paper_metrics row
//   trg_mark_important_paper  — AFTER INSERT on paper_metrics: sets is_important=TRUE when author_count >= 5
//   trg_after_paper_authors_insert/delete — keep paper_metrics.author_count in sync

const { getMySQL } = require('../../config/database');

class PaperModel {

  // ── CREATE ────────────────────────────────────────────────────────────────
  // Notes on triggers that fire automatically during create:
  //  1. trg_validate_paper       — rejects the INSERT if title is NULL or < 5 chars.
  //  2. trg_after_paper_insert   — creates the paper_metrics row (author_count=0).
  //  3. trg_update_journal_count — increments journals.paper_count for this journal.
  //  4. trg_mark_important_paper — fires after paper_metrics insert; sets is_important
  //                                when author_count >= 5 (won't apply at creation time
  //                                since author links are added after the paper row).
  static async create(paper) {
    const pool = getMySQL();
    const connection = await pool.getConnection();
    const { paper_id, title, year, journal, doi, is_covid19, has_full_text, authors, abstract } = paper;
    const normalisedJournal = journal ? String(journal).trim() : '';
    const normalisedAuthors = Array.isArray(authors)
      ? authors
          .map((author) => String(author || '').trim())
          .filter(Boolean)
          .filter((author, index, list) => list.indexOf(author) === index)
      : [];
    const createMeta = {
      paper_id,
      journal_id: null,
      createdJournalId: null,
      author_ids: [],
      createdAuthorIds: [],
    };

    try {
      await connection.beginTransaction();

      // Resolve or create journal
      let journal_id = null;
      if (normalisedJournal) {
        const [rows] = await connection.execute(
          'SELECT journal_id FROM journals WHERE journal_name = ?',
          [normalisedJournal]
        );
        if (rows.length > 0) {
          journal_id = rows[0].journal_id;
        } else {
          const [jResult] = await connection.execute(
            'INSERT INTO journals (journal_name) VALUES (?)',
            [normalisedJournal]
          );
          journal_id = jResult.insertId;
          createMeta.createdJournalId = journal_id;
        }
      }
      createMeta.journal_id = journal_id;

      // INSERT papers — trg_validate_paper fires here (rejects short titles),
      //                 trg_after_paper_insert fires after (creates paper_metrics),
      //                 trg_update_journal_count fires after (bumps journals.paper_count).
      const query = `
        INSERT INTO papers (paper_id, title, abstract, publish_year, doi, journal_id, is_covid19, has_full_text)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      `;
      const [result] = await connection.execute(query, [
        paper_id, title, abstract, year, doi, journal_id,
        is_covid19 || false, has_full_text || false,
      ]);

      // paper_metrics row is created by trg_after_paper_insert with author_count=0.
      // We do NOT insert it manually here — that would duplicate the trigger's work.
      // The explicit INSERT in the old code has been removed.

      // Insert authors — each INSERT into paper_authors fires
      // trg_after_paper_authors_insert, which increments paper_metrics.author_count.
      // When author_count reaches 5, trg_mark_important_paper sets is_important=TRUE.
      if (normalisedAuthors.length > 0) {
        for (let i = 0; i < normalisedAuthors.length; i++) {
          const authorName = normalisedAuthors[i];
          let author_id;

          const [aRows] = await connection.execute(
            'SELECT author_id FROM authors WHERE author_name = ?',
            [authorName]
          );
          if (aRows.length > 0) {
            author_id = aRows[0].author_id;
          } else {
            const [aResult] = await connection.execute(
              'INSERT INTO authors (author_name) VALUES (?)',
              [authorName]
            );
            author_id = aResult.insertId;
            createMeta.createdAuthorIds.push(author_id);
          }
          createMeta.author_ids.push(author_id);

          await connection.execute(
            'INSERT INTO paper_authors (paper_id, author_id, author_order) VALUES (?, ?, ?)',
            [paper_id, author_id, i + 1]
          );
          // trg_after_paper_authors_insert fires here → updates paper_metrics.author_count
          // When count hits 5, trg_mark_important_paper sets papers.is_important = TRUE
        }
      }

      await connection.commit();
      return {
        result,
        ...createMeta,
      };
    } catch (error) {
      await connection.rollback();
      throw error;
    } finally {
      connection.release();
    }
  }

  static async compensateCreate(createContext = {}) {
    const pool = getMySQL();
    const connection = await pool.getConnection();
    const {
      paper_id,
      journal_id = null,
      createdJournalId = null,
      createdAuthorIds = [],
    } = createContext;

    try {
      await connection.beginTransaction();

      if (paper_id) {
        await connection.execute('DELETE FROM papers WHERE paper_id = ?', [paper_id]);
      }

      if (createdAuthorIds.length > 0) {
        const authorPlaceholders = createdAuthorIds.map(() => '?').join(', ');
        const [orphanRows] = await connection.query(
          `
            SELECT a.author_id
            FROM authors a
            LEFT JOIN paper_authors pa ON pa.author_id = a.author_id
            WHERE a.author_id IN (${authorPlaceholders})
            GROUP BY a.author_id
            HAVING COUNT(pa.paper_id) = 0
          `,
          createdAuthorIds
        );

        if (orphanRows.length > 0) {
          const orphanIds = orphanRows.map((row) => row.author_id);
          const orphanPlaceholders = orphanIds.map(() => '?').join(', ');
          await connection.query(
            `DELETE FROM authors WHERE author_id IN (${orphanPlaceholders})`,
            orphanIds
          );
        }
      }

      const journalToRefresh = createdJournalId || journal_id;
      if (journalToRefresh) {
        const [[journalUsage]] = await connection.execute(
          'SELECT COUNT(*) AS paper_count FROM papers WHERE journal_id = ?',
          [journalToRefresh]
        );

        if (createdJournalId && journalUsage.paper_count === 0) {
          await connection.execute('DELETE FROM journals WHERE journal_id = ?', [createdJournalId]);
        } else {
          await connection.execute(
            'UPDATE journals SET paper_count = ? WHERE journal_id = ?',
            [journalUsage.paper_count, journalToRefresh]
          );
        }
      }

      await connection.commit();
    } catch (error) {
      await connection.rollback();
      throw error;
    } finally {
      connection.release();
    }
  }

  // ── FIND ALL ──────────────────────────────────────────────────────────────
  static async findAll(limit = 100, offset = 0, sortBy = 'recent', options = {}) {
    const connection = await getMySQL();
    const { highlyCollaborative = false } = options;

    const limitInt  = parseInt(limit, 10);
    const offsetInt = parseInt(offset, 10);

    if (!Number.isInteger(limitInt) || !Number.isInteger(offsetInt)) {
      throw new Error('Invalid pagination parameters');
    }

    let orderBy = 'p.publish_year DESC, p.title ASC';
    switch (sortBy) {
      case 'recent':  orderBy = 'p.publish_year DESC, p.title ASC'; break;
      case 'oldest':  orderBy = 'p.publish_year ASC, p.title ASC';  break;
      case 'title':   orderBy = 'p.title ASC';                       break;
      case 'journal': orderBy = 'j.journal_name ASC, p.publish_year DESC'; break;
      default:        orderBy = 'p.publish_year DESC, p.title ASC';
    }

    const whereClause = highlyCollaborative ? 'WHERE p.is_important = TRUE' : '';

    const sql = `
      SELECT p.*, p.publish_year AS year, j.journal_name AS journal, s.source_name AS source,
             p.is_important,
             GROUP_CONCAT(a.author_name ORDER BY pa.author_order SEPARATOR ', ') AS authors
      FROM   papers p
      LEFT JOIN journals      j  ON j.journal_id  = p.journal_id
      LEFT JOIN sources       s  ON s.source_id   = p.source_id
      LEFT JOIN paper_authors pa ON pa.paper_id   = p.paper_id
      LEFT JOIN authors       a  ON a.author_id   = pa.author_id
      ${whereClause}
      GROUP BY p.paper_id
      ORDER BY ${orderBy}
      LIMIT ${limitInt} OFFSET ${offsetInt}
    `;
    const [rows] = await connection.query(sql);
    return rows;
  }

  // ── FIND BY ID ────────────────────────────────────────────────────────────
  static async findById(paper_id) {
    const connection = await getMySQL();
    const query = `
      SELECT p.*, p.publish_year AS year, j.journal_name AS journal, s.source_name AS source,
             p.is_important,
             GROUP_CONCAT(a.author_name ORDER BY pa.author_order SEPARATOR ', ') AS authors
      FROM   papers p
      LEFT JOIN journals      j  ON j.journal_id  = p.journal_id
      LEFT JOIN sources       s  ON s.source_id   = p.source_id
      LEFT JOIN paper_authors pa ON pa.paper_id   = p.paper_id
      LEFT JOIN authors       a  ON a.author_id   = pa.author_id
      WHERE  p.paper_id = ?
      GROUP BY p.paper_id
    `;
    const [rows] = await connection.execute(query, [paper_id]);
    return rows[0];
  }

  // ── ADVANCED SEARCH ───────────────────────────────────────────────────────
  static async advancedSearch(params) {
    const connection = await getMySQL();
    const {
      query, yearFrom, yearTo, journal, author,
      limit = 20, offset = 0, sortBy = 'relevance',
      highlyCollaborative = false,
    } = params;

    let sql = `
      SELECT p.*, p.publish_year AS year, j.journal_name AS journal, s.source_name AS source,
             p.is_important,
             GROUP_CONCAT(a.author_name ORDER BY pa.author_order SEPARATOR ', ') AS authors
      FROM   papers p
      LEFT JOIN journals      j  ON j.journal_id  = p.journal_id
      LEFT JOIN sources       s  ON s.source_id   = p.source_id
      LEFT JOIN paper_authors pa ON pa.paper_id   = p.paper_id
      LEFT JOIN authors       a  ON a.author_id   = pa.author_id
      WHERE 1=1
    `;
    const conditions  = [];
    const queryParams = [];

    if (query)    { conditions.push('p.title LIKE ?');        queryParams.push(`%${query}%`); }
    if (yearFrom) { conditions.push('p.publish_year >= ?');   queryParams.push(parseInt(yearFrom, 10)); }
    if (yearTo)   { conditions.push('p.publish_year <= ?');   queryParams.push(parseInt(yearTo,   10)); }
    if (journal)  { conditions.push('j.journal_name LIKE ?'); queryParams.push(`%${journal}%`); }
    if (author)   { conditions.push('a.author_name LIKE ?');  queryParams.push(`%${author}%`); }
    if (highlyCollaborative) { conditions.push('p.is_important = TRUE'); }

    if (conditions.length > 0) sql += ' AND ' + conditions.join(' AND ');

    const sqlCount = `
      SELECT COUNT(DISTINCT p.paper_id) AS total
      FROM   papers p
      LEFT JOIN journals      j  ON j.journal_id  = p.journal_id
      LEFT JOIN sources       s  ON s.source_id   = p.source_id
      ${author ? 'LEFT JOIN paper_authors pa ON pa.paper_id = p.paper_id LEFT JOIN authors a ON a.author_id = pa.author_id' : ''}
      WHERE 1=1
      ${conditions.length > 0 ? 'AND ' + conditions.join(' AND ') : ''}
    `;
    const [countResult] = await connection.execute(sqlCount, queryParams);
    const total = countResult[0].total;

    let orderBy = 'p.publish_year DESC, p.title ASC';
    switch (sortBy) {
      case 'recent':    orderBy = 'p.publish_year DESC, p.title ASC'; break;
      case 'oldest':    orderBy = 'p.publish_year ASC, p.title ASC';  break;
      case 'title':     orderBy = 'p.title ASC';                       break;
      default:          orderBy = 'p.publish_year DESC, p.title ASC';
    }

    const limitInt  = parseInt(limit,  10);
    const offsetInt = parseInt(offset, 10);
    if (!Number.isInteger(limitInt) || !Number.isInteger(offsetInt)) {
      throw new Error('Invalid pagination parameters');
    }

    sql += ` GROUP BY p.paper_id ORDER BY ${orderBy} LIMIT ${limitInt} OFFSET ${offsetInt}`;
    const [rows] = await connection.query(sql, queryParams);
    return { papers: rows, total };
  }

  // ── SEARCH BY TITLE ───────────────────────────────────────────────────────
  static async searchByTitle(title, limit = 50) {
    const connection = await getMySQL();
    const limitInt   = parseInt(limit, 10);
    if (!Number.isInteger(limitInt)) throw new Error('Invalid limit parameter');

    const sql = `
      SELECT p.*, p.publish_year AS year, j.journal_name AS journal, s.source_name AS source,
             p.is_important,
             GROUP_CONCAT(a.author_name ORDER BY pa.author_order SEPARATOR ', ') AS authors
      FROM   papers p
      LEFT JOIN journals      j  ON j.journal_id  = p.journal_id
      LEFT JOIN sources       s  ON s.source_id   = p.source_id
      LEFT JOIN paper_authors pa ON pa.paper_id   = p.paper_id
      LEFT JOIN authors       a  ON a.author_id   = pa.author_id
      WHERE  p.title LIKE ?
      GROUP BY p.paper_id
      ORDER BY p.publish_year DESC
      LIMIT ${limitInt}
    `;
    const [rows] = await connection.query(sql, [`%${title}%`]);
    return rows;
  }

  // ── GET BY YEAR ───────────────────────────────────────────────────────────
  static async getByYear(year) {
    const connection = await getMySQL();
    const query = `
      SELECT p.*, p.publish_year AS year, j.journal_name AS journal, s.source_name AS source,
             GROUP_CONCAT(a.author_name ORDER BY pa.author_order SEPARATOR ', ') AS authors
      FROM   papers p
      LEFT JOIN journals      j  ON j.journal_id  = p.journal_id
      LEFT JOIN sources       s  ON s.source_id   = p.source_id
      LEFT JOIN paper_authors pa ON pa.paper_id   = p.paper_id
      LEFT JOIN authors       a  ON a.author_id   = pa.author_id
      WHERE  p.publish_year = ?
      GROUP BY p.paper_id
      ORDER BY p.title
    `;
    const [rows] = await connection.execute(query, [year]);
    return rows;
  }

  // ── GET BY YEAR RANGE ─────────────────────────────────────────────────────
  static async getByYearRange(yearFrom, yearTo) {
    const connection = await getMySQL();
    const query = `
      SELECT p.*, j.journal_name AS journal, s.source_name AS source
      FROM   papers p
      LEFT JOIN journals j ON j.journal_id = p.journal_id
      LEFT JOIN sources  s ON s.source_id  = p.source_id
      WHERE  p.publish_year BETWEEN ? AND ?
      ORDER BY p.publish_year DESC, p.title ASC
    `;
    const [rows] = await connection.execute(query, [parseInt(yearFrom, 10), parseInt(yearTo, 10)]);
    return rows;
  }

  // ── GET BY JOURNAL ────────────────────────────────────────────────────────
  static async getByJournal(journal) {
    const connection = await getMySQL();
    const query = `
      SELECT p.*, p.publish_year AS year, j.journal_name AS journal, s.source_name AS source,
             GROUP_CONCAT(a.author_name ORDER BY pa.author_order SEPARATOR ', ') AS authors
      FROM   papers p
      LEFT JOIN journals      j  ON j.journal_id  = p.journal_id
      LEFT JOIN sources       s  ON s.source_id   = p.source_id
      LEFT JOIN paper_authors pa ON pa.paper_id   = p.paper_id
      LEFT JOIN authors       a  ON a.author_id   = pa.author_id
      WHERE  j.journal_name LIKE ?
      GROUP BY p.paper_id
      ORDER BY p.publish_year DESC
    `;
    const [rows] = await connection.execute(query, [`%${journal}%`]);
    return rows;
  }

  // ── FILTER OPTIONS ────────────────────────────────────────────────────────
  static async getFilterOptions() {
    const connection = await getMySQL();
    const [years, journals, yearRange] = await Promise.all([
      connection.execute(
        'SELECT DISTINCT publish_year FROM papers WHERE publish_year IS NOT NULL ORDER BY publish_year DESC'
      ).then(([rows]) => rows),
      connection.execute(
        'SELECT DISTINCT j.journal_name FROM journals j WHERE j.journal_name IS NOT NULL ORDER BY j.journal_name ASC LIMIT 100'
      ).then(([rows]) => rows),
      connection.execute(
        'SELECT MIN(publish_year) AS min_year, MAX(publish_year) AS max_year FROM papers'
      ).then(([rows]) => rows),
    ]);
    return {
      years:     years.map(y => y.publish_year),
      journals:  journals.map(j => j.journal_name),
      yearRange: yearRange[0],
    };
  }

  // ── SUGGESTIONS ───────────────────────────────────────────────────────────
  static async getSuggestions(query, type = 'all') {
    const connection  = await getMySQL();
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

  // Returns trigger-backed paper flags for a batch of paper ids so Mongo paper
  // documents can be enriched without switching the whole listing flow to SQL.
  static async getFlagsByPaperIds(paperIds = []) {
    const connection = await getMySQL();
    const normalizedIds = [...new Set(
      paperIds
        .map((paperId) => String(paperId || '').trim())
        .filter(Boolean)
    )];

    if (normalizedIds.length === 0) {
      return new Map();
    }

    const placeholders = normalizedIds.map(() => '?').join(', ');
    const [rows] = await connection.execute(
      `
        SELECT p.paper_id, p.is_important, pm.author_count
        FROM papers p
        LEFT JOIN paper_metrics pm ON pm.paper_id = p.paper_id
        WHERE p.paper_id IN (${placeholders})
      `,
      normalizedIds
    );

    return new Map(
      rows.map((row) => [
        String(row.paper_id),
        {
          is_important: row.is_important,
          author_count: row.author_count,
        },
      ])
    );
  }

  // ── COUNT ─────────────────────────────────────────────────────────────────
  static async count(options = {}) {
    const connection = await getMySQL();
    const { highlyCollaborative = false } = options;
    const sql = highlyCollaborative
      ? 'SELECT COUNT(*) AS count FROM papers WHERE is_important = TRUE'
      : 'SELECT COUNT(*) AS count FROM papers';
    const [rows] = await connection.execute(sql);
    return rows[0].count;
  }

  // ── YEAR STATS ────────────────────────────────────────────────────────────
  static async getYearStats() {
    const connection = await getMySQL();
    const [rows] = await connection.execute(`
      SELECT publish_year, COUNT(*) AS count
      FROM   papers
      WHERE  publish_year IS NOT NULL
      GROUP BY publish_year
      ORDER BY publish_year DESC
    `);
    return rows;
  }

  // ── TOP JOURNALS (MySQL — uses denormalised paper_count maintained by trigger) ──
  // Reads directly from journals.paper_count instead of GROUP BY — fast O(1) per row.
  static async getTopJournals(limit = 10) {
    const connection = await getMySQL();
    const limitInt   = parseInt(limit, 10);
    const sql = `
      SELECT journal_name AS journal, paper_count AS count
      FROM   journals
      WHERE  paper_count > 0
      ORDER BY paper_count DESC
      LIMIT ${limitInt}
    `;
    // paper_count is kept current by trigger trg_update_journal_count
    const [rows] = await connection.query(sql);
    return rows;
  }

  // ── RECENT ────────────────────────────────────────────────────────────────
  static async getRecent(limit = 20) {
    const connection = await getMySQL();
    const limitInt   = parseInt(limit, 10);
    if (!Number.isInteger(limitInt)) throw new Error('Invalid limit parameter');

    const sql = `
      SELECT p.*, p.publish_year AS year, j.journal_name AS journal, s.source_name AS source,
             p.is_important,
             GROUP_CONCAT(a.author_name ORDER BY pa.author_order SEPARATOR ', ') AS authors
      FROM   papers p
      LEFT JOIN journals      j  ON j.journal_id  = p.journal_id
      LEFT JOIN sources       s  ON s.source_id   = p.source_id
      LEFT JOIN paper_authors pa ON pa.paper_id   = p.paper_id
      LEFT JOIN authors       a  ON a.author_id   = pa.author_id
      GROUP BY p.paper_id
      ORDER BY p.publish_year DESC, p.title ASC
      LIMIT ${limitInt}
    `;
    const [rows] = await connection.query(sql);
    return rows;
  }

  // ── GET BY MULTIPLE CRITERIA ──────────────────────────────────────────────
  static async getByMultipleCriteria(criteria) {
    const connection = await getMySQL();
    const { years, journals, authors, limit = 100, offset = 0 } = criteria;

    let sql = `
      SELECT p.*, p.publish_year AS year, j.journal_name AS journal, s.source_name AS source,
             GROUP_CONCAT(a.author_name ORDER BY pa.author_order SEPARATOR ', ') AS authors
      FROM   papers p
      LEFT JOIN journals      j  ON j.journal_id  = p.journal_id
      LEFT JOIN sources       s  ON s.source_id   = p.source_id
      LEFT JOIN paper_authors pa ON pa.paper_id   = p.paper_id
      LEFT JOIN authors       a  ON a.author_id   = pa.author_id
    `;
    const conditions = [];
    const params     = [];

    if (authors?.length > 0) {
      conditions.push(`(${authors.map(() => 'a.author_name = ?').join(' OR ')})`);
      params.push(...authors);
    }
    if (years?.length > 0) {
      conditions.push(`(${years.map(() => 'p.publish_year = ?').join(' OR ')})`);
      params.push(...years.map(y => parseInt(y, 10)));
    }
    if (journals?.length > 0) {
      conditions.push(`(${journals.map(() => 'j.journal_name = ?').join(' OR ')})`);
      params.push(...journals);
    }

    if (conditions.length > 0) sql += ' WHERE ' + conditions.join(' AND ');

    const limitInt  = parseInt(limit,  10);
    const offsetInt = parseInt(offset, 10);
    if (!Number.isInteger(limitInt) || !Number.isInteger(offsetInt)) {
      throw new Error('Invalid pagination parameters');
    }

    sql += ` GROUP BY p.paper_id ORDER BY p.publish_year DESC, p.title ASC LIMIT ${limitInt} OFFSET ${offsetInt}`;
    const [rows] = await connection.query(sql, params);
    return rows;
  }

  // ── UPDATE ────────────────────────────────────────────────────────────────
  static async update(paper_id, updates) {
    const connection = await getMySQL();
    const fields = [];
    const values = [];

    if (updates.title       !== undefined) { fields.push('title = ?');        values.push(updates.title); }
    if (updates.year        !== undefined) { fields.push('publish_year = ?'); values.push(parseInt(updates.year, 10)); }
    if (updates.is_covid19  !== undefined) { fields.push('is_covid19 = ?');   values.push(updates.is_covid19); }
    if (updates.doi         !== undefined) { fields.push('doi = ?');          values.push(updates.doi); }
    if (updates.abstract    !== undefined) { fields.push('abstract = ?');     values.push(updates.abstract); }

    if (fields.length === 0) return { affectedRows: 0 };

    values.push(paper_id);
    const query  = `UPDATE papers SET ${fields.join(', ')} WHERE paper_id = ?`;
    const [result] = await connection.execute(query, values);
    return result;
  }

  // ── DELETE ────────────────────────────────────────────────────────────────
  // trg_after_paper_delete fires after this DELETE (audit log extension point).
  static async delete(paper_id) {
    const connection = await getMySQL();
    const [result]   = await connection.execute('DELETE FROM papers WHERE paper_id = ?', [paper_id]);
    return result;
  }

  // ── MISC ──────────────────────────────────────────────────────────────────
  static async getAvgAbstractWordCount() {
    const connection = await getMySQL();
    const [rows]     = await connection.execute('SELECT AVG(abstract_word_count) AS avg_count FROM paper_metrics');
    return rows[0].avg_count;
  }
}

module.exports = PaperModel;
