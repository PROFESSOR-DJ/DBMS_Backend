// paperModel manages backend MySQL paper records.
// Active triggers on this model's tables:
//   trg_validate_paper        — BEFORE INSERT on papers: rejects title < 5 chars (raises SQLSTATE 45000)
//   trg_update_journal_count  — AFTER INSERT on papers: increments journals.paper_count automatically
//   trg_after_paper_insert    — AFTER INSERT on papers: inserts paper_metrics row
//   trg_mark_important_paper  — AFTER INSERT on paper_metrics: sets is_important=TRUE when author_count >= 5
//   trg_after_paper_authors_insert/delete — keep paper_metrics.author_count in sync
//
// NOTE: trg_update_journal_count only fires on INSERT.  On DELETE we must
//       manually decrement journals.paper_count so the count stays accurate
//       and the Journals page / journal-popularity endpoint are consistent.

const { getMySQL } = require('../../config/database');

class PaperModel {

  // ── CREATE ────────────────────────────────────────────────────────────────
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

      // Insert authors
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

  // Returns trigger-backed paper flags for a batch of paper ids
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

  // ── TOP JOURNALS ──────────────────────────────────────────────────────────
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
  // Full cascade cleanup without any schema changes.  The logic mirrors what
  // a set of AFTER DELETE triggers would do, implemented here in the model so
  // the existing trigger definitions are untouched.
  //
  // Steps (all inside one transaction):
  //   1. Snapshot the paper's journal_id and the ids of every linked author
  //      BEFORE the paper row is deleted (the CASCADE will remove paper_authors
  //      and paper_metrics automatically when the paper is deleted).
  //   2. Delete the paper.
  //   3. For the journal: recompute its real paper count.
  //        - If count is now 0  → delete the journal row entirely so it no
  //          longer shows up in the Journals page with a "0 papers" entry.
  //        - If count  > 0 → write the accurate count back to paper_count.
  //   4. For each author that was linked to this paper: count how many OTHER
  //      papers they still have.
  //        - If 0 remaining papers → delete the author row so they no longer
  //          appear in the Authors page with "0 papers".
  //        - If > 0 remaining → leave them untouched.
  //
  // Using a recompute (COUNT(*)) rather than a simple -1 decrement makes the
  // columns self-healing: any stale count from a previous bulk-import or
  // accidental duplicate create is also corrected at delete time.
  static async delete(paper_id) {
    const pool       = getMySQL();
    const connection = await pool.getConnection();
    try {
      await connection.beginTransaction();

      // ── Step 1: snapshot journal + authors before CASCADE removes them ──
      const [[paperRow]] = await connection.execute(
        'SELECT journal_id FROM papers WHERE paper_id = ?',
        [paper_id]
      );
      const journal_id = paperRow ? paperRow.journal_id : null;

      // Collect every author_id linked to this paper so we can check them
      // after the delete (paper_authors CASCADE will be gone by then).
      const [linkedAuthors] = await connection.execute(
        'SELECT author_id FROM paper_authors WHERE paper_id = ?',
        [paper_id]
      );
      const authorIds = linkedAuthors.map(r => r.author_id);

      // ── Step 2: delete the paper (CASCADE cleans paper_authors + paper_metrics) ──
      const [result] = await connection.execute(
        'DELETE FROM papers WHERE paper_id = ?',
        [paper_id]
      );

      // ── Step 3: journal cleanup ──────────────────────────────────────────
      if (journal_id) {
        const [[countRow]] = await connection.execute(
          'SELECT COUNT(*) AS cnt FROM papers WHERE journal_id = ?',
          [journal_id]
        );
        if (countRow.cnt === 0) {
          // No papers left in this journal — remove the journal row entirely
          // so it doesn't linger at "0 papers" in the Journals page.
          await connection.execute(
            'DELETE FROM journals WHERE journal_id = ?',
            [journal_id]
          );
        } else {
          // Write back the accurate count (self-heals any previous stale value).
          await connection.execute(
            'UPDATE journals SET paper_count = ? WHERE journal_id = ?',
            [countRow.cnt, journal_id]
          );
        }
      }

      // ── Step 4: author cleanup ───────────────────────────────────────────
      // For each author that was linked to the deleted paper, check whether
      // they have any remaining papers.  If not, delete them.
      // trg_before_author_delete guards against deleting authors that still
      // have linked papers, so we only attempt deletion when the count is 0.
      if (authorIds.length > 0) {
        for (const author_id of authorIds) {
          const [[remainingRow]] = await connection.execute(
            'SELECT COUNT(*) AS cnt FROM paper_authors WHERE author_id = ?',
            [author_id]
          );
          if (remainingRow.cnt === 0) {
            // Author has no papers left — safe to delete.
            // The trigger trg_before_author_delete will not block this because
            // the count is already 0 (paper_authors CASCADE deleted the link).
            await connection.execute(
              'DELETE FROM authors WHERE author_id = ?',
              [author_id]
            );
          }
          // If cnt > 0 the author still has other papers — leave them alone.
        }
      }

      await connection.commit();
      return result;
    } catch (error) {
      await connection.rollback();
      throw error;
    } finally {
      connection.release();
    }
  }

  // ── MISC ──────────────────────────────────────────────────────────────────
  static async getAvgAbstractWordCount() {
    const connection = await getMySQL();
    const [rows]     = await connection.execute('SELECT AVG(abstract_word_count) AS avg_count FROM paper_metrics');
    return rows[0].avg_count;
  }
}

module.exports = PaperModel;