// neo4jController handles backend graph analytics and network requests.
// All paper-count Cypher queries use COUNT(DISTINCT p) so that stale duplicate
// WROTE/PUBLISHED_IN relationships left behind by previous bulk imports do not
// inflate author or journal paper counts shown in the Network page.
const { runQuery, isNeo4jConnected } = require('../config/neo4jDatabase');
const { asyncHandler, AppError }     = require('../utils/errorHandler');

const toNum = (val) => {
  if (val === null || val === undefined) return 0;
  if (typeof val === 'number') return val;
  if (typeof val.toNumber === 'function') return val.toNumber();
  return Number(val);
};

const guardNeo4j = () => {
  if (!isNeo4jConnected()) {
    throw new AppError('Neo4j is not connected.', 503, 'NEO4J_UNAVAILABLE');
  }
};

const cleanAuthorDisplayName = (name) => String(name || '')
  .trim()
  .replace(/^[\s'"[\]]+|[\s'"[\]]+$/g, '')
  .replace(/\s+/g, ' ');

// ── GRAPH STATS ───────────────────────────────────────────────────────────────
const getGraphStats = asyncHandler(async (req, res) => {
  guardNeo4j();

  const records = await runQuery(`
    CALL {
      MATCH (p:Paper)   RETURN count(p)   AS papers
    }
    CALL {
      MATCH (a:Author)  RETURN count(a)   AS authors
    }
    CALL {
      MATCH (j:Journal)
      RETURN count(j) AS journals,
             sum(CASE WHEN j.sjr_rank IS NOT NULL THEN 1 ELSE 0 END) AS ranked_journals,
             sum(CASE WHEN j.best_quartile = 'Q1' THEN 1 ELSE 0 END) AS q1_journals,
             sum(CASE WHEN coalesce(j.oa, false) = true THEN 1 ELSE 0 END) AS open_access_journals
    }
    CALL {
      MATCH (s:Source)  RETURN count(s)   AS sources
    }
    CALL {
      MATCH (y:Year)    RETURN count(y)   AS years
    }
    CALL {
      MATCH ()-[r]->()  RETURN count(r)   AS relationships
    }
    RETURN papers, authors, journals, ranked_journals, q1_journals, open_access_journals, sources, years, relationships
  `);

  const r = records[0];
  res.json({
    stats: {
      papers:        toNum(r.get('papers')),
      authors:       toNum(r.get('authors')),
      journals:      toNum(r.get('journals')),
      ranked_journals: toNum(r.get('ranked_journals')),
      q1_journals:   toNum(r.get('q1_journals')),
      open_access_journals: toNum(r.get('open_access_journals')),
      sources:       toNum(r.get('sources')),
      years:         toNum(r.get('years')),
      relationships: toNum(r.get('relationships')),
    },
    source: 'neo4j',
    description: 'Research Knowledge Graph — node and relationship counts',
  });
});

// ── AUTHOR NETWORK ────────────────────────────────────────────────────────────
// Uses DISTINCT on Paper nodes so duplicate WROTE edges (from past bulk imports)
// do not count the same paper multiple times for co-author or paper tallies.
const getAuthorNetwork = asyncHandler(async (req, res) => {
  guardNeo4j();
  const { name } = req.params;
  const limit = Math.min(parseInt(req.query.limit) || 50, 200);

  // COUNT(DISTINCT p) ensures a paper shared via duplicate relationships is
  // counted only once per co-author pair.
  const records = await runQuery(
    `
    MATCH (a:Author {name: $name})-[:WROTE]->(p:Paper)<-[:WROTE]-(coAuthor:Author)
    WHERE coAuthor.name <> $name
    WITH coAuthor.name AS coAuthorName,
         COUNT(DISTINCT p) AS sharedPapers,
         collect(DISTINCT p.title)[0..3] AS samplePapers
    ORDER BY sharedPapers DESC
    LIMIT $limit
    RETURN coAuthorName, sharedPapers, samplePapers
    `,
    { name, limit: neo4jInt(limit) }
  );

  const coAuthors = records.map(r => ({
    name:         r.get('coAuthorName'),
    sharedPapers: toNum(r.get('sharedPapers')),
    samplePapers: r.get('samplePapers'),
  }));

  // COUNT(DISTINCT p) for the author's own paper list
  const paperRecords = await runQuery(
    `
    MATCH (a:Author {name: $name})-[:WROTE]->(p:Paper)
    WITH DISTINCT p
    OPTIONAL MATCH (p)-[:PUBLISHED_IN]->(j:Journal)
    OPTIONAL MATCH (p)-[:PUBLISHED_YEAR]->(y:Year)
    RETURN p.paper_id AS id, p.title AS title, j.name AS journal,
           y.value AS year
    ORDER BY year DESC
    LIMIT 20
    `,
    { name }
  );

  const papers = paperRecords.map(r => ({
    id:      r.get('id'),
    title:   r.get('title'),
    journal: r.get('journal'),
    year:    r.get('year'),
  }));

  res.json({
    author:     name,
    paperCount: papers.length,
    papers,
    coAuthors,
    totalCoAuthors: coAuthors.length,
    source: 'neo4j',
    advantage: 'Graph traversal finds co-authors in O(1) hops vs SQL multi-join',
  });
});

// ── TOP AUTHORS ───────────────────────────────────────────────────────────────
// COUNT(DISTINCT p) prevents duplicate WROTE edges from inflating author paper counts.
const getTopAuthors = asyncHandler(async (req, res) => {
  guardNeo4j();
  const limit = Math.min(parseInt(req.query.limit) || 10, 100);

  const records = await runQuery(
    `
    MATCH (a:Author)-[:WROTE]->(p:Paper)
    RETURN a.name AS author, COUNT(DISTINCT p) AS paperCount
    ORDER BY paperCount DESC
    LIMIT $limit
    `,
    { limit: neo4jInt(limit) }
  );

  const authors = records.map(r => ({
    name:       r.get('author'),
    displayName: cleanAuthorDisplayName(r.get('author')),
    paperCount: toNum(r.get('paperCount')),
  }));

  res.json({ authors, source: 'neo4j' });
});

// ── TOP JOURNALS ──────────────────────────────────────────────────────────────
// COUNT(DISTINCT p) prevents duplicate PUBLISHED_IN edges from inflating journal counts.
const getTopJournals = asyncHandler(async (req, res) => {
  guardNeo4j();
  const limit = Math.min(parseInt(req.query.limit) || 10, 100);

  const records = await runQuery(
    `
    MATCH (p:Paper)-[:PUBLISHED_IN]->(j:Journal)
    RETURN j.name AS journal,
           COUNT(DISTINCT p) AS paperCount,
           j.sjr_rank AS sjrRank,
           j.sjr_index AS sjrIndex,
           j.best_quartile AS quartile,
           j.h_index AS hIndex,
           j.citescore AS citeScore,
           j.country AS country,
           j.oa AS openAccess
    ORDER BY paperCount DESC, coalesce(j.sjr_index, 0) DESC, journal ASC
    LIMIT $limit
    `,
    { limit: neo4jInt(limit) }
  );

  const journals = records.map(r => ({
    name:       r.get('journal'),
    paperCount: toNum(r.get('paperCount')),
    sjrRank:    r.get('sjrRank'),
    sjrIndex:   r.get('sjrIndex') === null ? null : Number(r.get('sjrIndex')),
    quartile:   r.get('quartile'),
    hIndex:     r.get('hIndex') === null ? null : toNum(r.get('hIndex')),
    citeScore:  r.get('citeScore') === null ? null : Number(r.get('citeScore')),
    country:    r.get('country'),
    openAccess: r.get('openAccess'),
  }));

  res.json({ journals, source: 'neo4j' });
});

// ── PAPERS BY YEAR ────────────────────────────────────────────────────────────
// COUNT(DISTINCT p) prevents duplicate PUBLISHED_YEAR edges from double-counting.
const getPapersByYear = asyncHandler(async (req, res) => {
  guardNeo4j();

  const records = await runQuery(`
    MATCH (p:Paper)-[:PUBLISHED_YEAR]->(y:Year)
    RETURN y.value AS year, COUNT(DISTINCT p) AS paperCount
    ORDER BY year ASC
  `);

  const data = records.map(r => ({
    year:       r.get('year'),
    paperCount: toNum(r.get('paperCount')),
  }));

  res.json({ papersPerYear: data, source: 'neo4j' });
});

// ── PAPERS BY SOURCE ──────────────────────────────────────────────────────────
// COUNT(DISTINCT p) for the same reason.
const getPapersBySource = asyncHandler(async (req, res) => {
  guardNeo4j();

  const records = await runQuery(`
    MATCH (p:Paper)-[:FROM_SOURCE]->(s:Source)
    RETURN s.name AS source, COUNT(DISTINCT p) AS paperCount
    ORDER BY paperCount DESC
  `);

  const data = records.map(r => ({
    source:     r.get('source'),
    paperCount: toNum(r.get('paperCount')),
  }));

  res.json({ sources: data, source: 'neo4j' });
});

// ── AUTHOR PAPERS ─────────────────────────────────────────────────────────────
// Use DISTINCT p so duplicate WROTE edges do not return the same paper twice.
const getAuthorPapers = asyncHandler(async (req, res) => {
  guardNeo4j();
  const { name } = req.params;
  const limit = Math.min(parseInt(req.query.limit) || 20, 100);

  const records = await runQuery(
    `
    MATCH (a:Author {name: $name})-[:WROTE]->(p:Paper)
    WITH DISTINCT p
    OPTIONAL MATCH (p)-[:PUBLISHED_IN]->(j:Journal)
    OPTIONAL MATCH (p)-[:PUBLISHED_YEAR]->(y:Year)
    RETURN p.paper_id AS id, p.title AS title, p.abstract AS abstract,
           j.name AS journal, y.value AS year, p.doi AS doi
    ORDER BY year DESC
    LIMIT $limit
    `,
    { name, limit: neo4jInt(limit) }
  );

  const papers = records.map(r => ({
    id:       r.get('id'),
    title:    r.get('title'),
    abstract: r.get('abstract'),
    journal:  r.get('journal'),
    year:     r.get('year'),
    doi:      r.get('doi'),
  }));

  res.json({ author: name, papers, count: papers.length, source: 'neo4j' });
});

// ── JOURNAL AUTHORS ───────────────────────────────────────────────────────────
// COUNT(DISTINCT p) so duplicate edges don't inflate per-author paper counts.
const getJournalAuthors = asyncHandler(async (req, res) => {
  guardNeo4j();
  const { journal } = req.params;
  const limit = Math.min(parseInt(req.query.limit) || 15, 50);

  const records = await runQuery(
    `
    MATCH (a:Author)-[:WROTE]->(p:Paper)-[:PUBLISHED_IN]->(j:Journal)
    WHERE j.name CONTAINS $journal
    RETURN a.name AS author, COUNT(DISTINCT p) AS paperCount, j.name AS journalName
    ORDER BY paperCount DESC
    LIMIT $limit
    `,
    { journal, limit: neo4jInt(limit) }
  );

  const authors = records.map(r => ({
    name:        r.get('author'),
    paperCount:  toNum(r.get('paperCount')),
    journalName: r.get('journalName'),
  }));

  res.json({ journal, authors, source: 'neo4j' });
});

// ── SEARCH AUTHORS ────────────────────────────────────────────────────────────
// COUNT(DISTINCT p) for consistent paper counts in search results.
const searchAuthors = asyncHandler(async (req, res) => {
  guardNeo4j();
  const { q } = req.query;
  const query = String(q || '').trim().replace(/^['"]+|['"]+$/g, '');

  if (!query || query.length < 2) {
    throw new AppError('Query must be at least 2 characters.', 400, 'MISSING_PARAM');
  }

  const records = await runQuery(
    `
    MATCH (a:Author)
    WHERE toLower(a.name) CONTAINS toLower($q)
    WITH a
    ORDER BY a.name ASC
    LIMIT 50
    OPTIONAL MATCH (a)-[:WROTE]->(p:Paper)
    RETURN a.name AS author, COUNT(DISTINCT p) AS paperCount
    ORDER BY paperCount DESC
    LIMIT 20
    `,
    { q: query }
  );

  const authors = records.map(r => ({
    name:       r.get('author'),
    paperCount: toNum(r.get('paperCount')),
  }));

  res.json({ query, authors, source: 'neo4j' });
});

// CONFLICT OF INTEREST CHECK
const checkConflictOfInterest = asyncHandler(async (req, res) => {
  guardNeo4j();

  const reviewer = String(req.body.reviewer || '').trim();
  const authors = Array.isArray(req.body.authors)
    ? req.body.authors.map(author => String(author || '').trim()).filter(Boolean)
    : [];

  if (!reviewer || reviewer.length < 2) {
    throw new AppError('Reviewer name must be at least 2 characters.', 400, 'MISSING_PARAM');
  }
  if (!authors.length) {
    throw new AppError('Provide at least one paper author.', 400, 'MISSING_PARAM');
  }

  const uniqueAuthors = [...new Set(authors)].filter(author => author !== reviewer);

  if (!uniqueAuthors.length) {
    return res.json({
      reviewer,
      conflicts: authors.map(author => ({
        author,
        direct: author === reviewer ? 1 : 0,
        indirect: 0,
        conflict_level: author === reviewer ? 'HIGH' : 'NONE',
      })),
      overall_conflict_level: authors.includes(reviewer) ? 'HIGH' : 'NONE',
      source: 'neo4j',
    });
  }

  const records = await runQuery(
    `
    UNWIND $authors AS paperAuthor
    MATCH (r:Author {name: $reviewer})
    OPTIONAL MATCH directPath = (r)-[:WROTE]->(:Paper)<-[:WROTE]-(directAuthor:Author {name: paperAuthor})
    OPTIONAL MATCH indirectPath = (r)-[:WROTE]->(:Paper)<-[:WROTE]-(sharedAuthor:Author)
      -[:WROTE]->(:Paper)<-[:WROTE]-(indirectAuthor:Author {name: paperAuthor})
    WHERE sharedAuthor.name <> r.name
      AND sharedAuthor.name <> paperAuthor
    RETURN paperAuthor,
           count(DISTINCT directPath) AS direct_coauthorships,
           count(DISTINCT indirectPath) AS indirect_connections
    `,
    { reviewer, authors: uniqueAuthors }
  );

  const found = new Map(records.map((record) => {
    const direct = toNum(record.get('direct_coauthorships'));
    const indirect = toNum(record.get('indirect_connections'));
    return [record.get('paperAuthor'), {
      author: record.get('paperAuthor'),
      direct,
      indirect,
      conflict_level: direct > 0 ? 'HIGH' : indirect > 0 ? 'MEDIUM' : 'NONE',
    }];
  }));

  const conflicts = authors.map((author) => {
    if (author === reviewer) {
      return { author, direct: 1, indirect: 0, conflict_level: 'HIGH' };
    }
    return found.get(author) || { author, direct: 0, indirect: 0, conflict_level: 'NONE' };
  });

  const overallConflictLevel =
    conflicts.some(c => c.conflict_level === 'HIGH') ? 'HIGH' :
    conflicts.some(c => c.conflict_level === 'MEDIUM') ? 'MEDIUM' : 'NONE';

  res.json({
    reviewer,
    conflicts,
    overall_conflict_level: overallConflictLevel,
    source: 'neo4j',
    rule: 'HIGH = direct co-author; MEDIUM = shared co-author within 2 hops',
  });
});

// ── HEALTH ────────────────────────────────────────────────────────────────────
const getHealth = asyncHandler(async (req, res) => {
  if (!isNeo4jConnected()) {
    return res.status(503).json({ status: 'disconnected', message: 'Neo4j is not connected.' });
  }
  try {
    await runQuery('RETURN 1 AS ping');
    res.json({ status: 'connected', message: 'Neo4j is healthy.' });
  } catch (err) {
    res.status(503).json({ status: 'error', message: err.message });
  }
});

const neo4jInt = (n) => require('neo4j-driver').int(n);

module.exports = {
  getGraphStats,
  getAuthorNetwork,
  getTopAuthors,
  getTopJournals,
  getPapersByYear,
  getPapersBySource,
  getAuthorPapers,
  getJournalAuthors,
  searchAuthors,
  checkConflictOfInterest,
  getHealth,
};
