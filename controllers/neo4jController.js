/**
 * controllers/neo4jController.js
 *
 * All Neo4j graph queries for the Research Knowledge Graph.
 * Schema:
 *   (Author)-[:WROTE]->(Paper)-[:PUBLISHED_IN]->(Journal)
 *   (Paper)-[:FROM_SOURCE]->(Source)
 *   (Paper)-[:PUBLISHED_YEAR]->(Year)
 */
const { runQuery, isNeo4jConnected } = require('../config/neo4jDatabase');
const { asyncHandler, AppError }     = require('../utils/errorHandler');

/* ─────────────────────────────────────────
   Helper — convert Neo4j Integer to JS number
───────────────────────────────────────── */
const toNum = (val) => {
  if (val === null || val === undefined) return 0;
  if (typeof val === 'number') return val;
  // neo4j.Integer
  if (typeof val.toNumber === 'function') return val.toNumber();
  return Number(val);
};

const guardNeo4j = () => {
  if (!isNeo4jConnected()) {
    throw new AppError('Neo4j is not connected.', 503, 'NEO4J_UNAVAILABLE');
  }
};

/* ─────────────────────────────────────────
   GET /api/graph/stats
   Overview counts for the graph dashboard
───────────────────────────────────────── */
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
      MATCH (j:Journal) RETURN count(j)   AS journals
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
    RETURN papers, authors, journals, sources, years, relationships
  `);

  const r = records[0];
  res.json({
    stats: {
      papers:        toNum(r.get('papers')),
      authors:       toNum(r.get('authors')),
      journals:      toNum(r.get('journals')),
      sources:       toNum(r.get('sources')),
      years:         toNum(r.get('years')),
      relationships: toNum(r.get('relationships')),
    },
    source: 'neo4j',
    description: 'Research Knowledge Graph — node and relationship counts',
  });
});

/* ─────────────────────────────────────────
   GET /api/graph/author-network/:name
   Co-author network for a given author
───────────────────────────────────────── */
const getAuthorNetwork = asyncHandler(async (req, res) => {
  guardNeo4j();
  const { name } = req.params;
  const limit = Math.min(parseInt(req.query.limit) || 50, 200);

  // Find all authors who share at least one paper with the target author
  const records = await runQuery(
    `
    MATCH (a:Author {name: $name})-[:WROTE]->(p:Paper)<-[:WROTE]-(coAuthor:Author)
    WHERE coAuthor.name <> $name
    WITH coAuthor.name AS coAuthorName, count(p) AS sharedPapers,
         collect(p.title)[0..3] AS samplePapers
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

  // Also get the author's own papers
  const paperRecords = await runQuery(
    `
    MATCH (a:Author {name: $name})-[:WROTE]->(p:Paper)
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

/* ─────────────────────────────────────────
   GET /api/graph/top-authors?limit=10
   Authors ranked by paper count
───────────────────────────────────────── */
const getTopAuthors = asyncHandler(async (req, res) => {
  guardNeo4j();
  const limit = Math.min(parseInt(req.query.limit) || 10, 100);

  const records = await runQuery(
    `
    MATCH (a:Author)-[:WROTE]->(p:Paper)
    RETURN a.name AS author, count(p) AS paperCount
    ORDER BY paperCount DESC
    LIMIT $limit
    `,
    { limit: neo4jInt(limit) }
  );

  const authors = records.map(r => ({
    name:       r.get('author'),
    paperCount: toNum(r.get('paperCount')),
  }));

  res.json({ authors, source: 'neo4j' });
});

/* ─────────────────────────────────────────
   GET /api/graph/top-journals?limit=10
   Journals ranked by paper count
───────────────────────────────────────── */
const getTopJournals = asyncHandler(async (req, res) => {
  guardNeo4j();
  const limit = Math.min(parseInt(req.query.limit) || 10, 100);

  const records = await runQuery(
    `
    MATCH (p:Paper)-[:PUBLISHED_IN]->(j:Journal)
    RETURN j.name AS journal, count(p) AS paperCount
    ORDER BY paperCount DESC
    LIMIT $limit
    `,
    { limit: neo4jInt(limit) }
  );

  const journals = records.map(r => ({
    name:       r.get('journal'),
    paperCount: toNum(r.get('paperCount')),
  }));

  res.json({ journals, source: 'neo4j' });
});

/* ─────────────────────────────────────────
   GET /api/graph/papers-by-year
   Papers grouped by publication year
───────────────────────────────────────── */
const getPapersByYear = asyncHandler(async (req, res) => {
  guardNeo4j();

  const records = await runQuery(`
    MATCH (p:Paper)-[:PUBLISHED_YEAR]->(y:Year)
    RETURN y.value AS year, count(p) AS paperCount
    ORDER BY year ASC
  `);

  const data = records.map(r => ({
    year:       r.get('year'),
    paperCount: toNum(r.get('paperCount')),
  }));

  res.json({ papersPerYear: data, source: 'neo4j' });
});

/* ─────────────────────────────────────────
   GET /api/graph/papers-by-source
   Papers grouped by source dataset
───────────────────────────────────────── */
const getPapersBySource = asyncHandler(async (req, res) => {
  guardNeo4j();

  const records = await runQuery(`
    MATCH (p:Paper)-[:FROM_SOURCE]->(s:Source)
    RETURN s.name AS source, count(p) AS paperCount
    ORDER BY paperCount DESC
  `);

  const data = records.map(r => ({
    source:     r.get('source'),
    paperCount: toNum(r.get('paperCount')),
  }));

  res.json({ sources: data, source: 'neo4j' });
});

/* ─────────────────────────────────────────
   GET /api/graph/author-papers/:name
   All papers written by an author
───────────────────────────────────────── */
const getAuthorPapers = asyncHandler(async (req, res) => {
  guardNeo4j();
  const { name } = req.params;
  const limit = Math.min(parseInt(req.query.limit) || 20, 100);

  const records = await runQuery(
    `
    MATCH (a:Author {name: $name})-[:WROTE]->(p:Paper)
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

/* ─────────────────────────────────────────
   GET /api/graph/journal-authors/:journal
   Top authors in a specific journal
───────────────────────────────────────── */
const getJournalAuthors = asyncHandler(async (req, res) => {
  guardNeo4j();
  const { journal } = req.params;
  const limit = Math.min(parseInt(req.query.limit) || 15, 50);

  const records = await runQuery(
    `
    MATCH (a:Author)-[:WROTE]->(p:Paper)-[:PUBLISHED_IN]->(j:Journal)
    WHERE j.name CONTAINS $journal
    RETURN a.name AS author, count(p) AS paperCount, j.name AS journalName
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

/* ─────────────────────────────────────────
   GET /api/graph/search-authors?q=
   Author name search
───────────────────────────────────────── */
const searchAuthors = asyncHandler(async (req, res) => {
  guardNeo4j();
  const { q } = req.query;
  if (!q || q.trim().length < 2) {
    throw new AppError('Query must be at least 2 characters.', 400, 'MISSING_PARAM');
  }

  const records = await runQuery(
    `
    MATCH (a:Author)-[:WROTE]->(p:Paper)
    WHERE toLower(a.name) CONTAINS toLower($q)
    RETURN a.name AS author, count(p) AS paperCount
    ORDER BY paperCount DESC
    LIMIT 20
    `,
    { q: q.trim() }
  );

  const authors = records.map(r => ({
    name:       r.get('author'),
    paperCount: toNum(r.get('paperCount')),
  }));

  res.json({ query: q, authors, source: 'neo4j' });
});

/* ─────────────────────────────────────────
   GET /api/graph/health
   Neo4j connectivity check
───────────────────────────────────────── */
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

// neo4j-driver requires integers as neo4j.Integer for LIMIT/SKIP params
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
  getHealth,
};