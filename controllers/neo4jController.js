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
const getAuthorNetwork = asyncHandler(async (req, res) => {
  guardNeo4j();
  const { name } = req.params;
  const limit = Math.min(parseInt(req.query.limit) || 50, 200);

  const records = await runQuery(
    `
    MATCH (a:Author {name: $name})-[r:CO_AUTHORED]-(coAuthor:Author)
    RETURN coAuthor.name AS coAuthorName,
           r.shared_papers AS sharedPapers
    ORDER BY sharedPapers DESC
    LIMIT $limit
    `,
    { name, limit: neo4jInt(limit) }
  );

  const coAuthors = records.map(r => ({
    name:         r.get('coAuthorName'),
    sharedPapers: toNum(r.get('sharedPapers')),
  }));

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
    year:    toNum(r.get('year')),
  }));

  const firstAuthoredRecords = await runQuery(
    `
    MATCH (a:Author {name: $name})-[:FIRST_AUTHORED]->(p:Paper)
    WITH DISTINCT p
    OPTIONAL MATCH (p)-[:PUBLISHED_IN]->(j:Journal)
    OPTIONAL MATCH (p)-[:PUBLISHED_YEAR]->(y:Year)
    RETURN p.paper_id AS id, p.title AS title,
           j.name AS journal, y.value AS year
    ORDER BY year DESC
    LIMIT 10
    `,
    { name }
  );

  const firstAuthoredPapers = firstAuthoredRecords.map(r => ({
    id:      r.get('id'),
    title:   r.get('title'),
    journal: r.get('journal'),
    year:    toNum(r.get('year')),
  }));

  res.json({
    author:     name,
    paperCount: papers.length,
    papers,
    firstAuthoredCount: firstAuthoredPapers.length,
    firstAuthoredPapers,
    coAuthors,
    totalCoAuthors: coAuthors.length,
    source: 'neo4j',
    advantage: 'Graph traversal finds co-authors in O(1) hops vs SQL multi-join',
  });
});

// ── TOP AUTHORS ───────────────────────────────────────────────────────────────
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
const getPapersByYear = asyncHandler(async (req, res) => {
  guardNeo4j();

  const records = await runQuery(`
    MATCH (p:Paper)-[:PUBLISHED_YEAR]->(y:Year)
    RETURN y.value AS year, COUNT(DISTINCT p) AS paperCount
    ORDER BY year ASC
  `);

  const data = records.map(r => ({
    year:       toNum(r.get('year')),
    paperCount: toNum(r.get('paperCount')),
  }));

  res.json({ papersPerYear: data, source: 'neo4j' });
});

// ── PAPERS BY SOURCE ──────────────────────────────────────────────────────────
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
    year:     toNum(r.get('year')),
    doi:      r.get('doi'),
  }));

  res.json({ author: name, papers, count: papers.length, source: 'neo4j' });
});

// ── JOURNAL AUTHORS ───────────────────────────────────────────────────────────
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
    UNWIND $authors AS paperAuthorName
    MATCH (r:Author {name: $reviewer})
    OPTIONAL MATCH (r)-[direct:CO_AUTHORED]-(directAuthor:Author {name: paperAuthorName})
    OPTIONAL MATCH (r)-[:CO_AUTHORED]-(bridge:Author)-[:CO_AUTHORED]-(indirectAuthor:Author {name: paperAuthorName})
    WHERE bridge.name <> r.name AND bridge.name <> paperAuthorName
    RETURN paperAuthorName AS paperAuthor,
           SUM(DISTINCT direct.shared_papers) AS direct_coauthorships,
           COUNT(DISTINCT bridge) AS indirect_connections
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

// NEW FUNCTIONS

const getAuthorCollaborationStrength = asyncHandler(async (req, res) => {
  guardNeo4j();
  const { authorA, authorB } = req.query;

  if (!authorA || !authorB) {
    throw new AppError('Both authorA and authorB are required.', 400, 'MISSING_PARAM');
  }

  // Direct connection check via CO_AUTHORED
  const directRecords = await runQuery(
    `
    MATCH (a:Author {name: $authorA})-[r:CO_AUTHORED]-(b:Author {name: $authorB})
    RETURN r.shared_papers AS sharedPapers, true AS directLink
    `,
    { authorA, authorB }
  );

  // Two-hop path check
  const pathRecords = await runQuery(
    `
    MATCH path = shortestPath(
      (a:Author {name: $authorA})-[:CO_AUTHORED*1..3]-(b:Author {name: $authorB})
    )
    RETURN length(path) AS hops,
           [node IN nodes(path) | node.name] AS authorChain
    LIMIT 1
    `,
    { authorA, authorB }
  );

  const direct = directRecords[0];
  const path = pathRecords[0];

  res.json({
    authorA,
    authorB,
    direct_collaboration: !!direct,
    shared_papers: direct ? toNum(direct.get('sharedPapers')) : 0,
    shortest_path_hops: path ? toNum(path.get('hops')) : null,
    author_chain: path ? path.get('authorChain') : null,
    conflict_level: direct
      ? 'HIGH'
      : (path && toNum(path.get('hops')) <= 2 ? 'MEDIUM' : 'NONE'),
    source: 'neo4j',
  });
});

const getTopFirstAuthors = asyncHandler(async (req, res) => {
  guardNeo4j();
  const limit = Math.min(parseInt(req.query.limit) || 10, 100);

  const records = await runQuery(
    `
    MATCH (a:Author)-[:FIRST_AUTHORED]->(p:Paper)
    WITH a, COUNT(DISTINCT p) AS firstCount
    ORDER BY firstCount DESC
    LIMIT $limit
    MATCH (a)-[:WROTE]->(pAll:Paper)
    RETURN a.name AS author,
           firstCount,
           COUNT(DISTINCT pAll) AS totalCount
    ORDER BY firstCount DESC
    `,
    { limit: neo4jInt(limit) }
  );

  const authors = records.map(r => {
    const name = r.get('author');
    const firstCount = toNum(r.get('firstCount'));
    const totalCount = toNum(r.get('totalCount'));
    return {
      name,
      displayName: cleanAuthorDisplayName(name),
      firstAuthoredCount: firstCount,
      totalPaperCount: totalCount,
      leadershipRatio: totalCount > 0 ? Math.round((firstCount / totalCount) * 100) / 100 : 0,
    };
  });

  res.json({
    authors,
    source: 'neo4j',
    note: 'leadershipRatio = first_authored / total_papers. Optimized single-query lookup.',
  });
});

const findResearchPath = asyncHandler(async (req, res) => {
  guardNeo4j();
  const { from, to } = req.query;

  if (!from || !to) {
    throw new AppError('Both "from" and "to" author names are required.', 400, 'MISSING_PARAM');
  }
  if (from === to) {
    return res.json({ from, to, hops: 0, path: [from], source: 'neo4j' });
  }

  const records = await runQuery(
    `
    MATCH path = shortestPath(
      (a:Author {name: $from})-[:CO_AUTHORED*1..6]-(b:Author {name: $to})
    )
    RETURN length(path) AS hops,
           [node IN nodes(path) | node.name] AS authorNames,
           [rel IN relationships(path) | rel.shared_papers] AS sharedPapersAlongPath
    `,
    { from, to }
  );

  if (!records.length) {
    return res.json({
      from,
      to,
      hops: null,
      path: [],
      connected: false,
      message: 'No collaboration path found within 6 hops.',
      source: 'neo4j',
    });
  }

  const r = records[0];
  res.json({
    from,
    to,
    hops: toNum(r.get('hops')),
    path: r.get('authorNames'),
    shared_papers_along_path: (r.get('sharedPapersAlongPath') || []).map(toNum),
    connected: true,
    source: 'neo4j',
    use_case: 'Academic six-degrees of separation. Useful for finding indirect collaborators.',
  });
});

const getAuthorFirstAuthoredPapers = asyncHandler(async (req, res) => {
  guardNeo4j();
  const { name } = req.params;
  const limit = Math.min(parseInt(req.query.limit) || 20, 100);

  const records = await runQuery(
    `
    MATCH (a:Author {name: $name})-[:FIRST_AUTHORED]->(p:Paper)
    WITH DISTINCT p
    OPTIONAL MATCH (p)-[:PUBLISHED_IN]->(j:Journal)
    OPTIONAL MATCH (p)-[:PUBLISHED_YEAR]->(y:Year)
    RETURN p.paper_id AS id, p.title AS title,
           j.name AS journal, y.value AS year,
           j.best_quartile AS quartile
    ORDER BY year DESC
    LIMIT $limit
    `,
    { name, limit: neo4jInt(limit) }
  );

  const papers = records.map(r => ({
    id: r.get('id'),
    title: r.get('title'),
    journal: r.get('journal'),
    year: toNum(r.get('year')),
    quartile: r.get('quartile'),
  }));

  res.json({
    author: name,
    first_authored_papers: papers,
    count: papers.length,
    source: 'neo4j',
  });
});

const getCollaborationLeaderboard = asyncHandler(async (req, res) => {
  guardNeo4j();
  const limit = Math.min(parseInt(req.query.limit) || 15, 50);

  const records = await runQuery(
    `
    MATCH (a:Author)-[r:CO_AUTHORED]-(collaborator:Author)
    WITH a, COUNT(DISTINCT collaborator) AS uniqueCollaborators,
         SUM(r.shared_papers) AS totalSharedPapers
    ORDER BY uniqueCollaborators DESC
    LIMIT $limit
    RETURN a.name AS author,
           uniqueCollaborators,
           totalSharedPapers
    `,
    { limit: neo4jInt(limit) }
  );

  const leaderboard = records.map((r, i) => ({
    rank: i + 1,
    name: r.get('author'),
    displayName: cleanAuthorDisplayName(r.get('author')),
    uniqueCollaborators: toNum(r.get('uniqueCollaborators')),
    totalSharedPapers: toNum(r.get('totalSharedPapers')),
  }));

  res.json({
    leaderboard,
    source: 'neo4j',
    note: 'Ranked by number of unique co-authors. High score = research hub.',
  });
});

const getJournalImpactNetwork = asyncHandler(async (req, res) => {
  guardNeo4j();
  const { journal } = req.params;
  const limit = Math.min(parseInt(req.query.limit) || 10, 30);

  // Top authors in this journal
  const authorRecords = await runQuery(
    `
    MATCH (a:Author)-[:WROTE]->(p:Paper)-[:PUBLISHED_IN]->(j:Journal)
    WHERE toLower(j.name) CONTAINS toLower($journal)
    RETURN a.name AS author,
           COUNT(DISTINCT p) AS papers,
           j.name AS journalName,
           j.best_quartile AS quartile,
           j.sjr_index AS sjrIndex
    ORDER BY papers DESC
    LIMIT $limit
    `,
    { journal, limit: neo4jInt(limit) }
  );

  // Other journals these authors publish in (cross-journal activity)
  const crossJournalRecords = await runQuery(
    `
    MATCH (a:Author)-[:WROTE]->(p:Paper)-[:PUBLISHED_IN]->(j:Journal)
    WHERE toLower(j.name) CONTAINS toLower($journal)
    WITH COLLECT(DISTINCT a) AS authors
    UNWIND authors AS a
    MATCH (a)-[:WROTE]->(p2:Paper)-[:PUBLISHED_IN]->(j2:Journal)
    WHERE NOT toLower(j2.name) CONTAINS toLower($journal)
    RETURN j2.name AS relatedJournal,
           j2.best_quartile AS quartile,
           COUNT(DISTINCT p2) AS paperCount
    ORDER BY paperCount DESC
    LIMIT $limit
    `,
    { journal, limit: neo4jInt(limit) }
  );

  res.json({
    journal,
    top_authors: authorRecords.map(r => ({
      name: r.get('author'),
      papers: toNum(r.get('papers')),
      journalName: r.get('journalName'),
      quartile: r.get('quartile'),
      sjrIndex: r.get('sjrIndex') ? Number(r.get('sjrIndex')) : null,
    })),
    cross_journal_activity: crossJournalRecords.map(r => ({
      journal: r.get('relatedJournal'),
      quartile: r.get('quartile'),
      paperCount: toNum(r.get('paperCount')),
    })),
    source: 'neo4j',
  });
});

const getSourceDistribution = asyncHandler(async (req, res) => {
  guardNeo4j();

  const records = await runQuery(`
    MATCH (p:Paper)-[:FROM_SOURCE]->(s:Source)
    OPTIONAL MATCH (p)-[:PUBLISHED_YEAR]->(y:Year)
    RETURN s.name AS source,
           COUNT(DISTINCT p) AS paperCount,
           MIN(y.value) AS earliestYear,
           MAX(y.value) AS latestYear
    ORDER BY paperCount DESC
  `);

  const data = records.map(r => ({
    source: r.get('source'),
    paperCount: toNum(r.get('paperCount')),
    earliestYear: toNum(r.get('earliestYear')),
    latestYear: toNum(r.get('latestYear')),
  }));

  const total = data.reduce((sum, d) => sum + d.paperCount, 0);
  const enriched = data.map(d => ({
    ...d,
    percentage: total > 0 ? Math.round((d.paperCount / total) * 10000) / 100 : 0,
  }));

  res.json({ sources: enriched, totalPapers: total, source: 'neo4j' });
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
  // new
  getTopFirstAuthors,
  getAuthorFirstAuthoredPapers,
  getAuthorCollaborationStrength,
  findResearchPath,
  getCollaborationLeaderboard,
  getJournalImpactNetwork,
  getSourceDistribution,
};
