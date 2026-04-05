const { getNeo4jSession, isNeo4jConnected } = require('../config/neo4jDatabase');

const normalisePaperForGraph = (paper = {}) => {
  const authors = Array.isArray(paper.authors)
    ? paper.authors
        .map((author) => String(author || '').trim())
        .filter(Boolean)
        .filter((author, index, list) => list.indexOf(author) === index)
        .map((name, index) => ({ name, order: index + 1 }))
    : [];

  return {
    paper_id: String(paper.paper_id || '').trim(),
    title: String(paper.title || '').trim(),
    abstract: paper.abstract ? String(paper.abstract).trim() : '',
    doi: paper.doi ? String(paper.doi).trim() : '',
    journal: paper.journal ? String(paper.journal).trim() : '',
    source: paper.source ? String(paper.source).trim() : 'manual',
    year: Number.isInteger(Number(paper.year)) ? Number(paper.year) : null,
    has_full_text: Boolean(paper.has_full_text),
    is_covid19: Boolean(paper.is_covid19),
    authors,
  };
};

const ensureNeo4jAvailable = () => {
  if (!isNeo4jConnected()) {
    throw new Error('Neo4j is not connected.');
  }
};

const syncPaperToGraph = async (paper) => {
  ensureNeo4jAvailable();
  const payload = normalisePaperForGraph(paper);
  const session = getNeo4jSession();
  const tx = session.beginTransaction();

  try {
    await tx.run(
      `
        MERGE (p:Paper {paper_id: $paper_id})
        SET p.title = $title,
            p.abstract = $abstract,
            p.doi = $doi,
            p.has_full_text = $has_full_text,
            p.is_covid19 = $is_covid19
      `,
      payload
    );

    await tx.run(
      `
        MATCH (p:Paper {paper_id: $paper_id})
        OPTIONAL MATCH (p)-[rj:PUBLISHED_IN]->(:Journal)
        DELETE rj
      `,
      { paper_id: payload.paper_id }
    );

    await tx.run(
      `
        MATCH (p:Paper {paper_id: $paper_id})
        OPTIONAL MATCH (p)-[ry:PUBLISHED_YEAR]->(:Year)
        DELETE ry
      `,
      { paper_id: payload.paper_id }
    );

    await tx.run(
      `
        MATCH (p:Paper {paper_id: $paper_id})
        OPTIONAL MATCH (p)-[rs:FROM_SOURCE]->(:Source)
        DELETE rs
      `,
      { paper_id: payload.paper_id }
    );

    await tx.run(
      `
        MATCH (:Author)-[rw:WROTE]->(p:Paper {paper_id: $paper_id})
        DELETE rw
      `,
      { paper_id: payload.paper_id }
    );

    if (payload.journal) {
      await tx.run(
        `
          MATCH (p:Paper {paper_id: $paper_id})
          MERGE (j:Journal {name: $journal})
          MERGE (p)-[:PUBLISHED_IN]->(j)
        `,
        payload
      );
    }

    if (payload.year !== null) {
      await tx.run(
        `
          MATCH (p:Paper {paper_id: $paper_id})
          MERGE (y:Year {value: $year})
          MERGE (p)-[:PUBLISHED_YEAR]->(y)
        `,
        payload
      );
    }

    if (payload.source) {
      await tx.run(
        `
          MATCH (p:Paper {paper_id: $paper_id})
          MERGE (s:Source {name: $source})
          MERGE (p)-[:FROM_SOURCE]->(s)
        `,
        payload
      );
    }

    if (payload.authors.length) {
      await tx.run(
        `
          MATCH (p:Paper {paper_id: $paper_id})
          UNWIND $authors AS author
          MERGE (a:Author {name: author.name})
          MERGE (a)-[r:WROTE]->(p)
          SET r.author_order = author.order
        `,
        {
          paper_id: payload.paper_id,
          authors: payload.authors,
        }
      );
    }

    await tx.commit();
  } catch (error) {
    await tx.rollback();
    throw error;
  } finally {
    await session.close();
  }
};

const removePaperFromGraph = async (paperId) => {
  if (!isNeo4jConnected()) {
    return;
  }

  const session = getNeo4jSession();
  const tx = session.beginTransaction();

  try {
    await tx.run(
      `
        MATCH (p:Paper {paper_id: $paper_id})
        DETACH DELETE p
      `,
      { paper_id: paperId }
    );

    await tx.run('MATCH (a:Author) WHERE NOT (a)-[:WROTE]->(:Paper) DELETE a');
    await tx.run('MATCH (j:Journal) WHERE NOT (:Paper)-[:PUBLISHED_IN]->(j) DELETE j');
    await tx.run('MATCH (y:Year) WHERE NOT (:Paper)-[:PUBLISHED_YEAR]->(y) DELETE y');
    await tx.run('MATCH (s:Source) WHERE NOT (:Paper)-[:FROM_SOURCE]->(s) DELETE s');

    await tx.commit();
  } catch (error) {
    await tx.rollback();
    throw error;
  } finally {
    await session.close();
  }
};

module.exports = {
  syncPaperToGraph,
  removePaperFromGraph,
};
