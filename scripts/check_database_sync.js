// Checks whether MySQL, MongoDB, and Neo4j contain matching paper ids and
// whether an author visible in Neo4j also appears in MongoDB/MySQL.
//
// Usage:
//   cd DBMS_Backend
//   node scripts/check_database_sync.js
//   node scripts/check_database_sync.js "Acharyulu"

require('dotenv').config();
const mysql = require('mysql2/promise');
const { MongoClient } = require('mongodb');
const neo4j = require('neo4j-driver');

const sampleAuthor = process.argv[2] || '';

const cleanName = value => String(value || '')
  .trim()
  .replace(/^[\s[\]'"]+|[\s[\]'"]+$/g, '')
  .replace(/\s+/g, ' ');

const toNumber = value => {
  if (value === null || value === undefined) return 0;
  if (typeof value === 'number') return value;
  if (typeof value.toNumber === 'function') return value.toNumber();
  return Number(value);
};

async function main() {
  const mysqlPool = mysql.createPool({
    host: process.env.MYSQL_HOST || 'localhost',
    port: process.env.MYSQL_PORT || 3306,
    user: process.env.MYSQL_USER || 'root',
    password: process.env.MYSQL_PASSWORD || 'root',
    database: process.env.MYSQL_DATABASE || 'research_sql',
    waitForConnections: true,
    connectionLimit: 2,
  });

  const mongoClient = new MongoClient(process.env.MONGODB_URI);
  const neo4jDriver = neo4j.driver(
    process.env.NEO4J_URI || 'bolt://localhost:7687',
    neo4j.auth.basic(process.env.NEO4J_USER || 'neo4j', process.env.NEO4J_PASSWORD || 'Gitcode123$')
  );

  try {
    await mongoClient.connect();
    const mongoDb = mongoClient.db(process.env.MONGODB_DB || 'research_db');
    const papersCollection = mongoDb.collection('papers');
    const neoSession = neo4jDriver.session({ database: process.env.NEO4J_DATABASE || 'research-graph' });

    const [[mysqlPaperCount], [mysqlAuthorCount], mongoPaperCount, mongoAuthorCount, neoCounts] = await Promise.all([
      mysqlPool.query('SELECT COUNT(*) AS total FROM papers').then(([rows]) => rows),
      mysqlPool.query('SELECT COUNT(*) AS total FROM authors').then(([rows]) => rows),
      papersCollection.countDocuments(),
      papersCollection.aggregate([
        { $unwind: '$authors' },
        { $match: { authors: { $nin: [null, ''] } } },
        { $group: { _id: '$authors' } },
        { $count: 'total' },
      ], { allowDiskUse: true }).toArray().then(rows => rows[0]?.total || 0),
      neoSession.run(`
        MATCH (p:Paper)
        WITH count(p) AS papers
        MATCH (a:Author)
        RETURN papers, count(a) AS authors
      `).then(result => result.records[0]),
    ]);

    console.log('Counts');
    console.table([
      { store: 'MySQL', papers: mysqlPaperCount.total, authors: mysqlAuthorCount.total },
      { store: 'MongoDB', papers: mongoPaperCount, authors: mongoAuthorCount },
      {
        store: 'Neo4j',
        papers: toNumber(neoCounts.get('papers')),
        authors: toNumber(neoCounts.get('authors')),
      },
    ]);

    const [mysqlSamples] = await mysqlPool.query(`
      SELECT p.paper_id, p.title
      FROM papers p
      ORDER BY p.paper_id
      LIMIT 20
    `);

    const sampleIds = mysqlSamples.map(row => String(row.paper_id));
    const [mongoMatches, neoMatches] = await Promise.all([
      papersCollection
        .find({ paper_id: { $in: sampleIds } }, { projection: { paper_id: 1, title: 1 } })
        .toArray(),
      neoSession.run(
        `
        MATCH (p:Paper)
        WHERE p.paper_id IN $ids
        RETURN p.paper_id AS paper_id, p.title AS title
        `,
        { ids: sampleIds }
      ).then(result => result.records.map(record => ({
        paper_id: record.get('paper_id'),
        title: record.get('title'),
      }))),
    ]);

    console.log('Sample MySQL paper-id coverage in other stores');
    console.table({
      sampled_mysql_ids: sampleIds.length,
      found_in_mongodb: mongoMatches.length,
      found_in_neo4j: neoMatches.length,
      missing_in_mongodb: sampleIds.filter(id => !mongoMatches.some(paper => String(paper.paper_id) === id)).slice(0, 10).join(', '),
      missing_in_neo4j: sampleIds.filter(id => !neoMatches.some(paper => String(paper.paper_id) === id)).slice(0, 10).join(', '),
    });

    if (sampleAuthor) {
      const author = cleanName(sampleAuthor);
      const [mysqlAuthorRows, mongoAuthorRows, neoAuthorRows] = await Promise.all([
        mysqlPool.query(
          `
          SELECT a.author_name, COUNT(DISTINCT pa.paper_id) AS papers
          FROM authors a
          LEFT JOIN paper_authors pa ON pa.author_id = a.author_id
          WHERE a.author_name LIKE ?
          GROUP BY a.author_id, a.author_name
          ORDER BY papers DESC
          LIMIT 10
          `,
          [`%${author}%`]
        ).then(([rows]) => rows),
        papersCollection
          .find({ authors: { $regex: author.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), $options: 'i' } }, { projection: { paper_id: 1, title: 1, authors: 1 } })
          .limit(10)
          .toArray(),
        neoSession.run(
          `
          MATCH (a:Author)-[:WROTE]->(p:Paper)
          WHERE toLower(a.name) CONTAINS toLower($author)
          RETURN a.name AS author, count(DISTINCT p) AS papers
          ORDER BY papers DESC
          LIMIT 10
          `,
          { author }
        ).then(result => result.records.map(record => ({
          author: record.get('author'),
          papers: toNumber(record.get('papers')),
        }))),
      ]);

      console.log(`Author lookup: ${author}`);
      console.log('MySQL');
      console.table(mysqlAuthorRows);
      console.log('MongoDB papers with matching author');
      console.table(mongoAuthorRows.map(row => ({ paper_id: row.paper_id, title: row.title, authors: (row.authors || []).join(', ') })));
      console.log('Neo4j');
      console.table(neoAuthorRows);
    }

    await neoSession.close();
  } finally {
    await mysqlPool.end();
    await mongoClient.close().catch(() => {});
    await neo4jDriver.close().catch(() => {});
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
