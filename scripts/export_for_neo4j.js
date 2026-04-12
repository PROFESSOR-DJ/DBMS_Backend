// DBMS_Backend/scripts/export_for_neo4j.js
// Exports MySQL paper-author relationships + MongoDB keywords to CSV.
// Run: cd DBMS_Backend && node scripts/export_for_neo4j.js

require('dotenv').config();
const mysql = require('mysql2/promise');
const { MongoClient } = require('mongodb');
const fs = require('fs');
const path = require('path');
const uri = "mongodb://localhost:27017/research_db";

const OUT_DIR = path.join(__dirname, 'neo4j_export');
if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR);

const csv = (value) => {
  if (value === null || value === undefined) return '';
  const str = String(value).replace(/"/g, '""').replace(/\n/g, ' ').trim();
  return `"${str}"`;
};

const writeCsv = (filename, headers, rows) => {
  const filepath = path.join(OUT_DIR, filename);
  const lines = [
    headers.join(','),
    ...rows.map(row => headers.map(h => csv(row[h])).join(','))
  ];
  fs.writeFileSync(filepath, lines.join('\n'), 'utf8');
  console.log(`  ✓ ${filename}: ${rows.length.toLocaleString()} rows`);
};

async function main() {
  const pool = mysql.createPool({
    host: process.env.MYSQL_HOST || 'localhost',
    port: process.env.MYSQL_PORT || 3306,
    user: process.env.MYSQL_USER || 'root',
    password: process.env.MYSQL_PASSWORD || 'root',
    database: process.env.MYSQL_DATABASE || 'research_mysql2',
    waitForConnections: true,
    connectionLimit: 3,
  });

  console.log("Mongo URI is:", uri);

  const mongoClient = new MongoClient(uri);

  await mongoClient.connect();
  const mongoDB = mongoClient.db('research_db');

  console.log('\n📦 Exporting data for Neo4j...\n');

  try {
    // ── 1. PAPERS ────────────────────────────────────────────────────────
    console.log('Exporting papers...');
    const [papers] = await pool.query(`
      SELECT DISTINCT
        p.paper_id, p.title, p.publish_year AS year, p.doi,
        p.has_full_text, p.is_covid19,
        j.journal_name AS journal, s.source_name AS source
      FROM papers p
      INNER JOIN paper_authors pa ON pa.paper_id = p.paper_id
      LEFT JOIN journals j ON j.journal_id = p.journal_id
      LEFT JOIN sources  s ON s.source_id  = p.source_id
      WHERE p.title IS NOT NULL AND TRIM(p.title) <> ''
      ORDER BY p.paper_id
    `);
    writeCsv('papers.csv',
      ['paper_id','title','year','doi','has_full_text','is_covid19','journal','source'],
      papers);

    // ── 2. AUTHORS ───────────────────────────────────────────────────────
    // Export only authors linked to at least one paper. Clean name at export.
    console.log('Exporting authors...');
    const [authors] = await pool.query(`
      SELECT DISTINCT
        a.author_id,
        TRIM(REGEXP_REPLACE(
          REGEXP_REPLACE(a.author_name, '^[[:space:]\\'\\"\\\\[\\\\]]+|[[:space:]\\'\\"\\\\[\\\\]]+$', ''),
          '[[:space:]]+', ' '
        )) AS author_name
      FROM authors a
      INNER JOIN paper_authors pa ON pa.author_id = a.author_id
      WHERE a.author_name IS NOT NULL
        AND TRIM(a.author_name) <> ''
        AND LENGTH(TRIM(a.author_name)) >= 2
      ORDER BY a.author_id
    `);
    writeCsv('authors.csv', ['author_id','author_name'], authors);

    // ── 3. PAPER_AUTHORS (WROTE + FIRST_AUTHORED source) ─────────────────
    // Same JOIN as journalModel.getJournalProfile() — this is the accurate path.
    console.log('Exporting paper-author relationships...');
    const [relationships] = await pool.query(`
      SELECT
        pa.paper_id,
        pa.author_order,
        TRIM(REGEXP_REPLACE(
          REGEXP_REPLACE(a.author_name, '^[[:space:]\\'\\"\\\\[\\\\]]+|[[:space:]\\'\\"\\\\[\\\\]]+$', ''),
          '[[:space:]]+', ' '
        )) AS author_name
      FROM paper_authors pa
      INNER JOIN authors a ON a.author_id = pa.author_id
      INNER JOIN papers  p ON p.paper_id  = pa.paper_id
      WHERE a.author_name IS NOT NULL
        AND TRIM(a.author_name) <> ''
        AND LENGTH(TRIM(a.author_name)) >= 2
        AND p.title IS NOT NULL
      ORDER BY pa.paper_id, pa.author_order
    `);
    writeCsv('paper_authors.csv',
      ['paper_id','author_name','author_order'], relationships);

    // ── 4. CO_AUTHORED (pre-computed — saves Neo4j heavy lifting) ────────
    // Pairs of authors who share at least one paper. author_a < author_b
    // alphabetically to avoid duplicate pairs.
    console.log('Exporting co-authorship pairs (this may take a moment)...');
    const [coAuthored] = await pool.query(`
      SELECT
        TRIM(REGEXP_REPLACE(
          REGEXP_REPLACE(a1.author_name, '^[[:space:]\\'\\"\\\\[\\\\]]+|[[:space:]\\'\\"\\\\[\\\\]]+$', ''),
          '[[:space:]]+', ' '
        )) AS author_a,
        TRIM(REGEXP_REPLACE(
          REGEXP_REPLACE(a2.author_name, '^[[:space:]\\'\\"\\\\[\\\\]]+|[[:space:]\\'\\"\\\\[\\\\]]+$', ''),
          '[[:space:]]+', ' '
        )) AS author_b,
        COUNT(DISTINCT pa1.paper_id) AS shared_papers
      FROM paper_authors pa1
      INNER JOIN paper_authors pa2
        ON pa1.paper_id = pa2.paper_id AND pa1.author_id < pa2.author_id
      INNER JOIN authors a1 ON a1.author_id = pa1.author_id
      INNER JOIN authors a2 ON a2.author_id = pa2.author_id
      WHERE LENGTH(TRIM(a1.author_name)) >= 2
        AND LENGTH(TRIM(a2.author_name)) >= 2
      GROUP BY pa1.author_id, pa2.author_id
      HAVING shared_papers >= 1
      ORDER BY shared_papers DESC, author_a, author_b
    `);
    writeCsv('co_authored.csv',
      ['author_a','author_b','shared_papers'], coAuthored);

    // ── 5. JOURNALS ──────────────────────────────────────────────────────
    console.log('Exporting journals...');
    const [journals] = await pool.query(`
      SELECT DISTINCT
        j.journal_name,
        COALESCE(jr.best_quartile, 'UNRANKED') AS best_quartile,
        jr.sjr_rank, jr.sjr_index, jr.h_index,
        jr.citescore, jr.country, jr.oa, jr.publisher
      FROM journals j
      INNER JOIN papers p ON p.journal_id = j.journal_id
      LEFT JOIN journal_rankings jr ON LOWER(jr.title) = LOWER(j.journal_name)
      WHERE j.journal_name IS NOT NULL AND TRIM(j.journal_name) <> ''
      ORDER BY j.journal_name
    `);
    writeCsv('journals.csv',
      ['journal_name','best_quartile','sjr_rank','sjr_index',
       'h_index','citescore','country','oa','publisher'], journals);

    // ── 6. YEARS + SOURCES ───────────────────────────────────────────────
    const [years] = await pool.query(`
      SELECT DISTINCT publish_year AS year FROM papers
      WHERE publish_year IS NOT NULL AND publish_year BETWEEN 1900 AND 2030
      ORDER BY publish_year
    `);
    writeCsv('years.csv', ['year'], years);

    const [sources] = await pool.query(`
      SELECT DISTINCT s.source_name FROM sources s
      INNER JOIN papers p ON p.source_id = s.source_id
      WHERE s.source_name IS NOT NULL ORDER BY s.source_name
    `);
    writeCsv('sources.csv', ['source_name'], sources);

    // ── 7. TOPICS (from MongoDB keywords) ───────────────────────────────
    // NEW: export keyword→paper associations for COVERS_TOPIC relationship.
    console.log('Exporting topics from MongoDB keywords...');
    const papers_col = mongoDB.collection('papers');

    // Get all distinct keywords that appear in at least 3 papers (noise filter)
    const topicRows = await papers_col.aggregate([
      { $unwind: '$keywords' },
      { $match: { keywords: { $nin: [null, '', 'nan'] } } },
      { $group: { _id: { $toLower: '$keywords' }, count: { $sum: 1 } } },
      { $match: { count: { $gte: 3 } } },
      { $project: { topic: '$_id', _id: 0 } },
      { $sort: { topic: 1 } }
    ]).toArray();
    writeCsv('topics.csv', ['topic'], topicRows);

    // Paper → topic links
    const paperTopicRows = await papers_col.aggregate([
      { $unwind: '$keywords' },
      { $match: { keywords: { $nin: [null, '', 'nan'] } } },
      {
        $group: {
          _id: { paper_id: '$paper_id', topic: { $toLower: '$keywords' } }
        }
      },
      { $project: { paper_id: '$_id.paper_id', topic: '$_id.topic', _id: 0 } },
      { $sort: { paper_id: 1, topic: 1 } }
    ]).toArray();
    writeCsv('paper_topics.csv', ['paper_id','topic'], paperTopicRows);

    // ── SUMMARY ──────────────────────────────────────────────────────────
    console.log('\n✅ Export complete.\n');
    console.log('📊 Counts:');
    console.log(`   Papers:          ${papers.length.toLocaleString()}`);
    console.log(`   Authors:         ${authors.length.toLocaleString()}`);
    console.log(`   WROTE links:     ${relationships.length.toLocaleString()}`);
    console.log(`   CO_AUTHORED:     ${coAuthored.length.toLocaleString()} pairs`);
    console.log(`   Journals:        ${journals.length.toLocaleString()}`);
    console.log(`   Topics:          ${topicRows.length.toLocaleString()}`);
    console.log(`   Paper→Topic:     ${paperTopicRows.length.toLocaleString()}`);
    console.log('\nNext: copy all CSVs to Neo4j import directory, then run the Cypher script.\n');

  } finally {
    await pool.end();
    await mongoClient.close();
  }
}

main().catch(err => {
  console.error('Export failed:', err);
  process.exitCode = 1;
});