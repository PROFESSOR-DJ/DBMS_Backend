// Cleans leading/trailing list/quote artifacts from MongoDB paper authors.
//
// Run after taking a backup:
//   cd DBMS_Backend
//   node scripts/clean_author_names_mongodb.js

require('dotenv').config();
const { MongoClient } = require('mongodb');

const cleanAuthorName = (value) => String(value || '')
  .trim()
  .replace(/^[\s[\]'"]+|[\s[\]'"]+$/g, '')
  .replace(/\s+/g, ' ');

const sameArray = (left, right) =>
  left.length === right.length && left.every((value, index) => value === right[index]);

async function main() {
  const uri = process.env.MONGODB_URI;
  if (!uri) {
    throw new Error('MONGODB_URI is not set in .env');
  }

  const dbName = process.env.MONGODB_DB || 'research_db';
  const client = new MongoClient(uri);
  await client.connect();

  try {
    const collection = client.db(dbName).collection('papers');
    const cursor = collection.find(
      { authors: { $exists: true, $type: 'array' } },
      { projection: { _id: 1, authors: 1 } }
    );

    let scanned = 0;
    let updated = 0;

    for await (const paper of cursor) {
      scanned += 1;
      const cleanedAuthors = [...new Set(
        paper.authors
          .map(cleanAuthorName)
          .filter(Boolean)
      )];

      if (!sameArray(paper.authors, cleanedAuthors)) {
        await collection.updateOne(
          { _id: paper._id },
          { $set: { authors: cleanedAuthors, updated_at: new Date() } }
        );
        updated += 1;
      }
    }

    const remainingDirty = await collection.countDocuments({
      authors: { $elemMatch: { $regex: /^[\s\[\]'"]+|[\s\[\]'"]+$/ } },
    });

    console.log(`Scanned ${scanned} papers.`);
    console.log(`Updated ${updated} papers.`);
    console.log(`Remaining dirty author entries: ${remainingDirty}.`);
  } finally {
    await client.close();
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
