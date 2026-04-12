const { getMongoDB } = require('../config/database');
const { runQuery, isNeo4jConnected } = require('../config/neo4jDatabase');

/**
 * syncAuthorCreate
 * Propagates new author linkage to MongoDB and Neo4j.
 * @param {string} name - Author name
 * @param {string} paper_id - Linked paper ID
 */
const syncAuthorCreate = async (name, paper_id) => {
  // 1. MongoDB Sync: Add author to paper's authors array
  try {
    const db = getMongoDB();
    const collection = db.collection('papers');
    await collection.updateOne(
      { paper_id: String(paper_id) },
      { $addToSet: { authors: name } }
    );
  } catch (err) {
    console.error(`MongoDB syncAuthorCreate failed for paper ${paper_id}:`, err.message);
  }

  // 2. Neo4j Sync: Create Author node and WROTE relationship
  if (isNeo4jConnected()) {
    try {
      await runQuery(
        `
        MERGE (a:Author {name: $name})
        WITH a
        MATCH (p:Paper {paper_id: $paper_id})
        MERGE (a)-[:WROTE]->(p)
        `,
        { name, paper_id: String(paper_id) }
      );
    } catch (err) {
      console.error(`Neo4j syncAuthorCreate failed for author ${name}:`, err.message);
    }
  }
};

/**
 * syncAuthorUpdate
 * Renames author across all MongoDB documents and updates the Neo4j Author node.
 * @param {string} oldName - Previous author name
 * @param {string} newName - New author name
 */
const syncAuthorUpdate = async (oldName, newName) => {
  if (!oldName || !newName || oldName === newName) return;

  // 1. MongoDB Sync: Replace old name with new name in all authors arrays
  try {
    const db = getMongoDB();
    const collection = db.collection('papers');
    
    // Use the positional operator $[element] to update all occurrences of oldName in the authors array
    await collection.updateMany(
      { authors: oldName },
      { $set: { "authors.$[elem]": newName } },
      { arrayFilters: [{ "elem": oldName }] }
    );
  } catch (err) {
    console.error(`MongoDB syncAuthorUpdate failed from ${oldName} to ${newName}:`, err.message);
  }

  // 2. Neo4j Sync: Update Author node name
  if (isNeo4jConnected()) {
    try {
      // Find the author and update name. 
      // Note: If newName already exists, this might need merging logic, 
      // but for simple renaming we'll just update the property.
      await runQuery(
        `
        MATCH (a:Author {name: $oldName})
        SET a.name = $newName
        `,
        { oldName, newName }
      );
    } catch (err) {
      console.error(`Neo4j syncAuthorUpdate failed for author ${oldName}:`, err.message);
    }
  }
};

/**
 * syncAuthorDelete
 * Removes author from all MongoDB paper documents and completely deletes the Author node in Neo4j.
 * @param {string} authorName - Author name
 */
const syncAuthorDelete = async (authorName) => {
  if (!authorName) return;

  // 1. MongoDB Sync: Pull author name from all papers' authors arrays
  try {
    const db = getMongoDB();
    const collection = db.collection('papers');
    
    await collection.updateMany(
      { authors: authorName },
      { $pull: { authors: authorName } }
    );
  } catch (err) {
    console.error(`MongoDB syncAuthorDelete failed for author ${authorName}:`, err.message);
  }

  // 2. Neo4j Sync: Detach and delete the Author node completely
  if (isNeo4jConnected()) {
    try {
      await runQuery(
        `
        MATCH (a:Author {name: $authorName})
        DETACH DELETE a
        `,
        { authorName }
      );
    } catch (err) {
      console.error(`Neo4j syncAuthorDelete failed for author ${authorName}:`, err.message);
    }
  }
};

module.exports = {
  syncAuthorCreate,
  syncAuthorUpdate,
  syncAuthorDelete
};
