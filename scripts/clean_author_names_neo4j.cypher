// Cleans list/quote artifacts from Author.name in Neo4j in small batches.
//
// Run in Neo4j Browser after taking a backup:
//   :use research-graph
//   paste/run this file repeatedly until updated_count returns 0.
//   If Neo4j still reports MemoryPoolOutOfMemoryError, reduce LIMIT 100 to 25.
//
// This version does not require APOC. It removes [, ], ', and " characters
// anywhere in author names, then trims whitespace.

MATCH (a:Author)
WHERE a.name CONTAINS '['
   OR a.name CONTAINS ']'
   OR a.name CONTAINS "'"
   OR a.name CONTAINS '"'
WITH a,
     trim(
       replace(
         replace(
           replace(
             replace(a.name, '[', ''),
             ']', ''
           ),
           "'", ''
         ),
         '"', ''
       )
     ) AS cleanedName
WHERE cleanedName <> '' AND cleanedName <> a.name
WITH a, cleanedName
LIMIT 100
SET a.name = cleanedName
RETURN count(a) AS updated_count;

MATCH (a:Author)
WHERE a.name CONTAINS '['
   OR a.name CONTAINS ']'
   OR a.name CONTAINS "'"
   OR a.name CONTAINS '"'
RETURN a.name AS sample_remaining_dirty_name
LIMIT 10;

MATCH (a:Author)
WHERE a.name CONTAINS '['
   OR a.name CONTAINS ']'
   OR a.name CONTAINS "'"
   OR a.name CONTAINS '"'
RETURN a.name AS still_dirty
LIMIT 25;
