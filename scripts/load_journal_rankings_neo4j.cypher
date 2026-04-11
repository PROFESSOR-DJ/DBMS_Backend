// Import journal_ranking_data.csv into existing :Journal nodes in Neo4j.
// Place the CSV inside Neo4j's import directory and run this script in Neo4j Browser.
// Example source path: file:///journal_ranking_data.csv

CREATE CONSTRAINT journal_name_unique IF NOT EXISTS
FOR (j:Journal)
REQUIRE j.name IS UNIQUE;

LOAD CSV WITH HEADERS FROM 'file:///journal_ranking_data.csv' AS row
WITH row
WHERE row.Title IS NOT NULL AND trim(row.Title) <> ''
MERGE (j:Journal {name: trim(row.Title)})
SET j.sjr_rank = CASE
        WHEN row.Rank IS NULL OR trim(row.Rank) = '' THEN NULL
        ELSE toInteger(trim(row.Rank))
    END,
    j.oa = CASE
        WHEN row.OA IS NULL OR trim(row.OA) = '' THEN NULL
        WHEN toUpper(trim(row.OA)) IN ['YES', 'Y', 'TRUE', '1'] THEN true
        WHEN toUpper(trim(row.OA)) IN ['NO', 'N', 'FALSE', '0'] THEN false
        ELSE NULL
    END,
    j.country = CASE
        WHEN row.Country IS NULL OR trim(row.Country) = '' THEN NULL
        ELSE trim(row.Country)
    END,
    j.sjr_index = CASE
        WHEN row.`SJR-index` IS NULL OR trim(row.`SJR-index`) = '' THEN NULL
        ELSE toFloat(trim(row.`SJR-index`))
    END,
    j.citescore = CASE
        WHEN row.CiteScore IS NULL OR trim(row.CiteScore) = '' THEN NULL
        ELSE toFloat(trim(row.CiteScore))
    END,
    j.h_index = CASE
        WHEN row.`H-index` IS NULL OR trim(row.`H-index`) = '' THEN NULL
        ELSE toInteger(trim(row.`H-index`))
    END,
    j.best_quartile = CASE
        WHEN row.`Best Quartile` IS NULL OR trim(row.`Best Quartile`) = '' THEN NULL
        ELSE trim(row.`Best Quartile`)
    END,
    j.best_categories = CASE
        WHEN row.`Best Categories` IS NULL OR trim(row.`Best Categories`) = '' THEN NULL
        ELSE trim(row.`Best Categories`)
    END,
    j.best_subject_area = CASE
        WHEN row.`Best Subject Area` IS NULL OR trim(row.`Best Subject Area`) = '' THEN NULL
        ELSE trim(row.`Best Subject Area`)
    END,
    j.best_subject_rank = CASE
        WHEN row.`Best Subject Rank` IS NULL OR trim(row.`Best Subject Rank`) = '' THEN NULL
        ELSE trim(row.`Best Subject Rank`)
    END,
    j.total_docs = CASE
        WHEN row.`Total Docs.` IS NULL OR trim(row.`Total Docs.`) = '' THEN NULL
        ELSE toInteger(trim(row.`Total Docs.`))
    END,
    j.total_docs_3y = CASE
        WHEN row.`Total Docs. 3y` IS NULL OR trim(row.`Total Docs. 3y`) = '' THEN NULL
        ELSE toInteger(trim(row.`Total Docs. 3y`))
    END,
    j.total_refs = CASE
        WHEN row.`Total Refs.` IS NULL OR trim(row.`Total Refs.`) = '' THEN NULL
        ELSE toInteger(trim(row.`Total Refs.`))
    END,
    j.total_cites_3y = CASE
        WHEN row.`Total Cites 3y` IS NULL OR trim(row.`Total Cites 3y`) = '' THEN NULL
        ELSE toInteger(trim(row.`Total Cites 3y`))
    END,
    j.citable_docs_3y = CASE
        WHEN row.`Citable Docs. 3y` IS NULL OR trim(row.`Citable Docs. 3y`) = '' THEN NULL
        ELSE toInteger(trim(row.`Citable Docs. 3y`))
    END,
    j.cites_per_doc_2y = CASE
        WHEN row.`Cites/Doc. 2y` IS NULL OR trim(row.`Cites/Doc. 2y`) = '' THEN NULL
        ELSE toFloat(trim(row.`Cites/Doc. 2y`))
    END,
    j.refs_per_doc = CASE
        WHEN row.`Refs./Doc.` IS NULL OR trim(row.`Refs./Doc.`) = '' THEN NULL
        ELSE toFloat(trim(row.`Refs./Doc.`))
    END,
    j.publisher = CASE
        WHEN row.Publisher IS NULL OR trim(row.Publisher) = '' THEN NULL
        ELSE trim(row.Publisher)
    END,
    j.core_collection = CASE
        WHEN row.`Core Collection` IS NULL OR trim(row.`Core Collection`) = '' THEN NULL
        ELSE trim(row.`Core Collection`)
    END,
    j.coverage = CASE
        WHEN row.Coverage IS NULL OR trim(row.Coverage) = '' THEN NULL
        ELSE trim(row.Coverage)
    END,
    j.active = CASE
        WHEN row.Active IS NULL OR trim(row.Active) = '' THEN NULL
        WHEN toUpper(trim(row.Active)) IN ['YES', 'Y', 'TRUE', '1'] THEN true
        WHEN toUpper(trim(row.Active)) IN ['NO', 'N', 'FALSE', '0'] THEN false
        ELSE NULL
    END,
    j.in_press = CASE
        WHEN row.`In-Press` IS NULL OR trim(row.`In-Press`) = '' THEN NULL
        WHEN toUpper(trim(row.`In-Press`)) IN ['YES', 'Y', 'TRUE', '1'] THEN true
        WHEN toUpper(trim(row.`In-Press`)) IN ['NO', 'N', 'FALSE', '0'] THEN false
        ELSE NULL
    END,
    j.iso_language_code = CASE
        WHEN row.`ISO Language Code` IS NULL OR trim(row.`ISO Language Code`) = '' THEN NULL
        ELSE trim(row.`ISO Language Code`)
    END,
    j.asjc_codes = CASE
        WHEN row.`ASJC Codes` IS NULL OR trim(row.`ASJC Codes`) = '' THEN NULL
        ELSE trim(row.`ASJC Codes`)
    END,
    j.ranking_source = 'journal_ranking_data.csv',
    j.ranking_last_loaded_at = datetime();
