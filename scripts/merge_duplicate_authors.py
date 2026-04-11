import mysql.connector
import time

def merge_duplicate_authors(max_retries=5):
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="root",
        database="research_mysql2"
    )
    cursor = conn.cursor()

    while True:
        moved_links = 0
        deleted_authors = 0

        for attempt in range(max_retries):
            try:
                cursor.execute("""CREATE TEMPORARY TABLE duplicate_author_batch AS
                    SELECT a.author_id AS duplicate_author_id,
                           keepers.keep_author_id
                    FROM authors a
                    JOIN (
                      SELECT MIN(author_id) AS keep_author_id, author_name
                      FROM authors
                      WHERE author_name IS NOT NULL AND TRIM(author_name) <> ''
                      GROUP BY author_name
                      HAVING COUNT(*) > 1
                    ) keepers ON keepers.author_name = a.author_name
                    WHERE a.author_id <> keepers.keep_author_id
                    ORDER BY a.author_id
                    LIMIT 10;""")

                cursor.execute("""UPDATE IGNORE paper_authors pa
                    JOIN duplicate_author_batch batch ON batch.duplicate_author_id = pa.author_id
                    SET pa.author_id = batch.keep_author_id;""")
                conn.commit()
                cursor.execute("SELECT ROW_COUNT();")
                moved_links = cursor.fetchone()[0]

                cursor.execute("""DELETE a
                    FROM authors a
                    JOIN duplicate_author_batch batch ON batch.duplicate_author_id = a.author_id
                    LEFT JOIN paper_authors pa ON pa.author_id = a.author_id
                    WHERE pa.author_id IS NULL;""")
                conn.commit()
                cursor.execute("SELECT ROW_COUNT();")
                deleted_authors = cursor.fetchone()[0]

                cursor.execute("DROP TEMPORARY TABLE duplicate_author_batch;")
                break

            except mysql.connector.errors.DatabaseError as e:
                if "Lock wait timeout exceeded" in str(e):
                    print(f"Lock wait timeout, retrying... (attempt {attempt+1})")
                    time.sleep(2)
                    continue
                else:
                    raise

        print(f"Batch result: moved_links={moved_links}, deleted_authors={deleted_authors}")

        # stop looping when nothing left to merge
        if moved_links == 0 and deleted_authors == 0:
            print("No more duplicates found.")
            break

    cursor.close()
    conn.close()

if __name__ == "__main__":
    merge_duplicate_authors()