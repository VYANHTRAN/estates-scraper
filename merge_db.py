import sqlite3
import os

def merge_databases(downloaded_db_path, local_master_path):
    if not os.path.exists(downloaded_db_path):
        print(f"Error: {downloaded_db_path} not found.")
        return

    print(f"Merging {downloaded_db_path} into {local_master_path}...")
    
    conn_master = sqlite3.connect(local_master_path)
    cursor_master = conn_master.cursor()

    # Attach the downloaded database
    cursor_master.execute(f"ATTACH DATABASE '{downloaded_db_path}' AS downloaded")

    try:
        # This query inserts records that don't exist.
        merge_query = """
        INSERT INTO listings (
            property_id, listing_title, total_price, unit_price, 
            property_url, image_url, city, district, alley_width, 
            features, property_description, has_updated, updated_at
        )
        SELECT 
            property_id, listing_title, total_price, unit_price, 
            property_url, image_url, city, district, alley_width, 
            features, property_description, has_updated, updated_at
        FROM downloaded.listings
        WHERE property_id NOT IN (SELECT property_id FROM main.listings)
        """
        
        cursor_master.execute(merge_query)
        rows_added = cursor_master.rowcount
        conn_master.commit()
        print(f"Successfully added {rows_added} new records to master database.")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        cursor_master.execute("DETACH DATABASE downloaded")
        conn_master.close()

if __name__ == "__main__":
    merge_databases("listings.db", "data/listings.db")