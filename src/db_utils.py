import sqlite3
import threading
from src.config import DB_PATH

class DatabaseManager:
    def __init__(self):
        self.db_path = DB_PATH
        self.lock = threading.Lock()
        self.conn = None
        self._init_db()

    def _init_db(self):
        """Initializes SQLite database and creates the table."""
        try:
            self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            cursor = self.conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS listings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    property_id TEXT,
                    listing_title TEXT,
                    total_price TEXT,
                    unit_price TEXT,
                    property_url TEXT,
                    image_url TEXT,
                    city TEXT,
                    district TEXT,
                    alley_width TEXT,
                    features TEXT,
                    property_description TEXT,
                    has_updated INTEGER DEFAULT 0, 
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_property_id ON listings (property_id);")
            self.conn.commit()
        except Exception as e:
            print(f"[CRITICAL] Database init failed: {e}")
            raise

    def save_listing(self, listing, fieldnames):
        """Inserts listing and updates history flags."""
        if not listing or not self.conn:
            return

        # Prepare data
        data = listing.copy()

        if isinstance(data.get("features"), list):
            data["features"] = "; ".join(data.get("features", []))
        if isinstance(data.get("property_description"), list):
            data["property_description"] = ". ".join(data.get("property_description", []))

        data_to_insert = {k: v for k, v in data.items() if k in fieldnames}
        
        property_id = data_to_insert.get("property_id")
        if not property_id:
            return

        with self.lock:
            try:
                cursor = self.conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM listings WHERE property_id = ?", (property_id,))
                exists = cursor.fetchone()[0] > 0
                
                has_updated_flag = 0
                if exists:
                    cursor.execute("UPDATE listings SET has_updated = 1 WHERE property_id = ?", (property_id,))
                    has_updated_flag = 1
                
                 # Insert new record (Always INSERT, never replace, to keep history)
                query = """
                INSERT INTO listings (
                    listing_title, property_id, total_price, unit_price, 
                    property_url, image_url, city, district, alley_width, 
                    features, property_description, has_updated, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """
                values = (
                    data_to_insert.get("listing_title"),
                    data_to_insert.get("property_id"),
                    data_to_insert.get("total_price"),
                    data_to_insert.get("unit_price"),
                    data_to_insert.get("property_url"),
                    data_to_insert.get("image_url"),
                    data_to_insert.get("city"),
                    data_to_insert.get("district"),
                    data_to_insert.get("alley_width"),
                    data_to_insert.get("features"),
                    data_to_insert.get("property_description"),
                    has_updated_flag 
                )
                
                cursor.execute(query, values)
                self.conn.commit()
            except Exception as e:
                print(f"[ERROR] DB Write Error: {e}")

    def get_latest_listings(self):
        """Retrieves only the most recent entry for each property_id."""
        query = """
            SELECT * FROM listings 
            WHERE id IN (SELECT MAX(id) FROM listings GROUP BY property_id)
        """
        return self.conn.execute(query)

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None