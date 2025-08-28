import pandas as pd
from datetime import datetime, timezone
from sqlalchemy.orm import Session
from sqlalchemy import insert
from app.models.store_models import StoreStatus, TimeZone, MenuHours
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def parse_timestamp(timestamp_str: str):
    """Convert timestamp string to timezone-aware datetime"""
    timestamp_str = timestamp_str.replace(" UTC", "")
    dt = datetime.fromisoformat(timestamp_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def str_to_time(s):
    try:
        return pd.to_datetime(s, format="%H:%M:%S").time() if pd.notnull(s) else None
    except Exception:
        return None


def load_store_timezones(db: Session):
    """Load store timezones from CSV file"""
    try:
        df = pd.read_csv("data/timezones.csv")
        
        # Drop rows with missing required fields
        df = df.dropna(subset=["store_id", "timezone_str"])
        
        if df.empty:
            logging.warning("No valid timezone records to load")
            return
            
        records = [
            TimeZone(store_id=row["store_id"], timezone_str=row["timezone_str"])
            for _, row in df.iterrows()
        ]
        
        db.bulk_save_objects(records)
        db.commit()
        logging.info(f"Loaded {len(records)} store timezones")
        
    except FileNotFoundError:
        logging.error("data/timezones.csv file not found")
        db.rollback()
    except KeyError as e:
        logging.error(f"Missing required column {e} in timezones.csv")
        db.rollback()
    except Exception as e:
        logging.error(f"Error loading timezones: {e}")
        db.rollback()


def load_menu_hours(db: Session, csv_path="data/menu_hours.csv"):
    """Load business hours from CSV into DB safely (with timezone fallback)."""
    try:
        df = pd.read_csv(csv_path)

        if df.empty:
            logging.warning("menu_hours.csv is empty")
            return

        df = df.rename(columns={
            "dayOfWeek": "day_of_week",
            "start_time_local": "start_time_local",
            "end_time_local": "end_time_local",
        })

        # Convert times
        df["start_time_local"] = df["start_time_local"].apply(str_to_time)
        df["end_time_local"] = df["end_time_local"].apply(str_to_time)

        valid_store_ids = {str(row[0]) for row in db.query(TimeZone.store_id).all()}
        incoming_store_ids = set(df["store_id"].astype(str).unique())

        missing_store_ids = incoming_store_ids - valid_store_ids
        if missing_store_ids:
            logging.info(f"Adding {len(missing_store_ids)} missing stores with default timezone America/Chicago")
            db.execute(
                insert(TimeZone),
                [{"store_id": sid, "timezone_str": "America/Chicago"} for sid in missing_store_ids]
            )
            db.commit()
            valid_store_ids.update(missing_store_ids)

        df = df[df["store_id"].isin(valid_store_ids)]

        if df.empty:
            logging.warning("No valid menu_hours records to insert")
            return

        # Step 4: prepare records
        records = [
            {
                "store_id": row["store_id"],
                "day_of_week": int(row["day_of_week"]),
                "start_time_local": row["start_time_local"],
                "end_time_local": row["end_time_local"],
            }
            for _, row in df.iterrows()
        ]

        db.execute(insert(MenuHours), records)
        db.commit()

        logging.info(f"Loaded {len(records)} menu_hours records")

    except FileNotFoundError:
        logging.error("menu_hours.csv not found")
        db.rollback()
    except Exception as e:
        logging.error(f"Error loading business hours: {e}")
        db.rollback()


def load_store_status(db: Session):
    """Load store status data from CSV file in chunks"""
    chunk_size = 20000
    total = 0
    #max_records = 2000000

    try:
        valid_store_ids = {str(row[0]) for row in db.query(TimeZone.store_id).all()}
        
        if not valid_store_ids:
            logging.warning("No valid store IDs found in timezones table. Load timezones first.")
            return

        for chunk in pd.read_csv("data/store_status.csv", chunksize=chunk_size):
            # if total >= max_records:   # uncomment if max_record is required
            #     break


            chunk = chunk.dropna(subset=["store_id", "status", "timestamp_utc"])
            chunk["store_id"] = chunk["store_id"].astype(str)
            chunk = chunk[chunk["store_id"].isin(valid_store_ids)]

            if chunk.empty:
                continue

            # rows_left = max_records - total
            # if len(chunk) > rows_left:
            #     chunk = chunk.iloc[:rows_left]  #uncomment if max_record is required

            records = [
                {
                    "store_id": row["store_id"],
                    "status": row["status"],
                    "timestamp_utc": parse_timestamp(row["timestamp_utc"]),
                }
                for _, row in chunk.iterrows()
            ]

            if records:
                db.execute(insert(StoreStatus), records)
                db.commit()
                total += len(records)

        logging.info(f"Loaded {total} store status records")
        
    except FileNotFoundError:
        logging.error("data/store_status.csv file not found")
        db.rollback()
    except KeyError as e:
        logging.error(f"Missing required column {e} in store_status.csv")
        db.rollback()
    except Exception as e:
        logging.error(f"Error loading store status: {e}")
        db.rollback()


def load_csv(db: Session):
    """Master loader called from FastAPI endpoint"""
    logging.info("Starting CSV data loading process...")

    load_store_timezones(db)
    load_menu_hours(db)
    load_store_status(db)
    
    logging.info("\nCSV loading process completed!")