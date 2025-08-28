from collections import defaultdict
from uuid import UUID
import csv
from datetime import datetime, timedelta, time
from io import StringIO
from typing import Dict, List, Optional, Tuple, Generator
from pathlib import Path
import logging

import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import func
from zoneinfo import ZoneInfo

from app.db.session import SessionLocal
from app.models.report_models import Report, ReportJob, ReportResult, ReportStatus
from app.models.store_models import StoreStatus, TimeZone, MenuHours


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


BATCH_SIZE = 10000
DEFAULT_TIMEZONE = "America/Chicago"
REPORT_STORAGE_PATH = Path("reports")
REPORT_STORAGE_PATH.mkdir(exist_ok=True)


def _fetch_reference_data(session: Session) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Fetch reference tables (business hours and timezones) into memory for efficient lookups.
    """
    try:
        logger.info("Fetching reference data from database")
        
        # Fetch menu hours data
        mh_query = session.query(MenuHours).statement
        mh_df = pd.read_sql(mh_query, session.bind)
        
        # Fetch timezone data
        tz_query = session.query(TimeZone).statement
        tz_df = pd.read_sql(tz_query, session.bind)
        
        logger.info(f"Loaded {len(mh_df)} menu hours records and {len(tz_df)} timezone records")
        return mh_df, tz_df
        
    except Exception as e:
        logger.error(f"Failed to fetch reference data: {e}")
        raise


def _stream_status_logs(session: Session, batch_size: int = BATCH_SIZE) -> Generator[List[StoreStatus], None, None]:
    """
    Generator to fetch StoreStatus logs in batches for memory-efficient processing.
    """
    offset = 0
    total_processed = 0
    
    while True:
        try:
            batch = (session.query(StoreStatus)
                    .order_by(StoreStatus.store_id, StoreStatus.timestamp_utc)
                    .offset(offset)
                    .limit(batch_size)
                    .all())
            
            if not batch:
                logger.info(f"Completed processing {total_processed} log records")
                break
                
            total_processed += len(batch)
            logger.info(f"Processing batch of {len(batch)} records (total: {total_processed})")
            
            yield batch
            offset += batch_size
            
        except Exception as e:
            logger.error(f"Error fetching batch at offset {offset}: {e}")
            raise


def _build_timezone_lookup(tz_df: pd.DataFrame) -> Dict[UUID, str]:
    """
    Build a fast lookup dictionary mapping store_id to timezone string.
    """
    if tz_df.empty:
        logger.warning("No timezone data available, all stores will use default timezone")
        return {}

    def parse_timezone_row(row) -> Optional[Tuple[UUID, str]]:
        """Parse a single timezone row safely"""
        try:
            store_id = row['store_id']
            store_id = store_id if isinstance(store_id, UUID) else UUID(str(store_id))
            tz_str = str(row['timezone_str']) if pd.notnull(row['timezone_str']) else DEFAULT_TIMEZONE
            return store_id, tz_str
        except Exception as e:
            logger.warning(f"Skipping invalid timezone row: {e}")
            return None
        
    lookup = dict([
        pair for _, row in tz_df.iterrows() 
        if (pair := parse_timezone_row(row)) is not None
    ])

    logger.info(f"Built timezone lookup for {len(lookup)} stores")
    return lookup


def _convert_utc_to_local(batch_logs: List[StoreStatus], tz_lookup: Dict[UUID, str]) -> Generator[Dict, None, None]:
    """
    Convert UTC timestamps in batch logs to local timezone per store.
    """
    utc_tz = ZoneInfo("UTC")
    
    for log in batch_logs:
        try:
            store_id = UUID(str(log.store_id))
        except ValueError:
            continue

        tz_str = tz_lookup.get(store_id, DEFAULT_TIMEZONE)
        try:
            store_tz = ZoneInfo(tz_str)
        except Exception:
            store_tz = ZoneInfo(DEFAULT_TIMEZONE)

        # Ensure timestamp is timezone-aware in UTC
        if log.timestamp_utc.tzinfo is None:
            timestamp_utc = log.timestamp_utc.replace(tzinfo=utc_tz)
        else:
            timestamp_utc = log.timestamp_utc.astimezone(utc_tz)

        timestamp_local = timestamp_utc.astimezone(store_tz)

        yield {
            "store_id": store_id,
            "timestamp_local": timestamp_local,
            "status": log.status
        }


def _get_business_hours(mh_df: pd.DataFrame, store_id: UUID, day_of_week: int) -> Tuple[Optional[time], Optional[time]]:
    """
    Get business hours for a store on a specific day of week.
    """
    bh_rows = mh_df[(mh_df['store_id'] == store_id) & (mh_df['day_of_week'] == day_of_week)]
    
    if bh_rows.empty:
        return None, None
    
    bh_row = bh_rows.iloc[0]
    
    if pd.isna(bh_row['start_time_local']) or pd.isna(bh_row['end_time_local']):
        return None, None
    
    try:
        start_time_val = bh_row['start_time_local']
        end_time_val = bh_row['end_time_local']
        
        def convert_to_time(val):
            if isinstance(val, time):
                return val
            elif isinstance(val, str):
                return pd.to_datetime(val).time()
            else:
                return pd.to_datetime(str(val)).time()
        
        start_time = convert_to_time(start_time_val)
        end_time = convert_to_time(end_time_val)
        
        return start_time, end_time
        
    except Exception:
        # If parsing fails, assume 24x7
        return None, None


def _calculate_business_period_status(logs: List[Dict], business_start: datetime, business_end: datetime) -> Tuple[float, float]:
    """
    Calculate uptime and downtime for a single business period using interpolation/extrapolation.
    """
    total_business_minutes = (business_end - business_start).total_seconds() / 60
    
    if not logs:
        return total_business_minutes, 0.0

    business_logs = [
        log for log in logs 
        if business_start <= log['timestamp_local'] <= business_end
    ]
    
    if not business_logs:
        return total_business_minutes, 0.0
    
    business_logs.sort(key=lambda x: x['timestamp_local'])
    
    uptime = 0.0
    downtime = 0.0
    current_time = business_start
    
    first_log = business_logs[0]
    first_time = first_log['timestamp_local']
    first_status = first_log['status']
    
    if first_time > business_start:
        duration = (first_time - business_start).total_seconds() / 60
        if first_status == 'active':
            uptime += duration
        else:
            downtime += duration
        current_time = first_time
    
    current_status = first_status
    
    for i, log in enumerate(business_logs):
        log_time = log['timestamp_local']
        
        if log_time > current_time:
            duration = (log_time - current_time).total_seconds() / 60
            if current_status == 'active':
                uptime += duration
            else:
                downtime += duration
        
        current_time = log_time
        current_status = log['status']
    
    if current_time < business_end:
        duration = (business_end - current_time).total_seconds() / 60
        if current_status == 'active':
            uptime += duration
        else:
            downtime += duration
    
    return uptime, downtime


def _calculate_interval_metrics(store_logs: List[Dict], mh_df: pd.DataFrame, store_id: UUID, interval_start: datetime, interval_end: datetime) -> Dict[str, float]:
    """
    Calculate uptime/downtime metrics for a store within a time interval.
    Only considers business hours and uses interpolation for missing data.
    """
    interval_logs = [
        log for log in store_logs
        if interval_start <= log['timestamp_local'] <= interval_end
    ]
    
    total_uptime = 0.0
    total_downtime = 0.0
    
    current_date = interval_start.date()
    end_date = interval_end.date()
    
    while current_date <= end_date:
        day_start = datetime.combine(current_date, time.min).replace(tzinfo=interval_start.tzinfo)
        day_end = datetime.combine(current_date, time.max).replace(tzinfo=interval_start.tzinfo)
        
        day_start = max(day_start, interval_start)
        day_end = min(day_end, interval_end)
        
        if day_start >= day_end:
            current_date += timedelta(days=1)
            continue
        
        day_of_week = current_date.weekday()  # 0=Monday, 6=Sunday
        start_time, end_time = _get_business_hours(mh_df, store_id, day_of_week)
        
        if start_time is None or end_time is None:
            # 24x7 operation
            business_start = day_start
            business_end = day_end
        else:
            business_start = datetime.combine(current_date, start_time).replace(tzinfo=interval_start.tzinfo)
            business_end = datetime.combine(current_date, end_time).replace(tzinfo=interval_start.tzinfo)
            
            if end_time < start_time:
                business_end += timedelta(days=1)
            
            business_start = max(business_start, day_start)
            business_end = min(business_end, day_end)
        
        if business_start >= business_end:
            # No business hours this day
            current_date += timedelta(days=1)
            continue
        
        day_logs = [
            log for log in interval_logs
            if day_start <= log['timestamp_local'] <= day_end
        ]
        
        day_uptime, day_downtime = _calculate_business_period_status(day_logs, business_start, business_end)
        total_uptime += day_uptime
        total_downtime += day_downtime
        
        current_date += timedelta(days=1)
    
    return {
        "uptime_minutes": total_uptime,
        "downtime_minutes": total_downtime
    }


def _generate_store_report(store_logs: List[Dict], mh_df: pd.DataFrame, store_id: UUID, max_timestamp: datetime) -> ReportResult:
    """
    Generate uptime/downtime report for a single store.
    """
    last_hour_start = max_timestamp - timedelta(hours=1)
    last_day_start = max_timestamp - timedelta(days=1)
    last_week_start = max_timestamp - timedelta(days=7)

    hour_metrics = _calculate_interval_metrics(store_logs, mh_df, store_id, last_hour_start, max_timestamp)
    day_metrics = _calculate_interval_metrics(store_logs, mh_df, store_id, last_day_start, max_timestamp)
    week_metrics = _calculate_interval_metrics(store_logs, mh_df, store_id, last_week_start, max_timestamp)

    return ReportResult(
        store_id=str(store_id),
        uptime_last_hour_in_minutes=max(0, round(hour_metrics['uptime_minutes'])),
        uptime_last_day_in_hours=max(0.0, round(day_metrics['uptime_minutes'] / 60, 2)),
        uptime_last_week_in_hours=max(0.0, round(week_metrics['uptime_minutes'] / 60, 2)),
        downtime_last_hour_in_minutes=max(0, round(hour_metrics['downtime_minutes'])),
        downtime_last_day_in_hours=max(0.0, round(day_metrics['downtime_minutes'] / 60, 2)),
        downtime_last_week_in_hours=max(0.0, round(week_metrics['downtime_minutes'] / 60, 2))
    )


def generate_report_csv(results: List[ReportResult]) -> str:
    """
    Generate CSV content from report results.
    """
    if not results:
        logger.warning("No results to generate CSV")
        return ""
    
    output = StringIO()
    fieldnames = [
        'store_id',
        'uptime_last_hour(in minutes)',
        'uptime_last_day(in hours)',
        'uptime_last_week(in hours)',
        'downtime_last_hour(in minutes)',
        'downtime_last_day(in hours)',
        'downtime_last_week(in hours)'
    ]
    
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    
    for result in results:
        writer.writerow({
            'store_id': result.store_id,
            'uptime_last_hour(in minutes)': result.uptime_last_hour_in_minutes,
            'uptime_last_day(in hours)': result.uptime_last_day_in_hours,
            'uptime_last_week(in hours)': result.uptime_last_week_in_hours,
            'downtime_last_hour(in minutes)': result.downtime_last_hour_in_minutes,
            'downtime_last_day(in hours)': result.downtime_last_day_in_hours,
            'downtime_last_week(in hours)': result.downtime_last_week_in_hours
        })
    
    csv_content = output.getvalue()
    output.close()
    
    logger.info(f"Generated CSV with {len(results)} store records")
    return csv_content


def generate_full_report(report_id: UUID) -> None:
    """
    Generate a complete uptime/downtime report for all stores.
    """
    db = SessionLocal()
    try:
        logger.info(f"Starting report generation for report_id: {report_id}")

        mh_df, tz_df = _fetch_reference_data(db)
        tz_lookup = _build_timezone_lookup(tz_df)

        latest_status = db.query(StoreStatus).order_by(StoreStatus.timestamp_utc.desc()).first()
        if not latest_status:
            raise ValueError("No status logs found in database")

        if latest_status.timestamp_utc.tzinfo is None:
            max_timestamp = latest_status.timestamp_utc.replace(tzinfo=ZoneInfo("UTC"))
        else:
            max_timestamp = latest_status.timestamp_utc.astimezone(ZoneInfo("UTC"))

        store_logs_map: Dict[UUID, List[Dict]] = defaultdict(list)

        for batch_logs in _stream_status_logs(db):
            local_logs = _convert_utc_to_local(batch_logs, tz_lookup)
            for log in local_logs:
                store_logs_map[log['store_id']].append(log)

        results = []
        for store_id, store_logs in store_logs_map.items():
            try:
                # Convert reference timestamp to store's local timezone
                store_tz = ZoneInfo(tz_lookup.get(store_id, DEFAULT_TIMEZONE))
                local_max_timestamp = max_timestamp.astimezone(store_tz)
                
                result = _generate_store_report(store_logs, mh_df, store_id, local_max_timestamp)
                results.append(result)
            except Exception as e:
                logger.error(f"Error processing store {store_id}: {e}")
                continue

        # Generate CSV and save to file
        logging.info(" Generating the report please wait for some time")
        csv_content = generate_report_csv(results)
        report_file_path = REPORT_STORAGE_PATH / f"{report_id}.csv"
        
        logging.info("Write your report please wait....")
        with open(report_file_path, 'w', newline='', encoding='utf-8') as f:
            f.write(csv_content)

        # Update database records
        job = db.query(ReportJob).filter(ReportJob.report_id == report_id).first()
        report = db.query(Report).filter(Report.report_id == report_id).first()

        if job:
            job.status = ReportStatus.COMPLETE
            job.file_path = str(report_file_path)
        if report:
            report.status = ReportStatus.COMPLETE

        db.commit()
        logger.info(f"Report {report_id} completed successfully with {len(results)} stores")

    except Exception as e:
        logger.error(f"Report generation failed for {report_id}: {e}")
        
        # Update status to failed
        job = db.query(ReportJob).filter(ReportJob.report_id == report_id).first()
        report = db.query(Report).filter(Report.report_id == report_id).first()
        
        if job: 
            job.status = ReportStatus.FAILED
        if report: 
            report.status = ReportStatus.FAILED
            
        db.commit()
        raise
    finally:
        db.close()


def create_report_job(session: Session, report_id: UUID) -> ReportJob:
    """Create a new report job in the database"""
    job = ReportJob(
        report_id=report_id,
        status=ReportStatus.RUNNING
    )
    session.add(job)
    session.commit()
    session.refresh(job)
    return job


def update_report_job_status(session: Session, report_id: UUID, status: ReportStatus, file_path: Optional[str] = None) -> None:
    """Update report job status in database"""
    job = session.query(ReportJob).filter(ReportJob.report_id == report_id).first()
    if job:
        job.status = status.value
        if file_path:
            job.file_path = file_path
        job.updated_at = func.now()
        session.commit()


def get_report_job(session: Session, report_id: UUID) -> Optional[ReportJob]:
    """Get report job from database"""
    return session.query(ReportJob).filter(ReportJob.report_id == report_id).first()