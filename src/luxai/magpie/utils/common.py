import datetime 
from datetime import timezone 
from ulid import ULID

def get_utc_timestamp() -> float:
    """
    Generates a UTC timestamp.
    Returns:
        float: The UTC timestamp as a float representing seconds since the epoch.
    """
    dt = datetime.datetime.now(timezone.utc)     
    utc_time = dt.replace(tzinfo=timezone.utc) 
    return utc_time.timestamp()

def get_uinque_id() -> str:
    """
    Generates a unique identifier using ULID.
    Returns:
        str: A unique identifier as a string.
    """
    return str(ULID())

