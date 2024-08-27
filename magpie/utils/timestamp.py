import datetime 
from datetime import timezone 

def get_utc_timestamp() -> float:
    """
    Generates a UTC timestamp.
    Returns:
        float: The UTC timestamp as a float representing seconds since the epoch.
    """
    dt = datetime.datetime.now(timezone.utc)     
    utc_time = dt.replace(tzinfo=timezone.utc) 
    return utc_time.timestamp()


