from datetime import datetime
import json

class Logger:
    # Possible levels: "DEBUG", "INFO", "WARN", "ERROR"
    level_order = {"DEBUG": 10, "INFO": 20, "WARN": 30, "ERROR": 40}

    log_level = "INFO"   # Default

    @classmethod
    def set_level(cls, level: str):
        if level.upper() in cls.level_order:
            cls.log_level = level.upper()

    @classmethod
    def _get_formated_time(cls):
        try:
            return datetime.now().strftime('%Y.%m.%d %H:%M:%S.%f')
        except Exception:
            return ""

    @classmethod
    def _should_log(cls, level: str):
        return cls.level_order[level] >= cls.level_order[cls.log_level]

    @classmethod
    def debug(cls, message: str):
        if not cls._should_log("DEBUG"): return
        print(f"\033[90m[DEBUG] [{cls._get_formated_time()}]:\033[0m {message}")

    @classmethod
    def info(cls, message: str):
        if not cls._should_log("INFO"): return
        print(f"\033[32m[INFO] [{cls._get_formated_time()}]:\033[0m {message}")

    @classmethod
    def warning(cls, message: str):
        if not cls._should_log("WARN"): return
        print(f"\033[33m[WARN] [{cls._get_formated_time()}]:\033[0m {message}")

    @classmethod
    def error(cls, message: str):
        if not cls._should_log("ERROR"): return
        print(f"\033[31m[ERROR] [{cls._get_formated_time()}]:\033[0m {message}")

    @classmethod
    def pretty_print(cls, message: object):
        if not cls._should_log("INFO"): return
        print(f"\033[32m[INFO] [{cls._get_formated_time()}]:\033[0m\n{json.dumps(message, indent=2)}")
