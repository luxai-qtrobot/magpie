from datetime import datetime
import json 

class Logger:
    @classmethod
    def _get_formated_time(cls):
        try:
            return datetime.now().strftime('%Y.%m.%d %H:%M:%S.%f')
        except Exception as e: 
            return ""    

    @classmethod
    def debug(cls, message: str): 
        print(f"\033[90m[DEBUG] [{cls._get_formated_time()}]:\033[0m {message}")

    @classmethod
    def warning(cls, message: str): 
        print(f"\033[33m[WARN] [{cls._get_formated_time()}]:\033[0m {message}")

    @classmethod
    def error(cls, message: str):
        print(f"\033[31m[ERROR] [{cls._get_formated_time()}]:\033[0m {message}")

    @classmethod
    def info(cls, message: str):
        print(f"\033[32m[INFO] [{cls._get_formated_time()}]:\033[0m {message}")


    @classmethod
    def pretty_print(cls, message: object):
        print(f"\033[32m[INFO] [{cls._get_formated_time()}]:\033[0m\n{json.dumps(message, indent=2)}")
