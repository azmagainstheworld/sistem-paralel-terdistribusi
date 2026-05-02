import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]

def load_env():
    # optional: support .env files if python-dotenv is available
    try:
        from dotenv import load_dotenv
        load_dotenv(dotenv_path=BASE_DIR / '.env')
    except Exception:
        pass

    return {
        'REDIS_URL': os.getenv('REDIS_URL', 'redis://localhost:6379/0'),
        'NODE_HOST': os.getenv('NODE_HOST', '127.0.0.1'),
    }
