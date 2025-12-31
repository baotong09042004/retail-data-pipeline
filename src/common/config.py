import os
import yaml
from pathlib import Path
from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(BASE_DIR / ".env")

class Settings:
    def __init__(self):
        self._load_yaml()
        self._validate_critical_secrets()

    def _load_yaml(self):
        yaml_path = BASE_DIR / "config.yaml"
        
        if yaml_path.exists():
            with open(yaml_path, "r", encoding="utf-8") as f:
                self._yaml_data = yaml.safe_load(f)
        else:
            self._yaml_data = {}

    def _get(self, env_key, yaml_key_path, default):
        
        val = os.getenv(env_key)
        if val: return val
   
        try:
            data = self._yaml_data
            for key in yaml_key_path:
                data = data[key]
            return data
        except (KeyError, TypeError):
            pass
            
        return default

    def _validate_critical_secrets(self):
        if not os.getenv("POSTGRES_USER") or not os.getenv("POSTGRES_PASSWORD"):
             raise EnvironmentError("CRITICAL: Thiếu USER hoặc PASSWORD trong .env!")

   
    
    @property
    def DB_HOST(self):
        return self._get("POSTGRES_HOST", ['DATABASE', 'HOST'], "localhost")

    @property
    def DB_PORT(self):
        return self._get("POSTGRES_PORT", ['DATABASE', 'PORT'], 5432)
    
    @property
    def DB_NAME(self):
        return os.getenv("DATABASE") 
    
    @property
    def DB_USER(self):
        return os.getenv("POSTGRES_USER")

    @property
    def DB_PASSWORD(self):
        return os.getenv("POSTGRES_PASSWORD")
    
    @property
    def MINIO_ENDPOINT(self):
        return os.getenv("MINIO_ENDPOINT", "http://localhost:9000")

    @property
    def MINIO_ACCESS_KEY(self):
        # SỬA LẠI THÀNH 'admin'
        return os.getenv("MINIO_ACCESS_KEY", "admin") 

    @property
    def MINIO_SECRET_KEY(self):
        # SỬA LẠI THÀNH 'password'
        return os.getenv("MINIO_SECRET_KEY", "password")    

    @property
    def MINIO_BUCKET(self):
        return os.getenv("MINIO_BUCKET", "datalake")

    @property
    def STORAGE_OPTIONS(self):
        """
        Đây là biến quan trọng nhất để Pandas kết nối được MinIO. 
        """
        return {
            "key": self.MINIO_ACCESS_KEY,
            "secret": self.MINIO_SECRET_KEY,
            "client_kwargs": {
                "endpoint_url": self.MINIO_ENDPOINT
            }
        }

settings = Settings()