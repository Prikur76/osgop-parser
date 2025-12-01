from environs import Env


env = Env()
env.read_env()

class Config:
    PDF_MAX_SIZE_MB = 25
    DEBUG = True
    
    # API 1С настройки
    API_1C_BASE_URL = env.str("API_1C_BASE_URL")
    API_1C_USERNAME = env.str("API_1C_USERNAME")
    API_1C_PASSWORD = env.str("API_1C_PASSWORD")
    API_1C_ENABLED = env.bool("API_1C_ENABLED")
    API_1C_TIMEOUT = env.float("API_1C_TIMEOUT")
    API_1C_VERIFY_SSL = env.bool("API_1C_VERIFY_SSL")


config = Config()
