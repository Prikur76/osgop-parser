from environs import Env


env = Env()
env.read_env()

class Config:
    PDF_MAX_SIZE_MB = 25
    DEBUG = True
    
    # API 1С настройки
    ELEMENT_BASE_URL = env.str("ELEMENT_BASE_URL")
    ELEMENT_USERNAME = env.str("ELEMENT_USERNAME")
    ELEMENT_PASSWORD = env.str("ELEMENT_PASSWORD")
    ELEMENT_ENABLED = env.bool("ELEMENT_ENABLED")
    ELEMENT_TIMEOUT = env.float("ELEMENT_TIMEOUT")
    ELEMENT_VERIFY_SSL = env.bool("ELEMENT_VERIFY_SSL")


config = Config()
