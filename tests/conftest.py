import os
import sys


# Абсолютный путь к корневой директории проекта, чтобы `import app...`
# работал при запуске pytest из любого места.
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
