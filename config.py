# config.py

# Имя файла ключа (должен лежать рядом)
CREDENTIALS_FILE = 'service_account.json'

# --- НАСТРОЙКИ СПРАВОЧНИКА (Где лежат настройки товаров) ---
BRANDS_SHEET_NAME = 'brands'
BRANDS_WORKSHEET_NAME = 'Sheet1'

# --- НАСТРОЙКИ ОТЧЕТОВ (Где храним историю и ошибки) ---
REPORTS_SHEET_NAME = 'Production_Reports' # <--- НОВАЯ ТАБЛИЦА
ERROR_LOG_WORKSHEET = 'Errors_Log'        # Имя вкладки для ошибок
REPORT_WORKSHEET_PREFIX = 'План_'         # Префикс для листов с планом

# --- ID ПАПОК НА GOOGLE DRIVE ---
INPUT_FOLDER_ID = '1iGgpH7L-UWuz1gr1S4h45osstOEdQ9FF'
ARCHIVE_FOLDER_ID = '1Kym3v-U3MZEaFrM7EYcwZzIMwERdRtS8'

# --- TELEGRAM ---
TELEGRAM_TOKEN = '8309662002:AAEGeKD-w-_KS8ALnSa2sKKzE_OlS3GxYWk'
TELEGRAM_CHAT_ID = '307162724'

