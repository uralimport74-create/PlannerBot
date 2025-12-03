import os
import time
import schedule
import threading
from datetime import datetime
import pytz

# Импортируем твоих ботов
# Важно: убедись, что в production_bot и machine_planner убраны sys.exit()
import production_bot
import machine_planner

def create_creds_file():
    """Создает файл service_account.json из переменной окружения Railway"""
    json_creds = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if json_creds:
        with open("service_account.json", "w", encoding='utf-8') as f:
            f.write(json_creds)
        print("✅ Файл ключей service_account.json создан.")
    else:
        print("⚠️ Переменная GOOGLE_CREDENTIALS_JSON не найдена (если запускаешь локально и файл есть — всё ок).")

def job_production():
    print(f"⏰ Запуск Production Planner: {datetime.now()}")
    try:
        production_bot.main()
    except Exception as e:
        print(f"❌ Ошибка в Production Planner: {e}")

def job_machine():
    print(f"⏰ Запуск Machine Planner: {datetime.now()}")
    try:
        machine_planner.main()
    except Exception as e:
        print(f"❌ Ошибка в Machine Planner: {e}")

if __name__ == "__main__":
    print("--- ЗАПУСК ПЛАНИРОВЩИКА (UTC TIME) ---")
    
    # 1. Восстанавливаем файл с ключами
    create_creds_file()

    # 2. Настраиваем расписание (ТЕСТОВОЕ ВРЕМЯ)
    # Исправлены отступы: теперь тут ровно 4 пробела
    schedule.every().day.at("23:40").do(job_production)
    schedule.every().day.at("23:43").do(job_machine)

    print("📅 Расписание установлено. Жду времени Ч...")
    print(f"Текущее время сервера (UTC): {datetime.now(pytz.utc)}")

    # 3. Бесконечный цикл проверки
    while True:
        schedule.run_pending()
        time.sleep(30) # Проверка каждые 30 секунд
