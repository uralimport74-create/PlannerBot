# production_bot.py

import os
import sys
import io
import math
import re
from datetime import datetime
import pandas as pd
import pytz
import gspread
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from oauth2client.service_account import ServiceAccountCredentials
import telebot

import config # Импортируем файл с настройками

# --- ГЛОБАЛЬНЫЕ КОНСТАНТЫ ---
TIMEZONE = 'Asia/Yekaterinburg'
DAY_MAP = ['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс']

# Стандартизированные названия колонок для внутренней логики
COL_NAME_NOMENCLATURE = "Номенклатура"
COL_NAME_STOCK = "Остаток учитывая резерв кор."
COL_NAME_SALES = "Продажи за 2 недели"
COL_NAME_PACK = "Вложение"

# --- 4. ПОДКЛЮЧЕНИЕ К GOOGLE API ---

def get_creds():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    try:
        return ServiceAccountCredentials.from_json_keyfile_name(config.CREDENTIALS_FILE, scope)
    except Exception as e:
        raise ConnectionError(f"Ошибка чтения учетных данных '{config.CREDENTIALS_FILE}': {e}")

def connect_services():
    creds = get_creds()
    gc = gspread.authorize(creds)
    drive_service = build('drive', 'v3', credentials=creds)
    return gc, drive_service

# --- 5. ЛОГИРОВАНИЕ ОШИБОК ---

def log_error_to_sheet(gc, error_message, file_name="System"):
    try:
        tz = pytz.timezone(TIMEZONE)
        now_dt = datetime.now(tz)
        try:
            sh = gc.open(config.REPORTS_SHEET_NAME)
        except: return

        try:
            worksheet = sh.worksheet(config.ERROR_LOG_WORKSHEET)
        except:
            worksheet = sh.add_worksheet(title=config.ERROR_LOG_WORKSHEET, rows=100, cols=3)
            worksheet.append_row(["Дата (ЕКБ)", "Имя файла", "Текст ошибки"])

        date_str = now_dt.strftime('%Y-%m-%d %H:%M:%S')
        worksheet.append_row([date_str, file_name, error_message])
        print(f"Ошибка записана в лог.")
    except Exception as e:
        print(f"Критическая ошибка лога: {e}")

def log_unknown_skus_batch(gc, unknown_skus_list, file_name):
    if not unknown_skus_list: return
    try:
        tz = pytz.timezone(TIMEZONE)
        now_dt = datetime.now(tz)
        date_str = now_dt.strftime('%Y-%m-%d %H:%M:%S')
        
        try: sh = gc.open(config.REPORTS_SHEET_NAME)
        except: return

        try: worksheet = sh.worksheet(config.ERROR_LOG_WORKSHEET)
        except:
            worksheet = sh.add_worksheet(title=config.ERROR_LOG_WORKSHEET, rows=100, cols=3)
            worksheet.append_row(["Дата (ЕКБ)", "Имя файла", "Текст ошибки"])
        
        rows = [[date_str, file_name, f"UNKNOWN_SKU: {sku}"] for sku in unknown_skus_list]
        worksheet.append_rows(rows)
        print(f"Записано {len(rows)} неизвестных SKU.")
    except Exception as e:
        print(f"Ошибка записи SKU: {e}")

# --- 6. СПРАВОЧНИК BRANDS ---

def normalize_text(text):
    if pd.isna(text): return ""
    return re.sub(r'[^a-zA-Zа-яА-ЯёЁ0-9]', '', str(text).strip()).lower()

def load_brands_reference(client):
    print(f"Загружаю справочник из '{config.BRANDS_SHEET_NAME}'...")
    try: sh = client.open(config.BRANDS_SHEET_NAME)
    except Exception as e: raise RuntimeError(f"Ошибка открытия таблицы: {e}")

    ws = None
    # Логика поиска листа (как была)
    wn = getattr(config, "BRANDS_WORKSHEET_NAME", None)
    if wn:
        try: ws = sh.worksheet(wn)
        except: pass
    
    if ws is None:
        for w in sh.worksheets():
            if any(str(h).strip() == "brand_1c" for h in w.row_values(1)):
                ws = w; break
    
    if ws is None:
        for w in sh.worksheets():
            if "brand" in w.title.lower(): ws = w; break
            
    if ws is None: raise RuntimeError("Не найден лист Brands.")

    df = pd.DataFrame(ws.get_all_records())
    df['brand_1c'] = df['brand_1c'].astype(str).fillna('')
    df['search_key'] = df['brand_1c'].apply(normalize_text)

    if 'Min_Batch' not in df.columns: df['Min_Batch'] = 1
    df['Coeff'] = pd.to_numeric(df['Coeff'], errors='coerce').fillna(0.5)
    df['Min_Stock'] = pd.to_numeric(df['Min_Stock'], errors='coerce').fillna(0).astype(int)
    df['Min_Batch'] = pd.to_numeric(df['Min_Batch'], errors='coerce').fillna(1).astype(int)
    df['Shipping_Days'] = df['Shipping_Days'].astype(str).str.strip().replace({'nan': '', '': 'Пн, Вт, Ср, Чт, Пт'})

    return df

# --- 7. СКАЧИВАНИЕ И ПАРСИНГ (ОБНОВЛЕНО) ---

def find_latest_file(drive_service, folder_id):
    """Ищет файл и возвращает ID + MimeType."""
    print(f"Ищу файл в папке: {folder_id}")
    query = f"'{folder_id}' in parents and trashed=false"
    # Запрашиваем mimeType, чтобы понять, это Google Таблица или XLSX
    response = drive_service.files().list(
        q=query, orderBy='createdTime desc', pageSize=1, fields='files(id, name, mimeType)'
    ).execute()
    files = response.get('files', [])
    if not files: return None
    print(f"Найден файл: {files[0]['name']} (Тип: {files[0]['mimeType']})")
    return files[0]

def clean_number(value):
    if value is None or pd.isna(value) or value == '': return 0.0
    try:
        return float(value)
    except:
        s = str(value).replace('\xa0', '').replace(' ', '').replace(',', '.')
        s = re.sub(r'[^\d\.-]', '', s) # Оставляем только цифры, точки и минусы
        try: return float(s)
        except: return 0.0

def download_file_content(drive_service, file_info):
    """Умное скачивание: если Google Sheet -> конвертируем, иначе -> качаем как есть."""
    file_id = file_info['id']
    mime_type = file_info.get('mimeType', '')
    file_content = io.BytesIO()

    if mime_type == 'application/vnd.google-apps.spreadsheet':
        print("Это Google Таблица. Конвертирую в Excel...")
        request = drive_service.files().export_media(
            fileId=file_id, 
            mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    else:
        print("Скачиваю как бинарный файл (XLSX/CSV)...")
        request = drive_service.files().get_media(fileId=file_id)

    downloader = MediaIoBaseDownload(file_content, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
    
    file_content.seek(0)
    return file_content

def download_and_parse_report(drive_service, file_info, file_name):
    # 1. Скачиваем контент
    file_content = download_file_content(drive_service, file_info)
    
    df_raw = None
    success = False
    
    # 2. Пробуем Excel (XLSX/XLS)
    try:
        print("Попытка открытия как Excel...")
        df_raw = pd.read_excel(file_content, header=None, engine='openpyxl')
        # Быстрая проверка: если файл открылся, есть ли в нем кириллица?
        # Превращаем первые 20 строк в текст
        check_str = df_raw.head(20).astype(str).to_string().lower()
        if "номенклатура" in check_str or "остаток" in check_str:
            success = True
            print("Успешно открыт как Excel.")
    except Exception as e:
        print(f"Не Excel: {e}")
        file_content.seek(0)
    
    # 3. Если Excel не сработал, пробуем CSV с перебором кодировок
    if not success:
        # 1C обычно использует cp1251, ставим его первым
        encodings_to_try = ['cp1251', 'utf-8', 'utf-8-sig', 'latin1']
        
        for enc in encodings_to_try:
            print(f"Пробую открыть как CSV (кодировка {enc})...")
            try:
                file_content.seek(0)
                temp_df = pd.read_csv(
                    file_content, 
                    header=None, 
                    encoding=enc, 
                    sep=None,     # Авто-определение разделителя (; или ,)
                    engine='python', 
                    on_bad_lines='skip'
                )
                
                # ПРОВЕРКА: Если мы угадали кодировку, мы должны найти ключевые слова
                check_str = temp_df.head(20).astype(str).to_string().lower()
                
                # Ищем хотя бы одно ключевое слово, чтобы подтвердить, что это не кракозябры
                if "номенклатура" in check_str or "остаток" in check_str or "продажи" in check_str:
                    df_raw = temp_df
                    print(f"--> Успех! Кодировка распознана: {enc}")
                    success = True
                    break
                else:
                    print(f"--> Файл прочитан, но ключевые слова не найдены (возможно, неверная кодировка).")
            except Exception as e:
                print(f"--> Ошибка с кодировкой {enc}: {e}")
                continue

    if df_raw is None or df_raw.empty:
        raise ValueError("Не удалось прочитать файл. Возможно, неизвестный формат или кодировка.")

    # 4. Поиск заголовка
    header_row_idx = -1
    max_matches = 0
    keywords = ["номенклатура", "остаток", "резерв", "продажи", "наименование", "склад"]
    
    # Ищем строку заголовка в первых 50 строках
    for idx in range(min(50, len(df_raw))):
        # Собираем строку, приводим к нижнему регистру
        row_str = " ".join(df_raw.iloc[idx].astype(str).fillna('').str.lower().tolist())
        
        # Считаем, сколько ключевых слов нашлось в этой строке
        matches = sum(1 for kw in keywords if kw in row_str)
        
        if matches >= 2: # Если нашли хотя бы 2 слова (например "Номенклатура" и "Остаток")
            if matches > max_matches:
                max_matches = matches
                header_row_idx = idx

    if header_row_idx == -1:
        # ДЕБАГ: Если не нашли, выведем первые 10 строк в лог, чтобы понять, что видит бот
        print("!!! ЗАГОЛОВОК НЕ НАЙДЕН. ВОТ ЧТО ВИДИТ БОТ В ПЕРВЫХ 10 СТРОКАХ: !!!")
        print(df_raw.head(10).to_string())
        print("---------------------------------------------------------------------")
        raise ValueError("Не найден заголовок таблицы (строка с 'Номенклатура', 'Остаток'...).")

    print(f"Заголовок найден на строке {header_row_idx + 1}")

    # Применяем заголовок
    df_raw.columns = df_raw.iloc[header_row_idx]
    df_raw = df_raw.iloc[header_row_idx + 1:].reset_index(drop=True)
    df_raw.columns = df_raw.columns.astype(str).str.strip().str.lower()
    
    col_names = df_raw.columns.tolist()
    
    def find_col(kws, required=True):
        for kw in kws:
            for c in col_names:
                if kw in c: return c
        if required: raise ValueError(f"Не найдена колонка {kws}. Доступные колонки: {col_names}")
        return None
        
    real_nom = find_col(["номенклатура", "наименование"])
    real_stock = find_col(["учитывая резерв", "остаток", "склад"])
    real_sales = find_col(["продажи", "реализация"])
    real_pack = find_col(["вложение", "упаковка"], required=False)
    
    rename_map = {real_nom: COL_NAME_NOMENCLATURE, real_stock: COL_NAME_STOCK, real_sales: COL_NAME_SALES}
    if real_pack: rename_map[real_pack] = COL_NAME_PACK

    df_report = df_raw.rename(columns=rename_map)
    df_report = df_report.loc[:, ~df_report.columns.duplicated()]
    
    if COL_NAME_PACK not in df_report.columns:
        df_report[COL_NAME_PACK] = 1
        
    df_report['pack_size'] = df_report[COL_NAME_PACK].apply(clean_number).replace(0, 1)
    df_report['stock_raw'] = df_report[COL_NAME_STOCK].apply(clean_number)
    df_report['sales_raw'] = df_report[COL_NAME_SALES].apply(clean_number)
    
    # --- ОЧИСТКА ---
    df_report = df_report[~((df_report['stock_raw'] == 0) & (df_report['sales_raw'] == 0))]
    df_report[COL_NAME_NOMENCLATURE] = df_report[COL_NAME_NOMENCLATURE].astype(str).str.replace(r'\s+', ' ', regex=True).str.strip()

    JUNK_PHRASES = [
        "продукты для восточной кухни", "соусы порционные", "брендированный соус",
        "брендированный имбирь", "для офиса", "внепрайсовый ассортимент", "итого", "за наличку"
    ]
    pattern = '|'.join(JUNK_PHRASES)
    df_report = df_report[~df_report[COL_NAME_NOMENCLATURE].str.lower().str.contains(pattern, na=False)]
    df_report = df_report[~df_report[COL_NAME_NOMENCLATURE].str.match(r'^\s*\d+(\.\d+)*\.?\s', na=False)]
    df_report = df_report[df_report[COL_NAME_NOMENCLATURE].str.len() > 3]

    df_report['stock_box'] = df_report['stock_raw'] / df_report['pack_size']
    df_report['sales_box'] = df_report['sales_raw'] / df_report['pack_size']
    
    return df_report[[COL_NAME_NOMENCLATURE, COL_NAME_STOCK, COL_NAME_SALES, 'stock_box', 'sales_box']]
    
    # --- ОЧИСТКА МУСОРА (Обновлено под ваш файл) ---
    
    # 1. Удаляем нулевые строки
    df_report = df_report[~((df_report['stock_raw'] == 0) & (df_report['sales_raw'] == 0))]

    # 2. Нормализуем текст (удаляем лишние пробелы)
    df_report[COL_NAME_NOMENCLATURE] = df_report[COL_NAME_NOMENCLATURE].astype(str).str.replace(r'\s+', ' ', regex=True).str.strip()

    # 3. Фильтр фраз (добавлены фразы из вашего CSV)
    JUNK_PHRASES = [
        "продукты для восточной кухни",
        "соусы порционные",
        "брендированный соус",
        "брендированный имбирь",
        "для офиса",
        "внепрайсовый ассортимент",
        "итого",
        "за наличку"
    ]
    pattern = '|'.join(JUNK_PHRASES)
    df_report = df_report[~df_report[COL_NAME_NOMENCLATURE].str.lower().str.contains(pattern, na=False)]

    # 4. Удаляем нумерованные группы (например "1. Продукты", "23. Брендированный")
    # Регулярка ловит строки начинающиеся с цифр и точки (даже если перед ними пробел)
    df_report = df_report[~df_report[COL_NAME_NOMENCLATURE].str.match(r'^\s*\d+(\.\d+)*\.?\s', na=False)]

    # 5. Удаляем слишком короткие названия (меньше 3 букв)
    df_report = df_report[df_report[COL_NAME_NOMENCLATURE].str.len() > 3]

    # Расчет коробок (если в файле уже коробки, pack_size=1, если штуки - поделим)
    df_report['stock_box'] = df_report['stock_raw'] / df_report['pack_size']
    df_report['sales_box'] = df_report['sales_raw'] / df_report['pack_size']
    
    return df_report[[COL_NAME_NOMENCLATURE, COL_NAME_STOCK, COL_NAME_SALES, 'stock_box', 'sales_box']]

# --- 8. РАСЧЕТ ---

def get_planning_days(wd_idx):
    if wd_idx == 4: return ['Сб', 'Вс', 'Пн'], ['Вт', 'Ср']
    elif wd_idx in [5, 6]: return [], []
    else:
        d1 = (wd_idx + 1) % 7
        d2 = (wd_idx + 2) % 7
        d3 = (wd_idx + 3) % 7
        return [DAY_MAP[d1]], [DAY_MAP[d2], DAY_MAP[d3]]

def calc_row(row):
    sales = row['sales_box']
    coeff = row.get('Coeff')
    if pd.isna(coeff) or coeff == 0 or sales <= 0: return 0
        
    current = row['stock_box']
    min_stock = row['Min_Stock']
    min_batch = row['Min_Batch']
    
    target = max(sales * coeff, min_stock)
    need = target - current
    
    if need <= 0: return 0
    if current > min_stock and need < min_batch: return 0
        
    if min_batch > 1: return int(math.ceil(need / min_batch) * min_batch)
    else: return int(math.ceil(need))

def check_shipping(ship_days, targets):
    if not ship_days: return False
    avail = {d.strip() for d in ship_days.split(',')}
    for t in targets:
        if t in avail: return True
    return False

def calculate_logic(report_df, brands_df):
    tz = pytz.timezone(TIMEZONE)
    wd = datetime.now(tz).weekday()
    days_A, days_B = get_planning_days(wd)
    
    if not days_A:
        print("Выходной день.")
        return pd.DataFrame(), pd.DataFrame(), [], [], []

    report_df['search_key'] = report_df[COL_NAME_NOMENCLATURE].apply(normalize_text)
    merged = pd.merge(report_df, brands_df, on='search_key', how='left')
    
    unknown_skus = merged[merged['Coeff'].isna()][COL_NAME_NOMENCLATURE].unique().tolist()
    merged['need_qty'] = merged.apply(calc_row, axis=1)

    base = merged[(merged['need_qty'] > 0)]
    crit = base['stock_box'] < base['Min_Stock']
    ship_A = base.apply(lambda r: check_shipping(r['Shipping_Days'], days_A), axis=1)
    
    plan_A = base[crit | ship_A].copy()
    plan_B = base[~(crit | ship_A) & base.apply(lambda r: check_shipping(r['Shipping_Days'], days_B), axis=1)].copy()
    
    return plan_A, plan_B, days_A, days_B, unknown_skus

# --- 9. TELEGRAM & SAVE ---

def format_plan_msg(df, days, is_forecast=False):
    if df.empty: return ""
    icon = "🔮" if is_forecast else "🗓️"
    title = f"{icon} **ПЛАН НА {', '.join(days)} ({'Прогноз' if is_forecast else 'ОСНОВНОЙ'})**\n\n"
    
    sort_cols = [c for c in ['Тип', 'Категория', 'Бренд'] if c in df.columns]
    df = df.sort_values(by=sort_cols)

    body = ""
    total = 0
    for _, row in df.iterrows():
        alert = "🔥" if row['stock_box'] < row['Min_Stock'] else ""
        cat = row.get('Категория', '')
        ptype = row.get('Тип', '—')
        info = f"{cat}, {ptype}" if cat else ptype
        
        body += f"{alert}*_{row.get('Бренд', row[COL_NAME_NOMENCLATURE])}_* ({info}) — **{int(row['need_qty'])}** кор.\n"
        total += row['need_qty']
        
    return title + body + f"\nВСЕГО: **{int(total)}** кор.\n"

def send_telegram(bot, chat_id, text):
    if not text: return
    MAX = 4000
    parts, curr = [], ""
    for line in text.split('\n'):
        if len(curr) + len(line) + 1 <= MAX: curr += line + '\n'
        else:
            parts.append(curr); curr = line + '\n'
    if curr: parts.append(curr)
    
    for p in parts:
        try: bot.send_message(chat_id, p, parse_mode='Markdown')
        except: pass

def save_plan_to_sheet(gc, df, day_str):
    if df.empty: return
    sname = config.REPORT_WORKSHEET_PREFIX + day_str
    
    out = df.copy().rename(columns={'brand_1c':'1С Имя', 'stock_box':'Остаток', 'need_qty':'ПЛАН'})
    cols = ['Тип', 'Категория', 'Бренд', '1С Имя', 'Остаток', 'ПЛАН']
    out = out[[c for c in cols if c in out.columns]]
    
    try: sh = gc.open(config.REPORTS_SHEET_NAME)
    except: return

    try: sh.del_worksheet(sh.worksheet(sname))
    except: pass
    
    ws = sh.add_worksheet(title=sname, rows=len(out)+5, cols=len(out.columns))
    ws.update([out.columns.tolist()] + out.values.tolist())

def archive_file(drive, file_id):
    try:
        drive.files().update(fileId=file_id, addParents=config.ARCHIVE_FOLDER_ID,
                             removeParents=config.INPUT_FOLDER_ID).execute()
        print("Файл архивирован.")
    except Exception as e:
        print(f"Ошибка архивации: {e}")

# --- MAIN ---

def main():
    f_log = "System"
    gc = None
    bot = telebot.TeleBot(config.TELEGRAM_TOKEN)
    
    try:
        print("--- Запуск ---")
        gc, drive = connect_services()
        
        brands_df = load_brands_reference(gc)
        file_info = find_latest_file(drive, config.INPUT_FOLDER_ID)
        
        if not file_info:
            print("Нет файлов.")
            return
            
        f_log = file_info['name']
        # Теперь передаем весь объект file_info, а не только ID
        report_df = download_and_parse_report(drive, file_info, f_log)
        
        plan_A, plan_B, days_A, days_B, unknown_skus = calculate_logic(report_df, brands_df)
        
        if not days_A: return
        
        if unknown_skus:
            log_unknown_skus_batch(gc, unknown_skus, f_log)
            send_telegram(bot, config.TELEGRAM_CHAT_ID, f"⚠️ Неизвестные SKU ({len(unknown_skus)} шт). См. лог.")

        msg_A = format_plan_msg(plan_A, days_A)
        msg_B = format_plan_msg(plan_B, days_B, True)
        
        txt_A = msg_A if msg_A else f"*План А на {', '.join(days_A)} пуст.*"
        head = f"✅ **ОТЧЕТ {datetime.now(pytz.timezone(TIMEZONE)).strftime('%d.%m.%Y')}**\n\n"
        
        send_telegram(bot, config.TELEGRAM_CHAT_ID, head + txt_A)
        if msg_B: send_telegram(bot, config.TELEGRAM_CHAT_ID, msg_B)
        
        save_plan_to_sheet(gc, plan_A, datetime.now(pytz.timezone(TIMEZONE)).strftime('%d.%m'))
        archive_file(drive, file_info['id'])
        print("--- Готово ---")

    except Exception as e:
        err = f"❌ Ошибка '{f_log}':\n{e}"
        print(err)
        try: bot.send_message(config.TELEGRAM_CHAT_ID, err)
        except: pass
        if gc: log_error_to_sheet(gc, err, f_log)
        return

if __name__ == "__main__":
    main()