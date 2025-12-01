# machine_planner.py
"""
Модуль машинного планирования для цеха порционной продукции.

Что делает:
1. Берёт План A на сегодня из таблицы Production_Reports (лист "План_DD.MM").
2. Подтягивает из справочника Brands Тип/Категорию/Рецептуру/Shipping_Days.
3. Берёт настройки машин из таблицы Machine_Settings (листы "Machines" и "Routing").
4. Объединяет позиции по (Тип, Категория, Рецептура) в партии.
5. Раскладывает партии по машинам с учётом суточной производительности.
6. Переносит "хвосты", которые не влезли, на +1 и +2 рабочий день.
7. Балансирует загрузку между soy_pp_1 и soy_pp_2 (соевый соус ПП),
   НО ТОЛЬКО ЕСЛИ У НИХ ОДИНАКОВЫЙ ПРИОРИТЕТ.
8. Записывает результат обратно в Production_Reports:
   - добавляет колонки M0_cases / M1_cases / M2_cases в лист плана,
   - создаёт лист "Машинный_план_DD.MM" с детальной раскладкой.
9. Отправляет отдельный отчёт в Telegram.

Скрипт рассчитан на запуск вручную:
    python machine_planner.py
"""

import sys
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

import pandas as pd
import pytz
import gspread
import telebot  # pyTelegramBotAPI
from oauth2client.service_account import ServiceAccountCredentials

import config
from production_bot import (
    get_creds,
    log_error_to_sheet,
    load_brands_reference,
    send_telegram,
    TIMEZONE,
    DAY_MAP,
)

# --- Локальные константы для машинного планировщика ---

# Имя Google-таблицы с настройками машин
MACHINE_SETTINGS_SHEET_NAME = "Machine_Settings"
MACHINES_SHEET_NAME = "Machines"
ROUTING_SHEET_NAME = "Routing"

# Префикс листа с машинным планом в Production_Reports
MACHINE_PLAN_SHEET_PREFIX = "Машинный_план_"

# Максимальный сдвиг на будущее по дням (0 = сегодня, 1 = +1 раб. день, 2 = +2 раб. день)
MAX_SHIFT_DAYS = 2

# Горизонт рабочих дней вперёд (для отображения/отчёта)
HORIZON_WORKING_DAYS = 5


# --- Вспомогательные структуры данных ---

class Machine:
    def __init__(
        self,
        machine_id: str,
        name: str,
        categories: List[str],
        types: List[str],
        daily_capacity: int,
        priority: int,
        active: bool = True,
    ):
        self.id = machine_id
        self.name = name
        self.categories = set([c.strip() for c in categories if c.strip()])
        self.types = set([t.strip() for t in types if t.strip()])
        self.daily_capacity = int(daily_capacity)
        self.priority = int(priority)
        self.active = bool(active)

    def can_produce(self, product_type: str, category: str) -> bool:
        """Проверка, может ли машина производить данный тип/категорию."""
        pt = (product_type or "").strip()
        cat = (category or "").strip()
        if self.categories and cat and cat not in self.categories:
            return False
        if self.types and pt and pt not in self.types:
            return False
        return True


def connect_gspread():
    """Подключение только к Google Sheets (gspread), на базе get_creds из production_bot."""
    creds = get_creds()
    gc = gspread.authorize(creds)
    return gc


def parse_bool(value) -> bool:
    """Приводит значение из таблицы к bool."""
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    s = str(value).strip().upper()
    return s in ("TRUE", "1", "YES", "ДА", "Y", "T")


def parse_list(value) -> List[str]:
    """Парсит строку вида 'Соевый соус, Маринованный имбирь' в список."""
    if value is None:
        return []
    if isinstance(value, list):
        return [str(x).strip() for x in value if str(x).strip()]
    s = str(value)
    parts = [p.strip() for p in s.split(",")]
    return [p for p in parts if p]


def build_working_days(start_date) -> List[datetime]:
    """Строит список из HORIZON_WORKING_DAYS рабочих дней, начиная с start_date."""
    days = []
    cur = start_date
    while len(days) < HORIZON_WORKING_DAYS:
        # Пн=0 ... Вс=6; считаем рабочими Пн-Пт
        if cur.weekday() < 5:
            days.append(cur)
        cur += timedelta(days=1)
    return days


def load_machine_settings(gc) -> Tuple[Dict[str, Machine], pd.DataFrame]:
    """
    Загружает настройки машин и маршрутизации.

    Возвращает:
        machines: dict[Machine_ID] -> Machine
        routing_df: DataFrame с правилами маршрутизации (Routing)
    """
    try:
        sh = gc.open(MACHINE_SETTINGS_SHEET_NAME)
    except gspread.SpreadsheetNotFound:
        raise FileNotFoundError(
            f"Google Sheet '{MACHINE_SETTINGS_SHEET_NAME}' не найдена. "
            f"Создайте её с листами '{MACHINES_SHEET_NAME}' и '{ROUTING_SHEET_NAME}'."
        )

    # Лист Machines
    try:
        ws_m = sh.worksheet(MACHINES_SHEET_NAME)
    except gspread.WorksheetNotFound:
        raise FileNotFoundError(
            f"Лист '{MACHINES_SHEET_NAME}' в '{MACHINE_SETTINGS_SHEET_NAME}' не найден."
        )
    machines_records = ws_m.get_all_records()
    machines_df = pd.DataFrame(machines_records)

    required_m_cols = [
        "Machine_ID",
        "Name",
        "Category_Allowed",
        "Type_Allowed",
        "Daily_Capacity_cases",
        "Priority",
        "Active",
    ]
    missing = [c for c in required_m_cols if c not in machines_df.columns]
    if missing:
        raise ValueError(
            f"В листе '{MACHINES_SHEET_NAME}' нет колонок: {', '.join(missing)}"
        )

    machines: Dict[str, Machine] = {}
    for _, row in machines_df.iterrows():
        machine_id = str(row["Machine_ID"]).strip()
        if not machine_id:
            continue
        name = str(row["Name"]).strip() or machine_id
        categories = parse_list(row.get("Category_Allowed"))
        types = parse_list(row.get("Type_Allowed"))
        try:
            daily_capacity = int(row.get("Daily_Capacity_cases", 0) or 0)
        except Exception:
            daily_capacity = 0
        try:
            priority = int(row.get("Priority", 100) or 100)
        except Exception:
            priority = 100
        active = parse_bool(row.get("Active", True))

        if not daily_capacity:
            # Машина с нулевой мощностью нам не интересна
            continue

        machines[machine_id] = Machine(
            machine_id=machine_id,
            name=name,
            categories=categories,
            types=types,
            daily_capacity=daily_capacity,
            priority=priority,
            active=active,
        )

    # Лист Routing
    try:
        ws_r = sh.worksheet(ROUTING_SHEET_NAME)
    except gspread.WorksheetNotFound:
        raise FileNotFoundError(
            f"Лист '{ROUTING_SHEET_NAME}' в '{MACHINE_SETTINGS_SHEET_NAME}' не найден."
        )

    routing_records = ws_r.get_all_records()
    routing_df = pd.DataFrame(routing_records)

    if routing_df.empty:
        # Разрешаем пустой Routing: тогда будут использоваться только общие настройки машин
        routing_df = pd.DataFrame(
            columns=["Тип", "Категория", "Рецептура", "Preferred_Machine_ID", "Priority", "Active"]
        )

    # Приводим Priority/Active
    if "Priority" in routing_df.columns:
        routing_df["Priority"] = routing_df["Priority"].apply(
            lambda x: int(x) if str(x).strip() != "" else 100
        )
    else:
        routing_df["Priority"] = 100

    if "Active" in routing_df.columns:
        routing_df["Active"] = routing_df["Active"].apply(parse_bool)
    else:
        routing_df["Active"] = True

    return machines, routing_df


def get_candidate_machines_for_product(
    machines: Dict[str, Machine],
    routing_df: pd.DataFrame,
    product_type: str,
    category: str,
    recipe: str,
) -> List[Machine]:
    """
    Возвращает список машин-кандидатов для (Тип, Категория, Рецептура)
    в порядке приоритета: сперва Routing, потом просто по Machines.Priority.
    """
    pt = (product_type or "").strip()
    cat = (category or "").strip()
    rec = (recipe or "").strip()

    # 1. Правила из Routing
    candidates_ids: List[str] = []
    if not routing_df.empty:
        mask = (
            (routing_df["Тип"].astype(str).str.strip() == pt)
            & (routing_df["Категория"].astype(str).str.strip() == cat)
            & (routing_df["Рецептура"].astype(str).str.strip() == rec)
            & (routing_df["Active"] == True)
        )
        subset = routing_df[mask].copy()
        subset = subset.sort_values(by="Priority", ascending=True)
        for _, row in subset.iterrows():
            mid = str(row.get("Preferred_Machine_ID", "")).strip()
            if mid and mid not in candidates_ids:
                candidates_ids.append(mid)

    # 2. Если по Routing ничего нет, подбираем машины по совместимости
    #    (или дополняем список, если Routing даёт только часть).
    for mid, mach in machines.items():
        if not mach.active:
            continue
        if mach.can_produce(pt, cat) and mid not in candidates_ids:
            candidates_ids.append(mid)

    # 3. Преобразуем ids в объекты Machine с сортировкой по Machine.priority
    result: List[Machine] = []
    for mid in candidates_ids:
        mach = machines.get(mid)
        if mach and mach.active and mach.can_produce(pt, cat):
            result.append(mach)

    result.sort(key=lambda m: m.priority)
    return result


def compute_priority_for_line(row, today_idx: int, tomorrow_idx: int) -> int:
    """
    Вычисляет приоритет строки плана:
    - сегодняшние отгрузки выше,
    - критический остаток выше,
    - завтрашние отгрузки выше остальных.
    При отсутствии Shipping_Days — считаем, что отгрузка может быть в любой день.
    """
    base = 0
    stock = row.get("Остаток")
    min_stock = row.get("Мин.Ост")
    try:
        stock_val = int(stock)
    except Exception:
        stock_val = 0
    try:
        min_stock_val = int(min_stock)
    except Exception:
        min_stock_val = 0

    is_critical = stock_val < min_stock_val if min_stock is not None else False
    if is_critical:
        base += 50

    shipping = str(row.get("Shipping_Days", "") or "")
    if not shipping:
        # Если Shipping_Days пуст, считаем, что товар важен всегда
        base += 10
    else:
        today_name = DAY_MAP[today_idx]
        tomorrow_name = DAY_MAP[tomorrow_idx]
        if today_name in shipping:
            base += 100
        if tomorrow_name in shipping:
            base += 20

    return base


def enrich_plan_with_brands(gc, tz) -> Tuple[pd.DataFrame, str, List[datetime]]:
    """
    Загружает План A на сегодня, обогащает его данными из Brands
    (Тип, Категория, Рецептура, Shipping_Days),
    ЖЁСТКО подменяет эти поля на канонические из Brands
    и считает приоритеты.

    Возвращает: plan_df, sheet_name, working_days
    """
    now_dt = datetime.now(tz)
    today_date = now_dt.date()
    day_str = today_date.strftime("%d.%m")
    sheet_name = config.REPORT_WORKSHEET_PREFIX + day_str

    # --- План из Production_Reports ---
    try:
        sh = gc.open(config.REPORTS_SHEET_NAME)
    except gspread.SpreadsheetNotFound:
        raise FileNotFoundError(
            f"Google Sheet '{config.REPORTS_SHEET_NAME}' не найдена. "
            f"Сначала запустите основной планировщик."
        )

    try:
        ws_plan = sh.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        raise FileNotFoundError(
            f"Лист '{sheet_name}' в '{config.REPORTS_SHEET_NAME}' не найден. "
            f"Сначала сформируйте План A на сегодня."
        )

    plan_records = ws_plan.get_all_records()
    plan_df = pd.DataFrame(plan_records)

    if plan_df.empty:
        raise ValueError(f"Лист '{sheet_name}' пуст — нечего планировать по машинам.")

    if "ПЛАН" not in plan_df.columns:
        raise ValueError("В листе плана нет колонки 'ПЛАН'.")

    # --- Brands ---
    brands_df = load_brands_reference(gc)

    required_b_cols = ["brand_1c", "Бренд", "Тип", "Категория", "Рецептура", "Shipping_Days"]
    missing_b = [c for c in required_b_cols if c not in brands_df.columns]
    if missing_b:
        raise ValueError(
            "В справочнике Brands отсутствуют колонки: " + ", ".join(missing_b)
        )

    # --- Основной ключ: 1С Имя -> brand_1c ---
    if "1С Имя" in plan_df.columns:
        plan_df["key_1c"] = plan_df["1С Имя"].astype(str).str.strip()

        b_small = brands_df[required_b_cols].copy()
        b_small["key_1c"] = b_small["brand_1c"].astype(str).str.strip()
        # один 1С-код -> одна строка
        b_small = b_small.drop_duplicates(subset=["key_1c"])

        plan_df = plan_df.merge(
            b_small[["key_1c", "Тип", "Категория", "Рецептура", "Shipping_Days"]],
            on="key_1c",
            how="left",
            suffixes=("", "_ref"),
        )
        plan_df = plan_df.drop(columns=["key_1c"])

    else:
        # Резервный вариант: связываемся по красивому бренду (менее надёжно)
        if "Бренд" not in plan_df.columns:
            raise ValueError(
                "В листе плана нет ни '1С Имя', ни 'Бренд' — не к чему привязать Brands."
            )

        plan_df["key_brand"] = plan_df["Бренд"].astype(str).str.strip()

        b_small = brands_df[required_b_cols].copy()
        b_small["key_brand"] = b_small["Бренд"].astype(str).str.strip()
        # если по какому-то бренду в Brands несколько строк, берём первую
        b_small = b_small.drop_duplicates(subset=["key_brand"])

        plan_df = plan_df.merge(
            b_small[["key_brand", "Тип", "Категория", "Рецептура", "Shipping_Days"]],
            on="key_brand",
            how="left",
            suffixes=("", "_ref"),
        )
        plan_df = plan_df.drop(columns=["key_brand"])

    # --- ЖЁСТКО подменяем Тип/Категория/Рецептура/Shipping_Days на канонические ---
    for col in ["Тип", "Категория", "Рецептура", "Shipping_Days"]:
        ref_col = f"{col}_ref"
        if ref_col in plan_df.columns:
            # если в Brands что-то есть — берём это, иначе оставляем старое значение
            plan_df[col] = plan_df[ref_col].where(
                plan_df[ref_col].notna() & (plan_df[ref_col].astype(str).str.strip() != ""),
                plan_df.get(col),
            )

    # убираем служебные *_ref
    ref_cols = [c for c in plan_df.columns if c.endswith("_ref")]
    if ref_cols:
        plan_df = plan_df.drop(columns=ref_cols)

    # --- Проверка рецептуры ---
    if "Рецептура" not in plan_df.columns:
        raise ValueError("После объединения с Brands нет колонки 'Рецептура'.")

    if plan_df["Рецептура"].isna().any():
        missing_rec = plan_df[plan_df["Рецептура"].isna()]["Бренд"].unique().tolist()
        raise ValueError(
            "Для следующих брендов не найдена 'Рецептура' в Brands: "
            + ", ".join(map(str, missing_rec))
        )

    # --- Нормализуем ПЛАН и фильтруем ---
    plan_df["ПЛАН"] = pd.to_numeric(plan_df["ПЛАН"], errors="coerce").fillna(0).astype(int)
    plan_df = plan_df[plan_df["ПЛАН"] > 0].copy()
    if plan_df.empty:
        raise ValueError("После фильтрации по ПЛАН > 0 план пуст.")

    # --- Горизонт рабочих дней ---
    working_days = build_working_days(today_date)

    # --- Приоритеты строк ---
    today_idx = today_date.weekday()       # 0=Пн, 6=Вс
    tomorrow_idx = (today_idx + 1) % 7

    plan_df["priority"] = plan_df.apply(
        lambda row: compute_priority_for_line(row, today_idx, tomorrow_idx),
        axis=1,
    )

    return plan_df, sheet_name, working_days


def build_batches(plan_df: pd.DataFrame) -> Dict[str, dict]:
    """
    Группирует строки плана в партии по (Тип, Категория, Рецептура).

    Возвращает dict:
        batch_key -> {
            'Тип', 'Категория', 'Рецептура', 'priority',
            'lines': [ { 'idx', 'Бренд', 'ПЛАН', 'remaining' }, ... ]
        }
    """
    batches: Dict[str, dict] = {}
    for idx, row in plan_df.iterrows():
        pt = str(row.get("Тип", "") or "").strip()
        cat = str(row.get("Категория", "") or "").strip()
        rec = str(row.get("Рецептура", "") or "").strip()
        if not rec:
            # без рецептуры планировать нельзя
            raise ValueError(f"Пустая рецептура для строки с Брендом '{row.get('Бренд')}'")

        key = f"{pt}||{cat}||{rec}"
        if key not in batches:
            batches[key] = {
                "Тип": pt,
                "Категория": cat,
                "Рецептура": rec,
                "priority": 0,
                "lines": [],
            }

        plan_qty = int(row["ПЛАН"])
        line_priority = int(row.get("priority", 0))
        if line_priority > batches[key]["priority"]:
            batches[key]["priority"] = line_priority

        batches[key]["lines"].append(
            {
                "idx": idx,
                "Бренд": row.get("Бренд"),
                "ПЛАН": plan_qty,
                "remaining": plan_qty,
            }
        )

    return batches


def allocate_to_lines(
    batch: dict,
    qty: int,
    day_idx: int,
    line_assignments: Dict[int, Dict[int, int]],
):
    """
    Раскидывает qty коробок партии по строкам (брендам) в batch['lines'],
    обновляет remaining и line_assignments.

    Возвращает:
        not_allocated: сколько коробок НЕ удалось распределить;
        brand_alloc: dict[Бренд] = qty, распределённое в ЭТОЙ итерации.
    """
    need = qty
    brand_alloc: Dict[str, int] = {}

    for line in batch["lines"]:
        if need <= 0:
            break
        rem = line["remaining"]
        if rem <= 0:
            continue
        take = min(rem, need)
        if take <= 0:
            continue

        line["remaining"] -= take
        need -= take

        idx = line["idx"]
        if idx not in line_assignments:
            line_assignments[idx] = {}
        line_assignments[idx][day_idx] = line_assignments[idx].get(day_idx, 0) + take

        brand = str(line.get("Бренд") or "")
        if brand:
            brand_alloc[brand] = brand_alloc.get(brand, 0) + take
        else:
            brand_alloc["_NO_BRAND_"] = brand_alloc.get("_NO_BRAND_", 0) + take

    return need, brand_alloc


def distribute_batches(
    batches: Dict[str, dict],
    machines: Dict[str, Machine],
    routing_df: pd.DataFrame,
    working_days: List[datetime],
):
    """
    Основной алгоритм распределения партий по дням и машинам.

    Возвращает:
        line_assignments: dict[row_index][day_idx] = qty
        machine_schedule: dict[(day_idx, machine_id)][(batch_key, brand)] = qty
    """
    # Остаток по партии
    batch_remaining: Dict[str, int] = {}
    for key, batch in batches.items():
        total = sum(line["remaining"] for line in batch["lines"])
        batch_remaining[key] = total

    # Остаток мощности по машине и дню
    machine_free: Dict[Tuple[int, str], int] = {}
    for day_idx in range(MAX_SHIFT_DAYS + 1):
        for mid, mach in machines.items():
            if not mach.active:
                continue
            machine_free[(day_idx, mid)] = mach.daily_capacity

    line_assignments: Dict[int, Dict[int, int]] = {}
    machine_schedule: Dict[Tuple[int, str], Dict[Tuple[str, str], int]] = {}

    # Сортируем партии по приоритету (от большего к меньшему)
    batch_items = sorted(
        batches.items(),
        key=lambda kv: kv[1]["priority"],
        reverse=True,
    )

    # Идём по дням: сначала сегодня (0), потом +1, потом +2
    for day_idx in range(MAX_SHIFT_DAYS + 1):
        for batch_key, batch in batch_items:
            remaining = batch_remaining.get(batch_key, 0)
            if remaining <= 0:
                continue

            pt = batch["Тип"]
            cat = batch["Категория"]
            rec = batch["Рецептура"]

            candidates = get_candidate_machines_for_product(machines, routing_df, pt, cat, rec)
            if not candidates:
                # Нет ни одной подходящей машины — пропускаем, останется в остатке
                continue

            for mach in candidates:
                free = machine_free.get((day_idx, mach.id), 0)
                if free <= 0 or remaining <= 0:
                    continue

                take = min(free, remaining)
                # Раскидываем по строкам внутри партии
                not_allocated, brand_alloc = allocate_to_lines(
                    batch, take, day_idx, line_assignments
                )
                real_take = take - not_allocated

                if real_take <= 0:
                    continue

                # Обновляем остатки
                remaining -= real_take
                batch_remaining[batch_key] = remaining
                machine_free[(day_idx, mach.id)] = free - real_take

                # Расписываем по брендам в machine_schedule
                if (day_idx, mach.id) not in machine_schedule:
                    machine_schedule[(day_idx, mach.id)] = {}

                batches_dict = machine_schedule[(day_idx, mach.id)]
                for brand, bqty in brand_alloc.items():
                    if bqty <= 0:
                        continue
                    k = (batch_key, brand)
                    batches_dict[k] = batches_dict.get(k, 0) + bqty

                if remaining <= 0:
                    break

    return line_assignments, machine_schedule, batch_remaining


def balance_soy_pp_between_two_machines(
    machine_schedule: Dict[Tuple[int, str], Dict[Tuple[str, str], int]],
    machines: Dict[str, Machine],
    batches: Dict[str, dict],
    soy_machine_ids: Tuple[str, str] = ("soy_pp_1", "soy_pp_2"),
):
    """
    Балансирует нагрузку между soy_pp_1 и soy_pp_2 для соевого соуса ПП.

    ВАЖНО:
    - Если у машин РАЗНЫЙ Priority (в таблице Machines), балансировка НЕ выполняется.
      Считаем, что пользователь явно хотел загрузить приоритетную машину под завязку.
    - Если Priority ОДИНАКОВЫЙ, пытаемся выровнять нагрузку.
    - Не режем один и тот же бренд между машинами ради "идеальной" балансировки.
    - Перекладываем ТОЛЬКО целые куски (batch_key, brand) целиком.
    """

    m1_id, m2_id = soy_machine_ids
    if m1_id not in machines or m2_id not in machines:
        return  # нет смысла

    mach1 = machines[m1_id]
    mach2 = machines[m2_id]

    # --- ПРОВЕРКА ПРИОРИТЕТОВ ---
    # Если приоритеты не равны, выходим. Планировщик уже загрузил первую машину
    # под завязку на этапе distribute_batches, так как она имела высший приоритет.
    if mach1.priority != mach2.priority:
        print(f"Балансировка пропущена: приоритеты {m1_id}={mach1.priority}, {m2_id}={mach2.priority}. Работает режим 'Загрузка по очереди'.")
        return
    # ---------------------------

    for day_idx in range(MAX_SHIFT_DAYS + 1):
        # Считаем суммарную нагрузку на каждую машину по соусу ПП
        load1 = 0
        load2 = 0

        for (d_idx, mid), batches_dict in machine_schedule.items():
            if d_idx != day_idx or mid not in (m1_id, m2_id):
                continue
            for (batch_key, brand), qty in batches_dict.items():
                if qty <= 0:
                    continue
                batch = batches.get(batch_key)
                if not batch:
                    continue
                # только ПП + соевый соус
                if batch["Тип"] != "ПП":
                    continue
                if "соев" not in batch["Категория"].lower():
                    continue

                if mid == m1_id:
                    load1 += qty
                elif mid == m2_id:
                    load2 += qty

        if load1 == 0 and load2 == 0:
            continue  # нечего балансировать

        cap1 = mach1.daily_capacity
        cap2 = mach2.daily_capacity
        free1 = cap1 - load1
        free2 = cap2 - load2

        # Если обе машины забиты или и так примерно поровну — ничего не делаем
        if (free1 <= 0 and free2 <= 0) or abs(load1 - load2) <= 1:
            continue

        # Определяем донор/получателя
        if load1 > load2:
            donor_id, receiver_id = m1_id, m2_id
            donor_load, receiver_load = load1, load2
            donor_cap, receiver_cap = cap1, cap2
        else:
            donor_id, receiver_id = m2_id, m1_id
            donor_load, receiver_load = load2, load1
            donor_cap, receiver_cap = cap2, cap1

        free_receiver = receiver_cap - receiver_load
        if free_receiver <= 0:
            continue

        # Сколько В ИДЕАЛЕ хотим перенести
        desired_move = min(
            free_receiver,
            max(0, (donor_load - receiver_load) // 2),
        )
        if desired_move <= 0:
            continue

        donor_key = (day_idx, donor_id)
        receiver_key = (day_idx, receiver_id)

        if donor_key not in machine_schedule:
            continue

        donor_batches = machine_schedule[donor_key]
        if receiver_key not in machine_schedule:
            machine_schedule[receiver_key] = {}
        receiver_batches = machine_schedule[receiver_key]

        remaining_move = desired_move

        # Перебираем партии донорской машины
        # ВАЖНО: переносим только целые (batch_key, brand), не делим qty
        for (batch_key, brand), qty in list(donor_batches.items()):
            if remaining_move <= 0:
                break
            if qty <= 0:
                continue

            batch = batches.get(batch_key)
            if not batch:
                continue
            if batch["Тип"] != "ПП":
                continue
            if "соев" not in batch["Категория"].lower():
                continue

            # Если целый бренд не помещается в "бюджет переноса" — пропускаем,
            # чтобы не резать его между машинами.
            if qty > remaining_move:
                continue

            # Переносим бренд целиком
            move_qty = qty

            # снимаем с донора
            del donor_batches[(batch_key, brand)]

            # добавляем к получателю
            receiver_batches[(batch_key, brand)] = (
                receiver_batches.get((batch_key, brand), 0) + move_qty
            )

            remaining_move -= move_qty

        # Если не смогли найти подходящие целые бренды для переноса —
        # просто оставляем как есть (бренд > баланс).
        # Это ок: приоритет "бренд на одной машине" выше, чем идеальная балансировка.


def build_line_columns(plan_df: pd.DataFrame, line_assignments: Dict[int, Dict[int, int]]):
    """
    Собирает колонки M0_cases, M1_cases, M2_cases на основе line_assignments.
    """
    m0 = []
    m1 = []
    m2 = []

    for idx in range(len(plan_df)):
        slots = line_assignments.get(idx, {})
        m0.append(int(slots.get(0, 0)))
        m1.append(int(slots.get(1, 0)))
        m2.append(int(slots.get(2, 0)))

    plan_df["M0_cases"] = m0
    plan_df["M1_cases"] = m1
    plan_df["M2_cases"] = m2

    return plan_df


def write_plan_back_to_sheet(gc, sheet_name: str, plan_df: pd.DataFrame):
    """
    Перезаписывает лист плана (План_DD.MM) с добавленными колонками M0/M1/M2.
    Сохраняет все существующие колонки + новые.
    """
    sh = gc.open(config.REPORTS_SHEET_NAME)
    ws = sh.worksheet(sheet_name)

    # Заголовки текущего листа
    headers = ws.row_values(1)
    # Добавляем новые колонки в конец, если их ещё нет
    for col in ["M0_cases", "M1_cases", "M2_cases"]:
        if col not in headers:
            headers.append(col)

    # Обеспечим, что в DF есть все колонки из headers
    for col in headers:
        if col not in plan_df.columns:
            plan_df[col] = ""

    df_out = plan_df[headers].copy()

    # Приводим числа к int для некоторых колонок
    for col in ["Остаток", "Мин.Ост", "Продажи", "ПЛАН", "M0_cases", "M1_cases", "M2_cases"]:
        if col in df_out.columns:
            df_out[col] = pd.to_numeric(df_out[col], errors="coerce").fillna(0).astype(int)

    values = [headers] + df_out.astype(str).fillna("").values.tolist()

    # Очищаем и записываем
    ws.clear()
    ws.update("A1", values)


def build_machine_plan_rows(
    machine_schedule: Dict[Tuple[int, str], Dict[Tuple[str, str], int]],
    machines: Dict[str, Machine],
    batches: Dict[str, dict],
    working_days: List[datetime],
    source_sheet_name: str,
):
    """
    Собирает строки для листа Машинный_план_DD.MM.
    Здесь уже есть детализация по брендам.
    """
    rows = []
    for (day_idx, machine_id), batches_dict in machine_schedule.items():
        if day_idx >= len(working_days):
            continue
        day_date = working_days[day_idx]
        mach = machines.get(machine_id)
        if not mach:
            continue

        for (batch_key, brand), qty in batches_dict.items():
            if qty <= 0:
                continue
            batch = batches.get(batch_key)
            if not batch:
                continue

            pt = batch["Тип"]
            cat = batch["Категория"]
            rec = batch["Рецептура"]

            rows.append(
                {
                    "Дата_производства": day_date.strftime("%d.%m.%Y"),
                    "Machine_ID": machine_id,
                    "Machine_Name": mach.name,
                    "Тип": pt,
                    "Категория": cat,
                    "Рецептура": rec,
                    "Бренд": ("" if brand == "_NO_BRAND_" else brand),
                    "Qty_cases": int(qty),
                    "Source_Sheet": source_sheet_name,
                    "Notes": "",
                }
            )

    if not rows:
        return pd.DataFrame(
            columns=[
                "Дата_производства",
                "Machine_ID",
                "Machine_Name",
                "Тип",
                "Категория",
                "Рецептура",
                "Бренд",
                "Qty_cases",
                "Source_Sheet",
                "Notes",
            ]
        )

    df = pd.DataFrame(rows)
    return df


def write_machine_plan_sheet(gc, day_str: str, machine_plan_df: pd.DataFrame):
    """
    Создаёт/перезаписывает лист Машинный_план_DD.MM в Production_Reports.
    """
    sh = gc.open(config.REPORTS_SHEET_NAME)
    sheet_name = MACHINE_PLAN_SHEET_PREFIX + day_str

    # Удаляем старый лист, если есть
    try:
        ws_old = sh.worksheet(sheet_name)
        sh.del_worksheet(ws_old)
    except gspread.WorksheetNotFound:
        pass

    # Создаём новый лист
    rows = max(len(machine_plan_df) + 1, 2)
    cols = max(len(machine_plan_df.columns), 1)
    ws_new = sh.add_worksheet(title=sheet_name, rows=rows, cols=cols)

    headers = list(machine_plan_df.columns)
    if not headers:
        headers = [
            "Дата_производства",
            "Machine_ID",
            "Machine_Name",
            "Тип",
            "Категория",
            "Рецептура",
            "Бренд",
            "Qty_cases",
            "Source_Sheet",
            "Notes",
        ]
        machine_plan_df = pd.DataFrame(columns=headers)

    values = [headers] + machine_plan_df.astype(str).fillna("").values.tolist()
    ws_new.update("A1", values)


def format_machine_plan_message(
    machine_schedule: Dict[Tuple[int, str], Dict[Tuple[str, str], int]],
    machines: Dict[str, Machine],
    batches: Dict[str, dict],
    working_days: List[datetime],
) -> Tuple[str, str]:
    """
    Формирует 2 текста:
    - основной отчёт на сегодня по машинам;
    - доп. отчёт по переносам на +1 и +2 день (если есть).

    Формат для каждой машины:
        🛠 Машина — N кор.
        *Рецептура: ...*
        - Бренд_1: X кор.
        - Бренд_2: Y кор.
    """
    if not machine_schedule:
        return "Машинный план пуст — все ПЛАНы = 0.", ""

    today_date = working_days[0]
    today_str = today_date.strftime("%d.%m.%Y")

    # --- Сообщение 1: план на сегодня (day_idx = 0) ---
    parts_today: List[str] = []
    parts_today.append(f"🧩 *Машинный план производства на {today_str}*")

    for mid, mach in sorted(machines.items(), key=lambda kv: kv[1].priority):
        key = (0, mid)
        if key not in machine_schedule:
            continue
        batches_dict = machine_schedule[key]
        if not batches_dict:
            continue

        total_qty = sum(qty for (_, _), qty in batches_dict.items())
        if total_qty <= 0:
            continue

        parts_today.append("")
        parts_today.append(f"🛠 *{mach.name}* — {total_qty} кор.")

        # Группируем по рецептурам и брендам
        rec_brand_map: Dict[str, Dict[str, int]] = {}
        for (batch_key, brand), qty in batches_dict.items():
            if qty <= 0:
                continue
            batch = batches.get(batch_key)
            if not batch:
                continue
            rec = batch["Рецептура"]
            brand_name = "" if brand == "_NO_BRAND_" else brand
            if rec not in rec_brand_map:
                rec_brand_map[rec] = {}
            rec_brand_map[rec][brand_name] = rec_brand_map[rec].get(brand_name, 0) + qty

        for rec in sorted(rec_brand_map.keys()):
            parts_today.append(f"*Рецептура: {rec}*")
            brand_map = rec_brand_map[rec]
            for brand_name, qty in sorted(brand_map.items(), key=lambda kv: kv[0]):
                if not brand_name:
                    parts_today.append(f"- Без бренда: {qty} кор.")
                else:
                    parts_today.append(f"- {brand_name}: {qty} кор.")

    msg_today = "\n".join(parts_today) if parts_today else "Машинный план на сегодня пуст."

    # --- Сообщение 2: переносы на +1 и +2 день ---
    parts_future: List[str] = []
    for day_idx in range(1, MAX_SHIFT_DAYS + 1):
        if day_idx >= len(working_days):
            continue
        date = working_days[day_idx]
        date_str = date.strftime("%d.%m.%Y")
        day_header_added = False

        for mid, mach in sorted(machines.items(), key=lambda kv: kv[1].priority):
            key = (day_idx, mid)
            if key not in machine_schedule:
                continue
            batches_dict = machine_schedule[key]
            if not batches_dict:
                continue

            total_qty = sum(qty for (_, _), qty in batches_dict.items())
            if total_qty <= 0:
                continue

            if not day_header_added:
                parts_future.append("")
                if day_idx == 1:
                    parts_future.append(f"🔮 Переносы на +1 рабочий день ({date_str}):")
                elif day_idx == 2:
                    parts_future.append(f"🔮 Переносы на +2 рабочих дня ({date_str}):")
                else:
                    parts_future.append(f"🔮 План на {day_idx}-й рабочий день ({date_str}):")
                day_header_added = True

            parts_future.append(f"🛠 *{mach.name}* — {total_qty} кор.")

            rec_brand_map: Dict[str, Dict[str, int]] = {}
            for (batch_key, brand), qty in batches_dict.items():
                if qty <= 0:
                    continue
                batch = batches.get(batch_key)
                if not batch:
                    continue
                rec = batch["Рецептура"]
                brand_name = "" if brand == "_NO_BRAND_" else brand
                if rec not in rec_brand_map:
                    rec_brand_map[rec] = {}
                rec_brand_map[rec][brand_name] = rec_brand_map[rec].get(brand_name, 0) + qty

            for rec in sorted(rec_brand_map.keys()):
                parts_future.append(f"*Рецептура: {rec}*")
                brand_map = rec_brand_map[rec]
                for brand_name, qty in sorted(brand_map.items(), key=lambda kv: kv[0]):
                    if not brand_name:
                        parts_future.append(f"- Без бренда: {qty} кор.")
                    else:
                        parts_future.append(f"- {brand_name}: {qty} кор.")

    msg_future = "\n".join(parts_future).strip()
    return msg_today, msg_future


def main():
    tz = pytz.timezone(TIMEZONE)
    print("=== Запуск машинного планировщика ===")

    # Подключение к Google Sheets
    gc = connect_gspread()

    # Telegram-бот
    bot = telebot.TeleBot(config.TELEGRAM_TOKEN)

    try:
        # 1. План + Brands
        plan_df, plan_sheet_name, working_days = enrich_plan_with_brands(gc, tz)

        # 2. Машины и маршрутизация
        machines, routing_df = load_machine_settings(gc)
        if not machines:
            raise ValueError("Не найдено ни одной активной машины в Machine_Settings/Machines.")

        # 3. Партии
        batches = build_batches(plan_df)

        # 4. Распределение по дням и машинам
        (
            line_assignments,
            machine_schedule,
            batch_remaining,
        ) = distribute_batches(batches, machines, routing_df, working_days)

        # 5. Балансировка между soy_pp_1 и soy_pp_2 (ТЕПЕРЬ УЧИТЫВАЕТ ПРИОРИТЕТЫ)
        balance_soy_pp_between_two_machines(machine_schedule, machines, batches)

        # 6. Колонки M0/M1/M2 в плане
        plan_df_with_days = build_line_columns(plan_df.copy(), line_assignments)
        write_plan_back_to_sheet(gc, plan_sheet_name, plan_df_with_days)

        # 7. Лист Машинный_план_DD.MM
        day_str = working_days[0].strftime("%d.%m")
        machine_plan_df = build_machine_plan_rows(
            machine_schedule, machines, batches, working_days, plan_sheet_name
        )
        write_machine_plan_sheet(gc, day_str, machine_plan_df)

        # 8. Формирование и отправка отчёта в Telegram
        msg_today, msg_future = format_machine_plan_message(
            machine_schedule, machines, batches, working_days
        )
        send_telegram(bot, config.TELEGRAM_CHAT_ID, msg_today)
        if msg_future:
            send_telegram(bot, config.TELEGRAM_CHAT_ID, msg_future)

        print("=== Машинный план успешно сформирован и записан ===")

    except Exception as e:
        err_text = f"{e.__class__.__name__}: {e}"
        print("ОШИБКА машинного планировщика:", err_text)
        try:
            log_error_to_sheet(gc, err_text, file_name="Machine_Planner")
        except Exception as log_err:
            print("Доп. ошибка при логировании:", log_err)
        try:
            send_telegram(
                bot,
                config.TELEGRAM_CHAT_ID,
                f"❌ Ошибка машинного планировщика.\n{err_text}\n"
                f"Подробности см. в {config.REPORTS_SHEET_NAME} / {config.ERROR_LOG_WORKSHEET}.",
            )
        except Exception:
            pass
        return


if __name__ == "__main__":
    main()