import logging
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters
)
import sqlite3
import pytz
import pandas as pd
import re
import asyncio
import io

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Конфигурация
TOKEN = '8013532367:AAFZ5wtMMay6URryudqKBbHcyR-RvP5mPOQ'
ADMINS = ["viktorv_k", "nastyastroyuk"]
TIMEZONE = pytz.timezone('Europe/Moscow')
DB_NAME = 'bookings.db'
MIN_CHILDREN = 12
MAX_GROUP_SIZE = 36

# Праздничные дни
HOLIDAYS = [
    "01.01.2025", "02.01.2025", "03.01.2025", "06.01.2025", "07.01.2025",
    "23.02.2025", "08.03.2025", "01.05.2025", "09.05.2025", "12.06.2025",
    "04.11.2025"
]

CALENDAR_HEADER = "📅 Выберите дату:\n(Рабочие дни кроме вторников)"
DAY_NAMES = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]

# Описания занятий (полные версии)
CLASS_DESCRIPTIONS = {
    'energy': "💡 ГДЕ РОЖДАЕТСЯ ЭЛЕКТРОЭНЕРГИЯ? 💡\n👫 1-11 класс и студенты СПО\n⏰ Продолжительность 45-60 минут\n\nБлагодаря интерактивному занятию ребята узнают, откуда в розетке берётся электричество и какой путь от электростанции проходит электрический ток, разберутся, зачем нужны повышающий и понижающий трансформаторы, что такое Единая национальная энергетическая сеть (ЕНЭС), по какому принципу работает большинство электростанций, какие виды источников энергии существуют, как происходит цепная ядерная реакция и почему человечество развивается в сторону освоения атомной энергии.",
    'sources': "⚡️ ИСТОЧНИКИ ЭНЕРГИИ ⚡️\n👫 7-11 класс и студенты СПО\n⏰ Продолжительность 60 минут\n\nНа занятии ребята узнают об основной из сегодняшних экологических проблем – парниковом эффекте, познакомятся с принципами работы различных электростанций, проанализируют их плюсы и минусы и оценят их значение в энергосистемах будущего. На основе полученных знаний ребята построят свою энергосеть на одной из колонизируемых планет, а также проанализируют собственную стратегию осознанного потребления.",
    'consumption': "♻️ ОСОЗНАННОЕ ПОТРЕБЛЕНИЕ ♻️\n👫 5-11 класс и студенты СПО\n⏰ Продолжительность 60-90 минут\n\nСегодня многие говорят об экологии и осознанном потреблении. Но что это означает? На занятии школьники не только разберутся, что считается экологичным, а что нет, но и изучат различные виды отходов. Участники приобретут полезные экопривычки и определят, какие виды посуды и упаковки экологичны, а какие только притворяются ими.",
    'plastic': "🥤 МИР ПЛАСТИКА 🥤\n👫 1-11 класс и студенты СПО\n⏰ Продолжительность 45-60 минут\n\nКак и зачем появился пластик, где он применяется, а главное – что мы можем и должны сделать, чтобы пластик не был проблемой? На занятии ребята познакомятся с видами пластика и его кодами переработки, а также узнают, какую пользу может принести уже использованный пластик.",
    'water': "💧 ВОДА: РЕСУРС 21 ВЕКА 💧\n👫 6-11 класс и студенты СПО\n⏰ Продолжительность 60-90 минут\n\nНа практикуме школьники изучат значение воды для человеческого организма, нормы её потребления и источники пресной воды на Земле. Они также рассчитают количество воды, которое используется в быту за неделю.",
    'materials': "⚛️ ИЗ ЧЕГО ВСЁ СДЕЛАНО? ⚛️\n👫 1-7 классы\n⏰ Продолжительность 40-50 минут\n\nШкольники узнают, из каких «кирпичиков» построена Вселенная, исследуют строение атома и обсудят, как «неделимые» частицы объединяются, чтобы создать всё многообразие веществ вокруг нас.",
    'quantum': "💻 КВАНТОВЫЕ ТЕХНОЛОГИИ 💻\n👫 7-11 класс и студенты СПО\n⏰ Продолжительность 60-90 минут\n\nУчастники познакомятся с основами квантовой физики: узнают, что такое кванты, принцип неопределённости Гейзенберга и корпускулярно-волновой дуализм на простых примерах."
}

CLASSES = {
    'energy': "💡 Где рождается электроэнергия?",
    'sources': "⚡️ Источники энергии",
    'consumption': "♻️ Осознанное потребление",
    'plastic': "🥤 Мир пластика",
    'water': "💧 Вода: ресурс 21 века",
    'materials': "⚛️ Из чего всё сделано?",
    'quantum': "💻 Квантовые технологии"
}

TIMES = ["10:00", "11:00", "12:00", "13:00", "14:00", "15:00", "16:00"]

def get_db():
    conn = sqlite3.connect(DB_NAME)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_db() as conn:
        conn.execute('''CREATE TABLE IF NOT EXISTS bookings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            day TEXT NOT NULL,
            time TEXT NOT NULL,
            class TEXT NOT NULL,
            contact_name TEXT NOT NULL,
            contact_phone TEXT NOT NULL,
            organization TEXT NOT NULL,
            grade TEXT NOT NULL,
            children_count INTEGER NOT NULL,
            username TEXT NOT NULL,
            UNIQUE(date, time)
        )''')
        conn.commit()

def is_available_date(date):
    now = datetime.now(TIMEZONE).date()
    if date < now:
        return False
    if date.weekday() in [1, 5, 6]:  # Вторники и выходные
        return False
    return date.strftime("%d.%m.%Y") not in HOLIDAYS

async def generate_calendar(query, context, year=None, month=None):
    today = datetime.now(TIMEZONE).date()
    year = year or today.year
    month = month or today.month

    first_day = datetime(year, month, 1)
    last_day = datetime(year, month % 12 + 1, 1) - timedelta(days=1)

    keyboard = []
    month_name = first_day.strftime("%B %Y")
    keyboard.append([
        InlineKeyboardButton("◀️", callback_data=f"cal_prev_{year}_{month}"),
        InlineKeyboardButton(month_name, callback_data="ignore"),
        InlineKeyboardButton("▶️", callback_data=f"cal_next_{year}_{month}")
    ])

    keyboard.append([InlineKeyboardButton(day, callback_data="ignore") for day in DAY_NAMES])

    week = []
    for _ in range(first_day.weekday()):
        week.append(InlineKeyboardButton(" ", callback_data="ignore"))

    for day in range(1, last_day.day + 1):
        date = datetime(year, month, day).date()
        if is_available_date(date):
            btn_text = f"*{day}*" if date == today else str(day)
            week.append(InlineKeyboardButton(
                btn_text,
                callback_data=f"date_{date.strftime('%d.%m.%Y')}"
            ))
        else:
            week.append(InlineKeyboardButton("✖️", callback_data="ignore"))

        if len(week) == 7:
            keyboard.append(week)
            week = []

    if week:
        keyboard.append(week)

    keyboard.append([
        InlineKeyboardButton("🔙 Назад", callback_data='back_to_classes'),
        InlineKeyboardButton("❌ Отмена", callback_data='back_to_menu')
    ])

    await query.edit_message_text(
        text=CALENDAR_HEADER,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

def main_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📝 Записать группу", callback_data='signup')],
        [InlineKeyboardButton("📋 Мои записи", callback_data='my_bookings')],
        [InlineKeyboardButton("❌ Отменить запись", callback_data='cancel_booking')]
    ])

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    welcome_text = (
        "«Атомный практикум» — это серия занятий, посвящённых атомным технологиям. "
        "Электроэнергия, атом в космосе и медицине, квантовые компьютеры, зелёная энергетика, "
        "атомный ледокольный флот, термоядерная энергия и замыкание ядерного топливного цикла – "
        "эти и другие темы школьники смогут подробно разобрать с помощью игровых и мультимедийных технологий.\n\n"
        "📌 Все мероприятия ИЦАЭ бесплатные. Занятия проводятся по будням для организованных групп "
        f"от {MIN_CHILDREN} до {MAX_GROUP_SIZE} человек по предварительной записи по адресу: ул. Комарова, д.6."
    )

    await update.message.reply_text(
        welcome_text,
        reply_markup=main_menu()
    )

async def show_classes(query):
    keyboard = [
        [InlineKeyboardButton(name, callback_data=f'class_{key}')]
        for key, name in CLASSES.items()
    ]
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data='back_to_menu')])

    await query.edit_message_text(
        text="🏫 Выберите занятие:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def show_class_description(query, class_type):
    description = CLASS_DESCRIPTIONS.get(class_type, "Описание отсутствует")
    keyboard = [
        [InlineKeyboardButton("📅 Выбрать дату", callback_data=f'book_{class_type}')],
        [InlineKeyboardButton("🔙 Назад", callback_data='back_to_classes')]
    ]
    await query.edit_message_text(
        text=description,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data = query.data

    try:
        if data == 'signup':
            await show_classes(query)
        elif data == 'my_bookings':
            await show_user_bookings(query)
        elif data == 'cancel_booking':
            await show_user_bookings_for_cancellation(query)
        elif data == 'back_to_menu':
            await query.edit_message_text(text="Главное меню:", reply_markup=main_menu())
        elif data == 'back_to_classes':
            await show_classes(query)
        elif data.startswith('class_'):
            await show_class_description(query, data[6:])
        elif data.startswith('book_'):
            context.user_data['class'] = data[5:]
            await generate_calendar(query, context)
        elif data.startswith('cal_prev_'):
            _, _, year, month = data.split('_')
            await generate_calendar(query, context, int(year), int(month) - 1)
        elif data.startswith('cal_next_'):
            _, _, year, month = data.split('_')
            await generate_calendar(query, context, int(year), int(month) + 1)
        elif data.startswith('date_'):
            context.user_data['date'] = data[5:]
            await choose_time(query, context)
        elif data.startswith('time_'):
            context.user_data['time'] = data[5:]
            await ask_contact_info(query, context)
        elif data.startswith('cancel_'):
            await cancel_booking(query, int(data[7:]))
        elif data == 'admin_all_bookings':
            await show_all_bookings(query)
        elif data == 'admin_today':
            await show_today_bookings(query)
        else:
            await query.edit_message_text(text="Неизвестная команда", reply_markup=main_menu())
    except Exception as e:
        logger.error(f"Ошибка в обработчике кнопок: {e}")
        await query.edit_message_text(
            text="❌ Произошла ошибка. Попробуйте еще раз.",
            reply_markup=main_menu()
        )

async def choose_time(query, context):
    date = context.user_data['date']

    try:
        with get_db() as conn:
            booked_times = {row['time'] for row in conn.execute(
                'SELECT time FROM bookings WHERE date=?',
                (date,)
            )}

        available_times = [time for time in TIMES if time not in booked_times]

        if not available_times:
            await query.edit_message_text(
                text="⛔ На выбранную дату все времена заняты.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Назад", callback_data=f'book_{context.user_data["class"]}')],
                    [InlineKeyboardButton("❌ Отмена", callback_data='back_to_menu')]
                ])
            )
            return

        keyboard = [
            [InlineKeyboardButton(time, callback_data=f'time_{time}')]
            for time in available_times
        ]
        keyboard.append([
            InlineKeyboardButton("🔙 Назад", callback_data=f'book_{context.user_data["class"]}'),
            InlineKeyboardButton("❌ Отмена", callback_data='back_to_menu')
        ])

        await query.edit_message_text(
            text=f"⏰ Выберите время для {CLASSES[context.user_data['class']]} {date}:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    except Exception as e:
        logger.error(f"Ошибка при выборе времени: {e}")
        await query.edit_message_text(
            text="❌ Произошла ошибка при загрузке доступных времен.",
            reply_markup=main_menu()
        )

async def ask_contact_info(query, context):
    context.user_data['step'] = 'contact_name'
    await query.edit_message_text(
        text="👤 Введите ФИО контактного лица (полностью):",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ Отмена", callback_data='back_to_menu')]
        ])
    )

async def process_contact_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_data = context.user_data
    message = update.message

    if 'step' not in user_data:
        await message.reply_text("❌ Ошибка. Начните запись заново.", reply_markup=main_menu())
        return

    try:
        if user_data['step'] == 'contact_name':
            if not message.text.strip():
                await message.reply_text("❌ Введите непустое значение:")
                return
            user_data['contact_name'] = message.text.strip()
            user_data['step'] = 'contact_phone'
            await message.reply_text("📱 Введите номер телефона (например: +79161234567):")

        elif user_data['step'] == 'contact_phone':
            if not re.match(r'^\+?\d{10,15}$', message.text):
                await message.reply_text("❌ Введите корректный номер телефона:")
                return
            user_data['contact_phone'] = message.text
            user_data['step'] = 'organization'
            await message.reply_text("🏢 Введите название организации:")

        elif user_data['step'] == 'organization':
            if not message.text.strip():
                await message.reply_text("❌ Введите непустое значение:")
                return
            user_data['organization'] = message.text.strip()
            user_data['step'] = 'grade'
            await message.reply_text("🏫 Введите класс/курс (например: 5А или 1 курс):")

        elif user_data['step'] == 'grade':
            if not message.text.strip():
                await message.reply_text("❌ Введите непустое значение:")
                return
            user_data['grade'] = message.text.strip()
            user_data['step'] = 'children_count'
            await message.reply_text(
                f"👶 Введите количество детей (от {MIN_CHILDREN} до {MAX_GROUP_SIZE}):",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("❌ Отмена", callback_data='back_to_menu')]
                ])
            )

        elif user_data['step'] == 'children_count':
            try:
                count = int(message.text)
                if count < MIN_CHILDREN or count > MAX_GROUP_SIZE:
                    await message.reply_text(f"❌ Введите число от {MIN_CHILDREN} до {MAX_GROUP_SIZE}:")
                    return
                user_data['children_count'] = count
                await save_booking(update, context)
            except ValueError:
                await message.reply_text("❌ Введите число цифрами:")
    except Exception as e:
        logger.error(f"Ошибка при обработке контактной информации: {e}")
        await message.reply_text(
            "❌ Произошла ошибка. Начните запись заново.",
            reply_markup=main_menu()
        )

async def save_booking(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_data = context.user_data
    user = update.effective_user

    try:
        with get_db() as conn:
            existing = conn.execute(
                'SELECT 1 FROM bookings WHERE date=? AND time=?',
                (user_data['date'], user_data['time'])
            ).fetchone()

            if existing:
                await update.message.reply_text(
                    "⛔ Это время уже занято. Пожалуйста, выберите другое.",
                    reply_markup=main_menu()
                )
                return

            conn.execute('''
                INSERT INTO bookings (date, day, time, class, contact_name,
                                    contact_phone, organization, grade,
                                    children_count, username)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                user_data['date'],
                datetime.strptime(user_data['date'], '%d.%m.%Y').strftime('%A'),
                user_data['time'],
                CLASSES[user_data['class']],
                user_data['contact_name'],
                user_data['contact_phone'],
                user_data['organization'],
                user_data['grade'],
                user_data['children_count'],
                user.username or str(user.id)
            ))
            conn.commit()

            await notify_admins(context, {
                'date': user_data['date'],
                'time': user_data['time'],
                'class': CLASSES[user_data['class']],
                'contact_name': user_data['contact_name'],
                'contact_phone': user_data['contact_phone'],
                'organization': user_data['organization'],
                'grade': user_data['grade'],
                'children_count': user_data['children_count']
            }, user.username)

            confirm_message = (
                "✅ <b>Запись успешно сохранена!</b>\n\n"
                f"📅 <b>Дата:</b> {user_data['date']}\n"
                f"⏰ <b>Время:</b> {user_data['time']}\n"
                f"🏫 <b>Занятие:</b> {CLASSES[user_data['class']]}\n"
                f"👤 <b>Контактное лицо:</b> {user_data['contact_name']}\n"
                f"📱 <b>Телефон:</b> {user_data['contact_phone']}\n"
                f"🏢 <b>Организация:</b> {user_data['organization']}\n"
                f"🎓 <b>Класс/курс:</b> {user_data['grade']}\n"
                f"👶 <b>Количество детей:</b> {user_data['children_count']}"
            )

            await update.message.reply_text(
                confirm_message,
                reply_markup=main_menu(),
                parse_mode="HTML"
            )

    except sqlite3.IntegrityError:
        await update.message.reply_text(
            "⛔ Это время уже занято другим пользователем.",
            reply_markup=main_menu()
        )
    except Exception as e:
        logger.error(f"Ошибка при сохранении записи: {e}")
        await update.message.reply_text(
            "❌ Произошла ошибка при сохранении записи.",
            reply_markup=main_menu()
        )
    finally:
        context.user_data.clear()

async def show_user_bookings(query):
    username = query.from_user.username or str(query.from_user.id)

    try:
        with get_db() as conn:
            bookings = conn.execute('''
                SELECT id, date, time, class, organization, grade, children_count
                FROM bookings
                WHERE username = ?
                ORDER BY date, time
            ''', (username,)).fetchall()

        if not bookings:
            await query.edit_message_text(
                text="📭 У вас нет активных записей.",
                reply_markup=main_menu()
            )
            return

        message = "📋 <b>Ваши записи:</b>\n\n"
        for booking in bookings:
            message += (
                f"📅 <b>{booking['date']}</b> в <b>{booking['time']}</b>\n"
                f"🏫 {booking['class']}\n"
                f"🏢 {booking['organization']} ({booking['grade']})\n"
                f"👶 Детей: {booking['children_count']}\n\n"
            )

        await query.edit_message_text(
            text=message,
            reply_markup=main_menu(),
            parse_mode="HTML"
        )
    except Exception as e:
        logger.error(f"Ошибка при получении записей: {e}")
        await query.edit_message_text(
            "❌ Произошла ошибка при получении записей.",
            reply_markup=main_menu()
        )

async def show_user_bookings_for_cancellation(query):
    username = query.from_user.username or str(query.from_user.id)

    try:
        with get_db() as conn:
            bookings = conn.execute('''
                SELECT id, date, time, class
                FROM bookings
                WHERE username = ?
                ORDER BY date, time
            ''', (username,)).fetchall()

        if not bookings:
            await query.edit_message_text(
                text="📭 У вас нет активных записей для отмены.",
                reply_markup=main_menu()
            )
            return

        keyboard = [
            [InlineKeyboardButton(
                f"{booking['date']} {booking['time']} - {booking['class']}",
                callback_data=f"cancel_{booking['id']}"
            )]
            for booking in bookings
        ]
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data='back_to_menu')])

        await query.edit_message_text(
            text="❌ Выберите запись для отмены:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    except Exception as e:
        logger.error(f"Ошибка при получении записей: {e}")
        await query.edit_message_text(
            "❌ Произошла ошибка при получении записей.",
            reply_markup=main_menu()
        )

async def cancel_booking(query, booking_id):
    username = query.from_user.username or str(query.from_user.id)

    try:
        with get_db() as conn:
            booking = conn.execute('''
                SELECT *
                FROM bookings
                WHERE id = ?
                AND username = ?
            ''', (booking_id, username)).fetchone()

            if not booking:
                await query.edit_message_text(
                    text="❌ Запись не найдена или у вас нет прав для её отмены.",
                    reply_markup=main_menu()
                )
                return

            conn.execute('DELETE FROM bookings WHERE id = ?', (booking_id,))
            conn.commit()

            cancel_message = (
                "❌ <b>Запись отменена:</b>\n\n"
                f"📅 <b>Дата:</b> {booking['date']} ({booking['day']})\n"
                f"⏰ <b>Время:</b> {booking['time']}\n"
                f"🏫 <b>Занятие:</b> {booking['class']}\n"
                f"👤 <b>Контактное лицо:</b> {booking['contact_name']}\n"
                f"🏢 <b>Организация:</b> {booking['organization']}"
            )

            await query.edit_message_text(
                text=cancel_message,
                reply_markup=main_menu(),
                parse_mode="HTML"
            )
    except Exception as e:
        logger.error(f"Ошибка при отмене записи: {e}")
        await query.edit_message_text(
            "❌ Произошла ошибка при отмене записи.",
            reply_markup=main_menu()
        )

async def notify_admins(context: ContextTypes.DEFAULT_TYPE, booking_data: dict, username: str = None):
    try:
        admin_message = (
            "🔔 <b>НОВАЯ ЗАПИСЬ</b>\n\n"
            f"📅 <b>Дата:</b> {booking_data['date']}\n"
            f"⏰ <b>Время:</b> {booking_data['time']}\n"
            f"🏫 <b>Занятие:</b> {booking_data['class']}\n"
            f"👤 <b>Контакт:</b> {booking_data['contact_name']}\n"
            f"📱 <b>Телефон:</b> {booking_data['contact_phone']}\n"
            f"🏢 <b>Организация:</b> {booking_data['organization']}\n"
            f"🎓 <b>Класс/курс:</b> {booking_data['grade']}\n"
            f"👶 <b>Детей:</b> {booking_data['children_count']}\n"
            f"👤 <b>Записал:</b> @{username if username else 'не указан'}\n\n"
            f"🕒 <i>Запись создана: {datetime.now(TIMEZONE).strftime('%d.%m.%Y %H:%M')}</i>"
        )

        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("📋 Все записи", callback_data='admin_all_bookings')],
            [InlineKeyboardButton("📅 Сегодня", callback_data='admin_today')]
        ])

        for admin in ADMINS:
            try:
                await context.bot.send_message(
                    chat_id=admin,
                    text=admin_message,
                    reply_markup=keyboard,
                    parse_mode="HTML"
                )
            except Exception as e:
                logger.error(f"Ошибка уведомления админа {admin}: {e}")
    except Exception as e:
        logger.error(f"Ошибка в функции notify_admins: {e}")

async def show_all_bookings(query):
    try:
        with get_db() as conn:
            bookings = conn.execute('''
                SELECT date, time, class, contact_name, organization
                FROM bookings
                ORDER BY date DESC
                LIMIT 20
            ''').fetchall()

        message = "📋 <b>Последние 20 записей:</b>\n\n"
        for booking in bookings:
            message += (
                f"📅 <b>{booking['date']}</b> {booking['time']}\n"
                f"🏫 {booking['class']}\n"
                f"👤 {booking['contact_name']} ({booking['organization']})\n\n"
            )

        await query.edit_message_text(
            text=message,
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data='back_to_menu')]
            ])
        )
    except Exception as e:
        logger.error(f"Ошибка при получении записей: {e}")
        await query.edit_message_text(
            "❌ Ошибка при загрузке записей",
            reply_markup=main_menu()
        )

async def show_today_bookings(query):
    today = datetime.now(TIMEZONE).strftime('%d.%m.%Y')

    try:
        with get_db() as conn:
            bookings = conn.execute('''
                SELECT time, class, contact_name, organization
                FROM bookings
                WHERE date = ?
                ORDER BY time
            ''', (today,)).fetchall()

        message = f"📅 <b>Записи на сегодня ({today}):</b>\n\n"
        for booking in bookings:
            message += (
                f"⏰ {booking['time']} - {booking['class']}\n"
                f"👤 {booking['contact_name']} ({booking['organization']})\n\n"
            )

        await query.edit_message_text(
            text=message,
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data='back_to_menu')]
            ])
        )
    except Exception as e:
        logger.error(f"Ошибка при получении записей: {e}")
        await query.edit_message_text(
            "❌ Ошибка при загрузке записей",
            reply_markup=main_menu()
        )

async def export_to_excel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.username not in ADMINS:
        await update.message.reply_text("❌ У вас нет прав доступа")
        return

    try:
        with get_db() as conn:
            df = pd.read_sql('''
                SELECT
                    date as "Дата", day as "День недели", time as "Время", 
                    class as "Занятие", contact_name as "Контактное лицо", 
                    contact_phone as "Телефон", organization as "Организация", 
                    grade as "Класс/курс", children_count as "Кол-во детей", 
                    username as "Username"
                FROM bookings
                ORDER BY date, time
            ''', conn)

        if df.empty:
            await update.message.reply_text("ℹ️ Нет данных для экспорта")
            return

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Записи')

            workbook = writer.book
            worksheet = writer.sheets['Записи']

            for i, col in enumerate(df.columns):
                max_len = max(df[col].astype(str).map(len).max(), len(col)) + 2
                worksheet.set_column(i, i, max_len)

            worksheet.autofilter(0, 0, 0, len(df.columns) - 1)

        output.seek(0)
        await update.message.reply_document(
            document=output,
            filename=f"Записи_{datetime.now().strftime('%Y-%m-%d')}.xlsx",
            caption="📊 Все записи в формате Excel"
        )
    except Exception as e:
        logger.error(f"Ошибка при экспорте: {e}")
        await update.message.reply_text(f"❌ Ошибка при создании файла: {str(e)}")


async def show_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.username not in ADMINS:
        await update.message.reply_text("❌ У вас нет прав доступа")
        return

    try:
        today = datetime.now(TIMEZONE).date()
        next_week = today + timedelta(days=7)

        with get_db() as conn:
            stats = {
                'total': conn.execute("SELECT COUNT(*) FROM bookings").fetchone()[0] or 0,
                'today': conn.execute("SELECT COUNT(*) FROM bookings WHERE date=?",
                                      (today.strftime('%d.%m.%Y'),)).fetchone()[0] or 0,
                'next_7_days': conn.execute('''
                                            SELECT date, time, class, contact_name, contact_phone, organization, grade, children_count
                                            FROM bookings
                                            WHERE date >= ? AND date < ?
                                            ORDER BY date, time
                                            ''',
                                            (today.strftime('%d.%m.%Y'), next_week.strftime('%d.%m.%Y'))).fetchall(),
                'by_class': conn.execute('''
                                         SELECT class, COUNT(*) as count
                                         FROM bookings
                                         WHERE class IS NOT NULL
                                         GROUP BY class
                                         ORDER BY count DESC
                                             LIMIT 10
                                         ''').fetchall() or []
            }

        message = [
            "📊 <b>Статистика записей</b>",
            f"• Всего записей: <b>{stats['total']}</b>",
            f"• На сегодня: <b>{stats['today']}</b>",
            ""
        ]

        # Записи на ближайшие 7 дней (включая сегодня)
        message.append("<b>Ближайшие 7 дней:</b>")

        if stats['next_7_days']:
            current_date = None
            for booking in stats['next_7_days']:
                booking_date = booking['date']  # Уже в формате dd.mm.YYYY

                # Добавляем заголовок даты если она изменилась
                if booking_date != current_date:
                    day_name = datetime.strptime(booking_date, '%d.%m.%Y').strftime('%A')
                    message.append(f"\n📅 <b>{booking_date} ({day_name})</b>")
                    current_date = booking_date

                message.extend([
                    f"⏰ <b>{booking['time']}</b> - {booking['class']}",
                    f"👤 {booking['contact_name']} ({booking['organization']})",
                    f"📱 {booking['contact_phone']} | 👶 {booking['children_count']}",
                    ""
                ])
        else:
            message.append("\nНет записей")

        # Топ занятий
        message.extend(["", "<b>Популярные занятия:</b>"])
        if stats['by_class']:
            for i, row in enumerate(stats['by_class'], 1):
                message.append(f"{i}. {row['class']}: <b>{row['count']}</b>")
        else:
            message.append("Нет данных")

        await update.message.reply_text("\n".join(message), parse_mode="HTML")

    except Exception as e:
        logger.error(f"Ошибка статистики: {str(e)}", exc_info=True)
        await update.message.reply_text("❌ Ошибка при формировании статистики. Проверьте логи.")

async def admin_commands(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.username not in ADMINS:
        await update.message.reply_text("❌ У вас нет прав доступа")
        return

    commands = [
        "/export - Экспорт всех записей в Excel",
        "/stats - Показать статистику",
        "/admin - Показать это меню"
    ]
    await update.message.reply_text("🔐 Админ-команды:\n" + "\n".join(commands))

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error("Exception:", exc_info=context.error)
    if update.message:
        await update.message.reply_text(
            "❌ Произошла ошибка. Пожалуйста, попробуйте позже.",
            reply_markup=main_menu()
        )

def main():
    init_db()
    logger.info("База данных инициализирована")

    application = Application.builder().token(TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("admin", admin_commands, filters=filters.User(username=ADMINS)))
    application.add_handler(CommandHandler("export", export_to_excel, filters=filters.User(username=ADMINS)))
    application.add_handler(CommandHandler("stats", show_stats, filters=filters.User(username=ADMINS)))
    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, process_contact_info))
    application.add_error_handler(error_handler)

    logger.info("Бот запускается...")
    application.run_polling()

if __name__ == '__main__':
    main()