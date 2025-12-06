"""
Business: Telegram bot for garbage collection courier service with roles
Args: event - webhook from Telegram with updates
      context - cloud function context with request_id
Returns: HTTP response with statusCode 200
"""

import json
import os
import psycopg2
from typing import Dict, Any, Optional, List
from datetime import datetime
from threading import local

TELEGRAM_API = "https://api.telegram.org/bot{token}/{method}"

_context = local()

BAG_PRICE = 50
MAX_BAGS_QUICK_SELECT = 10

ORDER_STATUSES = {
    'waiting_payment': '💳 Ожидает оплаты',
    'searching_courier': '🔍 В поиске курьера',
    'courier_on_way': '🚗 Курьер едет',
    'courier_working': '🛠 Курьер выполняет заказ',
    'completed': '✅ Завершён',
    'cancelled': '❌ Отменён'
}

SCHEMA = 't_p39739760_garbage_bot_service'

def get_db_connection():
    database_url = os.environ.get('DATABASE_URL')
    return psycopg2.connect(database_url)

def send_message(chat_id: int, text: str, reply_markup: Optional[Dict] = None) -> None:
    token = os.environ.get('TELEGRAM_BOT_TOKEN')
    url = TELEGRAM_API.format(token=token, method='sendMessage')
    
    payload = {
        'chat_id': chat_id,
        'text': text,
        'parse_mode': 'HTML'
    }
    
    if reply_markup:
        payload['reply_markup'] = reply_markup
    
    import urllib.request
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode('utf-8'),
        headers={'Content-Type': 'application/json'}
    )
    try:
        urllib.request.urlopen(req)
    except Exception as e:
        print(f"Error sending message: {e}")

def edit_message(chat_id: int, message_id: int, text: str, reply_markup: Optional[Dict] = None) -> None:
    token = os.environ.get('TELEGRAM_BOT_TOKEN')
    url = TELEGRAM_API.format(token=token, method='editMessageText')
    
    payload = {
        'chat_id': chat_id,
        'message_id': message_id,
        'text': text,
        'parse_mode': 'HTML'
    }
    
    if reply_markup:
        payload['reply_markup'] = reply_markup
    
    import urllib.request
    try:
        req = urllib.request.Request(
            url,
            data=json.dumps(payload).encode('utf-8'),
            headers={'Content-Type': 'application/json'}
        )
        urllib.request.urlopen(req)
    except:
        pass

def send_or_edit_message(chat_id: int, text: str, reply_markup: Optional[Dict] = None, message_id: Optional[int] = None) -> None:
    if message_id:
        edit_message(chat_id, message_id, text, reply_markup)
    else:
        send_message(chat_id, text, reply_markup)

def smart_send_message(chat_id: int, text: str, reply_markup: Optional[Dict] = None) -> None:
    message_id = getattr(_context, 'message_id', None)
    if message_id:
        edit_message(chat_id, message_id, text, reply_markup)
    else:
        send_message(chat_id, text, reply_markup)

def delete_message(chat_id: int, message_id: int) -> None:
    token = os.environ.get('TELEGRAM_BOT_TOKEN')
    url = TELEGRAM_API.format(token=token, method='deleteMessage')
    
    payload = {
        'chat_id': chat_id,
        'message_id': message_id
    }
    
    import urllib.request
    try:
        req = urllib.request.Request(
            url,
            data=json.dumps(payload).encode('utf-8'),
            headers={'Content-Type': 'application/json'}
        )
        urllib.request.urlopen(req)
    except:
        pass

def check_user_role(telegram_id: int, conn) -> str:
    cursor = conn.cursor()
    
    cursor.execute(f"SELECT 1 FROM {SCHEMA}.admin_users WHERE telegram_id = %s", (telegram_id,))
    if cursor.fetchone():
        cursor.close()
        return 'admin'
    
    cursor.execute(f"SELECT 1 FROM {SCHEMA}.operator_users WHERE telegram_id = %s", (telegram_id,))
    if cursor.fetchone():
        cursor.close()
        return 'operator'
    
    cursor.execute(f"SELECT role FROM {SCHEMA}.users WHERE telegram_id = %s", (telegram_id,))
    user = cursor.fetchone()
    cursor.close()
    
    return user[0] if user else 'client'

def get_main_menu_keyboard(role: str) -> Dict:
    if role == 'admin':
        return {
            'inline_keyboard': [
                [{'text': '👑 Админ-панель', 'callback_data': 'admin_panel'}],
                [{'text': '📞 Режим оператора', 'callback_data': 'switch_to_operator'}],
                [{'text': '👔 Режим курьера', 'callback_data': 'switch_to_courier'}],
                [{'text': '📊 Статистика сервиса', 'callback_data': 'admin_stats'}],
                [{'text': '👔 Управление курьерами', 'callback_data': 'admin_couriers'}],
                [{'text': '👥 Управление операторами', 'callback_data': 'admin_operators'}],
                [{'text': '📦 Все заказы', 'callback_data': 'admin_all_orders'}]
            ]
        }
    elif role == 'operator':
        return {
            'inline_keyboard': [
                [{'text': '📞 Активные заказы', 'callback_data': 'operator_active_orders'}],
                [{'text': '💬 Чаты заказов', 'callback_data': 'operator_chats'}],
                [{'text': '📊 Статистика', 'callback_data': 'operator_stats'}]
            ]
        }
    elif role == 'courier':
        return get_courier_menu_keyboard()
    else:
        return {
            'inline_keyboard': [
                [{'text': '👔 Стать курьером', 'callback_data': 'apply_courier'}],
                [{'text': '👤 Для клиентов', 'callback_data': 'client_menu'}],
                [{'text': '💬 Поддержка', 'url': 'https://t.me/support'}]
            ]
        }

def get_courier_menu_keyboard() -> Dict:
    return {
        'inline_keyboard': [
            [{'text': '📦 Доступные заказы', 'callback_data': 'courier_available'}],
            [{'text': '🚚 Текущие заказы', 'callback_data': 'courier_current'}],
            [{'text': '📊 История заказов', 'callback_data': 'courier_history'}],
            [{'text': '💰 Статистика и финансы', 'callback_data': 'courier_stats'}],
            [{'text': '💬 Связаться с поддержкой', 'url': 'https://t.me/support'}],
            [{'text': '💵 Вывод денежных средств', 'callback_data': 'courier_withdraw'}],
            [{'text': '⬅️ Назад', 'callback_data': 'start'}]
        ]
    }

def get_client_menu_keyboard() -> Dict:
    return {
        'inline_keyboard': [
            [{'text': '➕ Сделать заказ', 'callback_data': 'client_new_order'}],
            [{'text': '📦 Активные заказы', 'callback_data': 'client_active'}],
            [{'text': '📊 История заказов', 'callback_data': 'client_history'}],
            [{'text': '💳 Способ оплаты', 'callback_data': 'client_payment'}],
            [{'text': '💬 Связаться с поддержкой', 'url': 'https://t.me/support'}],
            [{'text': '⭐ Подписка', 'callback_data': 'client_subscription'}],
            [{'text': '⬅️ Назад', 'callback_data': 'start'}]
        ]
    }

def archive_old_chats(conn) -> None:
    cursor = conn.cursor()
    
    cursor.execute(
        f"INSERT INTO {SCHEMA}.order_chat_archive (order_id, sender_id, message, created_at) "
        f"SELECT oc.order_id, oc.sender_id, oc.message, oc.created_at "
        f"FROM {SCHEMA}.order_chat oc "
        f"JOIN {SCHEMA}.orders o ON oc.order_id = o.id "
        "WHERE o.status IN ('completed', 'cancelled') "
        "AND o.updated_at < NOW() - INTERVAL '7 days' "
        "AND oc.is_archived = FALSE"
    )
    
    cursor.execute(
        f"UPDATE {SCHEMA}.order_chat SET is_archived = TRUE "
        "WHERE order_id IN ("
        f"    SELECT o.id FROM {SCHEMA}.orders o "
        "    WHERE o.status IN ('completed', 'cancelled') "
        "    AND o.updated_at < NOW() - INTERVAL '7 days'"
        ")"
    )
    
    conn.commit()
    cursor.close()

def get_or_create_user(telegram_id: int, username: str, first_name: str, conn) -> Dict:
    cursor = conn.cursor()
    
    cursor.execute(
        f"SELECT telegram_id, username, first_name, role FROM {SCHEMA}.users WHERE telegram_id = %s",
        (telegram_id,)
    )
    user = cursor.fetchone()
    
    if user:
        cursor.close()
        return {
            'telegram_id': user[0],
            'username': user[1],
            'first_name': user[2],
            'role': user[3]
        }
    
    cursor.execute(
        "INSERT INTO t_p39739760_garbage_bot_service.users (telegram_id, username, first_name, role) VALUES (%s, %s, %s, %s) RETURNING telegram_id, username, first_name, role",
        (telegram_id, username, first_name, 'client')
    )
    new_user = cursor.fetchone()
    conn.commit()
    cursor.close()
    
    return {
        'telegram_id': new_user[0],
        'username': new_user[1],
        'first_name': new_user[2],
        'role': new_user[3]
    }

def handle_start(chat_id: int, telegram_id: int, username: str, first_name: str, conn) -> None:
    get_or_create_user(telegram_id, username, first_name, conn)
    role = check_user_role(telegram_id, conn)
    
    try:
        archive_old_chats(conn)
    except Exception:
        pass
    
    if role == 'admin':
        welcome_text = "👑 <b>Админ-панель</b>\n\nДобро пожаловать в панель администратора."
    elif role == 'operator':
        welcome_text = "📞 <b>Панель оператора</b>\n\nДобро пожаловать в панель оператора."
    elif role == 'courier':
        welcome_text = "👔 <b>Меню курьера</b>\n\nВыберите действие:"
    else:
        welcome_text = (
            "🚚 <b>Курьерская служба «Экономь время»</b>\n\n"
            "Добро пожаловать! Мы предоставляем услуги вывоза мусора.\n\n"
            "Выберите действие:"
        )
    
    smart_send_message(chat_id, welcome_text, get_main_menu_keyboard(role))

def handle_apply_courier(chat_id: int, telegram_id: int, conn) -> None:
    cursor = conn.cursor()
    
    cursor.execute(
        "SELECT status FROM t_p39739760_garbage_bot_service.courier_applications WHERE telegram_id = %s ORDER BY created_at DESC LIMIT 1",
        (telegram_id,)
    )
    existing = cursor.fetchone()
    
    if existing and existing[0] == 'pending':
        text = "⏳ Ваша заявка на рассмотрении. Ожидайте одобрения администратора."
        cursor.close()
        keyboard = {'inline_keyboard': [[{'text': '⬅️ Назад', 'callback_data': 'start'}]]}
        smart_send_message(chat_id, text, keyboard)
        return
    
    cursor.execute(
        "INSERT INTO t_p39739760_garbage_bot_service.courier_applications (telegram_id, status) VALUES (%s, %s)",
        (telegram_id, 'pending')
    )
    conn.commit()
    cursor.close()
    
    text = (
        "✅ Заявка на роль курьера отправлена!\n\n"
        "Администратор рассмотрит её в ближайшее время."
    )
    keyboard = {'inline_keyboard': [[{'text': '⬅️ Назад', 'callback_data': 'start'}]]}
    smart_send_message(chat_id, text, keyboard)

def handle_client_menu(chat_id: int) -> None:
    text = "👤 <b>Меню клиента</b>\n\nВыберите действие:"
    smart_send_message(chat_id, text, get_client_menu_keyboard())

def handle_courier_available_orders(chat_id: int, telegram_id: int, conn) -> None:
    cursor = conn.cursor()
    cursor.execute(
        "SELECT id, address, description, price, detailed_status FROM t_p39739760_garbage_bot_service.orders WHERE status = %s ORDER BY created_at DESC LIMIT 10",
        ('pending',)
    )
    orders = cursor.fetchall()
    cursor.close()
    
    if not orders:
        text = "📦 <b>Доступные заказы</b>\n\nНет доступных заказов"
        keyboard = {'inline_keyboard': [[{'text': '⬅️ Назад', 'callback_data': 'start'}]]}
        smart_send_message(chat_id, text, keyboard)
        return
    
    text = "📦 <b>Доступные заказы</b>\n\n"
    keyboard_buttons = []
    
    for order in orders:
        order_id, address, description, price, detailed_status = order
        status_text = ORDER_STATUSES.get(detailed_status, detailed_status)
        text += f"🆔 Заказ #{order_id}\n"
        text += f"📍 {address}\n"
        text += f"📝 {description}\n"
        text += f"💰 {price} ₽\n"
        text += f"Статус: {status_text}\n\n"
        keyboard_buttons.append([{'text': f'✅ Принять #{order_id}', 'callback_data': f'accept_order_{order_id}'}])
    
    keyboard_buttons.append([{'text': '⬅️ Назад', 'callback_data': 'start'}])
    send_message(chat_id, text, {'inline_keyboard': keyboard_buttons})

def handle_accept_order(chat_id: int, telegram_id: int, order_id: int, conn) -> None:
    cursor = conn.cursor()
    
    role = check_user_role(telegram_id, conn)
    if role != 'courier':
        send_message(chat_id, "❌ Только курьеры могут принимать заказы")
        cursor.close()
        return
    
    cursor.execute("SELECT status, address, description, price, client_id FROM t_p39739760_garbage_bot_service.orders WHERE id = %s", (order_id,))
    order = cursor.fetchone()
    
    if not order or order[0] != 'pending':
        send_message(chat_id, "❌ Заказ недоступен или уже принят")
        cursor.close()
        return
    
    status, address, description, price, client_id = order
    
    cursor.execute(
        "UPDATE t_p39739760_garbage_bot_service.orders SET status = %s, courier_id = %s, accepted_at = %s, detailed_status = %s WHERE id = %s",
        ('accepted', telegram_id, datetime.now(), 'courier_on_way', order_id)
    )
    conn.commit()
    
    cursor.execute("SELECT first_name FROM t_p39739760_garbage_bot_service.users WHERE telegram_id = %s", (telegram_id,))
    courier = cursor.fetchone()
    courier_name = courier[0] if courier else "Курьер"
    
    cursor.close()
    
    keyboard = {
        'inline_keyboard': [
            [{'text': '💬 Написать курьеру', 'callback_data': f'client_chat_{order_id}'}]
        ]
    }
    send_message(client_id, f"🚗 Курьер {courier_name} едет к вам", keyboard)
    
    text = f"✅ <b>Заказ #{order_id} принят!</b>\n\n"
    text += f"📍 Адрес: {address}\n"
    text += f"📝 Описание: {description}\n"
    text += f"💰 Сумма: {price} ₽\n\n"
    text += f"Текущий статус: 🚗 <b>Еду к заказу</b>"
    
    keyboard = {
        'inline_keyboard': [
            [{'text': '🛠 Начать работу', 'callback_data': f'start_work_{order_id}'}],
            [{'text': '💬 Чат с клиентом', 'callback_data': f'courier_chat_{order_id}'}],
            [{'text': '⬅️ Назад', 'callback_data': 'start'}]
        ]
    }
    smart_send_message(chat_id, text, keyboard)

def handle_courier_current_orders(chat_id: int, telegram_id: int, conn) -> None:
    cursor = conn.cursor()
    cursor.execute(
        "SELECT id, address, description, price, detailed_status FROM t_p39739760_garbage_bot_service.orders WHERE courier_id = %s AND status = %s ORDER BY accepted_at DESC",
        (telegram_id, 'accepted')
    )
    orders = cursor.fetchall()
    cursor.close()
    
    if not orders:
        text = "🚚 <b>Текущие заказы</b>\n\nНет текущих заказов"
        keyboard = {'inline_keyboard': [[{'text': '⬅️ Назад', 'callback_data': 'start'}]]}
        smart_send_message(chat_id, text, keyboard)
        return
    
    text = "🚚 <b>Текущие заказы</b>\n\n"
    keyboard_buttons = []
    
    for order in orders:
        order_id, address, description, price, detailed_status = order
        status_text = ORDER_STATUSES.get(detailed_status, detailed_status)
        text += f"🆔 Заказ #{order_id}\n"
        text += f"📍 {address}\n"
        text += f"📝 {description}\n"
        text += f"💰 {price} ₽\n"
        text += f"Статус: {status_text}\n\n"
        
        order_buttons = []
        if detailed_status == 'courier_on_way':
            order_buttons.append({'text': f'🛠 Начать работу', 'callback_data': f'start_work_{order_id}'})
        elif detailed_status == 'courier_working':
            order_buttons.append({'text': f'✅ Завершить', 'callback_data': f'complete_order_{order_id}'})
        
        order_buttons.append({'text': f'💬 Чат', 'callback_data': f'courier_chat_{order_id}'})
        keyboard_buttons.append(order_buttons)
    
    keyboard_buttons.append([{'text': '⬅️ Назад', 'callback_data': 'start'}])
    send_message(chat_id, text, {'inline_keyboard': keyboard_buttons})

def handle_start_work(chat_id: int, telegram_id: int, order_id: int, conn) -> None:
    cursor = conn.cursor()
    
    cursor.execute(
        "SELECT courier_id, address, description, price, client_id FROM t_p39739760_garbage_bot_service.orders WHERE id = %s",
        (order_id,)
    )
    order = cursor.fetchone()
    
    if not order or order[0] != telegram_id:
        cursor.close()
        send_message(chat_id, "❌ Заказ не найден или не принадлежит вам")
        return
    
    courier_id, address, description, price, client_id = order
    
    cursor.execute(
        "UPDATE t_p39739760_garbage_bot_service.orders SET detailed_status = %s WHERE id = %s AND courier_id = %s",
        ('courier_working', order_id, telegram_id)
    )
    conn.commit()
    
    cursor.execute("SELECT first_name FROM t_p39739760_garbage_bot_service.users WHERE telegram_id = %s", (telegram_id,))
    courier = cursor.fetchone()
    courier_name = courier[0] if courier else "Курьер"
    
    cursor.close()
    
    send_message(client_id, f"🛠 {courier_name} начал работу")
    
    text = f"🛠 <b>Работа над заказом #{order_id} начата!</b>\n\n"
    text += f"📍 Адрес: {address}\n"
    text += f"📝 Описание: {description}\n"
    text += f"💰 Сумма: {price} ₽\n\n"
    text += f"Текущий статус: 🛠 <b>В работе</b>"
    
    keyboard = {
        'inline_keyboard': [
            [{'text': '✅ Завершить заказ', 'callback_data': f'complete_order_{order_id}'}],
            [{'text': '💬 Чат с клиентом', 'callback_data': f'courier_chat_{order_id}'}],
            [{'text': '⬅️ Назад', 'callback_data': 'courier_current'}]
        ]
    }
    smart_send_message(chat_id, text, keyboard)

def handle_complete_order(chat_id: int, telegram_id: int, order_id: int, conn) -> None:
    cursor = conn.cursor()
    
    cursor.execute("SELECT courier_id, price FROM t_p39739760_garbage_bot_service.orders WHERE id = %s", (order_id,))
    order = cursor.fetchone()
    
    if not order or order[0] != telegram_id:
        send_message(chat_id, "❌ Заказ не найден")
        cursor.close()
        return
    
    price = order[1]
    
    cursor.execute(
        "UPDATE t_p39739760_garbage_bot_service.orders SET status = %s, completed_at = %s, detailed_status = %s WHERE id = %s",
        ('completed', datetime.now(), 'completed', order_id)
    )
    
    cursor.execute(
        f"INSERT INTO {SCHEMA}.courier_stats (courier_id, total_orders, total_earnings) "
        "VALUES (%s, 1, %s) "
        f"ON CONFLICT (courier_id) DO UPDATE SET "
        f"total_orders = {SCHEMA}.courier_stats.total_orders + 1, "
        f"total_earnings = {SCHEMA}.courier_stats.total_earnings + %s, "
        "updated_at = %s",
        (telegram_id, price, price, datetime.now())
    )
    
    cursor.execute("SELECT client_id FROM t_p39739760_garbage_bot_service.orders WHERE id = %s", (order_id,))
    client = cursor.fetchone()
    client_id = client[0] if client else None
    
    cursor.execute("DELETE FROM t_p39739760_garbage_bot_service.chat_sessions WHERE telegram_id IN (%s, %s)", (telegram_id, client_id))
    
    conn.commit()
    cursor.close()
    
    if client_id:
        keyboard = {
            'inline_keyboard': [
                [{'text': '⭐ Оценить курьера', 'callback_data': f'rate_order_{order_id}'}]
            ]
        }
        send_message(client_id, f"✅ Заказ завершен", keyboard)
    
    text = f"✅ Заказ #{order_id} завершён!\n\n💰 Заработано: {price} ₽"
    keyboard = {
        'inline_keyboard': [
            [{'text': '💰 Статистика', 'callback_data': 'courier_stats'}],
            [{'text': '⬅️ Назад', 'callback_data': 'start'}]
        ]
    }
    smart_send_message(chat_id, text, keyboard)

def handle_courier_stats(chat_id: int, telegram_id: int, conn) -> None:
    cursor = conn.cursor()
    cursor.execute(
        "SELECT total_orders, total_earnings FROM t_p39739760_garbage_bot_service.courier_stats WHERE courier_id = %s",
        (telegram_id,)
    )
    stats = cursor.fetchone()
    
    cursor.execute(f"SELECT AVG(rating) FROM {SCHEMA}.ratings WHERE courier_id = %s", (telegram_id,))
    avg_rating = cursor.fetchone()
    cursor.close()
    
    if not stats:
        total_orders = 0
        total_earnings = 0
    else:
        total_orders = stats[0]
        total_earnings = stats[1]
    
    rating = round(avg_rating[0], 1) if avg_rating[0] else 0.0
    avg_check = round(total_earnings / total_orders) if total_orders > 0 else 0
    
    text = (
        "💰 <b>Финансовая статистика</b>\n\n"
        f"📦 Всего заказов: {total_orders}\n"
        f"💵 Заработано: {total_earnings} ₽\n"
        f"💳 Средний чек: {avg_check} ₽\n"
        f"⭐ Средний рейтинг: {rating}\n"
    )
    
    keyboard = {
        'inline_keyboard': [
            [{'text': '💵 Вывод средств', 'callback_data': 'courier_withdraw'}],
            [{'text': '⬅️ Назад', 'callback_data': 'start'}]
        ]
    }
    smart_send_message(chat_id, text, keyboard)



def handle_client_new_order(chat_id: int) -> None:
    text = (
        "🗑 <b>Выберите количество пакетов</b>\n\n"
        f"💰 Цена: {BAG_PRICE} ₽ за пакет (35л)\n\n"
        "Выберите количество или введите своё:"
    )
    
    keyboard_buttons = []
    for i in range(1, MAX_BAGS_QUICK_SELECT + 1):
        total = BAG_PRICE * i
        keyboard_buttons.append([{'text': f'{i} пакет - {total} ₽', 'callback_data': f'select_bags_{i}'}])
    
    keyboard_buttons.append([{'text': '✏️ Ввести своё количество', 'callback_data': 'custom_bags'}])
    keyboard_buttons.append([{'text': '⬅️ Назад', 'callback_data': 'client_menu'}])
    
    keyboard = {'inline_keyboard': keyboard_buttons}
    smart_send_message(chat_id, text, keyboard)

def handle_select_bags(chat_id: int, telegram_id: int, bag_count: int, conn) -> None:
    from datetime import timedelta
    
    cursor = conn.cursor()
    
    cursor.execute(
        f"SELECT id, type, bags_used_today, last_order_date FROM {SCHEMA}.subscriptions "
        "WHERE client_id = %s AND is_active = true AND end_date >= CURRENT_DATE "
        "ORDER BY end_date DESC LIMIT 1",
        (telegram_id,)
    )
    subscription = cursor.fetchone()
    
    is_subscription_order = False
    total_price = BAG_PRICE * bag_count
    
    if subscription and bag_count <= 2:
        sub_id, sub_type, bags_used, last_date = subscription
        today = datetime.now().date()
        
        can_use_sub = False
        if sub_type == 'daily':
            can_use_sub = True
        elif sub_type == 'alternate_day':
            if last_date is None or (today - last_date).days >= 2:
                can_use_sub = True
        
        if can_use_sub:
            if last_date != today:
                bags_used = 0
            
            if bags_used + bag_count <= 2:
                is_subscription_order = True
                total_price = 0
                
                cursor.execute(
                    f"UPDATE {SCHEMA}.subscriptions SET bags_used_today = %s, last_order_date = %s WHERE id = %s",
                    (bags_used + bag_count, today, sub_id)
                )
                conn.commit()
    
    cursor.execute(
        f"INSERT INTO {SCHEMA}.order_draft (telegram_id, state, order_data) "
        "VALUES (%s, %s, %s) "
        "ON CONFLICT (telegram_id) DO UPDATE SET state = %s, order_data = %s, updated_at = CURRENT_TIMESTAMP",
        (telegram_id, 'waiting_address', json.dumps({'bag_count': bag_count, 'is_subscription': is_subscription_order, 'price': total_price}),
         'waiting_address', json.dumps({'bag_count': bag_count, 'is_subscription': is_subscription_order, 'price': total_price}))
    )
    conn.commit()
    cursor.close()
    
    if is_subscription_order:
        text = (
            f"✅ <b>По подписке: {bag_count} пакетов</b>\n\n"
            f"💰 Стоимость: 0 ₽ (включено в подписку)\n\n"
            "📍 <b>Отправьте адрес доставки:</b>\n\n"
            "<b>Пример:</b>\n"
            "ул. Ленина, д. 45, кв. 12"
        )
    else:
        text = (
            f"📦 <b>Выбрано пакетов: {bag_count}</b>\n\n"
            f"💰 Стоимость: {total_price} ₽\n"
            f"({bag_count} × {BAG_PRICE}₽)\n\n"
            "📍 <b>Отправьте адрес доставки:</b>\n\n"
            "<b>Пример:</b>\n"
            "ул. Ленина, д. 45, кв. 12"
        )
    
    keyboard = {'inline_keyboard': [[{'text': '❌ Отмена', 'callback_data': 'client_menu'}]]}
    smart_send_message(chat_id, text, keyboard)

def handle_custom_bags_prompt(chat_id: int, telegram_id: int, conn) -> None:
    cursor = conn.cursor()
    cursor.execute(
        f"INSERT INTO {SCHEMA}.order_draft (telegram_id, state) "
        "VALUES (%s, %s) "
        "ON CONFLICT (telegram_id) DO UPDATE SET state = %s, updated_at = CURRENT_TIMESTAMP",
        (telegram_id, 'waiting_custom_bags', 'waiting_custom_bags')
    )
    conn.commit()
    cursor.close()
    
    text = (
        "✏️ <b>Введите количество пакетов</b>\n\n"
        "Напишите число от 1 до 100:"
    )
    
    keyboard = {'inline_keyboard': [[{'text': '❌ Отмена', 'callback_data': 'client_menu'}]]}
    smart_send_message(chat_id, text, keyboard)

def handle_client_active_orders(chat_id: int, telegram_id: int, conn) -> None:
    cursor = conn.cursor()
    cursor.execute(
        "SELECT o.id, o.address, o.description, o.price, o.detailed_status, u.first_name, o.courier_id, o.bag_count "
        "FROM t_p39739760_garbage_bot_service.orders o "
        "LEFT JOIN t_p39739760_garbage_bot_service.users u ON o.courier_id = u.telegram_id "
        "WHERE o.client_id = %s AND o.status IN (%s, %s) "
        "ORDER BY o.created_at DESC",
        (telegram_id, 'pending', 'accepted')
    )
    orders = cursor.fetchall()
    cursor.close()
    
    if not orders:
        text = "📦 <b>Активные заказы</b>\n\nНет активных заказов"
        keyboard = {'inline_keyboard': [[{'text': '⬅️ Назад', 'callback_data': 'client_menu'}]]}
    else:
        text = "📦 <b>Активные заказы</b>\n\n"
        keyboard_buttons = []
        
        for order in orders:
            order_id, address, description, price, detailed_status, courier_name, courier_id, bag_count = order
            status_text = ORDER_STATUSES.get(detailed_status, detailed_status)
            text += f"🆔 #{order_id}\n"
            text += f"📍 {address}\n"
            text += f"📦 {bag_count or 1} пакетов\n"
            text += f"💰 {price} ₽\n"
            text += f"Статус: {status_text}\n"
            if courier_name:
                text += f"Курьер: {courier_name}\n"
            text += "\n"
            
            order_buttons = []
            if courier_id:
                order_buttons.append({'text': f'💬 Чат', 'callback_data': f'client_chat_{order_id}'})
            
            if detailed_status == 'searching_courier':
                order_buttons.append({'text': f'❌ Отменить', 'callback_data': f'cancel_order_{order_id}'})
            
            if order_buttons:
                keyboard_buttons.append(order_buttons)
        
        keyboard_buttons.append([{'text': '⬅️ Назад', 'callback_data': 'client_menu'}])
        keyboard = {'inline_keyboard': keyboard_buttons}
    
    smart_send_message(chat_id, text, keyboard)

def handle_cancel_order(chat_id: int, telegram_id: int, order_id: int, conn) -> None:
    cursor = conn.cursor()
    
    role = check_user_role(telegram_id, conn)
    
    cursor.execute(
        "SELECT client_id, status, detailed_status FROM t_p39739760_garbage_bot_service.orders WHERE id = %s",
        (order_id,)
    )
    order = cursor.fetchone()
    
    if not order:
        cursor.close()
        send_message(chat_id, "❌ Заказ не найден")
        return
    
    client_id, status, detailed_status = order
    
    if client_id != telegram_id and role not in ['admin', 'operator']:
        cursor.close()
        send_message(chat_id, "❌ Это не ваш заказ")
        return
    
    if status != 'pending' or detailed_status != 'searching_courier':
        cursor.close()
        send_message(chat_id, "❌ Заказ уже принят курьером и не может быть отменен")
        return
    
    cursor.execute(
        "UPDATE t_p39739760_garbage_bot_service.orders SET status = %s, detailed_status = %s WHERE id = %s",
        ('cancelled', 'cancelled', order_id)
    )
    
    cursor.execute("DELETE FROM t_p39739760_garbage_bot_service.chat_sessions WHERE order_id = %s", (order_id,))
    
    conn.commit()
    cursor.close()
    
    text = f"❌ <b>Заказ #{order_id} отменен</b>\n\nВы можете создать новый заказ в любое время"
    keyboard = {
        'inline_keyboard': [
            [{'text': '➕ Новый заказ', 'callback_data': 'client_new_order'}],
            [{'text': '⬅️ Назад', 'callback_data': 'client_menu'}]
        ]
    }
    smart_send_message(chat_id, text, keyboard)

def handle_operator_active_orders(chat_id: int, conn) -> None:
    cursor = conn.cursor()
    cursor.execute(
        "SELECT o.id, o.address, o.description, o.price, o.detailed_status, "
        "u1.first_name as client_name, u2.first_name as courier_name "
        "FROM t_p39739760_garbage_bot_service.orders o "
        "JOIN t_p39739760_garbage_bot_service.users u1 ON o.client_id = u1.telegram_id "
        "LEFT JOIN t_p39739760_garbage_bot_service.users u2 ON o.courier_id = u2.telegram_id "
        "WHERE o.status IN (%s, %s) "
        "ORDER BY o.created_at DESC LIMIT 20",
        ('pending', 'accepted')
    )
    orders = cursor.fetchall()
    cursor.close()
    
    if not orders:
        text = "📞 <b>Активные заказы</b>\n\nНет активных заказов"
        keyboard = {'inline_keyboard': [[{'text': '⬅️ Назад', 'callback_data': 'start'}]]}
    else:
        text = "📞 <b>Активные заказы</b>\n\n"
        keyboard_buttons = []
        
        for order in orders:
            order_id, address, description, price, detailed_status, client_name, courier_name = order
            status_text = ORDER_STATUSES.get(detailed_status, detailed_status)
            text += f"🆔 #{order_id} | {status_text}\n"
            text += f"Клиент: {client_name}\n"
            if courier_name:
                text += f"Курьер: {courier_name}\n"
            text += f"💰 {price} ₽\n\n"
            
            keyboard_buttons.append([
                {'text': f'💬 Чат #{order_id}', 'callback_data': f'operator_chat_{order_id}'},
                {'text': f'📝 Статус #{order_id}', 'callback_data': f'operator_status_{order_id}'}
            ])
        
        keyboard_buttons.append([{'text': '⬅️ Назад', 'callback_data': 'start'}])
        keyboard = {'inline_keyboard': keyboard_buttons}
    
    smart_send_message(chat_id, text, keyboard)

def handle_operator_change_status(chat_id: int, order_id: int, conn) -> None:
    text = f"📝 Изменить статус заказа #{order_id}"
    
    keyboard = {
        'inline_keyboard': [
            [{'text': '🔍 В поиске курьера', 'callback_data': f'set_status_{order_id}_searching_courier'}],
            [{'text': '🚗 Курьер едет', 'callback_data': f'set_status_{order_id}_courier_on_way'}],
            [{'text': '🛠 Курьер выполняет заказ', 'callback_data': f'set_status_{order_id}_courier_working'}],
            [{'text': '✅ Завершён', 'callback_data': f'set_status_{order_id}_completed'}],
            [{'text': '❌ Отменён', 'callback_data': f'set_status_{order_id}_cancelled'}],
            [{'text': '⬅️ Назад', 'callback_data': 'operator_active_orders'}]
        ]
    }
    
    smart_send_message(chat_id, text, keyboard)

def handle_set_order_status(chat_id: int, order_id: int, new_status: str, conn) -> None:
    cursor = conn.cursor()
    
    status_mapping = {
        'completed': 'completed',
        'cancelled': 'cancelled',
        'searching_courier': 'pending',
        'courier_on_way': 'accepted',
        'courier_working': 'accepted'
    }
    
    main_status = status_mapping.get(new_status, 'pending')
    
    cursor.execute(
        "UPDATE t_p39739760_garbage_bot_service.orders SET detailed_status = %s, status = %s WHERE id = %s",
        (new_status, main_status, order_id)
    )
    conn.commit()
    cursor.close()
    
    status_text = ORDER_STATUSES.get(new_status, new_status)
    text = f"✅ Статус заказа #{order_id} изменён на: {status_text}"
    
    keyboard = {
        'inline_keyboard': [
            [{'text': '📞 Активные заказы', 'callback_data': 'operator_active_orders'}],
            [{'text': '⬅️ Назад', 'callback_data': 'start'}]
        ]
    }
    
    smart_send_message(chat_id, text, keyboard)

def handle_admin_subscriptions(chat_id: int, conn) -> None:
    cursor = conn.cursor()
    
    cursor.execute(f"SELECT COUNT(*) FROM {SCHEMA}.subscriptions WHERE is_active = true AND end_date >= CURRENT_DATE")
    active_count = cursor.fetchone()[0]
    
    cursor.execute(f"SELECT SUM(price) FROM {SCHEMA}.subscriptions WHERE is_active = true")
    total_revenue = cursor.fetchone()[0] or 0
    
    cursor.execute(
        f"SELECT s.id, u.first_name, u.telegram_id, s.type, s.end_date, s.bags_used_today "
        f"FROM {SCHEMA}.subscriptions s "
        f"JOIN {SCHEMA}.users u ON s.client_id = u.telegram_id "
        "WHERE s.is_active = true AND s.end_date >= CURRENT_DATE "
        "ORDER BY s.end_date ASC LIMIT 20"
    )
    subscriptions = cursor.fetchall()
    cursor.close()
    
    text = f"⭐ <b>Управление подписками</b>\n\n📊 Активных: {active_count}\n💰 Доход: {total_revenue}₽\n\n"
    keyboard_buttons = []
    
    if subscriptions:
        text += "<b>Активные подписки:</b>\n\n"
        for sub in subscriptions:
            sub_id, name, tg_id, sub_type, end_date, bags_used = sub
            sub_name = "Ежедневно" if sub_type == 'daily' else "Через день"
            days_left = (end_date - datetime.now().date()).days
            text += f"👤 {name} (ID: {tg_id})\n"
            text += f"📅 {sub_name}, до {end_date.strftime('%d.%m')}, {days_left}д\n"
            text += f"📦 Использовано: {bags_used}/2\n\n"
            
            keyboard_buttons.append([
                {'text': f'❌ Отменить {name}', 'callback_data': f'cancel_sub_{sub_id}'}
            ])
    else:
        text += "Нет активных подписок"
    
    keyboard_buttons.append([{'text': '➕ Выдать подписку', 'callback_data': 'admin_add_subscription'}])
    keyboard_buttons.append([{'text': '⬅️ Назад', 'callback_data': 'admin_panel'}])
    keyboard = {'inline_keyboard': keyboard_buttons}
    smart_send_message(chat_id, text, keyboard)

def handle_cancel_subscription(chat_id: int, sub_id: int, conn) -> None:
    cursor = conn.cursor()
    cursor.execute(f"SELECT client_id FROM {SCHEMA}.subscriptions WHERE id = %s", (sub_id,))
    client = cursor.fetchone()
    
    cursor.execute(f"UPDATE {SCHEMA}.subscriptions SET is_active = false WHERE id = %s", (sub_id,))
    conn.commit()
    cursor.close()
    
    if client:
        send_message(client[0], "❌ Ваша подписка отменена администратором")
    
    send_message(chat_id, "✅ Подписка отменена")
    handle_admin_subscriptions(chat_id, conn)

def handle_admin_add_subscription_prompt(chat_id: int) -> None:
    text = (
        "➕ <b>Выдать подписку</b>\n\n"
        "Отправьте данные в формате:\n"
        "<code>sub_add USER_ID TYPE</code>\n\n"
        "<b>TYPE:</b>\n"
        "• daily - ежедневно (2499₽)\n"
        "• alternate - через день (1399₽)\n\n"
        "<b>Пример:</b>\n"
        "<code>sub_add 123456789 daily</code>"
    )
    keyboard = {'inline_keyboard': [[{'text': '⬅️ Назад', 'callback_data': 'admin_subscriptions'}]]
    }
    smart_send_message(chat_id, text, keyboard)

def handle_admin_grant_subscription(chat_id: int, client_id: int, sub_type: str, conn) -> None:
    from datetime import timedelta
    
    if sub_type not in ['daily', 'alternate_day']:
        send_message(chat_id, "❌ Неверный тип подписки. Используйте: daily или alternate")
        return
    
    cursor = conn.cursor()
    cursor.execute(f"SELECT telegram_id FROM {SCHEMA}.users WHERE telegram_id = %s", (client_id,))
    user_exists = cursor.fetchone()
    
    if not user_exists:
        cursor.close()
        send_message(chat_id, "❌ Пользователь не найден")
        return
    
    price = 2499 if sub_type == 'daily' else 1399
    sub_name = "Ежедневно" if sub_type == 'daily' else "Через день"
    start_date = datetime.now().date()
    end_date = start_date + timedelta(days=30)
    
    cursor.execute(
        f"INSERT INTO {SCHEMA}.subscriptions (client_id, type, price, start_date, end_date, is_active) "
        "VALUES (%s, %s, %s, %s, %s, %s)",
        (client_id, sub_type, price, start_date, end_date, True)
    )
    conn.commit()
    cursor.close()
    
    text = (
        f"✅ <b>Подписка '{sub_name}' активирована!</b>\n\n"
        f"📅 Действует до: {end_date.strftime('%d.%m.%Y')}\n\n"
        "Теперь вы можете заказывать вывоз до 2 пакетов без доплаты!"
    )
    keyboard = {
        'inline_keyboard': [
            [{'text': '📦 Создать заказ', 'callback_data': 'client_new_order'}],
            [{'text': '⬅️ В меню', 'callback_data': 'client_menu'}]
        ]
    }
    send_message(client_id, text, keyboard)
    send_message(chat_id, f"✅ Подписка '{sub_name}' выдана пользователю {client_id}")

def handle_admin_panel(chat_id: int, conn) -> None:
    text = "👑 <b>Админ-панель</b>\n\nВыберите действие:"
    
    keyboard = {
        'inline_keyboard': [
            [{'text': '👔 Управление курьерами', 'callback_data': 'admin_couriers'}],
            [{'text': '👥 Управление операторами', 'callback_data': 'admin_operators'}],
            [{'text': '⭐ Управление подписками', 'callback_data': 'admin_subscriptions'}],
            [{'text': '📊 Статистика сервиса', 'callback_data': 'admin_stats'}],
            [{'text': '📦 Все заказы', 'callback_data': 'admin_all_orders'}],
            [{'text': '⬅️ Назад', 'callback_data': 'start'}]
        ]
    }
    
    smart_send_message(chat_id, text, keyboard)

def handle_admin_couriers_menu(chat_id: int, conn) -> None:
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM t_p39739760_garbage_bot_service.users WHERE role = %s", ('courier',))
    total_couriers = cursor.fetchone()[0]
    
    cursor.execute(
        "SELECT COUNT(*) FROM t_p39739760_garbage_bot_service.courier_applications WHERE status = %s",
        ('pending',)
    )
    pending_applications = cursor.fetchone()[0]
    
    cursor.close()
    
    text = (
        "👔 <b>Управление курьерами</b>\n\n"
        f"Всего курьеров: {total_couriers}\n"
        f"Заявок на рассмотрении: {pending_applications}"
    )
    
    keyboard = {
        'inline_keyboard': [
            [{'text': '📝 Заявки на роль курьера', 'callback_data': 'admin_courier_applications'}],
            [{'text': '👔 Список всех курьеров', 'callback_data': 'admin_couriers_list'}],
            [{'text': '🚫 Удалить курьера', 'callback_data': 'admin_remove_courier'}],
            [{'text': '⬅️ Назад', 'callback_data': 'admin_panel'}]
        ]
    }
    
    smart_send_message(chat_id, text, keyboard)

def handle_admin_operators_menu(chat_id: int, conn) -> None:
    cursor = conn.cursor()
    
    cursor.execute(f"SELECT COUNT(*) FROM {SCHEMA}.operator_users")
    total_operators = cursor.fetchone()[0]
    
    cursor.close()
    
    text = (
        "👥 <b>Управление операторами</b>\n\n"
        f"Всего операторов: {total_operators}"
    )
    
    keyboard = {
        'inline_keyboard': [
            [{'text': '➕ Добавить оператора', 'callback_data': 'admin_add_operator'}],
            [{'text': '👥 Список всех операторов', 'callback_data': 'admin_operators_list'}],
            [{'text': '🚫 Удалить оператора', 'callback_data': 'admin_remove_operator'}],
            [{'text': '⬅️ Назад', 'callback_data': 'admin_panel'}]
        ]
    }
    
    smart_send_message(chat_id, text, keyboard)

def handle_admin_add_operator(chat_id: int) -> None:
    text = (
        "➕ <b>Добавить оператора</b>\n\n"
        "Отправьте Telegram ID пользователя, которого хотите назначить оператором.\n\n"
        "Формат: <code>operator_add ID</code>\n\n"
        "<b>Пример:</b>\n"
        "<code>operator_add 123456789</code>"
    )
    keyboard = {'inline_keyboard': [[{'text': '⬅️ Назад', 'callback_data': 'admin_operators'}]]}
    smart_send_message(chat_id, text, keyboard)

def handle_admin_stats(chat_id: int, conn) -> None:
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM t_p39739760_garbage_bot_service.users WHERE role = %s", ('client',))
    total_clients = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM t_p39739760_garbage_bot_service.users WHERE role = %s", ('courier',))
    total_couriers = cursor.fetchone()[0]
    
    cursor.execute(f"SELECT COUNT(*) FROM {SCHEMA}.operator_users")
    total_operators = cursor.fetchone()[0]
    
    cursor.execute(f"SELECT COUNT(*) FROM {SCHEMA}.orders")
    total_orders = cursor.fetchone()[0]
    
    cursor.execute(f"SELECT COUNT(*) FROM {SCHEMA}.orders WHERE status = %s", ('completed',))
    completed_orders = cursor.fetchone()[0]
    
    cursor.execute(f"SELECT SUM(price) FROM {SCHEMA}.orders WHERE status = %s", ('completed',))
    total_revenue = cursor.fetchone()[0] or 0
    
    cursor.execute(
        f"SELECT AVG(price) FROM {SCHEMA}.orders WHERE status = %s",
        ('completed',)
    )
    avg_order = cursor.fetchone()[0] or 0
    
    cursor.close()
    
    text = (
        "📊 <b>Статистика сервиса</b>\n\n"
        f"👥 Пользователей:\n"
        f"  • Клиентов: {total_clients}\n"
        f"  • Курьеров: {total_couriers}\n"
        f"  • Операторов: {total_operators}\n\n"
        f"📦 Заказов:\n"
        f"  • Всего: {total_orders}\n"
        f"  • Завершено: {completed_orders}\n\n"
        f"💰 Финансы:\n"
        f"  • Общая выручка: {int(total_revenue)} ₽\n"
        f"  • Средний чек: {int(avg_order)} ₽"
    )
    
    keyboard = {'inline_keyboard': [[{'text': '⬅️ Назад', 'callback_data': 'admin_panel'}]]}
    smart_send_message(chat_id, text, keyboard)

def handle_admin_couriers_list(chat_id: int, conn) -> None:
    cursor = conn.cursor()
    cursor.execute(
        f"SELECT u.telegram_id, u.username, u.first_name, cs.total_orders, cs.total_earnings "
        f"FROM {SCHEMA}.users u "
        f"LEFT JOIN {SCHEMA}.courier_stats cs ON u.telegram_id = cs.courier_id "
        "WHERE u.role = %s "
        "ORDER BY cs.total_orders DESC NULLS LAST LIMIT 20",
        ('courier',)
    )
    couriers = cursor.fetchall()
    cursor.close()
    
    if not couriers:
        text = "👔 <b>Список курьеров</b>\n\nНет зарегистрированных курьеров"
    else:
        text = "👔 <b>Список курьеров</b>\n\n"
        for courier in couriers:
            telegram_id, username, first_name, total_orders, total_earnings = courier
            orders = total_orders or 0
            earnings = total_earnings or 0
            text += f"👤 {first_name} (@{username or 'нет'})\n"
            text += f"ID: {telegram_id}\n"
            text += f"Заказов: {orders} | Заработано: {earnings} ₽\n\n"
    
    keyboard = {'inline_keyboard': [[{'text': '⬅️ Назад', 'callback_data': 'admin_couriers'}]]}
    smart_send_message(chat_id, text, keyboard)

def handle_admin_operators_list(chat_id: int, conn) -> None:
    cursor = conn.cursor()
    cursor.execute(
        f"SELECT u.telegram_id, u.username, u.first_name, ou.created_at "
        f"FROM {SCHEMA}.operator_users ou "
        "JOIN t_p39739760_garbage_bot_service.users u ON ou.telegram_id = u.telegram_id "
        "ORDER BY ou.created_at DESC"
    )
    operators = cursor.fetchall()
    cursor.close()
    
    if not operators:
        text = "👥 <b>Список операторов</b>\n\nНет назначенных операторов"
    else:
        text = "👥 <b>Список операторов</b>\n\n"
        for operator in operators:
            telegram_id, username, first_name, created_at = operator
            date_str = created_at.strftime("%d.%m.%Y")
            text += f"👤 {first_name} (@{username or 'нет'})\n"
            text += f"ID: {telegram_id}\n"
            text += f"Назначен: {date_str}\n\n"
    
    keyboard = {'inline_keyboard': [[{'text': '⬅️ Назад', 'callback_data': 'admin_operators'}]]}
    smart_send_message(chat_id, text, keyboard)

def handle_admin_remove_courier_prompt(chat_id: int) -> None:
    text = (
        "🚫 <b>Удалить курьера</b>\n\n"
        "Отправьте Telegram ID курьера, которого хотите удалить.\n\n"
        "Формат: <code>courier_remove ID</code>\n\n"
        "<b>Пример:</b>\n"
        "<code>courier_remove 123456789</code>\n\n"
        "⚠️ Курьер потеряет доступ к заказам и будет переведён в статус клиента."
    )
    keyboard = {'inline_keyboard': [[{'text': '⬅️ Назад', 'callback_data': 'admin_couriers'}]]}
    smart_send_message(chat_id, text, keyboard)

def handle_admin_remove_operator_prompt(chat_id: int) -> None:
    text = (
        "🚫 <b>Удалить оператора</b>\n\n"
        "Отправьте Telegram ID оператора, которого хотите удалить.\n\n"
        "Формат: <code>operator_remove ID</code>\n\n"
        "<b>Пример:</b>\n"
        "<code>operator_remove 123456789</code>\n\n"
        "⚠️ Оператор потеряет доступ к панели управления заказами."
    )
    keyboard = {'inline_keyboard': [[{'text': '⬅️ Назад', 'callback_data': 'admin_operators'}]]}
    smart_send_message(chat_id, text, keyboard)

def handle_remove_courier(chat_id: int, courier_id: int, conn) -> None:
    cursor = conn.cursor()
    
    cursor.execute("SELECT role FROM t_p39739760_garbage_bot_service.users WHERE telegram_id = %s", (courier_id,))
    user = cursor.fetchone()
    
    if not user:
        cursor.close()
        send_message(chat_id, "❌ Пользователь не найден")
        return
    
    if user[0] != 'courier':
        cursor.close()
        send_message(chat_id, "❌ Этот пользователь не является курьером")
        return
    
    cursor.execute(
        "UPDATE t_p39739760_garbage_bot_service.users SET role = %s WHERE telegram_id = %s",
        ('client', courier_id)
    )
    conn.commit()
    cursor.close()
    
    send_message(courier_id, "❌ Вы больше не являетесь курьером. Статус изменён на клиента.")
    send_message(chat_id, f"✅ Курьер {courier_id} удалён и переведён в статус клиента")

def handle_remove_operator(chat_id: int, operator_id: int, conn) -> None:
    cursor = conn.cursor()
    
    cursor.execute("SELECT 1 FROM t_p39739760_garbage_bot_service.operator_users WHERE telegram_id = %s", (operator_id,))
    operator_exists = cursor.fetchone()
    
    if not operator_exists:
        cursor.close()
        send_message(chat_id, "❌ Этот пользователь не является оператором")
        return
    
    cursor.execute("DELETE FROM t_p39739760_garbage_bot_service.operator_users WHERE telegram_id = %s", (operator_id,))
    conn.commit()
    cursor.close()
    
    send_message(operator_id, "❌ Вы больше не являетесь оператором. Доступ к панели оператора отключён.")
    send_message(chat_id, f"✅ Оператор {operator_id} удалён")

def handle_add_operator(chat_id: int, admin_id: int, operator_id: int, conn) -> None:
    cursor = conn.cursor()
    
    cursor.execute("SELECT telegram_id FROM t_p39739760_garbage_bot_service.users WHERE telegram_id = %s", (operator_id,))
    user_exists = cursor.fetchone()
    
    if not user_exists:
        cursor.close()
        send_message(chat_id, "❌ Пользователь не найден. Попросите его сначала запустить бота через /start")
        return
    
    cursor.execute(
        "INSERT INTO t_p39739760_garbage_bot_service.operator_users (telegram_id, added_by) VALUES (%s, %s) ON CONFLICT (telegram_id) DO NOTHING",
        (operator_id, admin_id)
    )
    conn.commit()
    cursor.close()
    
    send_message(operator_id, "✅ Вы назначены оператором! Используйте /start для доступа к панели оператора.")
    send_message(chat_id, f"✅ Пользователь {operator_id} назначен оператором")

def handle_client_history(chat_id: int, telegram_id: int, conn) -> None:
    cursor = conn.cursor()
    cursor.execute(
        "SELECT o.id, o.address, o.description, o.price, o.detailed_status, u.first_name "
        "FROM t_p39739760_garbage_bot_service.orders o "
        "LEFT JOIN t_p39739760_garbage_bot_service.users u ON o.courier_id = u.telegram_id "
        "WHERE o.client_id = %s AND o.status = %s "
        "ORDER BY o.completed_at DESC LIMIT 10",
        (telegram_id, 'completed')
    )
    orders = cursor.fetchall()
    cursor.close()
    
    if not orders:
        text = "📊 <b>История заказов</b>\n\nНет завершённых заказов"
    else:
        text = "📊 <b>История заказов</b>\n\n"
        for order in orders:
            order_id, address, description, price, detailed_status, courier_name = order
            text += f"🆔 Заказ #{order_id}\n"
            text += f"📍 {address}\n"
            text += f"📝 {description}\n"
            text += f"💰 {price} ₽\n"
            if courier_name:
                text += f"Курьер: {courier_name}\n"
            text += "\n"
    
    keyboard = {'inline_keyboard': [[{'text': '⬅️ Назад', 'callback_data': 'client_menu'}]]}
    smart_send_message(chat_id, text, keyboard)

def handle_courier_history(chat_id: int, telegram_id: int, conn) -> None:
    cursor = conn.cursor()
    cursor.execute(
        f"SELECT id, address, description, price FROM {SCHEMA}.orders "
        "WHERE courier_id = %s AND status = %s "
        "ORDER BY completed_at DESC LIMIT 10",
        (telegram_id, 'completed')
    )
    orders = cursor.fetchall()
    cursor.close()
    
    if not orders:
        text = "📊 <b>История заказов</b>\n\nНет завершённых заказов"
    else:
        text = "📊 <b>История заказов</b>\n\n"
        for order in orders:
            order_id, address, description, price = order
            text += f"🆔 Заказ #{order_id}\n"
            text += f"📍 {address}\n"
            text += f"📝 {description}\n"
            text += f"💰 {price} ₽\n\n"
    
    keyboard = {'inline_keyboard': [[{'text': '⬅️ Назад', 'callback_data': 'start'}]]}
    smart_send_message(chat_id, text, keyboard)

def handle_client_payment(chat_id: int) -> None:
    text = (
        "💳 <b>Способ оплаты</b>\n\n"
        "Доступные способы оплаты:\n"
        "• 💳 Банковская карта\n"
        "• 💵 Наличные курьеру\n"
        "• 📱 СБП\n\n"
        "Способ оплаты выбирается при согласовании заказа с курьером."
    )
    keyboard = {'inline_keyboard': [[{'text': '⬅️ Назад', 'callback_data': 'client_menu'}]]}
    smart_send_message(chat_id, text, keyboard)

def handle_buy_subscription(chat_id: int, telegram_id: int, sub_type: str, conn) -> None:
    price = 2499 if sub_type == 'daily' else 1399
    sub_name = "Каждый день" if sub_type == 'daily' else "Через день"
    
    text = (
        f"⭐ <b>Заявка на подписку '{sub_name}'</b>\n\n"
        f"💰 Стоимость: {price}₽ в месяц\n\n"
        "📋 Что входит:\n"
        "• Вывоз до 2 пакетов в день\n"
        "• Без дополнительных платежей\n\n"
        "Свяжитесь с администратором для оплаты:"
    )
    
    cursor = conn.cursor()
    cursor.execute(f"SELECT u.telegram_id, u.first_name FROM {SCHEMA}.admin_users au JOIN {SCHEMA}.users u ON au.telegram_id = u.telegram_id LIMIT 1")
    admin = cursor.fetchone()
    cursor.close()
    
    keyboard = {
        'inline_keyboard': [
            [{'text': '👑 Связаться с администратором', 'url': f'https://t.me/user?id={admin[0]}' if admin else 'https://t.me/support'}],
            [{'text': '❌ Отменить', 'callback_data': 'client_subscription'}]
        ]
    }
    
    smart_send_message(chat_id, text, keyboard)

def handle_client_subscription(chat_id: int, telegram_id: int, conn) -> None:
    cursor = conn.cursor()
    cursor.execute(
        f"SELECT type, end_date, bags_used_today, last_order_date FROM {SCHEMA}.subscriptions "
        "WHERE client_id = %s AND is_active = true AND end_date >= CURRENT_DATE "
        "ORDER BY end_date DESC LIMIT 1",
        (telegram_id,)
    )
    subscription = cursor.fetchone()
    cursor.close()
    
    if subscription:
        sub_type, end_date, bags_used, last_date = subscription
        sub_name = "Через день" if sub_type == 'alternate_day' else "Ежедневно"
        days_left = (end_date - datetime.now().date()).days
        
        text = (
            f"✅ <b>Активна подписка: {sub_name}</b>\n\n"
            f"📅 До {end_date.strftime('%d.%m.%Y')} ({days_left} дней)\n"
            f"📦 Использовано сегодня: {bags_used}/2\n\n"
            "Подписка включает:\n"
            "• Вывоз до 2 пакетов в день\n"
            "• Без доплат за доставку\n"
        )
        keyboard = {'inline_keyboard': [[{'text': '⬅️ Назад', 'callback_data': 'client_menu'}]]}
    else:
        text = (
            "⭐ <b>Подписки на вывоз мусора</b>\n\n"
            "🔄 <b>Через день (1399₽/мес)</b>\n"
            "• До 2 пакетов через день\n"
            "• Без доплат\n"
            "• Экономия ~40%\n\n"
            "📅 <b>Каждый день (2499₽/мес)</b>\n"
            "• До 2 пакетов каждый день\n"
            "• Без доплат\n"
            "• Максимум удобства\n\n"
            "Выберите подходящий вариант:"
        )
        keyboard = {
            'inline_keyboard': [
                [{'text': '🔄 Через день - 1399₽', 'callback_data': 'buy_sub_alternate'}],
                [{'text': '📅 Каждый день - 2499₽', 'callback_data': 'buy_sub_daily'}],
                [{'text': '⬅️ Назад', 'callback_data': 'client_menu'}]
            ]
        }
    
    smart_send_message(chat_id, text, keyboard)

def handle_courier_withdraw(chat_id: int, telegram_id: int, conn) -> None:
    cursor = conn.cursor()
    cursor.execute(
        "SELECT total_earnings FROM t_p39739760_garbage_bot_service.courier_stats WHERE courier_id = %s",
        (telegram_id,)
    )
    stats = cursor.fetchone()
    cursor.close()
    
    balance = stats[0] if stats else 0
    
    text = (
        "💵 <b>Вывод средств</b>\n\n"
        f"Доступно для вывода: <b>{balance} ₽</b>\n\n"
        "Для вывода средств свяжитесь с администратором через кнопку ниже."
    )
    keyboard = {
        'inline_keyboard': [
            [{'text': '💬 Связаться с администратором', 'url': 'https://t.me/support'}],
            [{'text': '⬅️ Назад', 'callback_data': 'start'}]
        ]
    }
    smart_send_message(chat_id, text, keyboard)

def handle_operator_stats(chat_id: int, conn) -> None:
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM t_p39739760_garbage_bot_service.orders WHERE status = %s", ('pending',))
    pending = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM t_p39739760_garbage_bot_service.orders WHERE status = %s", ('accepted',))
    active = cursor.fetchone()[0]
    
    cursor.execute(
        "SELECT COUNT(*) FROM t_p39739760_garbage_bot_service.orders WHERE status = %s AND DATE(completed_at) = CURRENT_DATE",
        ('completed',)
    )
    today_completed = cursor.fetchone()[0]
    
    cursor.close()
    
    text = (
        "📊 <b>Статистика оператора</b>\n\n"
        f"🔍 Ожидают курьера: {pending}\n"
        f"🚚 В работе: {active}\n"
        f"✅ Завершено сегодня: {today_completed}"
    )
    
    keyboard = {'inline_keyboard': [[{'text': '⬅️ Назад', 'callback_data': 'start'}]]}
    smart_send_message(chat_id, text, keyboard)

def handle_operator_chats(chat_id: int, conn) -> None:
    cursor = conn.cursor()
    cursor.execute(
        "SELECT o.id, o.address, u1.first_name as client_name, u2.first_name as courier_name, "
        "(SELECT COUNT(*) FROM t_p39739760_garbage_bot_service.order_chat WHERE order_id = o.id AND is_archived = FALSE) as message_count, o.created_at, o.detailed_status "
        "FROM t_p39739760_garbage_bot_service.orders o "
        "JOIN t_p39739760_garbage_bot_service.users u1 ON o.client_id = u1.telegram_id "
        "LEFT JOIN t_p39739760_garbage_bot_service.users u2 ON o.courier_id = u2.telegram_id "
        "WHERE o.status NOT IN ('completed', 'cancelled') "
        "ORDER BY o.created_at DESC LIMIT 20"
    )
    orders = cursor.fetchall()
    cursor.close()
    
    if not orders:
        text = "💬 <b>Чаты заказов</b>\n\nНет активных заказов"
        keyboard = {'inline_keyboard': [
            [{'text': '🔍 Найти чат по номеру', 'callback_data': 'search_chat'}],
            [{'text': '⬅️ Назад', 'callback_data': 'start'}]
        ]}
    else:
        text = "💬 <b>Чаты заказов</b>\n\nВыберите заказ для просмотра чата:\n\n"
        keyboard_buttons = []
        
        for order in orders:
            order_id, address, client_name, courier_name, msg_count, created_at, detailed_status = order
            status_emoji = ORDER_STATUSES.get(detailed_status, '📦')
            text += f"🆔 Заказ #{order_id} {status_emoji}\n"
            text += f"👤 Клиент: {client_name}\n"
            text += f"👔 Курьер: {courier_name or 'не назначен'}\n"
            text += f"💬 Сообщений: {msg_count}\n\n"
            
            keyboard_buttons.append([{'text': f'💬 Чат #{order_id} - {client_name}', 'callback_data': f'view_chat_{order_id}'}])
        
        keyboard_buttons.append([{'text': '🔍 Найти чат по номеру', 'callback_data': 'search_chat'}])
        keyboard_buttons.append([{'text': '⬅️ Назад', 'callback_data': 'start'}])
        keyboard = {'inline_keyboard': keyboard_buttons}
    
    smart_send_message(chat_id, text, keyboard)

def handle_search_chat_prompt(chat_id: int) -> None:
    text = "🔍 <b>Поиск чата</b>\n\nОтправьте номер заказа для просмотра чата.\n\nНапример: <code>chat_123</code>"
    keyboard = {'inline_keyboard': [[{'text': '❌ Отмена', 'callback_data': 'operator_chats'}]]}
    send_message(chat_id, text, keyboard)

def handle_view_chat(chat_id: int, order_id: int, conn) -> None:
    cursor = conn.cursor()
    
    cursor.execute(
        "SELECT o.id, u1.first_name as client_name, u1.telegram_id as client_id, "
        "u2.first_name as courier_name, u2.telegram_id as courier_id "
        "FROM t_p39739760_garbage_bot_service.orders o "
        "JOIN t_p39739760_garbage_bot_service.users u1 ON o.client_id = u1.telegram_id "
        "LEFT JOIN t_p39739760_garbage_bot_service.users u2 ON o.courier_id = u2.telegram_id "
        "WHERE o.id = %s",
        (order_id,)
    )
    order_info = cursor.fetchone()
    
    if not order_info:
        cursor.close()
        send_message(chat_id, "❌ Заказ не найден")
        return
    
    order_id, client_name, client_id, courier_name, courier_id = order_info
    
    cursor.execute(
        "SELECT oc.message, oc.created_at, u.first_name, oc.sender_id "
        "FROM t_p39739760_garbage_bot_service.order_chat oc "
        "JOIN t_p39739760_garbage_bot_service.users u ON oc.sender_id = u.telegram_id "
        "WHERE oc.order_id = %s AND oc.is_archived = FALSE "
        "ORDER BY oc.created_at ASC LIMIT 50",
        (order_id,)
    )
    messages = cursor.fetchall()
    
    cursor.execute(
        f"SELECT oca.message, oca.created_at, u.first_name, oca.sender_id "
        f"FROM {SCHEMA}.order_chat_archive oca "
        f"JOIN {SCHEMA}.users u ON oca.sender_id = u.telegram_id "
        "WHERE oca.order_id = %s "
        "ORDER BY oca.created_at ASC",
        (order_id,)
    )
    archived_messages = cursor.fetchall()
    cursor.close()
    
    text = f"💬 <b>Чат заказа #{order_id}</b>\n\n"
    text += f"👤 Клиент: {client_name} (ID: {client_id})\n"
    text += f"👔 Курьер: {courier_name or 'не назначен'}"
    if courier_id:
        text += f" (ID: {courier_id})"
    text += "\n\n━━━━━━━━━━━━━━━━━━\n\n"
    
    if archived_messages:
        text += "📁 <b>Архивные сообщения:</b>\n\n"
        for msg in archived_messages:
            message_text, created_at, sender_name, sender_id = msg
            date_str = created_at.strftime("%d.%m %H:%M")
            
            if sender_id == client_id:
                icon = "👤"
            elif sender_id == courier_id:
                icon = "👔"
            else:
                icon = "⚙️"
            
            text += f"{icon} <b>{sender_name}</b> ({date_str}):\n{message_text}\n\n"
        
        text += "━━━━━━━━━━━━━━━━━━\n\n"
    
    if not messages:
        if not archived_messages:
            text += "Сообщений пока нет"
        else:
            text += "Новых сообщений нет"
    else:
        text += "💬 <b>Текущие сообщения:</b>\n\n"
        for msg in messages:
            message_text, created_at, sender_name, sender_id = msg
            time_str = created_at.strftime("%H:%M")
            
            if sender_id == client_id:
                icon = "👤"
            elif sender_id == courier_id:
                icon = "👔"
            else:
                icon = "⚙️"
            
            text += f"{icon} <b>{sender_name}</b> ({time_str}):\n{message_text}\n\n"
    
    text += "\n━━━━━━━━━━━━━━━━━━\n\n💬 Отправьте сообщение чтобы ответить"
    
    keyboard = {
        'inline_keyboard': [
            [{'text': '🔄 Обновить', 'callback_data': f'view_chat_{order_id}'}],
            [{'text': '❌ Закрыть чат', 'callback_data': 'close_chat'}],
            [{'text': '⬅️ Назад', 'callback_data': 'operator_chats'}]
        ]
    }
    
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO t_p39739760_garbage_bot_service.chat_sessions (telegram_id, order_id, updated_at) "
        "VALUES (%s, %s, %s) "
        "ON CONFLICT (telegram_id) DO UPDATE SET order_id = %s, updated_at = %s",
        (chat_id, order_id, datetime.now(), order_id, datetime.now())
    )
    conn.commit()
    cursor.close()
    
    smart_send_message(chat_id, text, keyboard)

def handle_send_chat_message(chat_id: int, telegram_id: int, order_id: int, message_text: str, conn) -> None:
    cursor = conn.cursor()
    
    if len(message_text) > 4000:
        cursor.close()
        send_message(chat_id, "❌ Сообщение слишком длинное (макс 4000 символов)")
        return
    
    cursor.execute(
        "SELECT client_id, courier_id FROM t_p39739760_garbage_bot_service.orders WHERE id = %s",
        (order_id,)
    )
    order = cursor.fetchone()
    
    if not order:
        cursor.close()
        send_message(chat_id, "❌ Заказ не найден")
        return
    
    client_id, courier_id = order
    
    role = check_user_role(telegram_id, conn)
    is_operator = role in ['operator', 'admin']
    
    if not is_operator and telegram_id != client_id and telegram_id != courier_id:
        cursor.close()
        send_message(chat_id, "❌ Вы не участник этого заказа")
        return
    
    cursor.execute(
        "INSERT INTO t_p39739760_garbage_bot_service.order_chat (order_id, sender_id, message) VALUES (%s, %s, %s)",
        (order_id, telegram_id, message_text)
    )
    conn.commit()
    
    cursor.execute("SELECT first_name FROM t_p39739760_garbage_bot_service.users WHERE telegram_id = %s", (telegram_id,))
    sender = cursor.fetchone()
    sender_name = sender[0] if sender else "Пользователь"
    
    cursor.close()
    
    if is_operator:
        if client_id:
            keyboard = {
                'inline_keyboard': [
                    [{'text': '💬 Ответить', 'callback_data': f'client_chat_{order_id}'}]
                ]
            }
            send_message(client_id, f"⚙️ <b>Оператор</b>: {message_text}", keyboard)
        
        if courier_id:
            keyboard = {
                'inline_keyboard': [
                    [{'text': '💬 Ответить', 'callback_data': f'courier_chat_{order_id}'}]
                ]
            }
            send_message(courier_id, f"⚙️ <b>Оператор</b>: {message_text}", keyboard)
    else:
        recipient_id = courier_id if telegram_id == client_id else client_id
        
        if recipient_id:
            role_text = "Курьер" if telegram_id == courier_id else "Клиент"
            recipient_type = "client" if recipient_id == client_id else "courier"
            keyboard = {
                'inline_keyboard': [
                    [{'text': '💬 Ответить', 'callback_data': f'{recipient_type}_chat_{order_id}'}]
                ]
            }
            send_message(recipient_id, f"<b>{role_text}</b>: {message_text}", keyboard)

def handle_open_chat(chat_id: int, telegram_id: int, order_id: int, user_type: str, conn) -> None:
    cursor = conn.cursor()
    
    cursor.execute(
        "SELECT o.id, u1.first_name as client_name, u2.first_name as courier_name "
        "FROM t_p39739760_garbage_bot_service.orders o "
        "JOIN t_p39739760_garbage_bot_service.users u1 ON o.client_id = u1.telegram_id "
        "LEFT JOIN t_p39739760_garbage_bot_service.users u2 ON o.courier_id = u2.telegram_id "
        "WHERE o.id = %s",
        (order_id,)
    )
    order_info = cursor.fetchone()
    
    if not order_info:
        cursor.close()
        send_message(chat_id, "❌ Заказ не найден")
        return
    
    order_id, client_name, courier_name = order_info
    
    cursor.execute(
        "SELECT oc.message, oc.created_at, u.first_name, oc.sender_id "
        "FROM t_p39739760_garbage_bot_service.order_chat oc "
        "JOIN t_p39739760_garbage_bot_service.users u ON oc.sender_id = u.telegram_id "
        "WHERE oc.order_id = %s "
        "ORDER BY oc.created_at DESC LIMIT 20",
        (order_id,)
    )
    messages = cursor.fetchall()
    cursor.close()
    
    text = f"💬 <b>Чат по заказу #{order_id}</b>\n\n"
    
    if user_type == 'client':
        text += f"👔 Курьер: {courier_name or 'не назначен'}\n\n"
    else:
        text += f"👤 Клиент: {client_name}\n\n"
    
    text += "━━━━━━━━━━━━━━━━━━\n\n"
    
    if not messages:
        text += "Сообщений пока нет\n\n"
    else:
        for msg in reversed(messages):
            message_text, created_at, sender_name, sender_id = msg
            time_str = created_at.strftime("%H:%M")
            
            if sender_id == telegram_id:
                text += f"<b>Вы</b> ({time_str}):\n{message_text}\n\n"
            else:
                text += f"<b>{sender_name}</b> ({time_str}):\n{message_text}\n\n"
    
    text += "━━━━━━━━━━━━━━━━━━\n\n💬 Отправьте сообщение для ответа"
    
    callback_key = 'client_active' if user_type == 'client' else 'courier_current'
    keyboard = {
        'inline_keyboard': [
            [{'text': '🔄 Обновить', 'callback_data': f'{user_type}_chat_{order_id}'}],
            [{'text': '❌ Закрыть чат', 'callback_data': 'close_chat'}],
            [{'text': '⬅️ Назад', 'callback_data': callback_key}]
        ]
    }
    
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO t_p39739760_garbage_bot_service.chat_sessions (telegram_id, order_id, updated_at) "
        "VALUES (%s, %s, %s) "
        "ON CONFLICT (telegram_id) DO UPDATE SET order_id = %s, updated_at = %s",
        (telegram_id, order_id, datetime.now(), order_id, datetime.now())
    )
    conn.commit()
    cursor.close()
    
    smart_send_message(chat_id, text, keyboard)

def handle_admin_courier_applications(chat_id: int, conn) -> None:
    cursor = conn.cursor()
    cursor.execute(
        "SELECT ca.id, ca.telegram_id, u.first_name, u.username "
        "FROM t_p39739760_garbage_bot_service.courier_applications ca "
        "JOIN t_p39739760_garbage_bot_service.users u ON ca.telegram_id = u.telegram_id "
        "WHERE ca.status = %s "
        "ORDER BY ca.created_at DESC LIMIT 10",
        ('pending',)
    )
    applications = cursor.fetchall()
    cursor.close()
    
    if not applications:
        text = "👔 <b>Заявки курьеров</b>\n\nНет новых заявок"
        keyboard = {'inline_keyboard': [[{'text': '⬅️ Назад', 'callback_data': 'admin_panel'}]]}
    else:
        text = "👔 <b>Заявки курьеров</b>\n\n"
        keyboard_buttons = []
        
        for app in applications:
            app_id, telegram_id, first_name, username = app
            text += f"👤 {first_name} (@{username or 'нет username'})\n"
            text += f"ID: {telegram_id}\n\n"
            
            keyboard_buttons.append([
                {'text': f'✅ Одобрить {first_name}', 'callback_data': f'approve_courier_{telegram_id}'},
                {'text': f'❌ Отклонить', 'callback_data': f'reject_courier_{telegram_id}'}
            ])
        
        keyboard_buttons.append([{'text': '⬅️ Назад', 'callback_data': 'admin_couriers'}])
        keyboard = {'inline_keyboard': keyboard_buttons}
    
    smart_send_message(chat_id, text, keyboard)

def handle_approve_courier(chat_id: int, admin_id: int, courier_id: int, conn) -> None:
    cursor = conn.cursor()
    
    cursor.execute(
        "UPDATE t_p39739760_garbage_bot_service.users SET role = %s WHERE telegram_id = %s",
        ('courier', courier_id)
    )
    
    cursor.execute(
        "UPDATE t_p39739760_garbage_bot_service.courier_applications SET status = %s, reviewed_by = %s, reviewed_at = %s WHERE telegram_id = %s AND status = %s",
        ('approved', admin_id, datetime.now(), courier_id, 'pending')
    )
    
    conn.commit()
    cursor.close()
    
    send_message(courier_id, "✅ Поздравляем! Ваша заявка на роль курьера одобрена.\n\nИспользуйте /start для доступа к меню курьера.")
    send_message(chat_id, "✅ Курьер одобрен")

def handle_reject_courier(chat_id: int, admin_id: int, courier_id: int, conn) -> None:
    cursor = conn.cursor()
    
    cursor.execute(
        "UPDATE t_p39739760_garbage_bot_service.courier_applications SET status = %s, reviewed_by = %s, reviewed_at = %s WHERE telegram_id = %s AND status = %s",
        ('rejected', admin_id, datetime.now(), courier_id, 'pending')
    )
    
    conn.commit()
    cursor.close()
    
    send_message(courier_id, "❌ К сожалению, ваша заявка на роль курьера отклонена.")
    send_message(chat_id, "❌ Заявка отклонена")

def handle_admin_all_orders(chat_id: int, conn) -> None:
    cursor = conn.cursor()
    cursor.execute(
        "SELECT COUNT(*) FROM t_p39739760_garbage_bot_service.orders WHERE status = %s",
        ('pending',)
    )
    pending = cursor.fetchone()[0]
    
    cursor.execute(
        "SELECT COUNT(*) FROM t_p39739760_garbage_bot_service.orders WHERE status = %s",
        ('accepted',)
    )
    active = cursor.fetchone()[0]
    
    cursor.execute(
        "SELECT COUNT(*) FROM t_p39739760_garbage_bot_service.orders WHERE status = %s",
        ('completed',)
    )
    completed = cursor.fetchone()[0]
    
    cursor.execute("SELECT SUM(price) FROM t_p39739760_garbage_bot_service.orders WHERE status = %s", ('completed',))
    total_revenue = cursor.fetchone()[0] or 0
    
    cursor.close()
    
    text = (
        "📦 <b>Все заказы</b>\n\n"
        f"🔍 В ожидании: {pending}\n"
        f"🚚 В работе: {active}\n"
        f"✅ Завершено: {completed}\n\n"
        f"💰 Общая выручка: {total_revenue} ₽"
    )
    
    keyboard = {
        'inline_keyboard': [
            [{'text': '⬅️ Назад', 'callback_data': 'admin_panel'}]
        ]
    }
    
    smart_send_message(chat_id, text, keyboard)

def handle_callback_query(callback_query: Dict, conn) -> None:
    chat_id = callback_query['message']['chat']['id']
    message_id = callback_query['message']['message_id']
    telegram_id = callback_query['from']['id']
    username = callback_query['from'].get('username', '')
    first_name = callback_query['from'].get('first_name', '')
    data = callback_query['data']
    
    _context.message_id = message_id
    
    role = check_user_role(telegram_id, conn)
    
    if data == 'start':
        handle_start(chat_id, telegram_id, username, first_name, conn)
    elif data == 'apply_courier':
        handle_apply_courier(chat_id, telegram_id, conn)
    elif data == 'client_menu':
        handle_client_menu(chat_id)
    elif data == 'courier_available':
        handle_courier_available_orders(chat_id, telegram_id, conn)
    elif data == 'courier_current':
        handle_courier_current_orders(chat_id, telegram_id, conn)
    elif data == 'courier_stats':
        handle_courier_stats(chat_id, telegram_id, conn)

    elif data == 'client_new_order':
        handle_client_new_order(chat_id)
    elif data == 'client_active':
        handle_client_active_orders(chat_id, telegram_id, conn)
    elif data == 'operator_active_orders':
        if role in ['operator', 'admin']:
            handle_operator_active_orders(chat_id, conn)
    elif data == 'admin_panel':
        if role == 'admin':
            handle_admin_panel(chat_id, conn)
    elif data == 'admin_couriers':
        if role == 'admin':
            handle_admin_couriers_menu(chat_id, conn)
    elif data == 'admin_operators':
        if role == 'admin':
            handle_admin_operators_menu(chat_id, conn)
    elif data == 'admin_courier_applications':
        if role == 'admin':
            handle_admin_courier_applications(chat_id, conn)
    elif data == 'admin_couriers_list':
        if role == 'admin':
            handle_admin_couriers_list(chat_id, conn)
    elif data == 'admin_operators_list':
        if role == 'admin':
            handle_admin_operators_list(chat_id, conn)
    elif data == 'admin_remove_courier':
        if role == 'admin':
            handle_admin_remove_courier_prompt(chat_id)
    elif data == 'admin_remove_operator':
        if role == 'admin':
            handle_admin_remove_operator_prompt(chat_id)
    elif data == 'admin_all_orders':
        if role == 'admin':
            handle_admin_all_orders(chat_id, conn)
    elif data == 'admin_subscriptions':
        if role == 'admin':
            handle_admin_subscriptions(chat_id, conn)
    elif data == 'admin_add_subscription':
        if role == 'admin':
            handle_admin_add_subscription_prompt(chat_id)
    elif data.startswith('cancel_sub_'):
        if role == 'admin':
            sub_id = int(data.split('_')[2])
            handle_cancel_subscription(chat_id, sub_id, conn)
    elif data == 'switch_to_operator':
        if role == 'admin':
            text = "📞 <b>Панель оператора</b>\n\nВыберите действие:"
            smart_send_message(chat_id, text, {
                'inline_keyboard': [
                    [{'text': '📞 Активные заказы', 'callback_data': 'operator_active_orders'}],
                    [{'text': '💬 Чаты заказов', 'callback_data': 'operator_chats'}],
                    [{'text': '📊 Статистика', 'callback_data': 'operator_stats'}],
                    [{'text': '⬅️ Назад в админку', 'callback_data': 'admin_panel'}]
                ]
            })
    elif data == 'switch_to_courier':
        if role == 'admin':
            text = "👔 <b>Режим курьера</b>\n\nВыберите действие:"
            smart_send_message(chat_id, text, {
                'inline_keyboard': [
                    [{'text': '📦 Доступные заказы', 'callback_data': 'courier_available'}],
                    [{'text': '🚚 Текущие заказы', 'callback_data': 'courier_current'}],
                    [{'text': '📊 История заказов', 'callback_data': 'courier_history'}],
                    [{'text': '💰 Статистика', 'callback_data': 'courier_stats'}],
                    [{'text': '⬅️ Назад в админку', 'callback_data': 'admin_panel'}]
                ]
            })
    elif data == 'admin_add_operator':
        if role == 'admin':
            handle_admin_add_operator(chat_id)
    elif data == 'admin_stats':
        if role == 'admin':
            handle_admin_stats(chat_id, conn)
    elif data == 'client_history':
        handle_client_history(chat_id, telegram_id, conn)
    elif data == 'courier_history':
        handle_courier_history(chat_id, telegram_id, conn)
    elif data == 'client_payment':
        handle_client_payment(chat_id)
    elif data == 'client_subscription':
        handle_client_subscription(chat_id, telegram_id, conn)
    elif data == 'courier_withdraw':
        handle_courier_withdraw(chat_id, telegram_id, conn)
    elif data == 'operator_stats':
        if role in ['operator', 'admin']:
            handle_operator_stats(chat_id, conn)
    elif data == 'operator_chats':
        if role in ['operator', 'admin']:
            handle_operator_chats(chat_id, conn)
    elif data == 'search_chat':
        if role in ['operator', 'admin']:
            handle_search_chat_prompt(chat_id)
    elif data.startswith('view_chat_'):
        if role in ['operator', 'admin']:
            order_id = int(data.split('_')[2])
            handle_view_chat(chat_id, order_id, conn)
    elif data.startswith('accept_order_'):
        order_id = int(data.split('_')[2])
        handle_accept_order(chat_id, telegram_id, order_id, conn)
    elif data.startswith('start_work_'):
        order_id = int(data.split('_')[2])
        handle_start_work(chat_id, telegram_id, order_id, conn)
    elif data.startswith('complete_order_'):
        order_id = int(data.split('_')[2])
        handle_complete_order(chat_id, telegram_id, order_id, conn)
    elif data.startswith('operator_status_'):
        if role in ['operator', 'admin']:
            order_id = int(data.split('_')[2])
            handle_operator_change_status(chat_id, order_id, conn)
    elif data.startswith('set_status_'):
        if role in ['operator', 'admin']:
            parts = data.split('_')
            order_id = int(parts[2])
            new_status = '_'.join(parts[3:])
            handle_set_order_status(chat_id, order_id, new_status, conn)
    elif data.startswith('approve_courier_'):
        if role == 'admin':
            courier_id = int(data.split('_')[2])
            handle_approve_courier(chat_id, telegram_id, courier_id, conn)
    elif data.startswith('reject_courier_'):
        if role == 'admin':
            courier_id = int(data.split('_')[2])
            handle_reject_courier(chat_id, telegram_id, courier_id, conn)
    elif data == 'close_chat':
        cursor = conn.cursor()
        cursor.execute("DELETE FROM t_p39739760_garbage_bot_service.chat_sessions WHERE telegram_id = %s", (telegram_id,))
        conn.commit()
        cursor.close()
        send_message(chat_id, "✅ Чат закрыт. Теперь вы можете создать новый заказ или вернуться в меню.")
        handle_start(chat_id, telegram_id, username, first_name, conn)
    elif data.startswith('client_chat_'):
        order_id = int(data.split('_')[2])
        handle_open_chat(chat_id, telegram_id, order_id, 'client', conn)
    elif data.startswith('courier_chat_'):
        order_id = int(data.split('_')[2])
        handle_open_chat(chat_id, telegram_id, order_id, 'courier', conn)
    elif data.startswith('cancel_order_'):
        order_id = int(data.split('_')[2])
        handle_cancel_order(chat_id, telegram_id, order_id, conn)
    elif data.startswith('select_bags_'):
        bag_count = int(data.split('_')[2])
        handle_select_bags(chat_id, telegram_id, bag_count, conn)
    elif data == 'custom_bags':
        handle_custom_bags_prompt(chat_id, telegram_id, conn)
    elif data == 'buy_sub_alternate':
        handle_buy_subscription(chat_id, telegram_id, 'alternate_day', conn)
    elif data == 'buy_sub_daily':
        handle_buy_subscription(chat_id, telegram_id, 'daily', conn)
    
    _context.message_id = None

def handle_message(message: Dict, conn) -> None:
    _context.message_id = None
    chat_id = message['chat']['id']
    telegram_id = message['from']['id']
    username = message['from'].get('username', '')
    first_name = message['from'].get('first_name', '')
    text = message.get('text', '')
    
    if text == '/start':
        handle_start(chat_id, telegram_id, username, first_name, conn)
        return
    
    cursor = conn.cursor()
    cursor.execute(f"SELECT order_id FROM {SCHEMA}.chat_sessions WHERE telegram_id = %s", (telegram_id,))
    active_chat = cursor.fetchone()
    cursor.close()
    
    if active_chat and text and not text.startswith('/') and not text.startswith('operator_') and not text.startswith('courier_') and not text.startswith('chat_'):
        order_id = active_chat[0]
        
        cursor = conn.cursor()
        cursor.execute(f"SELECT client_id, courier_id, status FROM {SCHEMA}.orders WHERE id = %s", (order_id,))
        order_info = cursor.fetchone()
        cursor.close()
        
        if order_info:
            client_id, courier_id, order_status = order_info
            
            if order_status == 'completed':
                cursor = conn.cursor()
                cursor.execute(f"DELETE FROM {SCHEMA}.chat_sessions WHERE telegram_id = %s", (telegram_id,))
                conn.commit()
                cursor.close()
            elif telegram_id == client_id or telegram_id == courier_id:
                handle_send_chat_message(chat_id, telegram_id, order_id, text, conn)
                return
    
    role = check_user_role(telegram_id, conn)
    
    if text.startswith('operator_add '):
        if role == 'admin':
            try:
                operator_id = int(text.split(' ')[1])
                handle_add_operator(chat_id, telegram_id, operator_id, conn)
            except (ValueError, IndexError):
                send_message(chat_id, "❌ Неверный формат. Используйте: operator_add ID")
        else:
            send_message(chat_id, "❌ Доступ запрещен")
        return
    
    if text.startswith('operator_remove '):
        if role == 'admin':
            try:
                operator_id = int(text.split(' ')[1])
                handle_remove_operator(chat_id, operator_id, conn)
            except (ValueError, IndexError):
                send_message(chat_id, "❌ Неверный формат. Используйте: operator_remove ID")
        else:
            send_message(chat_id, "❌ Доступ запрещен")
        return
    
    if text.startswith('courier_remove '):
        if role == 'admin':
            try:
                courier_id = int(text.split(' ')[1])
                handle_remove_courier(chat_id, courier_id, conn)
            except (ValueError, IndexError):
                send_message(chat_id, "❌ Неверный формат. Используйте: courier_remove ID")
        else:
            send_message(chat_id, "❌ Доступ запрещен")
        return
    
    if text.startswith('sub_add '):
        if role == 'admin':
            try:
                parts = text.split(' ')
                if len(parts) != 3:
                    send_message(chat_id, "❌ Неверный формат. Используйте: sub_add USER_ID TYPE")
                    return
                
                client_id = int(parts[1])
                sub_type_input = parts[2].lower()
                
                if sub_type_input == 'alternate':
                    sub_type = 'alternate_day'
                elif sub_type_input == 'daily':
                    sub_type = 'daily'
                else:
                    send_message(chat_id, "❌ Неверный тип подписки. Используйте: daily или alternate")
                    return
                
                handle_admin_grant_subscription(chat_id, client_id, sub_type, conn)
            except (ValueError, IndexError):
                send_message(chat_id, "❌ Неверный формат. Используйте: sub_add USER_ID TYPE")
        else:
            send_message(chat_id, "❌ Доступ запрещен")
        return
    
    if text.startswith('chat_'):
        if role in ['operator', 'admin']:
            try:
                order_id = int(text.replace('chat_', ''))
                
                cursor = conn.cursor()
                cursor.execute(f"SELECT id FROM {SCHEMA}.orders WHERE id = %s", (order_id,))
                order_exists = cursor.fetchone()
                cursor.close()
                
                if order_exists:
                    handle_view_chat(chat_id, order_id, conn)
                else:
                    send_message(chat_id, "❌ Заказ с таким номером не найден")
                return
            except ValueError:
                send_message(chat_id, "❌ Неверный формат. Используйте: chat_123")
                return
        else:
            try:
                parts = text.split(' ', 1)
                if len(parts) < 2:
                    send_message(chat_id, "❌ Неверный формат. Отправьте сообщение после номера заказа")
                    return
                
                order_id = int(parts[0].replace('chat_', ''))
                message_text = parts[1]
                handle_send_chat_message(chat_id, telegram_id, order_id, message_text, conn)
                return
            except (ValueError, IndexError):
                send_message(chat_id, "❌ Неверный формат чата. Используйте: chat_ID текст")
                return
    
    cursor = conn.cursor()
    cursor.execute(f"SELECT state, order_data FROM {SCHEMA}.order_draft WHERE telegram_id = %s", (telegram_id,))
    session = cursor.fetchone()
    
    if session:
        state, order_data_json = session
        
        if state == 'waiting_custom_bags':
            try:
                bag_count = int(text.strip())
                if bag_count < 1 or bag_count > 100:
                    send_message(chat_id, "❌ Введите число от 1 до 100")
                    cursor.close()
                    return
                
                handle_select_bags(chat_id, telegram_id, bag_count, conn)
                cursor.close()
                return
            except ValueError:
                send_message(chat_id, "❌ Введите корректное число")
                cursor.close()
                return
        
        elif state == 'waiting_address':
            address = text.strip()
            if len(address) > 500:
                send_message(chat_id, "❌ Адрес слишком длинный")
                cursor.close()
                return
            
            order_data = order_data_json if order_data_json else {}
            bag_count = order_data.get('bag_count', 1)
            is_subscription = order_data.get('is_subscription', False)
            total_price = order_data.get('price', BAG_PRICE * bag_count)
            
            cursor.execute(
                f"INSERT INTO {SCHEMA}.orders (client_id, address, description, price, status, detailed_status, bag_count, is_subscription_order, payment_status) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id",
                (telegram_id, address, f"Вывоз мусора ({bag_count} пакетов)", total_price, 'pending', 'waiting_payment', bag_count, is_subscription, 'pending')
            )
            order_id = cursor.fetchone()[0]
            conn.commit()
            
            cursor.execute(f"DELETE FROM {SCHEMA}.order_draft WHERE telegram_id = %s", (telegram_id,))
            conn.commit()
            cursor.close()
            
            try:
                import requests
                payment_response = requests.post(
                    'https://functions.poehali.dev/b4b440af-a2f4-4b49-86be-5c7dafb0762d',
                    json={
                        'amount': total_price,
                        'description': f"Заказ #{order_id}: Вывоз мусора ({bag_count} мешков)",
                        'order_id': order_id
                    },
                    timeout=10
                )
                
                if payment_response.status_code == 200:
                    payment_data = payment_response.json()
                    payment_url = payment_data.get('payment_url')
                    payment_id = payment_data.get('payment_id')
                    
                    cursor = conn.cursor()
                    cursor.execute(
                        f"UPDATE {SCHEMA}.orders SET payment_id = %s, payment_url = %s WHERE id = %s",
                        (payment_id, payment_url, order_id)
                    )
                    conn.commit()
                    cursor.close()
                    
                    keyboard = {
                        'inline_keyboard': [
                            [{'text': '💳 Оплатить', 'url': payment_url}],
                            [{'text': '📦 Мои заказы', 'callback_data': 'client_active'}],
                            [{'text': '⬅️ В меню', 'callback_data': 'start'}]
                        ]
                    }
                    smart_send_message(chat_id, 
                        f"✅ Заказ #{order_id} создан!\n\n"
                        f"💰 Сумма: {total_price} ₽\n"
                        f"📦 Мешков: {bag_count}\n"
                        f"📍 Адрес: {address}\n\n"
                        f"Для подтверждения заказа нажмите кнопку «Оплатить»", 
                        keyboard
                    )
                else:
                    keyboard = {
                        'inline_keyboard': [
                            [{'text': '📦 Мои заказы', 'callback_data': 'client_active'}],
                            [{'text': '⬅️ В меню', 'callback_data': 'start'}]
                        ]
                    }
                    smart_send_message(chat_id, "❌ Ошибка создания платежа. Попробуйте позже.", keyboard)
            except Exception as e:
                keyboard = {
                    'inline_keyboard': [
                        [{'text': '📦 Мои заказы', 'callback_data': 'client_active'}],
                        [{'text': '⬅️ В меню', 'callback_data': 'start'}]
                    ]
                }
                smart_send_message(chat_id, f"❌ Ошибка: {str(e)}", keyboard)
            
            return
    
    cursor.close()
    send_message(chat_id, "Используйте /start для начала работы")

def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    method: str = event.get('httpMethod', 'POST')
    
    if method == 'OPTIONS':
        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'POST, OPTIONS',
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Max-Age': '86400'
            },
            'body': '',
            'isBase64Encoded': False
        }
    
    if method == 'POST':
        body = json.loads(event.get('body', '{}'))
        
        conn = get_db_connection()
        
        if 'message' in body:
            handle_message(body['message'], conn)
        elif 'callback_query' in body:
            handle_callback_query(body['callback_query'], conn)
        
        conn.close()
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({'ok': True}),
            'isBase64Encoded': False
        }
    
    return {
        'statusCode': 405,
        'headers': {'Content-Type': 'application/json'},
        'body': json.dumps({'error': 'Method not allowed'}),
        'isBase64Encoded': False
    }