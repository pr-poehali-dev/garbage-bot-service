import json
import os
import psycopg2
from typing import Dict, Any

SCHEMA = 't_p39739760_garbage_bot_service'

def send_telegram_message(chat_id: int, text: str):
    '''Отправка сообщения через Telegram Bot API'''
    import requests
    
    bot_token = os.environ.get('TELEGRAM_BOT_TOKEN')
    if not bot_token:
        return
    
    url = f'https://api.telegram.org/bot{bot_token}/sendMessage'
    data = {
        'chat_id': chat_id,
        'text': text,
        'parse_mode': 'HTML'
    }
    
    try:
        requests.post(url, json=data, timeout=5)
    except Exception:
        pass

def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    '''
    Обработка webhook уведомлений от ЮMoney о статусе платежей
    Автоматически обновляет статус заказа и отправляет уведомление клиенту
    '''
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
    
    if method != 'POST':
        return {
            'statusCode': 405,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'error': 'Method not allowed'}),
            'isBase64Encoded': False
        }
    
    try:
        body_data = json.loads(event.get('body', '{}'))
        
        event_type = body_data.get('event')
        payment_object = body_data.get('object', {})
        
        if event_type != 'payment.succeeded':
            return {
                'statusCode': 200,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({'status': 'ignored'}),
                'isBase64Encoded': False
            }
        
        payment_id = payment_object.get('id')
        payment_status = payment_object.get('status')
        metadata = payment_object.get('metadata', {})
        order_id = metadata.get('order_id')
        
        if not order_id or not payment_id:
            return {
                'statusCode': 400,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({'error': 'Missing order_id or payment_id'}),
                'isBase64Encoded': False
            }
        
        dsn = os.environ.get('DATABASE_URL')
        if not dsn:
            return {
                'statusCode': 500,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({'error': 'Database not configured'}),
                'isBase64Encoded': False
            }
        
        conn = psycopg2.connect(dsn)
        cursor = conn.cursor()
        
        cursor.execute(
            f"UPDATE {SCHEMA}.orders SET payment_status = %s, paid_at = NOW(), detailed_status = %s WHERE id = %s RETURNING client_id, address, bag_count, price",
            (payment_status, 'searching_courier', order_id)
        )
        result = cursor.fetchone()
        
        if result:
            client_id, address, bag_count, price = result
            
            cursor.execute(
                f"SELECT telegram_id FROM {SCHEMA}.clients WHERE id = %s",
                (client_id,)
            )
            telegram_result = cursor.fetchone()
            
            if telegram_result:
                telegram_id = telegram_result[0]
                
                message = f"✅ <b>Оплата прошла успешно!</b>\n\n"
                message += f"📦 Заказ #{order_id}\n"
                message += f"🗑 Мешков: {bag_count}\n"
                message += f"📍 Адрес: {address}\n\n"
                message += "Курьер скоро свяжется с вами для согласования времени вывоза."
                
                send_telegram_message(telegram_id, message)
            
            cursor.execute(
                f"SELECT telegram_id FROM {SCHEMA}.users WHERE role = %s",
                ('courier',)
            )
            couriers = cursor.fetchall()
            
            notification_keyboard_json = json.dumps({
                'inline_keyboard': [
                    [{'text': '✅ Принять', 'callback_data': f'accept_order_{order_id}'}]
                ]
            })
            
            for courier in couriers:
                courier_id = courier[0]
                
                import requests
                bot_token = os.environ.get('TELEGRAM_BOT_TOKEN')
                if bot_token:
                    url = f'https://api.telegram.org/bot{bot_token}/sendMessage'
                    data = {
                        'chat_id': courier_id,
                        'text': f"🆕 Новый заказ #{order_id}\n📍 {address}\n📦 {bag_count} мешков\n💰 {price} ₽",
                        'reply_markup': notification_keyboard_json
                    }
                    try:
                        requests.post(url, json=data, timeout=5)
                    except Exception:
                        pass
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'status': 'processed'}),
            'isBase64Encoded': False
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'error': str(e)}),
            'isBase64Encoded': False
        }