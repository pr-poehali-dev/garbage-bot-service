import json
import os
import base64
from typing import Dict, Any
from decimal import Decimal

def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    '''
    Создание платежа в системе ЮMoney (Яндекс.Касса)
    Принимает: amount (сумма в рублях), description (описание), order_id (ID заказа)
    Возвращает: payment_id и confirmation_url для оплаты
    '''
    method: str = event.get('httpMethod', 'GET')
    
    if method == 'OPTIONS':
        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'POST, OPTIONS',
                'Access-Control-Allow-Headers': 'Content-Type, X-User-Id',
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
    
    import requests
    
    body_str = event.get('body', '{}')
    if not body_str or body_str.strip() == '':
        body_str = '{}'
    
    try:
        body_data = json.loads(body_str)
    except json.JSONDecodeError as e:
        return {
            'statusCode': 400,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'error': f'Invalid JSON: {str(e)}'}),
            'isBase64Encoded': False
        }
    
    amount = body_data.get('amount')
    description = body_data.get('description', 'Оплата заказа')
    order_id = body_data.get('order_id')
    
    if not amount or not order_id:
        return {
            'statusCode': 400,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'error': 'Missing required fields: amount, order_id'}),
            'isBase64Encoded': False
        }
    
    try:
        
        shop_id = os.environ.get('YOOMONEY_SHOP_ID')
        secret_key = os.environ.get('YOOMONEY_SECRET_KEY')
        
        if not shop_id or not secret_key:
            return {
                'statusCode': 500,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({'error': 'Payment credentials not configured'}),
                'isBase64Encoded': False
            }
        
        auth_string = f"{shop_id}:{secret_key}"
        auth_bytes = auth_string.encode('utf-8')
        auth_b64 = base64.b64encode(auth_bytes).decode('utf-8')
        
        payment_data = {
            'amount': {
                'value': f"{Decimal(str(amount)):.2f}",
                'currency': 'RUB'
            },
            'capture': True,
            'confirmation': {
                'type': 'redirect',
                'return_url': 'https://t.me/garbagetakeoutbot'
            },
            'description': description,
            'metadata': {
                'order_id': str(order_id)
            }
        }
        
        headers = {
            'Authorization': f'Basic {auth_b64}',
            'Content-Type': 'application/json',
            'Idempotence-Key': f'order_{order_id}_{context.request_id}'
        }
        
        response = requests.post(
            'https://api.yookassa.ru/v3/payments',
            json=payment_data,
            headers=headers,
            timeout=10
        )
        
        if response.status_code != 200:
            return {
                'statusCode': response.status_code,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({
                    'error': 'Payment creation failed',
                    'details': response.text
                }),
                'isBase64Encoded': False
            }
        
        payment_response = response.json()
        
        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'payment_id': payment_response['id'],
                'payment_url': payment_response['confirmation']['confirmation_url'],
                'status': payment_response['status']
            }),
            'isBase64Encoded': False
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'error': str(e)}),
            'isBase64Encoded': False
        }