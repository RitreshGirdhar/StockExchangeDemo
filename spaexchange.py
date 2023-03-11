from flask import Flask, jsonify, request
from kafka import KafkaProducer
from dataclasses import field, dataclass
from datetime import datetime
import random
import string
import uuid
import json

app = Flask(__name__)



def order_serializer(order):
    return json.dumps({
        'order_id': order.order_id,
        'stock_symbol': order.stock_symbol,
        'quantity': order.quantity,
        'side': order.side,
        'order_type': order.order_type,
        'price': order.price,
        'time_in_force': order.time_in_force
    }).encode('utf-8')

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=order_serializer)
topic = 'incoming_orders'


@dataclass
class StockDetails:
    symbol: str
    name: str
    exchange: str
    currency: str
    country: str
    sector: str
    industry: str
    market_cap: float
    beta: float
    pe_ratio: float
    eps: float
    dividend_yield: float
    dividend_per_share: float
    dividend_pay_date: str
    dividend_ex_date: str
    last_split_factor: str
    last_split_date: str  

@dataclass
class Trade:
    symbol: str
    qty: int
    price: float
    side: str
    timestamp: str
    order_id: str
    instmt: str
    trade_price: float
    trade_qty: int
    trade_side: str
    trade_id: str

@dataclass
class Order:
    order_id: str
    stock_symbol: str
    quantity: int
    side: str
    order_type: str
    price: float
    time_in_force: str


@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"


@app.route("/get")
def get_help():
    return "<p>Hello, World! Help!</p>"



@app.route("/api/v1/stock/<symbol>")
def get_stock_details(symbol):
    return jsonify(get_stock(symbol).__dict__)


@app.route("/api/v1/orders/place", methods=['POST'])
def submit_order():
    data = request.get_json()

    order_id = str(uuid.uuid4())

    # 21:{"order_id": 21, "stock_symbol": "AAPL1", "order_type": "BUY", "price": 130.00, "quantity": 100}
    order = Order(
        order_id = order_id,
        stock_symbol=data['symbol'],
        quantity=data['qty'],
        side=data['side'],
        order_type=data['type'],
        price=data['price'],
        time_in_force=data['time_in_force']
    )

    producer.send(topic, order)

    # Simulate order placement and return trade details
    trade = Trade(
        symbol=order.stock_symbol,
        qty=order.quantity,
        price=order.price,
        side=order.side,
        timestamp=datetime.utcnow().isoformat(),
        order_id=order.order_id,
        instmt=order.stock_symbol,
        trade_price=order.price,
        trade_qty=order.quantity,
        trade_side=order.side,
        trade_id=str(uuid.uuid4())
    )
    return jsonify(trade.__dict__)
    

def get_stock(symbol):
    # In practical world this will invoke service and look up into database
    return StockDetails(
        symbol='AAPL',
        name='Apple Inc.',
        exchange='NASDAQ',
        currency='USD',
        country='USA',
        sector='Technology',
        industry='Consumer Electronics',
        market_cap=2234926.77,
        beta=1.22,
        pe_ratio=30.92,
        eps=5.19,
        dividend_yield=0.61,
        dividend_per_share=0.88,
        dividend_pay_date='2023-03-11',
        dividend_ex_date='2023-03-14',
        last_split_factor='7:1',
        last_split_date='2023-03-11'
    )




@app.route('/push_message', methods=['POST'])
def push_message():
    data = request.get_json()
    order_id = str(uuid.uuid4())
    order = Order(
        order_id =order_id,
        stock_symbol=data['symbol'],
        quantity=data['qty'],
        side=data['side'],
        order_type=data['type'],
        price=data['price'],
        time_in_force=data['time_in_force']
    )
    # 21:{"order_id": 21, "stock_symbol": "AAPL1", "order_type": "BUY", "price": 130.00, "quantity": 100}
    producer.send(topic, order.encode())
    return 'Message pushed to Kafka'
