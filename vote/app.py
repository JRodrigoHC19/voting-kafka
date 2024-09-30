from confluent_kafka import Consumer, KafkaException, KafkaError
from flask import Flask, render_template, request, make_response, g
from redis import Redis
import os
import socket
import random
import json
import logging

option_a = os.getenv('OPTION_A', "Cats")
option_b = os.getenv('OPTION_B', "Dogs")
hostname = socket.gethostname()

app = Flask(__name__)


# Kafka - Consumer
kafka_conf = {
    'bootstrap.servers': 'kafka:29092',
    'group.id': 'temperature-consumer-group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(kafka_conf)
topic = "temperatures"
consumer.subscribe([topic])



gunicorn_error_logger = logging.getLogger('gunicorn.error')
app.logger.handlers.extend(gunicorn_error_logger.handlers)
app.logger.setLevel(logging.INFO)

def get_redis():
    if not hasattr(g, 'redis'):
        g.redis = Redis(host="redis", db=0, socket_timeout=5)
    return g.redis


@app.route("/", methods=['POST','GET'])
def hello():
    voter_id = request.cookies.get('voter_id')
    if not voter_id:
        voter_id = hex(random.getrandbits(64))[2:-1]

    vote = None
    temperature = "No data"

    if request.method == 'POST':
        redis = get_redis()
        vote = request.form['vote']
        app.logger.info('Received vote for %s', vote)
        data = json.dumps({'voter_id': voter_id, 'vote': vote})
        redis.rpush('votes', data)

    try:
        # Leer mensaje de Kafka
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            temperature = "No new data"
        elif msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                pass
            elif msg.error():
                raise KafkaException(msg.error())
        else:
            # Obtener el valor de temperatura del mensaje
            temperature = msg.value().decode('utf-8')
            app.logger.info(f"Temperatura recibida: {temperature} °C")
    except Exception as e:
        app.logger.error(f"Error consumiendo de Kafka: {e}")


    resp = make_response(render_template(
        'index.html',
        option_a=option_a,
        option_b=option_b,
        hostname=hostname,
        vote=vote,
        temperature=temperature
    ))
    resp.set_cookie('voter_id', voter_id)
    return resp


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=80, debug=True, threaded=True)
