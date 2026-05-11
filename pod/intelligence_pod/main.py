# VoidERP/pods/intelligence_pod/main.py

import time
import json
import os
import random
import pika

RABBITMQ_URL = os.environ.get("RABBITMQ_URL", "amqp://localhost")
SOURCE_QUEUE = "intelligence_queue"
TARGET_QUEUE = "execution_queue"

class IntelligenceAgent:
    def __init__(self, amqp_url: str):
        self.amqp_url = amqp_url
        self.connection = None
        self.channel = None
        self._connect_rabbitmq()

    def _connect_rabbitmq(self):
        try:
            parameters = pika.URLParameters(self.amqp_url)
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=SOURCE_QUEUE, durable=True)
            self.channel.queue_declare(queue=TARGET_QUEUE, durable=True)
            self.channel.basic_qos(prefetch_count=1)
            
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] system_status: connected | service: rabbitmq")
        except Exception as e:
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] system_status: error | service: rabbitmq | error: {e}")
            raise

    def compute_technical_analysis(self, tx_amount: float) -> dict:
        base_volatility = 0.02 + (tx_amount / 500000.0)
        atr_value = round(base_volatility * random.uniform(0.8, 1.2), 4)
        
        return {
            "atr_value": atr_value,
            "volatility_index": round(base_volatility, 4)
        }

    def compute_ml_classification(self, anomaly_score: float) -> dict:
        # Strict Classification: Sinyal kesinlikle 0 veya 1 olmalıdır.
        confidence = random.uniform(0.50, 0.99)
        
        # Eğer anomali skoru yüksekse, modelin işlemi 'Riskli/Hedge Edilmeli' (1) olarak sınıflandırma ihtimali artar.
        if anomaly_score > 0.75:
            signal = 1 if confidence > 0.60 else 0
        else:
            signal = 0 if confidence > 0.55 else 1

        return {
            "ml_classification_signal": signal,
            "ml_confidence": round(confidence, 4)
        }

    def compute_sentiment(self) -> float:
        """Bloomberg/Reuters haber akışı simülasyonu."""
        return round(random.uniform(-1.0, 1.0), 4)

    def process_message(self, ch, method, properties, body):
        """RabbitMQ'dan gelen mesajı işleyen asenkron callback fonksiyonu."""
        raw_data = json.loads(body)
        tx_id = raw_data.get("tx_id", "UNKNOWN")
        
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] action: consume | queue: {SOURCE_QUEUE} | tx_id: {tx_id}")

        try:
            # 1. Alt Ajan Görevleri (TA, ML, Sentiment)
            ta_data = self.compute_technical_analysis(raw_data.get("amount", 0))
            ml_data = self.compute_ml_classification(raw_data.get("anomaly_score", 0))
            sentiment_score = self.compute_sentiment()

            # 2. Intelligence Bundle (Atomic Payload) Oluşturulması
            # Tüm alt sinyaller tek bir pakette birleştirilir. Kısmi veri aktarımı yasaktır.
            intelligence_bundle = {
                "tx_id": tx_id,
                "timestamp": time.time(),
                "original_amount": raw_data.get("amount"),
                "atr_value": ta_data["atr_value"],
                "volatility_index": ta_data["volatility_index"],
                "ml_classification_signal": ml_data["ml_classification_signal"],
                "ml_confidence": ml_data["ml_confidence"],
                "sentiment_score": sentiment_score
            }

            # 3. Control Pod'a İletim (Publish)
            ch.basic_publish(
                exchange='',
                routing_key=TARGET_QUEUE,
                body=json.dumps(intelligence_bundle),
                properties=pika.BasicProperties(
                    delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
                )
            )
            
            # Mesajın RabbitMQ'dan silinmesi için onay (Acknowledge) gönder
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] action: publish | status: success | queue: {TARGET_QUEUE} | tx_id: {tx_id} | signal: {ml_data['ml_classification_signal']}")

        except Exception as e:
            # Hata durumunda mesajı reddet ve kuyrukta kalmasını engelle (Dead Letter Queue'ya düşmesi için requeue=False)
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] action: process | status: error | tx_id: {tx_id} | error: {str(e)}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def start_consuming(self):
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] pod_status: active | pod_id: intelligence_pod | listening: {SOURCE_QUEUE}")
        self.channel.basic_consume(queue=SOURCE_QUEUE, on_message_callback=self.process_message)
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            self.channel.stop_consuming()
            self.connection.close()
            print(f"\n[{time.strftime('%Y-%m-%d %H:%M:%S')}] pod_status: shutting_down | pod_id: intelligence_pod")

if __name__ == "__main__":
    agent = IntelligenceAgent(amqp_url=RABBITMQ_URL)
    agent.start_consuming()