import time
import json
import uuid
import random
import pika
import os
from typing import Dict, Any

RABBITMQ_URL = os.environ.get("RABBITMQ_URL", "amqp://localhost")
TARGET_QUEUE = "intelligence_queue"

class DataIngestionAgent:
    def __init__(self, amqp_url: str):
        self.source_system = "SAP_S4HANA_MOCK"
        self.amqp_url = amqp_url
        self.connection = None
        self.channel = None
        self._connect_rabbitmq()

    def _connect_rabbitmq(self):
        try:
            parameters = pika.URLParameters(self.amqp_url)
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=TARGET_QUEUE, durable=True)
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] system_status: connected | service: rabbitmq")
        except Exception as e:
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] system_status: error | service: rabbitmq | error: {e}")
            raise

    def fetch_mock_data(self) -> list[Dict[str, Any]]:
        """
        Gerçek SAP OData entegrasyonu yerine, gerçeğe yakın finansal işlemler üretir.
        Jüriye göstermek için anomali (büyük tutar) ve mükerrer kayıt senaryolarını içerir.
        """
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] action: fetching_data | source: {self.source_system}")
        
        # Gerçekçi veri dağılımı: %80 normal, %20 yüksek tutarlı (anomali riski)
        is_large_order = random.random() > 0.8
        amount = random.uniform(50000, 150000) if is_large_order else random.uniform(500, 15000)

        # Basit matematiksel anomali skoru (Büyük işlem = Yüksek skor)
        anomaly_score = min(amount / 200000.0, 0.99) + random.uniform(0.0, 0.1)

        mock_tx = {
            "tx_id": f"TXN-{uuid.uuid4().hex[:8].upper()}",
            "source_system": self.source_system,
            "timestamp": time.time(),
            "amount": round(amount, 2),
            "currency": "USD",
            "is_duplicate": random.random() > 0.95, # %5 mükerrer fatura senaryosu
            "anomaly_score": round(min(anomaly_score, 1.0), 4)
        }
        
        return [mock_tx]

    def audit_and_publish(self):
        """Çekilen veriyi denetler ve RabbitMQ'ya (Intelligence Pod'a) yollar."""
        transactions = self.fetch_mock_data()
        
        for tx in transactions:
            # 1. Denetim (Audit): Deterministik kural - Mükerrer ise anında reddet!
            if tx.get("is_duplicate"):
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] action: audit_check | status: blocked | reason: duplicate_invoice | tx_id: {tx['tx_id']}")
                continue # Mesajı kuyruğa atma, atla.
            
            # 2. Yayınlama (Publish): Temiz veriyi RabbitMQ'ya ilet.
            try:
                message = json.dumps(tx)
                self.channel.basic_publish(
                    exchange='',
                    routing_key=TARGET_QUEUE,
                    body=message,
                    properties=pika.BasicProperties(
                        delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE # Mesaj kalıcılığı
                    )
                )
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] action: publish | status: success | queue: {TARGET_QUEUE} | tx_id: {tx['tx_id']} | amount: {tx['amount']} {tx['currency']}")
            except Exception as e:
                 print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] action: publish | status: failed | tx_id: {tx['tx_id']} | error: {e}")
                 self._connect_rabbitmq() # Bağlantı koparsa yeniden dene

    def close(self):
        if self.connection and self.connection.is_open:
            self.connection.close()

if __name__ == "__main__":
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] pod_status: starting | pod_id: data_pod")
    
    agent = DataIngestionAgent(amqp_url=RABBITMQ_URL)
    
    try:
        # KEDA'nın sürekli dinlediğini varsayarak basit bir sonsuz döngü simülasyonu
        while True:
            agent.audit_and_publish()
            time.sleep(random.uniform(3.0, 7.0)) # 3 ile 7 saniye arası rastgele bekle
    except KeyboardInterrupt:
        print(f"\n[{time.strftime('%Y-%m-%d %H:%M:%S')}] pod_status: shutting_down | pod_id: data_pod")
        agent.close()