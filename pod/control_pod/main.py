import time
import json
import os
import pika

RABBITMQ_URL = os.environ.get("RABBITMQ_URL", "amqp://localhost")
SOURCE_QUEUE = "execution_queue"  
TARGET_QUEUE = "sap_execution"

TRUST_THRESHOLD = 0.60
ECE_REJECT_THRESHOLD = 0.15
MIN_RISK_REWARD = 1.5

class ControlAgent:
    def __init__(self, amqp_url: str):
        self.amqp_url = amqp_url
        self.connection = None
        self.channel = None
        self.historical_ece = 0.08
        
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

    def master_decision(self, bundle: dict) -> float:
        """Farklı ajanlardan gelen sinyalleri ağırlıklandırarak tek bir Güven Skoru (Trust Score) üretir."""
        
        ml_signal = bundle.get("ml_classification_signal", 0)
        ml_conf = bundle.get("ml_confidence", 0.0)
        sentiment = bundle.get("sentiment_score", 0.0)
        
        # ML modeli "1" (Riskli) diyorsa güveni artır, "0" diyorsa sentiment'e daha çok bak
        if ml_signal == 1:
            base_trust = (ml_conf * 0.7) + (sentiment * 0.3)
        else:
            base_trust = (ml_conf * 0.4) + (sentiment * 0.6)
            
        return max(0.0, min(base_trust, 1.0))

    # ECE kalibrasyonu
    def auditor_check(self, trust_score: float) -> bool:
        # ECE ne kadar yüksekse, AI o kadar overconfident veya hatalıdır
        calibrated_trust = trust_score - self.historical_ece
        
        if self.historical_ece > ECE_REJECT_THRESHOLD:
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] action: auditor_check | status: BLOCKED | reason: high_ece_error | current_ece: {self.historical_ece}")
            return False
            
        if calibrated_trust < TRUST_THRESHOLD:
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] action: auditor_check | status: DROP | reason: low_calibrated_trust | raw: {trust_score:.2f} | calibrated: {calibrated_trust:.2f}")
            return False
            
        return True

    def risk_engine(self, bundle: dict) -> dict:
        """Yapay zeka onayı alan işlemi, kesin (deterministik) finansal kurallarla (ATR) boyutlandırır."""
        
        atr = bundle.get("atr_value", 1.0)
        amount = bundle.get("original_amount", 0.0)
        
        # OCO (One Cancels Other) Emir Simülasyonu
        # ATR'ye göre Stop-Loss (Zarar Kes) ve Take-Profit (Kâr Al) hedefleri
        stop_loss_dist = atr * 2.0
        take_profit_dist = atr * 3.5
        
        risk_reward = take_profit_dist / stop_loss_dist
        
        if risk_reward < MIN_RISK_REWARD:
            return {"status": "DEFER", "reason": f"low_rr_ratio ({risk_reward:.2f})"}
            
        # Kelly Criterion Simülasyonu: Yüksek volatilite (ATR) varsa işlem boyutunu (amount) düşür.
        max_notional = 50000.0
        kelly_sizing = min(amount, max_notional * (1.0 / (atr * 10)))
        
        return {
            "status": "APPROVED",
            "execution_type": "hedge",
            "notional_value": round(kelly_sizing, 2),
            "stop_loss_atr": round(stop_loss_dist, 4)
        }

    def process_message(self, ch, method, properties, body):
        raw_bundle = json.loads(body)
        tx_id = raw_bundle.get("tx_id", "UNKNOWN")
        
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] action: consume | queue: {SOURCE_QUEUE} | tx_id: {tx_id}")

        try:
            # 1. Master Decision
            trust_score = self.master_decision(raw_bundle)
            
            # 2. Auditor AI (Gatekeeper)
            is_calibrated = self.auditor_check(trust_score)
            
            if not is_calibrated:
                # İşlem reddedildi. Mesajı yut (ack) ama işleme koyma.
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            # 3. Risk Engine
            risk_decision = self.risk_engine(raw_bundle)
            
            if risk_decision["status"] == "DEFER":
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] action: risk_check | status: DEFER | reason: {risk_decision['reason']} | tx_id: {tx_id}")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
                
            execution_order = {
                "tx_id": tx_id,
                "timestamp": time.time(),
                "action": risk_decision["execution_type"],
                "notional": risk_decision["notional_value"],
                "calibrated_trust": round(trust_score - self.historical_ece, 4)
            }

            ch.basic_publish(
                exchange='',
                routing_key=TARGET_QUEUE,
                body=json.dumps(execution_order),
                properties=pika.BasicProperties(delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE)
            )
            
            ch.basic_ack(delivery_tag=method.delivery_tag)
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] system_status: active | pod_id: control_pod | risk_score: {self.historical_ece} | calibration: successful | method: ece_check")
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] action: execution:{risk_decision['execution_type']} | target: sap_po_service | ID: {tx_id} | notional: {execution_order['notional']} USD")

        except Exception as e:
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] action: process | status: error | tx_id: {tx_id} | error: {str(e)}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def start_consuming(self):
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] pod_status: active | pod_id: control_pod | listening: {SOURCE_QUEUE}")
        self.channel.basic_consume(queue=SOURCE_QUEUE, on_message_callback=self.process_message)
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            self.channel.stop_consuming()
            self.connection.close()
            print(f"\n[{time.strftime('%Y-%m-%d %H:%M:%S')}] pod_status: shutting_down | pod_id: control_pod")

if __name__ == "__main__":
    agent = ControlAgent(amqp_url=RABBITMQ_URL)
    agent.start_consuming()