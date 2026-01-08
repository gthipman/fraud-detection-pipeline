"""
ASEAN Transaction Generator for Fraud Detection Pipeline

Generates realistic transaction events simulating Southeast Asian payment patterns:
- PromptPay (Thailand): Mobile proxy, instant P2P, high volume
- QRIS (Indonesia): QR merchant payments, multi-wallet
- GCash/PayMaya (Philippines): E-wallet, remittances, rural inclusion
- PayNow (Singapore): NRIC/mobile proxy, cross-border
- KHQR (Cambodia): QR payments, dual currency USD/KHR

Author: Gunner | Portfolio Project Q2 2026
"""

import json
import random
import uuid
import time
import logging
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from typing import Optional, List
from enum import Enum
import threading
from collections import defaultdict

from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# =============================================================================
# ENUMS & DATA CLASSES
# =============================================================================

class Currency(str, Enum):
    THB = "THB"  # Thai Baht
    IDR = "IDR"  # Indonesian Rupiah
    PHP = "PHP"  # Philippine Peso
    SGD = "SGD"  # Singapore Dollar
    USD = "USD"  # US Dollar (remittances)
    KHR = "KHR"  # Cambodian Riel
    MYR = "MYR"  # Malaysian Ringgit
    VND = "VND"  # Vietnamese Dong


class PaymentRail(str, Enum):
    PROMPTPAY = "PROMPTPAY"
    QRIS = "QRIS"
    GCASH = "GCASH"
    PAYMAYA = "PAYMAYA"
    PAYNOW = "PAYNOW"
    KHQR = "KHQR"
    DUITNOW = "DUITNOW"
    CARD = "CARD"
    BANK_TRANSFER = "BANK_TRANSFER"
    WALLET_TRANSFER = "WALLET_TRANSFER"


class TransactionType(str, Enum):
    PAYMENT = "PAYMENT"
    P2P_TRANSFER = "P2P_TRANSFER"
    MERCHANT_PAYMENT = "MERCHANT_PAYMENT"
    BILL_PAYMENT = "BILL_PAYMENT"
    TOP_UP = "TOP_UP"
    WITHDRAWAL = "WITHDRAWAL"
    REMITTANCE = "REMITTANCE"
    QR_PAYMENT = "QR_PAYMENT"


class DeviceType(str, Enum):
    MOBILE_APP = "MOBILE_APP"
    WEB = "WEB"
    POS = "POS"
    ATM = "ATM"
    API = "API"


@dataclass
class Transaction:
    """Transaction event matching Avro schema"""
    transaction_id: str
    user_id: str
    amount: float
    currency: str
    payment_rail: str
    transaction_type: str
    merchant_id: Optional[str]
    merchant_category_code: Optional[str]
    device_id: str
    device_type: str
    ip_address: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]
    country_code: str
    recipient_id: Optional[str]
    recipient_country: Optional[str]
    is_first_device_txn: bool
    account_age_days: int
    timestamp: int  # milliseconds
    event_time: int  # milliseconds

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())


# =============================================================================
# ASEAN PAYMENT PROFILES
# =============================================================================

@dataclass
class PaymentRailProfile:
    """Configuration for each payment rail's characteristics"""
    rail: PaymentRail
    currency: Currency
    country: str
    avg_amount: float
    std_amount: float
    min_amount: float
    max_amount: float
    p2p_ratio: float  # Ratio of P2P vs merchant payments
    typical_mccs: List[str]
    peak_hours: List[int]  # Local hours with high activity
    lat_range: tuple
    lon_range: tuple


# ASEAN Payment Rail Profiles
PAYMENT_PROFILES = {
    PaymentRail.PROMPTPAY: PaymentRailProfile(
        rail=PaymentRail.PROMPTPAY,
        currency=Currency.THB,
        country="TH",
        avg_amount=500,  # ~14 USD
        std_amount=800,
        min_amount=1,  # Supports micro-transactions
        max_amount=50000,
        p2p_ratio=0.65,  # High P2P usage
        typical_mccs=["5411", "5812", "5814", "7011", "5499"],  # Grocery, restaurants, hotels
        peak_hours=[12, 13, 18, 19, 20],  # Lunch and dinner
        lat_range=(5.6, 20.5),  # Thailand bounds
        lon_range=(97.3, 105.6),
    ),
    PaymentRail.QRIS: PaymentRailProfile(
        rail=PaymentRail.QRIS,
        currency=Currency.IDR,
        country="ID",
        avg_amount=75000,  # ~5 USD
        std_amount=150000,
        min_amount=1000,
        max_amount=10000000,
        p2p_ratio=0.3,  # More merchant-focused
        typical_mccs=["5411", "5812", "5814", "5541", "5942"],  # QR merchant payments
        peak_hours=[11, 12, 13, 18, 19],
        lat_range=(-11.0, 6.0),  # Indonesia bounds
        lon_range=(95.0, 141.0),
    ),
    PaymentRail.GCASH: PaymentRailProfile(
        rail=PaymentRail.GCASH,
        currency=Currency.PHP,
        country="PH",
        avg_amount=1500,  # ~27 USD
        std_amount=3000,
        min_amount=1,
        max_amount=100000,
        p2p_ratio=0.55,  # Mix of P2P and bills
        typical_mccs=["5411", "5812", "4814", "6300", "5311"],  # Utilities, remittance
        peak_hours=[9, 10, 15, 18, 19],  # Payday patterns
        lat_range=(4.5, 21.0),  # Philippines bounds
        lon_range=(116.0, 127.0),
    ),
    PaymentRail.PAYNOW: PaymentRailProfile(
        rail=PaymentRail.PAYNOW,
        currency=Currency.SGD,
        country="SG",
        avg_amount=150,  # ~110 USD
        std_amount=300,
        min_amount=0.01,
        max_amount=200000,
        p2p_ratio=0.5,
        typical_mccs=["5411", "5812", "5814", "5912", "5311"],
        peak_hours=[12, 13, 18, 19, 20],
        lat_range=(1.15, 1.47),  # Singapore bounds
        lon_range=(103.6, 104.1),
    ),
    PaymentRail.KHQR: PaymentRailProfile(
        rail=PaymentRail.KHQR,
        currency=Currency.USD,  # Dual currency but USD common
        country="KH",
        avg_amount=25,  # USD
        std_amount=50,
        min_amount=0.25,
        max_amount=5000,
        p2p_ratio=0.4,
        typical_mccs=["5411", "5812", "5814", "5541"],
        peak_hours=[11, 12, 17, 18, 19],
        lat_range=(10.0, 14.7),  # Cambodia bounds
        lon_range=(102.3, 107.6),
    ),
}


# =============================================================================
# USER & DEVICE SIMULATION
# =============================================================================

class UserProfile:
    """Simulated user with behavioral patterns"""
    
    def __init__(self, user_id: str, home_country: str, account_age_days: int):
        self.user_id = user_id
        self.home_country = home_country
        self.account_age_days = account_age_days
        self.devices: List[str] = [f"device_{uuid.uuid4().hex[:8]}"]
        self.device_ages: dict = {self.devices[0]: random.randint(1, account_age_days)}
        self.typical_amount = random.gauss(500, 200)
        self.is_remittance_sender = random.random() < 0.15  # 15% send remittances
        self.transaction_count = 0
        self.last_txn_time = None
        self.last_location = None
        
    def add_device(self) -> str:
        """Add new device (potential fraud signal)"""
        device_id = f"device_{uuid.uuid4().hex[:8]}"
        self.devices.append(device_id)
        self.device_ages[device_id] = 0
        return device_id
    
    def get_device(self, fraud_scenario: bool = False) -> tuple:
        """Get device for transaction, optionally from new device"""
        if fraud_scenario and random.random() < 0.3:
            device_id = self.add_device()
            is_first = True
        else:
            device_id = random.choice(self.devices)
            is_first = self.device_ages[device_id] == 0
            self.device_ages[device_id] += 1
        return device_id, is_first


class UserPool:
    """Pool of simulated users"""
    
    def __init__(self, size: int = 1000):
        self.users: dict = {}
        self._create_users(size)
        
    def _create_users(self, size: int):
        """Create diverse user pool"""
        countries = ["TH", "ID", "PH", "SG", "KH", "MY", "VN"]
        weights = [0.25, 0.30, 0.20, 0.10, 0.05, 0.05, 0.05]  # Population-weighted
        
        for i in range(size):
            user_id = f"user_{uuid.uuid4().hex[:12]}"
            country = random.choices(countries, weights)[0]
            
            # Account age distribution: mix of new and established users
            # Financial inclusion: many new digital users
            if random.random() < 0.2:
                account_age = random.randint(1, 30)  # New users
            elif random.random() < 0.5:
                account_age = random.randint(30, 180)  # Medium tenure
            else:
                account_age = random.randint(180, 1000)  # Established
                
            self.users[user_id] = UserProfile(user_id, country, account_age)
            
    def get_random_user(self) -> UserProfile:
        return random.choice(list(self.users.values()))
    
    def get_user(self, user_id: str) -> Optional[UserProfile]:
        return self.users.get(user_id)


# =============================================================================
# FRAUD SCENARIOS
# =============================================================================

class FraudScenario(str, Enum):
    """Fraud patterns to inject for testing detection rules"""
    VELOCITY_BURST = "velocity_burst"  # Many transactions quickly
    AMOUNT_ANOMALY = "amount_anomaly"  # Unusually large amount
    GEO_IMPOSSIBLE = "geo_impossible"  # Location jump
    SMALL_LARGE = "small_large"  # Small test then large
    NEW_DEVICE_HIGH = "new_device_high"  # New device + high value
    ACCOUNT_TAKEOVER = "account_takeover"  # Multiple signals combined


class FraudInjector:
    """Injects fraud patterns into transaction stream"""
    
    def __init__(self, fraud_rate: float = 0.02):
        """
        Args:
            fraud_rate: Percentage of transactions that are fraudulent (default 2%)
        """
        self.fraud_rate = fraud_rate
        self.active_scenarios: dict = defaultdict(list)
        
    def should_inject_fraud(self) -> bool:
        return random.random() < self.fraud_rate
    
    def get_scenario(self) -> FraudScenario:
        """Weighted random selection of fraud scenarios"""
        scenarios = list(FraudScenario)
        weights = [0.25, 0.20, 0.15, 0.15, 0.15, 0.10]
        return random.choices(scenarios, weights)[0]
    
    def apply_scenario(
        self, 
        user: UserProfile, 
        profile: PaymentRailProfile,
        scenario: FraudScenario
    ) -> List[Transaction]:
        """Generate fraudulent transaction(s) for scenario"""
        transactions = []
        now = int(time.time() * 1000)
        
        if scenario == FraudScenario.VELOCITY_BURST:
            # 12+ transactions in 1 hour (triggers velocity rule)
            for i in range(random.randint(12, 20)):
                txn = self._create_transaction(
                    user, profile, now + i * 1000 * random.randint(30, 180)
                )
                transactions.append(txn)
                
        elif scenario == FraudScenario.AMOUNT_ANOMALY:
            # Amount > 3x user's average
            txn = self._create_transaction(
                user, profile, now,
                amount=user.typical_amount * random.uniform(4, 10)
            )
            transactions.append(txn)
            
        elif scenario == FraudScenario.GEO_IMPOSSIBLE:
            # Two transactions from different countries within 30 min
            txn1 = self._create_transaction(user, profile, now)
            
            # Second transaction from different country
            other_profiles = [p for p in PAYMENT_PROFILES.values() 
                           if p.country != profile.country]
            other_profile = random.choice(other_profiles)
            txn2 = self._create_transaction(
                user, other_profile, now + random.randint(5, 25) * 60 * 1000
            )
            transactions.extend([txn1, txn2])
            
        elif scenario == FraudScenario.SMALL_LARGE:
            # Small transaction (<$10 equivalent) then large (>$500) within 5 min
            small_amount = random.uniform(1, 10) * self._get_usd_rate(profile.currency)
            large_amount = random.uniform(500, 2000) * self._get_usd_rate(profile.currency)
            
            txn1 = self._create_transaction(user, profile, now, amount=small_amount)
            txn2 = self._create_transaction(
                user, profile, now + random.randint(60, 240) * 1000,
                amount=large_amount
            )
            transactions.extend([txn1, txn2])
            
        elif scenario == FraudScenario.NEW_DEVICE_HIGH:
            # First transaction from new device AND amount > $200 equivalent
            device_id = user.add_device()
            high_amount = random.uniform(250, 1000) * self._get_usd_rate(profile.currency)
            txn = self._create_transaction(
                user, profile, now,
                amount=high_amount,
                device_id=device_id,
                is_first_device=True
            )
            transactions.append(txn)
            
        elif scenario == FraudScenario.ACCOUNT_TAKEOVER:
            # Combined: new device + different country + high amount
            device_id = user.add_device()
            other_profiles = [p for p in PAYMENT_PROFILES.values() 
                           if p.country != user.home_country]
            other_profile = random.choice(other_profiles)
            high_amount = random.uniform(500, 2000) * self._get_usd_rate(other_profile.currency)
            
            txn = self._create_transaction(
                user, other_profile, now,
                amount=high_amount,
                device_id=device_id,
                is_first_device=True
            )
            transactions.append(txn)
            
        return transactions
    
    def _create_transaction(
        self,
        user: UserProfile,
        profile: PaymentRailProfile,
        timestamp: int,
        amount: Optional[float] = None,
        device_id: Optional[str] = None,
        is_first_device: bool = False
    ) -> Transaction:
        """Create transaction with given parameters"""
        
        if amount is None:
            amount = max(
                profile.min_amount,
                min(profile.max_amount, random.gauss(profile.avg_amount, profile.std_amount))
            )
            
        if device_id is None:
            device_id, is_first_device = user.get_device()
            
        is_p2p = random.random() < profile.p2p_ratio
        
        return Transaction(
            transaction_id=f"txn_{uuid.uuid4().hex}",
            user_id=user.user_id,
            amount=round(amount, 2),
            currency=profile.currency.value,
            payment_rail=profile.rail.value,
            transaction_type=(TransactionType.P2P_TRANSFER.value if is_p2p 
                            else TransactionType.MERCHANT_PAYMENT.value),
            merchant_id=None if is_p2p else f"merchant_{random.randint(1000, 9999)}",
            merchant_category_code=None if is_p2p else random.choice(profile.typical_mccs),
            device_id=device_id,
            device_type=DeviceType.MOBILE_APP.value,
            ip_address=self._generate_ip(profile.country),
            latitude=random.uniform(*profile.lat_range),
            longitude=random.uniform(*profile.lon_range),
            country_code=profile.country,
            recipient_id=f"user_{uuid.uuid4().hex[:12]}" if is_p2p else None,
            recipient_country=profile.country if is_p2p else None,
            is_first_device_txn=is_first_device,
            account_age_days=user.account_age_days,
            timestamp=timestamp,
            event_time=timestamp
        )
    
    def _get_usd_rate(self, currency: Currency) -> float:
        """Approximate USD exchange rates"""
        rates = {
            Currency.THB: 35,
            Currency.IDR: 15000,
            Currency.PHP: 56,
            Currency.SGD: 1.35,
            Currency.USD: 1,
            Currency.KHR: 4100,
            Currency.MYR: 4.5,
            Currency.VND: 24000,
        }
        return rates.get(currency, 1)
    
    def _generate_ip(self, country: str) -> str:
        """Generate IP address for country (simplified)"""
        # Using common IP ranges for each country (simplified)
        ranges = {
            "TH": (203, 150),
            "ID": (114, 124),
            "PH": (120, 28),
            "SG": (116, 14),
            "KH": (203, 189),
            "MY": (175, 139),
            "VN": (113, 160),
        }
        prefix = ranges.get(country, (192, 168))
        return f"{prefix[0]}.{prefix[1]}.{random.randint(1, 254)}.{random.randint(1, 254)}"


# =============================================================================
# TRANSACTION GENERATOR
# =============================================================================

class TransactionGenerator:
    """Main transaction generator with Kafka integration"""
    
    def __init__(
        self,
        bootstrap_servers: str = "localhost:19092",
        topic: str = "transactions",
        events_per_second: int = 100,
        user_pool_size: int = 1000,
        fraud_rate: float = 0.02
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.events_per_second = events_per_second
        self.fraud_rate = fraud_rate
        
        # Initialize components
        self.user_pool = UserPool(user_pool_size)
        self.fraud_injector = FraudInjector(fraud_rate)
        self.producer: Optional[KafkaProducer] = None
        
        # Metrics
        self.total_sent = 0
        self.fraud_sent = 0
        self.errors = 0
        self._running = False
        
    def connect(self):
        """Connect to Kafka"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: v.encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=3,
                linger_ms=5,
                batch_size=16384,
            )
            logger.info(f"Connected to Kafka at {self.bootstrap_servers}")
        except KafkaError as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise
            
    def generate_transaction(self) -> List[Transaction]:
        """Generate one or more transactions"""
        user = self.user_pool.get_random_user()
        
        # Select payment rail based on user's home country
        country_to_rail = {
            "TH": PaymentRail.PROMPTPAY,
            "ID": PaymentRail.QRIS,
            "PH": random.choice([PaymentRail.GCASH, PaymentRail.PAYMAYA]),
            "SG": PaymentRail.PAYNOW,
            "KH": PaymentRail.KHQR,
            "MY": PaymentRail.DUITNOW,
        }
        rail = country_to_rail.get(user.home_country, PaymentRail.BANK_TRANSFER)
        profile = PAYMENT_PROFILES.get(rail)
        
        if profile is None:
            # Fallback for countries without specific profile
            profile = random.choice(list(PAYMENT_PROFILES.values()))
        
        # Check if we should inject fraud
        if self.fraud_injector.should_inject_fraud():
            scenario = self.fraud_injector.get_scenario()
            transactions = self.fraud_injector.apply_scenario(user, profile, scenario)
            logger.debug(f"Injected fraud scenario: {scenario.value} for user {user.user_id}")
            return transactions
        
        # Generate normal transaction
        return [self.fraud_injector._create_transaction(user, profile, int(time.time() * 1000))]
    
    def send_transaction(self, txn: Transaction):
        """Send transaction to Kafka"""
        try:
            future = self.producer.send(
                self.topic,
                key=txn.user_id,
                value=txn.to_json()
            )
            future.add_callback(self._on_send_success)
            future.add_errback(self._on_send_error)
        except KafkaError as e:
            logger.error(f"Failed to send transaction: {e}")
            self.errors += 1
            
    def _on_send_success(self, record_metadata):
        self.total_sent += 1
        if self.total_sent % 1000 == 0:
            logger.info(f"Sent {self.total_sent} transactions, {self.fraud_sent} fraudulent")
            
    def _on_send_error(self, exc):
        logger.error(f"Send error: {exc}")
        self.errors += 1
        
    def run(self, duration_seconds: Optional[int] = None):
        """Run generator for specified duration or indefinitely"""
        self._running = True
        interval = 1.0 / self.events_per_second
        start_time = time.time()
        
        logger.info(f"Starting generator: {self.events_per_second} events/sec, "
                   f"{self.fraud_rate*100:.1f}% fraud rate")
        
        try:
            while self._running:
                if duration_seconds and (time.time() - start_time) >= duration_seconds:
                    break
                    
                transactions = self.generate_transaction()
                for txn in transactions:
                    self.send_transaction(txn)
                    
                time.sleep(interval)
                
        except KeyboardInterrupt:
            logger.info("Shutting down...")
        finally:
            self.stop()
            
    def stop(self):
        """Stop generator and flush Kafka"""
        self._running = False
        if self.producer:
            self.producer.flush()
            self.producer.close()
        logger.info(f"Generator stopped. Total: {self.total_sent}, Errors: {self.errors}")


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="ASEAN Transaction Generator")
    parser.add_argument("--bootstrap-servers", default="localhost:19092",
                       help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="transactions",
                       help="Kafka topic name")
    parser.add_argument("--rate", type=int, default=100,
                       help="Events per second")
    parser.add_argument("--users", type=int, default=1000,
                       help="Number of simulated users")
    parser.add_argument("--fraud-rate", type=float, default=0.02,
                       help="Fraud injection rate (0-1)")
    parser.add_argument("--duration", type=int, default=None,
                       help="Run duration in seconds (None = indefinite)")
    
    args = parser.parse_args()
    
    generator = TransactionGenerator(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        events_per_second=args.rate,
        user_pool_size=args.users,
        fraud_rate=args.fraud_rate
    )
    
    generator.connect()
    generator.run(duration_seconds=args.duration)


if __name__ == "__main__":
    main()
