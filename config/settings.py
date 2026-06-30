import os
from pathlib import Path

# Caminhos do projeto
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
OUTPUT_DIR = BASE_DIR / "output"

# Lista de criptomoedas disponíveis
CRYPTO_FILES = [
    "coin_Aave.csv",
    "coin_BinanceCoin.csv",
    "coin_Bitcoin.csv",
    "coin_Cardano.csv",
    "coin_ChainLink.csv",
    "coin_Cosmos.csv",
    "coin_CryptocomCoin.csv",
    "coin_Dogecoin.csv",
    "coin_EOS.csv",
    "coin_Ethereum.csv",
    "coin_Iota.csv",
    "coin_Litecoin.csv",
    "coin_Monero.csv",
    "coin_NEM.csv",
    "coin_Polkadot.csv",
    "coin_Solana.csv",
    "coin_Stellar.csv",
    "coin_Tether.csv",
    "coin_Tron.csv",
    "coin_USDCoin.csv",
    "coin_Uniswap.csv",
    "coin_WrappedBitcoin.csv",
    "coin_XRP.csv"
]

# Config S3 (preencher quando for usar)
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY", "")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY", "")
S3_BUCKET_RAW = os.getenv("S3_BUCKET_RAW", "crypto-analysis-raw")
S3_BUCKET_PROCESSED = os.getenv("S3_BUCKET_PROCESSED", "crypto-analysis-processed")