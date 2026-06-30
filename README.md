# 📊 Crypto Seasonality Analysis com PySpark

Análise de sazonalidade e volatilidade de 22 criptomoedas utilizando PySpark, processando dados históricos do Kaggle.

## 🪙 Criptomoedas analisadas

Bitcoin (BTC), Ethereum (ETH), Cardano (ADA), Polkadot (DOT), Binance Coin (BNB), Solana (SOL), Dogecoin (DOGE), XRP, Uniswap (UNI), ChainLink (LINK), Litecoin (LTC), Stellar (XLM), Monero (XMR), Cosmos (ATOM), Tron (TRX), EOS, Tether (USDT), USD Coin (USDC), Wrapped Bitcoin (WBTC), Aave (AAVE), NEM (XEM), Iota (MIOTA)

## 📈 Análises realizadas

| Análise | Descrição |
|---------|-----------|
| Sazonalidade Mensal | Retorno médio por mês do ano |
| Sazonalidade por Dia da Semana | Retorno médio por dia (ex: "segunda-feira é dia de queda?") |
| Volatilidade Histórica | Desvio padrão móvel de 30 dias |
| Maiores Altas/Quedas | Top 20 variações diárias extremas |
| Correlação com Bitcoin | Covariância das altcoins com BTC |

## 🏗️ Arquitetura
Kaggle CSV (download manual)
↓
data/raw/ (22 arquivos coin_*.csv)
↓
PySpark (leitura + transformação)
↓
┌─────────────────────────────┐
│ Análises de negócio: │
│ - Sazonalidade mensal │
│ - Sazonalidade por dia │
│ - Volatilidade histórica │
│ - Maiores altas/quedas │
│ - Correlação com Bitcoin │
└─────────────────────────────┘
↓
output/ (Parquet particionado)
↓
(Produção: S3 → Athena)

## 🚀 Como executar

### Pré-requisitos
- Python 3.8+
- Java 11 (OpenJDK)
- macOS / Linux / Windows

### Instalação

```bash
# Clone o repositório
git clone https://github.com/seu-usuario/crypto-seasonality-spark.git
cd crypto-seasonality-spark

# Crie o ambiente virtual
python3 -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Instale as dependências
pip install -r requirements.txt

Download dos dados
Acesse Cryptocurrency Historical Prices no Kaggle

Baixe o dataset

Extraia os arquivos coin_*.csv para a pasta data/raw/

🛠️ Tecnologias
PySpark - Processamento distribuído

Parquet - Armazenamento colunar otimizado

AWS S3 - Storage (planejado)

AWS Athena - Consultas SQL (planejado)