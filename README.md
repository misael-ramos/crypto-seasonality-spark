# 🔥 crypto-seasonality-spark

Análise de **sazonalidade e volatilidade histórica** de 22 criptomoedas com **Apache Spark (PySpark)**, processando anos de dados do Kaggle em modo distribuído local com suporte a AWS S3 e Athena.

> **🆕 v1.1 — Release de Otimização:** Pipeline reescrito com 13 otimizações de performance. Veja o [changelog](#-changelog) completo abaixo.

Projeto parte de uma série de engenharia de dados — usa o mesmo domínio de dados do [crypto-pipeline](https://github.com/misael-ramos/crypto-pipeline) e [crypto-dw](https://github.com/misael-ramos/crypto-dw), mas com foco em análise histórica em escala.

---

## 🎯 Objetivo de negócio

Responder perguntas práticas para investidores e traders usando anos de dados históricos:

| Pergunta | Análise |
|---|---|
| Existe mês historicamente melhor para comprar/vender? | Sazonalidade mensal |
| Algum dia da semana tem padrão de alta consistente? | Sazonalidade semanal |
| Quais moedas são mais arriscadas historicamente? | Volatilidade histórica (σ móvel 30d) |
| Quais foram os maiores crashes e rallies? | Variações extremas |
| Altcoins seguem o Bitcoin? | Correlação com BTC |

---

## 📊 Principais insights encontrados

### Sazonalidade mensal
| Mês | Retorno médio diário | Tendência |
|---|---|---|
| Janeiro | +0.8% | 📈 Alta |
| Março | -0.4% | 📉 Queda |
| Outubro | +1.2% | 📈 Maior do ano ("Uptober") |
| Novembro | +0.9% | 📈 Continuação do rali |
| Dezembro | -0.3% | 📉 Realização de lucros |

### Ranking de volatilidade histórica
| Moeda | Volatilidade diária (σ) | Classificação |
|---|---|---|
| Dogecoin | ~8.5% | 🔴 Extrema |
| XRP | ~5.1% | 🟠 Alta |
| Ethereum | ~4.3% | 🟡 Moderada |
| Bitcoin | ~3.8% | 🟡 Moderada |
| Tether | ~0.1% | 🟢 Estável |

### Maiores eventos extremos
- **Maior alta:** XRP — **+252%** em um único dia (2018)
- **Maior queda:** Ethereum — **-52%** (crash de Maio/2021)

---

## 💡 Decisões técnicas

| Decisão | Alternativa considerada | Justificativa |
|---|---|---|
| PySpark em vez de Pandas | Pandas | Pandas carrega tudo em memória — com anos de dados de 22 moedas, Spark distribui o processamento |
| Schema definido na leitura | `inferSchema=True` | `inferSchema` lê o arquivo 2x para descobrir tipos — schema predefinido é 50% mais rápido |
| `isEmpty()` em vez de `count()` | `df.count() > 0` | `isEmpty()` para na primeira linha (O(1)); `count()` varre todo o DataFrame (O(n)) |
| `persist(MEMORY_AND_DISK)` | `.cache()` | `.cache()` usa só RAM — `MEMORY_AND_DISK` usa disco como fallback, evitando OOM |
| Funções nativas vs UDFs | UDFs Python | Funções nativas (`when()`, `lag()`) são compiladas pela JVM — 10-100x mais rápido |
| `repartition()` + `maxRecordsPerFile` | Escrita direta | Evita small files — Athena e S3 têm melhor performance com arquivos de ~128MB |
| Parquet + SNAPPY | CSV | Formato colunar com compressão — leitura seletiva de colunas e custo menor no Athena |

---

## 🪙 Criptomoedas analisadas (22)

Bitcoin, Ethereum, Cardano, Polkadot, BNB, Solana, Dogecoin, XRP, Uniswap, ChainLink, Litecoin, Stellar, Monero, Cosmos, Tron, EOS, Tether, USD Coin, Wrapped Bitcoin, Aave, NEM, Iota

---

## 🏗️ Arquitetura

```
Kaggle CSV (22 arquivos coin_*.csv, dados desde 2013)
        ↓
data/raw/ — armazenamento local dos CSVs
        ↓
PySpark local[*] — processamento distribuído na máquina
  ├── Schema definido (evita inferSchema)
  ├── Filtro cedo (remove nulos antes das transformações)
  ├── Feature engineering: day_name, month_name, daily_change_pct
  ├── persist(MEMORY_AND_DISK) — cache estratégico
  ├── 5 análises reutilizando o cache
  └── unpersist() — libera memória
        ↓
output/ (Parquet particionado por year/month)  ←  local
s3://bucket/processed/                          ←  produção
        ↓
Amazon Athena — consultas SQL sobre os resultados
```

---

## ⚡ 13 Otimizações Spark aplicadas (v1.1)

| # | Otimização | Ganho estimado |
|---|---|---|
| 1 | Schema definido na leitura CSV | 50% mais rápido |
| 2 | Kryo Serializer | 10x na serialização |
| 3 | Adaptive Query Execution (AQE) | Redução automática de skew |
| 4 | `spark.driver.memory=4g` | Previne OOM |
| 5 | Filtro cedo (`filter` antes das transformações) | Menos dados processados |
| 6 | Funções nativas (`when`, `lag`) sem UDFs | 10-100x mais rápido |
| 7 | `isEmpty()` em vez de `count()` | Sub-ms vs segundos |
| 8 | `persist(MEMORY_AND_DISK)` + `unpersist()` | Evita recomputação |
| 9 | `repartition(year, month)` antes de salvar | Anti-small files |
| 10 | `maxRecordsPerFile=5000` | Arquivos balanceados |
| 11 | `partitionBy("year", "month")` | Predicate pushdown no Athena |
| 12 | `skewJoin.enabled=true` | Skew automático via AQE |
| 13 | `maxPartitionBytes=128MB` | Partições no tamanho ideal |

---

## 🚀 Como executar

```bash
git clone https://github.com/misael-ramos/crypto-seasonality-spark.git
cd crypto-seasonality-spark
python3 -m venv venv && source venv/bin/activate
pip3 install -r requirements.txt
```

Baixe o dataset no [Kaggle](https://www.kaggle.com/datasets/sudalairajkumar/cryptocurrencypricehistory) e coloque os CSVs em `data/raw/`.

```bash
# rodar localmente
python3 scripts/pyspark_job.py

# rodar com S3
python3 scripts/pyspark_job.py --s3
```

---

## 📁 Estrutura

```
crypto-seasonality-spark/
├── config/
│   └── settings.py              # caminhos S3 e lista de arquivos
├── scripts/
│   ├── pyspark_job.py           # pipeline principal com 13 otimizações
│   └── upload_to_s3.py          # upload dos CSVs para S3
├── sql/
│   └── queries_athena.sql       # queries analíticas prontas
├── data/
│   └── raw/                     # CSVs do Kaggle (não versionados)
└── output/                      # resultados locais (não versionados)
```

---

## 📝 Changelog

### v1.1 (2026-07-01) — Release de Otimização
- 13 otimizações de performance aplicadas
- `isEmpty()` substituindo `count()` em verificações
- Cache estratégico com `MEMORY_AND_DISK`
- Estratégia anti-small files com `repartition()` + `maxRecordsPerFile`
- Kryo Serializer e AQE habilitados

### v1.0 (2026-06-30) — Lançamento inicial
- Pipeline ETL: CSV → PySpark → Parquet
- 5 análises de negócio implementadas
- Suporte a processamento local e S3
- 22 criptomoedas analisadas

---

## 🔗 Projetos relacionados

- [crypto-pipeline](https://github.com/misael-ramos/crypto-pipeline) — ETL batch com S3 e Athena
- [crypto-dw](https://github.com/misael-ramos/crypto-dw) — Data Warehouse com Star Schema
- [crypto-news-streaming](https://github.com/misael-ramos/crypto-news-streaming) — streaming de sentimento com Kafka
