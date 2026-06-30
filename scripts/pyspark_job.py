"""
PySpark Job - Análise de Sazonalidade de Criptomoedas
Dataset: Cryptocurrency Historical Prices (Kaggle)
22 criptomoedas
"""

from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    col, year, month, dayofweek, dayofmonth,
    stddev, avg, max, min, lag, round as spark_round,
    row_number, desc, asc, when, input_file_name, regexp_extract,
    count, sum as spark_sum, lit   # <-- ADICIONE "lit" aqui
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, DateType
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import RAW_DIR, OUTPUT_DIR, CRYPTO_FILES


def create_spark_session():
    """Cria a SparkSession local otimizada para macOS"""
    return SparkSession.builder \
        .appName("CryptoSazonalityAnalysis") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()


def read_all_cryptos(spark):
    """Lê todos os CSVs e combina em um único DataFrame"""
    
    print("\n" + "="*60)
    print(" LEITURA DOS DADOS")
    print("="*60)
    
    dfs = []
    
    for file in CRYPTO_FILES:
        file_path = RAW_DIR / file
        if not file_path.exists():
            print(f"⚠️  Arquivo não encontrado: {file}")
            continue
        
        # Extrai o nome da moeda do nome do arquivo (ex: coin_Bitcoin.csv -> Bitcoin)
        crypto_name = file.replace("coin_", "").replace(".csv", "")
        
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(str(file_path))
        
        # Log das colunas do primeiro arquivo para debug
        if len(dfs) == 0:
            print(f"📋 Colunas encontradas: {df.columns}")
        
        # Adiciona coluna com o nome da cripto
        df = df.withColumn("crypto", lit(crypto_name))
        
        dfs.append(df)
        print(f"✅ {crypto_name}: {df.count()} registros")
    
    # Combina todos os DataFrames
    df_all = dfs[0]
    for df in dfs[1:]:
        df_all = df_all.unionByName(df, allowMissingColumns=True)
    
    print(f"\n📊 Total combinado: {df_all.count()} registros")
    print(f"📋 Colunas finais: {df_all.columns}")
    
    return df_all


def transform_data(df):
    """Limpeza, padronização e feature engineering"""
    
    print("\n" + "="*60)
    print(" TRANSFORMAÇÃO DOS DADOS")
    print("="*60)
    
    # Padroniza nomes de colunas: lowercase + underscore
    df = df.toDF(*[c.strip().lower().replace(" ", "_").replace("-", "_") for c in df.columns])
    
    print(f"Colunas padronizadas: {df.columns}")
    
    # O dataset tem: sno, name, symbol, date, high, low, open, close, volume, marketcap
    # Converte coluna de data
    df = df.withColumn("date", col("date").cast(DateType()))
    
    # Converte colunas numéricas
    numeric_cols = ["open", "high", "low", "close", "volume", "marketcap"]
    for c in numeric_cols:
        if c in df.columns:
            df = df.withColumn(c, col(c).cast(DoubleType()))
    
    # Remove linhas com data nula ou close nulo
    df = df.filter(col("date").isNotNull() & col("close").isNotNull())
    
    print(f"Registros após filtro: {df.count()}")
    
    # Cria features temporais
    df = df.withColumn("year", year("date")) \
           .withColumn("month", month("date")) \
           .withColumn("day_of_week", dayofweek("date")) \
           .withColumn("day", dayofmonth("date"))
    
    # Nomes amigáveis
    df = df.withColumn("day_name",
        when(col("day_of_week") == 1, "Domingo")
        .when(col("day_of_week") == 2, "Segunda")
        .when(col("day_of_week") == 3, "Terça")
        .when(col("day_of_week") == 4, "Quarta")
        .when(col("day_of_week") == 5, "Quinta")
        .when(col("day_of_week") == 6, "Sexta")
        .when(col("day_of_week") == 7, "Sábado")
    )
    
    df = df.withColumn("month_name",
        when(col("month") == 1, "Janeiro")
        .when(col("month") == 2, "Fevereiro")
        .when(col("month") == 3, "Março")
        .when(col("month") == 4, "Abril")
        .when(col("month") == 5, "Maio")
        .when(col("month") == 6, "Junho")
        .when(col("month") == 7, "Julho")
        .when(col("month") == 8, "Agosto")
        .when(col("month") == 9, "Setembro")
        .when(col("month") == 10, "Outubro")
        .when(col("month") == 11, "Novembro")
        .when(col("month") == 12, "Dezembro")
    )
    
    # Variação percentual diária
    window_spec = Window.partitionBy("crypto").orderBy("date")
    df = df.withColumn("prev_close", lag("close").over(window_spec))
    df = df.withColumn("daily_change_pct",
        spark_round(((col("close") - col("prev_close")) / col("prev_close")) * 100, 2)
    )
    
    # Range diário (high-low)
    if "high" in df.columns and "low" in df.columns:
        df = df.withColumn("daily_range_pct",
            spark_round(((col("high") - col("low")) / col("low")) * 100, 2)
        )
    
    # Mostra schema e amostra (limitado para evitar erros de encoding)
    df.printSchema()
    print("\nAmostra dos dados (5 linhas):")
    df.select("date", "crypto", "close", "volume", "daily_change_pct").show(5, truncate=False)
    
    return df


# =============================================
# ANÁLISES DE NEGÓCIO
# =============================================

def analise_sazonalidade_mensal(df):
    """Retorno médio por mês para cada cripto"""
    print("\n" + "="*60)
    print(" ANÁLISE 1: SAZONALIDADE MENSAL")
    print("="*60)
    
    # Geral (todas as criptos)
    print("\n--- Retorno médio mensal (todas as criptos) ---")
    result_geral = df.groupBy("month", "month_name") \
        .agg(
            spark_round(avg("daily_change_pct"), 2).alias("retorno_medio_pct"),
            spark_round(stddev("daily_change_pct"), 2).alias("volatilidade_pct")
        ) \
        .orderBy("month")
    result_geral.show(12)
    
    # Top 5 criptos por volume (proxy de relevância)
    top_cryptos = df.groupBy("crypto") \
        .agg(avg("volume").alias("avg_volume")) \
        .orderBy(col("avg_volume").desc()) \
        .limit(5)
    
    top_list = [row.crypto for row in top_cryptos.collect()]
    
    print(f"\n--- Top 5 criptos por volume: {top_list} ---")
    result_top = df.filter(col("crypto").isin(top_list)) \
        .groupBy("crypto", "month", "month_name") \
        .agg(spark_round(avg("daily_change_pct"), 2).alias("retorno_medio_pct")) \
        .orderBy("crypto", "month")
    result_top.show(60)
    
    return result_geral


def analise_sazonalidade_dia_semana(df):
    """Retorno médio por dia da semana"""
    print("\n" + "="*60)
    print(" ANÁLISE 2: SAZONALIDADE POR DIA DA SEMANA")
    print("="*60)
    
    # Bitcoin especificamente (se existir)
    btc_df = df.filter(col("crypto") == "Bitcoin")
    if btc_df.count() > 0:
        print("\n--- Bitcoin: retorno por dia da semana ---")
        btc_result = btc_df.groupBy("day_of_week", "day_name") \
            .agg(
                spark_round(avg("daily_change_pct"), 2).alias("retorno_medio_pct"),
                spark_round(stddev("daily_change_pct"), 2).alias("volatilidade_pct")
            ) \
            .orderBy("day_of_week")
        btc_result.show(7)
    
    # Geral
    print("\n--- Geral (todas as criptos): retorno por dia da semana ---")
    result = df.groupBy("day_of_week", "day_name") \
        .agg(
            spark_round(avg("daily_change_pct"), 2).alias("retorno_medio_pct"),
            spark_round(stddev("daily_change_pct"), 2).alias("volatilidade_pct"),
            count("*").alias("observacoes")
        ) \
        .orderBy("day_of_week")
    result.show(7)
    
    return result


def analise_volatilidade_historica(df, window_days=30):
    """Desvio padrão móvel do retorno diário"""
    print("\n" + "="*60)
    print(f" ANÁLISE 3: VOLATILIDADE HISTÓRICA (janela {window_days} dias)")
    print("="*60)
    
    window_spec = Window.partitionBy("crypto").orderBy("date") \
        .rowsBetween(-window_days + 1, 0)
    
    df_vol = df.withColumn("volatilidade_30d",
        spark_round(stddev("daily_change_pct").over(window_spec), 2)
    )
    
    # Top 5 criptos mais voláteis no período mais recente
    print("\n--- Top 10 criptos mais voláteis (dado mais recente) ---")
    latest_date = df_vol.agg(max("date")).collect()[0][0]
    
    top_vol = df_vol.filter(
        (col("date") == latest_date) & (col("volatilidade_30d").isNotNull())
    ).select("crypto", "date", "close", "volatilidade_30d") \
     .orderBy(col("volatilidade_30d").desc()) \
     .limit(10)
    
    top_vol.show(10, truncate=False)
    
    return df_vol


def analise_maiores_altas_quedas(df):
    """Top 20 maiores variações diárias"""
    print("\n" + "="*60)
    print(" ANÁLISE 4: MAIORES ALTAS E QUEDAS")
    print("="*60)
    
    df_filtered = df.filter(col("daily_change_pct").isNotNull())
    
    # Maiores altas
    print("\n--- 🔥 TOP 20 MAIORES ALTAS DIÁRIAS ---")
    altas = df_filtered.select("date", "crypto", "close", "daily_change_pct") \
        .orderBy(col("daily_change_pct").desc()) \
        .limit(20)
    altas.show(20, truncate=False)
    
    # Maiores quedas
    print("\n--- 💀 TOP 20 MAIORES QUEDAS DIÁRIAS ---")
    quedas = df_filtered.select("date", "crypto", "close", "daily_change_pct") \
        .orderBy(col("daily_change_pct").asc()) \
        .limit(20)
    quedas.show(20, truncate=False)
    
    return altas, quedas


def analise_correlacao_bitcoin(df):
    """Correlação do Bitcoin com outras criptos (simplificada)"""
    print("\n" + "="*60)
    print(" ANÁLISE 5: CORRELAÇÃO COM BITCOIN")
    print("="*60)
    
    btc_df = df.filter(col("crypto") == "Bitcoin") \
        .select(col("date").alias("btc_date"), col("daily_change_pct").alias("btc_change"))
    
    others_df = df.filter(col("crypto") != "Bitcoin") \
        .select(col("date"), col("crypto"), col("daily_change_pct").alias("crypto_change"))
    
    joined = others_df.join(btc_df, others_df.date == btc_df.btc_date)
    
    # Correlação aproximada: média do produto dos retornos normalizados
    corr = joined.groupBy("crypto") \
        .agg(
            spark_round(avg(col("crypto_change") * col("btc_change")), 4).alias("covariancia_btc"),
            spark_round(avg("crypto_change"), 2).alias("media_crypto"),
            spark_round(avg("btc_change"), 2).alias("media_btc")
        ) \
        .orderBy(col("covariancia_btc").desc())
    
    corr.show(22, truncate=False)
    return corr


def gerar_relatorio_final(df):
    """Relatório consolidado"""
    print("\n" + "="*60)
    print(" 📊 RELATÓRIO FINAL CONSOLIDADO")
    print("="*60)
    
    # Período dos dados
    date_range = df.agg(min("date").alias("inicio"), max("date").alias("fim")).collect()[0]
    print(f"\n📅 Período: {date_range.inicio} até {date_range.fim}")
    
    # Estatísticas por cripto
    print("\n--- Resumo por Criptomoeda ---")
    stats = df.groupBy("crypto") \
        .agg(
            count("*").alias("dias_negociados"),
            spark_round(avg("daily_change_pct"), 2).alias("retorno_medio_diario"),
            spark_round(stddev("daily_change_pct"), 2).alias("volatilidade_diaria"),
            spark_round(max("close"), 2).alias("preco_maximo"),
            spark_round(min("close"), 2).alias("preco_minimo")
        ) \
        .orderBy(col("volatilidade_diaria").desc())
    
    stats.show(25, truncate=False)
    
    # Ranking de volatilidade
    print("\n--- 🎢 Ranking de Volatilidade ---")
    stats.select("crypto", "volatilidade_diaria") \
        .orderBy(col("volatilidade_diaria").desc()) \
        .show(22, truncate=False)


def salvar_parquet(df):
    """Salva dados processados em Parquet"""
    output_path = str(OUTPUT_DIR / "processed.parquet")
    
    df.write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet(output_path)
    
    print(f"\n💾 Dados salvos em Parquet: {output_path}")
    print(f"   Total: {df.count()} registros")


def main():
    spark = create_spark_session()
    
    try:
        # Pipeline ETL
        df_raw = read_all_cryptos(spark)
        df_transformed = transform_data(df_raw)
        
        # Cache para acelerar análises
        df_transformed.cache()
        
        # Análises de negócio
        analise_sazonalidade_mensal(df_transformed)
        analise_sazonalidade_dia_semana(df_transformed)
        analise_volatilidade_historica(df_transformed)
        analise_maiores_altas_quedas(df_transformed)
        analise_correlacao_bitcoin(df_transformed)
        
        # Relatório e persistência
        gerar_relatorio_final(df_transformed)
        salvar_parquet(df_transformed)
        
        print("\n" + "="*60)
        print(" ✅ PIPELINE CONCLUÍDO COM SUCESSO!")
        print("="*60)
        
    except Exception as e:
        print(f"\n❌ Erro no pipeline: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()