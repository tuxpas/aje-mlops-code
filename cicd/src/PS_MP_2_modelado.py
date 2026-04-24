import os
import json
import glob
import traceback
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, concat, countDistinct, lit
from pyspark.ml.feature import StringIndexer
from pyspark.ml.recommendation import ALS

os.environ["AWS_DEFAULT_REGION"] = "us-east-2"

INPUT_BASE = "/opt/ml/processing/input"
OUTPUT_BASE = "/opt/ml/processing/output/modelado"
os.makedirs(OUTPUT_BASE, exist_ok=True)


def load_paises_config():
    """Returns paises_config.json files from S3."""
    s3 = boto3.client("s3")
    key = f"{CONFIG_S3_PREFIX}/paises_config.json"
    print(f"[config] Local path not found; reading from s3://{BUCKET_CONFIG}/{key}")
    obj = s3.get_object(Bucket=BUCKET_CONFIG, Key=key)
    return json.loads(obj["Body"].read().decode("utf-8"))


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("SageMaker-ALS-MultiPais")
        .config("spark.hadoop.hadoop.security.authentication", "simple")
        .config("spark.hadoop.hadoop.security.authorization", "false")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )
    return spark


def als_training_job(spark, ruta_csv_path, id_prefix):
    """
    Trains implicit ALS for a single route CSV and returns recommendations DataFrame.
    id_prefix is used to build the full id_cliente (e.g., "EC", "MX", "CAM", "PE").
    """
    ventas = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"file://{ruta_csv_path}")
    )
    if ventas.count() == 0:
        return pd.DataFrame()

    ventas = ventas.withColumn("cod_articulo_magic", col("cod_articulo_magic").cast(IntegerType()))
    sku_len = ventas.select("cod_articulo_magic").distinct().count()
    ventas = ventas.na.drop(subset=["fecha_liquidacion"])

    ventas = ventas.groupBy(
        ["id_cliente", "cod_articulo_magic", "cod_compania", "cod_cliente"]
    ).agg(countDistinct("fecha_liquidacion").alias("frecuencia"))
    ventas = ventas.withColumn("clienteId", concat(col("cod_compania"), lit("|"), col("cod_cliente")))

    indexer = StringIndexer(inputCol="clienteId", outputCol="clienteId_numeric", handleInvalid="skip")
    ventas = indexer.fit(ventas).transform(ventas)

    als_records = ventas.withColumn("rating", col("frecuencia").cast("float"))
    als_records = als_records.withColumn("clienteId_numeric", col("clienteId_numeric").cast("integer"))
    als_records = als_records.select("clienteId", "clienteId_numeric", "cod_articulo_magic", "rating")
    als_records = als_records.dropDuplicates(["clienteId", "cod_articulo_magic"])
    als_records = als_records.dropna(subset=["clienteId_numeric", "cod_articulo_magic", "rating"])

    if als_records.count() < 10:
        print(f"  Pocos registros en esta ruta, omitiendo ALS.")
        return pd.DataFrame()

    als = ALS(
        rank=10,
        maxIter=5,
        implicitPrefs=True,
        ratingCol="rating",
        itemCol="cod_articulo_magic",
        userCol="clienteId_numeric",
        coldStartStrategy="drop"
    )
    model_als = als.fit(als_records)

    recs = model_als.recommendForAllUsers(sku_len)
    recs = recs.select("clienteId_numeric", "recommendations.cod_articulo_magic")
    recs = recs.join(
        als_records.select("clienteId", "clienteId_numeric").dropDuplicates(),
        on="clienteId_numeric",
        how="left",
    )

    recs_to_parse = recs.select("clienteId", "cod_articulo_magic").toPandas()
    if recs_to_parse.empty:
        return pd.DataFrame()

    lista_rec = [f"r{i+1}" for i in range(sku_len)]
    new_cols = pd.DataFrame(
        recs_to_parse["cod_articulo_magic"].tolist(),
        index=recs_to_parse.index,
        columns=lista_rec,
    )
    recs_to_parse = pd.concat([recs_to_parse, new_cols], axis=1)
    client_recs = pd.melt(recs_to_parse, id_vars=["clienteId"], value_vars=lista_rec)

    # Reconstruct full id_cliente: {id_prefix}|{cod_compania}|{cod_cliente}
    client_recs["compania"] = client_recs["clienteId"].str.split("|").str[-2].apply(
        lambda x: str(x).rjust(4, "0")
    )
    client_recs["cliente"] = client_recs["clienteId"].str.split("|").str[1]
    client_recs["id_cliente"] = id_prefix + "|" + client_recs["compania"] + "|" + client_recs["cliente"]
    client_recs["cod_articulo_magic"] = client_recs["value"]
    client_recs = client_recs[["id_cliente", "cod_articulo_magic"]].drop_duplicates().reset_index(drop=True)
    return client_recs


def procesar_pais(pais, spark):
    """Runs ALS for all routes of a given country and saves recommendations parquet."""
    print(f"\n[{pais}] Iniciando Modelado ALS...")
    paises_config = load_paises_config()
    id_prefix = paises_config.get(pais, {}).get("id_prefix", pais)

    input_rutas_dir = os.path.join(INPUT_BASE, "rutas")
    output_pais_dir = OUTPUT_BASE
    os.makedirs(output_pais_dir, exist_ok=True)

    archivos_rutas = glob.glob(os.path.join(input_rutas_dir, "**", "D_*_ventas.csv"), recursive=True)
    if not archivos_rutas:
        archivos_rutas = glob.glob(os.path.join(input_rutas_dir, "D_*_ventas.csv"))

    print(f"[{pais}] Archivos encontrados: {len(archivos_rutas)}")

    if not archivos_rutas:
        print(f"[{pais}] No se encontraron archivos. Generando parquet vacío.")
        pd.DataFrame(columns=["id_cliente", "cod_articulo_magic"]).to_parquet(
            os.path.join(output_pais_dir, "D_rutas_rec.parquet"), index=False
        )
        return

    lista_recomendaciones = []
    for ruta_path in archivos_rutas:
        nombre_archivo = os.path.basename(ruta_path)
        print(f"[{pais}] Procesando ALS para: {nombre_archivo}...")
        df_rec_ruta = als_training_job(spark, ruta_path, id_prefix)
        if not df_rec_ruta.empty:
            lista_recomendaciones.append(df_rec_ruta)

    if lista_recomendaciones:
        print(f"[{pais}] Consolidando recomendaciones...")
        df_final_recs = pd.concat(lista_recomendaciones, ignore_index=True).drop_duplicates()
        ruta_salida = os.path.join(output_pais_dir, "D_rutas_rec.parquet")
        df_final_recs.to_parquet(ruta_salida, index=False)
        print(f"[{pais}] Guardado: {ruta_salida} ({len(df_final_recs)} filas)")
    else:
        print(f"[{pais}] No se generaron recomendaciones.")
        pd.DataFrame(columns=["id_cliente", "cod_articulo_magic"]).to_parquet(
            os.path.join(output_pais_dir, "D_rutas_rec.parquet"), index=False
        )


def main():
    try:
        BUCKET_CONFIG = os.environ.get("BUCKET_CONFIG")
        CONFIG_S3_PREFIX = os.environ.get("CONFIG_S3_PREFIX")
        BUCKET_STEPS_RESULTS = os.environ.get("BUCKET_STEPS_RESULTS")
        pais = os.environ.get("PAIS", "").strip().upper()
        if not pais:
            raise ValueError("Variable de entorno PAIS no configurada.")

        print(f"Iniciando Modelado ALS para: {pais}")
        spark = create_spark_session()
        procesar_pais(pais, spark)
        spark.stop()
        print(f"\nModelado para {pais} finalizado.")

    except Exception as e:
        print("!!! ERROR CRÍTICO EN MODELADO !!!")
        print(traceback.format_exc())
        raise e


if __name__ == "__main__":
    main()
