import subprocess
import sys

subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "pip", "setuptools", "wheel"])
subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "pyarrow==17.0.0"])
subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "numpy", "pandas"])
subprocess.check_call([sys.executable, "-m", "pip", "install", "awswrangler[redshift]", "--no-build-isolation"])
subprocess.check_call([sys.executable, "-m", "pip", "install", "psycopg2-binary", "redshift-connector", "openpyxl"])

import os
import json
import boto3
import pytz
import io
import pandas as pd
import numpy as np
import awswrangler as wr
from datetime import datetime, timedelta

os.environ["AWS_DEFAULT_REGION"] = "us-east-2"
my_session = boto3.Session(region_name="us-east-2")

OUTPUT_DIR = "/opt/ml/processing/output/limpieza"
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(os.path.join(OUTPUT_DIR, "rutas"), exist_ok=True)

# Parámetros Globales - Ecuador Econoredes
BUCKET_ARTIFACTS = "aje-prd-analytics-artifacts-s3"
PREFIX_ECUADOR = "pedido_sugerido/data-v1/ecuador/"
BUCKET_DATALAKE = "aje-prod-datalake-399723489351-us-east-2-landing-s3"
PREFIX_DATALAKE = "analytics/pedido_sugerido/sellout/data/ecuador/"
COD_PAIS = "EC"

COMPANIAS_TEST = ['6003', '6010']

# Clientes de rutas test (se llena al descargar visitas)
clientes_ruta_test = []


def formatear_codigo(val):
    val_str = str(val).strip()
    if val_str.isdigit():
        return val_str.rjust(4, "0")
    else:
        return val_str


def comprobar_inputs():
    """Verifica que los archivos en S3 existan y hayan sido modificados hoy."""
    s3 = boto3.client("s3")
    hoy = datetime.now(pytz.timezone("America/Lima")).date()
    errores = []

    objetos = s3.list_objects_v2(Bucket=BUCKET_ARTIFACTS, Prefix=PREFIX_ECUADOR)
    if "Contents" not in objetos:
        raise ValueError("ERROR: No se encontraron archivos en la ruta especificada de S3.")

    for objeto in objetos["Contents"]:
        key = objeto["Key"]
        if key.endswith("/"):
            continue

        last_modified = objeto["LastModified"].date()
        size_kb = objeto["Size"] / 1024

        if objeto["Size"] == 0:
            errores.append(f"ERROR: El archivo {key} está vacío.")
        if last_modified != hoy:
            errores.append(f"ERROR: El archivo {key} no ha sido modificado hoy ({hoy}).")
        if size_kb < 1:
            print(f"ALERTA: El archivo {key} tiene un tamaño menor a 1 KB ({size_kb:.2f} KB).")

    if errores:
        for error in errores:
            print(error)
    print("Inputs comprobados correctamente :D")


def extraer_datos():
    """Descarga maestro, visitas, ventas. Aplica filtros específicos de Ecuador Econoredes."""
    global clientes_ruta_test

    # 1. Maestro de Productos (Redshift) - comercial_ecuador, instancia EC, ALL columns
    query = """select * from comercial_ecuador.dim_producto where estado='A' and instancia='EC';"""
    con = wr.data_api.redshift.connect(
        cluster_id="dwh-cloud-storage-salesforce-prod",
        database="dwh_prod",
        db_user="dwhuser",
        boto3_session=my_session
    )
    maestro_prod = wr.data_api.rds.read_sql_query(query, con)
    maestro_prod.groupby(["cod_articulo_magic", "desc_articulo"]).head(1)[["cod_articulo_magic", "desc_articulo"]].to_csv(
        os.path.join(OUTPUT_DIR, "EC_maestro_productos.csv"), index=False
    )

    s3 = my_session.client("s3")

    # 2. Descargar Visitas from DATALAKE bucket (NO pais filter, NO canal filter during download)
    # Filter only by compania__c
    objetos = s3.list_objects_v2(Bucket=BUCKET_DATALAKE, Prefix=PREFIX_DATALAKE)
    pan_visitas = pd.DataFrame()
    if "Contents" in objetos:
        for objeto in objetos["Contents"]:
            if objeto["Size"] > 0 and objeto["Key"].split("/")[-1] == "visitas_sellout000":
                response = s3.get_object(Bucket=BUCKET_DATALAKE, Key=objeto["Key"])
                content = response["Body"].read()
                df = pd.read_csv(io.BytesIO(content), sep=";")
                df = df[df["compania__c"].isin(COMPANIAS_TEST)].reset_index(drop=True)
                clientes_ruta_test = df["codigo_cliente__c"].unique()
                pan_visitas = df.copy()

    # 3. Descargar Ventas from DATALAKE bucket (only 1 file: ventas_sellout000)
    pan_ventas = pd.DataFrame()
    if "Contents" in objetos:
        for objeto in objetos["Contents"]:
            nombre_archivo = objeto["Key"].split("/")[-1]
            if objeto["Size"] > 0 and nombre_archivo == "ventas_sellout000":
                response = s3.get_object(Bucket=BUCKET_DATALAKE, Key=objeto["Key"])
                content = response["Body"].read()
                df = pd.read_csv(io.BytesIO(content), sep=";", low_memory=False)
                df["desc_formato"] = df["desc_formato"].astype(str)
                df = df[
                    (df["cod_compania"].isin(COMPANIAS_TEST))
                    | (df["cod_cliente"].isin(clientes_ruta_test))
                ].reset_index(drop=True)
                pan_ventas = pd.concat([pan_ventas, df], ignore_index=True)

    # After loading, filter again by companias_test only
    pan_ventas = pan_ventas[pan_ventas.cod_compania.isin(COMPANIAS_TEST)].reset_index(drop=True)

    # Extract cod_articulo_magic
    pan_ventas["cod_articulo_magic"] = pan_ventas["id_producto"].str.split("|").str[-1].astype(int)

    # Apply canal filter on visitas AFTER download
    pan_visitas = pan_visitas[pan_visitas.codigo_canal__c == 2].reset_index(drop=True)

    # Format cod_compania using formatear_codigo (handles alphanumeric like E001)
    pan_ventas["cod_compania"] = pan_ventas["cod_compania"].apply(formatear_codigo)
    pan_ventas["id_cliente"] = "EC|" + pan_ventas["cod_compania"].astype(str) + "|" + pan_ventas["cod_cliente"].astype(int).astype(str)

    pan_visitas["compania__c"] = pan_visitas["compania__c"].apply(formatear_codigo)
    pan_visitas["id_cliente"] = "EC|" + pan_visitas["compania__c"].astype(str) + "|" + pan_visitas["codigo_cliente__c"].astype(int).astype(str)

    # Deduplicate visitas - sort by ultima_visita, keep last per codigo_unico__c
    pan_visitas = pan_visitas.sort_values(
        by=["codigo_unico__c", "ultima_visita"], ascending=True, na_position="first"
    )
    pan_visitas = pan_visitas.drop_duplicates(subset="codigo_unico__c", keep="last")

    # Merge ventas and visitas
    cols_visitas = ["id_cliente", "dias_de_visita__c", "periodo_de_visita__c", "ultima_visita", "cod_ruta", "cod_modulo", "eje_potencial__c"]
    df_merged = pd.merge(pan_ventas, pan_visitas[cols_visitas], on="id_cliente", how="inner", suffixes=("_df1", "_df2"))

    df_merged["cod_ruta"] = df_merged["cod_ruta_df2"].combine_first(df_merged["cod_ruta_df1"]).astype(int)
    df_merged["cod_modulo"] = df_merged["cod_modulo_df2"].combine_first(df_merged["cod_modulo_df1"]).astype(int)
    df_merged = df_merged.drop(columns=["cod_ruta_df1", "cod_ruta_df2", "cod_modulo_df1", "cod_modulo_df2"])

    pan_ventas = df_merged.copy()

    # Strip and rename marca (DEPORADE->SPORADE rename NOT done for Ecuador)
    pan_ventas["desc_marca"] = pan_ventas["desc_marca"].str.strip()
    pan_ventas["cod_sucursal"] = pan_ventas["cod_sucursal"].astype(str)

    # Segmentation
    pan_ventas = pan_ventas.rename(columns={"eje_potencial__c": "new_segment"})
    mapping_segmento = {"S1": "BLINDAR", "S2": "DESARROLLAR", "S4": "MANTENER", "S5": "OPTIMIZAR"}
    pan_ventas["new_segment"] = pan_ventas["new_segment"].map(mapping_segmento).fillna("OPTIMIZAR")

    # Merge with maestro for desc_articulo
    pan_prod = pd.read_csv(os.path.join(OUTPUT_DIR, "EC_maestro_productos.csv"))
    pan_ventas = pd.merge(pan_ventas, pan_prod[["cod_articulo_magic", "desc_articulo"]], how="left", on="cod_articulo_magic")
    pan_ventas["mes"] = pd.to_datetime(pan_ventas["fecha_liquidacion"]).dt.strftime("%Y-%m-01")
    pan_ventas["desc_formato"] = pan_ventas["desc_formato"].astype(str)

    return pan_ventas


def filtrar_visitas_manana(df):
    """Filtra clientes con visita programada para mañana."""
    dia_actual = datetime.now(pytz.timezone("America/Lima")).weekday() + 1
    if dia_actual == 6:
        dia_siguiente = 7
    else:
        dia_siguiente = (dia_actual + 1) % 7

    df_final = df[
        df["dias_de_visita__c"].astype(str).apply(lambda x: str(dia_siguiente) in x.split(";"))
    ].reset_index(drop=True)

    return df_final


def preparar_rutas_y_pesos(df_ventas):
    """Calcula pesos por giro y divide los datasets por ruta. No low_sku logic for Ecuador."""
    df_ventas["fecha_liquidacion"] = pd.to_datetime(df_ventas["fecha_liquidacion"], format="%Y-%m-%d")
    df_ventas["desc_marca"] = df_ventas["desc_marca"].str.strip()
    df_ventas["desc_categoria"] = df_ventas["desc_categoria"].str.strip()

    mapeo_diccionario = {}
    for giro_v in df_ventas["desc_subgiro"].unique():
        temp = df_ventas[(df_ventas["desc_subgiro"] == giro_v)]
        ranks = temp.groupby("desc_categoria")["cant_cajafisica_vta"].sum().reset_index()
        ranks.columns = ["index", "desc_categoria"]
        ranks = ranks.sort_values(by="desc_categoria", ascending=False)
        if len(ranks) <= 5:
            ranks["Ranking"] = range(1, len(ranks) + 1)
        else:
            a = list(ranks["desc_categoria"])
            b = [1, 1, 2, 2]
            multiplicador = 4 if (np.std(a) / np.mean(a) <= 1.2) else 2
            for i in range(4, len(a)):
                if a[3] <= a[i] * multiplicador:
                    b.append(3)
                else:
                    b.append(3 + i)
            ranks["Ranking"] = b
        mapeo_diccionario[giro_v] = ranks.set_index("index")["Ranking"].to_dict()

    with open(os.path.join(OUTPUT_DIR, "mapeo_diccionario.json"), "w") as f:
        json.dump(mapeo_diccionario, f)

    # Ecuador: NO low_sku_ruta logic. All rutas are processed and saved as CSV.
    for ruta in df_ventas.cod_ruta.unique():
        temp = df_ventas[df_ventas["cod_ruta"] == ruta]
        temp.to_csv(os.path.join(OUTPUT_DIR, f"rutas/D_{ruta}_ventas.csv"), index=False)


def main():
    print("Iniciando Limpieza de Datos (Ecuador Econoredes)...")
    # comprobar_inputs() # Desactivado de momento

    print("Extrayendo y cruzando ventas/visitas...")
    df_maestro = extraer_datos()

    print("Filtrando clientes a visitar mañana...")
    df_manana = filtrar_visitas_manana(df_maestro)

    ruta_ventas_manana = os.path.join(OUTPUT_DIR, "ecuador_ventas_manana.parquet")
    df_manana.to_parquet(ruta_ventas_manana, index=False)

    print("Preparando archivos por ruta y calculando pesos...")
    preparar_rutas_y_pesos(df_manana)

    print(f"Limpieza finalizada exitosamente. Archivos guardados en {OUTPUT_DIR}")

if __name__ == "__main__":
    main()
