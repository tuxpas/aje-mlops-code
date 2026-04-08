import subprocess
import sys

# 1. Actualizar herramientas base y pyarrow
subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "pip", "setuptools", "wheel"])
subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "pyarrow==17.0.0"])

# 2. Forzar la actualización de numpy y pandas
subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "numpy", "pandas"])

# 3. Instalar awswrangler y conectores
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

# CONFIGURACIÓN DE REGIÓN US-EAST-2
os.environ["AWS_DEFAULT_REGION"] = "us-east-2"
my_session = boto3.Session(region_name="us-east-2")

# --- CONFIGURACIÓN DE RUTAS SAGEMAKER ---
OUTPUT_DIR = "/opt/ml/processing/output/limpieza"
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(os.path.join(OUTPUT_DIR, "rutas"), exist_ok=True)

# Parámetros Globales
RUTAS_TEST = [
    1008,1108,1208,1308,1408,1608,2808,1708,1808,1908,2008,2108,2208,2408,
    1003,1103,1203,1303,1403,1503,1603,1703,4403,1803,1903,2003,2103,2203,
    2303,2403,2503,2603,2703,2803,2903,3003,3103,3203,3303,3403,4703,4903,
    5203,5603,5703,
    1017,1117,1217,1317,1417,1517,1617,1717,1817,1917,2017,2117,2217,2317,
    2417,2517,2617,2717,2817,2917,3017,3117,3217,3317,3417,3517,3617,3717,
    3817,3917,4017,4117,4217,4317,4417,4517,4617,4717,4817,4917,5517,5617,
    5717,
    1006,1106,1206,1306,1406,1506,1606,1706,1806,1906,2006,2106,2206,2306,
    3806,4106,2406,2506,2606,2706,2806,2906,3006,3106,
    1007,1307,1507,1807,1907,2007,2107,2307,2407,2507,2607,2707,2807,3607,
    1107,1207,1407,1607,2207,3507,3707,3307,3107,
    1004,1104,1204,1304,1404,1504,1604,1704,1804,1904,2004,2104,2204,2304,
    2404,2504,2604,2704,2804,2904,3004,3104,3204,3304,3404,3504,3604,4504,
    4604,4704,4804,4904,5104,5204,5404,
    1011,1211,1311,1411,1611,1711,1811,1911,1511,2011,2111,2211,2511,2711,
    2811,4711,2311,2411,2611,2911,3411,3511,3611,3911,3011,3111,3211,3311,
    3711,3811,4011,4811,
    1009,1209,1609,1909,2009,2609,2709,2809,1109,1309,1409,1709,1809,2109,
    2209
]
BUCKET_ARTIFACTS = "aje-prd-analytics-artifacts-s3"
PREFIX = "pedido_sugerido/data-v1/ecuador/"


def comprobar_inputs():
    """Verifica que los archivos en S3 existan y hayan sido modificados hoy."""
    s3 = boto3.client("s3")
    hoy = datetime.now(pytz.timezone("America/Lima")).date()
    errores = []

    objetos = s3.list_objects_v2(Bucket=BUCKET_ARTIFACTS, Prefix=PREFIX)
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
    """Descarga el maestro de productos, visitas y ventas, y los cruza."""
    # 1. Maestro de Productos (Redshift)
    query = """select * from comercial_ecuador.dim_producto where estado='A' and instancia='EC';"""
    con = wr.data_api.redshift.connect(
        cluster_id="dwh-cloud-storage-salesforce-prod",
        database="dwh_prod",
        db_user="dwhuser",
        boto3_session=my_session
    )
    maestro_prod = wr.data_api.rds.read_sql_query(query, con)
    maestro_prod.to_csv(os.path.join(OUTPUT_DIR, "EC_maestro_productos.csv"), index=False)
    maestro_prod = maestro_prod[["cod_articulo_magic", "desc_articulo"]].drop_duplicates()
    maestro_prod = maestro_prod.groupby(["cod_articulo_magic"]).first().reset_index()
    maestro_prod.to_csv(os.path.join(OUTPUT_DIR, "D_pan_masterProd.csv"), index=False)

    s3 = my_session.client("s3")

    # 2. Descargar Visitas
    visitas_obj = s3.get_object(Bucket=BUCKET_ARTIFACTS, Key=f"{PREFIX}visitas_ecuador000")
    pan_visitas = pd.read_csv(io.BytesIO(visitas_obj["Body"].read()), sep=";")
    pan_visitas = pan_visitas[pan_visitas["cod_ruta"].isin(RUTAS_TEST)].reset_index(drop=True)
    clientes_ruta_test = pan_visitas["codigo_cliente__c"].unique()

    # Apply canal filter after download
    pan_visitas = pan_visitas[pan_visitas.codigo_canal__c == 2]

    # 3. Descargar Ventas (1 archivo: ventas_ecuador000)
    columnas_ventas = [
        'id_cliente', 'id_sucursal', 'id_producto', 'fecha_liquidacion', 'cod_ruta', 'cod_modulo',
        'cod_zona', 'cant_cajafisicavta', 'cant_cajaunitvta', 'imp_netovta', 'cod_compania',
        'desc_compania', 'cod_sucursal', 'desc_sucursal', 'cod_pais', 'fecha_creacion_cliente',
        'cod_cliente', 'desc_marca', 'desc_formato', 'desc_categoria', 'cod_giro', 'cod_subgiro',
        'desc_giro', 'desc_subgiro', 'fecha_proceso'
    ]
    ventas_obj = s3.get_object(Bucket=BUCKET_ARTIFACTS, Key=f"{PREFIX}ventas_ecuador000")
    pan_ventas = pd.read_csv(io.BytesIO(ventas_obj["Body"].read()), sep=";")
    pan_ventas = pan_ventas[
        (pan_ventas["cod_compania"] == 90) & ((pan_ventas["cod_ruta"].isin(RUTAS_TEST)) | (pan_ventas["cod_cliente"].isin(clientes_ruta_test)))
    ].reset_index(drop=True)[columnas_ventas]

    # Extraer cod_articulo_magic
    pan_ventas["cod_articulo_magic"] = pan_ventas["id_producto"].str.split("|").str[-1].astype(int)

    # Filtrar por fecha_proceso de hoy
    hoy_int = int(datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d'))
    # pan_ventas = pan_ventas[pan_ventas['fecha_proceso'] == hoy_int]

    # Backup de visitas diario en S3
    formatted_date = datetime.now(pytz.timezone("America/Lima")).strftime("%Y-%m-%d")
    wr.s3.to_csv(pan_visitas, f's3://aje-analytics-ps-backup/PS_Ecuador/Input/visitas_ecuador000_{formatted_date}.csv', index=False, boto3_session=my_session)

    # Preparar llaves para cruce - id_cliente con prefijo EC
    pan_ventas["cod_compania"] = pan_ventas["cod_compania"].astype(str).apply(lambda x: str(int(x)).rjust(4, "0"))
    pan_ventas["id_cliente"] = "EC|" + pan_ventas["cod_compania"] + "|" + pan_ventas["cod_cliente"].astype(int).astype(str)

    pan_visitas["compania__c"] = pan_visitas["compania__c"].astype(str).apply(lambda x: str(int(float(x))).rjust(4, "0") if x.replace('.','',1).isdigit() else str(x).rjust(4, "0"))
    pan_visitas["id_cliente"] = "EC|" + pan_visitas["compania__c"] + "|" + pan_visitas["codigo_cliente__c"].astype(int).astype(str)

    # Última visita (Deduplicación)
    visita_default = (datetime.now(pytz.timezone("America/Lima")) - timedelta(days=7)).strftime("%Y-%m-%d")
    pan_visitas["ultima_visita"] = pan_visitas["ultima_visita"].fillna(visita_default)

    # Deduplicar visitas: priorizar la fila que contenga el día de mañana
    dia_actual = datetime.now(pytz.timezone("America/Lima")).weekday() + 1
    dia_siguiente = 7 if dia_actual == 6 else (dia_actual + 1) % 7
    pan_visitas["tiene_dia_manana"] = pan_visitas["dias_de_visita__c"].astype(str).apply(lambda x: 1 if str(dia_siguiente) in x.split(";") else 0)
    pan_visitas = pan_visitas.sort_values(["id_cliente", "tiene_dia_manana", "ultima_visita"], ascending=[True, False, False]).groupby("id_cliente").head(1)
    pan_visitas = pan_visitas.drop(columns=["tiene_dia_manana"])

    # Cruce Ventas y Visitas
    cols_visitas = ["id_cliente", "dias_de_visita__c", "periodo_de_visita__c", "ultima_visita", "cod_ruta", "cod_modulo", "eje_potencial__c"]
    df_merged = pd.merge(pan_ventas, pan_visitas[cols_visitas], on="id_cliente", how="inner", suffixes=("_vta", "_vis"))

    df_merged["cod_ruta"] = df_merged["cod_ruta_vis"].combine_first(df_merged["cod_ruta_vta"]).astype(int)
    df_merged["cod_modulo"] = df_merged["cod_modulo_vis"].combine_first(df_merged["cod_modulo_vta"]).astype(int)
    df_merged = df_merged.drop(columns=["cod_ruta_vta", "cod_ruta_vis", "cod_modulo_vta", "cod_modulo_vis"])

    # Renombrar DEPORADE a SPORADE
    df_merged["desc_marca"] = df_merged["desc_marca"].str.strip()
    df_merged["desc_marca"] = df_merged["desc_marca"].replace({"DEPORADE": "SPORADE"})

    # Segmentación
    mapping_segmento = {"S1": "BLINDAR", "S2": "DESARROLLAR", "S4": "MANTENER", "S5": "OPTIMIZAR"}
    df_merged["new_segment"] = df_merged["eje_potencial__c"].map(mapping_segmento).fillna("OPTIMIZAR")
    df_merged["mes"] = pd.to_datetime(df_merged["fecha_liquidacion"]).dt.strftime("%Y-%m-01")

    return df_merged


def filtrar_visitas_manana(df):
    """Filtra clientes con visita programada para mañana."""
    dia_actual = datetime.now(pytz.timezone("America/Lima")).weekday() + 1
    dia_siguiente = 7 if dia_actual == 6 else (dia_actual + 1) % 7

    df_final = df[
        df["dias_de_visita__c"].astype(str).apply(lambda x: str(dia_siguiente) in x.split(";"))
    ].reset_index(drop=True)
    return df_final


def preparar_rutas_y_pesos(df_ventas):
    """Calcula pesos por giro y divide los datasets por ruta para el modelo ALS."""
    df_ventas["fecha_liquidacion"] = pd.to_datetime(df_ventas["fecha_liquidacion"], format="%Y-%m-%d")
    df_ventas["desc_marca"] = df_ventas["desc_marca"].str.strip()
    df_ventas["desc_categoria"] = df_ventas["desc_categoria"].str.strip()

    # 1. Mapeo de pesos por Giro
    mapeo_diccionario = {}
    for giro_v in df_ventas["desc_subgiro"].unique():
        temp = df_ventas[(df_ventas["desc_subgiro"] == giro_v)]
        ranks = temp.groupby("desc_categoria")["cant_cajafisicavta"].sum().reset_index()
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

    # Guardar diccionario para el Script 3 (Reglas de Negocio)
    with open(os.path.join(OUTPUT_DIR, "mapeo_diccionario.json"), "w") as f:
        json.dump(mapeo_diccionario, f)

    # 2. División por Rutas
    rutas = df_ventas.groupby(["cod_ruta"])["id_cliente"].nunique().sort_values(ascending=False).reset_index()["cod_ruta"].unique()
    low_sku_ruta = []

    for ruta in rutas:
        temp = df_ventas[df_ventas["cod_ruta"] == ruta]
        if temp["cod_articulo_magic"].nunique() < 10:
            low_sku_ruta.append(ruta)
        else:
            temp.to_csv(os.path.join(OUTPUT_DIR, f"rutas/D_{ruta}_ventas.csv"), index=False)

    if low_sku_ruta:
        temp_low = df_ventas[df_ventas["cod_ruta"].isin(low_sku_ruta)]
        temp_low.to_csv(os.path.join(OUTPUT_DIR, "rutas/D_low_ruta_ventas.csv"), index=False)


def main():
    print("Iniciando Limpieza de Datos (Ecuador)...")
    comprobar_inputs()

    print("Extrayendo y cruzando ventas/visitas...")
    df_maestro = extraer_datos()

    print("Filtrando clientes a visitar mañana...")
    df_manana = filtrar_visitas_manana(df_maestro)

    # Guardar la base filtrada maestra para el Script 3 (Reglas de negocio)
    ruta_ventas_manana = os.path.join(OUTPUT_DIR, "ecuador_ventas_manana.parquet")
    df_manana.to_parquet(ruta_ventas_manana, index=False)

    print("Preparando archivos por ruta y calculando pesos...")
    preparar_rutas_y_pesos(df_manana)

    print(f"Limpieza finalizada exitosamente. Archivos guardados en {OUTPUT_DIR}")

if __name__ == "__main__":
    main()