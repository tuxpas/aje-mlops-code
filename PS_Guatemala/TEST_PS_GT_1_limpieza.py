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
RUTAS_TEST = list(set([1202,1206,1361,1370,1195,1192,1146,1148,1402,1406,1311,1301,1120,1106,1101,1102,1103,1104,1105,1106,1107,1108,1109,1110,1111,1112,1113,1114,1115,1116,1117,1118,1119,1120,1160,1161,1162,1163,1164,1165,1166,1167,1168,1169,1170,1171,1172,1173,1174,1175,1176,1177,1178,1179,1180,1181,1182,1183,1184,1185,1186,1187,1188,1189,1190,1191,1192,1193,1194,1195,1196,1197,1198,1200,1201,1202,1203,1204,1205,1215,1207,1208,1209,1210,1211,1212,1216,1206,1213,1214,1217,1218,1219,1220,1300,1301,1302,1303,1304,1305,1306,1307,1308,1309,1310,1311,1312,1313,1350,1351,1352,1353,1354,1355,1356,1357,1358,1359,1360,1361,1362,1363,1364,1365,1366,1367,1368,1369,1370,1371,1372,1373,1374,1375,1376,1377,1378,1379,1144,1145,1146,1147,1148,1149,1150,1137,1138,1139,1140,1141,1142,1143,1131,1132,1133,1134,1135,1136,1400,1401,1402,1403,1404,1405,1406,1407,1408,1409,1410,1411,1412,1413,1414,1415,1416,1417,1418,1419,1420,1421,1422,1423,1424,1425,1426,1427,1428,1429,1430,1431,1432,1433,1434,1435]))
BUCKET_ARTIFACTS = "aje-prd-analytics-artifacts-s3"
PREFIX_CAM = "pedido_sugerido/data-v1/cam/"
COD_COMPANIA = "0076"
COD_PAIS = "GT"


def comprobar_inputs():
    """Verifica que los archivos en S3 existan y hayan sido modificados hoy."""
    s3 = boto3.client("s3")
    hoy = datetime.now(pytz.timezone("America/Lima")).date()
    errores = []

    objetos = s3.list_objects_v2(Bucket=BUCKET_ARTIFACTS, Prefix=PREFIX_CAM)
    if "Contents" not in objetos:
        raise ValueError("ERROR: No se encontraron archivos en la ruta especificada de S3.")

    for objeto in objetos["Contents"]:
        key = objeto["Key"]
        if key == f"{PREFIX_CAM}ventas_sellout_cam000":
            print(f"OJO: El key {key} aplica a la validacion, se omite.")
            continue
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
    query = """select * from comercial_cam.dim_producto where estado='A' and cod_compania = '0076';"""
    con = wr.data_api.redshift.connect(
        cluster_id="dwh-cloud-storage-salesforce-prod",
        database="dwh_prod",
        db_user="dwhuser",
        boto3_session=my_session
    )
    maestro_prod = wr.data_api.rds.read_sql_query(query, con)
    maestro_prod = maestro_prod[["cod_articulo_magic", "desc_articulo"]].drop_duplicates()
    maestro_prod = maestro_prod.groupby(["cod_articulo_magic"]).first().reset_index()
    maestro_prod.to_csv(os.path.join(OUTPUT_DIR, "D_pan_masterProd.csv"), index=False)

    s3 = my_session.client("s3")

    # 2. Descargar Visitas
    visitas_obj = s3.get_object(Bucket=BUCKET_ARTIFACTS, Key=f"{PREFIX_CAM}visitas_cam000")
    pan_visitas = pd.read_csv(io.BytesIO(visitas_obj["Body"].read()), sep=";")
    pan_visitas = pan_visitas[
        (pan_visitas["pais__c"] == COD_PAIS)
        & (pan_visitas["cod_ruta"].isin(RUTAS_TEST))
        & (pan_visitas["codigo_canal__c"] == 2)
    ].reset_index(drop=True)
    clientes_ruta_test = pan_visitas["codigo_cliente__c"].unique()

    # 3. Descargar Ventas (2 archivos: ventas_cam000 y ventas_cam001)
    columnas_ventas = [
        'id_cliente', 'id_sucursal', 'id_producto', 'fecha_liquidacion', 'cod_ruta', 'cod_modulo',
        'cod_zona', 'cant_cajafisicavta', 'cant_cajaunitvta', 'imp_netovta', 'cod_compania',
        'desc_compania', 'cod_sucursal', 'desc_sucursal', 'cod_pais', 'fecha_creacion_cliente',
        'cod_cliente', 'desc_marca', 'desc_formato', 'desc_categoria', 'cod_giro', 'cod_subgiro',
        'desc_giro', 'desc_subgiro', 'fecha_proceso'
    ]
    pan_ventas = pd.DataFrame()
    for archivo_ventas in ["ventas_cam000", "ventas_cam001"]:
        ventas_obj = s3.get_object(Bucket=BUCKET_ARTIFACTS, Key=f"{PREFIX_CAM}{archivo_ventas}")
        df_temp = pd.read_csv(io.BytesIO(ventas_obj["Body"].read()), sep=";")
        df_temp = df_temp[
            (df_temp["cod_ruta"].isin(RUTAS_TEST)) | (df_temp["cod_cliente"].isin(clientes_ruta_test))
        ].reset_index(drop=True)[columnas_ventas]
        pan_ventas = pd.concat([pan_ventas, df_temp], ignore_index=True)

    # Filtrar solo Guatemala
    pan_ventas = pan_ventas[pan_ventas["cod_pais"] == COD_PAIS].reset_index(drop=True)

    # Extraer cod_articulo_magic
    pan_ventas["cod_articulo_magic"] = pan_ventas["id_producto"].str.split("|").str[-1].astype(int)

    # Filtrar por fecha_proceso de hoy
    hoy_int = int(datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d'))
    # pan_ventas = pan_ventas[pan_ventas['fecha_proceso'] == hoy_int]

    # Backup de visitas diario en S3
    formatted_date = datetime.now(pytz.timezone("America/Lima")).strftime("%Y-%m-%d")
    # wr.s3.to_csv(pan_visitas, f's3://aje-analytics-ps-backup/PS_Guatemala/Input/visitas_cam000_{formatted_date}.csv', index=False, boto3_session=my_session)

    # Preparar llaves para cruce - id_cliente con prefijo CAM
    pan_ventas["cod_compania"] = pan_ventas["cod_compania"].astype(str).apply(lambda x: str(int(x)).rjust(4, "0"))
    pan_ventas["id_cliente"] = "CAM|" + pan_ventas["cod_compania"] + "|" + pan_ventas["cod_cliente"].astype(int).astype(str)

    pan_visitas["compania__c"] = pan_visitas["compania__c"].astype(str).apply(lambda x: str(int(float(x))).rjust(4, "0"))
    pan_visitas["id_cliente"] = "CAM|" + pan_visitas["compania__c"] + "|" + pan_visitas["codigo_cliente__c"].astype(int).astype(str)

    # Filtrar visitas canal 2 y compania 0076
    pan_visitas = pan_visitas[(pan_visitas["codigo_canal__c"] == 2) & (pan_visitas["compania__c"] == COD_COMPANIA)].reset_index(drop=True)

    # Última visita (Deduplicación)
    visita_default = (datetime.now(pytz.timezone("America/Lima")) - timedelta(days=7)).strftime("%Y-%m-%d")
    pan_visitas["ultima_visita"] = pan_visitas["ultima_visita"].fillna(visita_default)
    pan_visitas = pan_visitas.sort_values(["id_cliente", "ultima_visita"], ascending=False).groupby("id_cliente").head(1)

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
    """Filtra el DataFrame para quedarse solo con los clientes que tienen visita programada para mañana."""
    data_test = df[["id_cliente", "dias_de_visita__c", "periodo_de_visita__c", "ultima_visita"]].drop_duplicates().reset_index(drop=True)
    data_test["ultima_visita"] = pd.to_datetime(data_test["ultima_visita"], format="%Y-%m-%d")

    fecha_actual_dt = datetime.now()
    data_test["dias_pasados"] = (fecha_actual_dt - data_test["ultima_visita"]).dt.days

    dia_actual = datetime.now(pytz.timezone("America/Lima")).weekday() + 1
    dia_siguiente = 7 if dia_actual == 6 else (dia_actual + 1) % 7

    clientes_a_visitar_manana = data_test[
        data_test["dias_de_visita__c"].astype(str).apply(lambda x: str(dia_siguiente) in x.split(";"))
    ].reset_index(drop=True)

    condicion_f1 = clientes_a_visitar_manana["periodo_de_visita__c"] == "F1"
    condicion_f2 = (clientes_a_visitar_manana["periodo_de_visita__c"] == "F2") & (clientes_a_visitar_manana["dias_pasados"] > 13)
    condicion_f3 = (clientes_a_visitar_manana["periodo_de_visita__c"] == "F3") & (clientes_a_visitar_manana["dias_pasados"] > 20)
    condicion_f4 = (clientes_a_visitar_manana["periodo_de_visita__c"] == "F4") & (clientes_a_visitar_manana["dias_pasados"] > 27)

    clientes_a_visitar_manana = clientes_a_visitar_manana[condicion_f1 | condicion_f2 | condicion_f3 | condicion_f4]

    df_final = df[df["id_cliente"].isin(clientes_a_visitar_manana["id_cliente"])].reset_index(drop=True)
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
        if temp["cod_articulo_magic"].nunique() <= 5:
            low_sku_ruta.append(ruta)
        else:
            temp.to_csv(os.path.join(OUTPUT_DIR, f"rutas/D_{ruta}_ventas.csv"), index=False)

    if low_sku_ruta:
        temp_low = df_ventas[df_ventas["cod_ruta"].isin(low_sku_ruta)]
        temp_low.to_csv(os.path.join(OUTPUT_DIR, "rutas/D_low_ruta_ventas.csv"), index=False)


def main():
    print("Iniciando Limpieza de Datos (Guatemala)...")
    comprobar_inputs()

    print("Extrayendo y cruzando ventas/visitas...")
    df_maestro = extraer_datos()

    print("Filtrando clientes a visitar mañana...")
    df_manana = filtrar_visitas_manana(df_maestro)

    # Guardar la base filtrada maestra para el Script 3 (Reglas de negocio)
    ruta_ventas_manana = os.path.join(OUTPUT_DIR, "gt_ventas_manana.parquet")
    df_manana.to_parquet(ruta_ventas_manana, index=False)

    print("Preparando archivos por ruta y calculando pesos...")
    preparar_rutas_y_pesos(df_manana)

    print(f"Limpieza finalizada exitosamente. Archivos guardados en {OUTPUT_DIR}")

if __name__ == "__main__":
    main()
