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

# Parámetros Globales - Perú
BUCKET_ARTIFACTS = "aje-prd-analytics-artifacts-s3"
PREFIX_PERU = "pedido_sugerido/data-v1/peru/"
COD_PAIS = "PE"  # Prefijo de id_cliente

RUTAS_PERU = list(set([
10000,10001,10002,10003,10004,10005,10006,10007,10008,10009,10010,10011,10012,10013,10014,10015,10016,10017,10018,10019,
10020,10021,10022,10023,10024,10025,10026,10027,10028,10029,10030,10031,10032,10033,10034,10035,10036,10037,10038,10039,
10040,10041,10042,10043,10044,10045,10046,10047,10048,10049,10050,10051,10052,10053,10054,10055,10056,10057,10059,10061,
10062,10066,10067,10060,10063,10064,10065,10058,19001,13479,13486,13481,13485,13478,13487,13482,13483,13484,10400,10401,
10402,10403,10404,10405,10406,10407,10408,10409,10410,10576,10413,10578,10577,10579,10580,10414,10415,10416,10420,10421,
10422,10417,10418,10419,10426,10427,10428,10429,10430,10431,10432,10433,10434,10435,10423,10425,10444,10424,10445,10452,
10446,10447,10448,10449,10450,10451,10461,10465,10459,10460,10462,10464,10463,10466,10467,10473,10468,10469,10470,10472,
10471,10591,10600,10601,10602,10603,10604,10605,10606,10607,10608,10609,10610,10611,10612,10613,10614,10615,10616,10617,
10618,10619,10620,10621,10622,10623,10624,10625,10626,10627,10628,10629,10630,10631,10632,10633,10634,10635,10636,10637,
10638,10639,10640,10641,10642,10643,10644,10645,10646,10647,10648,10649,10650,10660,10651,10652,10653,10654,10655,10656,
10657,10658,10659,10661,10662,10663,10664,10665,10666,10667,10668,10669,10670,10730,10731,10800,10805,10806,10801,10807,
10802,10808,10803,10848,10804,10839,10812,10843,10838,10813,10846,10814,10815,10816,10817,10820,10822,10823,10825,10826,
10827,10828,10830,10831,10832,10836,10837,10840,10841,10842,11000,11001,11002,11003,11004,11005,11006,11007,11008,11009,
11010,11011,11012,11013,11014,11015,11017,11018,11019,11020,11021,11023,11024,11025,11026,11200,11201,11202,11203,11204,
11205,11206,11207,11215,11216,11217,11218,11214,11222,11224,11225,11226,11210,11211,11212,11213,11219,11220,11221,11223,
11400,11401,11402,11403,11404,11405,11410,11406,11407,11408,11409,11411,11412,11413,11414,11600,11601,11602,11603,11604,
11605,11606,11607,11608,11609,11610,11611,11612,11613,11614,11615,11616,11617,11627,11618,11620,11621,11622,11623,11624,
11625,11626,11619,11800,11801,11802,11803,11804,11805,11806,11807,11808,11809,11810,11811,11812,11813,11814,11815,11816,
11817,11818,11819,12000,12001,12002,12003,12004,12005,12006,12007,12008,12009,12010,12011,12012,12013,12200,12201,12202,
12203,12204,12205,12206,12207,12208,12209,12210,12211,12212,12213,12214,12215,12216,12217,12218,12219,12220,12221,12222,
12223,12230,12234,12224,12229,12235,12225,12226,12227,12228,12231,12232,12233,12236,12237,12238,12240,12239,12405,12400,
12401,12402,12403,12404,12500,12501,12502,12503,12504,12505,13530,13531,13532,13533,13535,13534,13536,13400,13401,13402,
13403,13404,13405,13406,13414,13415,13409,13416,13417,13410,13411,13412,13413,13418,13421,13420,13419,13422,13423,13424,
13425,13460,13461,13462,13463,13464,13465,13466,13467,13468,13469,13470,13471,13472,13473,13474,13475,13476,12973,12954,
12956,12969,12961,12958,12950,12953,12963,12959,12962,12951,12952,12965,12957,12972,12955,12974,12975,12960,12976,12964,
12967,12970,13543,13540,13544,13541,13545,13542,13546,13547,13539,13548,13549,13550,13551,13552,13553,13554,13555,13556,
13557,13558,13565,13559,13566,13560,13561,13567,13568,13562,13564,13563,13569,13570,13574,13571,13572,13573,13575,13576,
13577,13584,13581,13579,13585,13582,13586,13583,13578,13580,13587,13588,13589,13594,13591,13593,13592,13595,13596,13597,
13598,13654,13624,13620,13621,13623,13625,13622,13655,13656,13651,13653,13650,13652,13800,13801,13802,13803,13804,13805,
13806,13807,13808,13809,13813,13811,13814,13812,13815,13816,13817,13818,13819,13820,13850,13851,13852,13853,13854,13855,
14101,14100,14102,14103,14104,14105,14106,14107,14108,14109,14110,14111,14112,14113,14114,14115,14116,14117,14118,14119,
14120,14121,14450,14457,14461,14451,14459,14452,14460,14453,14454,14456,14455,14458,14465,14462,14466,14464,14467,14463,
14468,14509,14500,14502,14501,14503,14504,14505,14507,14506,14508,14511,14512,14510,14519,14513,14515,14514,14518,14516,
14524,14517,14525,14522,14523,14521,14609,14605,14606,14610,14603,14602,14604,14607,14608
]))

# Rutas y SKUs a excluir de Lima Centro
RUTAS_EXCLUIR_LIMA = [10673,10600,10601,10602,10603,10604,10605,10606,10607,
    10608,10609,10610,10611,10612,10613,10614,10615,10616,10617,10618,10619,
    10620,10621,10622,10623,10624,10625,10626,10627,10628,10629,10630,10631,
    10632,10633,10634,10635,10636,10637,10638,10639,10640,10641,10642,10643,
    10644,10645,10646,10647,10648,10649,10650,10651,10652,10653,10654,10655,
    10656,10657,10658,10659,10660,10661,10662,10663,10664,10665,10666,10667,
    10668,10669,10670]
SKUS_EXCLUIR_LIMA = [622396,621807,622807,622394,622395,608962,623130,623679,
    622356,699892,623456,621858,621851,699549,622836,621994,621996,623262,
    622957,622163,622956,621807,622807,623679,699892,622836,621994,621996,
    622956,622396,621807,622807,622394,608962,623130,623679,623456,621858,
    621851,699549,622836,621994,621996]

# Sucursales de Lima donde se mantiene TRES CRUCES
SUCURSALES_LIMA = ['LIMA-CALLAO', 'PUENTE PIEDRA', 'VILLA EL SALVADOR', 'HUACHIPA']


def comprobar_inputs():
    """Verifica que los archivos en S3 existan y hayan sido modificados hoy."""
    s3 = boto3.client("s3")
    hoy = datetime.now(pytz.timezone("America/Lima")).date()
    errores = []

    objetos = s3.list_objects_v2(Bucket=BUCKET_ARTIFACTS, Prefix=PREFIX_PERU)
    if "Contents" not in objetos:
        raise ValueError("ERROR: No se encontraron archivos en la ruta especificada de S3.")

    for objeto in objetos["Contents"]:
        key = objeto["Key"]
        if "pedidos_3meses" in key:
            print(f"OJO: El key {key} no pasa validacion, se omite.")
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
    """Descarga maestro, visitas, ventas. Aplica filtros específicos de Perú."""
    # 1. Maestro de Productos (Redshift) - comercial_peru, instancia PE
    query = """select cod_articulo_magic, desc_articulo from comercial_peru.dim_producto where estado='A' and instancia='PE';"""
    con = wr.data_api.redshift.connect(
        cluster_id="dwh-cloud-storage-salesforce-prod",
        database="dwh_prod",
        db_user="dwhuser",
        boto3_session=my_session
    )
    maestro_prod = wr.data_api.rds.read_sql_query(query, con)
    maestro_prod = maestro_prod[["cod_articulo_magic", "desc_articulo"]].groupby(["cod_articulo_magic"]).head(1).reset_index(drop=True)
    maestro_prod.to_csv(os.path.join(OUTPUT_DIR, "PE_maestro_productos.csv"), index=False)

    s3 = my_session.client("s3")

    # 2. Descargar Visitas (sin filtro de pais ni compania, solo por rutas)
    visitas_obj = s3.get_object(Bucket=BUCKET_ARTIFACTS, Key=f"{PREFIX_PERU}visitas_peru000")
    pan_visitas = pd.read_csv(io.BytesIO(visitas_obj["Body"].read()), sep=";")
    pan_visitas = pan_visitas[pan_visitas["cod_ruta"].isin(RUTAS_PERU)].reset_index(drop=True)
    clientes_ruta_test = pan_visitas["codigo_cliente__c"].unique()

    # 3. Descargar Ventas (ventas_peru000, ventas_peru001, ventas_peru002 si existe)
    columnas_ventas = [
        'id_cliente', 'id_sucursal', 'id_producto', 'fecha_liquidacion', 'cod_ruta', 'cod_modulo',
        'cod_zona', 'cant_cajafisicavta', 'cant_cajaunitvta', 'imp_netovta', 'cod_compania',
        'desc_compania', 'cod_sucursal', 'desc_sucursal', 'cod_pais', 'fecha_creacion_cliente',
        'cod_cliente', 'desc_marca', 'desc_formato', 'desc_categoria', 'cod_giro', 'cod_subgiro',
        'desc_giro', 'desc_subgiro', 'fecha_proceso'
    ]
    pan_ventas = pd.DataFrame()
    for archivo_ventas in ["ventas_peru000", "ventas_peru001", "ventas_peru002"]:
        try:
            ventas_obj = s3.get_object(Bucket=BUCKET_ARTIFACTS, Key=f"{PREFIX_PERU}{archivo_ventas}")
            df_temp = pd.read_csv(io.BytesIO(ventas_obj["Body"].read()), sep=";")
            df_temp = df_temp[
                (df_temp["cod_ruta"].isin(RUTAS_PERU)) | (df_temp["cod_cliente"].isin(clientes_ruta_test))
            ].reset_index(drop=True)[columnas_ventas]
            pan_ventas = pd.concat([pan_ventas, df_temp], ignore_index=True)
        except Exception as e:
            print(f"Archivo {archivo_ventas} no encontrado o error: {e}")

    # Extraer cod_articulo_magic
    pan_ventas["cod_articulo_magic"] = pan_ventas["id_producto"].str.split("|").str[-1].astype(int)
    pan_ventas["desc_marca"] = pan_ventas["desc_marca"].str.strip()

    # --- FILTROS ESPECÍFICOS DE PERÚ ---
    # Eliminar TRES CRUCES de sucursales que no sean de Lima
    pan_ventas_filtered = pan_ventas[
        (pan_ventas['desc_sucursal'].isin(SUCURSALES_LIMA)) |
        (~pan_ventas['desc_marca'].isin(['TRES CRUCES']))
    ].reset_index(drop=True)

    # Quitar SKUs de Lima Centro (combinación ruta + SKU)
    pan_ventas_filtered = pan_ventas_filtered[
        ~(pan_ventas_filtered['cod_ruta'].isin(RUTAS_EXCLUIR_LIMA) & pan_ventas_filtered['cod_articulo_magic'].isin(SKUS_EXCLUIR_LIMA))
    ]

    # Descargar Excel de SKUs a usar desde S3
    sagemaker_session = boto3.Session(region_name="us-east-2")
    sts = sagemaker_session.client("sts")
    account_id = sts.get_caller_identity()["Account"]
    default_bucket = f"sagemaker-us-east-2-{account_id}"
    excel_s3_path = f"s3://{default_bucket}/scripts/PE/pe_ps_marcas_skus_a_usar.xlsx"
    excel_local = "/opt/ml/processing/pe_ps_marcas_skus_a_usar.xlsx"
    wr.s3.download(path=excel_s3_path, local_file=excel_local, boto3_session=my_session)
    skus_a_usar = pd.read_excel(excel_local, sheet_name="Hoja1")

    # Filtrar solo SKUs del Excel + TRES CRUCES
    pan_ventas_filtered = pan_ventas_filtered[
        (pan_ventas_filtered.cod_articulo_magic.isin(skus_a_usar.cod_articulo_magic.unique()))
        | (pan_ventas_filtered.desc_marca == "TRES CRUCES")
    ].reset_index(drop=True)

    pan_ventas = pan_ventas_filtered

    # Backup de visitas diario en S3
    formatted_date = datetime.now(pytz.timezone("America/Lima")).strftime("%Y-%m-%d")
    # wr.s3.to_csv(pan_visitas, f's3://aje-analytics-ps-backup/PS_Peru/Input/visitas_peru000_{formatted_date}.csv', index=False, boto3_session=my_session)

    # Preparar llaves para cruce - id_cliente con prefijo PE
    pan_ventas["cod_compania"] = pan_ventas["cod_compania"].astype(str).apply(lambda x: str(int(x)).rjust(4, "0"))
    pan_ventas["id_cliente"] = "PE|" + pan_ventas["cod_compania"] + "|" + pan_ventas["cod_cliente"].astype(int).astype(str)

    pan_visitas["compania__c"] = pan_visitas["compania__c"].astype(str).apply(lambda x: str(int(float(x))).rjust(4, "0"))
    pan_visitas["id_cliente"] = "PE|" + pan_visitas["compania__c"] + "|" + pan_visitas["codigo_cliente__c"].astype(int).astype(str)

    # Filtrar visitas canal 2 (sin filtro de compañía, Perú tiene varias)
    pan_visitas = pan_visitas[pan_visitas.codigo_canal__c == 2].reset_index(drop=True)

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

    # Segmentación
    mapping_segmento = {"S1": "BLINDAR", "S2": "DESARROLLAR", "S4": "MANTENER", "S5": "OPTIMIZAR"}
    df_merged["new_segment"] = df_merged["eje_potencial__c"].map(mapping_segmento).fillna("OPTIMIZAR")

    # Renombrar DEPORADE a SPORADE
    df_merged["desc_marca"] = df_merged["desc_marca"].str.strip()
    df_merged["desc_marca"] = df_merged["desc_marca"].replace({"DEPORADE": "SPORADE"})

    df_merged["mes"] = pd.to_datetime(df_merged["fecha_liquidacion"]).dt.strftime("%Y-%m-01")

    return df_merged


def filtrar_visitas_manana(df):
    """Filtra clientes con visita programada para mañana."""
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
    """Calcula pesos por giro y divide los datasets por ruta."""
    df_ventas["fecha_liquidacion"] = pd.to_datetime(df_ventas["fecha_liquidacion"], format="%Y-%m-%d")
    df_ventas["desc_marca"] = df_ventas["desc_marca"].str.strip()
    df_ventas["desc_categoria"] = df_ventas["desc_categoria"].str.strip()

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

    with open(os.path.join(OUTPUT_DIR, "mapeo_diccionario.json"), "w") as f:
        json.dump(mapeo_diccionario, f)

    # División por Rutas (umbral < 5 para low SKU en Perú)
    rutas = df_ventas.groupby(["cod_ruta"])["id_cliente"].nunique().sort_values(ascending=False).reset_index()["cod_ruta"].unique()
    low_sku_ruta = []

    for ruta in rutas:
        temp = df_ventas[df_ventas["cod_ruta"] == ruta]
        if temp["cod_articulo_magic"].nunique() < 5:
            low_sku_ruta.append(ruta)
        else:
            temp.to_csv(os.path.join(OUTPUT_DIR, f"rutas/D_{ruta}_ventas.csv"), index=False)

    if low_sku_ruta:
        temp_low = df_ventas[df_ventas["cod_ruta"].isin(low_sku_ruta)]
        temp_low.to_csv(os.path.join(OUTPUT_DIR, "rutas/D_low_ruta_ventas.csv"), index=False)


def main():
    print("Iniciando Limpieza de Datos (Perú)...")
    comprobar_inputs()

    print("Extrayendo y cruzando ventas/visitas...")
    df_maestro = extraer_datos()

    print("Filtrando clientes a visitar mañana...")
    df_manana = filtrar_visitas_manana(df_maestro)

    ruta_ventas_manana = os.path.join(OUTPUT_DIR, "peru_ventas_manana.parquet")
    df_manana.to_parquet(ruta_ventas_manana, index=False)

    print("Preparando archivos por ruta y calculando pesos...")
    preparar_rutas_y_pesos(df_manana)

    print(f"Limpieza finalizada exitosamente. Archivos guardados en {OUTPUT_DIR}")

if __name__ == "__main__":
    main()
