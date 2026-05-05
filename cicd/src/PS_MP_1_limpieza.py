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

OUTPUT_BASE = "/opt/ml/processing/output/limpieza"

COLUMNAS_VENTAS = [
    'id_cliente', 'id_sucursal', 'id_producto', 'fecha_liquidacion', 'cod_ruta', 'cod_modulo',
    'cod_zona', 'cant_cajafisicavta', 'cant_cajaunitvta', 'imp_netovta', 'cod_compania',
    'desc_compania', 'cod_sucursal', 'desc_sucursal', 'cod_pais', 'fecha_creacion_cliente',
    'cod_cliente', 'desc_marca', 'desc_formato', 'desc_categoria', 'cod_giro', 'cod_subgiro',
    'desc_giro', 'desc_subgiro', 'fecha_proceso'
]

def load_pais_config(resource_variables, pais):
    """Returns config from dynamodb table."""
    dynamodb = boto3.resource("dynamodb")

    table_name = resource_variables["TABLE_CONFIG"]

    table = dynamodb.Table(table_name)
    response = table.get_item(
        Key={"code_country": pais}
    )
    item = response.get("Item")
    if not item:
        raise ValueError(f"No se encontró configuración para el país: {pais}")

    return item


def comprobar_inputs(config, resource_variables):
    """Verifica que los archivos en S3 existan y hayan sido modificados hoy."""
    s3 = boto3.client("s3")
    hoy = datetime.now(pytz.timezone("America/Lima")).date()
    errores = []
    prefix = config["s3_prefix"]

    objetos = s3.list_objects_v2(Bucket=resource_variables["BUCKET_ARTIFACTS"], Prefix=prefix)
    if "Contents" not in objetos:
        raise ValueError(f"ERROR: No se encontraron archivos en {prefix}")

    for objeto in objetos["Contents"]:
        key = objeto["Key"]
        if "pedidos_3meses" in key or key == f"{prefix}ventas_sellout_cam000":
            print(f"OJO: {key} se omite de la validación.")
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
    print(f"Inputs comprobados para {config['nombre']} :D")


def extraer_datos(pais, config, output_dir, resource_variables):
    """Descarga maestro, visitas y ventas; cruza y retorna df_merged."""
    s3 = my_session.client("s3")
    tz = pytz.timezone("America/Lima")
    formatted_date = datetime.now(tz).strftime("%Y-%m-%d")
    rutas = config["rutas"]
    prefix = config["s3_prefix"]
    id_prefix = config["id_prefix"]

    # 1. Maestro de Productos (S3)
    obj_maestro_prod = s3.get_object(Bucket=resource_variables["BUCKET_ARTIFACTS"], Key=f"pedido_sugerido/{config["nombre"].lower()}/{pais}_maestro_productos.csv")
    maestro_prod = pd.read_csv(io.BytesIO(obj_maestro_prod["Body"].read()), sep=";") 
    maestro_prod = maestro_prod[["cod_articulo_magic", "desc_articulo"]].drop_duplicates()
    maestro_prod = maestro_prod.groupby("cod_articulo_magic").first().reset_index()
    maestro_prod.to_csv(os.path.join(output_dir, config["maestro_csv"]), index=False)

    # 2. Descargar Visitas
    obj = s3.get_object(Bucket=resource_variables["BUCKET_ARTIFACTS"], Key=f"{prefix}{config['visitas_key']}")
    pan_visitas = pd.read_csv(io.BytesIO(obj["Body"].read()), sep=";")

    # Initial visitas filter (get clientes for cross-reference with ventas)
    if pais == "MX":
        visitas_compania = config.get("visitas_compania_int", config.get("compania_ventas_int"))
        pan_visitas = pan_visitas[
            (pan_visitas["compania__c"] == visitas_compania) &
            (pan_visitas["cod_ruta"].isin(rutas)) &
            (pan_visitas["codigo_canal__c"] == 2)
        ].reset_index(drop=True)
    elif config.get("cod_pais"):  # CAM countries
        pan_visitas = pan_visitas[
            (pan_visitas["pais__c"] == config["cod_pais"]) &
            (pan_visitas["cod_ruta"].isin(rutas)) &
            (pan_visitas["codigo_canal__c"] == 2)
        ].reset_index(drop=True)
    else:  # EC, PE
        pan_visitas = pan_visitas[pan_visitas["cod_ruta"].isin(rutas)].reset_index(drop=True)

    clientes_ruta_test = pan_visitas["codigo_cliente__c"].unique()

    # 3. Descargar Ventas
    pan_ventas = pd.DataFrame()
    for ventas_key in config["ventas_keys"]:
        try:
            obj = s3.get_object(Bucket=resource_variables["BUCKET_ARTIFACTS"], Key=f"{prefix}{ventas_key}")
            df_temp = pd.read_csv(io.BytesIO(obj["Body"].read()), sep=";")

            if config.get("compania_ventas_int"):
                df_temp = df_temp[
                    (df_temp["cod_compania"] == config["compania_ventas_int"]) &
                    ((df_temp["cod_ruta"].isin(rutas)) | (df_temp["cod_cliente"].isin(clientes_ruta_test)))
                ].reset_index(drop=True)[COLUMNAS_VENTAS]
            else:
                df_temp = df_temp[
                    (df_temp["cod_ruta"].isin(rutas)) | (df_temp["cod_cliente"].isin(clientes_ruta_test))
                ].reset_index(drop=True)[COLUMNAS_VENTAS]

            pan_ventas = pd.concat([pan_ventas, df_temp], ignore_index=True)
        except Exception as e:
            print(f"Archivo {ventas_key} no encontrado o error: {e}")

    if config.get("cod_pais"):  # CAM: filter by cod_pais after download
        pan_ventas = pan_ventas[pan_ventas["cod_pais"] == config["cod_pais"]].reset_index(drop=True)

    # Extract cod_articulo_magic
    pan_ventas["cod_articulo_magic"] = pan_ventas["id_producto"].str.split("|").str[-1].astype(int)

    # Peru-specific filters
    if pais == "PE":
        pe_special = config["pe_special"]
        # Eliminate TRES CRUCES from non-Lima sucursales
        pan_ventas["desc_marca"] = pan_ventas["desc_marca"].str.strip()
        pan_ventas = pan_ventas[
            (pan_ventas['desc_sucursal'].isin(pe_special["sucursales_lima"])) |
            (~pan_ventas['desc_marca'].isin(['TRES CRUCES']))
        ].reset_index(drop=True)
        # Exclude Lima Centro ruta+SKU combinations
        pan_ventas = pan_ventas[
            ~(pan_ventas['cod_ruta'].isin(pe_special["rutas_excluir_lima"]) &
              pan_ventas['cod_articulo_magic'].isin(pe_special["skus_excluir_lima"]))
        ]
        # Filter by Excel SKU allowlist from SageMaker default bucket
        sagemaker_session = boto3.Session(region_name="us-east-2")
        sts = sagemaker_session.client("sts")
        account_id = sts.get_caller_identity()["Account"]
        default_bucket = f"sagemaker-us-east-2-{account_id}"
        excel_s3_path = f"s3://{default_bucket}/{pe_special['skus_allowlist_s3_suffix']}"
        excel_local = "/opt/ml/processing/pe_ps_marcas_skus_a_usar.xlsx"
        wr.s3.download(path=excel_s3_path, local_file=excel_local, boto3_session=my_session)
        skus_a_usar = pd.read_excel(excel_local, sheet_name="Hoja1")
        pan_ventas = pan_ventas[
            (pan_ventas.cod_articulo_magic.isin(skus_a_usar.cod_articulo_magic.unique())) |
            (pan_ventas.desc_marca == "TRES CRUCES")
        ].reset_index(drop=True)

    # Backup de visitas diario en S3
    wr.s3.to_csv(pan_visitas, f's3://aje-analytics-ps-backup/{config["backup_s3"]}_{formatted_date}.csv',
                 index=False, boto3_session=my_session)

    # Prepare id_cliente keys
    pan_ventas["cod_compania"] = pan_ventas["cod_compania"].astype(str).apply(
        lambda x: str(int(x)).rjust(4, "0")
    )
    pan_ventas["id_cliente"] = (
        id_prefix + "|" + pan_ventas["cod_compania"] + "|" + pan_ventas["cod_cliente"].astype(int).astype(str)
    )

    pan_visitas["compania__c"] = pan_visitas["compania__c"].astype(str).apply(
        lambda x: str(int(float(x))).rjust(4, "0") if x.replace('.', '', 1).isdigit() else str(x).rjust(4, "0")
    )
    pan_visitas["id_cliente"] = (
        id_prefix + "|" + pan_visitas["compania__c"] + "|" + pan_visitas["codigo_cliente__c"].astype(int).astype(str)
    )

    # Post-download visitas filter
    if pais not in ("MX",):  # EC, CAM, PE: filter canal 2 here
        pan_visitas = pan_visitas[pan_visitas["codigo_canal__c"] == 2].reset_index(drop=True)
    if config.get("compania_str"):  # CAM: also filter by compania
        pan_visitas = pan_visitas[pan_visitas["compania__c"] == config["compania_str"]].reset_index(drop=True)
    if pais == "PE":
        pass  # PE has multiple companies, no additional company filter

    # Mexico-specific: rename sucursal__c -> cod_sucursal in visitas
    if config.get("mx_sucursal"):
        pan_visitas = pan_visitas.rename(columns={"sucursal__c": "cod_sucursal"})

    # Deduplicar visitas: priorizar filas con visita mañana
    visita_default = (datetime.now(tz) - timedelta(days=7)).strftime("%Y-%m-%d")
    pan_visitas["ultima_visita"] = pan_visitas["ultima_visita"].fillna(visita_default)
    dia_actual = datetime.now(tz).weekday() + 1
    dia_siguiente = 7 if dia_actual == 6 else (dia_actual + 1) % 7
    pan_visitas["tiene_dia_manana"] = pan_visitas["dias_de_visita__c"].astype(str).apply(
        lambda x: 1 if str(dia_siguiente) in x.split(";") else 0
    )
    pan_visitas = pan_visitas.sort_values(
        ["id_cliente", "tiene_dia_manana", "ultima_visita"], ascending=[True, False, False]
    ).groupby("id_cliente").head(1)
    pan_visitas = pan_visitas.drop(columns=["tiene_dia_manana"])

    # Cruce Ventas y Visitas
    cols_visitas = [
        "id_cliente", "dias_de_visita__c", "periodo_de_visita__c", "ultima_visita",
        "cod_ruta", "cod_modulo", "eje_potencial__c"
    ]
    if config.get("mx_sucursal"):
        cols_visitas = [
            "id_cliente", "dias_de_visita__c", "periodo_de_visita__c", "ultima_visita",
            "cod_ruta", "cod_modulo", "cod_sucursal", "eje_potencial__c"
        ]

    df_merged = pd.merge(pan_ventas, pan_visitas[cols_visitas], on="id_cliente",
                         how="inner", suffixes=("_vta", "_vis"))

    df_merged["cod_ruta"] = df_merged["cod_ruta_vis"].combine_first(df_merged["cod_ruta_vta"]).astype(int)
    df_merged["cod_modulo"] = df_merged["cod_modulo_vis"].combine_first(df_merged["cod_modulo_vta"]).astype(int)
    drop_cols = ["cod_ruta_vta", "cod_ruta_vis", "cod_modulo_vta", "cod_modulo_vis"]
    if config.get("mx_sucursal"):
        df_merged["cod_sucursal"] = df_merged["cod_sucursal_vis"].combine_first(df_merged["cod_sucursal_vta"]).astype(str)
        drop_cols += ["cod_sucursal_vta", "cod_sucursal_vis"]
    df_merged = df_merged.drop(columns=drop_cols)

    # Segment + brand renaming
    df_merged["desc_marca"] = df_merged["desc_marca"].str.strip().replace({"DEPORADE": "SPORADE"})
    mapping_segmento = {"S1": "BLINDAR", "S2": "DESARROLLAR", "S4": "MANTENER", "S5": "OPTIMIZAR"}
    df_merged["new_segment"] = df_merged["eje_potencial__c"].map(mapping_segmento).fillna("OPTIMIZAR")
    df_merged["mes"] = pd.to_datetime(df_merged["fecha_liquidacion"]).dt.strftime("%Y-%m-01")

    return df_merged


def filtrar_visitas_manana(df):
    """Filtra clientes con visita programada para mañana."""
    tz = pytz.timezone("America/Lima")
    dia_actual = datetime.now(tz).weekday() + 1
    dia_siguiente = 7 if dia_actual == 6 else (dia_actual + 1) % 7
    return df[
        df["dias_de_visita__c"].astype(str).apply(lambda x: str(dia_siguiente) in x.split(";"))
    ].reset_index(drop=True)


def preparar_rutas_y_pesos(df_ventas, output_dir, low_sku_threshold):
    """Calcula pesos por giro/categoría y divide el dataset por ruta."""
    df_ventas["fecha_liquidacion"] = pd.to_datetime(df_ventas["fecha_liquidacion"], format="%Y-%m-%d")
    df_ventas["desc_marca"] = df_ventas["desc_marca"].str.strip()
    df_ventas["desc_categoria"] = df_ventas["desc_categoria"].str.strip()

    mapeo_diccionario = {}
    for giro_v in df_ventas["desc_subgiro"].unique():
        temp = df_ventas[df_ventas["desc_subgiro"] == giro_v]
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
                b.append(3 if a[3] <= a[i] * multiplicador else 3 + i)
            ranks["Ranking"] = b
        mapeo_diccionario[giro_v] = ranks.set_index("index")["Ranking"].to_dict()

    with open(os.path.join(output_dir, "mapeo_diccionario.json"), "w") as f:
        json.dump(mapeo_diccionario, f)

    rutas = (
        df_ventas.groupby("cod_ruta")["id_cliente"].nunique()
        .sort_values(ascending=False).reset_index()["cod_ruta"].unique()
    )
    low_sku_ruta = []
    for ruta in rutas:
        temp = df_ventas[df_ventas["cod_ruta"] == ruta]
        if temp["cod_articulo_magic"].nunique() < low_sku_threshold:
            low_sku_ruta.append(ruta)
        else:
            temp.to_csv(os.path.join(output_dir, f"rutas/D_{ruta}_ventas.csv"), index=False)

    if low_sku_ruta:
        df_ventas[df_ventas["cod_ruta"].isin(low_sku_ruta)].to_csv(
            os.path.join(output_dir, "rutas/D_low_ruta_ventas.csv"), index=False
        )


def procesar_pais(pais, output_base, pais_config, resource_variables):
    config = pais_config
    output_dir = output_base
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(os.path.join(output_dir, "rutas"), exist_ok=True)

    print(f"\n{'='*60}")
    print(f"Procesando país: {config['nombre']} ({pais})")
    print(f"{'='*60}")

    comprobar_inputs(config, resource_variables)

    print(f"[{pais}] Extrayendo datos...")
    df_maestro = extraer_datos(pais, config, output_dir, resource_variables)

    print(f"[{pais}] Filtrando clientes para mañana...")
    df_manana = filtrar_visitas_manana(df_maestro)

    parquet_path = os.path.join(output_dir, config["ventas_parquet"])
    df_manana.to_parquet(parquet_path, index=False)
    print(f"[{pais}] Guardado parquet: {parquet_path} ({len(df_manana)} filas)")

    print(f"[{pais}] Preparando rutas y pesos...")
    preparar_rutas_y_pesos(df_manana, output_dir, config["low_sku_threshold"])

    print(f"[{pais}] Limpieza finalizada.")


def main():
    resource_variables = {
        "BUCKET_ARTIFACTS": os.environ.get("BUCKET_ARTIFACTS"),
        "TABLE_CONFIG": os.environ.get("TABLE_CONFIG")
    }
    
    missing_vars = [key for key, value in resource_variables.items() if not value]
    if missing_vars:
        raise ValueError(f"Variables de recursos faltantes: {', '.join(missing_vars)}")

    pais = os.environ.get("PAIS", "").strip().upper()
    if not pais:
        raise ValueError("Variable de entorno PAIS no configurada.")
    
    pais_config = load_pais_config(resource_variables, pais)

    print(f"Iniciando Limpieza para: {pais}")
    os.makedirs(OUTPUT_BASE, exist_ok=True)
    procesar_pais(pais, OUTPUT_BASE, pais_config, resource_variables)
    print(f"\nLimpieza para {pais} finalizada.")


if __name__ == "__main__":
    main()
