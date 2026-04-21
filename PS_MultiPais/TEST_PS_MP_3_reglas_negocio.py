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
import pandas as pd
import numpy as np
import awswrangler as wr
from datetime import datetime, timedelta

os.environ["AWS_DEFAULT_REGION"] = "us-east-2"
my_session = boto3.Session(region_name="us-east-2")

INPUT_BASE_LIMPIEZA = "/opt/ml/processing/input/limpieza"
INPUT_BASE_MODELADO = "/opt/ml/processing/input/modelado"
S3_BUCKET_BACKUP = "aje-analytics-ps-backup"

tz_lima = pytz.timezone("America/Lima")
fecha_actual = datetime.now(tz_lima)
fecha_tomorrow = (fecha_actual + timedelta(days=1)).strftime("%Y-%m-%d")

CONFIG_PATH = "/opt/ml/processing/input/config/paises_config.json"


def clasificar_valor(x):
    if x > 0: return "S"
    elif x == 0: return "M"
    else: return "B"


def aplicar_filtros_disponibilidad(pan_rec, df_ventas, config):
    """Applies availability and stock filters (rules 5.-9 through 5.-3)."""
    id_prefix = config["id_prefix"]
    print(f"  Inicio: {pan_rec.id_cliente.nunique()} clientes, {pan_rec.shape[0]} filas")

    # --- 5.-9 SKUs con ventas en los últimos 14 días ---
    fecha_limite = (datetime.now() - timedelta(days=14)).strftime('%Y-%m-%d')
    ventas_filtradas = df_ventas[df_ventas["fecha_liquidacion"] >= fecha_limite]
    productos_por_ruta = ventas_filtradas.groupby("cod_ruta")["cod_articulo_magic"].unique().reset_index()

    pan_rec = pan_rec.merge(df_ventas[["id_cliente", "cod_ruta"]].drop_duplicates(), on="id_cliente", how="left")
    rec_validas = pan_rec.merge(productos_por_ruta, on="cod_ruta", how="inner")
    rec_validas = rec_validas[rec_validas.apply(
        lambda row: row["cod_articulo_magic_x"] in row["cod_articulo_magic_y"], axis=1
    )]
    pan_rec = rec_validas[["id_cliente", "cod_articulo_magic_x", "cod_ruta"]].rename(
        columns={"cod_articulo_magic_x": "cod_articulo_magic"}
    ).reset_index(drop=True)
    print(f"  Después 5.-9 (ventas 14d): {pan_rec.id_cliente.nunique()} clientes, {pan_rec.shape[0]} filas")

    # --- 5.-8 Subida, Bajada, Mantener ---
    fecha_30dias = (fecha_actual - timedelta(days=30)).strftime('%Y-%m-%d')
    fecha_60dias = (fecha_actual - timedelta(days=60)).strftime('%Y-%m-%d')
    df_ultimos_30 = df_ventas[
        (df_ventas['fecha_liquidacion'] > fecha_30dias) &
        (df_ventas['fecha_liquidacion'] <= fecha_actual.strftime('%Y-%m-%d'))
    ]
    df_31_60 = df_ventas[
        (df_ventas['fecha_liquidacion'] > fecha_60dias) &
        (df_ventas['fecha_liquidacion'] <= fecha_30dias)
    ]
    ventas_30 = df_ultimos_30.groupby(['cod_ruta', 'cod_articulo_magic'])['imp_netovta'].sum().reset_index().assign(mes="0_30")
    ventas_60 = df_31_60.groupby(['cod_ruta', 'cod_articulo_magic'])['imp_netovta'].sum().reset_index().assign(mes="31_60")
    df_grouped = pd.concat([ventas_30, ventas_60], ignore_index=True)
    df_grouped = pd.pivot_table(
        df_grouped, values="imp_netovta", index=["cod_ruta", "cod_articulo_magic"],
        columns=["mes"], aggfunc="sum"
    ).reset_index().fillna(0)
    df_grouped["vp"] = (
        (df_grouped.get("0_30", 0) - df_grouped.get("31_60", 0)) / df_grouped.get("31_60", 1) * 100
    ).fillna(-1).replace([np.inf, -np.inf], -1)
    df_grouped["flag_rank"] = df_grouped["vp"].apply(clasificar_valor).map({"S": 0, "M": 1, "B": 2})
    pan_rec["original_order"] = pan_rec.index
    pan_rec = pd.merge(pan_rec, df_grouped[["cod_ruta", "cod_articulo_magic", "flag_rank"]],
                       on=["cod_ruta", "cod_articulo_magic"], how="left")
    pan_rec["flag_rank"] = pan_rec["flag_rank"].fillna(3)
    pan_rec = pan_rec.sort_values(by=["id_cliente", "flag_rank", "original_order"]).reset_index(drop=True)
    print(f"  Después 5.-8 (S/M/B): {pan_rec.id_cliente.nunique()} clientes, {pan_rec.shape[0]} filas")

    # --- 5.-7 Archivo de Validación (maestro_productos) ---
    skus_val = wr.s3.read_csv(config["maestro_val_s3"], sep=";", boto3_session=my_session)
    if config.get("maestro_val_compania") is not None:
        skus_val = skus_val[skus_val.cod_compania == config["maestro_val_compania"]].copy()
    skus_val["cod_compania"] = skus_val["cod_compania"].astype(str).str.zfill(4)
    skus_val["cod_cliente"] = pd.to_numeric(skus_val["cod_cliente"], errors="coerce").fillna(0).astype(int)
    skus_val["id_cliente"] = id_prefix + "|" + skus_val["cod_compania"] + "|" + skus_val["cod_cliente"].astype(str)
    pan_rec = pd.merge(pan_rec, skus_val[['cod_articulo_magic', 'id_cliente']].drop_duplicates(),
                       on=["id_cliente", "cod_articulo_magic"], how="inner")
    print(f"  Después 5.-7 (maestro): {pan_rec.id_cliente.nunique()} clientes, {pan_rec.shape[0]} filas")

    # --- Peru special: exclude SKU 608597 from specific routes ---
    if config.get("pe_special"):
        rutas_excluir_608597 = config["pe_special"]["rutas_excluir_608597"]
        pan_rec = pan_rec[
            ~((pan_rec.cod_ruta.isin(rutas_excluir_608597)) & (pan_rec.cod_articulo_magic == 608597))
        ]
        print(f"  Después filtro PE 608597: {pan_rec.id_cliente.nunique()} clientes, {pan_rec.shape[0]} filas")

    # --- 5.-5 Filtro STOCK ---
    stock = wr.s3.read_csv(config["stock_s3"], boto3_session=my_session)
    stock = stock.drop(columns=["Fecha", "Database"])
    stock.columns = ["cod_compania", "cod_sucursal", "cod_articulo_magic", "stock_cf"]
    stock["cod_compania"] = stock["cod_compania"].astype(str).str.zfill(4)
    stock["cod_sucursal"] = stock["cod_sucursal"].astype(str).str.zfill(2)

    fecha_12_dias = (fecha_actual - timedelta(days=12)).strftime('%Y-%m-%d')
    prom_diario_vta = df_ventas[
        (df_ventas.cant_cajafisicavta > 0) & (df_ventas.fecha_liquidacion >= fecha_12_dias)
    ]
    prom_diario_vta = prom_diario_vta.groupby(
        ["cod_compania", "cod_sucursal", "cod_articulo_magic"]
    ).cant_cajafisicavta.mean().reset_index()
    prom_diario_vta["cod_compania"] = prom_diario_vta["cod_compania"].astype(str).str.zfill(4)
    prom_diario_vta["cod_sucursal"] = prom_diario_vta["cod_sucursal"].astype(str).str.zfill(2)

    df_stock = pd.merge(prom_diario_vta, stock, on=["cod_compania", "cod_sucursal", "cod_articulo_magic"], how="left")
    df_stock["dias_stock"] = df_stock["stock_cf"] / df_stock["cant_cajafisicavta"]
    df_stock = df_stock[(df_stock.dias_stock > 3) & (df_stock.cant_cajafisicavta > 0)]

    pan_rec = pan_rec.merge(
        df_ventas[["id_cliente", "cod_compania", "cod_sucursal"]].drop_duplicates(), on="id_cliente", how="left"
    )
    pan_rec["cod_compania"] = pan_rec["cod_compania"].astype(str).str.zfill(4)
    pan_rec["cod_sucursal"] = pan_rec["cod_sucursal"].astype(str).str.zfill(2)
    pan_rec = pd.merge(pan_rec, df_stock, on=["cod_compania", "cod_sucursal", "cod_articulo_magic"], how="inner")
    print(f"  Después 5.-5 (stock): {pan_rec.id_cliente.nunique()} clientes, {pan_rec.shape[0]} filas")

    # --- 5.-3 Quitar SKUs sin precio ---
    if config["skus_sin_precio"]:
        pan_rec = pan_rec[~pan_rec["cod_articulo_magic"].isin(config["skus_sin_precio"])].reset_index(drop=True)
    print(f"  Después 5.-3 (sin precio): {pan_rec.id_cliente.nunique()} clientes, {pan_rec.shape[0]} filas")

    return pan_rec[["id_cliente", "cod_articulo_magic"]].drop_duplicates().reset_index(drop=True)


def aplicar_filtros_historia(pan_rec, df_ventas, config):
    """Applies historical recommendation (5.-2) and purchase exclusion (5.3) filters."""
    id_prefix = config["id_prefix"]
    s3_output_prefix = config["s3_output_prefix"]

    # 5.-2 Histórico de recomendaciones (últimos 14 días)
    s3 = my_session.client("s3")
    objetos = s3.list_objects_v2(Bucket=S3_BUCKET_BACKUP, Prefix=s3_output_prefix)
    fechas_recs = []
    if "Contents" in objetos:
        for obj in objetos["Contents"]:
            if obj["Key"].endswith(".csv") and "D_base_pedidos_" in obj["Key"]:
                fecha_str = obj["Key"].split("_")[-1].replace(".csv", "")
                if len(fecha_str) == 10 and fecha_str[4] == "-":
                    fechas_recs.append(fecha_str)

    last_14_days = [f for f in sorted(fechas_recs) if f != fecha_tomorrow][-14:]
    last_14_recs = pd.DataFrame()
    for fecha in last_14_days:
        s3_uri = f"s3://{S3_BUCKET_BACKUP}/{s3_output_prefix}D_base_pedidos_{fecha}.csv"
        try:
            df_temp = wr.s3.read_csv(s3_uri, dtype={"Compania": str, "Cliente": str}, boto3_session=my_session)
            df_temp["id_cliente"] = id_prefix + "|" + df_temp["Compania"] + "|" + df_temp["Cliente"]
            df_temp = df_temp[df_temp["id_cliente"].isin(pan_rec["id_cliente"].unique())]
            last_14_recs = pd.concat([last_14_recs, df_temp], axis=0)
        except Exception as e:
            print(f"  No se pudo leer {s3_uri}: {e}")

    if not last_14_recs.empty:
        df_combinado = pd.merge(
            pan_rec, last_14_recs,
            left_on=['id_cliente', 'cod_articulo_magic'], right_on=['id_cliente', 'Producto'],
            how='left', indicator=True
        )
        df_unicos = df_combinado[df_combinado['_merge'] == 'left_only'][["id_cliente", "cod_articulo_magic"]].drop_duplicates()
        df_coinciden = df_combinado[df_combinado['_merge'] == 'both'][["id_cliente", "cod_articulo_magic"]].drop_duplicates()
        pan_rec = pd.concat([df_unicos, df_coinciden], ignore_index=True)
    print(f"  Después 5.-2 (histórico 14d): {pan_rec.id_cliente.nunique()} clientes, {pan_rec.shape[0]} filas")

    # 5.3 Excluir compras de las últimas 2 semanas
    last_2_weeks = (datetime.now() - timedelta(days=14)).strftime('%Y-%m-%d')
    df_ventas["fecha_liquidacion"] = pd.to_datetime(df_ventas["fecha_liquidacion"]).dt.strftime('%Y-%m-%d')
    compras_recientes = df_ventas[df_ventas["fecha_liquidacion"] >= last_2_weeks][
        ["id_cliente", "cod_articulo_magic"]
    ].drop_duplicates()
    pan_rec = pan_rec.merge(compras_recientes, on=['id_cliente', 'cod_articulo_magic'], how='left', indicator=True)
    pan_rec = pan_rec[pan_rec['_merge'] == 'left_only'].drop(columns=['_merge'])
    print(f"  Después 5.3 (compras 2 sem): {pan_rec.id_cliente.nunique()} clientes, {pan_rec.shape[0]} filas")

    return pan_rec.reset_index(drop=True)


def calcular_metricas_y_ensamblar(pan_rec, df_ventas, limpieza_dir, config):
    """Calculates metrics, applies segment-based limits, and assembles final dataset."""
    maestro_prod = pd.read_csv(os.path.join(limpieza_dir, config["maestro_csv"]))
    with open(os.path.join(limpieza_dir, "mapeo_diccionario.json"), "r") as f:
        mapeo_diccionario = json.load(f)

    marca_articulo = df_ventas[["desc_categoria", "cod_articulo_magic"]].drop_duplicates()
    cliente_rec_marca = pd.merge(pan_rec, marca_articulo, on="cod_articulo_magic", how="left")
    cliente_rec_marca["desc_categoria"] = cliente_rec_marca["desc_categoria"].str.strip()
    cods2 = cliente_rec_marca.groupby("id_cliente")["desc_categoria"].nunique().reset_index()

    # Irregularidad (5.0)
    now_mes = pd.to_datetime(fecha_actual.strftime("%Y-%m-01"))
    lista_m12 = [now_mes - pd.DateOffset(months=12) + pd.DateOffset(months=i) for i in range(12)]
    lista_m6 = [now_mes - pd.DateOffset(months=6) + pd.DateOffset(months=i) for i in range(6)]
    temp_v = df_ventas[df_ventas["id_cliente"].isin(pan_rec["id_cliente"].unique())]
    qw = (temp_v[["id_cliente", "mes"]].drop_duplicates()
          .sort_values(["id_cliente", "mes"])
          .groupby("id_cliente").tail(12).reset_index(drop=True))
    qw["mes"] = pd.to_datetime(qw["mes"])
    qw["m12"] = qw["mes"].isin(lista_m12)
    qw["m6"] = qw["mes"].isin(lista_m6)
    cat_cliente = qw.groupby("id_cliente")[["m12", "m6"]].sum().reset_index()
    cat_cliente["categoria_cliente_2"] = np.where(cat_cliente["m6"] < 4, "Irregular", "Regular")

    # Antigüedad (5.4)
    pan_ventas_antig = df_ventas.rename(columns={"fecha_creacion_cliente": "fecha_creacion"}).copy()
    pan_ventas_antig["fecha_creacion"] = pd.to_numeric(
        pan_ventas_antig["fecha_creacion"], errors="coerce"
    ).fillna(0).astype(int)
    pan_ventas_antig["fecha_creacion"] = pd.to_datetime(
        pan_ventas_antig["fecha_creacion"], format="%Y%m%d", errors="coerce"
    )
    hace_12_meses = datetime.now() - timedelta(days=365)
    pan_ventas_antig["antiguedad"] = pan_ventas_antig["fecha_creacion"].apply(
        lambda x: "nf" if pd.isnull(x) else ("new_client" if x >= hace_12_meses else "old_client")
    )
    pan_antiguedad = pan_ventas_antig[["id_cliente", "fecha_creacion", "antiguedad"]].drop_duplicates().reset_index(drop=True)

    # Assemble final dataset
    final_rec = cliente_rec_marca.groupby(["id_cliente", "desc_categoria"]).first().reset_index()
    final_rec = pd.merge(final_rec, cods2, how="left", on="id_cliente")
    maestro_prod = (maestro_prod[["cod_articulo_magic", "desc_articulo"]]
                    .drop_duplicates()
                    .groupby("cod_articulo_magic").first().reset_index())
    final_rec = pd.merge(final_rec, maestro_prod, how="left", on="cod_articulo_magic")
    final_rec.columns = ["id_cliente", "marca_rec", "sku", "len_marca_rec", "desc_rec"]

    giros = df_ventas[["id_cliente", "desc_giro", "desc_subgiro"]].drop_duplicates()
    final_rec = pd.merge(final_rec, giros, on="id_cliente", how="left")
    final_rec = pd.merge(final_rec, cat_cliente[["id_cliente", "categoria_cliente_2"]], on="id_cliente", how="left")
    final_rec.loc[final_rec["categoria_cliente_2"] == "Irregular", "desc_giro"] = np.nan
    final_rec = pd.merge(final_rec, pan_antiguedad, how="left", on="id_cliente")

    info_segmentos = (
        df_ventas[["id_cliente", "new_segment", "dias_de_visita__c", "periodo_de_visita__c", "ultima_visita"]]
        .drop_duplicates().groupby("id_cliente").first().reset_index()
    )
    final_rec = pd.merge(final_rec, info_segmentos, how="left", on="id_cliente")

    datos_sf = (
        df_ventas.groupby(["id_cliente", "cod_compania", "cod_sucursal", "cod_cliente", "cod_ruta"])
        [["id_cliente", "cod_compania", "cod_sucursal", "cod_cliente", "cod_modulo", "cod_ruta"]]
        .head(1).reset_index(drop=True)
    )
    final_rec = pd.merge(final_rec, datos_sf, how="left", on="id_cliente")

    # Apply category weights and take top 5
    final_rec["peso"] = final_rec.apply(
        lambda row: mapeo_diccionario.get(row["desc_subgiro"], {}).get(row["marca_rec"], 5), axis=1
    )
    final_rec = final_rec.sort_values(["id_cliente", "peso"]).groupby("id_cliente").head(5)
    final_rec["marca_rec_rank"] = final_rec.groupby("id_cliente").cumcount() + 1

    # Segment-based recommendation limit
    if config.get("pa_special") and "cod_ruta" in final_rec.columns:
        pa = config["pa_special"]
        rutas_especiales = set(pa["rutas_especiales"])

        def apply_pa_limits(g):
            ruta = g["cod_ruta"].iloc[0] if "cod_ruta" in g.columns else None
            limits = pa["reglas_rutas_especiales"] if ruta in rutas_especiales else pa["reglas_otras_rutas"]
            return g.head(limits.get(g["new_segment"].iloc[0], 5))

        final_rec = final_rec.groupby("id_cliente").apply(apply_pa_limits).reset_index(drop=True)
    else:
        limites_segmento = {"BLINDAR": 1, "MANTENER": 2, "DESARROLLAR": 3, "OPTIMIZAR": 4}
        final_rec = final_rec.groupby("id_cliente").apply(
            lambda g: g.head(limites_segmento.get(g["new_segment"].iloc[0], 5))
        ).reset_index(drop=True)
    print(f"  Después segmento: {final_rec.id_cliente.nunique()} clientes, {final_rec.shape[0]} filas")

    return final_rec


def exportar_resultados(final_rec, config, pais):
    """Exports final recommendations to S3 in both analytics and Salesforce formats."""
    pais_sf = pais
    print(f"  Exportando a S3 para fecha {fecha_tomorrow}...")

    # Analytics dataset
    s3_path_da = f"s3://{S3_BUCKET_BACKUP}/{config['s3_output_data_prefix']}D_pan_recs_data_{fecha_tomorrow}.csv"
    wr.s3.to_csv(final_rec, s3_path_da, index=False, boto3_session=my_session)

    # Salesforce format
    rec_sf = final_rec[["cod_compania", "cod_sucursal", "cod_cliente", "cod_modulo", "sku"]].copy()
    rec_sf["Pais"] = pais_sf
    rec_sf["Cajas"] = int(1)
    rec_sf["Unidades"] = int(0)
    rec_sf["Fecha"] = fecha_tomorrow
    rec_sf = rec_sf[["Pais", "cod_compania", "cod_sucursal", "cod_cliente", "cod_modulo",
                      "sku", "Cajas", "Unidades", "Fecha"]]
    rec_sf.columns = ["Pais", "Compania", "Sucursal", "Cliente", "Modulo",
                      "Producto", "Cajas", "Unidades", "Fecha"]
    rec_sf["Compania"] = rec_sf["Compania"].apply(lambda x: str(int(x)).rjust(4, "0"))
    rec_sf["Sucursal"] = rec_sf["Sucursal"].apply(lambda x: str(int(x)).rjust(2, "0"))
    rec_sf["tipoRecomendacion"] = rec_sf.groupby(
        ["Pais", "Compania", "Sucursal", "Cliente"]
    ).cumcount().apply(lambda x: f"PS{x+1}")
    rec_sf["ultFecha"] = ''
    rec_sf["Destacar"] = "true"

    s3_path_sf = f"s3://{S3_BUCKET_BACKUP}/{config['s3_output_prefix']}D_base_pedidos_{fecha_tomorrow}.csv"
    wr.s3.to_csv(rec_sf, s3_path_sf, index=False, boto3_session=my_session)

    print(f"  Clientes: {rec_sf.Cliente.nunique()} | SKUs: {rec_sf.Producto.nunique()}")
    print(f"  Archivos subidos a S3.")


def procesar_pais(pais, paises_config):
    config = paises_config[pais]
    limpieza_dir = INPUT_BASE_LIMPIEZA
    modelado_dir = INPUT_BASE_MODELADO

    print(f"\n{'='*60}")
    print(f"Reglas de Negocio: {config['nombre']} ({pais})")
    print(f"{'='*60}")

    ruta_ventas = os.path.join(limpieza_dir, config["ventas_parquet"])
    ruta_recs = os.path.join(modelado_dir, "D_rutas_rec.parquet")

    df_ventas = pd.read_parquet(ruta_ventas)
    pan_rec = pd.read_parquet(ruta_recs)

    print("Aplicando filtros de disponibilidad...")
    pan_rec_disp = aplicar_filtros_disponibilidad(pan_rec, df_ventas, config)

    # Ecuador: exclude Pedido Recurrente
    if config.get("has_pedido_recurrente"):
        print("Quitando productos de Pedido Recurrente...")
        try:
            pr = wr.s3.read_csv(
                f"{config['pedido_recurrente_s3']}{fecha_tomorrow}.csv",
                boto3_session=my_session
            )
            pr["Compania"] = pr["Compania"].astype(str).str.zfill(4)
            pr["id_cliente"] = config["id_prefix"] + "|" + pr["Compania"] + "|" + pr["Cliente"].astype(str)
            pr.rename(columns={"Producto": "cod_articulo_magic"}, inplace=True)
            merge_temp = pan_rec_disp.merge(
                pr[["id_cliente", "cod_articulo_magic"]].drop_duplicates(),
                on=["id_cliente", "cod_articulo_magic"], how="left", indicator=True
            )
            pan_rec_disp = merge_temp[merge_temp["_merge"] == "left_only"].drop(columns=["_merge"]).reset_index(drop=True)
            print(f"  Recurrente excluido. Recomendaciones restantes: {pan_rec_disp.shape[0]}")
        except Exception as e:
            print(f"  No se pudo leer pedido recurrente: {e}. Se continúa sin excluir.")

    print("Aplicando filtros históricos...")
    pan_rec_hist = aplicar_filtros_historia(pan_rec_disp, df_ventas, config)

    print("Calculando métricas y ensamblando dataset final...")
    final_rec = calcular_metricas_y_ensamblar(pan_rec_hist, df_ventas, limpieza_dir, config)

    exportar_resultados(final_rec, config, pais)
    print(f"[{pais}] Proceso finalizado.")


def main():
    with open(CONFIG_PATH, "r") as f:
        paises_config = json.load(f)

    pais = os.environ.get("PAIS", "").strip().upper()
    if not pais or pais not in paises_config:
        raise ValueError(f"Variable de entorno PAIS inválida o no configurada: '{pais}'")

    print(f"Iniciando Reglas de Negocio para: {pais}")
    procesar_pais(pais, paises_config)
    print(f"\nReglas de Negocio para {pais} finalizadas.")


if __name__ == "__main__":
    main()
