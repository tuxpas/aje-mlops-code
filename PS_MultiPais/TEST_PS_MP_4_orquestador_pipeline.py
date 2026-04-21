"""
Orquestador Multi-País - Pedido Sugerido
Crea y ejecuta un pipeline de SageMaker por país, usando los scripts
TEST_PS_MP_1_limpieza.py, TEST_PS_MP_2_modelado.py, TEST_PS_MP_3_reglas_negocio.py
que deben estar subidos en s3://{sagemaker_bucket}/scripts/MP/.
"""

import os
import boto3
import sagemaker
from sagemaker import get_execution_role
from sagemaker.processing import ProcessingInput, ProcessingOutput
from sagemaker.sklearn.processing import SKLearnProcessor
from sagemaker.spark.processing import PySparkProcessor
from sagemaker.workflow.steps import ProcessingStep, CacheConfig
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.parameters import ParameterString
from sagemaker.workflow.functions import Join

REGION = "us-east-2"
BUCKET_ARTIFACTS = "aje-prd-analytics-artifacts-s3"
SCRIPT_S3_PREFIX = "scripts/MP"


country = ParameterString(name="country") # "EC", "MX", "PE", "CR", "GT", "NI" o "PA"

def upload_scripts_to_s3(default_bucket, base_dir=None):
    """Uploads the MP scripts and config JSON to S3."""
    s3 = boto3.client("s3")
    if base_dir is None:
        base_dir = os.path.dirname(os.path.abspath(__file__))

    scripts = [
        "TEST_PS_MP_1_limpieza.py",
        "TEST_PS_MP_2_modelado.py",
        "TEST_PS_MP_3_reglas_negocio.py",
    ]
    for script in scripts:
        local_path = os.path.join(base_dir, script)
        s3_key = f"{SCRIPT_S3_PREFIX}/{script}"
        if not os.path.exists(local_path):
            print(f"ADVERTENCIA: Script no encontrado localmente: {local_path}")
            continue
        s3.upload_file(local_path, default_bucket, s3_key)
        print(f"Subido: s3://{default_bucket}/{s3_key}")

    # Upload country config JSON (one level up from the scripts folder)
    config_local = os.path.join(os.path.dirname(base_dir), "PS_PaisesConfig", "paises_config.json")
    config_key = f"{SCRIPT_S3_PREFIX}/paises_config.json"
    if os.path.exists(config_local):
        s3.upload_file(config_local, default_bucket, config_key)
        print(f"Subido: s3://{default_bucket}/{config_key}")
    else:
        print(f"ADVERTENCIA: Config JSON no encontrado: {config_local}")


def crear_pipeline(sagemaker_session, role, default_bucket):
    """
    Creates a single 3-step SageMaker Pipeline parameterized by country.
    The 'country' ParameterString is passed as the PAIS environment variable
    to each processing step at execution time.
    """
    prefix_scripts = f"s3://{default_bucket}/{SCRIPT_S3_PREFIX}"

    # ---------- ProcessingStep 1: Limpieza ----------
    proc_sklearn_1 = SKLearnProcessor(
        framework_version="1.2-1",
        role=role,
        instance_type="ml.m5.4xlarge",
        instance_count=1,
        base_job_name="ps-mp-limpieza",
        sagemaker_session=sagemaker_session,
    )

    step_limpieza = ProcessingStep(
        name="Limpieza",
        processor=proc_sklearn_1,
        inputs=[
            ProcessingInput(
                input_name="config",
                source=f"{prefix_scripts}/paises_config.json",
                destination="/opt/ml/processing/input/config",
            ),
        ],
        outputs=[
            ProcessingOutput(
                output_name="output-limpieza",
                source="/opt/ml/processing/output/limpieza",
                destination=Join(on="/", values=[f"s3://{default_bucket}/ps-mp-pipeline", country, "limpieza"]),
            )
        ],
        code=f"{prefix_scripts}/TEST_PS_MP_1_limpieza.py",
        job_arguments=[],
        kms_key=None,
        cache_config=CacheConfig(enable_caching=True, expire_after="P3D"),
        environment={"PAIS": country},
    )

    # ---------- ProcessingStep 2: Modelado (PySpark / ALS) ----------
    proc_spark = PySparkProcessor(
        framework_version="3.3",
        role=role,
        instance_type="ml.m5.2xlarge",
        instance_count=1,
        base_job_name="ps-mp-modelado",
        sagemaker_session=sagemaker_session,
        max_runtime_in_seconds=7200,
    )

    step_modelado = ProcessingStep(
        name="Modelado",
        processor=proc_spark,
        inputs=[
            ProcessingInput(
                input_name="limpieza",
                source=step_limpieza.properties.ProcessingOutputConfig.Outputs[
                    "output-limpieza"
                ].S3Output.S3Uri,
                destination="/opt/ml/processing/input",
            ),
            ProcessingInput(
                input_name="config",
                source=f"{prefix_scripts}/paises_config.json",
                destination="/opt/ml/processing/input/config",
            ),
        ],
        outputs=[
            ProcessingOutput(
                output_name="output-modelado",
                source="/opt/ml/processing/output/modelado",
                destination=Join(on="/", values=[f"s3://{default_bucket}/ps-mp-pipeline", country, "modelado"]),
            )
        ],
        code=f"{prefix_scripts}/TEST_PS_MP_2_modelado.py",
        cache_config=CacheConfig(enable_caching=True, expire_after="P3D"),
        environment={"PAIS": country},
    )

    # ---------- ProcessingStep 3: Reglas de Negocio ----------
    proc_sklearn_3 = SKLearnProcessor(
        framework_version="1.2-1",
        role=role,
        instance_type="ml.m5.4xlarge",
        instance_count=1,
        base_job_name="ps-mp-reglas",
        sagemaker_session=sagemaker_session,
    )

    step_reglas = ProcessingStep(
        name="ReglasNegocio",
        processor=proc_sklearn_3,
        inputs=[
            ProcessingInput(
                input_name="limpieza",
                source=step_limpieza.properties.ProcessingOutputConfig.Outputs[
                    "output-limpieza"
                ].S3Output.S3Uri,
                destination="/opt/ml/processing/input/limpieza",
            ),
            ProcessingInput(
                input_name="modelado",
                source=step_modelado.properties.ProcessingOutputConfig.Outputs[
                    "output-modelado"
                ].S3Output.S3Uri,
                destination="/opt/ml/processing/input/modelado",
            ),
            ProcessingInput(
                input_name="config",
                source=f"{prefix_scripts}/paises_config.json",
                destination="/opt/ml/processing/input/config",
            ),
        ],
        outputs=[
            ProcessingOutput(
                output_name="output-reglas",
                source="/opt/ml/processing/output/reglas",
                destination=Join(on="/", values=[f"s3://{default_bucket}/ps-mp-pipeline", country, "reglas"]),
            )
        ],
        code=f"{prefix_scripts}/TEST_PS_MP_3_reglas_negocio.py",
        cache_config=CacheConfig(enable_caching=True, expire_after="P3D"),
        environment={"PAIS": country},
    )

    pipeline = Pipeline(
        name="Pipeline-PedidoSugerido-OneRun",
        parameters=[country],
        steps=[step_limpieza, step_modelado, step_reglas],
        sagemaker_session=sagemaker_session,
    )
    return pipeline


def main(pais=None, ejecutar=False):
    """
    Creates (or updates) the single-country SageMaker Pipeline and optionally starts it.

    Args:
        pais: country code to run (e.g. 'EC', 'MX'). Reads PAIS env var if not provided.
        ejecutar: if True, starts the pipeline execution. If False, only upserts the pipeline.
    """
    sagemaker_session = sagemaker.Session()
    role = get_execution_role()
    default_bucket = sagemaker_session.default_bucket()

    print(f"SageMaker role: {role}")
    print(f"Default bucket: {default_bucket}")

    upload_scripts_to_s3(default_bucket)

    pipeline = crear_pipeline(sagemaker_session, role, default_bucket)
    pipeline.upsert(role_arn=role)
    print("Pipeline creado/actualizado en SageMaker.")

    if ejecutar:
        if pais is None:
            pais = os.environ.get("PAIS", "").strip().upper()
        if not pais:
            raise ValueError(
                "Se requiere especificar el país. "
                "Pasa el argumento 'pais' o define la variable de entorno PAIS."
            )
        print(f"Iniciando ejecución para: {pais}")
        execution = pipeline.start(parameters={"country": pais})
        print(f"Ejecución iniciada: {execution.arn}")
        return execution
    else:
        print("Pipeline listo (ejecutar=False, no se inició).")
        return None


if __name__ == "__main__":
    main()

