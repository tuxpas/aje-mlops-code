"""
Orquestador Multi-País - Pedido Sugerido
Crea un pipeline de SageMaker usando imágenes ECR personalizadas y
los scripts PS_MP_X.py ya incluidos en cada imagen.
"""

import os
import boto3
import sagemaker
from sagemaker import get_execution_role
from botocore.exceptions import ClientError
from sagemaker.processing import Processor, ProcessingInput, ProcessingOutput
from sagemaker.workflow.steps import ProcessingStep, CacheConfig
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.parameters import ParameterString
from sagemaker.workflow.functions import Join

REGION = "us-east-2"

BUCKET_ARTIFACTS = "aje-dev-analytics-artifacts-s3" # Bucket for storing the inputs for the sm pipeline
BUCKET_BACKUP = "aje-analytics-ps-backup" # Bucket for storing the results of the reglas_negocio pipeline step
BUCKET_STEPS_RESULTS = "aje-dev-analytics-artifacts-s3" # Bucket for the results of the pipeline steps, for now is the same as bucket artifacts
TABLE_CONFIG = "aje-dev-ps-configtable-dynamodb" # Table for storing the specific configs of countries

PROCESSING_IMAGE_NAME = "aje-ps-processing"
SPARK_IMAGE_NAME = "aje-ps-spark"
IMAGE_TAG = "latest"

code_country = ParameterString(name="code_country")  # "EC", "MX", "PE", "CR", "GT", "NI" or "PA"


def get_ecr_image_uri(image_name, account_id, region=REGION, tag=IMAGE_TAG):
    return f"{account_id}.dkr.ecr.{region}.amazonaws.com/{image_name}:{tag}"

def crear_pipeline(sagemaker_session, role, default_bucket, account_id):
    """
    Creates a single 3-step SageMaker Pipeline parameterized by code_country.
    Each step uses a custom ECR image that already contains the processing script.
    """
    
    ecr_processing = get_ecr_image_uri(PROCESSING_IMAGE_NAME, account_id)
    ecr_spark = get_ecr_image_uri(SPARK_IMAGE_NAME, account_id)

    # ---------- ProcessingStep 1: Limpieza ----------
    proc_1 = Processor(
        image_uri=ecr_processing,
        role=role,
        instance_count=1,
        instance_type="ml.m5.4xlarge",
        entrypoint=["python3", "/opt/program/PS_MP_1_limpieza.py"],
        base_job_name="ps-limpieza",
        sagemaker_session=sagemaker_session,
    )

    step_limpieza = ProcessingStep(
        name="Limpieza",
        processor=proc_1,
        outputs=[
            ProcessingOutput(
                output_name="output-limpieza",
                source="/opt/ml/processing/output/limpieza",
                destination=Join(on="/", values=[f"s3://{default_bucket}/ps-pipeline", code_country, "limpieza"]),
            )
        ],
        cache_config=CacheConfig(enable_caching=True, expire_after="P3D"),
        environment={
            "PAIS": code_country,
            "BUCKET_ARTIFACTS": BUCKET_ARTIFACTS,
            "TABLE_CONFIG": TABLE_CONFIG
        },
    )

    # ---------- ProcessingStep 2: Modelado (PySpark / ALS) ----------
    proc_2 = Processor(
        image_uri=ecr_spark,
        role=role,
        instance_count=1,
        instance_type="ml.m5.2xlarge",
        entrypoint=["python3", "/opt/program/PS_MP_2_modelado.py"],
        base_job_name="ps-modelado",
        sagemaker_session=sagemaker_session,
        max_runtime_in_seconds=7200,
    )

    step_modelado = ProcessingStep(
        name="Modelado",
        processor=proc_2,
        inputs=[
            ProcessingInput(
                input_name="limpieza",
                source=step_limpieza.properties.ProcessingOutputConfig.Outputs[
                    "output-limpieza"
                ].S3Output.S3Uri,
                destination="/opt/ml/processing/input",
            )
        ],
        outputs=[
            ProcessingOutput(
                output_name="output-modelado",
                source="/opt/ml/processing/output/modelado",
                destination=Join(on="/", values=[f"s3://{default_bucket}/ps-pipeline", code_country, "modelado"]),
            )
        ],
        cache_config=CacheConfig(enable_caching=True, expire_after="P3D"),
        environment={
            "PAIS": code_country,
            "TABLE_CONFIG": TABLE_CONFIG
        },
    )

    # ---------- ProcessingStep 3: Reglas de Negocio ----------
    proc_3 = Processor(
        image_uri=ecr_processing,
        role=role,
        instance_count=1,
        instance_type="ml.m5.4xlarge",
        entrypoint=["python3", "/opt/program/PS_MP_3_reglas_negocio.py"],
        base_job_name="ps-reglas",
        sagemaker_session=sagemaker_session,
    )

    step_reglas = ProcessingStep(
        name="ReglasNegocio",
        processor=proc_3,
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
        ],
        outputs=[
            ProcessingOutput(
                output_name="output-reglas",
                source="/opt/ml/processing/output/reglas",
                destination=Join(on="/", values=[f"s3://{default_bucket}/ps-pipeline", code_country, "reglas"]),
            )
        ],
        environment={
            "PAIS": code_country,
            "BUCKET_BACKUP": BUCKET_BACKUP,
            "TABLE_CONFIG": TABLE_CONFIG
        },
    )

    pipeline = Pipeline(
        name="aje-dev-ps-pipeline-sagemaker",
        parameters=[code_country],
        steps=[step_limpieza, step_modelado, step_reglas],
        sagemaker_session=sagemaker_session,
    )
    return pipeline


def main(pais=None, ejecutar=False):
    """
    Creates (or updates) the SageMaker Pipeline and optionally starts it.

    Args:
        pais: code_country code to run (e.g. 'EC', 'MX'). Reads PAIS env var if not provided.
        ejecutar: if True, starts the pipeline execution. If False, only upserts the pipeline.
    """
    sagemaker_session = sagemaker.Session()
    role = os.getenv("SAGEMAKER_ROLE_ARN") or get_execution_role()
    #default_bucket = sagemaker_session.default_bucket()
    default_bucket = BUCKET_STEPS_RESULTS
    sts = boto3.client("sts", region_name=REGION)
    account_id = sts.get_caller_identity()["Account"]

    print(f"SageMaker role: {role}")
    print(f"Default bucket: {default_bucket}")
    print(f"Account ID: {account_id}")

    pipeline = crear_pipeline(sagemaker_session, role, default_bucket, account_id)
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
        execution = pipeline.start(parameters={"code_country": pais})
        print(f"Ejecución iniciada: {execution.arn}")
        return execution
    else:
        print("Pipeline listo (ejecutar=False, no se inició).")
        return None


if __name__ == "__main__":
    main()

