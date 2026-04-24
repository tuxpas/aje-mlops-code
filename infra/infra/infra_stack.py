from aws_cdk import (
    Duration,
    Stack,
    aws_lambda as _lambda,
    aws_events as events,
    aws_events_targets as targets,
    aws_codepipeline as codepipeline,
    aws_codepipeline_actions as cpactions,
    aws_codebuild as codebuild,
    aws_iam as iam,
    aws_sns as sns,
    aws_cloudwatch as cloudwatch,
    aws_cloudwatch_actions as cw_actions,
)
from constructs import Construct


class InfraStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ── Lambda ──────────────────────────────────────────────────────────────
        lambda_role = iam.Role(
            self, "LambdaTriggerRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ],
        )
        lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=["sagemaker:StartPipelineExecution"],
                resources=["*"],
            )
        )

        trigger_fn = _lambda.Function(
            self, "PSTriggerLambda",
            function_name="ps-trigger-lambda",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="lambda.lambda_handler",
            code=_lambda.Code.from_asset("./lambda"),
            role=lambda_role,
            timeout=Duration.seconds(30),
            environment={"code_country": "EC"},
        )

        # ── EventBridge ─────────────────────────────────────────────────────────
        schedule_rule = events.Rule(
            self, "PSScheduleRule",
            rule_name="ps-weekly-trigger",
            description="Triggers the Pedido Sugerido SageMaker pipeline weekly",
            schedule=events.Schedule.cron(
                minute="0",
                hour="11",
                week_day="*", #ALL(*) ; MON,TUE,WED,THU,FRI,SAT,SUN ; MON-FRI ; MON,WED,FRI
            ),
        )
        schedule_rule.add_target(targets.LambdaFunction(trigger_fn))

        # ── CodeBuild – Build (Docker + ECR push) ────────────────────────────────
        build_role = iam.Role(
            self, "CodeBuildBuildRole",
            assumed_by=iam.ServicePrincipal("codebuild.amazonaws.com"),
        )
        build_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "ecr:GetAuthorizationToken",
                    "ecr:BatchCheckLayerAvailability",
                    "ecr:GetDownloadUrlForLayer",
                    "ecr:BatchGetImage",
                    "ecr:PutImage",
                    "ecr:InitiateLayerUpload",
                    "ecr:UploadLayerPart",
                    "ecr:CompleteLayerUpload",
                    "ecr:CreateRepository",
                    "ecr:DescribeRepositories",
                    "sts:GetCallerIdentity",
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:GetObjectVersion",
                ],
                resources=["*"],
            )
        )

        build_project = codebuild.Project(
            self, "PSBuildProject",
            project_name="aje-dev-ps-build",
            build_spec=codebuild.BuildSpec.from_source_filename("cicd/buildspec-build.yml"),
            environment=codebuild.BuildEnvironment(
                build_image=codebuild.LinuxBuildImage.STANDARD_7_0,
                privileged=True,
            ),
            role=build_role,
        )

        # ── CodeBuild – Deploy (SageMaker pipeline upsert) ───────────────────────
        deploy_role = iam.Role(
            self, "CodeBuildDeployRole",
            assumed_by=iam.ServicePrincipal("codebuild.amazonaws.com"),
        )
        deploy_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "sagemaker:CreatePipeline",
                    "sagemaker:UpdatePipeline",
                    "sagemaker:StartPipelineExecution",
                    "sagemaker:DescribePipeline",
                    "sagemaker:ListPipelineExecutions",
                    "iam:PassRole",
                    "s3:PutObject",
                    "s3:GetObject",
                    "s3:GetObjectVersion",
                    "s3:ListBucket",
                    "sts:GetCallerIdentity",
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                resources=["*"],
            )
        )

        deploy_project = codebuild.Project(
            self, "PSDeployProject",
            project_name="aje-dev-ps-deploy",
            build_spec=codebuild.BuildSpec.from_source_filename("cicd/buildspec-deploy.yml"),
            environment=codebuild.BuildEnvironment(
                build_image=codebuild.LinuxBuildImage.STANDARD_7_0,
                privileged=False,
            ),
            role=deploy_role,
            environment_variables={
                "EJECUTAR": codebuild.BuildEnvironmentVariable(value="false"),
                "PAIS": codebuild.BuildEnvironmentVariable(value=""),
            },
        )

        # ── GitLab connection (AWS CodeStar Connection) ─────────────────────────
        # Pre-requisite: create the connection in the AWS Console (Settings ▸
        # Connections) and paste the ARN as a CDK context value or parameter.
        # cdk deploy --context gitlab_connection_arn=arn:aws:codeconnections:...
        gitlab_connection_arn = self.node.try_get_context("gitlab_connection_arn") or ""

        # ── CodePipeline ─────────────────────────────────────────────────────────
        source_output = codepipeline.Artifact("SourceOutput")
        build_output = codepipeline.Artifact("BuildOutput")

        codepipeline.Pipeline(
            self, "PSPipeline",
            pipeline_name="aje-dev-ps-cicd-pipeline",
            stages=[
                codepipeline.StageProps(
                    stage_name="Source",
                    actions=[
                        cpactions.CodeStarConnectionsSourceAction(
                            action_name="GitLab_Source",
                            connection_arn=gitlab_connection_arn,
                            owner="DimAje",
                            repo="aje-analytics-model-pedido-sugerido",
                            branch="main",
                            output=source_output,
                        )
                    ],
                ),
                codepipeline.StageProps(
                    stage_name="Build",
                    actions=[
                        cpactions.CodeBuildAction(
                            action_name="Build_ECR_Images",
                            project=build_project,
                            input=source_output,
                            outputs=[build_output],
                        )
                    ],
                ),
                codepipeline.StageProps(
                    stage_name="Deploy",
                    actions=[
                        cpactions.CodeBuildAction(
                            action_name="Deploy_SageMaker_Pipeline",
                            project=deploy_project,
                            input=build_output,
                        )
                    ],
                ),
            ],
        )

        # ── SNS Topic – alerts ───────────────────────────────────────────────────
        alerts_topic = sns.Topic(
            self, "PSAlertsTopic",
            topic_name="aje-dev-ps-pipeline-alerts",
            display_name="AJE Pedido Sugerido – Pipeline Alerts",
        )

        # ── CloudWatch Alarms ────────────────────────────────────────────────────
        # The SageMaker pipeline name is supplied via CDK context so it can be
        # overridden per environment:  cdk deploy --context sagemaker_pipeline_name=...
        sm_pipeline_name = (
            self.node.try_get_context("sagemaker_pipeline_name") or "aje-dev-ps-pipeline"
        )

        def _sm_metric(metric_name: str) -> cloudwatch.Metric:
            return cloudwatch.Metric(
                namespace="AWS/SageMaker",
                metric_name=metric_name,
                dimensions_map={"PipelineName": sm_pipeline_name},
                period=Duration.minutes(60),
                statistic="Sum",
            )

        failed_alarm = cloudwatch.Alarm(
            self, "PSExecutionsFailedAlarm",
            alarm_name="aje-dev-ps-executions-failed",
            alarm_description="One or more SageMaker pipeline executions failed in the last hour.",
            metric=_sm_metric("ExecutionsFailed"),
            threshold=1,
            evaluation_periods=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )
        failed_alarm.add_alarm_action(cw_actions.SnsAction(alerts_topic))
        failed_alarm.add_ok_action(cw_actions.SnsAction(alerts_topic))

        stopped_alarm = cloudwatch.Alarm(
            self, "PSExecutionsStoppedAlarm",
            alarm_name="aje-dev-ps-executions-stopped",
            alarm_description="One or more SageMaker pipeline executions were stopped in the last hour.",
            metric=_sm_metric("ExecutionsStopped"),
            threshold=1,
            evaluation_periods=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )
        stopped_alarm.add_alarm_action(cw_actions.SnsAction(alerts_topic))

        # ── CloudWatch Dashboard ─────────────────────────────────────────────────
        dashboard = cloudwatch.Dashboard(
            self, "PSDashboard",
            dashboard_name="aje-dev-ps-pipeline-monitoring",
        )
        dashboard.add_widgets(
            cloudwatch.GraphWidget(
                title="SageMaker Pipeline Executions (hourly)",
                left=[
                    _sm_metric("ExecutionsSucceeded"),
                    _sm_metric("ExecutionsFailed"),
                    _sm_metric("ExecutionsStopped"),
                ],
                width=24,
            ),
        )
        dashboard.add_widgets(
            cloudwatch.AlarmWidget(
                title="Executions Failed",
                alarm=failed_alarm,
                width=12,
            ),
            cloudwatch.AlarmWidget(
                title="Executions Stopped",
                alarm=stopped_alarm,
                width=12,
            ),
        )
