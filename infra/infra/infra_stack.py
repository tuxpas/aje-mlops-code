from aws_cdk import (
    Duration,
    RemovalPolicy,
    Stack,
    aws_s3 as s3,
    aws_dynamodb as dynamodb,
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


class AjePsInfraStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, stage: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ── S3 ──────────────────────────────────────────────────────────────
        codepipeline_bucket = s3.Bucket(
            self, f"Aje{stage.capitalize()}PsCodePipelineBucketS3",
            bucket_name=f"aje-{stage}-ps-codepipelinebucket-s3"
        )

        # ── Lambda ──────────────────────────────────────────────────────────────
        lambda_policy = iam.Policy(
            self, f"Aje{stage.capitalize()}PsLambdaPolicyIam",
            policy_name=f"aje-{stage}-ps-lambdapolicy-iam",
            statements=[
                iam.PolicyStatement(
                    actions=[
                        "sagemaker:StartPipelineExecution"
                    ],
                    resources=[f"arn:aws:sagemaker:{Stack.of(self).region}:{Stack.of(self).account}:pipeline/aje-{stage}-ps-pipeline"],
                ),
                iam.PolicyStatement(
                    actions=[
                        "logs:CreateLogGroup",
                        "logs:CreateLogStream",
                        "logs:PutLogEvents"
                    ],
                    resources=["*"],
                )
            ],
        )

        lambda_role = iam.Role(
            self, f"Aje{stage.capitalize()}PsLambdaRoleIam",
            role_name=f"aje-{stage}-ps-lambdarole-iam",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
        )
        lambda_policy.attach_to_role(lambda_role)

        trigger_fn = _lambda.Function(
            self, f"Aje{stage.capitalize()}PsTriggerFunctionLambda",
            function_name=f"aje-{stage}-ps-triggerfunction-lambda",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="lambda.lambda_handler",
            code=_lambda.Code.from_asset("./lambda"),
            role=lambda_role,
            timeout=Duration.seconds(30),
            environment={
                "code_country": "EC",
                "stage":stage
            },
        )

        # ── EventBridge ─────────────────────────────────────────────────────────
        for code_country in ["CR","EC","GT","MX","NI","PA","PE"]:
            schedule_rule = events.Rule(
                self, f"Aje{stage.capitalize()}PsScheduleRule{code_country}EventBridge",
                rule_name=f"aje-{stage}-ps-schedulerule-{code_country}-eventbridge",
                description="Triggers the Pedido Sugerido SageMaker pipeline",
                schedule=events.Schedule.cron(
                    minute="0",
                    hour="11",
                    week_day="*", #ALL(*) ; MON,TUE,WED,THU,FRI,SAT,SUN ; MON-FRI ; MON,WED,FRI
                ),
            )

            schedule_rule.add_target(
                targets.LambdaFunction(
                    trigger_fn,
                    event=events.RuleTargetInput.from_object({
                        "code_country": code_country
                    })
                )
            )

        # ── DynamoDB – Country config table ─────────────────────────────────────
        config_table = dynamodb.Table(
            self, f"Aje{stage.capitalize()}PsConfigTableDynamoDB",
            table_name=f"aje-{stage}-ps-configtable-dynamodb",
            partition_key=dynamodb.Attribute(
                name="code_country",
                type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
        )

        # ── CodeBuild – Build (Docker + ECR push) ────────────────────────────────
        build_policy = iam.Policy(
            self, f"Aje{stage.capitalize()}PsBuildPolicyIam",
            policy_name=f"aje-{stage}-ps-buildpolicy-iam",
            statements=[
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
            ],
        )

        build_role = iam.Role(
            self, f"Aje{stage.capitalize()}PsBuildRoleIam",
            role_name=f"aje-{stage}-ps-buildrole-iam",
            assumed_by=iam.ServicePrincipal("codebuild.amazonaws.com"),
        )
        build_policy.attach_to_role(build_role)

        build_project = codebuild.Project(
            self, f"Aje{stage.capitalize()}PsBuildProjectCodeBuild",
            project_name=f"aje-{stage}-ps-buildproject-codebuild",
            build_spec=codebuild.BuildSpec.from_asset("../cicd/buildspec-build.yml"),
            environment=codebuild.BuildEnvironment(
                build_image=codebuild.LinuxBuildImage.STANDARD_7_0,
                privileged=True,
            ),
            role=build_role,
        )

        # ── CodeBuild – Deploy (SageMaker pipeline upsert) ───────────────────────
        deploy_policy = iam.Policy(
            self, f"Aje{stage.capitalize()}PsDeployPolicyIam",
            policy_name=f"aje-{stage}-ps-deploypolicy-iam",
            statements=[
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
            ],
        )

        deploy_role = iam.Role(
            self, f"Aje{stage.capitalize()}PsDeployRoleIam",
            role_name=f"aje-{stage}-ps-deployrole-iam",
            assumed_by=iam.ServicePrincipal("codebuild.amazonaws.com"),
        )

        deploy_policy.attach_to_role(deploy_role)

        deploy_project = codebuild.Project(
            self, f"Aje{stage.capitalize()}PsDeployProjectCodeBuild",
            project_name=f"aje-{stage}-ps-deployproject-codebuild",
            build_spec=codebuild.BuildSpec.from_asset("../cicd/buildspec-deploy.yml"),
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

        pipeline_policy = iam.Policy(
            self, f"Aje{stage.capitalize()}PsPipelinePolicyIam",
            policy_name=f"aje-{stage}-ps-pipelinepolicy-iam",
            statements=[
                iam.PolicyStatement(
                    actions=[
                        "s3:GetObject",
                        "s3:PutObject",
                        "s3:GetObjectVersion",
                        "s3:GetBucketVersioning",
                        "s3:ListBucket",
                    ],
                    resources=[
                        f"arn:aws:s3:::aje-{stage}*",
                        f"arn:aws:s3:::aje-{stage}*/*"
                    ],
                ),
                iam.PolicyStatement(
                    actions=["codestar-connections:UseConnection"],
                    resources=[gitlab_connection_arn] if gitlab_connection_arn else ["*"],
                ),
                iam.PolicyStatement(
                    actions=[
                        "codebuild:BatchGetBuilds",
                        "codebuild:StartBuild",
                        "codebuild:StopBuild",
                    ],
                    resources=[
                        build_project.project_arn,
                        deploy_project.project_arn,
                    ],
                ),
                iam.PolicyStatement(
                    actions=["iam:PassRole"],
                    resources=[f"arn:aws:iam::{self.account}:role/aje-{stage}-ps-*"],
                ),
            ],
        )

        pipeline_role = iam.Role(
            self, f"Aje{stage.capitalize()}PsPipelineRoleIam",
            role_name=f"aje-{stage}-ps-pipelinerole-iam",
            assumed_by=iam.ServicePrincipal("codepipeline.amazonaws.com"),
        )
        pipeline_policy.attach_to_role(pipeline_role)

        # ── CodePipeline action roles ────────────────────────────────────────
        # Permissions are attached by CDK via auto-generated DefaultPolicy on each role.
        source_action_role = iam.Role(
            self, f"Aje{stage.capitalize()}PsSourceActionRoleIam",
            role_name=f"aje-{stage}-ps-sourceactionrole-iam",
            assumed_by=iam.ArnPrincipal(pipeline_role.role_arn),
        )

        build_action_role = iam.Role(
            self, f"Aje{stage.capitalize()}PsBuildActionRoleIam",
            role_name=f"aje-{stage}-ps-buildactionrole-iam",
            assumed_by=iam.ArnPrincipal(pipeline_role.role_arn),
        )

        deploy_action_role = iam.Role(
            self, f"Aje{stage.capitalize()}PsDeployActionRoleIam",
            role_name=f"aje-{stage}-ps-deployactionrole-iam",
            assumed_by=iam.ArnPrincipal(pipeline_role.role_arn),
        )

        codepipeline.Pipeline(
            self, f"Aje{stage.capitalize()}PsPipelineCodePipeline",
            pipeline_name=f"aje-{stage}-ps-pipeline-codepipeline",
            role=pipeline_role,
            artifact_bucket=codepipeline_bucket,
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
                            role=source_action_role,
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
                            role=build_action_role,
                            environment_variables={
                                "STAGE": codebuild.BuildEnvironmentVariable(value=stage),
                                "REGION": codebuild.BuildEnvironmentVariable(value=self.region),
                                "ACCOUNT": codebuild.BuildEnvironmentVariable(value=self.account),
                            }
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
                            role=deploy_action_role,
                            environment_variables={
                                "STAGE": codebuild.BuildEnvironmentVariable(value=stage),
                                "REGION": codebuild.BuildEnvironmentVariable(value=self.region),
                                "ACCOUNT": codebuild.BuildEnvironmentVariable(value=self.account),
                            }
                        )
                    ],
                ),
            ],
        )

        # ── SNS Topic – alerts ───────────────────────────────────────────────────
        alerts_topic = sns.Topic(
            self, f"Aje{stage.capitalize()}PsAlertTopicSns",
            topic_name=f"aje-{stage}-ps-alert-topic-sns",
            display_name="AJE Pedido Sugerido – Pipeline Alerts",
        )

        # ── CloudWatch Alarms ────────────────────────────────────────────────────

        def _sm_metric(metric_name: str) -> cloudwatch.Metric:
            return cloudwatch.Metric(
                namespace="AWS/SageMaker",
                metric_name=metric_name,
                dimensions_map={"PipelineName": f"aje-{stage}-ps-pipeline-sagemaker"},
                period=Duration.minutes(60),
                statistic="Sum",
            )

        failed_alarm = cloudwatch.Alarm(
            self, f"Aje{stage.capitalize()}PsExecutionFailedAlarmCloudWatch",
            alarm_name=f"aje-{stage}-ps-executionfailedalarm-cloudwatch",
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
            self, f"Aje{stage.capitalize()}PsExecutionStoppedAlarmCloudWatch",
            alarm_name=f"aje-{stage}-ps-executionstoppedalarm-cloudwatch",
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
            self, f"Aje{stage.capitalize()}PsMonitoringDashboardCloudWatch",
            dashboard_name=f"aje-{stage}-ps-monitoringdashboard-cloudwatch",
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
