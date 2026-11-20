module "mod_coupa_data_integration" {
  source = "git::ssh://org-62409820@GitHub.com/McK-Internal/cip-terraform_glue_shared_infra.git//modules/glue_job?ref=v3.0.4-hashicorpaws6"
  glue_version                  = "5.0"
  schedule                      = "cron(30 11 * * ? *)"
  product_id                    = local.product_id
  product_name                  = local.product_name
  environment                   = local.environment
  additional_tags               = local.additional_tags
  script_path                   = "${path.module}/jobs/coupa_data_integration.py"
  #common_library_version        = "1.0.36"
  code_bucket                   = module.app_infra.aws_s3_bucket_code.bucket
  glue_job_role_arn             = module.app_infra.aws_iam_role_service.arn
  #glue_logging_role_arn         = module.app_infra.aws_iam_role_logging.arn
  glue_logging_kms_key          = module.app_infra.aws_logging_kms_key
  security_configuration        = module.app_infra.aws_glue_security_configuration.name
  timeout                       = 2880
  connections                   = ["glue-fdm-12790-connection-glue_network_connection"]
  job_parameters                = {
  "--additional-python-modules" : join(",",
    [for i in module.app_infra.whl_path : "s3://${module.app_infra.aws_s3_bucket_code.bucket}/${i}"],
    ["s3://${module.app_infra.aws_s3_bucket_code.bucket}/${module.app_infra.common_lib_path}"]
  ),
  "--extra-jars": join(",", [ for i in module.app_infra.jar_path: "s3://${module.app_infra.aws_s3_bucket_code.bucket}/${i}"]),
  "--sap_secrets_arn" : local.sap_secrets_arn,
  "--snowflake_passwords_arn" : local.snowflake_passwords_arn,
  "--sf_warehouse" = var.sf_warehouse,
  "--sf_db" = var.sf_db,
  "--sap_url" : var.sap_url,
  "glue_version": "3.0",
	"number_of_workers":2,
  "--proof_point_arn" : local.proof_point_arn,
  "--env": var.env,
  "--coupa_database" = var.coupa_database,
  "--coupa_schema"   = var.coupa_schema,
  "--coupa_table"    = var.coupa_table,
  "--load_mode"      = "INCREMENTAL",
  "--email_list" : "FIN-RAA_&_EWs@mckinsey.com"
  }
}