resource "aws_glue_job" "job1-jaf-pipeline" {
  name     = "job1-jaf-pipeline"
  role_arn = "${var.rolearn}"
  connections = "${var.glueconnections}"

  command {
    script_location = "s3://${var.bucket_name}/scripts/data-quality/dataquality_analyzers_raw.py"
    python_version = 3
  }

  max_retries = 0
  timeout = 60
  glue_version =  "3.0"
  worker_type =  "G.1X"
  number_of_workers = 10

  default_arguments = {
    "--enable-auto-scaling" = true
    "--PAYLOAD"="${var.payload_dataquality_analyzers_raw}"
    "--extra-jars"="s3://${var.bucket_name}/libs/deequ-1.2.2-spark-2.4.jar"
    "--extra-py-files"="s3://${var.bucket_name}/libs/pydeequ.zip"
  }

}


resource "aws_glue_job" "job2-jaf-pipeline" {
  name     = "job2-jaf-pipeline"
  role_arn = "${var.rolearn}"
  connections = "${var.glueconnections}"

  command {
    script_location = "s3://${var.bucket_name}/scripts/transformacao/transformacao_dados.py"
    python_version = 3
  }

  max_retries = 0
  timeout = 60
  glue_version =  "3.0"
  worker_type =  "G.1X"
  number_of_workers = 10

  default_arguments = {
    "--enable-auto-scaling" = true
    "--PAYLOAD"="${var.payload_transformacao_dados}"
  }

}


resource "aws_glue_job" "job3-jaf-pipeline" {
  name     = "job3-jaf-pipeline"
  role_arn = "${var.rolearn}"
  connections = "${var.glueconnections}"

  command {
    script_location = "s3://${var.bucket_name}/scripts/data-quality/dataquality_analyzers_processed.py"
    python_version = 3
  }

  max_retries = 0
  timeout = 60
  glue_version =  "3.0"
  worker_type =  "G.1X"
  number_of_workers = 10

  default_arguments = {
    "--enable-auto-scaling" = true
    "--PAYLOAD"="${var.payload-dataquality_processed}"
    "--extra-jars"="s3://${var.bucket_name}/libs/deequ-1.2.2-spark-2.4.jar"
    "--extra-py-files"="s3://${var.bucket_name}/libs/pydeequ.zip"
  }

}
