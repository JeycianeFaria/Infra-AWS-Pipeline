resource "aws_glue_crawler" "jaf-crawler-pipeline" {
  database_name = aws_glue_catalog_database.glue_database_raw_data.name
  name          = "jaf-crawler-pipeline"
  role          = "${var.rolearn}"

  s3_target {
    path = "s3://${var.bucket_name}/raw-data/"
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }
  
}