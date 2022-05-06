resource "aws_glue_catalog_database" "glue_database_raw_data" {
  name = "jaf-raw-data"
}

resource "aws_glue_catalog_database" "glue_database_processed_data" {
  name = "jaf-processed-data"
}
