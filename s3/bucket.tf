resource "aws_s3_bucket" "jaf-pipeline-dado" {
  bucket = "-jaf-teste-infra"
}

resource "aws_s3_bucket_acl" "jaf-pipeline-dado-acl" {
  bucket = aws_s3_bucket.jaf-pipeline-dado.id
  acl    = "private"
}