resource "aws_s3_object" "raw-data-empresa" { 
  bucket = aws_s3_bucket.jaf-pipeline-dado.id
  key    = "raw-datas/empresas/"
}

resource "aws_s3_object" "raw-data-socios" { 
  bucket = aws_s3_bucket.jaf-pipeline-dado.id
  key    = "raw-datas/socios/"
}

resource "aws_s3_object" "processed-data" {
  bucket = aws_s3_bucket.jaf-pipeline-dado.id
  key    = "processed-data/"
}

resource "aws_s3_object" "scripts" {
  count = length(var.scripts_glue)  
  bucket = aws_s3_bucket.jaf-pipeline-dado.id
  key    = "scripts/${var.scripts_glue[count.index].script_name}"
  source = "s3/scripts/${var.scripts_glue[count.index].script_name}"
}

resource "aws_s3_object" "deeque_jar" {
  bucket = aws_s3_bucket.jaf-pipeline-dado.id
  key = "libs/deequ-1.2.2-spark-2.4.jar"
  source =  "s3/libs/deequ-1.2.2-spark-2.4.jar"
}

resource "aws_s3_object" "pydeequ_zip" {
  bucket = aws_s3_bucket.jaf-pipeline-dado.id
  key = "libs/pydeequ.zip"
  source =  "s3/libs/pydeequ.zip"
}