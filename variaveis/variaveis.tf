variable "bucket_name" {
    type = string
}

variable "rolearn" {
   type = string
}

variable "glueconnections" {
    type = list(string)
}

variable "scripts_glue"{
    type = list(object({
        script_name = string
    }))
}

variable "payload_dataquality_analyzers_raw" {
  type = string
}

variable "payload_transformacao_dados" {
  type = string
}

variable "payload-dataquality_processed" {
  type = string
}
