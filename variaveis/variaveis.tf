variable "scripts_glue"{
    type = list(scripts({
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