
#Variavies de configuração

bucket_name = "jaf-pipeline-dado"
rolearn = "arn:aws:iam::111776639170:role/glue-validador-geral"
glueconnections = ["S3", "validador-conexao"]



#variavel scripts do glue

scripts_glue = [ 
    {
        scripts_name = "transformacao/transformacao_dados.py"
    },
    {
        scripts_name = "data-quality/dataquality_analyzers_raw.py"
    },
    {
        scripts_name = "data-quality/dataquality_analyzers_processed.py"
    }
]


#Variaveis de payloads

payload_transformacao_dados = "{\"BUCKET_NAME\":\"jaf-pipeline-dado\",\"GLUE_DB\":\"jaf-raw-data\",\"GLUE_TB_EMP\":\"empresas\",\"GLUE_TB_SOC\":\"socios\",\"COLUMNS\": {\"EMPRESAS\": [\"cnpj_basico\", \"razao_social_nome_empresarial\", \"natureza_juridica\", \"qualificacao_do_responsavel\", \"capital_social_da_empresa\", \"porte_da_empresa\", \"ente_federativo_responsavel\"],\"SOCIOS\": [\"cnpj_basico\", \"identificador_de_socio\", \"nome_do_socio_ou_razao_social\", \"cnpj_ou_cpf_do_socio\", \"qualificacao_do_socio\", \"data_de_entrada_sociedade\", \"pais\", \"representante_legal\", \"nome_do_representante\", \"qualificacao_do_representante_legal\", \"faixa_etaria\"]}}"

payload_dataquality_analyzers_raw = "{\"METRICS_REPOSITORY\":\"jaf-pipeline-dado/metricas-data-quality/\",\"GLUE_DB\":{\"folder\":\"dados-brutos\",\"db\":\"jaf-raw-data\"},\"TABLES\":[{\"tb_name\":\"empresas\"},{\"tb_name\":\"socios\"}]}"

payload-dataquality_processed = "{\"METRICS_REPOSITORY\":\"jaf-pipeline-dado/metricas-data-quality/\",\"GLUE_DB\":{\"folder\":\"dados-processados\",\"db\":\"jaf-processed-data\"},\"TABLES\":[{\"tb_name\":\"empresas\"},{\"tb_name\":\"socios\"}]}"

