import sys
import json 
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pydeequ.analyzers import *
from pydeequ.verification import *
from pydeequ.suggestions import *
from pydeequ.checks import *
from pydeequ.repository import FileSystemMetricsRepository, ResultKey
from datetime import datetime

#carregar variaveis
args = getResolvedOptions(sys.argv, ['PAYLOAD'])
PAYLOAD = args['PAYLOAD']
metadata = json.loads(PAYLOAD)
METRICS_REPOSITORY =  metadata['METRICS_REPOSITORY']
DATABASE_NAME =  metadata['GLUE_DB']
TABLES = metadata['TABLES']

#iniciar contexto do glue e spark session 
glue_context = GlueContext(SparkContext.getOrCreate())
spark = glue_context.spark_session


def configurar_repository(folder, tb_name):
    data = datetime.now()
    path = 's3://' + METRICS_REPOSITORY + folder + '/' + tb_name + '/' + data.strftime('%Y') + '/' + data.strftime('%m') + '/' + data.strftime('%d')  + '/metrics-' + data.strftime('%H%M%S') + '.json'
    repository = FileSystemMetricsRepository(spark,path)
    return repository


def configurar_result_key(database_name,table_name):
    key_tags = {'glue_database': '{}'.format(database_name), 'table':'{}'.format(table_name), 'metrics_repository':'{}'.format(METRICS_REPOSITORY)}
    result_key = ResultKey(spark_session=spark, dataSetDate=int(datetime.now().strftime('%d%m%Y')), tags=key_tags)
    return result_key


def carregar_dataframe(database_name, table_name):
    dynamic_frame = glue_context.create_dynamic_frame.from_catalog(database = database_name, table_name = table_name)
    dataframe = dynamic_frame.toDF()
    dataframe.printSchema()
    return dataframe


def iniciar_analyzers_empresas(dataframe,repository,result_key):
    result_analysis = AnalysisRunner(spark).onData(dataframe)\
                        .addAnalyzer(Size())\
                        .addAnalyzer(Completeness('cnpj_basico'))\
                        .addAnalyzer(Completeness('razao_social_nome_empresarial'))\
                        .addAnalyzer(Completeness('natureza_juridica'))\
                        .addAnalyzer(Completeness('qualificacao_do_responsavel'))\
                        .addAnalyzer(Completeness('capital_social_da_empresa'))\
                        .addAnalyzer(Completeness('porte_da_empresa'))\
                        .addAnalyzer(Completeness('ente_federativo_responsavel'))\
                        .addAnalyzer(Compliance('porte_da_empresa contained in MICRO EMPRESA,EMPRESA DE PEQUENO PORTE,DEMAIS,NAO INFORMADO',
                         '`porte_da_empresa` IS NULL OR `porte_da_empresa` IN ("MICRO EMPRESA","EMPRESA DE PEQUENO PORTE","DEMAIS","NAO INFORMADO")'))\
                        .addAnalyzer(Maximum('capital_social_da_empresa'))\
                        .addAnalyzer(Uniqueness(['cnpj_basico']))\
                        .useRepository(repository)\
                        .saveOrAppendResult(result_key)\
                        .run()

    result_analysis_df = AnalyzerContext.successMetricsAsDataFrame(spark, result_analysis)
    result_analysis_df.show(truncate=False)

def iniciar_analyzers_socios(dataframe,repository,result_key):
    result_analysis = AnalysisRunner(spark).onData(dataframe)\
                        .addAnalyzer(Size())\
                        .addAnalyzer(Completeness('cnpj_basico'))\
                        .addAnalyzer(Completeness('identificador_de_socio'))\
                        .addAnalyzer(Completeness('nome_do_socio_ou_razao_social'))\
                        .addAnalyzer(Completeness('cnpj_ou_cpf_do_socio'))\
                        .addAnalyzer(Completeness('qualificacao_do_socio'))\
                        .addAnalyzer(Completeness('data_de_entrada_sociedade'))\
                        .addAnalyzer(Completeness('pais'))\
                        .addAnalyzer(Completeness('representante_legal'))\
                        .addAnalyzer(Completeness('nome_do_representante'))\
                        .addAnalyzer(Completeness('qualificacao_do_representante_legal'))\
                        .addAnalyzer(Completeness('faixa_etaria'))\
                        .addAnalyzer(Completeness('intervalo_anos_socios'))\
                        .addAnalyzer(Compliance('intervalo_anos_socios contained in NAO SE APLICA,0-12 ANOS,13-20 ANOS,21-30 ANOS,31-40 ANOS,41-50 ANOS,51-60 ANOS,61-70 ANOS,71-80 ANOS,+80 ANOS',
                         '`intervalo_anos_socios` IS NULL OR `intervalo_anos_socios` IN ("NAO SE APLICA","0-12 ANOS","13-20 ANOS","21-30 ANOS","31-40 ANOS","41-50 ANOS","51-60 ANOS","61-70 ANOS","71-80 ANOS","+80 ANOS")'))\
                        .addAnalyzer(Compliance('identificador_de_socio contained in PJ,PF,ESTRANGEIRO',
                         '`identificador_de_socio` IS NULL OR `identificador_de_socio` IN ("PJ","PF","ESTRANGEIRO")'))\
                        .addAnalyzer(MaxLength('cnpj_ou_cpf_do_socio'))\
                        .addAnalyzer(Uniqueness(['cnpj_basico','cnpj_ou_cpf_do_socio']))\
                        .useRepository(repository)\
                        .saveOrAppendResult(result_key)\
                        .run()

    result_analysis_df = AnalyzerContext.successMetricsAsDataFrame(spark, result_analysis)
    result_analysis_df.show(truncate=False)


def main():
    name_folder = DATABASE_NAME['folder']
    db_glue = DATABASE_NAME['db']
    for tb in TABLES:
        tb_name = tb['tb_name']
        repository = configurar_repository(name_folder,tb_name)
        result_key = configurar_result_key(db_glue,tb_name)
        dataframe = carregar_dataframe(db_glue, tb_name)
        if(tb_name == 'empresas'):
            iniciar_analyzers_empresas(dataframe,repository,result_key)
        else:
             iniciar_analyzers_socios(dataframe,repository,result_key)

main()
        