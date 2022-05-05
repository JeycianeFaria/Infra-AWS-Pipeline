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
                        .addAnalyzer(Completeness('_c0'))\
                        .addAnalyzer(Completeness('_c1'))\
                        .addAnalyzer(Completeness('_c2'))\
                        .addAnalyzer(Completeness('_c3'))\
                        .addAnalyzer(Completeness('_c4'))\
                        .addAnalyzer(Completeness('_c5'))\
                        .addAnalyzer(Completeness('_c6'))\
                        .addAnalyzer(Maximum('_c4'))\
                        .addAnalyzer(Minimum('_c4'))\
                        .addAnalyzer(Uniqueness(['_c0']))\
                        .useRepository(repository)\
                        .saveOrAppendResult(result_key)\
                        .run()

    result_analysis_df = AnalyzerContext.successMetricsAsDataFrame(spark, result_analysis)
    result_analysis_df.show(truncate=False)

def iniciar_analyzers_socios(dataframe,repository,result_key):
    result_analysis = AnalysisRunner(spark).onData(dataframe)\
                        .addAnalyzer(Size())\
                        .addAnalyzer(Completeness('_c0'))\
                        .addAnalyzer(Completeness('_c1'))\
                        .addAnalyzer(Completeness('_c2'))\
                        .addAnalyzer(Completeness('_c3'))\
                        .addAnalyzer(Completeness('_c4'))\
                        .addAnalyzer(Completeness('_c5'))\
                        .addAnalyzer(Completeness('_c6'))\
                        .addAnalyzer(Completeness('_c7'))\
                        .addAnalyzer(Completeness('_c8'))\
                        .addAnalyzer(Completeness('_c9'))\
                        .addAnalyzer(Completeness('_c10'))\
                        .addAnalyzer(MaxLength('_c3'))\
                        .addAnalyzer(Uniqueness(['_c0']))\
                        .addAnalyzer(Uniqueness(['_c3']))\
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
        