import sys
import json 
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import *


args = getResolvedOptions(sys.argv, ['PAYLOAD'])
PAYLOAD = args['PAYLOAD']
metadata = json.loads(PAYLOAD)
BUCKET_NAME = metadata['BUCKET_NAME']


glue_context = GlueContext(SparkContext.getOrCreate())
spark = glue_context.spark_session


#carregando dataframes 
dynamic_frame = glue_context.create_dynamic_frame.from_catalog(database = metadata['GLUE_DB'], table_name = metadata['GLUE_TB_EMP'])
tb_empresas = dynamic_frame.toDF()
tb_empresas.printSchema()

dynamic_frame = glue_context.create_dynamic_frame.from_catalog(database = metadata['GLUE_DB'], table_name = metadata['GLUE_TB_SOC'])
tb_socios = dynamic_frame.toDF()
tb_socios.printSchema()


def renomear_colunas(dataframe, names_columns):

    for  index, col_name in enumerate(names_columns):
        dataframe = dataframe.withColumnRenamed(f'_c{index}', col_name)
    
    return dataframe


def limpar_dados_empresas(empresas):
    
    emp_clean = empresas\
                .withColumn('capital_social_da_empresa', regexp_replace('capital_social_da_empresa', ',', '.').cast(DoubleType()))\
                .withColumn('porte_da_empresa', when(empresas.porte_da_empresa == 1, 'MICRO EMPRESA')\
                               .when(empresas.porte_da_empresa == 3, 'EMPRESA DE PEQUENO PORTE')\
                               .when(empresas.porte_da_empresa == 5, 'DEMAIS')\
                               .otherwise('NAO INFORMADO'))

    return emp_clean


def limpar_dados_socios(socios):

    soc_clean = socios\
                .withColumn('data_de_entrada_sociedade', to_date(socios.data_de_entrada_sociedade.cast(StringType()), 'yyyyMMdd'))\
                .withColumn('identificador_de_socio', when(socios.identificador_de_socio == 1, 'PJ')\
                           .when(socios.identificador_de_socio == 2, 'PF')\
                           .otherwise('ESTRANGEIRO'))\
                .withColumn('intervalo_anos_socios', when( socios.faixa_etaria == 0, 'NAO SE APLICA')\
                            .when(socios.faixa_etaria == 1, '0-12 ANOS')\
                            .when(socios.faixa_etaria == 2, '13-20 ANOS')\
                            .when(socios.faixa_etaria == 3, '21-30 ANOS')\
                            .when(socios.faixa_etaria == 4, '31-40 ANOS')\
                            .when(socios.faixa_etaria == 5, '41-50 ANOS')\
                            .when(socios.faixa_etaria == 6, '51-60 ANOS')\
                            .when(socios.faixa_etaria == 7, '61-70 ANOS')\
                            .when(socios.faixa_etaria == 8, '71-80 ANOS')\
                            .otherwise('+80 ANOS'))

    return soc_clean


def adicionar_colunas_datas(dataframe):

    new_dataframe = dataframe\
                    .withColumn('timestamp', current_timestamp())\
                    .withColumn('year', date_format(col('timestamp'),'yyyy'))\
                    .withColumn('month', date_format(col('timestamp'),'MM'))\
                    .withColumn('day', date_format(col('timestamp'),'dd'))\
                    .drop('timestamp')

    return new_dataframe


def escrever_arquivo_parquet_s3(dataframes, tb_names):

    for index, df in enumerate(dataframes):

        dataframe = adicionar_colunas_datas(df)

        dynamic_frame = DynamicFrame.fromDF(dataframe=dataframe, glue_ctx=glue_context, name='dynamic_frame')
        dynamic_frame = dynamic_frame.repartition(1)

        sink = glue_context\
            .getSink(
                connection_type='s3', 
                path='s3://' + BUCKET_NAME + '/processed-data/dados-particionados/' + tb_names[index] + '/',
                enableUpdateCatalog=True,
                partitionKeys=['year', 'month', 'day']
            )
        sink.setFormat('glueparquet')
        sink.setCatalogInfo(catalogDatabase='jaf-processed-data', catalogTableName=tb_names[index])
        sink.writeFrame(dynamic_frame)


def main():

    name_columns_emp = metadata['COLUMNS']['EMPRESAS']
    name_columns_soc = metadata['COLUMNS']['SOCIOS']
    

    empresas = renomear_colunas(tb_empresas,name_columns_emp)
    socios = renomear_colunas(tb_socios, name_columns_soc)
    emp_clean = limpar_dados_empresas(empresas)
    soc_clean = limpar_dados_socios(socios)

    emp_clean.printSchema()
    soc_clean.printSchema()


    dataframes = [emp_clean,soc_clean]
    tb_names = ['empresas','socios']

    escrever_arquivo_parquet_s3(dataframes,tb_names)


main()
