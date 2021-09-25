# ### description:
# 

#_______________________________________________________________________________________________
# Imports 
from datetime import datetime
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
from google.cloud import storage
from google.oauth2 import service_account
from google.cloud import bigquery

#_______________________________________________________________________________________________
# Variáveis globais
job_name = 'trm_cnpj_receita_federal'

project_id = 'gcp_project_id'
bucket_name = 'bucket'
bucket_tmp = 'treated'
destination_blob_name = 'rf_cnpj/input/'
gcs_key_file = '/opt/process_cnpj/gcp-project-id.json'

storage_client = storage.Client.from_service_account_json(gcs_key_file)
credentials = service_account.Credentials.from_service_account_file(gcs_key_file, scopes=["https://www.googleapis.com/auth/cloud-platform"])
bq_client = bigquery.Client(credentials=credentials, project=credentials.project_id)

#_______________________________________________________________________________________________
# Definição dos schemas
schema_simples = StructType() \
    .add("cnpj_basico",StringType(),True) \
    .add("idc_op_simples",StringType(),True) \
    .add("dt_op_simples",IntegerType(),True) \
    .add("dt_exclusao_simples",IntegerType(),True) \
    .add("idc_op_mei",StringType(),True) \
    .add("dt_op_mei",IntegerType(),True) \
    .add("dt_exclusao_mei",IntegerType(),True) 

schema_cnae = StructType() \
    .add("cod_cnae",IntegerType(),True) \
    .add("nom_cnae",StringType(),True) 

# schema_motivo = StructType()

schema_municipio = StructType() \
    .add("cod_municipio",IntegerType(),True) \
    .add("nom_municipio",StringType(),True) 

schema_natureza_juridica  = StructType() \
    .add("cod_nat_juridica",IntegerType(),True) \
    .add("nom_nat_juridica",StringType(),True) 

schema_pais = StructType() \
    .add("cod_pais",IntegerType(),True) \
    .add("nom_pais",StringType(),True) 

schema_qualificacao_socio = StructType() \
    .add("cod_qualif_socio",IntegerType(),True) \
    .add("nom_qualif_socio",StringType(),True) 

schema_socio = StructType() \
    .add("cnpj_basico",StringType(),True) \
    .add("cod_ident_socio",IntegerType(),True) \
    .add("nom_socio",StringType(),True) \
    .add("cnpj_cpf_socio",StringType(),True) \
    .add("cod_qualif_socio",IntegerType(),True) \
    .add("dt_entrada_sociedade",IntegerType(),True) \
    .add("cod_pais",IntegerType(),True) \
    .add("cpf_representante",IntegerType(),True) \
    .add("nom_representante",StringType(),True) \
    .add("cod_qualif_representante",IntegerType(),True) \
    .add("cod_faixa_etaria",IntegerType(),True) \
    .add("razao_social",StringType(),True) \

schema_empresa = StructType() \
    .add("cnpj_basico",StringType(),True) \
    .add("razao_social",StringType(),True) \
    .add("cod_nat_juridica",IntegerType(),True) \
    .add("cod_qualif_socio",IntegerType(),True) \
    .add("capital_social",StringType(),True) \
    .add("cod_porte",IntegerType(),True) \
    .add("ente_federativo",IntegerType(),True) 

schema_estabelecimento = StructType() \
    .add("cnpj_basico",StringType(),True) \
    .add("cnpj_origem",StringType(),True) \
    .add("cnpj_dv",StringType(),True) \
    .add("cod_matriz_filial",IntegerType(),True) \
    .add("nom_fantasia",StringType(),True) \
    .add("cod_situacao_cadastral",IntegerType(),True) \
    .add("dt_situacao_cadastral",IntegerType(),True) \
    .add("cod_motivo_situcao_cadastral",IntegerType(),True) \
    .add("nom_cidade_exterior",StringType(),True) \
    .add("cod_pais",IntegerType(),True) \
    .add("dt_inicio_atividade",IntegerType(),True) \
    .add("cod_cnae_principal",IntegerType(),True) \
    .add("cod_cnae_secundario",IntegerType(),True) \
    .add("tipo_logradouro",StringType(),True)  \
    .add("logradouro",StringType(),True)  \
    .add("numero",IntegerType(),True)  \
    .add("complemento",StringType(),True) \
    .add("bairro",StringType(),True) \
    .add("cep",StringType(),True) \
    .add("uf",StringType(),True) \
    .add("cod_municipio",IntegerType(),True) \
    .add("ddd_1",IntegerType(),True) \
    .add("telefone_1",IntegerType(),True) \
    .add("ddd_2",IntegerType(),True) \
    .add("telefone_2",IntegerType(),True) \
    .add("ddd_fax",IntegerType(),True) \
    .add("fax",IntegerType(),True) \
    .add("email",StringType(),True) \
    .add("sit_especial",StringType(),True) \
    .add("dt_sit_especial",IntegerType(),True)  

#_______________________________________________________________________________________________
#lista arquivos do bucket
def list_blobs(bucket_name, prefix, delimiter=None):
    ls = []
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix, delimiter=delimiter)

    for blob in blobs:
        ls.append(blob.name)

    if delimiter:
        for prefix in blobs.prefixes:
            ls.append(prefix)

    return ls   
#_______________________________________________________________________________________________
def delete_blob(bucket_name, blob_name):
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.delete() 

#_______________________________________________________________________________________________
def remove_parquet(bucket_name, parquet_path):
    ls = list_blobs(bucket_name, parquet_path)
    
    for blob_name in ls:
        delete_blob(bucket_name, blob_name)

#_______________________________________________________________________________________________
def write_parquet_to_bigquery(table_id, parquet_path, df):  
    tmp_table = '{0}.{1}'.format(project_id, table_id)
    df.write.format('bigquery').option('temporaryGcsBucket', bucket_tmp).mode('overwrite').save(tmp_table)

#_______________________________________________________________________________________________
def run():

    start_time = datetime.now()

    # 
    spark = SparkSession.builder.appName(job_name).config(conf=SparkConf().set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED").set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED").set("spark.sql.legacy.parquet.dateRebaseModeInWrite", "CORRECTED")).getOrCreate()
    spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile",gcs_key_file)
    #spark.sparkContext.getConf().getAll()

    print('Inicio carga SIMPLES.CSV')
    #.SIMPLES.CSV
    df_simples = spark.read.option("header", "true").option("delimiter", ";").option("charset", "iso-8859-1") \
                            .schema(schema_simples).csv("gs://bucket/base_cnpj/input/tmp/*SIMPLES.CSV*")#.withColumn("filename", F.input_file_name())
    
    df_simples = df_simples.withColumn("dt_op_simples",         F.when(F.col('dt_op_simples') == 00000000,F.lit(None)).otherwise(F.to_date(F.col("dt_op_simples").cast("string"), 'yyyyMMdd')))
    df_simples = df_simples.withColumn("dt_exclusao_simples",   F.when(F.col('dt_exclusao_simples') == 00000000,F.lit(None)).otherwise(F.to_date(F.col("dt_exclusao_simples").cast("string"), 'yyyyMMdd')))
    df_simples = df_simples.withColumn("dt_op_mei",             F.when(F.col('dt_op_mei') == 00000000,F.lit(None)).otherwise(F.to_date(F.col("dt_op_mei").cast("string"), 'yyyyMMdd')))
    df_simples = df_simples.withColumn("dt_exclusao_mei",       F.when(F.col('dt_exclusao_mei') == 00000000,F.lit(None)).otherwise(F.to_date(F.col("dt_exclusao_mei").cast("string"), 'yyyyMMdd')))

    write_parquet_to_bigquery('receita_cnpj.simples', 'simples/tmp/simples.parquet', df_simples)
    print('Fim carga SIMPLES.CSV')

    print('Inicio carga CNAECSV')
    # #.CNAECSV
    df_cnae = spark.read.option("header", "true").option("delimiter", ";").option("charset", "iso-8859-1") \
                            .schema(schema_cnae).csv("gs://bucket/base_cnpj/input/tmp/*.CNAECSV")#.withColumn("filename", F.input_file_name())

    write_parquet_to_bigquery('receita_cnpj.cnae', 'cnae/tmp/cnae.parquet', df_cnae)

    print('Fim carga CNAECSV')

    # #.MOTICSV
    # # df_qualificacao = spark.read.option("header", "true").option("delimiter", ";").option("charset", "iso-8859-1") \
    # #                         .schema(schema_motivo).csv("gs://bucket/base_cnpj/input/tmp/*.MOTICSV").withColumn("filename", F.input_file_name())
    # #write_parquet_to_bigquery('receita_cnpj.cnae', 'cnae/tmp/cnae.parquet', df_cnae)

    print('Inicio carga MUNICCSV')
    # #.MUNICCSV
    df_municipio = spark.read.option("header", "true").option("delimiter", ";").option("charset", "iso-8859-1") \
                            .schema(schema_municipio).csv("gs://bucket/base_cnpj/input/tmp/*.MUNICCSV")#.withColumn("filename", F.input_file_name())
    write_parquet_to_bigquery('receita_cnpj.municipio', 'municipio/tmp/municipio.parquet', df_municipio)

    print('Fim carga MUNICCSV')

    print('Inicio carga NATJUCSV')
    # #.NATJUCSV
    df_natureza_juridica = spark.read.option("header", "true").option("delimiter", ";").option("charset", "iso-8859-1") \
                            .schema(schema_natureza_juridica).csv("gs://bucket/base_cnpj/input/tmp/*.NATJUCSV")#.withColumn("filename", F.input_file_name())
    write_parquet_to_bigquery('receita_cnpj.natureza_juridica', 'natureza_juridica/tmp/natureza_juridica.parquet', df_natureza_juridica)

    print('Fim carga NATJUCSV')

    print('Inicio carga PAISCSV')
    #.PAISCSV
    df_pais = spark.read.option("header", "true").option("delimiter", ";").option("charset", "iso-8859-1") \
                            .schema(schema_pais).csv("gs://bucket/base_cnpj/input/tmp/*.PAISCSV")#.withColumn("filename", F.input_file_name())

    # df_pais.write.format('bigquery').mode('overwrite').option('temporaryGcsBucket', 'treated').save('{0}.receita_cnpj.pais'.format(project_id)) 
    write_parquet_to_bigquery('receita_cnpj.pais', 'pais/tmp/pais.parquet', df_pais)

    print('Fim carga PAISCSV')

    print('Inicio carga QUALSCSV')
    # #.QUALSCSV
    df_qualificacao_socio = spark.read.option("header", "true").option("delimiter", ";").option("charset", "iso-8859-1") \
                            .schema(schema_qualificacao_socio).csv("gs://bucket/base_cnpj/input/tmp/*.QUALSCSV")#.withColumn("filename", F.input_file_name())
    write_parquet_to_bigquery('receita_cnpj.qualificacao_socio', 'qualificacao_socio/tmp/qualificacao_socio.parquet', df_qualificacao_socio)

    print('Fim carga QUALSCSV')

    print('Inicio carga QUALSCSV')
    # #.SOCIOCSV
    df_socio = spark.read.option("header", "true").option("delimiter", ";").option("charset", "iso-8859-1") \
                        .schema(schema_socio).csv("gs://bucket/base_cnpj/input/tmp/*.SOCIOCSV")#.withColumn("filename", F.input_file_name())   
    df_socio = df_socio.withColumn("tipo_socio", 
                                        F.when( F.col("cod_ident_socio") == 1, F.lit('PESSOA JURÍDICA')).otherwise(
                                            F.when(F.col("cod_ident_socio") == 2, F.lit('PESSOA FÍSICA')).otherwise(
                                                F.when(F.col("cod_ident_socio") == 3, F.lit('ESTRANGEIRO'))
                                        )).cast("String"))

    df_socio = df_socio.withColumn("dt_entrada_sociedade",       F.when(F.col('dt_entrada_sociedade') == 00000000,F.lit(None).cast('date')).otherwise(F.to_date(F.col("dt_entrada_sociedade").cast("string"), 'yyyyMMdd')))

    write_parquet_to_bigquery('receita_cnpj.socio', 'socio/tmp/socio.parquet', df_socio.filter(F.col('dt_entrada_sociedade') >= '2010-01-01'))
    
    print('Fim carga QUALSCSV')

    print('Inicio carga EMPRECSV')
    # #.EMPRECSV
    df_empresa = spark.read.option("header", "true").option("delimiter", ";").option("charset", "iso-8859-1") \
                    .schema(schema_empresa).csv("gs://bucket/base_cnpj/input/tmp/*.EMPRECSV")#.withColumn("filename", F.input_file_name())

    df_empresa = df_empresa.withColumn("capital_social",  F.regexp_replace(F.col('capital_social'), ',', '.').cast('Double'))
    df_empresa = df_empresa.withColumn("porte_empresa", 
                                            F.when( F.col("cod_porte") == 1, F.lit('NÃO INFORMADO')).otherwise(
                                                F.when(F.col("cod_porte") == 2, F.lit('MICRO EMPRESA')).otherwise(
                                                    F.when(F.col("cod_porte") == 3, F.lit('EMPRESA DE PEQUENO PORTE')).otherwise(
                                                        F.when(F.col("cod_porte") == 5, F.lit('DEMAIS'))
                                            ))).cast("String"))

    write_parquet_to_bigquery('receita_cnpj.empresa', 'empresa/tmp/empresa.parquet', df_empresa)

    print('Fim carga EMPRECSV')

    print('Inicio carga ESTABELE')
    #.ESTABELE
    df_estabelecimento = spark.read.option("header", "true").option("delimiter", ";").option("escape", "\"").option("charset", "iso-8859-1") \
                            .schema(schema_estabelecimento).csv("gs://bucket/base_cnpj/input/tmp/*.ESTABELE").withColumn("filename", F.input_file_name())

    df_estabelecimento = df_estabelecimento.withColumn("matriz_filial", 
                                                            F.when( F.col("cod_matriz_filial") == 1, F.lit('MATRIZ')).otherwise(
                                                                F.when( F.col("cod_matriz_filial") == 1, F.lit('FILIAL'))
                                                            ).cast("String"))
    df_estabelecimento = df_estabelecimento.withColumn("situacao_cadastral", 
                                                        F.when( F.col("cod_situacao_cadastral") == 1, F.lit('NULA')).otherwise(
                                                            F.when( F.col("cod_situacao_cadastral") == 2, F.lit('ATIVA')).otherwise(
                                                                F.when( F.col("cod_situacao_cadastral") == 3, F.lit('SUSPENSA')).otherwise(
                                                                    F.when( F.col("cod_situacao_cadastral") == 4, F.lit('INAPTA')).otherwise(
                                                                        F.when( F.col("cod_situacao_cadastral") == 8, F.lit('BAIXADA'))
                                                        )))).cast("String"))
    df_estabelecimento = df_estabelecimento.withColumn("dt_situacao_cadastral", F.when(F.col('dt_situacao_cadastral') == 00000000,F.lit(None).cast('date')).otherwise(F.to_date(F.col("dt_situacao_cadastral").cast("string"), 'yyyyMMdd')))
    df_estabelecimento = df_estabelecimento.withColumn("dt_inicio_atividade",   F.when(F.col('dt_inicio_atividade') == 00000000,F.lit(None).cast('date')).otherwise(F.to_date(F.col("dt_inicio_atividade").cast("string"), 'yyyyMMdd')))
    df_estabelecimento = df_estabelecimento.withColumn("dt_sit_especial",       F.when(F.col('dt_sit_especial') == 00000000,F.lit(None).cast('date')).otherwise(F.to_date(F.col("dt_sit_especial").cast("string"), 'yyyyMMdd')))

    write_parquet_to_bigquery('receita_cnpj.estabelecimento', 'estabelecimento/tmp/estabelecimento.parquet', df_estabelecimento)                                                                                                           

    print('Fim carga ESTABELE')
    print('Tempo gasto com o tratamento: {0}'.format((datetime.now() - start_time)))
#________________________________________________________________________________________
if __name__ == '__main__':
    try:
        print('Start Job')
        run()
    except Exception as e:
        msg = 'error on job: "{0}"...'.format(str(e))
        print(msg)
    finally:
        print('Finalize Job')
 