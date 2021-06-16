import os
import io
import re   
import base64
import ast
from sys import argv
import argparse
import logging
from datetime import datetime, timezone
import datetime

from zipfile import ZipFile, ZipInfo, is_zipfile
import dask.dataframe as dd
import fastparquet
import pyarrow
import pyarrow.parquet as pq
import gcsfs

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.dataframe.io import read_csv

from google.cloud import storage

from join_utils import Join
from utils import zip_extract, find_2nd, is_number_regex, printfn, get_schema_patience_stage, get_schema_exam_stage, get_schema_patience_raw, get_schema_exam_raw, get_schema_curated, set_global_vars
from beam_utils import ParseStageExams, FiltersHotPatiences, FiltersRejectedPatiences, ParseRawPaciente, ParseRawExams, FiltersHotExams, FiltersRejectedExams, add_entity, del_unwanted_cols_patients, del_unwanted_cols_exams
from gcs_utils import upload_csv, download_csv_string

# TODO LIST
# 1 - Check/Download file as UTF-8
# 2 - Compress file with Sanppy compress
# 3 - Save files partitioned by CD_PAIS, CD_UF , although the google storage and google bigquery APIs using external table, you can create automatic partitioning by the requested fields

#Arguments/Apache Beam Variables
PROJECT_ID = 'sas-engenharia-analytics'
REGION='us-central1'
RUNNER = 'DataflowRunner'#'Spark'
JOB_NAME='IngestionCovidFile'
STAGING_LOCATION='gs://dataflow_samples_staging/'
TEMP_LOCATION='gs://dataflow_samples_temp/'

#Folder and Files Variables
INPUT_FOLDER = "input"
PROCESSING_FOLDERNAME = 'processing'
STAGING_FOLDERNAME = 'stage'
RAW_FOLDERNAME ='raw'
CURATED_HOT_FOLDERNAME ='curated/hot'
CURATED_REJECTED_FOLDERNAME ='curated/rejected'
SERVICE_FOLDERNAME ='service'
PROCESSED_FOLDERNAME ='processed'
ZIP_EXTENSION='.zip'

#Auxiliar Variables
object_list = ['Pacientes','Exames']
months = ['janeiro','fevereiro','marco','abril','maio','junho','julho','agosto','setembro','outubro','novembro','dezembro']
pcoll_join_keys = ['id_entidade', 'id_paciente']

def run():
    try:
        #==============================================================
        #Atividade 1: Ingestão
        #==============================================================

        #Download GCS ZIP File
        print("Download GCS ZIP File - started...")

        zipbytes = download_csv_stringIO(PROJECT_ID, None,bucket,zipfilename)
        
        if zipbytes = None:
            raise Exception('Zip file not found')
  
        input_filename_patients, input_filename_exams = get_zip_files_name(zipbytes, object_list)
        
        if input_filename_patients == None or input_filename_patients == '':
            raise Exception('Patiences file not found')
        
        if input_filename_exams == None or input_filename_exams == '':
            raise Exception('Exams file not found')

        # Setting Local Variables
        processing_date = datetime.datetime.now()
        file_timestamp = processing_date.strftime('%Y_%m_%d_%H_%M_%S_%f')
        entity = input_filename_patients[0:input_filename_patients.find('_')]
        patience_file_prefix= input_filename_patients[input_filename_patients.find('_')+1:input_filename_patients.find('.')]
        patience_object = input_filename_patients[input_filename_patients.find('_')+1:find_2nd(input_filename_patients,'_')].lower()
        exam_file_prefix= input_filename_exams[input_filename_exams.find('_')+1:input_filename_exams.find('.')]
        exam_object = input_filename_exams[input_filename_exams.find('_')+1:find_2nd(input_filename_exams,'_')].lower()
    
        print("Entidade:{}".format(entity))
        print("Data do processamento =", processing_date)            
        print("Arquivo de Pacientes:{}".format(input_filename_patients))
        print("Arquivo de Exames:{}".format(input_filename_exams))

        print("Uncompressing files in processing folder - started...")
        zip_extract(bucket, PROCESSING_FOLDERNAME, name[1:])

        # Parsing External and Internal Arguments
        parser = argparse.ArgumentParser()

        # Here we add some specific command line arguments we expect.
        parser.add_argument('--project', dest='project', required=False, help='GCP Project', default=PROJECT_ID)
        parser.add_argument('--runner', dest='runner', required=False, help='Runner.',default=RUNNER)
        parser.add_argument('--staging_location', dest='staging_location', required=False, help='Input file to read. This can be a local file or a file in a Google Storage Bucket.', default=STAGING_LOCATION)
        parser.add_argument('--temp_location', dest='temp_location', required=False, help='Patients File to Export.',default=TEMP_LOCATION)
        parser.add_argument('--region', dest='region', required=False, help='Input file to read. This can be a local file or a file in a Google Storage Bucket.', default=REGION)
        parser.add_argument('--job_name', dest='job_name', required=False, help='Patients File to Export.',default=JOB_NAME)

        known_args, pipeline_args  = parser.parse_known_args(argv)

        # PROCESSING PATIENCE STAGE FILE
        print("Processing Patience Stage file - started...")
        with beam.Pipeline(options=PipelineOptions(pipeline_args) ) as p:
            patience_stage_input = p | 'Read Patients CSV file' >> beam.io.ReadFromText('gs://{}/{}/{}'.format(bucket,PROCESSING_FOLDERNAME,input_filename_patients),skip_header_lines=1)
            patience_stage_split = patience_stage_input | 'Split Patients' >> beam.Map(lambda x: x.split('|'))
            patience_stage_parse = patience_stage_split | 'FormatToDictPatients' >> beam.Map(lambda x: {"id_paciente": x[0], "ic_sexo": x[1], "aa_nascimento": x[2], "cd_uf": x[3], "cd_municipio": x[4], "cd_cepreduzido": x[5], "cd_pais": x[6], "data_carga": processing_date})     
            patience_stage_file = patience_stage_parse | 'Output: Export to Parquet' >> beam.io.parquetio.WriteToParquet(
                        file_path_prefix='gs://{}/{}/{}/{}/{}'.format(bucket,RAW_FOLDERNAME,patience_object,entity,patience_file_prefix),
                        schema=get_schema_patience_raw(),
                        record_batch_size=10000,
                        file_name_suffix='.parquet')

        # PROCESSING EXAMS STAGE FILE
        print("Processing Exams Stage file - started...")
        with beam.Pipeline(options=PipelineOptions(pipeline_args) ) as p:
            exam_stage_input = p | 'Read Exams CSV file' >> beam.io.ReadFromText('gs://{}/{}/{}'.format(bucket,PROCESSING_FOLDERNAME,input_filename_exams),skip_header_lines=1)
            exam_stage_split = exam_stage_input | 'SplitData' >> beam.Map(lambda x: x.split('|'))
            exam_stage_parse = exam_stage_split | 'FormatToDict' >> beam.Map(lambda x: {"id_paciente": x[0], "dt_coleta": x[1], "de_origem": x[2], "de_exame": x[3], "de_analito": x[4], "de_resultado": x[5], "cd_unidade": x[6], "de_valor_referencia": x[7], "data_carga": processing_date})       
            exam_stage_convert = exam_stage_parse | 'Parse raw Patients' >> beam.ParDo(ParseStageExams())
            exam_stage_file = exam_stage_convert | 'Output: Export to Parquet' >> beam.io.parquetio.WriteToParquet(
                        file_path_prefix='gs://{}/{}/{}/{}/{}'.format(bucket,RAW_FOLDERNAME,exam_object,entity,exam_file_prefix),
                        schema=get_schema_patience_stage(),
                        record_batch_size=200000,
                        file_name_suffix='.parquet')

        # TODO
        # Move CSV processing files to CSV to 
        # Move stage file with suffix YYY_MM_DD_HH_MI_SSS
        
        copy_blob(bucket, '{}/{}'.format(bucket,PROCESSING_FOLDERNAME,input_filename_patients), bucket, "{}/{}_{}{}".format(STAGING_FOLDERNAME, patience_file_prefix file_timestamp, ZIP_EXTENSION))
        copy_blob(bucket, '{}/{}'.format(bucket,PROCESSING_FOLDERNAME,input_filename_exams), bucket, "{}/{}_{}{}".format(STAGING_FOLDERNAME, exam_file_prefix, file_timestamp, ZIP_EXTENSION))
        copy_blob(bucket, zipfilename, bucket, "{}/{}_{}{}".format(PROCESSED_FOLDERNAME,filename , file_timestamp, ZIP_EXTENSION))
        
        delete_blob(bucket, '{}/{}'.format(bucket,PROCESSING_FOLDERNAME,input_filename_patients))
        delete_blob(bucket, '{}/{}'.format(bucket,PROCESSING_FOLDERNAME,input_filename_exams))
        delete_blob(bucket, zipfilename)
        #==============================================================
        #Atividade 2: Normalização e aplicação de regras de qualidade
        #==============================================================

        # PROCESSING PATIENCE RAW FILE
        print("Processing Patience Raw file - started...")
        with beam.Pipeline(options=PipelineOptions(pipeline_args) ) as p:
          patience_raw_input = p | 'Read Raw Parquet Patients' >> beam.io.parquetio.ReadFromParquet('{}*'.format('gs://{}/{}/{}/{}/{}'.format(bucket,RAW_FOLDERNAME,patience_object,entity,patience_file_prefix)))
          patience_raw_parse = patience_raw_input | 'Parse raw Patients' >> beam.ParDo(ParseRawPaciente())
          patience_raw_hot = patience_raw_parse | 'Hot Raw Exams' >> beam.ParDo(FiltersHotPatiences())
          patience_raw_rejected = patience_raw_parse | 'Rejecteds Raw Patients' >> beam.ParDo(FiltersRejectedPatiences())
          patience_raw_hot_file = patience_raw_hot | 'Export Hot Exams Parquet' >> beam.io.parquetio.WriteToParquet(
                  file_path_prefix='gs://{}/{}/{}/{}_{}'.format(bucket,CURATED_HOT_FOLDERNAME,patience_object,entity,patience_file_prefix),
                  schema=get_schema_exam_stage(),
                  record_batch_size=10000,
                  file_name_suffix='.parquet')
          patience_raw_rejected_file = patience_raw_rejected | 'Export rejected Exams Parquet' >> beam.io.parquetio.WriteToParquet(
                file_path_prefix='gs://{}/{}/{}/{}_{}'.format(bucket,CURATED_REJECTED_FOLDERNAME,patience_object,entity,patience_file_prefix),
                schema=parquet_schema,
                record_batch_size=100000,
                file_name_suffix='.parquet')

        # PROCESSING EXAMS RAW FILE
        print("Processing Exams Raw file - started...")
        with beam.Pipeline(options=PipelineOptions(pipeline_args) ) as p:
          exam_raw_input = p | 'Read Raw Parquet Exams' >> beam.io.parquetio.ReadFromParquet('{}*'.format('gs://{}/{}/{}/{}/{}'.format(bucket,RAW_FOLDERNAME,exam_object,entity,exam_file_prefix)))
          exam_raw_parse = exam_raw_input | 'Parse raw Patients' >> beam.ParDo(ParseRawExams())
          exam_raw_hot = exam_raw_parse | 'Hot Raw Exams' >> beam.ParDo(FiltersHotExams())
          exam_raw_rejected = exam_raw_parse | 'Rejecteds Raw Exams' >> beam.ParDo(FiltersRejectedExams())
          exam_raw_hot_file = exam_raw_hot | 'Export Hot Exams Parquet' >> beam.io.parquetio.WriteToParquet(
                  file_path_prefix='gs://{}/{}/{}/{}_{}'.format(bucket,CURATED_HOT_FOLDERNAME,exam_object,entity,exam_file_prefix),
                  schema=get_schema_exam_raw(),
                  record_batch_size=10000,
                  file_name_suffix='.parquet')
          exam_raw_rejected_file = exam_raw_rejected | 'Export rejected Exams Parquet' >> beam.io.parquetio.WriteToParquet(
                file_path_prefix='gs://{}/{}/{}/{}_{}'.format(bucket,CURATED_REJECTED_FOLDERNAME,exam_object,entity,exam_file_prefix),
                schema=get_schema_exam_raw(),
                record_batch_size=100000,
                file_name_suffix='.parquet')

        #==============================================================
        #Atividade 3: Transformação e processamento
        #==============================================================
        print("Processing Hot and Rejected Curate files - started...")
        left_pcol_name = 'final_patients'
        right_pcol_name = 'final_exams'
        with beam.Pipeline(options=PipelineOptions(pipeline_args) ) as p:
          patience_curate_hot_input = p | 'Read All Patients' >> beam.io.parquetio.ReadFromParquet('gs://{}/{}/{}/*'.format(bucket,CURATED_HOT_FOLDERNAME,patience_object))
          exam_curate_hot_input = p | 'Read All Exams' >> beam.io.parquetio.ReadFromParquet('gs://{}/{}/{}/*'.format(bucket,CURATED_HOT_FOLDERNAME,exam_object))  
          patience_curate_hot_parsed = patience_curate_hot_input | 'DeleteUnwantedData Patients' >> beam.Map(del_unwanted_cols_patients)
          exam_curate_hot_parsed = exam_curate_hot_input | 'DeleteUnwantedData exames' >> beam.Map(del_unwanted_cols_exams)
          left_pcol_name = 'final_patients'
          final_patients = patience_curate_hot_parsed | 'Add Source in final Patients' >> beam.Map(
                  add_entity,
                  entity=entity,
              )
          right_pcol_name = 'final_exams'
          final_exams = exam_curate_hot_parsed | 'Add Source in final Exams' >> beam.Map(
                  add_entity,
                  entity=entity,
              )  
          join_keys = {left_pcol_name: pcoll_join_keys, right_pcol_name: pcoll_join_keys}
          # Input PCollection for the Join transform
          pipelines_dictionary = {left_pcol_name: final_patients, right_pcol_name: final_exams}

          # Perform a left join
          unified_patiences_exams = pipelines_dictionary | 'left join' >> Join(left_pcol_name=left_pcol_name, left_pcol=final_patients,
                                                                      right_pcol_name=right_pcol_name, right_pcol=final_exams,
                                                                      join_type='left', join_keys=join_keys)
          exams_per_patient_sp = unified_patiences_exams | 'Filter SP' >> beam.Filter(lambda element: element['cd_uf'] == 'SP')
          exams_per_patient_not_sp = unified_patiences_exams | 'Filter Not SP' >> beam.Filter(lambda element: element['cd_uf'] != 'SP')
          #imprime = exams_per_patient_sp | "print" >> beam.Map(printfn)
          exams_per_patient_sp_file = exams_per_patient_sp | 'Export File Patients SP' >> beam.io.parquetio.WriteToParquet(
                  file_path_prefix='gs://{}/{}/exames_por_paciente_sp'.format(bucket,SERVICE_FOLDERNAME),
                  schema=get_schema_curated(),
                  record_batch_size=100,
                  file_name_suffix='.parquet')
          exams_per_patient_not_sp_file = exams_per_patient_not_sp | 'Export File Patients Not SP' >> beam.io.parquetio.WriteToParquet(
                  file_path_prefix='gs://{}/{}/exames_por_paciente'.format(bucket,SERVICE_FOLDERNAME),
                  schema=get_schema_curated(),
                  record_batch_size=10000,
                  file_name_suffix='.parquet') 

        #==============================================================
        #Atividade 4: Uso dos dados transformados
        #==============================================================
        print("Generating Dashboard - started...")
        
    except Exception as e:
        print('Erro:{}'.format(e))
    finally:
        print('Ingestion Process - Finished...')

def load_covid_data(event, context):
 
    logging.getLogger().setLevel(logging.INFO)
    print('Ingestion Process - Started...')

    global data_extract, bucket, name, filename, zipfilename
    
    #====================================
    #THIS CODE WORKS WITH LOCAL EXECUTION
    event= "{'bucket': 'dataflow-samples-sas', 'contentType': 'application/zip', 'crc32c': 'dQ8jPw==', 'etag': 'CPPSgKqdkvECEAE=', 'generation': '1623505510607219', 'id': 'dataflow-samples-sas/input/EINSTEINAgosto.zip/1623505510607219', 'kind': 'storage#object', 'md5Hash': '6axtPzQR4dlglWX1di98XQ==', 'mediaLink': 'https://www.googleapis.com/download/storage/v1/b/dataflow-samples-sas/o/input/EINSTEINAgosto.zip?generation=1623505510607219&alt=media', 'metageneration': '1', 'name': '/input/EINSTEINAgosto.zip', 'selfLink': 'https://www.googleapis.com/storage/v1/b/dataflow-samples-sas/o/input/EINSTEINAgosto.zip', 'size': '32478485', 'storageClass': 'STANDARD', 'timeCreated': '2021-06-12T13:45:10.699Z', 'timeStorageClassUpdated': '2021-06-12T13:45:10.699Z', 'updated': '2021-06-12T13:45:10.699Z'}"
    event = ast.literal_eval(event)    
    pubsub_message = event
    data_extract = pubsub_message
    #====================================
    
    #------------------------------------
    #THIS CODE WORKS WITH PUBSUB CALL
    #pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    #data_extract = json.loads(pubsub_message)
    #------------------------------------
    
    print(f'Payload: {data_extract}')
    
    if data_extract['name'].lower().startswith("/{}/".format(INPUT_FOLDER)) and ZIP_EXTENSION in data_extract['name'].lower() and any(item in data_extract['name'].lower()  for item in months):     
        bucket, name, filename, zipfilename = get_global_vars(event,INPUT_FOLDER,ZIP_EXTENSION)     

        print('Bucket:{}'.format(data_extract['bucket']))
        print('File Name:{}'.format(data_extract['name']))
        print('name:{}'.format(new_name.upper()))
        print('Time Created:{}'.format(data_extract['timeCreated']))
        run()
    else:
        print('Do not ingestion File:{}'.format(data_extract['selfLink']))
        
    return