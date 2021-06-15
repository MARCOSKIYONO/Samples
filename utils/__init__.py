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

def get_global_vars(event,input_folder,zip_extension):
  bucket = event['bucket'] if 'bucket' in event else None 
  name = event['name'] if 'name' in event else None 
  zipfilename = event['name'][1:] if 'name' in event else None 
  filename = event['name'].replace('/{}/'.format(input_folder),'').replace(zip_extension,'') if 'name' in event else None 

  return bucket, name, filename, zipfilename
  
def zip_extract(bucketname, folder, zipfilename_with_path):

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucketname)

    destination_blob_pathname = zipfilename_with_path
    
    blob = bucket.blob(destination_blob_pathname)
    zipbytes = io.BytesIO(blob.download_as_string())

    if is_zipfile(zipbytes):
        with ZipFile(zipbytes, 'r') as myzip:
            for contentfilename in myzip.namelist():
                if not 'Dicionario' in contentfilename:   
                    contentfile = myzip.read(contentfilename)
                    blob = bucket.blob(folder + "/" + contentfilename)
                    blob.upload_from_string(contentfile)


def get_zip_files_name(zipbytes):
    if is_zipfile(zipbytes):

        with ZipFile(zipbytes, 'r') as zipObj:
            # Get list of files names in zip
            listOfiles = zipObj.namelist()

            matching = [s for s in listOfiles if any(xs in s for xs in object_list)]

            for item in matching:
                if object_list[0] in item:
                    input_filename_patients = item

            for item in matching:
                if object_list[1] in item:
                    input_filename_exams = item
                    
            return input_filename_patients, input_filename_exams


def find_2nd(string, substring):
   return string.find(substring, string.find(substring) + 1)
   
def is_number_regex(s):
    """ Returns True is string is a number. """
    if re.match("^\d+?\.\d+?$", s) is None:
        return s.isdigit()
    return True   
    
    
def printfn(elem):
    print(elem)
    
    
def get_schema_patience_stage():
    return pyarrow.schema(
        [('id_paciente', pyarrow.string()),
        ('dt_coleta', pyarrow.date64()),
        ('de_origem', pyarrow.string()),
        ('de_exame', pyarrow.string()),
        ('de_analito', pyarrow.string()),
        ('de_resultado', pyarrow.string()),
        ('cd_unidade', pyarrow.string()),
        ('de_valor_referencia', pyarrow.string()),
        ('data_carga', pyarrow.date64())
          ]
    )


def get_schema_exam_stage():
    return    
    parquet_schema = pyarrow.schema(
        [('id_paciente', pyarrow.string()),
        ('ic_sexo', pyarrow.string()),
        ('aa_nascimento', pyarrow.string()),
        ('cd_pais', pyarrow.string()),
        ('cd_uf', pyarrow.string()),
        ('cd_municipio', pyarrow.string()),
        ('cd_cepreduzido', pyarrow.string()),
        ('data_carga', pyarrow.date64())
          ]
    )
    
    
def get_schema_patience_raw():
    return  
    parquet_schema = pyarrow.schema(
        [('id_paciente', pyarrow.string()),
        ('ic_sexo', pyarrow.string()),
        ('aa_nascimento', pyarrow.string()),
        ('cd_pais', pyarrow.string()),
        ('cd_uf', pyarrow.string()),
        ('cd_municipio', pyarrow.string()),
        ('cd_cepreduzido', pyarrow.string()),
        ('data_carga', pyarrow.date64())
          ]
    )
    
    
def get_schema_exam_raw():
    return  
    parquet_schema  = pyarrow.schema(
        [('id_paciente', pyarrow.string()),
        ('dt_coleta', pyarrow.string()),
        ('de_origem', pyarrow.string()),
        ('de_exame', pyarrow.string()),
        ('de_analito', pyarrow.string()),
        ('de_resultado', pyarrow.string()),
        ('cd_unidade', pyarrow.string()),
        ('de_valor_referencia', pyarrow.string()),
        ('data_carga', pyarrow.date64())
          ]
    )
    
    
def get_schema_curated():
    return  
    parquet_schema  = pyarrow.schema(
        [
        ('id_paciente', pyarrow.string()),
        ('ic_sexo', pyarrow.string()),
        ('cd_pais', pyarrow.string()),
        ('cd_uf', pyarrow.string()),
        ('cd_municipio', pyarrow.string()),
        ('cd_cepreduzido', pyarrow.string()),
        ('vl_idade', pyarrow.string()),    
        ('id_entidade', pyarrow.string()), 
        ('dt_coleta', pyarrow.string()),
        ('de_origem', pyarrow.string()),
        ('de_exame', pyarrow.string()),
        ('de_analito', pyarrow.string()),
        ('de_resultado', pyarrow.string()),
        ('cd_unidade', pyarrow.string()),
        ('de_valor_referencia', pyarrow.string())
          ]
    )