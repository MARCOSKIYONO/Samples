import os
#from google.colab import auth
#from oauth2client.client import GoogleCredentials
import ast
import io
import re   
import base64
from zipfile import ZipFile, ZipInfo, is_zipfile
from sys import argv
import argparse
from datetime import datetime, timezone
import datetime

import logging
import dask.dataframe as dd
import fastparquet
import pyarrow
import pyarrow.parquet as pq
import gcsfs

class ParseStageExams(beam.DoFn):
	def process(self, element):
	  element['dt_coleta'] = datetime.strptime(dt_coleta,'%d/%m/%Y') if dt_coleta else None
	  return [element]

  
class FiltersHotPatiences(beam.DoFn):
    def process(self, element):
        if element['id_paciente'] != ''  and element['id_paciente'] is not None and element['aa_nascimento'] != ''  and element['aa_nascimento'] != '' and element['cd_municipio'] != ''  and element['cd_municipio'] is not None and element['cd_pais'] != ''  and element['cd_pais'] is not None:
            return [element]


class FiltersRejectedPatiences(beam.DoFn):
    def process(self, element):
        if not (element['id_paciente'] != ''  and element['id_paciente'] is not None and element['aa_nascimento'] != ''  and element['aa_nascimento'] != '' and element['cd_municipio'] != ''  and element['cd_municipio'] is not None and element['cd_pais'] != ''  and element['cd_pais'] is not None):
            return [element]


class ParseRawPaciente(beam.DoFn):
    def process(self, element):
      element['cd_municipio'] = 'HIDDEN' if element['cd_municipio'] == 'MMMM' else element['cd_municipio']
      element['cd_cepreduzido'] = 'HIDDEN' if element['cd_cepreduzido'] == 'CCCC' else element['cd_cepreduzido']
      return [element]
  
  
class ParseRawExams(beam.DoFn):
    def process(self, element):
      element['de_resultado'] = " ".join(element['de_resultado'].lower().split())
      return [element]


class FiltersHotExams(beam.DoFn):
    def process(self, element):
        if element['id_paciente'] != ''  and element['id_paciente'] is not None and element['dt_coleta'] != ''  and element['dt_coleta'] != '' and element['de_origem'] != ''  and element['de_origem'] is not None and element['de_exame'] != ''  and element['de_exame'] is not None and element['de_resultado'] != ''  and element['de_resultado'] is not None:
            return [element]


class FiltersRejectedExams(beam.DoFn):
    def process(self, element):
        if not (element['id_paciente'] != ''  and element['id_paciente'] is not None and element['dt_coleta'] != ''  and element['dt_coleta'] != '' and element['de_origem'] != ''  and element['de_origem'] is not None and element['de_exame'] != ''  and element['de_exame'] is not None) and element['de_resultado'] != ''  and element['de_resultado'] is not None:
            return [element]      
      
      
def add_entity(plant, entity):
  plant['id_entidade'] = entity
  return plant
  
  
def del_unwanted_cols_patients(data):
    """Delete the unwanted columns"""
    data['vl_idade'] = str(datetime.datetime.now().year - int(data['aa_nascimento'])) if is_number_regex(data['aa_nascimento']) else data['aa_nascimento']
    del data['aa_nascimento']
    del data['data_carga']
    return data

def del_unwanted_cols_exams(data):
    """Delete the unwanted columns"""
    del data['data_carga']
    return data  