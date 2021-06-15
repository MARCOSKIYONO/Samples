# Teste para seleção de Engenheiro de Dados - Sociedade Beneficiente Israelita Brasileira


## Setup

1. Install [`pip` and `virtualenv`][cloud_python_setup] if you do not already have them.

1. Clone this repository:

    ```
    git clone https://github.com/MARCOSKIYONO/Samples.git
    ```

1. Obtain authentication credentials.

    Create local credentials by running the following command and following the
    oauth2 flow (read more about the command [here][auth_command]):

    ```
    gcloud auth application-default login
    ```

    Read more about [Google Cloud Platform Authentication][gcp_auth].

## How to prepare the environment and deploy the programs

1. Change directory to one of the sample folders, e.g. `logging/cloud-client`:

    ```
    cd logging/cloud-client/
    ```

1. Create a virtualenv. Samples are compatible with Python 3.6+.

    ```
    python3 -m venv env
    source env/bin/activate
    ```

1. Install the dependencies needed to run the samples.

    ```
    pip install -r requirements.txt
    ```

1. Create the bucket...

    ```
    gsutil mb -l us-central1 gs://rep_einstein_kiyono/
    ```

1. Create the empty folder...

    ```
	touch emptyfile
	gsutil cp emptyfile gs://rep_einstein_kiyono/input/
    ```

1. Deploy Program that will be triggered by the upload files in gs://rep_einstein_kiyono/input

    ```
	functions deploy load_covid_data --env-vars-file=.env.yaml --runtime=python38 --memory=1GB --region=us-central1 --timeout=540s --max-instances=3 --retry --trigger-resource gs://rep_einstein_kiyono/ --trigger-event google.storage.object.finalize --entry-point=load_covid_data
    ```


## How to run the program

1. Upload zip file to Bucket/folder "rep_einstein_kiyono/input" 

    ```
    gsutil cp <file_name>.csv gs://rep_einstein_kiyono/input
    ```	

## contato kiyono.marcos@gmail.com
