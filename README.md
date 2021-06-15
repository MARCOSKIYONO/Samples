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

## How to run a sample

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

1. Run the sample:

    ```
    python snippets.py
    ```
	
# Create the bucket...
gsutil mb -l us-central1 gs://rep_einstein_kiyono/

# Create the empty folder...
mkdir input 
touch input/file1
gsutil cp -r test gs://rep_einstein_kiyono/

# Upload your file...
gsutil cp <file_name>.csv gs://<bucket_name>/

# Deploy de function 
gcloud functions deploy load_covid_data --env-vars-file=.env.yaml --runtime=python38 --memory=1GB --region=us-central1 --timeout=540s --max-instances=3 --retry --trigger-resource gs://rep_einstein_kiyono/ --trigger-event google.storage.object.finalize --entry-point=load_covid_data

## Contributing

Contributions welcome! See the [Contributing Guide](CONTRIBUTING.md).

[slack_badge]: https://img.shields.io/badge/slack-Google%20Cloud%20Platform-E01563.svg	
[slack_link]: https://googlecloud-community.slack.com/
[cloud]: https://cloud.google.com/
[cloud_python_setup]: https://cloud.google.com/python/setup
[auth_command]: https://cloud.google.com/sdk/gcloud/reference/beta/auth/application-default/login
[gcp_auth]: https://cloud.google.com/docs/authentication#projects_and_resources

[py-2.7-shield]: https://storage.googleapis.com/cloud-devrel-public/python-docs-samples/badges/py-2.7.svg
[py-2.7-link]: https://storage.googleapis.com/cloud-devrel-public/python-docs-samples/badges/py-2.7.html
[py-3.6-shield]: https://storage.googleapis.com/cloud-devrel-public/python-docs-samples/badges/py-3.6.svg
[py-3.6-link]: https://storage.googleapis.com/cloud-devrel-public/python-docs-samples/badges/py-3.6.html
[py-3.7-shield]: https://storage.googleapis.com/cloud-devrel-public/python-docs-samples/badges/py-3.7.svg
[py-3.7-link]: https://storage.googleapis.com/cloud-devrel-public/python-docs-samples/badges/py-3.7.html
[py-3.8-shield]: https://storage.googleapis.com/cloud-devrel-public/python-docs-samples/badges/py-3.8.svg
[py-3.8-link]: https://storage.googleapis.com/cloud-devrel-public/python-docs-samples/badges/py-3.8.html