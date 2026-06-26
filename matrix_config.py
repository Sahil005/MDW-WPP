import os


class ComputeConfig:
    """
    Configuration for Compute Engine instance.

    Attributes:
        gce_instance (str): Name of the instance.
        gce_zone (str): Zone where the instance is located.
        setup_command (str): The command to set up the environment by 
            copying scripts from Google Cloud Storage.
        user (str): The username associated with the configuration.
    """
    """"""
    def __init__(self):
        self.instance_name = 'matrix-etl-vm-windows'
        self.zone = 'australia-southeast1-a'
        self.setup_command = 'gsutil -m cp -r gs://au-accel-mdw-dev-scripts/matrix_export ./scripts/'
        self.user = 'matt_burrows'

class StorageConfig:
    """
    Configuration class for storage settings.

    Attributes:
        data_bucket (str): The name of the data bucket, fetched from the
            environment variable 'DATA_BUCKET'.
    """
    def __init__(self):
        self.data_bucket = 'au-accel-mdw-dev-data'

class BigQueryConfig:
    """
    Configuration class for BigQuery settings.

    Attributes:
        project_id (str): The Google Cloud Project ID.
        dataset (str): BigQuery dataset name.
        skip_leading_rows (int): Number of leading rows to skip.
        source_format (str): Format of the source file.
        create_disposition (str): Specifies whether the table should be
            created if it does not exist.
        write_disposition (str): Specifies the action that occurs if
            the destination table already exists.
        autodetect (bool): Specifies whether to enable schema 
            auto-detection for CSV and JSON files.
    """
    def __init__(self):
        self.project_id = "au-accel-mdw-dev"
        self.dataset = 'L01_raw_matrix'
        self.skip_leading_rows = 1
        self.source_format = 'json'
        self.create_disposition = 'CREATE_IF_NEEDED'
        self.write_disposition = 'WRITE_APPEND'
        self.autodetect = False

class MatrixConfig:
    """
    Configuration class for connecting to the Matrix server.
    Attributes:
        server_name (str): The server name for the SSAS instance.
        catalog (str): The catalog name for the database.
        user_id_secret (str): Name of the secret storing the user ID.
        password_secret (str): Name of the secret storing the password.
    """
    def __init__(self):
        self.server_name = 'asazure://australiaeast.asazure.windows.net/aeaansematrixauprd01'
        self.catalog = 'bookingmonitoring'
        self.user_id_secret = 'airflow-variables-matrix_upn'
        self.password_secret = 'airflow-variables-matrix_password'

class Config:
    """
    Configuration class for setting up various components and commands.

    Attributes:
        compute (ComputeConfig): Configuration for compute resources.
        storage (StorageConfig): Configuration for storage resources.
        bigquery (BigQueryConfig): Configuration for BigQuery resources.
        matrix (MatrixConfig): Configuration for matrix-specific settings.
        extract_command (str): Command template for extracting data.
        fact_tables (list): List of fact tables to be extracted.
    """
    def __init__(self):
        self.compute = ComputeConfig()
        self.storage = StorageConfig()
        self.bigquery = BigQueryConfig()
        self.matrix = MatrixConfig()

        self.extract_command = """python scripts/matrix_export/main.py
         --project-id={project}
         --destination-bucket={destination_bucket}
         --server-name={server_name}
         --catalog={catalog}
         --user-id-secret={user_id_secret}
         --password-secret={password_secret}
         --source-table="{source_table}"
         --client-ids="{client_ids}"
        """

        self.fact_tables = [
            'booking_spot',
            # 'buying_brief',
        ]

        self.export_list = [
            {
                'client_name': 'cba',
                'client_ids': "15173,15603,15604,15605,15606,15607,15608,15609,15610,15612,15613,15614,15616,15617,15670,16230,16308,16309,15729",
                'start_date': '2025-03-02',
            },
            {
                'client_name': 'dpe',
                'client_ids': "7168,7835,7837,5927,9740",
                'start_date': '2025-02-02',
            },
            {
                'client_name': 'ford',
                'client_ids': "15854,15853,15442,15425,15362,15333,15304,15303,15302,15301,15300,15299,15298,15297,15296,15295,15294,15293,14154,11993,8352,7207,7051,6926,6162,6159,6146,5122,3496,637,636,635,634,633,632,631,630,629,628,627,444,307,306,197,177,174,172,59,58",
                'start_date': '2023-06-01',
            },
            {
                'client_name': 'mswa',
                'client_ids': "7107",
                'start_date': '2023-06-01',
            },
            {
                'client_name': 'allianz',
                'client_ids': "5733,5734,5736,7434,7435,9413,9749,14220,15863,15892",
                'start_date': '2023-06-01',
            },
            {
                'client_name': 'jetstar',
                'client_ids': "3742,4140,5930,10049,13423",
                'start_date': '2023-06-01',
            }
        ]
