"""
Matrix Configuration v3 - Dynamic Backfill Implementation

This configuration file is specifically designed for the v3 dynamic backfill 
implementation and is completely isolated from the production configuration.

Key differences from matrix_config.py:
- Uses test bucket: au-accel-mdw-dev-biglake-poc
- Only contains DPE client for isolated testing
- Earlier start date (2023-01-01) for comprehensive backfill testing
- References matrix_export_v3 scripts with hive-partitioned output support
"""

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
        self.setup_command = 'gsutil -m cp -r gs://au-accel-mdw-dev-scripts/matrix_export_v3/ ./scripts_v3/'
        self.user = 'matt_burrows'

class StorageConfig:
    """
    Configuration class for storage settings.

    Attributes:
        data_bucket (str): The name of the test bucket for v3 implementation.
    """
    def __init__(self):
        self.data_bucket = 'au-accel-mdw-dev-biglake-data'

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
    Configuration class for v3 dynamic backfill implementation.

    Attributes:
        compute (ComputeConfig): Configuration for compute resources.
        storage (StorageConfig): Configuration for storage resources.
        bigquery (BigQueryConfig): Configuration for BigQuery resources.
        matrix (MatrixConfig): Configuration for matrix-specific settings.
        extract_command (str): Command template for extracting data.
        fact_tables (list): List of fact tables to be extracted.
        export_list (list): List of clients for v3 dynamic extraction.
    """
    def __init__(self):
        self.compute = ComputeConfig()
        self.storage = StorageConfig()
        self.bigquery = BigQueryConfig()
        self.matrix = MatrixConfig()

        # v3 extract command template with hive-partitioned output
        self.extract_command = """python scripts_v3/main.py
         --project-id={project}
         --destination-bucket={destination_bucket}
         --output-path={output_path}
         --server-name={server_name}
         --catalog={catalog}
         --user-id-secret={user_id_secret}
         --password-secret={password_secret}
         --source-table="{source_table}"
         --client-ids="{client_ids}"
         --client-name="{client_name}"
         --start-date={start_date}
         --end-date={end_date}
         --output-filename="{output_filename}"
        """

        self.fact_tables = [
            'booking_spot',
        ]

        # v3 export list - isolated for testing with earlier start date
        self.export_list = [
            {
                'client_name': 'foxtel',
                'client_ids': "6762,6763,6764,6765,6766,6767,6768,6769,6770,6771,6772,6829,6830,6872,6888,6895,6904,6907,6985,7016,7043,7132,7161,7269,7407,7664,7665,7666,7667,7668,7669,7670,7671,7672,7673,7674,7675,7676,7677,7678,7679,7680,7681,7682,7683,7684,7685,7686,7687,7688,7689,7690,7691,7692,7693,7694,7695,7696,7697,7698,7699,7700,7701,7702,7703,7704,7705,7706,7707,7708,7709,7710,7711,7712,7768,7769,7770,7771,7801,7821,7912,7913,7914,7944,7945,8548,8549,8550,8551,8552,8553,8554,8555,8556,8557,8558,8707,8708,8709,8710,8711,8899,9720,11483,14127,14319,14759,15166,15268",
                'start_date': '2022-07-01',
            },
            {
                'client_name': 'dpe',
                'client_ids': "7168,7835,7837,5927,9740",
                'start_date': '2023-01-01',
                'description': 'DPE client for v2 dynamic backfill testing with BigLake integration'
            },
            {
                'client_name': 'cba',
                'client_ids': "15173,15603,15604,15605,15606,15607,15608,15609,15610,15612,15613,15614,15616,15617,15670,16230,16308,16309,15729",
                'start_date': '2023-01-01',
            },
            {
                'client_name': 'allianz_au',
                'client_ids': "57433,15885,15887",
                'start_date': '2022-06-01',
            },
            {
                'client_name': 'bankwest',
                'client_ids': "5530,3624,504,3284,15629,15634,9453",
                'start_date': '2023-03-01',
            },
            {
                'client_name': 'kfc',
                'client_ids': "16413,16243,16241,16238,16237,16210,16191,16177,16153,16034,15965,15549,15501,15470,15419,15344,15343,15254,15252,15216,15215,14368,14277,14216,12330,12328,12327,12325,10918,10917,9731,9730,8734,8685,8684,8683,8682,8681,8640,8502,7440,7311,7184,7018,6905,6660,6659,6658,6639,6630,6629,6448,6236,6158,6157,6156,6042,5327,5145,5026,5025,5024,5023,5022,5021,5018",
                'start_date': '2023-01-01',
            }
        ]
