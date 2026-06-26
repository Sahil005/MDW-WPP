import os
import json
import time
import logging
import pandas as pd
from datetime import datetime
from sys import path
from google.cloud import secretmanager, storage
from utils import query_lookup, parse_arguments
path.append('\\Program Files\\Microsoft.NET\\ADOMD.NET\\160')
from pyadomd import Pyadomd

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def access_secret_version(project_id, secret_id, version_id='latest'):
    """
    Access the payload for the given secret version if one exists.
    """
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    logging.info(f"Fetching secret: {name}")
    response = client.access_secret_version(request={"name": name})
    payload = response.payload.data.decode("UTF-8")
    return payload

def main():
    logging.info('Parsing args')
    args = parse_arguments()

    logging.info('Retrieving authentication credentials')
    user_id = access_secret_version(args.project_id, args.user_id_secret)
    password = access_secret_version(args.project_id, args.password_secret)

    logging.info('Configuring MSOLAP connection')
    connection_string = (
        f'Provider=MSOLAP;Data Source={args.server_name};Catalog={args.catalog};'
        f'User ID={user_id};Password={password};'
    )

    # Look for the table in the query lookup config
    for row in query_lookup:
        table_name = row['source_table_name']

        if table_name == args.source_table:
            export_config = next(
                (item for item in query_lookup if item['source_table_name'] == table_name),
                None
            )

            logging.info(f"Preparing to process table: {table_name}")
            cleaned_ids = args.client_ids.strip("()[]{}\"' ").replace(" ", "")
            client_id_formatted = "{" + cleaned_ids + "}"
            logging.info(f"Cleaned client IDs: {client_id_formatted}")
            start_date= args.start_date.replace("-", "")
            end_date= args.end_date.replace("-", "")
            logging.info(f"start date: {start_date}")
            # Format query with filters if needed
            if table_name == "booking_spot_2":
                # Use calendar_week if provided (legacy), otherwise generate from current date
                calendar_week = getattr(args, 'calendar_week', datetime.now().strftime('%Y%m%d'))
                query = export_config['query'].format(
                    calendar_week=calendar_week,
                    client_ids=client_id_formatted,
                    date_from=start_date,
                    date_to=end_date,
                )
            else:
                query = export_config['query']

            logging.info(f'Executing query:\n{query}')

            # Extract data from Analysis Services
            try:
                with Pyadomd(connection_string) as conn:
                    with conn.cursor().execute(query) as cur:
                        df = pd.DataFrame(
                            cur.fetchall(),
                            columns=[i.name for i in cur.description]
                        )
            except Exception as e:
                logging.error(f"Error executing query: {e}")
                return

            # Clean column names
            df.columns = [col.split('[')[-1].strip(']') for col in df.columns]
            df.columns = df.columns.str.replace('[^A-Za-z0-9_]+', '', regex=True)

            # Determine output path - use new hive-partitioned structure if provided
            if hasattr(args, 'output_path') and args.output_path:
                # New hive-partitioned structure
                filename = f"{args.output_path}/{args.output_filename}"
                logging.info(f"Using hive-partitioned path: {filename}")
            else:
                # Legacy structure for backward compatibility
                calendar_week = getattr(args, 'calendar_week', datetime.now().strftime('%Y%m%d'))
                filename = f"matrix/tables/{export_config['destination_table_name']}_{args.client_name}/{calendar_week}.csv"
                logging.info(f"Using legacy path: {filename}")

            logging.info(f"Saving to GCS at: {filename}")

            # Upload to GCS
            try:
                storage_client = storage.Client()
                bucket = storage_client.bucket(args.destination_bucket)
                bucket.blob(filename).upload_from_string(
                    df.to_csv(index=False),
                    'text/csv'
                )
                logging.info(f"Exported table '{table_name}' for client '{args.client_name}' to: {filename}")
            except Exception as e:
                logging.error(f"Upload failed: {e}")
                return

            time.sleep(3)

if __name__ == '__main__':
    main()
