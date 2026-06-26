
python3 main.py \
    --project-id au-accel-mdw-dev \
    --server-name asazure://australiaeast.asazure.windows.net/aeaansematrixauprd01 \
    --catalog bookingmonitoring \
    --user-id-secret airflow-variables-matrix_upn \
    --password-secret airflow-variables-matrix_password \
    --client dominos \
    --year 2024


python matrix_connect.py --project=au-accel-mdw-dev --destination-bucket=au-accel-mdw-dev-data --server-name=asazure://australiaeast.asazure.windows.net/aeaansematrixauprd01 --catalog=bookingmonitoring --user-id-secret=airflow-variables-matrix_upn --password-secret=airflow-variables-matrix_password --client="domino's" --year=2024