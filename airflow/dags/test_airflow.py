from airflow.operators.python import PythonOperator
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator

from airflow import DAG

with DAG(dag_id="dag_test") as dag:
    (
        ClickHouseOperator(
            task_id="test_count",
            database="highlights",
            sql=(
                """
                SELECT count() from videos
                """
            ),
            # query_id is templated and allows to quickly identify query in ClickHouse logs
            query_id="{{ ti.dag_id }}-{{ ti.task_id }}-{{ ti.run_id }}-{{ ti.try_number }}",
            clickhouse_conn_id="clickhouse_default",
        )
        >> PythonOperator(
            task_id="print_test_count",
            python_callable=lambda task_instance:
            # pulling XCom value and printing it
            print(task_instance.xcom_pull(task_ids="dag_test")),
        )
    )
