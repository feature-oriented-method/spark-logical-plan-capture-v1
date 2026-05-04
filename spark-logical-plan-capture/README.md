# spark-logical-plan-capture

Python package for connecting the Spark extension
`io.github.mt.logicplan.LogicalPlanCaptureExtension` via `spark-submit`
or from PySpark code.

## Install

```bash
pip install spark-logical-plan-capture
```

## Usage in PySpark

```python
from pyspark.sql import SparkSession
from spark_logical_plan_capture import get_spark_conf, get_jar_path

conf = get_spark_conf()

spark = (
    SparkSession.builder
    .config("spark.sql.extensions", conf["spark.sql.extensions"])
    .config("spark.jars", conf["spark.jars"])
    .getOrCreate()
)
```

## Usage with spark-submit

```bash
spark-submit \
  --conf "spark.sql.extensions=io.github.mt.logicplan.LogicalPlanCaptureExtension" \
  --jars "$(python -c 'from spark_logical_plan_capture import get_jar_path; print(get_jar_path())')" \
  your_job.py
```

Current package line is built for Spark `3.5.2`, Scala `2.13.8`, JVM `11.0.25`.

## Restore SQL from Project JSON

The package also contains a SQL restorer for logical plan JSON with
`org.apache.spark.sql.catalyst.plans.logical.Project`:

```python
from spark_logical_plan_capture import project_json_to_sql

plan_json = """
{
  "class": "org.apache.spark.sql.catalyst.plans.logical.Project",
  "projectList": [
    {
      "class": "org.apache.spark.sql.catalyst.expressions.Alias",
      "name": "a",
      "child": {
        "class": "org.apache.spark.sql.catalyst.expressions.Literal",
        "value": 1,
        "dataType": "integer"
      }
    }
  ],
  "child": {"class": "org.apache.spark.sql.catalyst.plans.logical.OneRowRelation"}
}
"""

sql = project_json_to_sql(plan_json)
# SELECT 1 AS a FROM (SELECT 1)
```
