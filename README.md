# spark-logical-plan-capture-v1

Repository containing the Spark logical plan capture extension and Python wrapper.

## Project Structure

- **spark-logical-plan-capture/** - Python package for connecting to the Spark extension `io.github.mt.logicplan.LogicalPlanCaptureExtension`

## Python Package

The Python package allows you to connect to the Spark logical plan capture extension via `spark-submit` or from PySpark code.

### Installation

```bash
pip install spark-logical-plan-capture
```

### Quick Start

```python
from pyspark.sql import SparkSession
from spark_logical_plan_capture import get_spark_conf

conf = get_spark_conf()

spark = (
    SparkSession.builder
    .config("spark.sql.extensions", conf["spark.sql.extensions"])
    .config("spark.jars", conf["spark.jars"])
    .getOrCreate()
)
```

For detailed usage instructions, see [spark-logical-plan-capture/README.md](spark-logical-plan-capture/README.md).

## Requirements

- Spark `3.5.2`
- Scala `2.13.8`
- JVM `11.0.25`
- Python `>=3.8`

## License

MIT
