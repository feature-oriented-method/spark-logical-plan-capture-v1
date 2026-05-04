from importlib import resources
from pathlib import Path
from typing import Dict, List

EXTENSION_CLASS = "io.github.mt.logicplan.LogicalPlanCaptureExtension"


def _get_jar_path_cached() -> str:
    """Internal cached lookup for the extension JAR path."""
    jars_dir = resources.files("spark_logical_plan_capture").joinpath("jars")
    jar_files = sorted(p for p in jars_dir.iterdir() if p.suffix == ".jar")
    if not jar_files:
        raise FileNotFoundError("No extension jar found in package data")
    return str(Path(jar_files[-1]))


def get_jar_path() -> str:
    """Return the path to the extension JAR file."""
    return _get_jar_path_cached()


def get_spark_conf() -> Dict[str, str]:
    """Return Spark configuration dictionary for the logical plan capture extension."""
    jar_path = get_jar_path()
    return {
        "spark.sql.extensions": EXTENSION_CLASS,
        "spark.jars": jar_path,
    }


def get_submit_args() -> List[str]:
    """Return command-line arguments for spark-submit."""
    conf = get_spark_conf()
    return [
        "--conf",
        f"spark.sql.extensions={conf['spark.sql.extensions']}",
        "--jars",
        conf["spark.jars"],
    ]
