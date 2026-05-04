from importlib import resources
from pathlib import Path
from typing import Dict, List

EXTENSION_CLASS = "io.github.mt.logicplan.LogicalPlanCaptureExtension"


def get_jar_path() -> str:
    jars_dir = resources.files("spark_logical_plan_capture").joinpath("jars")
    jar_files = sorted(p for p in jars_dir.iterdir() if p.suffix == ".jar")
    if not jar_files:
        raise FileNotFoundError("No extension jar found in package data")
    return str(Path(jar_files[-1]))


def get_spark_conf() -> Dict[str, str]:
    return {
        "spark.sql.extensions": EXTENSION_CLASS,
        "spark.jars": get_jar_path(),
    }


def get_submit_args() -> List[str]:
    conf = get_spark_conf()
    return [
        "--conf",
        f"spark.sql.extensions={conf['spark.sql.extensions']}",
        "--jars",
        conf["spark.jars"],
    ]
