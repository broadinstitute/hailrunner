#!/bin/bash
# Minimal Hail initialization for Dataproc clusters.
# Installs Hail without Jupyter/notebook dependencies, so it works
# on networks that block github.com (e.g. Terra/Cromwell).
set -euo pipefail

ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)

if [ "$ROLE" != "Master" ]; then
    echo "Worker node — skipping Hail install."
    exit 0
fi

echo "=== Installing Hail on master node ==="

# Get Hail wheel path from cluster metadata
WHEEL=$(/usr/share/google/get_metadata_value attributes/WHEEL)
echo "Wheel: $WHEEL"

# Copy wheel from GCS and install (with deps from PyPI)
gsutil cp "$WHEEL" /tmp/hail.whl
pip install --quiet /tmp/hail.whl
rm -f /tmp/hail.whl

# Locate the Hail JAR
HAIL_HOME=$(python3 -c "import hail, os; print(os.path.dirname(hail.__file__))")
HAIL_JAR="$HAIL_HOME/backend/hail-all-spark.jar"

if [ ! -f "$HAIL_JAR" ]; then
    echo "ERROR: Hail JAR not found at $HAIL_JAR" >&2
    exit 1
fi
echo "Hail JAR: $HAIL_JAR"

# Set environment variables on master
SPARK_LIB="/usr/lib/spark/python/lib"
PY_ZIPS=$(find "$SPARK_LIB" -name '*.zip' -printf '%p:' 2>/dev/null | sed 's/:$//')

for conf_file in /etc/environment /usr/lib/spark/conf/spark-env.sh; do
    cat >> "$conf_file" <<ENV
export PYTHONHASHSEED=0
export PYTHONPATH=$PY_ZIPS
export SPARK_HOME=/usr/lib/spark/
export PYSPARK_PYTHON=/opt/conda/default/bin/python
export PYSPARK_DRIVER_PYTHON=/opt/conda/default/bin/python
export HAIL_LOG_DIR=/home/hail
export HAIL_DATAPROC=1
ENV
done

# Configure Spark defaults for Hail
cat >> /etc/spark/conf/spark-defaults.conf <<SPARK
spark.executorEnv.PYTHONHASHSEED=0
spark.app.name=Hail
spark.jars=$HAIL_JAR
spark.driver.extraClassPath=$HAIL_JAR
spark.executor.extraClassPath=./hail-all-spark.jar
SPARK

echo "=== Hail installation complete ==="
