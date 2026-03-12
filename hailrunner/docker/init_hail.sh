#!/bin/bash
# Minimal Hail initialization for Dataproc clusters.
# Modeled on hailctl's init_notebook.py but without Jupyter/notebook/
# sparkmonitor/jgscm setup (which requires github.com access).
set -euo pipefail

ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)

if [ "$ROLE" != "Master" ]; then
    echo "Worker node — skipping Hail install."
    exit 0
fi

echo "=== Installing Hail on driver node ==="

# Download pre-bundled wheels from staging bucket (includes hail + all deps)
WHEELS=$(/usr/share/google/get_metadata_value attributes/WHEELS)
echo "Wheels: $WHEELS"

mkdir -p /tmp/hail-wheels
gsutil -m cp "$WHEELS/*" /tmp/hail-wheels/
pip install --no-index --find-links=/tmp/hail-wheels/ hail
rm -rf /tmp/hail-wheels

# Locate the Hail JAR via pip (same approach as init_notebook.py)
HAIL_HOME=$(python3 -m pip show hail | grep ^Location | awk '{print $2}')/hail
HAIL_JAR="$HAIL_HOME/backend/hail-all-spark.jar"

if [ ! -f "$HAIL_JAR" ]; then
    echo "ERROR: Hail JAR not found at $HAIL_JAR" >&2
    exit 1
fi
echo "Hail JAR: $HAIL_JAR"

# Set environment variables (same locations as init_notebook.py)
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

# Configure Spark defaults for Hail (same as init_notebook.py)
cat >> /etc/spark/conf/spark-defaults.conf <<SPARK
spark.executorEnv.PYTHONHASHSEED=0
spark.app.name=Hail
spark.jars=$HAIL_JAR
spark.driver.extraClassPath=$HAIL_JAR
spark.executor.extraClassPath=./hail-all-spark.jar
SPARK

echo "=== Hail installation complete ==="
