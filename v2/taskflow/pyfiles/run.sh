
if [ -z $1 ]; then
	echo "usage run.sh <filename>"
	exit 2
fi

echo "running script ${1}"

unset PYSPARK_DRIVER_PYTHON

spark-submit $1
