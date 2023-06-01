
if [ -z $1 ]; then
	echo "usage run.sh <filename>"
	exit 2
fi

if [ $1 == "void" ]; then
    sleep $2
    exit 0
fi

echo "running script ${1}"

unset PYSPARK_DRIVER_PYTHON

spark-submit $1
