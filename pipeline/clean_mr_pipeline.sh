set -x # prints commands as they are executed

read -p "hit if you are sure you want to delete $1"
hdfs dfs -rm -r /user/nilmeier/$1


