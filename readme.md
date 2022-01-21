# run cluster
```shell
gcloud beta dataproc clusters create pbd-cluster-big-data --enable-component-gateway --bucket pbd_dm_21 --region europe-west4 --zone europe-west4-c --master-machine-type n1-standard-4 --master-boot-disk-size 50 --num-workers 2 --worker-machine-type n1-standard-2 --worker-boot-disk-size 50 --image-version 2.0-debian10 --optional-components ZEPPELIN --project propane-dogfish-328022 --max-age=3h
```
# in cluster shell:

## install SBT
```shell
echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | sudo tee /etc/apt/sources.list.d/sbt.list
echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | sudo tee /etc/apt/sources.list.d/sbt_old.list
curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | sudo apt-key add
sudo apt-get update
sudo apt-get install sbt
```



## Load data into hdfs
```shell
gsutil cp gs://pbd_dm_21/airbnb_c.zip .
unzip airbnb_c.zip
hdfs dfs -put airbnb_c/* project
```

## clone repo
```shell
git clone https://github.com/mumot-inf-7/bigdata-project2.git
```

## create tables
```shell
cd bigdata-project2
spark-shell -i src/Database.scala
```

## compile and run scala
```shell
sbt package 
spark-submit target/scala-2.12/spark_airbnb_2.12-0.1.jar project
```
