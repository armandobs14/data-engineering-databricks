# Apache Spark Operator & Kubenetes
https://github.com/helm/charts/tree/master/incubator/sparkoperator
https://github.com/GoogleCloudPlatform/spark-on-k8s-operator/blob/master/docs/quick-start-guide.md
https://github.com/GoogleCloudPlatform/spark-on-k8s-operator/blob/master/docs/user-guide.md
https://github.com/GoogleCloudPlatform/spark-on-k8s-operator/tree/master/sparkctl

### installing operator
```sh
# create namespace
k create namespace spark

# add helm repo reference
helm repo add incubator http://storage.googleapis.com/kubernetes-charts-incubator

# install operator
helm install spark incubator/sparkoperator -f /Users/luanmorenomaciel/BitBucket/databricks/kubernetes/spark_operator-values.yaml --namespace spark --set serviceName="spark" --set sparkJobNamespace=spark

# check helm deployment
helm ls --all-namespaces
```

### info operator
```sh
# change context
kubens spark

# deployments
k get deployments

# get pods
k get pods

# describe pods
k describe pod spark-sparkoperator-7859d8869b-mxk9d

# access container
kubectl exec -c sparkoperator -it spark-sparkoperator-7859d8869b-mxk9d -- /bin/bash
```

### deploying apps
```sh
# yaml file location
/Users/luanmorenomaciel/BitBucket/databricks/kubernetes/etl-users-python.yaml

# deploy test spark
kubectl apply -f /Users/luanmorenomaciel/BitBucket/databricks/kubernetes/etl-users-python.yaml

# verify submit
kubectl describe sparkapplication etl-users-python

# pods [scale]
k get pods --watch

# describe pod
k describe pod etl-users-python-driver   

# logs
k logs etl-users-python-driver spark-kubernetes-driver

# read details of spark app
kubectl get sparkapplications etl-users-python -o=yaml

# port foward to spark ui
kubectl port-forward etl-users-python-driver 4040:4040

# delete spark app
k delete SparkApplication etl-users-python
```

### deploying scala app
```sh
# yaml file location
/Users/luanmorenomaciel/BitBucket/databricks/kubernetes/etl-users-scala.yaml

# deploy test spark
kubectl apply -f /Users/luanmorenomaciel/BitBucket/databricks/kubernetes/etl-users-scala.yaml

# verify submit
kubectl describe sparkapplication etl-users-scala

# pods [scale]
k get pods --watch

# describe pod
k describe pod etl-users-python-driver   

# logs
k logs etl-users-python-driver spark-kubernetes-driver

# read details of spark app
kubectl get sparkapplications etl-users-python -o=yaml

# port foward to spark ui
kubectl port-forward etl-users-python-driver 4040:4040

# delete spark app
k delete SparkApplication etl-users-python
```
