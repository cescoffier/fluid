# Deployment on OpenShift

## Login to OpenShift

* Login to your OpenShift instance and select your project

## Deploy Kafka

* Deploy Kafka using the template located in the openshift directory. The template comes from https://github.com/EnMasseProject/barnabas/blob/master/kafka-statefulsets/resources/openshift-template.yaml (but reduce the number of pods because of memory restriction)
```
oc create -f openshift/kafka-template.yaml
```

* Instantiate the template using:
```
oc new-app barnabas
```

* Wait for deployment completion. Check with `oc status` until all pods are ready:
```
In project fluid-example on server https://api.rh-us-east-1.openshift.com:443

svc/kafka - 172.30.66.244:9092
  statefulset/kafka manages enmasseproject/kafka-statefulsets:latest, created about a minute ago - 1 pod

svc/kafka-headless (headless):9092
  statefulset/kafka manages enmasseproject/kafka-statefulsets:latest, created about a minute ago - 1 pod

svc/zookeeper - 172.30.54.78:2181
  statefulset/zookeeper manages enmasseproject/zookeeper:latest, created about a minute ago - 1 pod

svc/zookeeper-headless (headless) ports 2181, 2888, 3888
  statefulset/zookeeper manages enmasseproject/zookeeper:latest, created about a minute ago - 1 pod

View details with 'oc describe <resource>/<name>' or list everything with 'oc get all'.
```

## Deploy config map containing the fluid config

* `oc policy add-role-to-user view -n $(oc project -q) -z default`
* `oc create configmap my-fluid-config --from-file=openshift/fluid-config-map.yaml`

## Deploy the application

* `mvn clean fabric8:deploy -Popenshift`
