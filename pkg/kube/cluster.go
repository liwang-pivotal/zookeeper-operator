package kube

import (
	"github.com/liwang-pivotal/zookeeper-operator/spec"
)

func CreateCluster(cluster spec.ZookeeperCluster, client Kubernetes) error {

	headlessSVC := generateHeadlessService(cluster)
	err := client.CreateOrUpdateService(headlessSVC)
	if err != nil {
		return err
	}

	configMap := generateConfigMap(cluster)
	err = client.CreateOrUpdateConfigMap(configMap)
	if err != nil {
		return err
	}

	sts := generateZookeeperStatefulset(cluster)
	err = client.CreateOrUpdateStatefulSet(sts)
	if err != nil {
		return err
	}

	return nil
}

func DeleteCluster(cluster spec.ZookeeperCluster, client Kubernetes) error {
	sts := generateZookeeperStatefulset(cluster)
	err := client.deleteStatefulset(sts)
	if err != nil {
		return err
	}

	configMap := generateConfigMap(cluster)
	err = client.deleteConfigMap(configMap)
	if err != nil {
		return err
	}

	headlessSVC := generateHeadlessService(cluster)
	err = client.deleteService(headlessSVC)
	if err != nil {
		return err
	}

	return nil
}