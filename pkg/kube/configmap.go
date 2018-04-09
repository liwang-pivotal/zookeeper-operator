package kube

import (
	"github.com/liwang-pivotal/zookeeper-operator/spec"

	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	log "github.com/sirupsen/logrus"
)

func generateConfigMap(cluster spec.ZookeeperCluster) *v1.ConfigMap {
	configMap := &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: "zk-config",
			Namespace: cluster.ObjectMeta.Namespace,
		},
		Data: map[string]string{
			"ensemble": "zk-0;zk-1;zk-2",
			"jvm.heap": "512M",
			"tick": "2000",
			"init": "10",
			"sync": "5",
			"client.cnxns": "60",
			"snap.retain": "3",
			"purge.interval": "1",
		},
	}

	return configMap
}

func (k *Kubernetes) CreateOrUpdateConfigMap(configMap *v1.ConfigMap) error {
	methodLogger := logger.WithFields(log.Fields{
		"method":    "CreateOrUpdateConfigMap",
		"name":      configMap.ObjectMeta.Name,
		"namespace": configMap.ObjectMeta.Namespace,
	})

	exists, err := k.IfConfigMapExists(configMap)
	if err != nil {
		methodLogger.WithField("error", err).Error("Error while checking if ConfigMap exists")
		return err
	}
	if !exists {
		err = k.createConfigMap(configMap)
	} else {
		err = k.updateConfigMap(configMap)
	}
	if err != nil {
		methodLogger.WithField("error", err).Error("Error while creating or updating ConfigMap")
	}
	return err
}

func (k *Kubernetes) IfConfigMapExists(configMap *v1.ConfigMap) (bool, error) {
	methodLogger := logger.WithFields(log.Fields{
		"method":    "IfConfigMapExists",
		"name":      configMap.ObjectMeta.Name,
		"namespace": configMap.ObjectMeta.Namespace,
	})
	namespace := configMap.ObjectMeta.Namespace
	cf, err := k.Client.CoreV1().ConfigMaps(namespace).Get(configMap.ObjectMeta.Name, k.DefaultOption)

	if err != nil {
		if errors.IsNotFound(err) {
			methodLogger.Debug("ConfigMap doesn't exist")
			return false, nil
		} else {
			methodLogger.WithFields(log.Fields{
				"error": err,
			}).Error("Cant get ConfigMap INFO from API")
			return false, err
		}

	}
	if len(cf.Name) == 0 {
		methodLogger.Debug("ConfigMap.Name == 0, therefore it doesn't exists")
		return false, nil
	}
	return true, nil
}

func (k *Kubernetes) createConfigMap(configMap *v1.ConfigMap) error {
	_, err := k.Client.CoreV1().ConfigMaps(configMap.ObjectMeta.Namespace).Create(configMap)
	return err
}

func (k *Kubernetes) updateConfigMap(configMap *v1.ConfigMap) error {
	_, err := k.Client.CoreV1().ConfigMaps(configMap.ObjectMeta.Namespace).Update(configMap)
	return err
}

func (k *Kubernetes) deleteConfigMap(configMap *v1.ConfigMap) error {
	methodLogger := logger.WithFields(log.Fields{
		"method":    "deleteConfigMap",
		"name":      configMap.ObjectMeta.Name,
		"namespace": configMap.ObjectMeta.Namespace,
	})
	exists, err := k.IfConfigMapExists(configMap)
	if err != nil {
		methodLogger.WithField("error", err).Error("Error while checking if ConfigMap exists")
		return err
	}
	if exists {
		err = k.Client.CoreV1().ConfigMaps(configMap.ObjectMeta.Namespace).Delete(configMap.ObjectMeta.Name, &metav1.DeleteOptions{})
		if err != nil {
			methodLogger.WithField("error", err).Error("Can delete ConfigMap")
			return err
		}
	} else {
		methodLogger.Debug("Trying to delete but ConfigMap doesn't exist.")

	}
	return nil
}