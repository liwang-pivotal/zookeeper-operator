package kube

import (
	"github.com/liwang-pivotal/zookeeper-operator/spec"

	"k8s.io/api/core/v1"
	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func generateHeadlessService(cluster spec.ZookeeperCluster) *v1.Service {
	labelSelectors := createLabels(cluster)

	objectMeta := metav1.ObjectMeta{
		Name:        "zk-headless",
		Labels:      map[string]string{
			"app": "zk-headless",
		},
		Namespace: cluster.ObjectMeta.Namespace,
	}

	service := &v1.Service{
		ObjectMeta: objectMeta,

		Spec: v1.ServiceSpec{
			Ports: []v1.ServicePort{
				{
					Name: "server",
					Port: 2888,
				},
				{
					Name: "leader-election",
					Port: 3888,
				},
				{
					Name: "client",
					Port: 2181,
				},
			},
			ClusterIP: "None",
			Selector: labelSelectors,
		},
	}

	return service
}

func (k *Kubernetes) CreateOrUpdateService(service *v1.Service) error {
	methodLogger := logger.WithFields(log.Fields{
		"method":    "CreateOrUpdateService",
		"name":      service.ObjectMeta.Name,
		"namespace": service.ObjectMeta.Namespace,
	})

	exists, err := k.IfServiceExists(service)
	if err != nil {
		methodLogger.WithField("error", err).Error("Error while checking if services exists")
		return err
	}
	if !exists {
		err = k.createService(service)
	} else {
		// Update will cause issue: https://www.timcosta.io/kubernetes-service-invalid-clusterip-or-resourceversion
		//err = k.updateService(service)
	}
	if err != nil {
		methodLogger.WithField("error", err).Error("Error while creating or updating service")
	}
	return err
}

func (k *Kubernetes) IfServiceExists(service *v1.Service) (bool, error) {
	methodLogger := logger.WithFields(log.Fields{
		"method":    "IfServiceExists",
		"name":      service.ObjectMeta.Name,
		"namespace": service.ObjectMeta.Namespace,
	})
	namespace := service.ObjectMeta.Namespace
	svc, err := k.Client.CoreV1().Services(namespace).Get(service.ObjectMeta.Name, k.DefaultOption)

	if err != nil {
		if errors.IsNotFound(err) {
			methodLogger.Debug("Service dosnt exist")
			return false, nil
		} else {
			methodLogger.WithFields(log.Fields{
				"error": err,
			}).Error("Cant get Service INFO from API")
			return false, err
		}

	}
	if len(svc.Name) == 0 {
		methodLogger.Debug("Service.Name == 0, therefore it dosnt exists")
		return false, nil
	}
	return true, nil
}

func (k *Kubernetes) createService(service *v1.Service) error {
	_, err := k.Client.CoreV1().Services(service.ObjectMeta.Namespace).Create(service)
	return err
}

func (k *Kubernetes) updateService(service *v1.Service) error {
	_, err := k.Client.CoreV1().Services(service.ObjectMeta.Namespace).Update(service)
	return err
}

func (k *Kubernetes) deleteService(service *v1.Service) error {
	methodLogger := logger.WithFields(log.Fields{
		"method":    "DeleteService",
		"name":      service.ObjectMeta.Name,
		"namespace": service.ObjectMeta.Namespace,
	})
	exists, err := k.IfServiceExists(service)
	if err != nil {
		methodLogger.WithField("error", err).Error("Error while checking if services exists")
		return err
	}
	if exists {
		err = k.Client.CoreV1().Services(service.ObjectMeta.Namespace).Delete(service.ObjectMeta.Name, &metav1.DeleteOptions{})
		if err != nil {
			methodLogger.WithField("error", err).Error("Can delete service")
			return err
		}
	} else {
		methodLogger.Debug("Trying to delete but Service dosnt exist.")

	}
	return nil
}