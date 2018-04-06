package kube

import (
	"github.com/liwang-pivotal/zookeeper-operator/spec"

	"k8s.io/api/core/v1"
	appsv1Beta2 "k8s.io/api/apps/v1beta2"
	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	defaultCPU    = "10m"
	defaultDisk   = "1Gi"
	defaultMemory = "50Mi"
)


func CreateCluster(cluster spec.ZookeeperCluster, client Kubernetes) error {

	headlessSVC := generateHeadlessService(cluster)
	err := client.CreateOrUpdateService(headlessSVC)
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

	headlessSVC := generateHeadlessService(cluster)
	err = client.deleteService(headlessSVC)
	if err != nil {
		return err
	}

	return nil
}

func generateZookeeperStatefulset(cluster spec.ZookeeperCluster) *appsv1Beta2.StatefulSet {

	name := cluster.ObjectMeta.Name
	replicas := cluster.Spec.BrokerCount

	cpus, err := resource.ParseQuantity(cluster.Spec.Resources.CPU)
	if err != nil {
		cpus, _ = resource.ParseQuantity(defaultCPU)
	}

	memory, err := resource.ParseQuantity(cluster.Spec.Resources.Memory)
	if err != nil {
		memory, _ = resource.ParseQuantity(defaultMemory)
	}

	statefulSet := &appsv1Beta2.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: createLabels(cluster),
			Namespace: cluster.ObjectMeta.Namespace,
		},
		Spec: appsv1Beta2.StatefulSetSpec{
			Replicas: &replicas,

			Selector: &metav1.LabelSelector{
				MatchLabels: createLabels(cluster),
			},
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: createLabels(cluster),
				},
				Spec: v1.PodSpec{
					Affinity: &v1.Affinity{
						PodAntiAffinity: &v1.PodAntiAffinity{
							PreferredDuringSchedulingIgnoredDuringExecution: []v1.WeightedPodAffinityTerm{
								{
									Weight: 100,
									PodAffinityTerm: v1.PodAffinityTerm{
										Namespaces: []string{cluster.ObjectMeta.Namespace},
										LabelSelector: &metav1.LabelSelector{
											MatchLabels: createLabels(cluster),
										},
										TopologyKey: "kubernetes.io/hostname",
									},
								},
							},
						},
					},
					Containers: []v1.Container{
						{
							Name:  "sysctl-conf",
							Image: "busybox:1.26.2",
							Command: []string{
								"sh",
								"-c",
								"sysctl -w vm.max_map_count=262166 && while true; do sleep 86400; done",
							},
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									"cpu":    cpus,
									"memory": memory,
								},
								Requests: v1.ResourceList{
									"cpu":    cpus,
									"memory": memory,
								},
							},
							SecurityContext: &v1.SecurityContext{
								Privileged: &[]bool{true}[0],
							},
						},
					},
				},
			},
		},
	}

	return statefulSet;
}

func (k *Kubernetes) CreateOrUpdateStatefulSet(statefulset *appsv1Beta2.StatefulSet) error {
	methodLogger := logger.WithFields(log.Fields{
		"method":    "CreateOrUpdateStatefulSet",
		"name":      statefulset.ObjectMeta.Name,
		"namespace": statefulset.ObjectMeta.Namespace,
	})

	exists, err := k.IfStatefulSetExists(statefulset)
	if err != nil {
		methodLogger.WithField("error", err).Error("Error while checking if statefulsets exists")
		return err
	}

	if !exists {
		err = k.createStatefulSet(statefulset)
	} else {
		err = k.updateStatefulSet(statefulset)
	}
	if err != nil {
		methodLogger.WithField("error", err).Error("Error while creating or updating statefulset")
	}
	return err
}

func (k *Kubernetes) IfStatefulSetExists(statefulset *appsv1Beta2.StatefulSet) (bool, error) {
	methodLogger := logger.WithFields(log.Fields{
		"method":    "IfStatefulSetExists",
		"name":      statefulset.ObjectMeta.Name,
		"namespace": statefulset.ObjectMeta.Namespace,
	})
	namespace := statefulset.ObjectMeta.Namespace
	sts, err := k.Client.AppsV1beta2().StatefulSets(namespace).Get(statefulset.ObjectMeta.Name, k.DefaultOption)

	if err != nil {
		if errors.IsNotFound(err) {
			methodLogger.Debug("StatefulSet doesn't exist")
			return false, nil
		} else {
			methodLogger.WithFields(log.Fields{
				"error": err,
			}).Error("Cant get StatefulSet INFO from API")
			return false, err
		}

	}
	if len(sts.Name) == 0 {
		methodLogger.Debug("StatefulSet.Name == 0, therefore it doesn't exists")
		return false, nil
	}
	return true, nil
}

func (k *Kubernetes) createStatefulSet(statefulset *appsv1Beta2.StatefulSet) error {
	_, err := k.Client.AppsV1beta2().StatefulSets(statefulset.ObjectMeta.Namespace).Create(statefulset)
	return err
}

func (k *Kubernetes) updateStatefulSet(statefulset *appsv1Beta2.StatefulSet) error {
	_, err := k.Client.AppsV1beta2().StatefulSets(statefulset.ObjectMeta.Namespace).Update(statefulset)
	return err
}

func (k *Kubernetes) deleteStatefulset(statefulset *appsv1Beta2.StatefulSet) error {
	methodLogger := logger.WithFields(log.Fields{
		"method":    "DeleteStatefulset",
		"name":      statefulset.ObjectMeta.Name,
		"namespace": statefulset.ObjectMeta.Namespace,
	})
	exists, err := k.IfStatefulSetExists(statefulset)
	if err != nil {
		methodLogger.WithField("error", err).Error("Error while checking if StatefulSet exists")
		return err
	}
	if exists {
		//Scale the statefulset down to zero (https://github.com/kubernetes/client-go/issues/91)
		statefulset.Spec.Replicas = new(int32)
		err = k.updateStatefulSet(statefulset)
		if err != nil {
			methodLogger.WithField("error", err).Error("Could not scale statefulset: %s", statefulset.Name)
		} else {
			methodLogger.Info("Scaled statefulset %s to zero: ", statefulset.Name)
		}

		err := k.Client.AppsV1beta1().StatefulSets(statefulset.ObjectMeta.Namespace).Delete(statefulset.ObjectMeta.Name, &metav1.DeleteOptions{
			PropagationPolicy: func() *metav1.DeletionPropagation {
				foreground := metav1.DeletePropagationForeground
				return &foreground
			}(),
		})
		if err != nil {
			methodLogger.WithField("error", err).Error("Could not delete statefulset: %s", statefulset.Name)
			return err
		} else {
			methodLogger.Info("Deleting statefulset: %s", statefulset.Name)
		}
	} else {
		methodLogger.Debug("Trying to delete but StatefulSet doesn't exist.")

	}
	return nil
}

func createLabels(cluster spec.ZookeeperCluster) map[string]string {
	labels := map[string]string{
		"app": "zk",
	}
	return labels
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
		err = k.updateService(service)
	}
	if err != nil {
		methodLogger.WithField("error", err).Error("Error while creating or updating service")
	}
	return err
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