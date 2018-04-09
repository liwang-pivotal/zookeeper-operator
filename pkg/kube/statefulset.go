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
	defaultCPU    = "500m"
	defaultDisk   = "1Gi"
	defaultMemory = "200Mi"
)

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
			ServiceName: "zk-headless",
			Selector: &metav1.LabelSelector{
				MatchLabels: createLabels(cluster),
			},
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: createLabels(cluster),
					Annotations: map[string]string{
						"pod.alpha.kubernetes.io/initialized": "true",
					},
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
							Name:  "k8szk",
							ImagePullPolicy: "Always",
							Image: "gcr.io/google_samples/k8szk:v1",
							Ports: []v1.ContainerPort{
								{
									Name:          "client",
									ContainerPort: 2181,
									Protocol:      v1.ProtocolTCP,
								},
								{
									Name:          "server",
									ContainerPort: 2888,
									Protocol:      v1.ProtocolTCP,
								},
								{
									Name:          "leader-election",
									ContainerPort: 3888,
									Protocol:      v1.ProtocolTCP,
								},
							},
							Env: []v1.EnvVar{
								{
									Name: "ZK_ENSEMBLE",
									ValueFrom: &v1.EnvVarSource{
										ConfigMapKeyRef: &v1.ConfigMapKeySelector{
											LocalObjectReference: v1.LocalObjectReference{Name: "zk-config"},
											Key:                  "ensemble",
										},
									},
								},
								{
									Name: "ZK_HEAP_SIZE",
									ValueFrom: &v1.EnvVarSource{
										ConfigMapKeyRef: &v1.ConfigMapKeySelector{
											LocalObjectReference: v1.LocalObjectReference{Name: "zk-config"},
											Key:                  "jvm.heap",
										},
									},
								},
								{
									Name: "ZK_TICK_TIME",
									ValueFrom: &v1.EnvVarSource{
										ConfigMapKeyRef: &v1.ConfigMapKeySelector{
											LocalObjectReference: v1.LocalObjectReference{Name: "zk-config"},
											Key:                  "tick",
										},
									},
								},
								{
									Name: "ZK_INIT_LIMIT",
									ValueFrom: &v1.EnvVarSource{
										ConfigMapKeyRef: &v1.ConfigMapKeySelector{
											LocalObjectReference: v1.LocalObjectReference{Name: "zk-config"},
											Key:                  "init",
										},
									},
								},
								{
									Name: "ZK_SYNC_LIMIT",
									ValueFrom: &v1.EnvVarSource{
										ConfigMapKeyRef: &v1.ConfigMapKeySelector{
											LocalObjectReference: v1.LocalObjectReference{Name: "zk-config"},
											Key:                  "sync",
										},
									},
								},
								{
									Name: "ZK_MAX_CLIENT_CNXNS",
									ValueFrom: &v1.EnvVarSource{
										ConfigMapKeyRef: &v1.ConfigMapKeySelector{
											LocalObjectReference: v1.LocalObjectReference{Name: "zk-config"},
											Key:                  "client.cnxns",
										},
									},
								},
								{
									Name: "ZK_SNAP_RETAIN_COUNT",
									ValueFrom: &v1.EnvVarSource{
										ConfigMapKeyRef: &v1.ConfigMapKeySelector{
											LocalObjectReference: v1.LocalObjectReference{Name: "zk-config"},
											Key:                  "snap.retain",
										},
									},
								},
								{
									Name: "ZK_PURGE_INTERVAL",
									ValueFrom: &v1.EnvVarSource{
										ConfigMapKeyRef: &v1.ConfigMapKeySelector{
											LocalObjectReference: v1.LocalObjectReference{Name: "zk-config"},
											Key:                  "purge.interval",
										},
									},
								},
								{
									Name: "ZK_MAX_CLIENT_CNXNS",
									ValueFrom: &v1.EnvVarSource{
										ConfigMapKeyRef: &v1.ConfigMapKeySelector{
											LocalObjectReference: v1.LocalObjectReference{Name: "zk-config"},
											Key:                  "client.cnxns",
										},
									},
								},
								{
									Name:  "ZK_CLIENT_PORT",
									Value: "2181",
								},
								{
									Name:  "ZK_SERVER_PORT",
									Value: "2888",
								},
								{
									Name:  "ZK_ELECTION_PORT",
									Value: "3888",
								},
							},
							Command: []string{
								"sh",
								"-c",
								"zkGenConfig.sh && zkServer.sh start-foreground",
							},
							ReadinessProbe: &v1.Probe{
								Handler: v1.Handler{
									Exec: &v1.ExecAction{
										Command: []string{
											"zkOk.sh",
										},
									},
								},
								InitialDelaySeconds: 10,
								TimeoutSeconds:      5,
							},
							LivenessProbe: &v1.Probe{
								Handler: v1.Handler{
									Exec: &v1.ExecAction{
										Command: []string{
											"zkOk.sh",
										},
									},
								},
								InitialDelaySeconds: 10,
								TimeoutSeconds:      5,
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
							VolumeMounts: []v1.VolumeMount{
								{
									Name:      "datadir",
									MountPath: "/var/lib/zookeeper",
								},
							},
							SecurityContext: &v1.SecurityContext{
								Privileged: &[]bool{true}[0],
							},
						},
					},
					Volumes: []v1.Volume{
						{
							Name: "datadir",
							VolumeSource: v1.VolumeSource{
								EmptyDir: &v1.EmptyDirVolumeSource{},
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