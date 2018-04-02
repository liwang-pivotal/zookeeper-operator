package controller

import (
	"time"
	"fmt"
	log "github.com/sirupsen/logrus"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/liwang-pivotal/zookeeper-operator/pkg/apis/zookeeperoperator"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/util/errors"
)

var (
	logger = log.WithFields(log.Fields{
		"package": "controller",
	})
)

type CustomResourceController struct {
	ApiExtensionsClient *apiextensionsclient.Clientset
	DefaultOption       metav1.GetOptions
	crdClient           *rest.RESTClient
	namespace           string
}

func GetClientConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	return rest.InClusterConfig()
}


func New(kubeConfigFile, masterHost string, namespace string) (*CustomResourceController, error) {
	methodLogger := logger.WithFields(log.Fields{"method": "New"})

	// Create the client config. Use kubeconfig if given, otherwise assume in-cluster.
	config, err := GetClientConfig(kubeConfigFile)

	apiextensionsclientset, err := apiextensionsclient.NewForConfig(config)
	if err != nil {
		methodLogger.WithFields(log.Fields{
			"error":  err,
			"config": config,
			"client": apiextensionsclientset,
		}).Error("could not init Kubernetes client")
		return nil, err
	}

	crdClient, err := rest.RESTClientFor(config)
	if err != nil {
		methodLogger.WithFields(log.Fields{
			"Error":  err,
			"Client": crdClient,
			"Config": config,
		}).Error("Could not initialize CustomResourceDefinition Zookeeper cluster client")
		return nil, err
	}

	k := &CustomResourceController{
		crdClient:           crdClient,
		ApiExtensionsClient: apiextensionsclientset,
		namespace:           namespace,
	}
	methodLogger.Info("Initilized CustomResourceDefinition Zookeeper cluster client")

	return k, nil
}

func (c *CustomResourceController) CreateCustomResourceDefinition() (*apiextensionsv1beta1.CustomResourceDefinition, error) {

	methodLogger := logger.WithFields(log.Fields{"method": "CreateCustomResourceDefinition"})

	crd := &apiextensionsv1beta1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name: zookeeperoperator.Name,
		},
		Spec: apiextensionsv1beta1.CustomResourceDefinitionSpec{
			Group:   zookeeperoperator.GroupName,
			Version: zookeeperoperator.Version,
			Scope:   apiextensionsv1beta1.NamespaceScoped,
			Names: apiextensionsv1beta1.CustomResourceDefinitionNames{
				Plural: zookeeperoperator.ResourcePlural,
				Kind:   zookeeperoperator.ResourceKind,
			},
		},
	}

	_, err := c.ApiExtensionsClient.ApiextensionsV1beta1().CustomResourceDefinitions().Create(crd)
	if err != nil {
		methodLogger.WithFields(log.Fields{
			"error": err,
			"crd":   crd,
		}).Error("Error while creating CRD")
		return nil, err
	}

	// wait for CRD being established
	methodLogger.Debug("Created CRD, wating till its established")
	err = wait.Poll(500*time.Millisecond, 60*time.Second, func() (bool, error) {
		crd, err = c.ApiExtensionsClient.ApiextensionsV1beta1().CustomResourceDefinitions().Get(zookeeperoperator.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		for _, cond := range crd.Status.Conditions {
			switch cond.Type {
			case apiextensionsv1beta1.Established:
				if cond.Status == apiextensionsv1beta1.ConditionTrue {
					return true, err
				}
			case apiextensionsv1beta1.NamesAccepted:
				if cond.Status == apiextensionsv1beta1.ConditionFalse {
					fmt.Printf("Name conflict: %v\n", cond.Reason)
					methodLogger.WithFields(log.Fields{
						"error":  err,
						"crd":    crd,
						"reason": cond.Reason,
					}).Error("Naming Conflict with created CRD")
				}
			}
		}
		return false, err
	})
	if err != nil {
		deleteErr := c.ApiExtensionsClient.ApiextensionsV1beta1().CustomResourceDefinitions().Delete(zookeeperoperator.Name, nil)
		if deleteErr != nil {
			return nil, errors.NewAggregate([]error{err, deleteErr})
		}
		return nil, err
	}
	return crd, nil
}