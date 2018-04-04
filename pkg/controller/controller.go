package controller

import (
	"time"
	"fmt"
	"reflect"

	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/rest"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/liwang-pivotal/zookeeper-operator/spec"
	log "github.com/sirupsen/logrus"
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

	crdClient, err := newCRDClient(config)
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
			Name: spec.CRDFullName,
		},
		Spec: apiextensionsv1beta1.CustomResourceDefinitionSpec{
			Group:   spec.CRDGroupName,
			Version: spec.CRDVersion,
			Scope:   apiextensionsv1beta1.NamespaceScoped,
			Names: apiextensionsv1beta1.CustomResourceDefinitionNames{
				Plural: spec.CRDRessourcePlural,
				Kind:   reflect.TypeOf(spec.ZookeeperCluster{}).Name(),
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
		crd, err = c.ApiExtensionsClient.ApiextensionsV1beta1().CustomResourceDefinitions().Get(spec.CRDFullName, metav1.GetOptions{})
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
		deleteErr := c.ApiExtensionsClient.ApiextensionsV1beta1().CustomResourceDefinitions().Delete(spec.CRDFullName, nil)
		if deleteErr != nil {
			return nil, errors.NewAggregate([]error{err, deleteErr})
		}
		return nil, err
	}
	return crd, nil
}

func newCRDClient(config *rest.Config) (*rest.RESTClient, error) {

	var cdrconfig *rest.Config
	cdrconfig = config
	configureConfig(cdrconfig)

	crdClient, err := rest.RESTClientFor(cdrconfig)
	if err != nil {
		panic(err)
	}

	return crdClient, nil
}

func configureConfig(cfg *rest.Config) error {
	scheme := runtime.NewScheme()

	if err := spec.AddToScheme(scheme); err != nil {
		return err
	}

	cfg.GroupVersion = &spec.SchemeGroupVersion
	cfg.APIPath = "/apis"
	cfg.ContentType = runtime.ContentTypeJSON
	cfg.NegotiatedSerializer = serializer.DirectCodecFactory{CodecFactory: serializer.NewCodecFactory(scheme)}

	return nil
}


func (c *CustomResourceController) MonitorZookeeperEvents(eventsChannel chan spec.ZookeeperClusterWatchEvent, signalChannel chan int) {
	methodLogger := logger.WithFields(log.Fields{"method": "MonitorZookeeperEvents"})
	methodLogger.Info("Starting Monitoring")

	stop := make(chan struct{}, 1)
	source := cache.NewListWatchFromClient(
		c.crdClient,
		spec.CRDRessourcePlural,
		c.namespace,
		fields.Everything())

	store, controller := cache.NewInformer(
		source,

		&spec.ZookeeperCluster{},

		// resyncPeriod
		// Every resyncPeriod, all resources in the cache will retrigger events.
		// Set to 0 to disable the resync.
		0,

		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				cluster := obj.(*spec.ZookeeperCluster)
				methodLogger.WithFields(log.Fields{"watchFunction": "ADDED",}).Info(spec.PrintCluster(cluster))
				var event spec.ZookeeperClusterWatchEvent
				event.Type = "ADDED"
				event.Object = *cluster
				eventsChannel <- event
			},

			UpdateFunc: func(old, new interface{}) {
				oldCluster := old.(*spec.ZookeeperCluster)
				newCluster := new.(*spec.ZookeeperCluster)
				methodLogger.WithFields(log.Fields{
					"eventType": "UPDATED",
					"old":       spec.PrintCluster(oldCluster),
					"new":       spec.PrintCluster(newCluster),
				}).Info("Recieved Update Event")
				var event spec.ZookeeperClusterWatchEvent
				//TODO refactor this. use old/new in EventChannel
				event.Type = "UPDATED"
				event.Object = *newCluster
				event.OldObject = *oldCluster
				eventsChannel <- event
			},

			DeleteFunc: func(obj interface{}) {
				cluster := obj.(*spec.ZookeeperCluster)
				methodLogger.WithFields(log.Fields{"watchFunction": "DELETED",}).Info(spec.PrintCluster(cluster))
				var event spec.ZookeeperClusterWatchEvent
				event.Type = "DELETED"
				event.Object = *cluster
				eventsChannel <- event
			},
		})

	// the controller run starts the event processing loop
	go controller.Run(stop)
	methodLogger.Info(store)

	go func() {
		select {
		case <-signalChannel:
			methodLogger.Warn("recieved shutdown signal, stopping informer")
			close(stop)
		}
	}()
}

