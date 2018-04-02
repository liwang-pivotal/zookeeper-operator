package controller

import (
	log "github.com/sirupsen/logrus"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/apimachinery/pkg/runtime/serializer"
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
