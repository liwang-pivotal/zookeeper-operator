package k8sutil

import (
	"k8s.io/client-go/rest"
	"k8s.io/client-go/kubernetes"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"github.com/sirupsen/logrus"
	"strconv"
	"k8s.io/client-go/tools/clientcmd"
)

// K8sutil defines the kube object
type K8sutil struct {
	Config     *rest.Config
	CrdClient  genclient.Interface
	Kclient    kubernetes.Interface
	KubeExt    apiextensionsclient.Interface
	K8sVersion []int
	MasterHost string
}

// New creates a new instance of k8sutil
func New(kubeCfgFile, masterHost string) (*K8sutil, error) {

	crdClient, kubeClient, kubeExt, k8sVersion, err := newKubeClient(kubeCfgFile)

	if err != nil {
		logrus.Fatalf("Could not init Kubernetes client! [%s]", err)
	}

	k := &K8sutil{
		Kclient:    kubeClient,
		MasterHost: masterHost,
		K8sVersion: k8sVersion,
		CrdClient:  crdClient,
		KubeExt:    kubeExt,
	}

	return k, nil
}

func buildConfig(kubeCfgFile string) (*rest.Config, error) {
	if kubeCfgFile != "" {
		logrus.Infof("Using OutOfCluster k8s config with kubeConfigFile: %s", kubeCfgFile)
		config, err := clientcmd.BuildConfigFromFlags("", kubeCfgFile)
		if err != nil {
			panic(err.Error())
		}

		return config, nil
	}

	logrus.Info("Using InCluster k8s config")
	return rest.InClusterConfig()
}

func newKubeClient(kubeCfgFile string) (genclient.Interface, kubernetes.Interface, apiextensionsclient.Interface, []int, error) {

	// Create the client config. Use kubeconfig if given, otherwise assume in-cluster.
	Config, err := buildConfig(kubeCfgFile)
	if err != nil {
		panic(err)
	}

	// Create the kubernetes client
	clientSet, err := clientset.NewForConfig(Config)
	if err != nil {
		panic(err)
	}

	kubeClient, err := kubernetes.NewForConfig(Config)
	if err != nil {
		panic(err)
	}

	kubeExtCli, err := apiextensionsclient.NewForConfig(Config)
	if err != nil {
		panic(err)
	}

	version, err := kubeClient.ServerVersion()
	if err != nil {
		logrus.Error("Could not get version from api server:", err)
	}

	majorVer, _ := strconv.Atoi(version.Major)
	minorVer, _ := strconv.Atoi(version.Minor)
	k8sVersion := []int{majorVer, minorVer}

	return clientSet, kubeClient, kubeExtCli, k8sVersion, nil
}