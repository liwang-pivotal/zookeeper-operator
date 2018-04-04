package main

import (
	"fmt"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"net/http"

	log "github.com/sirupsen/logrus"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/liwang-pivotal/zookeeper-operator/pkg/kube"
	"github.com/liwang-pivotal/zookeeper-operator/pkg/controller"
	"github.com/liwang-pivotal/zookeeper-operator/pkg/processor"
)

var (
	appVersion = "0.0.1"

	printVersion bool
	baseImage    string
	kubeConfigFile  string
	masterHost   string

	metricListenAddress string
	metricListenPath    string

	namespace string

	logger = log.WithFields(log.Fields{
		"package": "main",
	})
)

func init() {
	flag.BoolVar(&printVersion, "version", false, "Show version and quit")
	flag.StringVar(&baseImage, "baseImage", "liwang0513/docker-zookeeper-kubernetes:1.0.0_0", "Base image to use when spinning up the zookeeper components.")
	flag.StringVar(&kubeConfigFile, "kubecfg-file", "", "Location of kubecfg file for access to kubernetes master service; --kube_master_url overrides the URL part of this; if neither this nor --kube_master_url are provided, defaults to service account tokens")
	flag.StringVar(&masterHost, "masterhost", "http://127.0.0.1:8001", "Full url to k8s api server")

	flag.StringVar(&metricListenAddress, "listen-address", ":9090", "The address to listen on for HTTP requests.")
	flag.StringVar(&metricListenPath, "metric-path", "/metrics", "Path under which the the prometheus metrics can be found")
	flag.StringVar(&namespace, "namespace", "", "Namespace on which the operator listens to CR, if not set then all Namespaces will be used")

	flag.Parse()
}


func Main() int {
	if printVersion {
		fmt.Println("zookeeper-operator", appVersion)
		os.Exit(0)
	}

	log.Info("zookeeper operator starting up!")

	// Print params configured
	log.Info("Using Variables:")
	log.Infof("   baseImage: %s", baseImage)

	//Creating osSignals first so we can exit at any time.
	osSignals := make(chan os.Signal, 2)
	signal.Notify(osSignals, syscall.SIGINT, syscall.SIGKILL, os.Interrupt)

	controlChannel := make(chan int, 2)

	go func() {
		for {
			select {
			case sig := <-osSignals:
				logger.WithFields(log.Fields{"signal": sig}).Info("Got Signal from OS shutting Down: ")
				controlChannel <- 1
				os.Exit(1)
			}
		}
	}()

	// Init
	kube, err := kube.New(kubeConfigFile, masterHost)
	if err != nil {
		logger.WithFields(log.Fields{
			"error":      err,
			"configFile": kubeConfigFile,
			"masterHost": masterHost,
		}).Fatal("Error initilizing kubernetes client ")
		return 1
	}


	controller, err := controller.New(kubeConfigFile, masterHost, namespace)
	if err != nil {
		logger.WithFields(log.Fields{
			"error":      err,
			"configFile": kubeConfigFile,
			"masterHost": masterHost,
		}).Fatal("Error initilizing ThirdPartyRessource (ZookeeperClusters) client ")
		return 1
	}

	controller.CreateCustomResourceDefinition()

	processor, err := processor.New(baseImage, *controller, controlChannel, *kube)
	processor.Run()

	http.Handle(metricListenPath, promhttp.Handler())
	//Blocking ListenAndServer, so we dont exit
	logger.Fatal(http.ListenAndServe(metricListenAddress, nil))
	logger.Info("Exiting now")

	return 0
}

func main() {
	os.Exit(Main())
}