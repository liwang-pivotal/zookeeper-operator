package main

import (
	"fmt"
	"flag"
	"os"

	"github.com/sirupsen/logrus"

	"github.com/liwang-pivotal/zookeeper-operator/pkg/k8sutil"
)

var (
	appVersion = "0.0.1"

	printVersion bool
	baseImage    string
	kubeCfgFile  string
	masterHost   string
)

func init() {
	flag.BoolVar(&printVersion, "version", false, "Show version and quit")
	flag.StringVar(&baseImage, "baseImage", "liwang0513/docker-zookeeper-kubernetes:1.0.0_0", "Base image to use when spinning up the zookeeper components.")
	flag.StringVar(&kubeCfgFile, "kubecfg-file", "", "Location of kubecfg file for access to kubernetes master service; --kube_master_url overrides the URL part of this; if neither this nor --kube_master_url are provided, defaults to service account tokens")
	flag.StringVar(&masterHost, "masterhost", "http://127.0.0.1:8001", "Full url to k8s api server")
	flag.Parse()
}


func Main() int {
	if printVersion {
		fmt.Println("zookeeper-operator", appVersion)
		os.Exit(0)
	}

	logrus.Info("zookeeper operator starting up!")

	// Print params configured
	logrus.Info("Using Variables:")
	logrus.Infof("   baseImage: %s", baseImage)

	// Init
	k8sclient, err := k8sutil.New(kubeCfgFile, masterHost)
	if err != nil {
		logrus.Error("Could not init k8sclient! ", err)
		return 1
	}

	controller, err := controller.New("zookeeper-cluster", k8sclient)
	if err != nil {
		logrus.Error("Could not init Controller! ", err)
		return 1
	}
}

func main() {
	os.Exit(Main())
}