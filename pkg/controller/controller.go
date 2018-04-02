package controller

import (
	"github.com/liwang-pivotal/zookeeper-operator/pkg/k8sutil"
	"github.com/sirupsen/logrus"
)

// Config defines properties of the controller
type Config struct {
	k8sclient *k8sutil.K8sutil
}

// Controller object
type Controller struct {
	Config
}

// New up a Controller
func New(name string, k8sclient *k8sutil.K8sutil) (*Controller, error) {

	c := &Controller{
		Config: Config{
			k8sclient: k8sclient,
		},
	}

	return c, nil
}

// Run gets the party started
func (c *Controller) Run() error {

	// Init TPR
	err := c.init()

	if err != nil {
		logrus.Error("Error in init(): ", err)
		return err
	}

	return nil
}

func (c *Controller) init() error {
	err := c.k8sclient.CreateKubernetesCustomResourceDefinition()
	if err != nil {
		return err
	}

	err = c.k8sclient.CreateNodeInitDaemonset("default")

	if err != nil {
		return err
	}

	return nil
}