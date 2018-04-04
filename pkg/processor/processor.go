package processor

import (
	log "github.com/sirupsen/logrus"
	"github.com/liwang-pivotal/zookeeper-operator/pkg/kube"
	"github.com/liwang-pivotal/zookeeper-operator/pkg/controller"
	"github.com/liwang-pivotal/zookeeper-operator/spec"
)

type Processor struct {
	baseBrokerImage    string
	crdController      controller.CustomResourceController
	watchEventsChannel chan spec.ZookeeperClusterWatchEvent
	control            chan int
	errors             chan error
	kube               kube.Kubernetes
}

func New(image string,
	crdClient controller.CustomResourceController,
	control chan int,
	kube kube.Kubernetes) (*Processor, error){
	p := &Processor{
		baseBrokerImage:    image,
		crdController:      crdClient,
		control:            control,
		errors:             make(chan error),
		kube:               kube,
	}
	log.Info("Created Processor")
	return p, nil
}

func (p *Processor) Run() error {
	log.Info("Running Processor")
	//p.watchEvents()
	return nil
}

func (p *Processor) watchEvents() {

	p.crdController.MonitorZookeeperEvents(p.watchEventsChannel, p.control)
	log.Debug("Watching Events")
	go func() {
		for {
			select {
			case event := <-p.watchEventsChannel:
				log.Info("recieved event through event channel", event.Type)
			case err := <-p.errors:
				log.WithField("error", err).Error("Recieved Error through error channel")
			case ctl := <-p.control:
				log.WithField("control-event", ctl).Warn("Recieved Something on Control Channel, shutting down")
				return
			}
		}
	}()
}