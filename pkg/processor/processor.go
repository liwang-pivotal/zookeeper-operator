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
		watchEventsChannel: make(chan spec.ZookeeperClusterWatchEvent, 100),
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
	p.watchEvents()
	return nil
}

func (p *Processor) watchEvents() {

	p.crdController.MonitorZookeeperEvents(p.watchEventsChannel, p.control)
	log.Info("Watching Events")
	go func() {
		for {
			select {
			case event := <-p.watchEventsChannel:
				log.Info("recieved event through event channel", event.Type)
				p.processEvent(event)
			case err := <-p.errors:
				log.WithField("error", err).Error("Recieved Error through error channel")
			case ctl := <-p.control:
				log.WithField("control-event", ctl).Warn("Recieved Something on Control Channel, shutting down")
				return
			}
		}
	}()
}

func (p *Processor) processEvent(currentEvent spec.ZookeeperClusterWatchEvent) {
	methodLogger := log.WithFields(log.Fields{
		"method":                "processEvent",
		"clusterName":           currentEvent.Object.Name,
		"ZookeeperClusterEventType": currentEvent.Type,
	})
	methodLogger.WithField("event-type", currentEvent.Type).Info("Caught new cluster event: ", currentEvent.Type)
	switch {
	case currentEvent.Type == "ADDED" || currentEvent.Type == "UPDATED":
		p.processZookeeperCluster(currentEvent.Object)

	case currentEvent.Type == "DELETED":
		p.deleteZookeeperCluster(currentEvent.Object)
	}
}

func (p *Processor) processZookeeperCluster(clusterSpec spec.ZookeeperCluster) {
	methodLogger := log.WithFields(log.Fields{
		"method":      "CreateZookeeperCluster",
		"clusterName": clusterSpec.ObjectMeta.Name,
	})

	err := kube.CreateCluster(clusterSpec, p.kube)
	if err != nil {
		methodLogger.WithField("error", err).Fatal("Cant create statefulset")
	}
}

func (p *Processor) deleteZookeeperCluster(clusterSpec spec.ZookeeperCluster) error {
	client := p.kube
	err := kube.DeleteCluster(clusterSpec, client)
	if err != nil {
		//Error while deleting, just resubmit event after wait time.
		return err
	}
	return nil
}
