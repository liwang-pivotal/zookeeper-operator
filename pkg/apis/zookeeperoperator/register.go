package zookeeperoperator

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	ResourceKind   = "ZookeeperCluster"
	ResourcePlural = "ZookeeperClusters"
	GroupName      = "liwang.pivotal.io"
	ShortName      = "zookeepercluster"
	Version        = "v1"
)

var (
	Name               = fmt.Sprintf("%s.%s", ResourcePlural, GroupName)
	SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: Version}
)
