module github.com/ovirt/csi-driver

go 1.16

require (
	github.com/container-storage-interface/spec v1.2.0
	github.com/golang/protobuf v1.5.2
	github.com/kubernetes-csi/csi-lib-utils v0.7.0
	github.com/onsi/ginkgo v1.16.4
	github.com/onsi/gomega v1.13.0
	github.com/openshift/api v0.0.0-20210706092853-b63d499a70ce
	github.com/ovirt/go-ovirt v0.0.0-20201023070830-77e357c438d5
	github.com/pkg/errors v0.9.1
	golang.org/x/net v0.0.0-20210428140749-89ef3d95e781
	google.golang.org/grpc v1.29.1
	gopkg.in/yaml.v2 v2.4.0
	k8s.io/apimachinery v0.21.2
	k8s.io/client-go v0.21.2
	k8s.io/klog v1.0.0
	k8s.io/utils v0.0.0-20210527160623-6fdb442a123b
	sigs.k8s.io/controller-runtime v0.9.2
)
