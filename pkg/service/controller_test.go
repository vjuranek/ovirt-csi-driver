package service_test

import (
	"context"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/ovirt/csi-driver/pkg/service"
)

func TestVolumeCreation(t *testing.T) {
	helper := getMockHelper(t)
	controller := service.NewOvirtCSIDriver(helper.GetClient(), "test")
	testStorageDomain, err := helper.GetClient().GetStorageDomain(helper.GetStorageDomainID())
	if err != nil {
		t.Fatalf("failed to fetch test storage domain")
	}

	createVolumeResponse, err := controller.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name: "test",
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 4096,
			LimitBytes:    4096,
		},
		Parameters: map[string]string{
			"storageClass":      "ovirt-test-domain",
			"storageDomainName": testStorageDomain.Name(),
			"thinProvisioning":  "true",
		},
	})
	if err != nil {
		t.Fatalf("failed to create volume (%v)", err)
	}
	if createVolumeResponse.Volume.CapacityBytes < 4096 {
		t.Fatalf("created volume is too small (%v)", err)
	}
	diskList, err := helper.GetClient().ListDisks()
	if err != nil {
		t.Fatalf("failed to list disks (%v)", err)
	}
	if len(diskList) != 1 {
		t.Fatalf("incorrect number of disks created (%d)", len(diskList))
	}
	disk := diskList[0]
	if disk.TotalSize() < 4096 {
		t.Fatalf("incorrect disk size on the backend: %d", disk.TotalSize())
	}
}
