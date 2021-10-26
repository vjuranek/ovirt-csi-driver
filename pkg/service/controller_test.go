package service_test

import (
	"context"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/ovirt/csi-driver/pkg/service"
	ovirtclient "github.com/ovirt/go-ovirt-client"
)

func TestVolumeCreation(t *testing.T) {
	helper := getMockHelper(t)
	controller := service.NewOvirtCSIDriver(helper.GetClient(), "test")

	createVolumeResponse, err := createTestVolume(helper, controller)
	if err != nil {
		t.Fatalf("failed to create test volume (%v)", err)
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

func TestDeleteVolume(t *testing.T) {
	helper := getMockHelper(t)
	controller := service.NewOvirtCSIDriver(helper.GetClient(), "test")
	createVolumeResponse, err := createTestVolume(helper, controller)
	if err != nil {
		t.Fatalf("failed to create test volume (%v)", err)
	}

	_, err = controller.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
		VolumeId: createVolumeResponse.Volume.VolumeId,
	})
	if err != nil {
		t.Fatalf("failed to delete volume (%v)", err)
	}
	diskList, err := helper.GetClient().ListDisks()
	if err != nil {
		t.Fatalf("failed to list disks (%v)", err)
	}
	if len(diskList) != 0 {
		t.Fatalf("disk wasn't deleted, number of disks on backend: %d", len(diskList))
	}
}

func TestDeleteNonExistentVolume(t *testing.T) {
	helper := getMockHelper(t)
	controller := service.NewOvirtCSIDriver(helper.GetClient(), "test")

	_, err := controller.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
		VolumeId: "doesn't exists",
	})
	if err != nil {
		t.Fatalf("deleting volume which doesn't exist failed (%v)", err)
	}
}

func TestControllerPublishVolume(t *testing.T) {
	helper := getMockHelper(t)
	publishRequest := &csi.ControllerPublishVolumeRequest{
		VolumeId: "",
		NodeId:   "",
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{
					FsType: "ext4",
				},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		},
		Readonly: true,
	}
	vmId, diskId, err := publishTestVolume(helper, publishRequest)
	if err != nil {
		t.Fatalf("failed to publish the volume (%v)", err)
	}

	attachments, err := helper.GetClient().ListDiskAttachments(vmId, ovirtclient.ContextStrategy(context.Background()))
	if err != nil {
		t.Fatalf("failed to list disk attachments (%v)", err)
	}
	if len(attachments) != 1 {
		t.Fatalf("incorrect number of disk attachmanets (%d)", len(attachments))
	}
	attachment := attachments[0]
	if attachment.DiskID() != diskId {
		t.Fatalf("incorrect disk ID: %s", attachment.DiskID())
	}
	disk, err := attachment.Disk()
	if err != nil {
		t.Fatalf("failed to get disk (%v)", err)
	}
	if disk.TotalSize() < 4096 {
		t.Fatalf("incorrect disk size on the backend: %d", disk.TotalSize())
	}
}

func TestControllerPublishVolumeTwice(t *testing.T) {
	helper := getMockHelper(t)

	publishRequest := &csi.ControllerPublishVolumeRequest{
		VolumeId: "",
		NodeId:   "",
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{
					FsType: "ext4",
				},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		},
		Readonly: true,
	}

	// Publish volume.
	_, _, err := publishTestVolume(helper, publishRequest)
	if err != nil {
		t.Fatalf("failed to publish the volume (%v)", err)
	}

	// Publish volume again, should succeed.
	_, _, err = publishTestVolume(helper, publishRequest)
	if err != nil {
		t.Fatalf("failed to publish volume which was already published (%v)", err)
	}
}

func createTestVolume(helper ovirtclient.TestHelper, controller *service.OvirtCSIDriver) (*csi.CreateVolumeResponse, error) {
	testStorageDomain, err := helper.GetClient().GetStorageDomain(helper.GetStorageDomainID())
	if err != nil {
		return nil, err
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
		return nil, err
	}

	return createVolumeResponse, nil
}

func publishTestVolume(helper ovirtclient.TestHelper, publishReq *csi.ControllerPublishVolumeRequest) (string, string, error) {
	if publishReq.NodeId == "" {
		vm, err := helper.GetClient().CreateVM(
			helper.GetClusterID(),
			helper.GetBlankTemplateID(),
			ovirtclient.CreateVMParams().MustWithName("test"))
		if err != nil {
			return "", "", err
		}
		publishReq.NodeId = vm.ID()
	}
	controller := service.NewOvirtCSIDriver(helper.GetClient(), publishReq.NodeId)
	if publishReq.VolumeId == "" {
		createVolumeResponse, err := createTestVolume(helper, controller)
		if err != nil {
			return "", "", err
		}
		publishReq.VolumeId = createVolumeResponse.Volume.VolumeId
	}

	_, err := controller.ControllerPublishVolume(context.Background(), publishReq)
	if err != nil {
		return "", "", err
	}

	return publishReq.NodeId, publishReq.VolumeId, nil
}
