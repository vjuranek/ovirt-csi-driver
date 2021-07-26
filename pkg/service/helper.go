package service

import (
	"fmt"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	utilrand "k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/klog"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"time"

	configv1 "github.com/openshift/api/config/v1"
	ovirtsdk "github.com/ovirt/go-ovirt"
)

const (
	globalInfrastuctureName = "cluster"
)

func diskAttachmentByVmAndDisk(connection *ovirtsdk.Connection, vmId string, diskId string) (*ovirtsdk.DiskAttachment, error) {
	vmService := connection.SystemService().VmsService().VmService(vmId)
	attachments, err := vmService.DiskAttachmentsService().List().Send()
	if err != nil {
		return nil, fmt.Errorf("failed to get disk attachment by disk %s for VM %s, error: %w", diskId, vmId, err)
	}

	for _, attachment := range attachments.MustAttachments().Slice() {
		if diskId == attachment.MustDisk().MustId() {
			return attachment, nil
		}
	}
	return nil, nil
}

func findDiskAttachmentByDiskInVMs(conn *ovirtsdk.Connection, diskId string, vms []*ovirtsdk.Vm) (*ovirtsdk.DiskAttachment, error) {
	for _, vm := range vms {
		diskAttachment, err := diskAttachmentByVmAndDisk(conn, vm.MustId(), diskId)
		if err != nil {
			klog.Error(err)
			return nil, errors.Wrap(err, "failed finding disk attachments")
		}
		if diskAttachment != nil {
			return diskAttachment, nil
		}
	}
	return nil, nil
}

func findDiskAttachmentByDiskInCluster(ctx context.Context, c client.Client, conn *ovirtsdk.Connection, diskId string) (*ovirtsdk.DiskAttachment, error) {
	tag, err := getClusterTag(ctx, c)
	if err != nil {
		klog.Error(err)
		return nil, errors.Wrap(err, "failed finding openshift cluster tag")
	}
	vms, err := getVmsWithTag(conn, tag)
	if err != nil {
		klog.Error(err)
		return nil, errors.Wrap(err, "failed searching VMs")
	}
	return findDiskAttachmentByDiskInVMs(conn, diskId, vms)
}

func getVmsWithTag(connection *ovirtsdk.Connection, tag string) ([]*ovirtsdk.Vm, error) {
	searchTerm := fmt.Sprintf("tag=%s", tag)
	vmsWithTag, err := connection.SystemService().VmsService().List().Search(searchTerm).Send()
	if err != nil {
		return nil, fmt.Errorf("faild listing VMs with tag %s, error: %w", tag, err)
	}
	return vmsWithTag.MustVms().Slice(), nil
}

func getClusterTag(ctx context.Context, c client.Client) (string, error) {
	infra := &configv1.Infrastructure{}
	objKey := client.ObjectKey{Name: globalInfrastuctureName}
	if err := c.Get(ctx, objKey, infra); err != nil {
		return "", errors.Wrap(err, "error getting infrastucture data")
	}
	return infra.Status.InfrastructureName, nil
}

func expandDiskByDiskDiskAttachment(ctx context.Context, conn *ovirtsdk.Connection, diskAttachment *ovirtsdk.DiskAttachment, bytes int64) error {
	correlationID := fmt.Sprintf("image_transfer_%s", utilrand.String(5))

	disk, err := getDiskFromDiskAttachment(conn, diskAttachment)
	if err != nil {
		return fmt.Errorf("failed to expand disk attachment %s, error: %w", diskAttachment.MustId(), err)
	}
	// Check That the disk is ready for expansion
	if err = waitForDiskStatusOk(ctx, conn, disk.MustId()); err != nil {
		return fmt.Errorf("failed to expand disk attachment %s, error: %w", diskAttachment.MustId(), err)
	}

	diskAttachment.MustDisk().SetProvisionedSize(bytes)
	_, err = conn.SystemService().VmsService().VmService(diskAttachment.MustVm().MustId()).
		DiskAttachmentsService().AttachmentService(diskAttachment.MustId()).
		Update().DiskAttachment(diskAttachment).
		Query("correlation_id", correlationID).
		Send()
	if err != nil {
		return fmt.Errorf("failed to expand disk attachment %s, error: %w", diskAttachment.MustId(), err)
	}
	finished, err := checkJobFinished(ctx, conn, correlationID)
	if err != nil {
		return fmt.Errorf("failed to expand disk attachment %s, error: %w", diskAttachment.MustId(), err)
	}
	if !finished {
		return fmt.Errorf("expand disk attachment %s expansion job didn't finish, error: %w",
			diskAttachment.MustId(), err)
	}
	if err = waitForDiskStatusOk(ctx, conn, disk.MustId()); err != nil {
		return fmt.Errorf("failed to expand disk attachment %s, error: %w", diskAttachment.MustId(), err)
	}
	return nil
}

func getDiskFromDiskAttachment(conn *ovirtsdk.Connection, diskAttachment *ovirtsdk.DiskAttachment) (*ovirtsdk.Disk, error) {
	disk, err := conn.FollowLink(diskAttachment.MustDisk())
	if err != nil {
		return nil, fmt.Errorf("failed to following disk attachment %v disk href %w", diskAttachment.MustId(), err)
	}
	d, ok := disk.(*ovirtsdk.Disk)
	if !ok {
		return nil, errors.New("couldn't retrieve disk from attachment")
	}
	return d, nil
}

func getDiskByName(conn *ovirtsdk.Connection, diskName string) (*ovirtsdk.Disk, error) {
	diskByName, err := conn.SystemService().DisksService().List().Search(diskName).Send()
	if err != nil {
		return nil, err
	}
	disks, ok := diskByName.Disks()
	if !ok {
		return nil, fmt.Errorf(
			"error, failed searching for disk with name %s", diskName)
	}
	if len(disks.Slice()) > 1 {
		return nil, fmt.Errorf(
			"error, found more then one disk with the name %s, please use ID instead", diskName)
	}
	if len(disks.Slice()) == 0 {
		return nil, nil
	}
	return disks.Slice()[0], nil
}

func waitForDiskStatusOk(ctx context.Context, conn *ovirtsdk.Connection, diskID string) error {
	var lastStatus ovirtsdk.DiskStatus
	diskService := conn.SystemService().DisksService().DiskService(diskID)
	for {
		req, err := diskService.Get().Send()
		if err != nil {
			return fmt.Errorf("failed getting disk %s, error %w", diskID, err)
		}
		disk, ok := req.Disk()
		if !ok {
			return fmt.Errorf("disk with ID %s doesn't exist", diskID)
		}
		if disk.MustStatus() == ovirtsdk.DISKSTATUS_OK {
			return nil
		} else {
			lastStatus = disk.MustStatus()
		}
		select {
		case <-time.After(5 * time.Second):
		case <-ctx.Done():
			return fmt.Errorf(
				"timeout while waiting for disk to be ok, last status: %s, error: %w)", lastStatus, ctx.Err())
		}
	}
}

func checkJobFinished(ctx context.Context, conn *ovirtsdk.Connection, correlationID string) (bool, error) {
	for {
		jobResp, err := conn.SystemService().JobsService().List().
			Search(fmt.Sprintf("correlation_id=%s", correlationID)).Send()
		if err != nil {
			return false, fmt.Errorf("faild searching jobs, error: %w", err)
		}
		if jobSlice, ok := jobResp.Jobs(); ok {
			if len(jobSlice.Slice()) == 0 {
				// Job wasn't found either an old job which has been cleaned or never started
				klog.Infof("didn't find job with correlation_id %s", correlationID)
				return true, nil
			}
			for _, job := range jobSlice.Slice() {
				if status, _ := job.Status(); status != ovirtsdk.JOBSTATUS_STARTED {
					return true, nil
				}
			}
		}
		select {
		case <-time.After(5 * time.Second):
		case <-ctx.Done():
			return false, fmt.Errorf(
				"timeout while waiting for job with correlation_id %s to finish, error %w", correlationID, ctx.Err())
		}
	}
}
