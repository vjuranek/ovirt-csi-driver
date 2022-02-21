package service

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	volumemanager "github.com/ovirt/csi-driver/pkg/utils"
	ovirtclient "github.com/ovirt/go-ovirt-client"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog"
	"k8s.io/utils/mount"
)

type NodeService struct {
	nodeId      string
	ovirtClient ovirtclient.Client
}

var NodeCaps = []csi.NodeServiceCapability_RPC_Type{
	csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
	csi.NodeServiceCapability_RPC_GET_VOLUME_STATS,
	csi.NodeServiceCapability_RPC_EXPAND_VOLUME,
}

func baseDevicePathByInterface(diskInterface ovirtclient.DiskInterface) (string, error) {
	switch diskInterface {
	case ovirtclient.DiskInterfaceVirtIO:
		return "/dev/disk/by-id/virtio-", nil
	case ovirtclient.DiskInterfaceVirtIOSCSI:
		return "/dev/disk/by-id/scsi-0QEMU_QEMU_HARDDISK_", nil
	}
	return "", errors.New("device type is unsupported")
}

func (n *NodeService) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	vId := req.VolumeId
	if vId == "" {
		return nil, fmt.Errorf("NodeStageVolumeRequest didn't contain required field VolumeId")
	}
	klog.Infof("Staging volume %s with %+v", vId, req)

	// get device attached to the node VM
	device, err := n.getDeviceByAttachmentId(ctx, vId, n.nodeId)
	if err != nil {
		klog.Errorf("Failed to fetch device by attachment-id for volume %s on node %s", vId, n.nodeId)
		return nil, err
	}

	isBlockDev := req.VolumeCapability.GetBlock() != nil

	// create FS on the device if it's file device
	fsType := ""
	if !isBlockDev {
		// file based storage
		// is there a filesystem on this device?
		err = createFsIfNeeded(req, device)
		if err != nil {
			return nil, err
		}
		fsType = req.VolumeCapability.GetMount().FsType
	} else {
		// block based storage
		klog.Infof("Volume %s is a block volume, will not create any FS", vId)
	}

	// create staging path if needed
	err = createMountDest(req.GetStagingTargetPath(), isBlockDev)
	if err != nil {
		return nil, err
	}

	// mount volume to the staging path
	err = mountStagingPath(req, device, fsType)
	if err != nil {
		klog.Errorf("Could not mount device %s to staging path %s", device, req.GetStagingTargetPath())
		return nil, err
	}

	return &csi.NodeStageVolumeResponse{}, nil
}

func (n *NodeService) NodeUnstageVolume(_ context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	stagingPath := req.GetStagingTargetPath()
	mounter := mount.New("")
	notMounted, err := mount.IsNotMountPoint(mounter, stagingPath)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("error checking if stagingPath %s is mounted: %w", stagingPath, err)
		}
		notMounted = true
	}
	if notMounted {
		klog.Infof("stagingPath %s already unmounted", stagingPath)
		return &csi.NodeUnstageVolumeResponse{}, nil
	}

	klog.Infof("Unmounting stagingPath %s", stagingPath)
	err = mounter.Unmount(stagingPath)
	if err != nil {
		klog.Infof("Failed to unmount")
		return nil, err
	}

	return &csi.NodeUnstageVolumeResponse{}, nil
}

func (n *NodeService) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	vId := req.VolumeId
	if vId == "" {
		return nil, fmt.Errorf("NodeStageVolumeRequest didn't contain required field VolumeId")
	}

	isBlockDev := req.VolumeCapability.GetBlock() != nil
	targetPath := req.GetTargetPath()
	stagingPath := req.GetStagingTargetPath()

	// if isBlockDev {
	// 	device, err := n.getDeviceByAttachmentId(ctx, vId, n.nodeId)
	// 	if err != nil {
	// 		klog.Errorf("Failed to fetch device by attachment-id for volume %s on node %s", vId, n.nodeId)
	// 		return nil, err
	// 	}
	// 	stagingPath = device
	// }

	_, err := os.Lstat(stagingPath)
	if os.IsNotExist(err) {
		return nil, fmt.Errorf("staging path %s doesn't exists", stagingPath)
	}

	err = createMountDest(targetPath, isBlockDev)
	if err != nil {
		return nil, err
	}

	fsType := ""
	if !isBlockDev {
		fsType = req.VolumeCapability.GetMount().FsType
	}

	klog.Infof("Mounting stagingPath %s, on targetPath: %s with FS type: %s", stagingPath, targetPath, fsType)
	mounter := mount.New("")
	err = mounter.Mount(stagingPath, targetPath, fsType, []string{"bind"})
	if err != nil {
		klog.Errorf("Failed mounting %v", err)
		return nil, err
	}

	return &csi.NodePublishVolumeResponse{}, nil
}

func (n *NodeService) NodeUnpublishVolume(_ context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	mounter := mount.New("")
	klog.Infof("Unmounting %s", req.GetTargetPath())
	err := mounter.Unmount(req.GetTargetPath())
	if err != nil {
		klog.Infof("Failed to unmount")
		return nil, err
	}

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (n *NodeService) NodeGetVolumeStats(_ context.Context, req *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	if len(req.VolumeId) == 0 {
		return nil, status.Error(codes.InvalidArgument, "NodeGetVolumeStats volume ID was empty")
	}

	if len(req.VolumePath) == 0 {
		return nil, status.Error(codes.InvalidArgument, "NodeGetVolumeStats volume path was empty")
	}

	_, err := os.Lstat(req.VolumePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, status.Errorf(codes.NotFound, "Path %s does not exist", req.VolumePath)
		}
		return nil, status.Errorf(codes.Internal, "Unknown error when getting stats on %s: %v", req.VolumePath, err)
	}

	isBlock, err := volumemanager.IsBlockDevice(req.VolumePath)

	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to determine whether %s is block device: %v", req.VolumePath, err)
	}

	// If volume is a block device, return only size in bytes.
	if isBlock {
		bcap, err := volumemanager.GetBlockSizeBytes(req.VolumePath)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "Failed to get block size on path %s: %v", req.VolumePath, err)
		}
		return &csi.NodeGetVolumeStatsResponse{
			Usage: []*csi.VolumeUsage{
				{
					Unit:  csi.VolumeUsage_BYTES,
					Total: bcap,
				},
			},
		}, nil
	}

	// We assume filesystem presence on volume as raw block device is ruled out and try to get fs stats
	available, capacity, used, inodesFree, inodes, inodesUsed, err := volumemanager.StatFS(req.VolumePath)

	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get fs info on path %s: %v", req.VolumePath, err)
	}

	return &csi.NodeGetVolumeStatsResponse{
		Usage: []*csi.VolumeUsage{
			{
				Unit:      csi.VolumeUsage_BYTES,
				Available: available,
				Total:     capacity,
				Used:      used,
			},
			{
				Unit:      csi.VolumeUsage_INODES,
				Available: inodesFree,
				Total:     inodes,
				Used:      inodesUsed,
			},
		},
	}, nil
}

func (n *NodeService) NodeExpandVolume(_ context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	volumePath := req.GetVolumePath()
	if len(volumePath) == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume path must be provided")
	}
	volumeCapability := req.GetVolumeCapability()
	if len(volumePath) == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume capability must be provided")
	}
	var resizeCmd string
	fsType := volumeCapability.GetMount().FsType
	if strings.HasPrefix(fsType, "ext") {
		resizeCmd = "resize2fs"
	} else if strings.HasPrefix(fsType, "xfs") {
		resizeCmd = "xfs_growfs"
	} else {
		return nil, status.Error(codes.InvalidArgument, "fsType is neither xfs or ext[234]")
	}
	klog.Infof("Resizing filesystem %s mounted on %s with %s", fsType, volumePath, resizeCmd)

	device, err := getDeviceByMountPoint(volumePath)
	if err != nil {
		return nil, err
	}

	cmd := exec.Command(resizeCmd, device)
	err = cmd.Run()
	var exitError *exec.ExitError
	if err != nil && errors.As(err, &exitError) {
		return nil, status.Error(codes.Internal, err.Error()+" resize failed with "+exitError.Error())
	}

	klog.Infof("Resized %s filesystem on device %s)", fsType, device)
	return &csi.NodeExpandVolumeResponse{}, nil
}

func (n *NodeService) NodeGetInfo(context.Context, *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{NodeId: n.nodeId}, nil
}

func (n *NodeService) NodeGetCapabilities(context.Context, *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	caps := make([]*csi.NodeServiceCapability, 0, len(NodeCaps))
	for _, c := range NodeCaps {
		caps = append(
			caps,
			&csi.NodeServiceCapability{
				Type: &csi.NodeServiceCapability_Rpc{
					Rpc: &csi.NodeServiceCapability_RPC{
						Type: c,
					},
				},
			},
		)
	}
	return &csi.NodeGetCapabilitiesResponse{Capabilities: caps}, nil
}

func mountStagingPath(req *csi.NodeStageVolumeRequest, device string, fsType string) error {
	stagingPath := req.GetStagingTargetPath()
	klog.Infof("Mounting devicePath %s, on stagingPath: %s with FS type: %s",
		device, stagingPath, fsType)

	mountOptions := []string{}
	if req.VolumeCapability.GetBlock() != nil {
		mountOptions = []string{"bind"}
	}

	mounter := mount.New("")
	err := mounter.Mount(device, stagingPath, fsType, mountOptions)
	if err != nil {
		klog.Errorf("Failed mounting %v", err)
		return err
	}

	return nil
}

func createFsIfNeeded(req *csi.NodeStageVolumeRequest, device string) error {
	filesystem, err := getDeviceInfo(device)
	if err != nil {
		klog.Errorf("Failed to fetch device info for device %s on volume %s", device, req.VolumeId)
		return err
	}

	if filesystem == "" {
		// no FS, create it
		fsType := req.VolumeCapability.GetMount().FsType
		klog.Infof("Creating FS %s on device %s", fsType, device)
		err := makeFS(device, fsType)
		if err != nil {
			klog.Errorf("Could not create filesystem %s on %s", fsType, device)
			return err
		}
	} else {
		klog.Infof("Detected fs %s, will not create any FS", filesystem)
	}

	return nil
}

func createMountDest(mountDest string, isBlockDevice bool) error {
	_, err := os.Stat(mountDest)
	if err == nil {
		return nil
	}

	if isBlockDevice {
		file, err := os.OpenFile(mountDest, os.O_CREATE, os.FileMode(0644))
		if err != nil {
			if !os.IsExist(err) {
				return status.Errorf(codes.Internal, "Failed to create targetPath %s, err: %v", mountDest, err)
			}
		}
		defer file.Close()
	} else {
		err := os.MkdirAll(mountDest, 0644)
		if err != nil {
			return errors.New(err.Error())
		}
	}
	return nil
}

func (n *NodeService) getDeviceByAttachmentId(ctx context.Context, volumeID, nodeID string) (string, error) {
	attachment, err := diskAttachmentByVmAndDisk(ctx, n.ovirtClient, nodeID, volumeID)
	if err != nil {
		return "", fmt.Errorf("failed finding disk attachment, error: %w", err)
	}
	if attachment == nil {
		return "", fmt.Errorf("attachment wasn't found for VM %s", nodeID)
	}

	klog.Infof("Extracting pvc volume name %s", volumeID)
	disk, err := attachment.Disk()
	if err != nil {
		if isNotFound(err) {
			return "", fmt.Errorf("disk was not found for attachment %s, error: %w", attachment.ID(), err)
		}
		return "", fmt.Errorf("error while retrieving disk from attachment, error: %w", err)
	}
	klog.Infof("Extracted disk ID from PVC %s", disk.ID())

	baseDevicePath, err := baseDevicePathByInterface(attachment.DiskInterface())
	if err != nil {
		return "", err
	}

	// verify the device path exists
	device := baseDevicePath + disk.ID()
	_, err = os.Stat(device)
	if err == nil {
		klog.Infof("Device path %s exists", device)
		return device, nil
	}

	if os.IsNotExist(err) {
		// try with short disk ID, where the serial ID is only 20 chars long (controlled by udev)
		shortDevice := baseDevicePath + disk.ID()[:20]
		_, err = os.Stat(shortDevice)
		if err == nil {
			klog.Infof("Device path %s exists", shortDevice)
			return shortDevice, nil
		}
	}
	klog.Errorf("Device path for disk ID %s does not exists", disk.ID())
	return "", errors.New("device was not found")
}

// getDeviceInfo will return the first Device which is a partition and its filesystem.
// if the given Device disk has no partition then an empty zero valued device will return
func getDeviceInfo(device string) (string, error) {
	devicePath, err := filepath.EvalSymlinks(device)
	if err != nil {
		klog.Errorf("Unable to evaluate symlink for device %s", device)
		return "", errors.New(err.Error())
	}

	klog.Info("lsblk -nro FSTYPE ", devicePath)
	cmd := exec.Command("lsblk", "-nro", "FSTYPE", devicePath)
	out, err := cmd.Output()
	exitError, incompleteCmd := err.(*exec.ExitError)
	if err != nil && incompleteCmd {
		return "", errors.New(err.Error() + "lsblk failed with " + string(exitError.Stderr))
	}

	reader := bufio.NewReader(bytes.NewReader(out))
	line, _, err := reader.ReadLine()
	if err != nil {
		klog.Errorf("Error occured while trying to read lsblk output")
		return "", err
	}
	return string(line), nil
}

func makeFS(device string, fsType string) error {
	// caution, use force flag when creating the filesystem if it doesn't exit.
	klog.Infof("Mounting device %s, with FS %s", device, fsType)

	var cmd *exec.Cmd
	var stdout, stderr bytes.Buffer
	if strings.HasPrefix(fsType, "ext") {
		cmd = exec.Command("mkfs", "-F", "-t", fsType, device)
	} else if strings.HasPrefix(fsType, "xfs") {
		cmd = exec.Command("mkfs", "-t", fsType, "-f", device)
	} else {
		return errors.New(fsType + " is not supported, only xfs and ext are supported")
	}
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	exitError, incompleteCmd := err.(*exec.ExitError)
	if err != nil && incompleteCmd {
		klog.Errorf("stdout: %s", string(stdout.Bytes()))
		klog.Errorf("stderr: %s", string(stderr.Bytes()))
		return errors.New(err.Error() + " mkfs failed with " + exitError.Error())
	}

	return nil
}

func getDeviceByMountPoint(mp string) (string, error) {
	out, err := exec.Command("findmnt", "-nfc", mp).Output()
	if err != nil {
		return "", fmt.Errorf("error: %v\n", err)
	}

	s := strings.Fields(string(out))
	if len(s) < 2 {
		return "", fmt.Errorf("could not parse command output: >%s<", string(out))
	}
	return s[1], nil
}
