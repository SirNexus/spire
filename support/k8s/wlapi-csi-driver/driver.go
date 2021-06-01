package main

import (
	"context"
	"os"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/spiffe/spire/pkg/common/version"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	pluginName = "wlapi-mounter.spire.spiffe.io"

	// Mount flags
	msRdOnly uintptr = 1    // LINUX MS_RDONLY
	msBind   uintptr = 4096 // LINUX MS_BIND
)

type Driver struct {
	csi.UnimplementedIdentityServer
	csi.UnimplementedNodeServer

	NodeID         string
	WorkloadAPIDir string
}

/////////////////////////////////////////////////////////////////////////////
// Identity Server
/////////////////////////////////////////////////////////////////////////////

func (d *Driver) GetPluginInfo(ctx context.Context, req *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{
		Name:          pluginName,
		VendorVersion: version.Version(),
	}, nil
}

func (d *Driver) GetPluginCapabilities(ctx context.Context, req *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	// Only the Node server is implemented. No other capabilities are available.
	return &csi.GetPluginCapabilitiesResponse{}, nil
}

func (d *Driver) Probe(ctx context.Context, req *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	return &csi.ProbeResponse{}, nil
}

/////////////////////////////////////////////////////////////////////////////
// Node Server implementation
/////////////////////////////////////////////////////////////////////////////

func (d *Driver) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	ephemeralMode := req.GetVolumeContext()["csi.storage.k8s.io/ephemeral"]

	// Validate request
	switch {
	case req.VolumeId == "":
		return nil, status.Error(codes.InvalidArgument, "request missing required volume id")
	case req.TargetPath == "":
		return nil, status.Error(codes.InvalidArgument, "request missing required target path")
	case req.VolumeCapability == nil:
		return nil, status.Error(codes.InvalidArgument, "request missing required volume capability")
	case req.VolumeCapability.AccessType == nil:
		return nil, status.Error(codes.InvalidArgument, "request missing required volume capability access type")
	case !isVolumeCapabilityPlainMount(req.VolumeCapability):
		return nil, status.Error(codes.InvalidArgument, "request volume capability access type must be mount")
	case req.VolumeCapability.AccessMode == nil:
		return nil, status.Error(codes.InvalidArgument, "request missing required volume capability access mode")
	case isVolumeCapabilityAccessModeReadOnly(req.VolumeCapability.AccessMode):
		return nil, status.Error(codes.InvalidArgument, "request missing required volume capability access mode")
	case ephemeralMode != "true":
		return nil, status.Error(codes.InvalidArgument, "only ephemeral volumes are supported")
	}

	// Create the target path (required by CSI interface)
	if err := os.Mkdir(req.TargetPath, 0777); err != nil && !os.IsExist(err) {
		return nil, status.Errorf(codes.Internal, "unable to create target path %q: %v", req.TargetPath, err)
	}
	// Bind mount the agent socket directory to the target path
	if err := unix.Mount(d.WorkloadAPIDir, req.TargetPath, "none", msBind|msRdOnly, ""); err != nil {
		return nil, status.Errorf(codes.Internal, "unable to mount %q: %v", req.TargetPath, err)
	}

	return &csi.NodePublishVolumeResponse{}, nil
}

func (d *Driver) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	// Validate request
	switch {
	case req.VolumeId == "":
		return nil, status.Error(codes.InvalidArgument, "request missing required volume id")
	case req.TargetPath == "":
		return nil, status.Error(codes.InvalidArgument, "request missing required target path")
	}

	if err := unix.Unmount(req.TargetPath, 0); err != nil {
		return nil, status.Errorf(codes.Internal, "unable to unmount %q: %v", req.TargetPath, err)
	}
	if err := os.Remove(req.TargetPath); err != nil {
		return nil, status.Errorf(codes.Internal, "unable to remove target path %q: %v", req.TargetPath, err)
	}

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (d *Driver) NodeGetCapabilities(ctx context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	return &csi.NodeGetCapabilitiesResponse{}, nil
}

func (d *Driver) NodeGetInfo(ctx context.Context, req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{
		NodeId:            d.NodeID,
		MaxVolumesPerNode: 1,
	}, nil
}

func isVolumeCapabilityPlainMount(volumeCapability *csi.VolumeCapability) bool {
	mount := volumeCapability.GetMount()
	switch {
	case mount == nil:
		return false
	case mount.FsType != "":
		return false
	case len(mount.MountFlags) != 0:
		return false
	}
	return true
}

func isVolumeCapabilityAccessModeReadOnly(accessMode *csi.VolumeCapability_AccessMode) bool {
	return accessMode.Mode == csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY
}
