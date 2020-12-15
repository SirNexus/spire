package csi

import (
	"context"
	"os"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/spiffe/spire/pkg/common/api/rpccontext"
	"github.com/spiffe/spire/pkg/common/version"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	msRdOnly uintptr = 1    // LINUX MS_RDONLY
	msBind   uintptr = 4096 // LINUX MS_BIND
)

func Register(s *grpc.Server, p *Plugin) {
	csi.RegisterIdentityServer(s, p)
	csi.RegisterNodeServer(s, p)
}

type Config struct {
	SocketDir string
}

type Plugin struct {
	csi.UnimplementedIdentityServer
	csi.UnimplementedNodeServer

	config Config
}

func New(config Config) *Plugin {
	return &Plugin{
		config: config,
	}
}

/////////////////////////////////////////////////////////////////////////////
// Identity Server
/////////////////////////////////////////////////////////////////////////////

func (p *Plugin) GetPluginInfo(ctx context.Context, req *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{
		Name:          "agent.spire.csi.spiffe.io",
		VendorVersion: version.Version(),
	}, nil
}

func (p *Plugin) GetPluginCapabilities(ctx context.Context, req *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	// Only the Node server is implemented. No other capabilities are available.
	return &csi.GetPluginCapabilitiesResponse{}, nil
}

func (p *Plugin) Probe(ctx context.Context, req *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	return &csi.ProbeResponse{}, nil
}

/////////////////////////////////////////////////////////////////////////////
// Node Server implementation
/////////////////////////////////////////////////////////////////////////////

func (p *Plugin) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	rpccontext.Logger(ctx).WithField("req", req.String()).Warn("Not implemented")
	return nil, status.Errorf(codes.Unimplemented, "method NodeStageVolume not implemented")
}

func (p *Plugin) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	rpccontext.Logger(ctx).WithField("req", req.String()).Warn("Not implemented")
	return nil, status.Errorf(codes.Unimplemented, "method NodeUnstageVolume not implemented")
}

func (p *Plugin) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	rpccontext.Logger(ctx).WithField("req", req.String()).Debug("Called")
	resp, err := p.nodePublishVolume(ctx, req)
	if err != nil {
		rpccontext.Logger(ctx).WithError(err).Error("Failed")
		return nil, err
	}
	rpccontext.Logger(ctx).WithField("resp", resp.String()).Debug("OK")
	return resp, nil
}

func (p *Plugin) nodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
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

	// Bind mount the agent socekt directory to the target path
	if err := unix.Mount(p.config.SocketDir, req.TargetPath, "none", msBind|msRdOnly, ""); err != nil {
		return nil, status.Errorf(codes.Internal, "unable to mount %q to %q: %v", p.config.SocketDir, req.TargetPath, err)
	}
	return &csi.NodePublishVolumeResponse{}, nil
}

func (p *Plugin) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	rpccontext.Logger(ctx).WithField("req", req.String()).Debug("Called")
	resp, err := p.nodeUnpublishVolume(ctx, req)
	if err != nil {
		rpccontext.Logger(ctx).WithError(err).Error("Failed")
		return nil, err
	}
	rpccontext.Logger(ctx).WithField("resp", resp.String()).Debug("OK")
	return resp, nil
}

func (p *Plugin) nodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
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

func (p *Plugin) NodeGetVolumeStats(ctx context.Context, req *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	rpccontext.Logger(ctx).WithField("req", req.String()).Warn("Not implemented")
	return nil, status.Errorf(codes.Unimplemented, "method NodeGetVolumeStats not implemented")
}

func (p *Plugin) NodeExpandVolume(ctx context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	rpccontext.Logger(ctx).WithField("req", req.String()).Warn("Not implemented")
	return nil, status.Errorf(codes.Unimplemented, "method NodeExpandVolume not implemented")
}

func (p *Plugin) NodeGetCapabilities(ctx context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	rpccontext.Logger(ctx).WithField("req", req.String()).Warn("Not implemented")
	return &csi.NodeGetCapabilitiesResponse{}, nil
}

func (p *Plugin) NodeGetInfo(ctx context.Context, req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	rpccontext.Logger(ctx).WithField("req", req.String()).Debug("Called")
	resp, err := p.nodeGetInfo(ctx, req)
	if err != nil {
		rpccontext.Logger(ctx).WithError(err).Error("Failed")
		return nil, err
	}
	rpccontext.Logger(ctx).WithField("resp", resp.String()).Debug("OK")
	return resp, nil
}

func (p *Plugin) nodeGetInfo(ctx context.Context, req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{
		// TODO: fill in with agent SPIFFE ID
		NodeId:            "spiffe://example.org/node",
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
