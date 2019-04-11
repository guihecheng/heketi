package cmdexec

import (
	"encoding/xml"
	"fmt"

	"github.com/lpabon/godbc"

	"github.com/heketi/heketi/executors"
	"github.com/heketi/heketi/pkg/paths"
	rex "github.com/heketi/heketi/pkg/remoteexec"
)

func (s *CmdExecutor) SubvolumeCreate(host string, volume string,
	subvolume *executors.SubvolumeRequest) (*executors.Subvolume, error) {

	godbc.Require(host != "")
	godbc.Require(volume != "")
	godbc.Require(subvolume != nil)

	mountPath := paths.VolumeMountPoint(volume)

	cmds := []string{

		fmt.Sprintf("mkdir -p %v", mountPath),

		fmt.Sprintf("mount -t glusterfs %v:/%v %v", host, volume, mountPath),

		fmt.Sprintf("mkdir -p %v/%v", mountPath, subvolume.Name),

		fmt.Sprintf("%v volume quota %v limit-usage /%v %vGB",
			s.glusterCommand(), volume, subvolume.Name, subvolume.Size),

		fmt.Sprintf("umount %v", mountPath),
	}

	err := rex.AnyError(s.RemoteExecutor.ExecCommands(host, cmds,
		s.GlusterCliExecTimeout()))
	if err != nil {
		return nil, err
	}

	return &executors.Subvolume{}, nil
}

func (s *CmdExecutor) SubvolumeDestroy(host string, volume string,
	subvolume string) error {

	godbc.Require(host != "")
	godbc.Require(volume != "")
	godbc.Require(subvolume != "")

	mountPath := paths.VolumeMountPoint(volume)

	cmds := []string{
		fmt.Sprintf("mount -t glusterfs %v:/%v %v", host, volume, mountPath),
	}

	err := rex.AnyError(s.RemoteExecutor.ExecCommands(host, cmds,
		s.GlusterCliExecTimeout()))
	if err != nil {
		logger.LogError("Unable to mount volume %v: %v", volume, err)
	}

	cmds = []string{
		fmt.Sprintf("rm -rf %v/%v", mountPath, subvolume),
		fmt.Sprintf("umount %v", mountPath),
	}

	err = rex.AnyError(s.RemoteExecutor.ExecCommands(host, cmds,
		s.GlusterCliExecTimeout()))
	if err != nil {
		return logger.Err(fmt.Errorf("Unable to delete subvolume %v from volume %v: %v",
			subvolume, volume, err))
	}

	return nil
}

func (s *CmdExecutor) SubvolumeInfo(host string, volume string,
	subvolume string) (*executors.Subvolume, error) {

	godbc.Require(host != "")
	godbc.Require(volume != "")
	godbc.Require(subvolume != "")

	type CliOutput struct {
		OpRet      int                  `xml:"opRet"`
		OpErrno    int                  `xml:"opErrno"`
		OpErrStr   string               `xml:"opErrstr"`
		SubvolInfo executors.SubvolInfo `xml:"volQuota"`
	}

	command := []string{
		fmt.Sprintf("%v volume quota %v list /%v --xml", s.glusterCommand(), volume, subvolume),
	}

	//Get the xml output of subvolume info
	results, err := s.RemoteExecutor.ExecCommands(host, command,
		s.GlusterCliExecTimeout())
	if err := rex.AnyError(results, err); err != nil {
		return nil, fmt.Errorf("Unable to get subvolume info of subvolume name: %v, volume %v",
			subvolume, volume)
	}
	var subvolInfo CliOutput
	err = xml.Unmarshal([]byte(results[0].Output), &subvolInfo)
	if err != nil {
		return nil, fmt.Errorf("Unable to determine subvolume info of subvolume name: %v, volume %v",
			subvolume, volume)
	}
	logger.Debug("%+v\n", subvolInfo)
	return &subvolInfo.SubvolInfo.SubvolList[0], nil
}

func (s *CmdExecutor) SubvolumesInfo(host string, volume string) (*executors.SubvolInfo, error) {

	godbc.Require(host != "")
	godbc.Require(volume != "")

	type CliOutput struct {
		OpRet      int                  `xml:"opRet"`
		OpErrno    int                  `xml:"opErrno"`
		OpErrStr   string               `xml:"opErrstr"`
		SubvolInfo executors.SubvolInfo `xml:"volQuota"`
	}

	command := []string{
		fmt.Sprintf("%v volume quota %v list --xml", s.glusterCommand(), volume),
	}

	//Get the xml output of subvolume info
	results, err := s.RemoteExecutor.ExecCommands(host, command,
		s.GlusterCliExecTimeout())
	if err := rex.AnyError(results, err); err != nil {
		return nil, fmt.Errorf("Unable to get subvolume info of volume: %v", volume)
	}
	var subvolInfo CliOutput
	err = xml.Unmarshal([]byte(results[0].Output), &subvolInfo)
	if err != nil {
		return nil, fmt.Errorf("Unable to unmarshal subvolume info of volume %v", volume)
	}
	return &subvolInfo.SubvolInfo, nil
}
