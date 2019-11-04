package cmdexec

import (
	"encoding/xml"
	"fmt"

	"github.com/lpabon/godbc"

	"github.com/heketi/heketi/executors"
	rex "github.com/heketi/heketi/pkg/remoteexec"
)

func (s *CmdExecutor) DirvolumeCreate(host string, volume string, mountpoint string,
	dirvolume *executors.DirvolumeRequest) (*executors.Dirvolume, error) {

	godbc.Require(host != "")
	godbc.Require(volume != "")
	godbc.Require(mountpoint != "")
	godbc.Require(dirvolume != nil)

	cmds := []string{
		fmt.Sprintf("findmnt %v/%v", mountpoint, volume),
	}

	err := rex.AnyError(s.RemoteExecutor.ExecCommands(host, cmds,
		s.GlusterCliExecTimeout()))
	if err != nil {
		return nil, err
	}

	cmds = []string{

		fmt.Sprintf("mkdir -p %v/%v/%v", mountpoint, volume, dirvolume.Name),

		fmt.Sprintf("%v volume quota %v limit-usage /%v %vGB",
			s.glusterCommand(), volume, dirvolume.Name, dirvolume.Size),

		fmt.Sprintf("%v volume set %v export-dir \"%v\"",
			s.glusterCommand(), volume, dirvolume.ExportDirStr),
	}

	err = rex.AnyError(s.RemoteExecutor.ExecCommands(host, cmds,
		s.GlusterCliExecTimeout()))
	if err != nil {
		return nil, err
	}

	return &executors.Dirvolume{}, nil
}

func (s *CmdExecutor) DirvolumeDestroy(host string, volume string, mountpoint string,
	dirvolume *executors.DirvolumeRequest) error {

	godbc.Require(host != "")
	godbc.Require(volume != "")
	godbc.Require(mountpoint != "")
	godbc.Require(dirvolume != nil)

	cmds := []string{
		fmt.Sprintf("findmnt %v/%v", mountpoint, volume),
	}

	err := rex.AnyError(s.RemoteExecutor.ExecCommands(host, cmds,
		s.GlusterCliExecTimeout()))
	if err != nil {
		return err
	}

	cmds = []string{
		fmt.Sprintf("rm -rf %v/%v/%v", mountpoint, volume, dirvolume.Name),
	}

	exportCmd := ""
	if len(dirvolume.ExportDirStr) == 0 {
		exportCmd = fmt.Sprintf("%v volume reset %v export-dir",
			s.glusterCommand(), volume)
	} else {
		exportCmd = fmt.Sprintf("%v volume set %v export-dir \"%v\"",
			s.glusterCommand(), volume, dirvolume.ExportDirStr)
	}

	cmds = append(cmds, exportCmd)

	err = rex.AnyError(s.RemoteExecutor.ExecCommands(host, cmds,
		s.GlusterCliExecTimeout()))
	if err != nil {
		return logger.Err(fmt.Errorf("Unable to delete dirvolume %v from volume %v: %v",
			dirvolume.Name, volume, err))
	}

	return nil
}

func (s *CmdExecutor) DirvolumeStats(host string, volume string,
	dirvolume string) (*executors.Dirvolume, error) {

	godbc.Require(host != "")
	godbc.Require(volume != "")
	godbc.Require(dirvolume != "")

	type CliOutput struct {
		OpRet      int                  `xml:"opRet"`
		OpErrno    int                  `xml:"opErrno"`
		OpErrStr   string               `xml:"opErrstr"`
		DirvolInfo executors.DirvolInfo `xml:"volQuota"`
	}

	command := []string{
		fmt.Sprintf("%v volume quota %v list /%v --xml", s.glusterCommand(), volume, dirvolume),
	}

	//Get the xml output of dirvolume info
	results, err := s.RemoteExecutor.ExecCommands(host, command,
		s.GlusterCliExecTimeout())
	if err := rex.AnyError(results, err); err != nil {
		return nil, fmt.Errorf("Unable to get dirvolume info of dirvolume name: %v, volume %v",
			dirvolume, volume)
	}
	var dirvolInfo CliOutput
	err = xml.Unmarshal([]byte(results[0].Output), &dirvolInfo)
	if err != nil {
		return nil, fmt.Errorf("Unable to determine dirvolume info of dirvolume name: %v, volume %v",
			dirvolume, volume)
	}
	logger.Debug("%+v\n", dirvolInfo)
	// OpErrno:30802 implies "Another transaction is in progress for ofs. Please try again after sometime."
	// OpErrno:30800 implies "Locking failed on... Please check log file for details."
	if dirvolInfo.OpErrno == 30802 || dirvolInfo.OpErrno == 30800 {
		return nil, executors.CmdRetryError
	}
	if dirvolInfo.OpErrno != 0 || len(dirvolInfo.DirvolInfo.DirvolList) == 0 {
		return nil, fmt.Errorf("Unexpected errno for glusterfs quota command")
	}
	return &dirvolInfo.DirvolInfo.DirvolList[0], nil
}

func (s *CmdExecutor) DirvolumesInfo(host string, volume string) (*executors.DirvolInfo, error) {

	godbc.Require(host != "")
	godbc.Require(volume != "")

	type CliOutput struct {
		OpRet      int                  `xml:"opRet"`
		OpErrno    int                  `xml:"opErrno"`
		OpErrStr   string               `xml:"opErrstr"`
		DirvolInfo executors.DirvolInfo `xml:"volQuota"`
	}

	command := []string{
		fmt.Sprintf("%v volume quota %v list --xml", s.glusterCommand(), volume),
	}

	//Get the xml output of dirvolume info
	results, err := s.RemoteExecutor.ExecCommands(host, command,
		s.GlusterCliExecTimeout())
	if err := rex.AnyError(results, err); err != nil {
		return nil, fmt.Errorf("Unable to get dirvolume info of volume: %v", volume)
	}
	var dirvolInfo CliOutput
	err = xml.Unmarshal([]byte(results[0].Output), &dirvolInfo)
	if err != nil {
		return nil, fmt.Errorf("Unable to unmarshal dirvolume info of volume %v", volume)
	}
	return &dirvolInfo.DirvolInfo, nil
}

func (s *CmdExecutor) DirvolumeExpand(host string, volume string,
	dirvolume *executors.DirvolumeRequest) (*executors.Dirvolume, error) {

	godbc.Require(host != "")
	godbc.Require(volume != "")
	godbc.Require(dirvolume != nil)

	cmds := []string{
		fmt.Sprintf("%v volume quota %v limit-usage /%v %vGB",
			s.glusterCommand(), volume, dirvolume.Name, dirvolume.Size),
	}

	err := rex.AnyError(s.RemoteExecutor.ExecCommands(host, cmds,
		s.GlusterCliExecTimeout()))
	if err != nil {
		return nil, err
	}

	return &executors.Dirvolume{}, nil
}

func (s *CmdExecutor) DirvolumeUpdateExport(host string, volume string,
	dirvolume *executors.DirvolumeRequest) (*executors.Dirvolume, error) {

	godbc.Require(host != "")
	godbc.Require(volume != "")
	godbc.Require(dirvolume != nil)

	cmds := []string{
		fmt.Sprintf("%v volume set %v export-dir \"%v\"",
			s.glusterCommand(), volume, dirvolume.ExportDirStr),
	}

	err := rex.AnyError(s.RemoteExecutor.ExecCommands(host, cmds,
		s.GlusterCliExecTimeout()))
	if err != nil {
		return nil, err
	}

	return &executors.Dirvolume{}, nil
}
