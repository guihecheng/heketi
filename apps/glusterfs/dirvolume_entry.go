package glusterfs

import (
	"bytes"
	"encoding/gob"
	"errors"
	"strings"

	"github.com/boltdb/bolt"
	"github.com/heketi/heketi/executors"
	wdb "github.com/heketi/heketi/pkg/db"
	"github.com/heketi/heketi/pkg/glusterfs/api"
	"github.com/heketi/heketi/pkg/idgen"
	"github.com/lpabon/godbc"
)

// Operate the glusterfs "export-dir" option

func appendExport(dvName string, exportDirStr string) string {
	if len(exportDirStr) > 0 {
		exportDirStr += ","
	}
	// If no IPs specified, default to allow access to all,
	// So we only allow access to localhost by default
	exportDirStr += "/" + dvName + "(127.0.0.1)"
	return exportDirStr
}

func deleteExport(dvName string, exportDirStr string) string {
	out := ""
	for _, entry := range strings.Split(exportDirStr, ",") {
		if !strings.Contains(entry, dvName) {
			if len(out) > 0 {
				out += ","
			}
			out += entry
		}
	}
	return out
}

func appendIpListToExport(dvName string, ipList []string, exportDirStr string) string {
	out := ""
	for _, entry := range strings.Split(exportDirStr, ",") {
		if strings.Contains(entry, dvName) {
			newEntry := "/" + dvName

			i := strings.Index(entry, "(")
			newIpListStr := ""
			if i > -1 {
				newIpListStr = entry[i+1 : len(entry)-1]
			}

			exists := make(map[string]bool)
			for _, ip := range strings.Split(newIpListStr, "|") {
				exists[ip] = true
			}
			for _, ip := range ipList {
				if !exists[ip] {
					if len(newIpListStr) > 0 {
						newIpListStr += "|"
					}
					newIpListStr += ip
				}
			}

			if len(newIpListStr) > 0 {
				newEntry += "("
			}
			newEntry += newIpListStr
			if len(newIpListStr) > 0 {
				newEntry += ")"
			}
			entry = newEntry
		}

		if len(out) > 0 {
			out += ","
		}
		out += entry
	}
	return out
}

func deleteIpListToExport(dvName string, ipList []string, exportDirStr string) string {
	out := ""
	for _, entry := range strings.Split(exportDirStr, ",") {
		if strings.Contains(entry, dvName) {
			newEntry := "/" + dvName

			i := strings.Index(entry, "(")
			oldIpList := ""
			newIpListStr := ""
			if i > -1 {
				oldIpList = entry[i+1 : len(entry)-1]
			}

			exists := make(map[string]bool)
			for _, ip := range ipList {
				exists[ip] = true
			}
			for _, ip := range strings.Split(oldIpList, "|") {
				if !exists[ip] {
					if len(newIpListStr) > 0 {
						newIpListStr += "|"
					}
					newIpListStr += ip
				}
			}

			if len(newIpListStr) > 0 {
				newEntry += "("
			}
			newEntry += newIpListStr
			if len(newIpListStr) > 0 {
				newEntry += ")"
			}
			entry = newEntry
		}

		if len(out) > 0 {
			out += ","
		}
		out += entry
	}
	return out
}

type DirvolumeEntry struct {
	Info    api.DirvolumeInfo
	Pending PendingItem
}

func DirvolumeList(tx *bolt.Tx) ([]string, error) {

	list := EntryKeys(tx, BOLTDB_BUCKET_DIRVOLUME)
	if list == nil {
		return nil, ErrAccessList
	}
	return list, nil
}

func NewDirvolumeEntry() *DirvolumeEntry {
	entry := &DirvolumeEntry{}
	return entry
}

func NewDirvolumeEntryFromId(tx *bolt.Tx, id string) (*DirvolumeEntry, error) {
	godbc.Require(tx != nil)

	entry := NewDirvolumeEntry()
	err := EntryLoad(tx, entry, id)
	if err != nil {
		return nil, err
	}

	return entry, nil
}

func NewDirvolumeEntryFromRequest(req *api.DirvolumeCreateRequest) *DirvolumeEntry {
	godbc.Require(req != nil)

	dvol := NewDirvolumeEntry()
	dvol.Info.Size = req.Size
	dvol.Info.ClusterId = req.ClusterId
	dvol.Info.Id = idgen.GenUUID()

	// Set default name
	if req.Name == "" {
		dvol.Info.Name = "dvol_" + dvol.Info.Id
	} else {
		dvol.Info.Name = req.Name
	}

	return dvol
}

func (dv *DirvolumeEntry) BucketName() string {
	return BOLTDB_BUCKET_DIRVOLUME
}

func (dv *DirvolumeEntry) Save(tx *bolt.Tx) error {
	godbc.Require(tx != nil)
	godbc.Require(len(dv.Info.Id) > 0)

	return EntrySave(tx, dv, dv.Info.Id)
}

func (dv *DirvolumeEntry) Delete(tx *bolt.Tx) error {
	return EntryDelete(tx, dv, dv.Info.Id)
}

func DirvolumeEntryUpgrade(tx *bolt.Tx) error {
	return nil
}

func (dv *DirvolumeEntry) Marshal() ([]byte, error) {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(*dv)

	return buffer.Bytes(), err
}

func (dv *DirvolumeEntry) Unmarshal(buffer []byte) error {
	dec := gob.NewDecoder(bytes.NewReader(buffer))
	err := dec.Decode(dv)
	if err != nil {
		return err
	}

	return nil
}

func (dv *DirvolumeEntry) createDirvolume(db wdb.RODB,
	executor executors.Executor) error {

	godbc.Require(db != nil)

	dvr, host, err := dv.createDirvolumeRequest(db)
	if err != nil {
		return err
	}

	if _, err := executor.DirvolumeCreate(host, DirPoolVolumeName, dvr); err != nil {
		return err
	}
	return nil
}

func (dv *DirvolumeEntry) createDirvolumeRequest(db wdb.RODB) (*executors.DirvolumeRequest,
	string, error) {

	godbc.Require(db != nil)

	dvr := &executors.DirvolumeRequest{}
	var exportDirStr, sshhost string
	err := db.View(func(tx *bolt.Tx) error {

		cluster, err := NewClusterEntryFromId(tx, dv.Info.ClusterId)
		if err != nil {
			return err
		}

		exportDirStr = cluster.Info.ExportDirStr

		// TODO: verify if the node is available/online?
		// picking the 1st node for now...
		node, err := NewNodeEntryFromId(tx, cluster.Info.Nodes[0])
		if err != nil {
			return err
		}
		sshhost = node.ManageHostName()

		return nil
	})
	if err != nil {
		return nil, "", err
	}

	if sshhost == "" {
		return nil, "", errors.New("failed to find host for creating dirvolume for cluster " + dv.Info.ClusterId)
	}

	dvr.Name = dv.Info.Name
	dvr.Size = dv.Info.Size
	dvr.ExportDirStr = exportDirStr

	return dvr, sshhost, nil
}

func (dv *DirvolumeEntry) checkCreateDirvolume(db wdb.DB) error {
	err := db.View(func(tx *bolt.Tx) error {
		cluster, err := NewClusterEntryFromId(tx, dv.Info.ClusterId)
		if err != nil {
			return err
		}

		var found bool
		found, err = dirvolumeNameExistsInCluster(tx, cluster, dv.Info.Name)
		if err != nil {
			return err
		}
		if found {
			err = errors.New("Name " + dv.Info.Name + " already in use in cluster " + dv.Info.ClusterId)
		}
		return err
	})

	if err != nil {
		return err
	}
	return dv.saveCreateDirvolume(db)
}

func (dv *DirvolumeEntry) saveCreateDirvolume(db wdb.DB) error {
	err := db.Update(func(tx *bolt.Tx) error {
		// Save cluster
		cluster, err := NewClusterEntryFromId(tx, dv.Info.ClusterId)
		if err != nil {
			return err
		}
		cluster.DirvolumeAdd(dv.Info.Id)
		cluster.Info.ExportDirStr = appendExport(dv.Info.Name, cluster.Info.ExportDirStr)

		return cluster.Save(tx)
	})
	return err
}

func dirvolumeNameExistsInCluster(tx *bolt.Tx, cluster *ClusterEntry,
	name string) (found bool, e error) {
	for _, dvolId := range cluster.Info.Dirvolumes {
		dv, err := NewDirvolumeEntryFromId(tx, dvolId)
		if err != nil {
			return false, err
		}
		if name == dv.Info.Name {
			found = true
			return
		}
	}

	return
}

func (dv *DirvolumeEntry) destroyDirvolume(db wdb.RODB,
	executor executors.Executor) error {

	godbc.Require(db != nil)

	dvr, host, err := dv.createDirvolumeRequest(db)
	if err != nil {
		return err
	}

	dvr.ExportDirStr = deleteExport(dv.Info.Name, dvr.ExportDirStr)

	if err := executor.DirvolumeDestroy(host, DirPoolVolumeName, dvr); err != nil {
		return err
	}
	return nil
}

func (dv *DirvolumeEntry) teardown(db wdb.DB) error {
	return db.Update(func(tx *bolt.Tx) error {
		if dv.Info.ClusterId != "" {
			cluster, err := NewClusterEntryFromId(tx, dv.Info.ClusterId)
			if err != nil {
				return err
			}
			cluster.DirvolumeDelete(dv.Info.Id)
			cluster.Info.ExportDirStr = deleteExport(dv.Info.Name, cluster.Info.ExportDirStr)
			err = cluster.Save(tx)
			if err != nil {
				return err
			}
		}
		return dv.Delete(tx)
	})
}

func (dv *DirvolumeEntry) NewInfoResponse(tx *bolt.Tx) (*api.DirvolumeInfoResponse, error) {
	godbc.Require(tx != nil)

	info := api.NewDirvolumeInfoResponse()
	info.Size = dv.Info.Size
	info.Name = dv.Info.Name
	info.Id = dv.Info.Id
	info.ClusterId = dv.Info.ClusterId
	info.Export = dv.Info.Export

	return info, nil
}

func (dv *DirvolumeEntry) expandDirvolume(db wdb.RODB,
	executor executors.Executor) error {

	godbc.Require(db != nil)

	dvr, host, err := dv.createDirvolumeRequest(db)
	if err != nil {
		return err
	}

	if _, err := executor.DirvolumeExpand(host, DirPoolVolumeName, dvr); err != nil {
		return err
	}
	return nil
}

func (dv *DirvolumeEntry) exportDirvolume(db wdb.RODB,
	executor executors.Executor) error {

	godbc.Require(db != nil)

	dvr, host, err := dv.createDirvolumeRequest(db)
	if err != nil {
		return err
	}

	dvr.ExportDirStr = appendIpListToExport(dv.Info.Name, dv.Info.Export.IpList, dvr.ExportDirStr)

	if _, err := executor.DirvolumeUpdateExport(host, DirPoolVolumeName, dvr); err != nil {
		return err
	}
	return nil
}

func (dv *DirvolumeEntry) unexportDirvolume(db wdb.RODB,
	executor executors.Executor) error {

	godbc.Require(db != nil)

	dvr, host, err := dv.createDirvolumeRequest(db)
	if err != nil {
		return err
	}

	dvr.ExportDirStr = deleteIpListToExport(dv.Info.Name, dv.Info.Export.IpList, dvr.ExportDirStr)

	if _, err := executor.DirvolumeUpdateExport(host, DirPoolVolumeName, dvr); err != nil {
		return err
	}
	return nil
}

func (dv *DirvolumeEntry) statDirvolume(db wdb.RODB,
	executor executors.Executor) (*api.DirvolumeStatsResponse, error) {

	godbc.Require(db != nil)

	_, host, err := dv.createDirvolumeRequest(db)
	if err != nil {
		return nil, err
	}

	stats := api.NewDirvolumeStatsResponse()
	var edv *executors.Dirvolume
	if edv, err = executor.DirvolumeStats(host, DirPoolVolumeName, dv.Info.Name); err != nil {
		return nil, err
	}

	stats.Id = dv.Info.Id
	stats.TotalSize = edv.TotalSize
	stats.UsedSize = edv.UsedSize
	stats.AvailSize = edv.AvailSize

	return stats, nil
}
