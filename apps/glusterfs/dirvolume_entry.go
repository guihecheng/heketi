package glusterfs

import (
	"bytes"
	"encoding/gob"
	"errors"

	"github.com/boltdb/bolt"
	"github.com/heketi/heketi/executors"
	wdb "github.com/heketi/heketi/pkg/db"
	"github.com/heketi/heketi/pkg/glusterfs/api"
	"github.com/heketi/heketi/pkg/idgen"
	"github.com/lpabon/godbc"
)

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
	var sshhost string
	err := db.View(func(tx *bolt.Tx) error {

		cluster, err := NewClusterEntryFromId(tx, dv.Info.ClusterId)
		if err != nil {
			return err
		}

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
		return nil, "", errors.New("failed to find host for creating subvolme for cluster " + dv.Info.ClusterId)
	}

	dvr.Name = dv.Info.Name
	dvr.Size = dv.Info.Size

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

	var sshhost string
	err := db.View(func(tx *bolt.Tx) error {
		cluster, err := NewClusterEntryFromId(tx, dv.Info.ClusterId)
		if err != nil {
			return err
		}

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
		return err
	}

	if sshhost == "" {
		return errors.New("failed to find host for destroying subvolme for cluster " + dv.Info.ClusterId)
	}

	if err := executor.DirvolumeDestroy(sshhost, DirPoolVolumeName, dv.Info.Name); err != nil {
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
