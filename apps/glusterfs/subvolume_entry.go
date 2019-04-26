package glusterfs

import (
	"bytes"
	"encoding/gob"
	"errors"

	"github.com/boltdb/bolt"
	"github.com/heketi/heketi/executors"
	wdb "github.com/heketi/heketi/pkg/db"
	"github.com/heketi/heketi/pkg/glusterfs/api"
	"github.com/lpabon/godbc"
)

type SubvolumeEntry struct {
	Info    api.SubvolumeInfo
	Pending PendingItem
}

func NewSubvolumeEntry() *SubvolumeEntry {
	entry := &SubvolumeEntry{}
	return entry
}

func NewSubvolumeEntryFromId(tx *bolt.Tx, id string) (*SubvolumeEntry, error) {
	godbc.Require(tx != nil)

	entry := NewSubvolumeEntry()
	err := EntryLoad(tx, entry, id)
	if err != nil {
		return nil, err
	}

	return entry, nil
}

func (sv *SubvolumeEntry) BucketName() string {
	return BOLTDB_BUCKET_SUBVOLUME
}

func (sv *SubvolumeEntry) Save(tx *bolt.Tx) error {
	godbc.Require(tx != nil)
	godbc.Require(len(sv.Info.Id) > 0)

	return EntrySave(tx, sv, sv.Info.Id)
}

func (sv *SubvolumeEntry) Delete(tx *bolt.Tx) error {
	return EntryDelete(tx, sv, sv.Info.Id)
}

func (sv *SubvolumeEntry) Marshal() ([]byte, error) {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(*sv)

	return buffer.Bytes(), err
}

func (sv *SubvolumeEntry) Unmarshal(buffer []byte) error {
	dec := gob.NewDecoder(bytes.NewReader(buffer))
	err := dec.Decode(sv)
	if err != nil {
		return err
	}

	return nil
}

func (sv *SubvolumeEntry) createSubvolume(db wdb.RODB,
	executor executors.Executor) error {

	godbc.Require(db != nil)

	svr, host, volume, err := sv.createSubvolumeRequest(db)
	if err != nil {
		return err
	}

	if _, err := executor.SubvolumeCreate(host, volume, svr); err != nil {
		return err
	}
	return nil
}

func (sv *SubvolumeEntry) createSubvolumeRequest(db wdb.RODB) (*executors.SubvolumeRequest,
	string, string, error) {

	godbc.Require(db != nil)

	svr := &executors.SubvolumeRequest{}
	var sshhost, volume string
	err := db.View(func(tx *bolt.Tx) error {
		vol, err := NewVolumeEntryFromId(tx, sv.Info.VolumeId)
		if err != nil {
			return err
		}

		volume = vol.Info.Name

		cluster, err := NewClusterEntryFromId(tx, vol.Info.Cluster)
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
		return nil, "", "", err
	}

	if sshhost == "" {
		return nil, "", "", errors.New("failed to find host for creating subvolme for volume " + volume)
	}

	svr.Name = sv.Info.Name
	svr.Size = sv.Info.Size

	return svr, sshhost, volume, nil
}

func (sv *SubvolumeEntry) destroySubvolume(db wdb.RODB,
	executor executors.Executor) error {

	godbc.Require(db != nil)

	var sshhost, volume string
	err := db.View(func(tx *bolt.Tx) error {
		vol, err := NewVolumeEntryFromId(tx, sv.Info.VolumeId)
		if err != nil {
			return err
		}

		volume = vol.Info.Name

		cluster, err := NewClusterEntryFromId(tx, vol.Info.Cluster)
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
		return errors.New("failed to find host for destroying subvolme for volume " + volume)
	}

	if err := executor.SubvolumeDestroy(sshhost, volume, sv.Info.Name); err != nil {
		return err
	}

	return nil
}

func (sv *SubvolumeEntry) teardown(db wdb.DB) error {
	return db.Update(func(tx *bolt.Tx) error {
		return sv.Delete(tx)
	})
}
