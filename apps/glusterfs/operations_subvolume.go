package glusterfs

import (
	"fmt"

	"github.com/heketi/heketi/executors"
	wdb "github.com/heketi/heketi/pkg/db"

	"github.com/boltdb/bolt"
)

type SubvolumeCreateOperation struct {
	OperationManager
	svol       *SubvolumeEntry
	maxRetries int
}

func NewSubvolumeCreateOperation(
	svol *SubvolumeEntry, db wdb.DB) *SubvolumeCreateOperation {

	return &SubvolumeCreateOperation{
		OperationManager: OperationManager{
			db: db,
			op: NewPendingOperationEntry(NEW_ID),
		},
		maxRetries: VOLUME_MAX_RETRIES,
		svol:       svol,
	}
}

func loadSubvolumeCreateOperation(
	db wdb.DB, p *PendingOperationEntry) (*SubvolumeCreateOperation, error) {

	svols, err := subvolumesFromOp(db, p)
	if err != nil {
		return nil, err
	}
	if len(svols) != 1 {
		return nil, fmt.Errorf(
			"Incorrect number of subvolumes (%v) for create operation: %v",
			len(svols), p.Id)
	}

	return &SubvolumeCreateOperation{
		OperationManager: OperationManager{
			db: db,
			op: p,
		},
		maxRetries: VOLUME_MAX_RETRIES,
		svol:       svols[0],
	}, nil
}

func (svc *SubvolumeCreateOperation) Label() string {
	return "Create Subvolume"
}

func (svc *SubvolumeCreateOperation) ResourceUrl() string {
	return fmt.Sprintf("/subvolumes/%v", svc.svol.Info.Id)
}

func (svc *SubvolumeCreateOperation) MaxRetries() int {
	return svc.maxRetries
}

func (svc *SubvolumeCreateOperation) Build() error {
	return svc.db.Update(func(tx *bolt.Tx) error {
		svc.op.RecordAddSubvolume(svc.svol)
		if e := svc.svol.Save(tx); e != nil {
			return e
		}
		if e := svc.op.Save(tx); e != nil {
			return e
		}
		return nil
	})
}

func (svc *SubvolumeCreateOperation) Exec(executor executors.Executor) error {
	err := svc.svol.createSubvolume(svc.db, executor)
	if err != nil {
		logger.LogError("Error executing create subvolume: %v", err)
		return OperationRetryError{err}
	}
	return nil
}

func (svc *SubvolumeCreateOperation) Finalize() error {
	return svc.db.Update(func(tx *bolt.Tx) error {
		svc.op.FinalizeSubvolume(svc.svol)
		if e := svc.svol.Save(tx); e != nil {
			return e
		}

		svc.op.Delete(tx)
		return nil
	})
}

func (svc *SubvolumeCreateOperation) Rollback(executor executors.Executor) error {
	return rollbackViaClean(svc, executor)
}

func (svc *SubvolumeCreateOperation) Clean(executor executors.Executor) error {
	var err error
	logger.Info("Starting Clean for %v op:%v", svc.Label(), svc.op.Id)
	err = removeSubvolumeWithOp(svc.db, executor, svc.op, svc.svol.Info.Id)
	return err
}

func (svc *SubvolumeCreateOperation) CleanDone() error {
	logger.Info("Clean is done for %v op:%v", svc.Label(), svc.op.Id)
	var err error
	// set in-memory copy of subvolume to match (torn down) db state
	svc.svol, err = expungeSubvolumeWithOp(svc.db, svc.op, svc.svol.Info.Id)
	return err
}

type SubvolumeDeleteOperation struct {
	OperationManager
	noRetriesOperation
	svol *SubvolumeEntry
}

func NewSubvolumeDeleteOperation(
	svol *SubvolumeEntry, db wdb.DB) *SubvolumeDeleteOperation {

	return &SubvolumeDeleteOperation{
		OperationManager: OperationManager{
			db: db,
			op: NewPendingOperationEntry(NEW_ID),
		},
		svol: svol,
	}
}

func loadSubvolumeDeleteOperation(
	db wdb.DB, p *PendingOperationEntry) (*SubvolumeDeleteOperation, error) {

	svols, err := subvolumesFromOp(db, p)
	if err != nil {
		return nil, err
	}
	if len(svols) != 1 {
		return nil, fmt.Errorf(
			"Incorrect number of subvolumes (%v) for delete operation: %v",
			len(svols), p.Id)
	}

	return &SubvolumeDeleteOperation{
		OperationManager: OperationManager{
			db: db,
			op: p,
		},
		svol: svols[0],
	}, nil
}

func (svd *SubvolumeDeleteOperation) Label() string {
	return "Delete Subvolume"
}

func (svd *SubvolumeDeleteOperation) ResourceUrl() string {
	return ""
}

func (svd *SubvolumeDeleteOperation) Build() error {
	return svd.db.Update(func(tx *bolt.Tx) error {
		sv, err := NewSubvolumeEntryFromId(tx, svd.svol.Info.Id)
		if err != nil {
			return err
		}
		svd.svol = sv
		if svd.svol.Pending.Id != "" {
			logger.LogError("Pending subvolume %v can not be deleted",
				svd.svol.Info.Id)
			return ErrConflict
		}
		svd.op.RecordDeleteSubvolume(svd.svol)
		if e := svd.op.Save(tx); e != nil {
			return e
		}
		if e := svd.svol.Save(tx); e != nil {
			return e
		}
		return nil
	})
}

func (svd *SubvolumeDeleteOperation) Exec(executor executors.Executor) error {
	var err error
	err = removeSubvolumeWithOp(svd.db, executor, svd.op, svd.svol.Info.Id)
	if err != nil {
		logger.LogError("Error executing delete subvolume: %v", err)
	}
	return err
}

func (svd *SubvolumeDeleteOperation) Rollback(executor executors.Executor) error {
	return svd.db.Update(func(tx *bolt.Tx) error {
		svd.op.FinalizeSubvolume(svd.svol)
		if err := svd.svol.Save(tx); err != nil {
			return err
		}

		svd.op.Delete(tx)
		return nil
	})
}

func (svd *SubvolumeDeleteOperation) Finalize() error {
	_, err := expungeSubvolumeWithOp(svd.db, svd.op, svd.svol.Info.Id)
	return err
}

func (svd *SubvolumeDeleteOperation) Clean(executor executors.Executor) error {
	logger.Info("Starting Clean for %v op:%v", svd.Label(), svd.op.Id)
	return svd.Exec(executor)
}

func (svd *SubvolumeDeleteOperation) CleanDone() error {
	logger.Info("Clean is done for %v op:%v", svd.Label(), svd.op.Id)
	return svd.Finalize()
}

func removeSubvolumeWithOp(
	db wdb.RODB, executor executors.Executor,
	op *PendingOperationEntry, svolId string) error {

	var (
		err error
		sv  *SubvolumeEntry
	)
	logger.Info("preparing to remove subvolume %v in op:%v", svolId, op.Id)
	err = db.View(func(tx *bolt.Tx) error {
		// get a fresh volume object from db
		sv, err = NewSubvolumeEntryFromId(tx, svolId)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		logger.LogError(
			"failed to get state needed to destroy subvolume: %v", err)
		return err
	}
	logger.Info("executing removal of subvolume %v in op:%v", svolId, op.Id)
	return sv.destroySubvolume(db, executor)
}

func expungeSubvolumeWithOp(
	db wdb.DB,
	op *PendingOperationEntry, svolId string) (*SubvolumeEntry, error) {

	var sv *SubvolumeEntry
	return sv, db.Update(func(tx *bolt.Tx) error {
		var err error
		txdb := wdb.WrapTx(tx)
		sv, err = NewSubvolumeEntryFromId(tx, svolId)
		if err != nil {
			return err
		}
		if err := sv.teardown(txdb); err != nil {
			return err
		}
		return op.Delete(tx)
	})
}
