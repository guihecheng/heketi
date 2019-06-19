package glusterfs

import (
	"fmt"

	"github.com/heketi/heketi/executors"
	wdb "github.com/heketi/heketi/pkg/db"

	"github.com/boltdb/bolt"
)

type DirvolumeCreateOperation struct {
	OperationManager
	dvol       *DirvolumeEntry
	maxRetries int
}

func NewDirvolumeCreateOperation(
	dvol *DirvolumeEntry, db wdb.DB) *DirvolumeCreateOperation {

	return &DirvolumeCreateOperation{
		OperationManager: OperationManager{
			db: db,
			op: NewPendingOperationEntry(NEW_ID),
		},
		maxRetries: VOLUME_MAX_RETRIES,
		dvol:       dvol,
	}
}

func loadDirvolumeCreateOperation(
	db wdb.DB, p *PendingOperationEntry) (*DirvolumeCreateOperation, error) {

	dvols, err := dirvolumesFromOp(db, p)
	if err != nil {
		return nil, err
	}
	if len(dvols) != 1 {
		return nil, fmt.Errorf(
			"Incorrect number of dirvolumes (%v) for create operation: %v",
			len(dvols), p.Id)
	}

	return &DirvolumeCreateOperation{
		OperationManager: OperationManager{
			db: db,
			op: p,
		},
		maxRetries: VOLUME_MAX_RETRIES,
		dvol:       dvols[0],
	}, nil
}

func (dvc *DirvolumeCreateOperation) Label() string {
	return "Create Dirvolume"
}

func (dvc *DirvolumeCreateOperation) ResourceUrl() string {
	return fmt.Sprintf("/dirvolumes/%v", dvc.dvol.Info.Id)
}

func (dvc *DirvolumeCreateOperation) MaxRetries() int {
	return dvc.maxRetries
}

func (dvc *DirvolumeCreateOperation) Build() error {
	return dvc.db.Update(func(tx *bolt.Tx) error {
		txdb := wdb.WrapTx(tx)
		e := dvc.dvol.checkCreateDirvolume(txdb)
		if e != nil {
			return e
		}

		dvc.op.RecordAddDirvolume(dvc.dvol)
		if e = dvc.dvol.Save(tx); e != nil {
			return e
		}
		if e = dvc.op.Save(tx); e != nil {
			return e
		}
		return nil
	})
}

func (dvc *DirvolumeCreateOperation) Exec(executor executors.Executor) error {
	err := dvc.dvol.createDirvolume(dvc.db, executor)
	if err != nil {
		logger.LogError("Error executing create dirvolume: %v", err)
		return OperationRetryError{err}
	}
	return nil
}

func (dvc *DirvolumeCreateOperation) Finalize() error {
	return dvc.db.Update(func(tx *bolt.Tx) error {
		dvc.op.FinalizeDirvolume(dvc.dvol)
		if e := dvc.dvol.Save(tx); e != nil {
			return e
		}

		dvc.op.Delete(tx)
		return nil
	})
}

func (dvc *DirvolumeCreateOperation) Rollback(executor executors.Executor) error {
	return rollbackViaClean(dvc, executor)
}

func (dvc *DirvolumeCreateOperation) Clean(executor executors.Executor) error {
	var err error
	logger.Info("Starting Clean for %v op:%v", dvc.Label(), dvc.op.Id)
	err = removeDirvolumeWithOp(dvc.db, executor, dvc.op, dvc.dvol.Info.Id)
	return err
}

func (dvc *DirvolumeCreateOperation) CleanDone() error {
	logger.Info("Clean is done for %v op:%v", dvc.Label(), dvc.op.Id)
	var err error
	// set in-memory copy of dirvolume to match (torn down) db state
	dvc.dvol, err = expungeDirvolumeWithOp(dvc.db, dvc.op, dvc.dvol.Info.Id)
	return err
}

type DirvolumeDeleteOperation struct {
	OperationManager
	noRetriesOperation
	dvol *DirvolumeEntry
}

func NewDirvolumeDeleteOperation(
	dvol *DirvolumeEntry, db wdb.DB) *DirvolumeDeleteOperation {

	return &DirvolumeDeleteOperation{
		OperationManager: OperationManager{
			db: db,
			op: NewPendingOperationEntry(NEW_ID),
		},
		dvol: dvol,
	}
}

func loadDirvolumeDeleteOperation(
	db wdb.DB, p *PendingOperationEntry) (*DirvolumeDeleteOperation, error) {

	dvols, err := dirvolumesFromOp(db, p)
	if err != nil {
		return nil, err
	}
	if len(dvols) != 1 {
		return nil, fmt.Errorf(
			"Incorrect number of dirvolumes (%v) for delete operation: %v",
			len(dvols), p.Id)
	}

	return &DirvolumeDeleteOperation{
		OperationManager: OperationManager{
			db: db,
			op: p,
		},
		dvol: dvols[0],
	}, nil
}

func (dvd *DirvolumeDeleteOperation) Label() string {
	return "Delete Dirvolume"
}

func (dvd *DirvolumeDeleteOperation) ResourceUrl() string {
	return ""
}

func (dvd *DirvolumeDeleteOperation) Build() error {
	return dvd.db.Update(func(tx *bolt.Tx) error {
		dv, err := NewDirvolumeEntryFromId(tx, dvd.dvol.Info.Id)
		if err != nil {
			return err
		}
		dvd.dvol = dv
		if dvd.dvol.Pending.Id != "" {
			logger.LogError("Pending dirvolume %v can not be deleted",
				dvd.dvol.Info.Id)
			return ErrConflict
		}
		dvd.op.RecordDeleteDirvolume(dvd.dvol)
		if e := dvd.op.Save(tx); e != nil {
			return e
		}
		if e := dvd.dvol.Save(tx); e != nil {
			return e
		}
		return nil
	})
}

func (dvd *DirvolumeDeleteOperation) Exec(executor executors.Executor) error {
	var err error
	err = removeDirvolumeWithOp(dvd.db, executor, dvd.op, dvd.dvol.Info.Id)
	if err != nil {
		logger.LogError("Error executing delete dirvolume: %v", err)
	}
	return err
}

func (dvd *DirvolumeDeleteOperation) Rollback(executor executors.Executor) error {
	return dvd.db.Update(func(tx *bolt.Tx) error {
		dvd.op.FinalizeDirvolume(dvd.dvol)
		if err := dvd.dvol.Save(tx); err != nil {
			return err
		}

		dvd.op.Delete(tx)
		return nil
	})
}

func (dvd *DirvolumeDeleteOperation) Finalize() error {
	_, err := expungeDirvolumeWithOp(dvd.db, dvd.op, dvd.dvol.Info.Id)
	return err
}

func (dvd *DirvolumeDeleteOperation) Clean(executor executors.Executor) error {
	logger.Info("Starting Clean for %v op:%v", dvd.Label(), dvd.op.Id)
	return dvd.Exec(executor)
}

func (dvd *DirvolumeDeleteOperation) CleanDone() error {
	logger.Info("Clean is done for %v op:%v", dvd.Label(), dvd.op.Id)
	return dvd.Finalize()
}

func removeDirvolumeWithOp(
	db wdb.RODB, executor executors.Executor,
	op *PendingOperationEntry, dvolId string) error {

	var (
		err error
		dv  *DirvolumeEntry
	)
	logger.Info("preparing to remove dirvolume %v in op:%v", dvolId, op.Id)
	err = db.View(func(tx *bolt.Tx) error {
		// get a fresh volume object from db
		dv, err = NewDirvolumeEntryFromId(tx, dvolId)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		logger.LogError(
			"failed to get state needed to destroy dirvolume: %v", err)
		return err
	}
	logger.Info("executing removal of dirvolume %v in op:%v", dvolId, op.Id)
	return dv.destroyDirvolume(db, executor)
}

func expungeDirvolumeWithOp(
	db wdb.DB,
	op *PendingOperationEntry, dvolId string) (*DirvolumeEntry, error) {

	var dv *DirvolumeEntry
	return dv, db.Update(func(tx *bolt.Tx) error {
		var err error
		txdb := wdb.WrapTx(tx)
		dv, err = NewDirvolumeEntryFromId(tx, dvolId)
		if err != nil {
			return err
		}
		if err := dv.teardown(txdb); err != nil {
			return err
		}
		return op.Delete(tx)
	})
}

type DirvolumeExpandOperation struct {
	OperationManager
	noRetriesOperation
	dvol       *DirvolumeEntry
	ExpandSize int
}

func NewDirvolumeExpandOperation(
	dvol *DirvolumeEntry, db wdb.DB, sizeGB int) *DirvolumeExpandOperation {

	return &DirvolumeExpandOperation{
		OperationManager: OperationManager{
			db: db,
			op: NewPendingOperationEntry(NEW_ID),
		},
		dvol:       dvol,
		ExpandSize: sizeGB,
	}
}

func loadDirvolumeExpandOperation(
	db wdb.DB, p *PendingOperationEntry) (*DirvolumeExpandOperation, error) {

	dvols, err := dirvolumesFromOp(db, p)
	if err != nil {
		return nil, err
	}
	if len(dvols) != 1 {
		return nil, fmt.Errorf(
			"Incorrect number of dirvolumes (%v) for expand operation: %v",
			len(dvols), p.Id)
	}

	return &DirvolumeExpandOperation{
		OperationManager: OperationManager{
			db: db,
			op: p,
		},
		dvol: dvols[0],
	}, nil
}

func (dve *DirvolumeExpandOperation) Label() string {
	return "Expand Dirvolume"
}

func (dve *DirvolumeExpandOperation) ResourceUrl() string {
	return fmt.Sprintf("/dirvolumes/%v", dve.dvol.Info.Id)
}

func (dve *DirvolumeExpandOperation) Build() error {
	return dve.db.Update(func(tx *bolt.Tx) error {
		dve.dvol.Info.Size += dve.ExpandSize
		dve.op.RecordExpandDirvolume(dve.dvol, dve.ExpandSize)
		if e := dve.op.Save(tx); e != nil {
			return e
		}
		return nil
	})
}

func (dve *DirvolumeExpandOperation) Exec(executor executors.Executor) error {
	err := dve.dvol.expandDirvolume(dve.db, executor)
	if err != nil {
		logger.LogError("Error executing expand dirvolume: %v", err)
		return OperationRetryError{err}
	}
	return nil
}

func (dve *DirvolumeExpandOperation) Finalize() error {
	return dve.db.Update(func(tx *bolt.Tx) error {
		dve.op.FinalizeDirvolume(dve.dvol)
		if e := dve.dvol.Save(tx); e != nil {
			return e
		}

		dve.op.Delete(tx)
		return nil
	})
}

func (dve *DirvolumeExpandOperation) Rollback(executor executors.Executor) error {
	return rollbackViaClean(dve, executor)
}

func (dve *DirvolumeExpandOperation) Clean(executor executors.Executor) error {
	logger.Info("Starting Clean for %v op:%v", dve.Label(), dve.op.Id)
	return nil
}

func (dve *DirvolumeExpandOperation) CleanDone() error {
	logger.Info("Clean is done for %v op:%v", dve.Label(), dve.op.Id)
	return dve.db.Update(func(tx *bolt.Tx) error {
		return dve.op.Delete(tx)
	})
}
