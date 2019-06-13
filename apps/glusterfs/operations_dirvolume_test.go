package glusterfs

import (
	"fmt"
	"os"
	//	"strings"
	//	"sync"
	"testing"

	//"github.com/heketi/heketi/executors"
	"github.com/heketi/heketi/pkg/glusterfs/api"

	"github.com/boltdb/bolt"
	"github.com/heketi/tests"
)

func TestDirvolumeCreatePendingCreatedCleared(t *testing.T) {
	tmpfile := tests.Tempfile()
	defer os.Remove(tmpfile)

	// Create the app
	app := NewTestApp(tmpfile)
	defer app.Close()

	err := setupSampleDbWithTopology(app,
		1,    // clusters
		3,    // nodes_per_cluster
		1,    // devices_per_node,
		6*TB, // disksize)
	)
	tests.Assert(t, err == nil)

	var clusterId string
	err = app.db.View(func(tx *bolt.Tx) error {
		clusters, err := ClusterList(tx)
		if err != nil {
			return err
		}

		tests.Assert(t, len(clusters) == 1)
		cluster, err := NewClusterEntryFromId(tx, clusters[0])
		tests.Assert(t, err == nil)

		clusterId = cluster.Info.Id

		return nil
	})
	tests.Assert(t, err == nil)
	tests.Assert(t, clusterId != "")

	req := &api.DirvolumeCreateRequest{}
	req.Size = 1024
	req.ClusterId = clusterId

	dvol := NewDirvolumeEntryFromRequest(req)
	dvc := NewDirvolumeCreateOperation(dvol, app.db)

	// verify that there are no dirvolumes or pending operations
	app.db.View(func(tx *bolt.Tx) error {
		dvl, e := DirvolumeList(tx)
		tests.Assert(t, e == nil)
		tests.Assert(t, len(dvl) == 0)
		pol, e := PendingOperationList(tx)
		tests.Assert(t, e == nil)
		tests.Assert(t, len(pol) == 0)
		return nil
	})

	e := dvc.Build()
	tests.Assert(t, e == nil)

	// verify dirvolumes and pending ops exist
	app.db.View(func(tx *bolt.Tx) error {
		dvl, e := DirvolumeList(tx)
		tests.Assert(t, e == nil)
		tests.Assert(t, len(dvl) == 1)
		pol, e := PendingOperationList(tx)
		tests.Assert(t, e == nil)
		tests.Assert(t, len(pol) == 1)

		for _, dvid := range dvl {
			dv, e := NewDirvolumeEntryFromId(tx, dvid)
			tests.Assert(t, e == nil)
			tests.Assert(t, dv.Pending.Id == pol[0])
		}
		return nil
	})

	e = dvc.Exec(app.executor)
	tests.Assert(t, e == nil)

	e = dvc.Finalize()
	tests.Assert(t, e == nil)

	// verify dirvolumes exist but pending is gone
	app.db.View(func(tx *bolt.Tx) error {
		dvl, e := DirvolumeList(tx)
		tests.Assert(t, e == nil)
		tests.Assert(t, len(dvl) == 1)
		pol, e := PendingOperationList(tx)
		tests.Assert(t, e == nil)
		tests.Assert(t, len(pol) == 0)

		for _, dvid := range dvl {
			dv, e := NewDirvolumeEntryFromId(tx, dvid)
			tests.Assert(t, e == nil)
			tests.Assert(t, dv.Pending.Id == "")
		}
		return nil
	})
}

func TestDirvolumeCreatePendingRollback(t *testing.T) {
	tmpfile := tests.Tempfile()
	defer os.Remove(tmpfile)

	// Create the app
	app := NewTestApp(tmpfile)
	defer app.Close()

	err := setupSampleDbWithTopology(app,
		1,    // clusters
		3,    // nodes_per_cluster
		1,    // devices_per_node,
		6*TB, // disksize)
	)
	tests.Assert(t, err == nil)

	var clusterId string
	err = app.db.View(func(tx *bolt.Tx) error {
		clusters, err := ClusterList(tx)
		if err != nil {
			return err
		}

		tests.Assert(t, len(clusters) == 1)
		cluster, err := NewClusterEntryFromId(tx, clusters[0])
		tests.Assert(t, err == nil)

		clusterId = cluster.Info.Id

		return nil
	})
	tests.Assert(t, err == nil)
	tests.Assert(t, clusterId != "")

	req := &api.DirvolumeCreateRequest{}
	req.Size = 1024
	req.ClusterId = clusterId

	dvol := NewDirvolumeEntryFromRequest(req)
	dvc := NewDirvolumeCreateOperation(dvol, app.db)

	// verify that there are no dirvolumes or pending operations
	app.db.View(func(tx *bolt.Tx) error {
		dvl, e := DirvolumeList(tx)
		tests.Assert(t, e == nil)
		tests.Assert(t, len(dvl) == 0)
		pol, e := PendingOperationList(tx)
		tests.Assert(t, e == nil)
		tests.Assert(t, len(pol) == 0)
		return nil
	})

	e := dvc.Build()
	tests.Assert(t, e == nil)

	// verify dirvolumes and pending ops exist
	app.db.View(func(tx *bolt.Tx) error {
		dvl, e := DirvolumeList(tx)
		tests.Assert(t, e == nil)
		tests.Assert(t, len(dvl) == 1)
		pol, e := PendingOperationList(tx)
		tests.Assert(t, e == nil)
		tests.Assert(t, len(pol) == 1)

		for _, dvid := range dvl {
			dv, e := NewDirvolumeEntryFromId(tx, dvid)
			tests.Assert(t, e == nil)
			tests.Assert(t, dv.Pending.Id == pol[0])
		}
		return nil
	})

	e = dvc.Exec(app.executor)
	tests.Assert(t, e == nil)

	e = dvc.Rollback(app.executor)
	tests.Assert(t, e == nil)

	// verify no dirvolumes or pending exist
	app.db.View(func(tx *bolt.Tx) error {
		dvl, e := DirvolumeList(tx)
		tests.Assert(t, e == nil)
		tests.Assert(t, len(dvl) == 0)
		pol, e := PendingOperationList(tx)
		tests.Assert(t, e == nil)
		tests.Assert(t, len(pol) == 0)
		return nil
	})
}

func TestDirvolumeCreatePendingRollbackCleanupFailure(t *testing.T) {
	tmpfile := tests.Tempfile()
	defer os.Remove(tmpfile)

	// Create the app
	app := NewTestApp(tmpfile)
	defer app.Close()

	err := setupSampleDbWithTopology(app,
		1,    // clusters
		3,    // nodes_per_cluster
		1,    // devices_per_node,
		6*TB, // disksize)
	)
	tests.Assert(t, err == nil)

	var clusterId string
	err = app.db.View(func(tx *bolt.Tx) error {
		clusters, err := ClusterList(tx)
		if err != nil {
			return err
		}

		tests.Assert(t, len(clusters) == 1)
		cluster, err := NewClusterEntryFromId(tx, clusters[0])
		tests.Assert(t, err == nil)

		clusterId = cluster.Info.Id

		return nil
	})
	tests.Assert(t, err == nil)
	tests.Assert(t, clusterId != "")

	req := &api.DirvolumeCreateRequest{}
	req.Size = 1024
	req.ClusterId = clusterId

	dvol := NewDirvolumeEntryFromRequest(req)
	dvc := NewDirvolumeCreateOperation(dvol, app.db)

	// verify that there are no dirvolumes or pending operations
	app.db.View(func(tx *bolt.Tx) error {
		dvl, e := DirvolumeList(tx)
		tests.Assert(t, e == nil)
		tests.Assert(t, len(dvl) == 0)
		pol, e := PendingOperationList(tx)
		tests.Assert(t, e == nil)
		tests.Assert(t, len(pol) == 0)
		return nil
	})

	e := dvc.Build()
	tests.Assert(t, e == nil)

	// verify dirvolumes and pending ops exist
	app.db.View(func(tx *bolt.Tx) error {
		dvl, e := DirvolumeList(tx)
		tests.Assert(t, e == nil)
		tests.Assert(t, len(dvl) == 1)
		pol, e := PendingOperationList(tx)
		tests.Assert(t, e == nil)
		tests.Assert(t, len(pol) == 1)

		for _, dvid := range dvl {
			dv, e := NewDirvolumeEntryFromId(tx, dvid)
			tests.Assert(t, e == nil)
			tests.Assert(t, dv.Pending.Id == pol[0])
		}
		return nil
	})

	e = dvc.Exec(app.executor)
	tests.Assert(t, e == nil)

	// now we're going to pretend exec failed and inject an
	// error condition into DirvolumeDestroy

	app.xo.MockDirvolumeDestroy = func(host string, volume string, dirvolume string) error {
		return fmt.Errorf("fake error")
	}

	e = dvc.Rollback(app.executor)
	tests.Assert(t, e != nil)
	markFailedIfSupported(dvc)

	// verify that the pending items remain in the db due to rollback
	// failure
	app.db.View(func(tx *bolt.Tx) error {
		dvl, e := DirvolumeList(tx)
		tests.Assert(t, e == nil)
		tests.Assert(t, len(dvl) == 1)
		pol, e := PendingOperationList(tx)
		tests.Assert(t, e == nil)
		tests.Assert(t, len(pol) == 1)
		pop, e := NewPendingOperationEntryFromId(tx, pol[0])
		tests.Assert(t, e == nil)
		tests.Assert(t, pop.Status == FailedOperation)
		return nil
	})
}

func TestDirvolumeCreateOperationBasics(t *testing.T) {
	tmpfile := tests.Tempfile()
	defer os.Remove(tmpfile)

	// Create the app
	app := NewTestApp(tmpfile)
	defer app.Close()

	err := setupSampleDbWithTopology(app,
		1,    // clusters
		3,    // nodes_per_cluster
		1,    // devices_per_node,
		6*TB, // disksize)
	)
	tests.Assert(t, err == nil)

	var clusterId string
	err = app.db.View(func(tx *bolt.Tx) error {
		clusters, err := ClusterList(tx)
		if err != nil {
			return err
		}

		tests.Assert(t, len(clusters) == 1)
		cluster, err := NewClusterEntryFromId(tx, clusters[0])
		tests.Assert(t, err == nil)

		clusterId = cluster.Info.Id

		return nil
	})
	tests.Assert(t, err == nil)
	tests.Assert(t, clusterId != "")

	req := &api.DirvolumeCreateRequest{}
	req.Size = 1024
	req.ClusterId = clusterId

	dvol := NewDirvolumeEntryFromRequest(req)
	dvol.Info.Id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	dvc := NewDirvolumeCreateOperation(dvol, app.db)

	tests.Assert(t, dvc.Id() == dvc.op.Id)
	tests.Assert(t, dvc.Label() == "Create Dirvolume")
	tests.Assert(t, dvc.ResourceUrl() == "/dirvolumes/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
}

func TestDirvolumeDeleteOperation(t *testing.T) {
	tmpfile := tests.Tempfile()
	defer os.Remove(tmpfile)

	// Create the app
	app := NewTestApp(tmpfile)
	defer app.Close()

	err := setupSampleDbWithTopology(app,
		1,    // clusters
		3,    // nodes_per_cluster
		1,    // devices_per_node,
		6*TB, // disksize)
	)
	tests.Assert(t, err == nil)

	var clusterId string
	err = app.db.View(func(tx *bolt.Tx) error {
		clusters, err := ClusterList(tx)
		if err != nil {
			return err
		}

		tests.Assert(t, len(clusters) == 1)
		cluster, err := NewClusterEntryFromId(tx, clusters[0])
		tests.Assert(t, err == nil)

		clusterId = cluster.Info.Id

		return nil
	})
	tests.Assert(t, err == nil)
	tests.Assert(t, clusterId != "")

	req := &api.DirvolumeCreateRequest{}
	req.Size = 1024
	req.ClusterId = clusterId

	// first we need to create a dirvolume to delete
	dvol := NewDirvolumeEntryFromRequest(req)
	dvc := NewDirvolumeCreateOperation(dvol, app.db)

	e := dvc.Build()
	tests.Assert(t, e == nil)
	e = dvc.Exec(app.executor)
	tests.Assert(t, e == nil)
	e = dvc.Finalize()
	tests.Assert(t, e == nil)

	app.db.View(func(tx *bolt.Tx) error {
		po, e := PendingOperationList(tx)
		tests.Assert(t, e == nil)
		tests.Assert(t, len(po) == 0)
		return nil
	})

	dvd := NewDirvolumeDeleteOperation(dvol, app.db)
	e = dvd.Build()
	tests.Assert(t, e == nil)

	app.db.View(func(tx *bolt.Tx) error {
		po, e := PendingOperationList(tx)
		tests.Assert(t, e == nil)
		tests.Assert(t, len(po) == 1)
		return nil
	})

	e = dvd.Exec(app.executor)
	tests.Assert(t, e == nil)
	e = dvd.Finalize()
	tests.Assert(t, e == nil)

	app.db.View(func(tx *bolt.Tx) error {
		po, e := PendingOperationList(tx)
		tests.Assert(t, e == nil)
		tests.Assert(t, len(po) == 0)
		return nil
	})
}

func TestDirvolumeDeleteOperationRollback(t *testing.T) {
	tmpfile := tests.Tempfile()
	defer os.Remove(tmpfile)

	// Create the app
	app := NewTestApp(tmpfile)
	defer app.Close()

	err := setupSampleDbWithTopology(app,
		1,    // clusters
		3,    // nodes_per_cluster
		1,    // devices_per_node,
		6*TB, // disksize)
	)
	tests.Assert(t, err == nil)

	var clusterId string
	err = app.db.View(func(tx *bolt.Tx) error {
		clusters, err := ClusterList(tx)
		if err != nil {
			return err
		}

		tests.Assert(t, len(clusters) == 1)
		cluster, err := NewClusterEntryFromId(tx, clusters[0])
		tests.Assert(t, err == nil)

		clusterId = cluster.Info.Id

		return nil
	})
	tests.Assert(t, err == nil)
	tests.Assert(t, clusterId != "")

	req := &api.DirvolumeCreateRequest{}
	req.Size = 1024
	req.ClusterId = clusterId

	// first we need to create a dirvolume to delete
	dvol := NewDirvolumeEntryFromRequest(req)
	dvc := NewDirvolumeCreateOperation(dvol, app.db)

	e := dvc.Build()
	tests.Assert(t, e == nil)
	e = dvc.Exec(app.executor)
	tests.Assert(t, e == nil)
	e = dvc.Finalize()
	tests.Assert(t, e == nil)

	app.db.View(func(tx *bolt.Tx) error {
		po, e := PendingOperationList(tx)
		tests.Assert(t, e == nil)
		tests.Assert(t, len(po) == 0)
		return nil
	})

	dvd := NewDirvolumeDeleteOperation(dvol, app.db)
	e = dvd.Build()
	tests.Assert(t, e == nil)

	app.db.View(func(tx *bolt.Tx) error {
		po, e := PendingOperationList(tx)
		tests.Assert(t, e == nil)
		tests.Assert(t, len(po) == 1)
		return nil
	})

	e = dvd.Rollback(app.executor)
	tests.Assert(t, e == nil)

	app.db.View(func(tx *bolt.Tx) error {
		dvl, e := DirvolumeList(tx)
		tests.Assert(t, e == nil)
		tests.Assert(t, len(dvl) == 1)
		po, e := PendingOperationList(tx)
		tests.Assert(t, e == nil)
		tests.Assert(t, len(po) == 0)
		return nil
	})
}

func TestDirvolumeDeleteOperationTwice(t *testing.T) {
	tmpfile := tests.Tempfile()
	defer os.Remove(tmpfile)

	// Create the app
	app := NewTestApp(tmpfile)
	defer app.Close()

	err := setupSampleDbWithTopology(app,
		1,    // clusters
		3,    // nodes_per_cluster
		1,    // devices_per_node,
		6*TB, // disksize)
	)
	tests.Assert(t, err == nil)

	var clusterId string
	err = app.db.View(func(tx *bolt.Tx) error {
		clusters, err := ClusterList(tx)
		if err != nil {
			return err
		}

		tests.Assert(t, len(clusters) == 1)
		cluster, err := NewClusterEntryFromId(tx, clusters[0])
		tests.Assert(t, err == nil)

		clusterId = cluster.Info.Id

		return nil
	})
	tests.Assert(t, err == nil)
	tests.Assert(t, clusterId != "")

	req := &api.DirvolumeCreateRequest{}
	req.Size = 1024
	req.ClusterId = clusterId

	// first we need to create a dirvolume to delete
	dvol := NewDirvolumeEntryFromRequest(req)
	dvc := NewDirvolumeCreateOperation(dvol, app.db)

	e := dvc.Build()
	tests.Assert(t, e == nil)
	e = dvc.Exec(app.executor)
	tests.Assert(t, e == nil)
	e = dvc.Finalize()
	tests.Assert(t, e == nil)

	app.db.View(func(tx *bolt.Tx) error {
		po, e := PendingOperationList(tx)
		tests.Assert(t, e == nil)
		tests.Assert(t, len(po) == 0)
		return nil
	})

	dvd := NewDirvolumeDeleteOperation(dvol, app.db)
	e = dvd.Build()
	tests.Assert(t, e == nil)

	app.db.View(func(tx *bolt.Tx) error {
		po, e := PendingOperationList(tx)
		tests.Assert(t, e == nil)
		tests.Assert(t, len(po) == 1)

		dv, e := NewDirvolumeEntryFromId(tx, dvol.Info.Id)
		tests.Assert(t, e == nil)
		tests.Assert(t, dv.Pending.Id != "")
		return nil
	})

	dvd2 := NewDirvolumeDeleteOperation(dvol, app.db)
	e = dvd2.Build()
	tests.Assert(t, e == ErrConflict)
}

func TestListCompleteDirvolumesDuringOperation(t *testing.T) {
	tmpfile := tests.Tempfile()
	defer os.Remove(tmpfile)

	// Create the app
	app := NewTestApp(tmpfile)
	defer app.Close()

	err := setupSampleDbWithTopology(app,
		1,    // clusters
		3,    // nodes_per_cluster
		1,    // devices_per_node,
		6*TB, // disksize)
	)
	tests.Assert(t, err == nil)

	var clusterId string
	err = app.db.View(func(tx *bolt.Tx) error {
		clusters, err := ClusterList(tx)
		if err != nil {
			return err
		}

		tests.Assert(t, len(clusters) == 1)
		cluster, err := NewClusterEntryFromId(tx, clusters[0])
		tests.Assert(t, err == nil)

		clusterId = cluster.Info.Id

		return nil
	})
	tests.Assert(t, err == nil)
	tests.Assert(t, clusterId != "")

	t.Run("DirvolumeCreate", func(t *testing.T) {
		req := &api.DirvolumeCreateRequest{}
		req.Size = 1024
		req.ClusterId = clusterId
		dvol := NewDirvolumeEntryFromRequest(req)
		o := NewDirvolumeCreateOperation(dvol, app.db)
		e := o.Build()
		tests.Assert(t, e == nil)
		defer o.Rollback(app.executor)

		app.db.View(func(tx *bolt.Tx) error {
			dvols, err := ListCompleteDirvolumes(tx)
			tests.Assert(t, err == nil)
			tests.Assert(t, len(dvols) == 0)
			dvols, err = DirvolumeList(tx)
			tests.Assert(t, err == nil)
			tests.Assert(t, len(dvols) == 1)
			return nil
		})
	})
	t.Run("DirvolumeDelete", func(t *testing.T) {
		req := &api.DirvolumeCreateRequest{}
		req.Size = 1024
		req.ClusterId = clusterId
		dvol := NewDirvolumeEntryFromRequest(req)
		o := NewDirvolumeCreateOperation(dvol, app.db)
		e := RunOperation(o, app.executor)
		tests.Assert(t, e == nil)

		app.db.View(func(tx *bolt.Tx) error {
			dvols, err := ListCompleteDirvolumes(tx)
			tests.Assert(t, err == nil)
			tests.Assert(t, len(dvols) == 1)
			dvols, err = DirvolumeList(tx)
			tests.Assert(t, err == nil)
			tests.Assert(t, len(dvols) == 1)
			return nil
		})

		do := NewDirvolumeDeleteOperation(dvol, app.db)
		e = do.Build()
		tests.Assert(t, e == nil)
		defer func() {
			e := do.Exec(app.executor)
			tests.Assert(t, e == nil)
			e = do.Finalize()
			tests.Assert(t, e == nil)
		}()

		app.db.View(func(tx *bolt.Tx) error {
			dvols, err := ListCompleteDirvolumes(tx)
			tests.Assert(t, err == nil)
			tests.Assert(t, len(dvols) == 0)
			dvols, err = DirvolumeList(tx)
			tests.Assert(t, err == nil)
			tests.Assert(t, len(dvols) == 1)
			return nil
		})
	})
}
