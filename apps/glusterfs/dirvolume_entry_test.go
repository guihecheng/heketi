package glusterfs

import (
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/boltdb/bolt"
	"github.com/heketi/heketi/pkg/glusterfs/api"
	"github.com/heketi/heketi/pkg/utils"
	"github.com/heketi/tests"
)

func createSampleDirvolumeEntry(size int, cluster string) *DirvolumeEntry {
	req := &api.DirvolumeCreateRequest{}
	req.Size = size
	req.ClusterId = cluster

	dv := NewDirvolumeEntryFromRequest(req)

	return dv
}

func TestNewDirvolumeEntry(t *testing.T) {
	dv := NewDirvolumeEntry()

	tests.Assert(t, len(dv.Info.Id) == 0)
	tests.Assert(t, len(dv.Info.ClusterId) == 0)
}

func TestNewDirvolumeEntryFromRequestSizeCluster(t *testing.T) {
	req := &api.DirvolumeCreateRequest{}
	req.Size = 1024
	req.ClusterId = "123"

	dv := NewDirvolumeEntryFromRequest(req)
	tests.Assert(t, dv.Info.Name == "dvol_"+dv.Info.Id)
	tests.Assert(t, dv.Info.Size == 1024)
	tests.Assert(t, dv.Info.ClusterId == "123")
	tests.Assert(t, len(dv.Info.Id) != 0)
}

func TestNewDirvolumeEntryFromRequestName(t *testing.T) {
	req := &api.DirvolumeCreateRequest{}
	req.Size = 1024
	req.ClusterId = "123"
	req.Name = "mydvol"

	dv := NewDirvolumeEntryFromRequest(req)
	tests.Assert(t, dv.Info.Name == "mydvol")
	tests.Assert(t, dv.Info.Size == 1024)
	tests.Assert(t, dv.Info.ClusterId == "123")
	tests.Assert(t, len(dv.Info.Id) != 0)
}

func TestDirvolumeEntryFromIdNotFound(t *testing.T) {
	tmpfile := tests.Tempfile()
	defer os.Remove(tmpfile)

	// Create the app
	app := NewTestApp(tmpfile)
	defer app.Close()

	// Test for ID not found
	err := app.db.View(func(tx *bolt.Tx) error {
		_, err := NewDirvolumeEntryFromId(tx, "123")
		return err
	})
	tests.Assert(t, err == ErrNotFound)
}

func TestDirvolumeEntryFromId(t *testing.T) {
	tmpfile := tests.Tempfile()
	defer os.Remove(tmpfile)

	// Create the app
	app := NewTestApp(tmpfile)
	defer app.Close()

	// Create a dirvolume entry
	dv := createSampleDirvolumeEntry(1024, "123")

	// Save in database
	err := app.db.Update(func(tx *bolt.Tx) error {
		return dv.Save(tx)
	})
	tests.Assert(t, err == nil)

	// Load from database
	var entry *DirvolumeEntry
	err = app.db.View(func(tx *bolt.Tx) error {
		var err error
		entry, err = NewDirvolumeEntryFromId(tx, dv.Info.Id)
		return err
	})
	tests.Assert(t, err == nil)
	tests.Assert(t, reflect.DeepEqual(entry, dv))
}

func TestNewDirvolumeEntryMarshal(t *testing.T) {
	req := &api.DirvolumeCreateRequest{}
	req.Size = 1024
	req.ClusterId = "123"
	req.Name = "mydvol"

	dv := NewDirvolumeEntryFromRequest(req)

	buffer, err := dv.Marshal()
	tests.Assert(t, err == nil)
	tests.Assert(t, buffer != nil)
	tests.Assert(t, len(buffer) > 0)

	um := &DirvolumeEntry{}
	err = um.Unmarshal(buffer)
	tests.Assert(t, err == nil)
	tests.Assert(t, reflect.DeepEqual(dv, um))
}

func TestDirvolumeEntrySaveDelete(t *testing.T) {
	tmpfile := tests.Tempfile()
	defer os.Remove(tmpfile)

	// Create the app
	app := NewTestApp(tmpfile)
	defer app.Close()

	// Create a dirvolume entry
	dv := createSampleDirvolumeEntry(1024, "123")

	// Save in database
	err := app.db.Update(func(tx *bolt.Tx) error {
		return dv.Save(tx)
	})
	tests.Assert(t, err == nil)

	var entry *DirvolumeEntry
	err = app.db.Update(func(tx *bolt.Tx) error {
		var err error
		entry, err = NewDirvolumeEntryFromId(tx, dv.Info.Id)
		if err != nil {
			return err
		}

		err = entry.Delete(tx)
		if err != nil {
			return err
		}

		return nil

	})
	tests.Assert(t, err == nil)

	// Check dirvolume has been deleted and is not in db
	err = app.db.View(func(tx *bolt.Tx) error {
		var err error
		entry, err = NewDirvolumeEntryFromId(tx, dv.Info.Id)
		if err != nil {
			return err
		}
		return nil

	})
	tests.Assert(t, err == ErrNotFound)
}

func TestNewDirvolumeEntryNewInfoResponse(t *testing.T) {
	tmpfile := tests.Tempfile()
	defer os.Remove(tmpfile)

	// Create the app
	app := NewTestApp(tmpfile)
	defer app.Close()

	// Create a dirvolume entry
	dv := createSampleDirvolumeEntry(1024, "123")

	// Save in database
	err := app.db.Update(func(tx *bolt.Tx) error {
		return dv.Save(tx)
	})
	tests.Assert(t, err == nil)

	// Retrieve info response
	var info *api.DirvolumeInfoResponse
	err = app.db.View(func(tx *bolt.Tx) error {
		dvol, err := NewDirvolumeEntryFromId(tx, dv.Info.Id)
		if err != nil {
			return err
		}

		info, err = dvol.NewInfoResponse(tx)
		if err != nil {
			return err
		}

		return nil

	})
	tests.Assert(t, err == nil, err)

	tests.Assert(t, info.ClusterId == dv.Info.ClusterId)
	tests.Assert(t, info.Name == dv.Info.Name)
	tests.Assert(t, info.Id == dv.Info.Id)
	tests.Assert(t, info.Size == dv.Info.Size)
}

func TestDirvolumeEntryCreateMissingCluster(t *testing.T) {
	tmpfile := tests.Tempfile()
	defer os.Remove(tmpfile)

	// Create the app
	app := NewTestApp(tmpfile)
	defer app.Close()

	// Create a dirvolume entry
	dv := createSampleDirvolumeEntry(1024, "123")

	// Save in database
	err := app.db.Update(func(tx *bolt.Tx) error {
		return dv.Save(tx)
	})
	tests.Assert(t, err == nil)

	err = dv.createDirvolume(app.db, app.executor)
	tests.Assert(t, err != nil, "expected err != nil")
	tests.Assert(t, strings.Contains(err.Error(), "Id not found"),
		`expected strings.Contains(err.Error(), "Id not found"), got:`, err)
}

func TestDirvolumeEntryDestroy(t *testing.T) {
	tmpfile := tests.Tempfile()
	defer os.Remove(tmpfile)

	// Create the app
	app := NewTestApp(tmpfile)
	defer app.Close()

	// Create a cluster
	err := setupSampleDbWithTopology(app,
		1,      // clusters
		3,      // nodes_per_cluster
		1,      // devices_per_node,
		500*GB, // disksize)
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

	// Create a dirvolume entry
	dv := createSampleDirvolumeEntry(1024, clusterId)

	err = dv.createDirvolume(app.db, app.executor)
	tests.Assert(t, err == nil)

	// Destroy the dirvolume
	err = dv.destroyDirvolume(app.db, app.executor)
	tests.Assert(t, err == nil)
}

func TestDirvolumeCreateConcurrent(t *testing.T) {
	tmpfile := tests.Tempfile()
	defer os.Remove(tmpfile)

	// Create the app
	app := NewTestApp(tmpfile)
	defer app.Close()

	err := setupSampleDbWithTopology(app,
		1,      // clusters
		3,      // nodes_per_cluster
		1,      // devices_per_node,
		500*GB, // disksize)
	)

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

	sg := utils.NewStatusGroup()
	dvols := [](*DirvolumeEntry){}
	for i := 0; i < 9; i++ {
		req := &api.DirvolumeCreateRequest{}
		req.Size = 4
		req.ClusterId = clusterId
		dv := NewDirvolumeEntryFromRequest(req)
		dvols = append(dvols, dv)

		sg.Add(1)
		go func() {
			defer sg.Done()
			err := dv.createDirvolume(app.db, app.executor)
			sg.Err(err)
		}()
	}

	err = sg.Result()
	tests.Assert(t, err == nil, "expected err == nil, got:", err)
}

func TestDirvolumeEntryExpand(t *testing.T) {
	tmpfile := tests.Tempfile()
	defer os.Remove(tmpfile)

	// Create the app
	app := NewTestApp(tmpfile)
	defer app.Close()

	// Create a cluster
	err := setupSampleDbWithTopology(app,
		1,      // clusters
		3,      // nodes_per_cluster
		1,      // devices_per_node,
		500*GB, // disksize)
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

	// Create a dirvolume entry
	dv := createSampleDirvolumeEntry(1024, clusterId)

	err = dv.createDirvolume(app.db, app.executor)
	tests.Assert(t, err == nil)

	// Expand the dirvolume
	err = dv.expandDirvolume(app.db, app.executor)
	tests.Assert(t, err == nil)
}

func TestAppendExport(t *testing.T) {
	dvName1 := "dir1"
	dvName2 := "dir2"
	exportDirStr1 := ""

	res1 := appendExport(dvName1, exportDirStr1)
	tests.Assert(t, res1 == "/dir1(127.0.0.1)")
	res2 := appendExport(dvName2, res1)
	tests.Assert(t, res2 == "/dir1(127.0.0.1),/dir2(127.0.0.1)")
}

func TestDeleteExport(t *testing.T) {
	dvName1 := "dir1"
	dvName2 := "dir2"
	exportDirStr := "/dir1(127.0.0.1),/dir2(127.0.0.1)"

	res1 := deleteExport(dvName2, exportDirStr)
	tests.Assert(t, res1 == "/dir1(127.0.0.1)")
	res2 := deleteExport(dvName1, res1)
	tests.Assert(t, res2 == "")
}

func TestAppendIpListToExport(t *testing.T) {
	dvName1 := "dir1"
	dvName2 := "dir2"
	exportDirStr := "/dir1(127.0.0.1),/dir2(127.0.0.1)"

	ipList1 := []string{"10.0.0.1"}
	ipList2 := []string{"10.0.0.1", "10.0.0.2"}

	res1 := appendIpListToExport(dvName1, ipList1, exportDirStr)
	tests.Assert(t, res1 == "/dir1(127.0.0.1|10.0.0.1),/dir2(127.0.0.1)")
	res2 := appendIpListToExport(dvName2, ipList2, res1)
	tests.Assert(t, res2 == "/dir1(127.0.0.1|10.0.0.1),/dir2(127.0.0.1|10.0.0.1|10.0.0.2)")
}

func TestDeleteIpListToExport(t *testing.T) {
	dvName1 := "dir1"
	dvName2 := "dir2"
	exportDirStr := "/dir1(127.0.0.1|10.0.0.1),/dir2(127.0.0.1|10.0.0.1|10.0.0.2)"

	ipList1 := []string{"10.0.0.1"}
	ipList2 := []string{"10.0.0.1", "10.0.0.2"}

	res1 := deleteIpListToExport(dvName2, ipList2, exportDirStr)
	tests.Assert(t, res1 == "/dir1(127.0.0.1|10.0.0.1),/dir2(127.0.0.1)", res1)
	res2 := deleteIpListToExport(dvName1, ipList1, res1)
	tests.Assert(t, res2 == "/dir1(127.0.0.1),/dir2(127.0.0.1)")
}

func TestDirvolumeEntryExportUnexport(t *testing.T) {
	tmpfile := tests.Tempfile()
	defer os.Remove(tmpfile)

	// Create the app
	app := NewTestApp(tmpfile)
	defer app.Close()

	// Create a cluster
	err := setupSampleDbWithTopology(app,
		1,      // clusters
		3,      // nodes_per_cluster
		1,      // devices_per_node,
		500*GB, // disksize)
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

	// Create a dirvolume entry
	dv := createSampleDirvolumeEntry(1024, clusterId)

	err = dv.createDirvolume(app.db, app.executor)
	tests.Assert(t, err == nil)

	// Export the dirvolume
	err = dv.exportDirvolume(app.db, app.executor)
	tests.Assert(t, err == nil)

	// Unexport the dirvolume
	err = dv.unexportDirvolume(app.db, app.executor)
	tests.Assert(t, err == nil)
}
