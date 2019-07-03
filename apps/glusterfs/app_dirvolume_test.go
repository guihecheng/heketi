package glusterfs

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/boltdb/bolt"
	"github.com/gorilla/mux"
	"github.com/heketi/heketi/pkg/glusterfs/api"
	"github.com/heketi/heketi/pkg/utils"
	"github.com/heketi/tests"
)

func TestDirvolumeCreate(t *testing.T) {
	tmpfile := tests.Tempfile()
	defer os.Remove(tmpfile)

	// Create the app
	app := NewTestApp(tmpfile)
	defer app.Close()
	router := mux.NewRouter()
	app.SetRoutes(router)

	// Setup the server
	ts := httptest.NewServer(router)
	defer ts.Close()

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

	// VolumeCreate using default durability
	request := []byte(`{
        "size" : 100,
		"cluster": "` + clusterId + `"
    }`)

	// Send request
	r, err := http.Post(ts.URL+"/dirvolumes", "application/json", bytes.NewBuffer(request))
	tests.Assert(t, err == nil)
	tests.Assert(t, r.StatusCode == http.StatusAccepted)
	location, err := r.Location()
	tests.Assert(t, err == nil)

	// Query queue until finished
	var info api.DirvolumeInfoResponse
	for {
		r, err = http.Get(location.String())
		tests.Assert(t, err == nil)
		tests.Assert(t, r.StatusCode == http.StatusOK)
		if r.ContentLength <= 0 {
			time.Sleep(time.Millisecond * 10)
			continue
		} else {
			// Should have node information here
			tests.Assert(t, r.Header.Get("Content-Type") == "application/json; charset=UTF-8")
			err = utils.GetJsonFromResponse(r, &info)
			tests.Assert(t, err == nil)
			break
		}
	}
	tests.Assert(t, info.Id != "")
	tests.Assert(t, info.Size == 100)
	tests.Assert(t, info.ClusterId != "")
	tests.Assert(t, info.Name == "dvol_"+info.Id)
}

func TestDirvolumeCreateBadJson(t *testing.T) {
	tmpfile := tests.Tempfile()
	defer os.Remove(tmpfile)

	// Create the app
	app := NewTestApp(tmpfile)
	defer app.Close()
	router := mux.NewRouter()
	app.SetRoutes(router)

	// Setup the server
	ts := httptest.NewServer(router)
	defer ts.Close()

	// VolumeCreate JSON Request
	request := []byte(`{
        asdfsdf
    }`)

	// Send request
	r, err := http.Post(ts.URL+"/dirvolumes", "application/json", bytes.NewBuffer(request))
	tests.Assert(t, err == nil)
	tests.Assert(t, r.StatusCode == 422)
}

func TestDirvolumeCreateInvalidSize(t *testing.T) {
	tmpfile := tests.Tempfile()
	defer os.Remove(tmpfile)

	// Create the app
	app := NewTestApp(tmpfile)
	defer app.Close()
	router := mux.NewRouter()
	app.SetRoutes(router)

	// Setup the server
	ts := httptest.NewServer(router)
	defer ts.Close()

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

	// VolumeCreate JSON Request
	request := []byte(`{
        "size" : 0,
		"cluster": "` + clusterId + `"
    }`)

	// Send request
	r, err := http.Post(ts.URL+"/dirvolumes", "application/json", bytes.NewBuffer(request))
	tests.Assert(t, err == nil)
	tests.Assert(t, r.StatusCode == http.StatusBadRequest)
	body, err := ioutil.ReadAll(io.LimitReader(r.Body, r.ContentLength))
	tests.Assert(t, err == nil)
	r.Body.Close()
	tests.Assert(t, strings.Contains(string(body), "size: cannot be blank"), string(body))
}

func TestDirvolumeCreateBadClusters(t *testing.T) {
	tmpfile := tests.Tempfile()
	defer os.Remove(tmpfile)

	// Create the app
	app := NewTestApp(tmpfile)
	defer app.Close()
	router := mux.NewRouter()
	app.SetRoutes(router)

	// Setup the server
	ts := httptest.NewServer(router)
	defer ts.Close()

	err := setupSampleDbWithTopology(app,
		1,    // clusters
		3,    // nodes_per_cluster
		1,    // devices_per_node,
		6*TB, // disksize)
	)
	tests.Assert(t, err == nil)

	// VolumeCreate JSON Request
	// fill with a bad clusterId
	request := []byte(`{
        "size" : 10,
        "cluster" : "bad"
    }`)

	// Send request
	r, err := http.Post(ts.URL+"/dirvolumes", "application/json", bytes.NewBuffer(request))
	tests.Assert(t, err == nil)
	tests.Assert(t, r.StatusCode == http.StatusBadRequest)
	body, err := ioutil.ReadAll(io.LimitReader(r.Body, r.ContentLength))
	tests.Assert(t, err == nil)
	r.Body.Close()
	tests.Assert(t, strings.Contains(string(body), "cluster: bad is not a valid UUID"))
}

func TestDirvolumeInfoIdNotFound(t *testing.T) {
	tmpfile := tests.Tempfile()
	defer os.Remove(tmpfile)

	// Create the app
	app := NewTestApp(tmpfile)
	defer app.Close()
	router := mux.NewRouter()
	app.SetRoutes(router)

	// Setup the server
	ts := httptest.NewServer(router)
	defer ts.Close()

	// Now that we have some data in the database, we can
	// make a request for the clutser list
	r, err := http.Get(ts.URL + "/dirvolumes/12345")
	tests.Assert(t, r.StatusCode == http.StatusNotFound)
	tests.Assert(t, err == nil)
}

func TestDirvolumeInfo(t *testing.T) {
	tmpfile := tests.Tempfile()
	defer os.Remove(tmpfile)

	// Create the app
	app := NewTestApp(tmpfile)
	defer app.Close()
	router := mux.NewRouter()
	app.SetRoutes(router)

	// Setup the server
	ts := httptest.NewServer(router)
	defer ts.Close()

	// Setup database
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

	// Create a dirvolume
	var dv *DirvolumeEntry
	err = app.db.Update(func(tx *bolt.Tx) error {
		dv = createSampleDirvolumeEntry(100, clusterId)
		err := dv.Save(tx)
		if err != nil {
			return err
		}
		return nil

	})
	tests.Assert(t, err == nil)
	tests.Assert(t, dv != nil)

	// Now that we have some data in the database, we can
	// make a request for the clutser list
	r, err := http.Get(ts.URL + "/dirvolumes/" + dv.Info.Id)
	tests.Assert(t, r.StatusCode == http.StatusOK, r.StatusCode, ts.URL, dv.Info.Id)
	tests.Assert(t, err == nil)
	tests.Assert(t, r.Header.Get("Content-Type") == "application/json; charset=UTF-8")

	// Read response
	var msg api.DirvolumeInfoResponse
	err = utils.GetJsonFromResponse(r, &msg)
	tests.Assert(t, err == nil)

	tests.Assert(t, msg.Id == dv.Info.Id)
	tests.Assert(t, msg.ClusterId == dv.Info.ClusterId)
	tests.Assert(t, msg.Name == dv.Info.Name)
	tests.Assert(t, msg.Size == dv.Info.Size)
}

func TestDirvolumeList(t *testing.T) {
	tmpfile := tests.Tempfile()
	defer os.Remove(tmpfile)

	// Create the app
	app := NewTestApp(tmpfile)
	defer app.Close()
	router := mux.NewRouter()
	app.SetRoutes(router)

	// Setup the server
	ts := httptest.NewServer(router)
	defer ts.Close()

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

	// Create some dirvolumes
	numDirvolumes := 1000
	err = app.db.Update(func(tx *bolt.Tx) error {

		for i := 0; i < numDirvolumes; i++ {
			dv := createSampleDirvolumeEntry(100, clusterId)
			err := dv.Save(tx)
			if err != nil {
				return err
			}
		}

		return nil

	})
	tests.Assert(t, err == nil)

	// Get dirvolumes
	r, err := http.Get(ts.URL + "/dirvolumes")
	tests.Assert(t, r.StatusCode == http.StatusOK)
	tests.Assert(t, err == nil)
	tests.Assert(t, r.Header.Get("Content-Type") == "application/json; charset=UTF-8")

	// Read response
	var msg api.DirvolumeListResponse
	err = utils.GetJsonFromResponse(r, &msg)
	tests.Assert(t, err == nil)
	tests.Assert(t, len(msg.Dirvolumes) == numDirvolumes)

	// Check that all the volumes are in the database
	err = app.db.View(func(tx *bolt.Tx) error {
		for _, id := range msg.Dirvolumes {
			_, err := NewDirvolumeEntryFromId(tx, id)
			if err != nil {
				return err
			}
		}

		return nil
	})
	tests.Assert(t, err == nil)
}

func TestDirvolumeListEmpty(t *testing.T) {
	tmpfile := tests.Tempfile()
	defer os.Remove(tmpfile)

	// Create the app
	app := NewTestApp(tmpfile)
	defer app.Close()
	router := mux.NewRouter()
	app.SetRoutes(router)

	// Setup the server
	ts := httptest.NewServer(router)
	defer ts.Close()

	// Get dirvolumes, there should be none
	r, err := http.Get(ts.URL + "/dirvolumes")
	tests.Assert(t, r.StatusCode == http.StatusOK)
	tests.Assert(t, err == nil)
	tests.Assert(t, r.Header.Get("Content-Type") == "application/json; charset=UTF-8")

	// Read response
	var msg api.DirvolumeListResponse
	err = utils.GetJsonFromResponse(r, &msg)
	tests.Assert(t, err == nil)
	tests.Assert(t, len(msg.Dirvolumes) == 0)
}

func TestDirvolumeDelete(t *testing.T) {
	tmpfile := tests.Tempfile()
	defer os.Remove(tmpfile)

	// Create the app
	app := NewTestApp(tmpfile)
	defer app.Close()
	router := mux.NewRouter()
	app.SetRoutes(router)

	// Setup the server
	ts := httptest.NewServer(router)
	defer ts.Close()

	// Setup database
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

	// Create a dirvolume
	var dv *DirvolumeEntry
	err = app.db.Update(func(tx *bolt.Tx) error {
		dv = createSampleDirvolumeEntry(100, clusterId)
		err := dv.Save(tx)
		if err != nil {
			return err
		}
		return nil

	})
	tests.Assert(t, err == nil)
	tests.Assert(t, dv != nil)

	// Delete the dirvolume
	req, err := http.NewRequest("DELETE", ts.URL+"/dirvolumes/"+dv.Info.Id, nil)
	tests.Assert(t, err == nil)
	r, err := http.DefaultClient.Do(req)
	tests.Assert(t, err == nil)
	tests.Assert(t, r.StatusCode == http.StatusAccepted)
	location, err := r.Location()
	tests.Assert(t, err == nil)

	// Query queue until finished
	for {
		r, err = http.Get(location.String())
		tests.Assert(t, err == nil)
		if r.Header.Get("X-Pending") == "true" {
			tests.Assert(t, r.StatusCode == http.StatusOK)
			time.Sleep(time.Millisecond * 10)
			continue
		} else {
			tests.Assert(t, r.StatusCode == http.StatusNoContent)
			tests.Assert(t, err == nil)
			break
		}
	}

	// Check it is not there
	r, err = http.Get(ts.URL + "/dirvolumes/" + dv.Info.Id)
	tests.Assert(t, r.StatusCode == http.StatusNotFound)
	tests.Assert(t, err == nil)
}

func TestDirvolumeDeleteIdNotFound(t *testing.T) {
	tmpfile := tests.Tempfile()
	defer os.Remove(tmpfile)

	// Create the app
	app := NewTestApp(tmpfile)
	defer app.Close()
	router := mux.NewRouter()
	app.SetRoutes(router)

	// Setup the server
	ts := httptest.NewServer(router)
	defer ts.Close()

	// Now that we have some data in the database, we can
	// make a request for the clutser list
	req, err := http.NewRequest("DELETE", ts.URL+"/dirvolumes/12345", nil)
	tests.Assert(t, err == nil)
	r, err := http.DefaultClient.Do(req)
	tests.Assert(t, err == nil)
	tests.Assert(t, r.StatusCode == http.StatusNotFound)
}

func TestDirvolumeExpand(t *testing.T) {
	tmpfile := tests.Tempfile()
	defer os.Remove(tmpfile)

	// Create the app
	app := NewTestApp(tmpfile)
	defer app.Close()
	router := mux.NewRouter()
	app.SetRoutes(router)

	// Setup the server
	ts := httptest.NewServer(router)
	defer ts.Close()

	// Setup database
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

	// Create a dirvolume
	var dv *DirvolumeEntry
	err = app.db.Update(func(tx *bolt.Tx) error {
		dv = createSampleDirvolumeEntry(100, clusterId)
		err := dv.Save(tx)
		if err != nil {
			return err
		}
		return nil

	})
	tests.Assert(t, err == nil)
	tests.Assert(t, dv != nil)

	// JSON Request
	request := []byte(`{
        "expand_size" : 1000
    }`)

	// Send request
	r, err := http.Post(ts.URL+"/dirvolumes/"+dv.Info.Id+"/expand",
		"application/json",
		bytes.NewBuffer(request))
	tests.Assert(t, err == nil)
	tests.Assert(t, r.StatusCode == http.StatusAccepted)
	location, err := r.Location()
	tests.Assert(t, err == nil)

	// Query queue until finished
	var info api.DirvolumeInfoResponse
	for {
		r, err = http.Get(location.String())
		tests.Assert(t, err == nil)
		tests.Assert(t, r.StatusCode == http.StatusOK)
		if r.Header.Get("X-Pending") == "true" {
			time.Sleep(time.Millisecond * 10)
			continue
		} else {
			err = utils.GetJsonFromResponse(r, &info)
			tests.Assert(t, err == nil)
			break
		}
	}

	tests.Assert(t, info.Size == 100+1000, info.Size)
}

func TestDirvolumeExpandBadJson(t *testing.T) {
	tmpfile := tests.Tempfile()
	defer os.Remove(tmpfile)

	// Create the app
	app := NewTestApp(tmpfile)
	defer app.Close()
	router := mux.NewRouter()
	app.SetRoutes(router)

	// Setup the server
	ts := httptest.NewServer(router)
	defer ts.Close()

	// VolumeExpand JSON Request
	request := []byte(`{
        asdfsdf  0
    }`)

	// Send request
	r, err := http.Post(ts.URL+"/dirvolumes", "application/json", bytes.NewBuffer(request))
	tests.Assert(t, err == nil)
	tests.Assert(t, r.StatusCode == 422)
}

func TestDirvolumeExportBadJson(t *testing.T) {
	tmpfile := tests.Tempfile()
	defer os.Remove(tmpfile)

	// Create the app
	app := NewTestApp(tmpfile)
	defer app.Close()
	router := mux.NewRouter()
	app.SetRoutes(router)

	// Setup the server
	ts := httptest.NewServer(router)
	defer ts.Close()

	// VolumeExpand JSON Request
	request := []byte(`{
		"asdfghjkl"
    }`)

	// Send request
	r, err := http.Post(ts.URL+"/dirvolumes", "application/json", bytes.NewBuffer(request))
	tests.Assert(t, err == nil)
	tests.Assert(t, r.StatusCode == 422)
}

func TestDirvolumeExportBadIP(t *testing.T) {
	tmpfile := tests.Tempfile()
	defer os.Remove(tmpfile)

	// Create the app
	app := NewTestApp(tmpfile)
	defer app.Close()
	router := mux.NewRouter()
	app.SetRoutes(router)

	// Setup the server
	ts := httptest.NewServer(router)
	defer ts.Close()

	// VolumeExpand JSON Request
	request := []byte(`{
        "iplist" : [
            "10.0.0"
        ]
    }`)

	// Send request
	r, err := http.Post(ts.URL+"/dirvolumes", "application/json", bytes.NewBuffer(request))
	tests.Assert(t, err == nil)
	tests.Assert(t, r.StatusCode == 400)
}
