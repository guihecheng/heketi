package glusterfs

import (
	"encoding/json"
	"net/http"

	"github.com/boltdb/bolt"
	"github.com/gorilla/mux"
	"github.com/heketi/heketi/pkg/glusterfs/api"
	"github.com/heketi/heketi/pkg/utils"
)

func (a *App) DirvolumeCreate(w http.ResponseWriter, r *http.Request) {
	var msg api.DirvolumeCreateRequest
	err := utils.GetJsonFromRequest(r, &msg)
	if err != nil {
		http.Error(w, "request unable to be parsed", 422)
		return
	}

	err = msg.Validate()
	if err != nil {
		http.Error(w, "validation failed: "+err.Error(), http.StatusBadRequest)
		logger.LogError("validation failed: " + err.Error())
		return
	}

	if msg.Size < 1 {
		http.Error(w, "Invalid dirvolume size", http.StatusBadRequest)
		logger.LogError("Invalid dirvolume size")
		return
	}

	// Check that the cluster requested is available
	err = a.db.View(func(tx *bolt.Tx) error {
		var err error // needed otherwise 'cluster' will be nil after View()
		_, err = NewClusterEntryFromId(tx, msg.ClusterId)
		if err == ErrNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
			return err
		} else if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return err
		}

		return nil
	})
	if err != nil {
		return
	}

	dvol := NewDirvolumeEntryFromRequest(&msg)

	dvc := NewDirvolumeCreateOperation(dvol, a.db)
	if err := AsyncHttpOperation(a, w, r, dvc); err != nil {
		OperationHttpErrorf(w, err, "Failed to allocate new dirvolume: %v", err)
		return
	}
}

func (a *App) DirvolumeInfo(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	var info *api.DirvolumeInfoResponse
	err := a.db.View(func(tx *bolt.Tx) error {
		entry, err := NewDirvolumeEntryFromId(tx, id)
		if err == ErrNotFound /* || !entry.Visible() */ {
			// treat an invisible entry like it doesn't exist
			http.Error(w, "Id not found", http.StatusNotFound)
			return ErrNotFound
		} else if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return err
		}

		info, err = entry.NewInfoResponse(tx)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return err
		}

		return nil
	})
	if err != nil {
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(info); err != nil {
		panic(err)
	}
}

func (a *App) DirvolumeDelete(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	var dvol *DirvolumeEntry
	err := a.db.View(func(tx *bolt.Tx) error {

		var err error
		dvol, err = NewDirvolumeEntryFromId(tx, id)
		if err == ErrNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
			return err
		} else if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return err
		}

		return nil
	})
	if err != nil {
		return
	}

	dvd := NewDirvolumeDeleteOperation(dvol, a.db)
	if err := AsyncHttpOperation(a, w, r, dvd); err != nil {
		OperationHttpErrorf(w, err, "Failed to set up dirvolume delete: %v", err)
		return
	}
}

func (a *App) DirvolumeList(w http.ResponseWriter, r *http.Request) {

	var list api.DirvolumeListResponse

	err := a.db.View(func(tx *bolt.Tx) error {
		var err error

		list.Dirvolumes, err = ListCompleteDirvolumes(tx)
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		logger.Err(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Send list back
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(list); err != nil {
		panic(err)
	}
}

func (a *App) DirvolumeExpand(w http.ResponseWriter, r *http.Request) {
	var msg api.DirvolumeExpandRequest
	err := utils.GetJsonFromRequest(r, &msg)
	if err != nil {
		http.Error(w, "request unable to be parsed", 422)
		return
	}

	err = msg.Validate()
	if err != nil {
		http.Error(w, "validation failed: "+err.Error(), http.StatusBadRequest)
		logger.LogError("validation failed: " + err.Error())
		return
	}

	if msg.Size < 1 {
		http.Error(w, "Invalid dirvolume size", http.StatusBadRequest)
		logger.LogError("Invalid dirvolume size")
		return
	}

	vars := mux.Vars(r)
	id := vars["id"]

	var dvol *DirvolumeEntry
	err = a.db.View(func(tx *bolt.Tx) error {
		var err error
		dvol, err = NewDirvolumeEntryFromId(tx, id)
		if err == ErrNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
			return err
		} else if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return err
		}

		return nil
	})
	if err != nil {
		return
	}

	dve := NewDirvolumeExpandOperation(dvol, a.db, msg.Size)
	if err := AsyncHttpOperation(a, w, r, dve); err != nil {
		OperationHttpErrorf(w, err, "Failed to set up dirvolume expand: %v", err)
		return
	}
}

func (a *App) DirvolumeExport(w http.ResponseWriter, r *http.Request) {
	var msg api.DirvolumeExportRequest
	err := utils.GetJsonFromRequest(r, &msg)
	if err != nil {
		http.Error(w, "request unable to be parsed", 422)
		return
	}

	err = msg.Validate()
	if err != nil {
		http.Error(w, "validation failed: "+err.Error(), http.StatusBadRequest)
		logger.LogError("validation failed: " + err.Error())
		return
	}

	vars := mux.Vars(r)
	id := vars["id"]

	var dvol *DirvolumeEntry
	err = a.db.View(func(tx *bolt.Tx) error {
		var err error
		dvol, err = NewDirvolumeEntryFromId(tx, id)
		if err == ErrNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
			return err
		} else if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return err
		}

		return nil
	})
	if err != nil {
		return
	}

	dvx := NewDirvolumeExportOperation(dvol, a.db, msg.IpList)
	if err := AsyncHttpOperation(a, w, r, dvx); err != nil {
		OperationHttpErrorf(w, err, "Failed to set up dirvolume export: %v", err)
		return
	}
}

func (a *App) DirvolumeUnexport(w http.ResponseWriter, r *http.Request) {
	var msg api.DirvolumeExportRequest
	err := utils.GetJsonFromRequest(r, &msg)
	if err != nil {
		http.Error(w, "request unable to be parsed", 422)
		return
	}

	err = msg.Validate()
	if err != nil {
		http.Error(w, "validation failed: "+err.Error(), http.StatusBadRequest)
		logger.LogError("validation failed: " + err.Error())
		return
	}

	if len(msg.IpList) == 0 {
		http.Error(w, "Empty IP list", http.StatusBadRequest)
		logger.LogError("Empty IP list")
		return
	}

	vars := mux.Vars(r)
	id := vars["id"]

	var dvol *DirvolumeEntry
	err = a.db.View(func(tx *bolt.Tx) error {
		var err error
		dvol, err = NewDirvolumeEntryFromId(tx, id)
		if err == ErrNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
			return err
		} else if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return err
		}

		return nil
	})
	if err != nil {
		return
	}

	dvx := NewDirvolumeUnexportOperation(dvol, a.db, msg.IpList)
	if err := AsyncHttpOperation(a, w, r, dvx); err != nil {
		OperationHttpErrorf(w, err, "Failed to set up dirvolume unexport: %v", err)
		return
	}
}

func (a *App) DirvolumeStats(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	var stats *api.DirvolumeStatsResponse
	var dv *DirvolumeEntry
	err := a.db.View(func(tx *bolt.Tx) error {
		entry, err := NewDirvolumeEntryFromId(tx, id)
		if err == ErrNotFound {
			http.Error(w, "Id not found", http.StatusNotFound)
			return ErrNotFound
		} else if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return err
		}

		dv = entry

		return nil
	})
	if err != nil {
		return
	}

	for attempt := 0; attempt < DIRVOLUME_MAX_RETRIES; attempt++ {
		stats, err = dv.statDirvolume(a.db, a.executor)
		if err == nil {
			break
		}
		logger.Info("Retrying stats")
	}

	if err != nil {
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(stats); err != nil {
		panic(err)
	}
}
