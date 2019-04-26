package glusterfs

import (
	"encoding/json"
	"net/http"

	"github.com/boltdb/bolt"
	"github.com/gorilla/mux"
	"github.com/heketi/heketi/pkg/glusterfs/api"
	"github.com/heketi/heketi/pkg/utils"
)

func (a *App) SubvolumeCreate(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	vol_id := vars["id"]

	var msg api.SubvolumeCreateRequest
	err := utils.GetJsonFromRequest(r, &msg)
	if err != nil {
		http.Error(w, "request unable to be parsed", 422)
		return
	}
	/*
		err = msg.Validate()
		if err != nil {
			http.Error(w, "validation failed: "+err.Error(), http.StatusBadRequest)
			logger.LogError("validation failed: " + err.Error())
			return
		}
	*/

	if msg.Size < 1 {
		http.Error(w, "Invalid subvolume size", http.StatusBadRequest)
		logger.LogError("Invalid subvolume size")
		return
	}

	// Check that the volume requested is available
	var volume *VolumeEntry
	err = a.db.View(func(tx *bolt.Tx) error {
		var err error // needed otherwise 'volume' will be nil after View()
		volume, err = NewVolumeEntryFromId(tx, vol_id)
		if err == ErrNotFound || !volume.Visible() {
			// treat an invisible volume like it doesn't exist
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

	svol := NewSubvolumeEntryFromRequest(&msg)

	svc := NewSubvolumeCreateOperation(svol, a.db)
	if err := AsyncHttpOperation(a, w, r, svc); err != nil {
		OperationHttpErrorf(w, err, "Failed to allocate new subvolume: %v", err)
		return
	}
}

func (a *App) SubvolumeInfo(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	var info *api.SubvolumeInfoResponse
	err := a.db.View(func(tx *bolt.Tx) error {
		entry, err := NewSubvolumeEntryFromId(tx, id)
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

func (a *App) SubvolumeDelete(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	var svol *SubvolumeEntry
	err := a.db.View(func(tx *bolt.Tx) error {

		var err error
		svol, err = NewSubvolumeEntryFromId(tx, id)
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

	svd := NewSubvolumeDeleteOperation(svol, a.db)
	if err := AsyncHttpOperation(a, w, r, svd); err != nil {
		OperationHttpErrorf(w, err, "Failed to set up subvolume delete: %v", err)
		return
	}
}

func (a *App) SubvolumeList(w http.ResponseWriter, r *http.Request) {

	var list api.SubvolumeListResponse

	err := a.db.View(func(tx *bolt.Tx) error {
		var err error

		list.Subvolumes, err = ListCompleteSubvolumes(tx)
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
