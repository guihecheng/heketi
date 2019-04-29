package client

import (
	"bytes"
	"encoding/json"
	"net/http"
	"time"

	"github.com/heketi/heketi/pkg/glusterfs/api"
	"github.com/heketi/heketi/pkg/utils"
)

func (c *Client) SubvolumeCreate(request *api.SubvolumeCreateRequest) (
	*api.SubvolumeInfoResponse, error) {

	// Marshal request to JSON
	buffer, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}

	// Create a request
	req, err := http.NewRequest("POST",
		c.host+"/subvolumes",
		bytes.NewBuffer(buffer))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	// Set token
	err = c.setToken(req)
	if err != nil {
		return nil, err
	}

	// Send request
	r, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusAccepted {
		return nil, utils.GetErrorFromResponse(r)
	}

	// Wait for response
	r, err = c.waitForResponseWithTimer(r, time.Second)
	if err != nil {
		return nil, err
	}
	if r.StatusCode != http.StatusOK {
		return nil, utils.GetErrorFromResponse(r)
	}

	// Read JSON response
	var subvolume api.SubvolumeInfoResponse
	err = utils.GetJsonFromResponse(r, &subvolume)
	if err != nil {
		return nil, err
	}

	return &subvolume, nil

}

func (c *Client) SubvolumeDelete(id string) error {

	// Create a request
	req, err := http.NewRequest("DELETE", c.host+"/subvolumes/"+id, nil)
	if err != nil {
		return err
	}

	// Set token
	err = c.setToken(req)
	if err != nil {
		return err
	}

	// Send request
	r, err := c.do(req)
	if err != nil {
		return err
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusAccepted {
		return utils.GetErrorFromResponse(r)
	}

	// Wait for response
	r, err = c.waitForResponseWithTimer(r, time.Second)
	if err != nil {
		return err
	}
	if r.StatusCode != http.StatusNoContent {
		return utils.GetErrorFromResponse(r)
	}

	return nil
}

func (c *Client) SubvolumeInfo(id string) (*api.SubvolumeInfoResponse, error) {

	// Create request
	req, err := http.NewRequest("GET", c.host+"/subvolumes/"+id, nil)
	if err != nil {
		return nil, err
	}

	// Set token
	err = c.setToken(req)
	if err != nil {
		return nil, err
	}

	// Get info
	r, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusOK {
		return nil, utils.GetErrorFromResponse(r)
	}

	// Read JSON response
	var subvolume api.SubvolumeInfoResponse
	err = utils.GetJsonFromResponse(r, &subvolume)
	if err != nil {
		return nil, err
	}

	return &subvolume, nil
}

func (c *Client) SubvolumeList() (*api.SubvolumeListResponse, error) {

	// Create request
	req, err := http.NewRequest("GET", c.host+"/subvolumes", nil)
	if err != nil {
		return nil, err
	}

	// Set token
	err = c.setToken(req)
	if err != nil {
		return nil, err
	}

	// Get info
	r, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusOK {
		return nil, utils.GetErrorFromResponse(r)
	}

	// Read JSON response
	var subvolumes api.SubvolumeListResponse
	err = utils.GetJsonFromResponse(r, &subvolumes)
	if err != nil {
		return nil, err
	}

	return &subvolumes, nil
}
