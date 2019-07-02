package client

import (
	"bytes"
	"encoding/json"
	"net/http"
	"time"

	"github.com/heketi/heketi/pkg/glusterfs/api"
	"github.com/heketi/heketi/pkg/utils"
)

func (c *Client) DirvolumeCreate(request *api.DirvolumeCreateRequest) (
	*api.DirvolumeInfoResponse, error) {

	// Marshal request to JSON
	buffer, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}

	// Create a request
	req, err := http.NewRequest("POST",
		c.host+"/dirvolumes",
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
	var dirvolume api.DirvolumeInfoResponse
	err = utils.GetJsonFromResponse(r, &dirvolume)
	if err != nil {
		return nil, err
	}

	return &dirvolume, nil

}

func (c *Client) DirvolumeDelete(id string) error {

	// Create a request
	req, err := http.NewRequest("DELETE", c.host+"/dirvolumes/"+id, nil)
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

func (c *Client) DirvolumeInfo(id string) (*api.DirvolumeInfoResponse, error) {

	// Create request
	req, err := http.NewRequest("GET", c.host+"/dirvolumes/"+id, nil)
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
	var dirvolume api.DirvolumeInfoResponse
	err = utils.GetJsonFromResponse(r, &dirvolume)
	if err != nil {
		return nil, err
	}

	return &dirvolume, nil
}

func (c *Client) DirvolumeList() (*api.DirvolumeListResponse, error) {

	// Create request
	req, err := http.NewRequest("GET", c.host+"/dirvolumes", nil)
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
	var dirvolumes api.DirvolumeListResponse
	err = utils.GetJsonFromResponse(r, &dirvolumes)
	if err != nil {
		return nil, err
	}

	return &dirvolumes, nil
}

func (c *Client) DirvolumeExpand(id string, request *api.DirvolumeExpandRequest) (
	*api.DirvolumeInfoResponse, error) {

	// Marshal request to JSON
	buffer, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}

	// Create a request
	req, err := http.NewRequest("POST",
		c.host+"/dirvolumes/"+id+"/expand",
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
	var dirvolume api.DirvolumeInfoResponse
	err = utils.GetJsonFromResponse(r, &dirvolume)
	if err != nil {
		return nil, err
	}

	return &dirvolume, nil
}

func (c *Client) DirvolumeExport(id string, request *api.DirvolumeExportRequest) (
	*api.DirvolumeInfoResponse, error) {

	// Marshal request to JSON
	buffer, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}

	// Create a request
	req, err := http.NewRequest("POST",
		c.host+"/dirvolumes/"+id+"/export",
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
	var dirvolume api.DirvolumeInfoResponse
	err = utils.GetJsonFromResponse(r, &dirvolume)
	if err != nil {
		return nil, err
	}

	return &dirvolume, nil
}

func (c *Client) DirvolumeUnexport(id string, request *api.DirvolumeExportRequest) (
	*api.DirvolumeInfoResponse, error) {

	// Marshal request to JSON
	buffer, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}

	// Create a request
	req, err := http.NewRequest("POST",
		c.host+"/dirvolumes/"+id+"/unexport",
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
	var dirvolume api.DirvolumeInfoResponse
	err = utils.GetJsonFromResponse(r, &dirvolume)
	if err != nil {
		return nil, err
	}

	return &dirvolume, nil
}
