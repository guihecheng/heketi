package main

import (
	"fmt"
	client "github.com/heketi/heketi/client/api/go-client"
	api "github.com/heketi/heketi/pkg/glusterfs/api"
	"strings"
)

func main() {
	url := "http://localhost:8080"
	user := "My user"
	key := "My key"

	// Create a client
	heketi := client.NewClient(url, user, key)
	if heketi == nil {
		fmt.Printf("Connect to Heketi failed\n")
		return
	}

	// List clusters
	clistResp, err := heketi.ClusterList()
	if err != nil {
		fmt.Printf("List clusters failed - %v\n", err)
		return
	}
	if len(clistResp.Clusters) == 0 {
		fmt.Printf("No cluster found\n")
		return
	}
	if len(clistResp.Clusters) > 1 {
		fmt.Printf("Multiple clusters found, we only need one\n")
		return
	}
	clusterid := clistResp.Clusters[0]
	fmt.Printf("The one cluster is found: %v\n", clusterid)

	// Create a Dirvolume
	createReq := &api.DirvolumeCreateRequest{}
	createReq.Size = 4
	createReq.ClusterId = clusterid
	createResp, err := heketi.DirvolumeCreate(createReq)
	if err != nil {
		fmt.Printf("Create dirvolume failed - %v\n", err)
		return
	}
	dvid := createResp.Id
	fmt.Printf("Dirvolume created ID: %v\n", dvid)

	// Dirvolume Info
	info, err := heketi.DirvolumeInfo(dvid)
	if err != nil {
		fmt.Printf("Info dirvolume failed - %v\n", err)
		return
	}
	fmt.Printf("Dirvolume Info Name: %v\n", info.Name)

	// Dirvolume Stats
	stats, err := heketi.DirvolumeStats(dvid)
	if err != nil {
		fmt.Printf("Stat dirvolume failed - %v\n", err)
		return
	}
	fmt.Printf("Dirvolume Stat Used: %v\n", stats.UsedSize)

	// Dirvolume List
	listResp, err := heketi.DirvolumeList()
	if err != nil {
		fmt.Printf("List dirvolume failed - %v\n", err)
		return
	}
	for i, id := range listResp.Dirvolumes {
		fmt.Printf("Dirvolume %v: %v\n", i, id)
	}

	// Expand dirvolume
	expandReq := &api.DirvolumeExpandRequest{}
	expandReq.Size = 4
	expandResp, err := heketi.DirvolumeExpand(dvid, expandReq)
	if err != nil {
		fmt.Printf("Expand dirvolume failed - %v\n", err)
		return
	}
	fmt.Printf("Expand dirvolume to new size: %v\n", expandResp.Size)

	// Export dirvolume
	exportReq := &api.DirvolumeExportRequest{}
	exportReq.IpList = []string{"10.0.1.1", "10.0.1.2"}
	exportResp, err := heketi.DirvolumeExport(dvid, exportReq)
	if err != nil {
		fmt.Printf("Export dirvolume failed - %v\n", err)
		return
	}
	fmt.Printf("Export dirvolume to new VMs: %v\n", strings.Join(exportResp.Export.IpList[:], ","))

	// Unexport dirvolume
	unexportReq := &api.DirvolumeExportRequest{}
	unexportReq.IpList = []string{"10.0.1.2"}
	unexportResp, err := heketi.DirvolumeUnexport(dvid, unexportReq)
	if err != nil {
		fmt.Printf("Unexport dirvolume failed - %v\n", err)
		return
	}
	fmt.Printf("Export dirvolume only to VMs: %v\n", strings.Join(unexportResp.Export.IpList[:], ","))

	// Delete dirvolume
	err = heketi.DirvolumeDelete(dvid)
	if err != nil {
		fmt.Printf("Delete dirvolume failed - %v\n", err)
		return
	}

	return
}
