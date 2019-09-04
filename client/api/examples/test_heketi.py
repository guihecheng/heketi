import requests
import heketi

TEST_ADMIN_KEY = "My Secret"
TEST_SERVER = "http://localhost:8080"
TEST_POLL_DELAY = 0.2


def main():
    # Connect to Heketi
    client = heketi.HeketiClient(TEST_SERVER, "admin", TEST_ADMIN_KEY)

    # List clusters
    clist_resp = client.cluster_list()
    if clist_resp == None:
        print("List clusters failed")
        return
    if len(clist_resp['clusters']) == 0:
        print("No cluster found")
        return
    if len(clist_resp['clusters']) > 1:
        print("Multiple clusters found, we only need one")
        return
    clusterid = clist_resp['clusters'][0]
    print("The one cluster is found: %s" % clusterid)

    # Create a Dirvolume
    create_req = {}
    create_req['size'] = 4
    create_req['cluster'] = clusterid
    dv = client.dirvolume_create(create_req)
    if dv == None:
        print("Create dirvolume failed")
        return

    # Dirvolume Info
    info = client.dirvolume_info(dv['id'])
    if info == None:
        print("Dirvolume info failed")
        return

    # Dirvolume Stats
    stats = client.dirvolume_stats(dv['id'])
    if stats == None:
        print("Dirvolume stat failed")
        return

    # Dirvolume list
    list_resp = client.dirvolume_list()
    if list_resp == None:
        print("Dirvolume list failed")
        return

    for i in range(len(list_resp['dirvolumes'])):
        print("Dirvolume %d: %s" %(i, list_resp['dirvolumes'][i]))

    # expand dirvolume
    expand_req = {}
    expand_req['expand_size'] = 4
    expand_resp = client.dirvolume_expand(dv['id'], expand_req)
    if expand_resp == None:
        print("Dirvolume expand failed")
        return
    print("Expand dirvolume to new size: %d" % expand_resp['size'])

    # export dirvolume
    export_req = {}
    export_req['iplist'] = ["10.0.1.1", "10.0.1.2"]
    export_resp = client.dirvolume_export(dv['id'], export_req)
    if export_resp == None:
        print("Dirvolume export failed")
        return
    if export_resp['export']['iplist'] != None:
        print("Export dirvolume to new VMs: %s" % ','.join(export_resp['export']['iplist']))

    # unexport dirvolume
    unexport_req = {}
    unexport_req['iplist'] = ["10.0.1.2"]
    unexport_resp = client.dirvolume_unexport(dv['id'], unexport_req)
    if unexport_resp == None:
        print("Dirvolume unexport failed")
        return
    if unexport_resp['export']['iplist'] != None:
        print("Export dirvolume only to VMs: %s" % ','.join(unexport_resp['export']['iplist']))

    # delete dirvolume
    delete_resp = client.dirvolume_delete(dv['id'])
    if not delete_resp:
        print("Delete dirvolume failed")
        return

if __name__ == '__main__':
    main()
