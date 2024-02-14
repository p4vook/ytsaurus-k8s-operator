package controllers

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ytv1 "github.com/ytsaurus/yt-k8s-operator/api/v1"
)

const (
	ytsaurusName      = "testsaurus"
	testYtsaurusImage = "test-ytsaurus-image"
	dndsNameOne       = "dn-1"
)

func TestYtsaurusFromScratch(t *testing.T) {
	require.NoError(t, os.Setenv("ENABLE_NEW_FLOW", "true"))
	namespace := "ytsaurus-from-scratch"
	h := newTestHelper(t, namespace)
	h.start()
	defer h.stop()

	h.ytsaurusInMemory.Set("//sys/@hydra_read_only", false)

	remoteYtsaurusResource := buildMinimalYtsaurus(h, ytsaurusName)
	deployObject(h, &remoteYtsaurusResource)

	for _, compName := range []string{
		"discovery",
		"master",
		"http-proxy",
	} {
		fetchAndCheckConfigMapContainsEventually(
			h,
			"yt-"+compName+"-config",
			"ytserver-"+compName+".yson",
			"ms-0.masters."+namespace+".svc.cluster.local:9010",
		)
	}
	fetchAndCheckConfigMapContainsEventually(
		h,
		"yt-data-node-"+dndsNameOne+"-config",
		"ytserver-data-node.yson",
		"ms-0.masters."+namespace+".svc.cluster.local:9010",
	)

	for _, stsName := range []string{
		"ds",
		"ms",
		"hp",
		"dnd-" + dndsNameOne,
	} {
		fetchEventually(
			h,
			stsName,
			&appsv1.StatefulSet{},
		)
	}

	fetchAndCheckEventually(
		h,
		"yt-client-secret",
		&corev1.Secret{},
		func(obj client.Object) bool {
			secret := obj.(*corev1.Secret)
			return len(secret.Data["YT_TOKEN"]) != 0
		},
	)

	// emulate master read only is done
	job := &batchv1.Job{}
	fetchEventually(
		h,
		"yt-master-init-job-exit-read-only",
		job,
	)
	h.ytsaurusInMemory.Set("//sys/@hydra_read_only", true)
	job.Status.Succeeded = 1
	updateObjectStatus(h, job)

	// emulate tablet cells recovered
	h.ytsaurusInMemory.Set("//sys/tablet_cells", map[string]any{
		"1-602-2bc-955ed415": nil,
	})

	fetchAndCheckEventually(
		h,
		ytsaurusName,
		&ytv1.Ytsaurus{},
		func(obj client.Object) bool {
			state := obj.(*ytv1.Ytsaurus).Status.State
			return state == ytv1.ClusterStateRunning
		},
	)
}

func buildMinimalYtsaurus(h *testHelper, name string) ytv1.Ytsaurus {
	remoteYtsaurus := ytv1.Ytsaurus{
		ObjectMeta: h.getObjectMeta(name),
		Spec: ytv1.YtsaurusSpec{
			CoreImage:        testYtsaurusImage,
			IsManaged:        true,
			EnableFullUpdate: false,
			UseShortNames:    true,

			Discovery: ytv1.DiscoverySpec{
				InstanceSpec: ytv1.InstanceSpec{
					InstanceCount: 3,
				},
			},
			PrimaryMasters: ytv1.MastersSpec{
				InstanceSpec: ytv1.InstanceSpec{
					InstanceCount: 3,
					Locations: []ytv1.LocationSpec{
						{
							LocationType: "MasterChangelogs",
							Path:         "/yt/master-data/master-changelogs",
						},
						{
							LocationType: "MasterSnapshots",
							Path:         "/yt/master-data/master-snapshots",
						},
					},
				},
				CellTag: 1,
			},
			HTTPProxies: []ytv1.HTTPProxiesSpec{
				{
					InstanceSpec: ytv1.InstanceSpec{InstanceCount: 3},
					ServiceType:  corev1.ServiceTypeNodePort,
				},
			},
			DataNodes: []ytv1.DataNodesSpec{
				{
					InstanceSpec: ytv1.InstanceSpec{
						InstanceCount: 5,
						Locations: []ytv1.LocationSpec{
							{
								LocationType: "ChunkStore",
								Path:         "/yt/node-data/chunk-store",
							},
						},
					},
					ClusterNodesSpec: ytv1.ClusterNodesSpec{},
					Name:             dndsNameOne,
				},
			},
		},
	}
	return remoteYtsaurus
}
