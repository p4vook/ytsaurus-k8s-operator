package components

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/record"
	ptr "k8s.io/utils/pointer"
	ctrlrt "sigs.k8s.io/controller-runtime"

	ytv1 "github.com/ytsaurus/yt-k8s-operator/api/v1"
	"github.com/ytsaurus/yt-k8s-operator/pkg/apiproxy"
	"github.com/ytsaurus/yt-k8s-operator/pkg/testutil"
	"github.com/ytsaurus/yt-k8s-operator/pkg/ytconfig"
)

const (
	ytsaurusName = "testsaurus"
)

func testComponentFlow(
	t *testing.T,
	shortName, longName string,
	build func(*ytconfig.Generator, *apiproxy.Ytsaurus) Component,
	setImage func(*ytv1.Ytsaurus, *string),
) {
	ctx := context.Background()
	namespace := longName

	h := testutil.NewTestHelper(t, namespace, filepath.Join("..", "..", "config", "crd", "bases"))
	h.Start(func(mgr ctrlrt.Manager) error { return nil })
	defer h.Stop()

	ytsaurusResource := testutil.BuildMinimalYtsaurus(namespace, ytsaurusName)
	// Deploy of ytsaurus spec is required, so it could set valid owner references for child resources.
	testutil.DeployObject(h, &ytsaurusResource)

	scheme := runtime.NewScheme()
	utilruntime.Must(ytv1.AddToScheme(scheme))
	fakeRecorder := record.NewFakeRecorder(100)

	ytsaurus := apiproxy.NewYtsaurus(&ytsaurusResource, h.GetK8sClient(), fakeRecorder, scheme)
	domain := "testdomain"
	cfgen := ytconfig.NewGenerator(&ytsaurusResource, domain)

	// initial creation
	component := build(cfgen, ytsaurus)
	testutil.Eventually(h, shortName+" became ready", func() bool {
		st, err := component.Status(ctx)
		require.NoError(t, err)
		if st.SyncStatus == SyncStatusReady {
			return true
		}
		require.NoError(t, component.Sync(ctx))
		return false
	})

	cmData := testutil.FetchConfigMapData(h, "yt-"+longName+"-config", "ytserver-"+longName+".yson")
	require.Contains(t, cmData, "ms-0.masters."+namespace+".svc."+domain+":9010")

	// TODO: replace with get
	testutil.FetchEventually(
		h,
		shortName,
		&appsv1.StatefulSet{},
	)

	// update
	// TODO: update2 to be sure that first update doesn't end with wrong state
	// after fix jobs deletion
	for i := 1; i <= 1; i++ {
		t.Logf("Update %s #%d", shortName, i)
		newImage := ptr.String(fmt.Sprintf("new-image-%d", i))
		setImage(&ytsaurusResource, newImage)

		component = build(cfgen, ytsaurus)
		testutil.Eventually(h, shortName+" became ready", func() bool {
			st, err := component.Status(ctx)
			require.NoError(t, err)
			if st.SyncStatus == SyncStatusReady {
				return true
			}
			require.NoError(t, component.Sync(ctx))
			return false
		})
		sts := &appsv1.StatefulSet{}
		testutil.GetObject(h, shortName, sts)
		require.Equal(t, *newImage, sts.Spec.Template.Spec.Containers[0].Image)
	}

}
