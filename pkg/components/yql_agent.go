package components

import (
	"context"
	"fmt"
	"k8s.io/utils/strings/slices"
	"strings"

	ytv1 "github.com/ytsaurus/yt-k8s-operator/api/v1"

	"github.com/ytsaurus/yt-k8s-operator/pkg/apiproxy"
	"github.com/ytsaurus/yt-k8s-operator/pkg/consts"
	"github.com/ytsaurus/yt-k8s-operator/pkg/labeller"
	"github.com/ytsaurus/yt-k8s-operator/pkg/resources"
	"github.com/ytsaurus/yt-k8s-operator/pkg/ytconfig"
	corev1 "k8s.io/api/core/v1"
)

type yqlAgent struct {
	ServerComponentBase
	master          Component
	initEnvironment *InitJob
	secret          *resources.StringSecret
}

func NewYQLAgent(cfgen *ytconfig.Generator, ytsaurus *apiproxy.Ytsaurus, master Component) Component {
	resource := ytsaurus.GetResource()
	l := labeller.Labeller{
		ObjectMeta:     &resource.ObjectMeta,
		APIProxy:       ytsaurus.APIProxy(),
		ComponentLabel: consts.YTComponentLabelYqlAgent,
		ComponentName:  "YqlAgent",
		MonitoringPort: consts.YQLAgentMonitoringPort,
	}

	server := NewServer(
		&l,
		ytsaurus,
		&resource.Spec.YQLAgents.InstanceSpec,
		"/usr/bin/ytserver-yql-agent",
		"ytserver-yql-agent.yson",
		cfgen.GetYQLAgentStatefulSetName(),
		cfgen.GetYQLAgentServiceName(),
		cfgen.GetYQLAgentConfig,
		cfgen.NeedYQLAgentConfigReload,
	)

	return &yqlAgent{
		ServerComponentBase: ServerComponentBase{
			ComponentBase: ComponentBase{
				labeller: &l,
				ytsaurus: ytsaurus,
				cfgen:    cfgen,
			},
			server: server,
		},
		master: master,
		initEnvironment: NewInitJob(
			&l,
			ytsaurus.APIProxy(),
			ytsaurus,
			resource.Spec.ImagePullSecrets,
			"yql-agent-environment",
			consts.ClientConfigFileName,
			resource.Spec.CoreImage,
			cfgen.GetNativeClientConfig,
			cfgen.NeedNativeClientConfigReload),
		secret: resources.NewStringSecret(
			l.GetSecretName(),
			&l,
			ytsaurus.APIProxy()),
	}
}

func (yqla *yqlAgent) GetName() string {
	return yqla.labeller.ComponentName
}

func (yqla *yqlAgent) Fetch(ctx context.Context) error {
	return resources.Fetch(ctx, []resources.Fetchable{
		yqla.server,
		yqla.initEnvironment,
		yqla.secret,
	})
}

func (yqla *yqlAgent) initUsers() string {
	token, _ := yqla.secret.GetValue(consts.TokenSecretKey)
	commands := createUserCommand(consts.YqlUserName, "", token, true)
	commands = append(commands, createUserCommand("yql_agent", "", "", true)...)
	return strings.Join(commands, "\n")
}

func (yqla *yqlAgent) createInitScript() string {
	var sb strings.Builder
	sb.WriteString("[")
	for _, addr := range yqla.cfgen.GetYQLAgentAddresses() {
		sb.WriteString("\"")
		sb.WriteString(addr)
		sb.WriteString("\";")
	}
	sb.WriteString("]")
	yqlAgentAddrs := sb.String()
	script := []string{
		initJobWithNativeDriverPrologue(),
		yqla.initUsers(),
		"/usr/bin/yt add-member --member yql_agent --group superusers || true",
		"/usr/bin/yt create document //sys/yql_agent/config --attributes '{}' --recursive --ignore-existing",
		fmt.Sprintf("/usr/bin/yt set //sys/@cluster_connection/yql_agent '{stages={production={channel={addresses=%v}}}}'", yqlAgentAddrs),
		fmt.Sprintf("/usr/bin/yt get //sys/@cluster_connection | /usr/bin/yt set //sys/clusters/%s", yqla.labeller.GetClusterName()),
	}

	return strings.Join(script, "\n")
}

func (yqla *yqlAgent) doSync(ctx context.Context, dry bool) (SyncStatus, error) {
	var err error

	if yqla.ytsaurus.GetClusterState() == ytv1.ClusterStateRunning && yqla.server.NeedUpdate() {
		return SyncStatusNeedLocalUpdate, err
	}

	if yqla.ytsaurus.GetClusterState() == ytv1.ClusterStateUpdating {
		if yqla.ytsaurus.GetUpdateState() == ytv1.UpdateStateWaitingForPodsRemoval {
			updatingComponents := yqla.ytsaurus.GetLocalUpdatingComponents()
			if updatingComponents == nil || slices.Contains(updatingComponents, yqla.GetName()) {
				return SyncStatusUpdating, yqla.removePods(ctx, dry)
			}
		}
	}

	if yqla.master.Status(ctx) != SyncStatusReady {
		return SyncStatusBlocked, err
	}

	if yqla.secret.NeedSync(consts.TokenSecretKey, "") {
		if !dry {
			s := yqla.secret.Build()
			s.StringData = map[string]string{
				consts.TokenSecretKey: ytconfig.RandString(30),
			}
			err = yqla.secret.Sync(ctx)
		}
		return SyncStatusPending, err
	}

	if yqla.server.NeedSync() {
		if !dry {
			ss := yqla.server.BuildStatefulSet()
			container := &ss.Spec.Template.Spec.Containers[0]
			container.EnvFrom = []corev1.EnvFromSource{yqla.secret.GetEnvSource()}
			container.Env = []corev1.EnvVar{{Name: "YT_FORCE_IPV4", Value: "1"}, {Name: "YT_FORCE_IPV6", Value: "0"}}
			err = yqla.server.Sync(ctx)
		}
		return SyncStatusPending, err
	}

	if !yqla.server.ArePodsReady(ctx) {
		return SyncStatusBlocked, err
	}

	if !dry {
		yqla.initEnvironment.SetInitScript(yqla.createInitScript())
	}

	return yqla.initEnvironment.Sync(ctx, dry)
}

func (yqla *yqlAgent) Status(ctx context.Context) SyncStatus {
	status, err := yqla.doSync(ctx, true)
	if err != nil {
		panic(err)
	}

	return status
}

func (yqla *yqlAgent) Sync(ctx context.Context) error {
	_, err := yqla.doSync(ctx, false)
	return err
}
