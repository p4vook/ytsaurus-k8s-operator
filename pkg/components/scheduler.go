package components

import (
	"context"
	"fmt"
	"strings"

	"go.ytsaurus.tech/library/go/ptr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ytv1 "github.com/ytsaurus/yt-k8s-operator/api/v1"

	corev1 "k8s.io/api/core/v1"

	"github.com/ytsaurus/yt-k8s-operator/pkg/apiproxy"
	"github.com/ytsaurus/yt-k8s-operator/pkg/consts"
	"github.com/ytsaurus/yt-k8s-operator/pkg/labeller"
	"github.com/ytsaurus/yt-k8s-operator/pkg/resources"
	"github.com/ytsaurus/yt-k8s-operator/pkg/ytconfig"
)

var (
	SchedulerUpdateOpArchivePrepareStartedCond  = isTrue("UpdateOpArchivePrepareStarted")
	SchedulerUpdateOpArchivePrepareFinishedCond = isTrue("UpdateOpArchivePrepareFinished")
	SchedulerUpdateOpArchiveFinishedCond        = isTrue("UpdateOpArchiveFinished")
)

type Scheduler struct {
	localServerComponent
	cfgen *ytconfig.Generator
	//master Component
	//execNodes     []Component
	tabletNodesCount int
	initUser         *InitJob
	initOpArchive    *InitJob
	secret           *resources.StringSecret
}

func NewScheduler(
	cfgen *ytconfig.Generator,
	ytsaurus *apiproxy.Ytsaurus,
	tabletNodesCount int,
	// master Component,
	// execNodes,
	// tabletNodes []Component,
) *Scheduler {
	resource := ytsaurus.GetResource()
	l := labeller.Labeller{
		ObjectMeta:     &resource.ObjectMeta,
		APIProxy:       ytsaurus.APIProxy(),
		ComponentLabel: consts.YTComponentLabelScheduler,
		ComponentName:  string(consts.SchedulerType),
		Annotations:    resource.Spec.ExtraPodAnnotations,
	}

	if resource.Spec.Schedulers.InstanceSpec.MonitoringPort == nil {
		resource.Spec.Schedulers.InstanceSpec.MonitoringPort = ptr.Int32(consts.SchedulerMonitoringPort)
	}

	srv := newServer(
		&l,
		ytsaurus,
		&resource.Spec.Schedulers.InstanceSpec,
		"/usr/bin/ytserver-scheduler",
		"ytserver-scheduler.yson",
		cfgen.GetSchedulerStatefulSetName(),
		cfgen.GetSchedulerServiceName(),
		func() ([]byte, error) {
			return cfgen.GetSchedulerConfig(resource.Spec.Schedulers)
		},
	)

	return &Scheduler{
		localServerComponent: newLocalServerComponent(&l, ytsaurus, srv),
		cfgen:                cfgen,
		tabletNodesCount:     tabletNodesCount,
		//master:               master,
		//execNodes:            execNodes,
		//tabletNodes:          tabletNodes,
		initUser: NewInitJob(
			&l,
			ytsaurus.APIProxy(),
			ytsaurus,
			resource.Spec.ImagePullSecrets,
			"user",
			consts.ClientConfigFileName,
			resource.Spec.CoreImage,
			cfgen.GetNativeClientConfig),
		initOpArchive: NewInitJob(
			&l,
			ytsaurus.APIProxy(),
			ytsaurus,
			resource.Spec.ImagePullSecrets,
			"op-archive",
			consts.ClientConfigFileName,
			resource.Spec.CoreImage,
			cfgen.GetNativeClientConfig),
		secret: resources.NewStringSecret(
			l.GetSecretName(),
			&l,
			ytsaurus.APIProxy()),
	}
}

func (s *Scheduler) IsUpdatable() bool {
	return true
}

func (s *Scheduler) GetType() consts.ComponentType { return consts.SchedulerType }

func (s *Scheduler) Fetch(ctx context.Context) error {
	return resources.Fetch(ctx,
		s.server,
		s.initOpArchive,
		s.initUser,
		s.secret,
	)
}

func (s *Scheduler) Status(ctx context.Context) (ComponentStatus, error) {
	if err := s.Fetch(ctx); err != nil {
		return ComponentStatus{}, fmt.Errorf("failed to fetch component %s: %w", s.GetName(), err)
	}

	st, msg, err := s.getFlow().Status(ctx, s.condManager)
	return ComponentStatus{
		SyncStatus: st,
		Message:    msg,
	}, err
}

func (s *Scheduler) StatusOld(ctx context.Context) ComponentStatus {
	status, err := s.doSync(ctx, true)
	if err != nil {
		panic(err)
	}

	return status
}

func (s *Scheduler) Sync(ctx context.Context) error {
	_, err := s.getFlow().Run(ctx, s.condManager)
	return err
}

func (s *Scheduler) doSync(ctx context.Context, dry bool) (ComponentStatus, error) {
	var err error

	if ytv1.IsReadyToUpdateClusterState(s.ytsaurus.GetClusterState()) && s.server.needUpdate() {
		return SimpleStatus(SyncStatusNeedLocalUpdate), err
	}

	if s.ytsaurus.GetClusterState() == ytv1.ClusterStateUpdating {
		if IsUpdatingComponent(s.ytsaurus, s) {
			if s.ytsaurus.GetUpdateState() == ytv1.UpdateStateWaitingForPodsRemoval {
				if !dry {
					err = removePods(ctx, s.server, &s.localComponent)
				}
				return WaitingStatus(SyncStatusUpdating, "pods removal"), err
			}

			if status, err := s.updateOpArchive(ctx, dry); status != nil {
				return *status, err
			}

			if s.ytsaurus.GetUpdateState() != ytv1.UpdateStateWaitingForPodsCreation &&
				s.ytsaurus.GetUpdateState() != ytv1.UpdateStateWaitingForOpArchiveUpdate {
				return NewComponentStatus(SyncStatusReady, "Nothing to do now"), err
			}
		} else {
			return NewComponentStatus(SyncStatusReady, "Not updating component"), err
		}
	}

	//if !IsRunningStatus(s.master.Status(ctx).SyncStatus) {
	//	return WaitingStatus(SyncStatusBlocked, s.master.GetName()), err
	//}

	//if s.execNodes == nil || len(s.execNodes) > 0 {
	//	for _, end := range s.execNodes {
	//		if !IsRunningStatus(end.Status(ctx).SyncStatus) {
	//			// It makes no sense to start scheduler without exec nodes.
	//			return WaitingStatus(SyncStatusBlocked, end.GetName()), err
	//		}
	//	}
	//}

	if s.secret.NeedSync(consts.TokenSecretKey, "") {
		if !dry {
			secretSpec := s.secret.Build()
			secretSpec.StringData = map[string]string{
				consts.TokenSecretKey: ytconfig.RandString(30),
			}
			err = s.secret.Sync(ctx)
		}
		return WaitingStatus(SyncStatusPending, s.secret.Name()), err
	}

	if s.NeedSync() {
		if !dry {
			err = s.server.Sync(ctx)
		}
		return WaitingStatus(SyncStatusPending, "components"), err
	}

	if !s.server.arePodsReady(ctx) {
		return WaitingStatus(SyncStatusBlocked, "pods"), err
	}

	if !s.needOpArchiveInit() {
		// Don't initialize operations archive.
		return SimpleStatus(SyncStatusReady), err
	}

	return s.initOpAchieve(ctx, dry)
}

func (s *Scheduler) getFlow() Step {
	name := s.GetName()
	buildStartedCond := buildStarted(name)
	builtFinishedCond := buildFinished(name)
	initCond := initializationFinished(name)
	updateRequiredCond := updateRequired(name)
	rebuildStartedCond := rebuildStarted(name)
	rebuildFinishedCond := rebuildFinished(name)

	return StepComposite{
		Steps: []Step{
			StepRun{
				Name:               StepStartBuild,
				RunIfCondition:     not(buildStartedCond),
				RunFunc:            s.server.Sync,
				OnSuccessCondition: buildStartedCond,
			},
			StepCheck{
				Name:               StepWaitBuildFinished,
				RunIfCondition:     not(builtFinishedCond),
				OnSuccessCondition: builtFinishedCond,
				RunFunc: func(ctx context.Context) (ok bool, err error) {
					diff, err := s.server.hasDiff(ctx)
					return !diff, err
				},
			},
			StepCheck{
				Name:               StepInitFinished,
				RunIfCondition:     not(initCond),
				OnSuccessCondition: initCond,
				RunFunc: func(ctx context.Context) (ok bool, err error) {
					s.initUser.SetInitScript(s.createInitUserScript())
					st, err := s.initUser.Sync(ctx, false)
					return st.SyncStatus == SyncStatusReady, err
				},
			},
			StepComposite{
				Name: StepUpdate,
				// Update should be run if either diff exists or updateRequired condition is set,
				// because a diff should disappear in the middle of the update, but it still need
				// to finish actions after the update (master exit read only, safe mode, etc.).
				StatusConditionFunc: func(ctx context.Context) (SyncStatus, string, error) {
					diff, err := s.server.hasDiff(ctx)
					if err != nil {
						return "", "", err
					}
					if diff {
						if err = s.condManager.SetCond(ctx, updateRequiredCond); err != nil {
							return "", "", err
						}
					}
					// Sync either if diff or is condition set
					// in the middle of update there will be no diff, so we need a condition.
					if diff || s.condManager.IsSatisfied(updateRequiredCond) {
						return SyncStatusNeedSync, "", nil
					}
					return SyncStatusReady, "", nil
				},
				OnSuccessCondition: not(updateRequiredCond),
				OnSuccessFunc:      s.cleanupAfterUpdate,
				Steps: []Step{
					StepRun{
						Name:               StepStartRebuild,
						RunIfCondition:     not(rebuildStartedCond),
						OnSuccessCondition: rebuildStartedCond,
						RunFunc:            s.server.removePods,
					},
					StepCheck{
						Name:               StepWaitRebuildFinished,
						RunIfCondition:     not(rebuildFinishedCond),
						OnSuccessCondition: rebuildFinishedCond,
						RunFunc: func(ctx context.Context) (ok bool, err error) {
							diff, err := s.server.hasDiff(ctx)
							return !diff, err
						},
					},
					StepRun{
						Name:               "StartPrepareUpdateOpArchive",
						RunIfCondition:     not(SchedulerUpdateOpArchivePrepareStartedCond),
						OnSuccessCondition: SchedulerUpdateOpArchivePrepareStartedCond,
						RunFunc: func(ctx context.Context) error {
							return s.initOpArchive.prepareRestart(ctx, false)
						},
					},
					StepCheck{
						Name:               "WaitUpdateOpArchivePrepared",
						RunIfCondition:     not(SchedulerUpdateOpArchivePrepareFinishedCond),
						OnSuccessCondition: SchedulerUpdateOpArchivePrepareFinishedCond,
						RunFunc: func(ctx context.Context) (bool, error) {
							return s.initOpArchive.isRestartPrepared(), nil
						},
						OnSuccessFunc: func(ctx context.Context) error {
							s.prepareInitOperationArchive(s.initOpArchive)
							return nil
						},
					},
					StepCheck{
						Name:               "WaitUpdateOpArchive",
						RunIfCondition:     not(SchedulerUpdateOpArchiveFinishedCond),
						OnSuccessCondition: SchedulerUpdateOpArchiveFinishedCond,
						RunFunc: func(ctx context.Context) (ok bool, err error) {
							st, err := s.initOpArchive.Sync(ctx, false)
							return st.SyncStatus == SyncStatusReady, err
						},
					},
				},
			},
		},
	}
}

func (s *Scheduler) cleanupAfterUpdate(ctx context.Context) error {
	for _, cond := range s.getConditionsSetByUpdate() {
		if err := s.condManager.SetCond(ctx, not(cond)); err != nil {
			return err
		}
	}
	return nil
}

func (s *Scheduler) getConditionsSetByUpdate() []Condition {
	var result []Condition
	conds := []Condition{
		rebuildStarted(s.GetName()),
		rebuildFinished(s.GetName()),
		SchedulerUpdateOpArchivePrepareStartedCond,
		SchedulerUpdateOpArchivePrepareFinishedCond,
		SchedulerUpdateOpArchiveFinishedCond,
	}
	for _, cond := range conds {
		if s.condManager.IsSatisfied(cond) {
			result = append(result, cond)
		}
	}
	return result
}

func (s *Scheduler) initOpAchieve(ctx context.Context, dry bool) (ComponentStatus, error) {
	if !dry {
		s.initUser.SetInitScript(s.createInitUserScript())
	}

	status, err := s.initUser.Sync(ctx, dry)
	if status.SyncStatus != SyncStatusReady {
		return status, err
	}

	//for _, tnd := range s.tabletNodes {
	//	if !IsRunningStatus(tnd.Status(ctx).SyncStatus) {
	//		// Wait for tablet nodes to proceed with operations archive init.
	//		return WaitingStatus(SyncStatusBlocked, tnd.GetName()), err
	//	}
	//}

	if !dry {
		s.prepareInitOperationArchive(s.initOpArchive)
	}
	return s.initOpArchive.Sync(ctx, dry)
}

func (s *Scheduler) updateOpArchive(ctx context.Context, dry bool) (*ComponentStatus, error) {
	var err error
	switch s.ytsaurus.GetUpdateState() {
	case ytv1.UpdateStateWaitingForOpArchiveUpdatingPrepare:
		if !s.needOpArchiveInit() {
			s.setConditionNotNecessaryToUpdateOpArchive(ctx)
			return ptr.T(SimpleStatus(SyncStatusUpdating)), nil
		}
		if !s.initOpArchive.isRestartPrepared() {
			return ptr.T(SimpleStatus(SyncStatusUpdating)), s.initOpArchive.prepareRestart(ctx, dry)
		}
		if !dry {
			s.setConditionOpArchivePreparedForUpdating(ctx)
		}
		return ptr.T(SimpleStatus(SyncStatusUpdating)), err
	case ytv1.UpdateStateWaitingForOpArchiveUpdate:
		if !s.initOpArchive.isRestartCompleted() {
			return nil, nil
		}
		if !dry {
			s.setConditionOpArchiveUpdated(ctx)
		}
		return ptr.T(SimpleStatus(SyncStatusUpdating)), err
	default:
		return nil, nil
	}
}

func (s *Scheduler) needOpArchiveInit() bool {
	return s.tabletNodesCount > 0
}

func (s *Scheduler) setConditionNotNecessaryToUpdateOpArchive(ctx context.Context) {
	s.ytsaurus.SetUpdateStatusCondition(ctx, metav1.Condition{
		Type:    consts.ConditionNotNecessaryToUpdateOpArchive,
		Status:  metav1.ConditionTrue,
		Reason:  "NotNecessaryToUpdateOpArchive",
		Message: "Operations archive does not need to be updated",
	})
}

func (s *Scheduler) setConditionOpArchivePreparedForUpdating(ctx context.Context) {
	s.ytsaurus.SetUpdateStatusCondition(ctx, metav1.Condition{
		Type:    consts.ConditionOpArchivePreparedForUpdating,
		Status:  metav1.ConditionTrue,
		Reason:  "OpArchivePreparedForUpdating",
		Message: "Operations archive prepared for updating",
	})
}

func (s *Scheduler) setConditionOpArchiveUpdated(ctx context.Context) {
	s.ytsaurus.SetUpdateStatusCondition(ctx, metav1.Condition{
		Type:    consts.ConditionOpArchiveUpdated,
		Status:  metav1.ConditionTrue,
		Reason:  "OpArchiveUpdated",
		Message: "Operations archive updated",
	})
}

func (s *Scheduler) createInitUserScript() string {
	token, _ := s.secret.GetValue(consts.TokenSecretKey)
	commands := createUserCommand("operation_archivarius", "", token, true)
	script := []string{
		initJobWithNativeDriverPrologue(),
	}
	script = append(script, commands...)

	return strings.Join(script, "\n")
}

func (s *Scheduler) prepareInitOperationArchive(job *InitJob) {
	script := []string{
		initJobWithNativeDriverPrologue(),
		fmt.Sprintf("/usr/bin/init_operation_archive --force --latest --proxy %s",
			s.cfgen.GetHTTPProxiesServiceAddress(consts.DefaultHTTPProxyRole)),
		SetWithIgnoreExisting("//sys/cluster_nodes/@config", "'{\"%true\" = {job_agent={enable_job_reporter=%true}}}'"),
	}

	job.SetInitScript(strings.Join(script, "\n"))
	batchJob := s.initOpArchive.Build()
	container := &batchJob.Spec.Template.Spec.Containers[0]
	container.EnvFrom = []corev1.EnvFromSource{s.secret.GetEnvSource()}
}
