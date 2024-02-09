package controllers

import (
	"context"

	"github.com/ytsaurus/yt-k8s-operator/pkg/components"
)

type baseStep struct {
	name         string
	runCondition func() bool
}
type dummyStep struct {
	name string
}
type componentStep struct {
	baseStep
	component components.Component2
}
type actionStep struct {
	baseStep
	action    func(context.Context) error
	doneCheck func(context.Context) (bool, error)
}

func (s *baseStep) GetName() string {
	return s.name
}
func (s *baseStep) ShouldRun() bool {
	if s.runCondition == nil {
		return true
	}
	return !s.runCondition()
}

func newComponentStep(component components.Component2) *componentStep {
	return &componentStep{component: component}
}
func (s *componentStep) WithRunCondition(condition func() bool) *componentStep {
	s.runCondition = condition
	return s
}
func (s *componentStep) Done(ctx context.Context) (bool, error) {
	status, err := s.component.Status2(ctx)
	return status.IsReady(), err
}
func (s *componentStep) Run(ctx context.Context) error {
	return s.component.Sync(ctx)
}

func newActionStep(
	action func(context.Context) error,
	doneCheck func(context.Context) (bool, error),
) *actionStep {
	return &actionStep{
		action:    action,
		doneCheck: doneCheck,
	}
}
func (s *actionStep) WithRunCondition(condition func() bool) *actionStep {
	s.runCondition = condition
	return s
}
func (s *actionStep) Done(ctx context.Context) (bool, error) {
	return s.doneCheck(ctx)
}
func (s *actionStep) Run(ctx context.Context) error {
	return s.action(ctx)
}

func newDummyStep() *dummyStep {
	return &dummyStep{}
}
func (s *dummyStep) Skip() bool {
	return true
}
func (s *dummyStep) Done(ctx context.Context) (bool, error) {
	return true, nil
}
func (s *dummyStep) Do(_ context.Context) error {
	return nil
}
