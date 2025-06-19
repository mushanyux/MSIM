package msnet

import "github.com/mushanyux/MSIM/pkg/mslog"

type ReactorMain struct {
	acceptor *acceptor
	eg       *Engine
	mslog.Log
}

func NewReactorMain(eg *Engine) *ReactorMain {

	return &ReactorMain{
		acceptor: newAcceptor(eg),
		eg:       eg,
		Log:      mslog.NewMSLog("ReactorMain"),
	}
}

func (m *ReactorMain) Start() error {
	return m.acceptor.Start()
}

func (m *ReactorMain) Stop() error {
	return m.acceptor.Stop()
}
