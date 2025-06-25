package api

import (
	"strings"
	"time"

	"github.com/RussellLuo/timingwheel"
	"github.com/mushanyux/MSIM/internal/ingress"
	"github.com/mushanyux/MSIM/internal/options"
	"github.com/mushanyux/MSIM/pkg/mslog"
)

type Server struct {
	requset     *request
	client      *ingress.Client
	timingWheel *timingwheel.TimingWheel // Time wheel delay task
	mslog.Log
	uptime        time.Time
	migrateTask   *MigrateTask   // 迁移任务
	apiServer     *apiServer     // api服务
	managerServer *managerServer // api服务（管理）
}

func New() *Server {
	s := &Server{
		Log:         mslog.NewMSLog("ApiServer"),
		requset:     newRequset(),
		timingWheel: timingwheel.NewTimingWheel(options.G.TimingWheelTick, options.G.TimingWheelSize),
		uptime:      time.Now(),
		client:      ingress.NewClient(),
	}
	s.apiServer = newApiServer(s)
	s.migrateTask = NewMigrateTask(s) // 迁移任务
	s.managerServer = newManagerServer(s)
	return s
}

func (s *Server) Start() error {

	s.timingWheel.Start()
	s.apiServer.start()

	if options.G.Manager.On {
		s.managerServer.start()
	}

	// 判断是否开启迁移任务
	if strings.TrimSpace(options.G.OldV1Api) != "" {
		s.migrateTask.Run()
	}

	return nil
}

func (s *Server) Stop() {
	s.timingWheel.Stop()
	s.apiServer.stop()
	if options.G.Manager.On {
		s.managerServer.stop()
	}
}

// Schedule 延迟任务
func (s *Server) Schedule(interval time.Duration, f func()) *timingwheel.Timer {
	return s.timingWheel.ScheduleFunc(&everyScheduler{
		Interval: interval,
	}, f)
}

type everyScheduler struct {
	Interval time.Duration
}

func (s *everyScheduler) Next(prev time.Time) time.Time {
	return prev.Add(s.Interval)
}
