package server

import (
	"fmt"
	"io/fs"
	"net/http"
	"strings"

	"github.com/gin-contrib/gzip"
	"github.com/gin-gonic/gin"
	"github.com/mushanyux/MSIM/pkg/mshttp"
	"github.com/mushanyux/MSIM/pkg/mslog"
	"github.com/mushanyux/MSIM/version"
	"go.uber.org/zap"
)

type DemoServer struct {
	r    *mshttp.MSHttp
	addr string
	s    *Server
	mslog.Log
}

// NewDemoServer new一个demo server
func NewDemoServer(s *Server) *DemoServer {
	// r := mshttp.New()
	log := mslog.NewMSLog("DemoServer")
	r := mshttp.NewWithLogger(mshttp.LoggerWithWklog(log))
	r.Use(mshttp.CORSMiddleware())

	ds := &DemoServer{
		r:    r,
		addr: s.opts.Demo.Addr,
		s:    s,
		Log:  log,
	}
	return ds
}

// Start 开始
func (s *DemoServer) Start() {

	s.r.GetGinRoute().Use(gzip.Gzip(gzip.DefaultCompression))

	st, _ := fs.Sub(version.DemoFs, "demo/chatdemo/dist")
	s.r.GetGinRoute().NoRoute(func(c *gin.Context) {

		if c.Request.URL.Path == "" || c.Request.URL.Path == "/" {
			c.Redirect(http.StatusFound, fmt.Sprintf("/chatdemo?apiurl=%s", s.s.opts.External.APIUrl))
			c.Abort()
			return
		}

		if strings.HasPrefix(c.Request.URL.Path, "/chatdemo") {
			c.FileFromFS("./", http.FS(st))
			return
		}
	})

	s.r.GetGinRoute().StaticFS("/chatdemo", http.FS(st))

	s.setRoutes()
	go func() {
		err := s.r.Run(s.addr) // listen and serve
		if err != nil {
			panic(err)
		}
	}()
	s.Info("Demo server started", zap.String("addr", s.addr))

	_, port := parseAddr(s.addr)
	s.Info(fmt.Sprintf("Chat demo address： http://localhost:%d/chatdemo", port))
}

// Stop 停止服务
func (s *DemoServer) Stop() {
}

func (s *DemoServer) setRoutes() {

}
