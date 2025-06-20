package mshttp

import (
	"fmt"
	"time"

	"github.com/mushanyux/MSIM/pkg/mslog"
	"go.uber.org/zap"
)

// Logger instance a Logger middleware with mslog.
func LoggerWithMslog(log mslog.Log) HandlerFunc {
	return func(c *Context) {
		// Start timer
		start := time.Now()

		// Process request
		c.Next()

		// Stop timers
		latency := time.Since(start)

		if latency > time.Minute {
			latency = latency.Truncate(time.Second)
		}

		path := c.Request.URL.Path
		raw := c.Request.URL.RawQuery
		if raw != "" {
			path = path + "?" + raw
		}

		log.Debug(fmt.Sprintf("|%s| %d| %s", c.Request.Method, c.Writer.Status(), path),
			zap.String("clientip", c.ClientIP()),
			zap.Int("size", c.Writer.Size()),
			zap.String("latency", fmt.Sprintf("%v", latency)))
	}
}
