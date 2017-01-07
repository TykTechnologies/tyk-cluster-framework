package store

import (
	"github.com/TykTechnologies/logrus"
	"strings"
)

type ConvertedLogrusLogger struct {
	LogInstance *logrus.Logger
	Prefix string
}

func (c *ConvertedLogrusLogger) Write(p []byte) (n int, err error) {
	if c.LogInstance != nil {
		asStr := string(p)
		if strings.HasPrefix(asStr, "[DEBUG]") {
			fixed := strings.Replace(asStr, "[DEBUG]", "", 1)
			c.LogInstance.WithFields(logrus.Fields{
				"prefix": c.Prefix,
			}).Debug(fixed)
		}
		if strings.HasPrefix(asStr, "[INFO]") {
			fixed := strings.Replace(asStr, "[INFO]", "", 1)
			c.LogInstance.WithFields(logrus.Fields{
				"prefix": c.Prefix,
			}).Info(fixed)
		}
		if strings.HasPrefix(asStr, "[ERR]") {
			fixed := strings.Replace(asStr, "[ERR]", "", 1)
			c.LogInstance.WithFields(logrus.Fields{
				"prefix": c.Prefix,
			}).Error(fixed)
		}
		if strings.HasPrefix(asStr, "[WARN]") {
			fixed := strings.Replace(asStr, "[WARNING]", "", 1)
			c.LogInstance.WithFields(logrus.Fields{
				"prefix": c.Prefix,
			}).Warning(fixed)
		}
	}
	return len(p), nil
}
