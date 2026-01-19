package dto

import "time"

type MetricsRow struct {
	Path               string
	Replication        int
	ModificationTime   time.Time
	AccessTime         time.Time
	PreferredBlockSize int
	BlocksCount        int
	FileSize           float64
	NSQUOTA            int
	DSQUOTA            int
	Permission         int
	UserName           string
	GroupName          string
}

type MetricsTable []MetricsRow
