package proton

import (
	"fmt"
	"net/url"
)

type StreamStorageConfig struct {
	// The max size a stream can grow. Any non-positive value means unlimited size. Defaulted to 10 GiB.
	RetentionBytes int `json:"logstore_retention_bytes,omitempty" example:"10737418240"`

	// The max time the data can be retained in the stream. Any non-positive value means unlimited time. Defaulted to 7 days.
	RetentionMS int `json:"logstore_retention_ms,omitempty" example:"604800000"`
}

func NewDefaultStreamStorageConfig() *StreamStorageConfig {
	return NewStreamStorageConfig(-1, -1)
}

// Set it to `-1` to disable retention
func NewStreamStorageConfig(retentionBytes, retentionMS int) *StreamStorageConfig {
	if retentionBytes < -1 {
		retentionBytes = -1
	}
	if retentionMS < -1 {
		retentionMS = -1
	}

	return &StreamStorageConfig{
		RetentionBytes: retentionBytes,
		RetentionMS:    retentionMS,
	}
}

func (c *StreamStorageConfig) AppendToParams(params *url.Values) {
	params.Add("logstore_retention_bytes", fmt.Sprintf("%d", c.RetentionBytes))
	params.Add("logstore_retention_ms", fmt.Sprintf("%d", c.RetentionMS))
}
