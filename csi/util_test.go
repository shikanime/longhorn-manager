package csi

import (
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"

	longhornclient "github.com/longhorn/longhorn-manager/client"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func TestGetVolumeOptions(t *testing.T) {
	testCases := []struct {
		name           string
		volumeID       string
		volumeOptions  map[string]string
		expectedVolume *longhornclient.Volume
		expectedError  bool
	}{
		{
			name:     "defaults",
			volumeID: "test-vol",
			volumeOptions: map[string]string{
				"numberOfReplicas": "3",
			},
			expectedVolume: &longhornclient.Volume{
				NumberOfReplicas:    3,
				StaleReplicaTimeout: defaultStaleReplicaTimeout,
				AccessMode:          string(longhorn.AccessModeReadWriteOnce),
				DataEngine:          string(longhorn.DataEngineTypeV1),
				RevisionCounterDisabled: true,
			},
			expectedError: false,
		},
		{
			name:     "exclusive access",
			volumeID: "test-vol-exclusive",
			volumeOptions: map[string]string{
				"exclusive": "true",
			},
			expectedVolume: &longhornclient.Volume{
				StaleReplicaTimeout: defaultStaleReplicaTimeout,
				AccessMode:          string(longhorn.AccessModeReadWriteOncePod),
				DataEngine:          string(longhorn.DataEngineTypeV1),
				RevisionCounterDisabled: true,
			},
			expectedError: false,
		},
		{
			name:     "shared access",
			volumeID: "test-vol-shared",
			volumeOptions: map[string]string{
				"share": "true",
			},
			expectedVolume: &longhornclient.Volume{
				StaleReplicaTimeout: defaultStaleReplicaTimeout,
				AccessMode:          string(longhorn.AccessModeReadWriteMany),
				DataEngine:          string(longhorn.DataEngineTypeV1),
				RevisionCounterDisabled: true,
			},
			expectedError: false,
		},
		{
			name:     "exclusive and shared conflict",
			volumeID: "test-vol-conflict",
			volumeOptions: map[string]string{
				"exclusive": "true",
				"share":     "true",
			},
			expectedVolume: nil,
			expectedError:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			vol, err := getVolumeOptions(tc.volumeID, tc.volumeOptions)
			if tc.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedVolume, vol)
			}
		})
	}
}

func TestRequireExclusiveAccess(t *testing.T) {
	testCases := []struct {
		name       string
		volume     *longhornclient.Volume
		capability *csi.VolumeCapability
		expected   bool
	}{
		{
			name: "rwop volume",
			volume: &longhornclient.Volume{
				AccessMode: string(longhorn.AccessModeReadWriteOncePod),
			},
			capability: &csi.VolumeCapability{},
			expected:   true,
		},
		{
			name: "single node single writer capability",
			volume: &longhornclient.Volume{
				AccessMode: string(longhorn.AccessModeReadWriteOnce),
			},
			capability: &csi.VolumeCapability{
				AccessMode: &csi.VolumeCapability_AccessMode{
					Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER,
				},
			},
			expected: true,
		},
		{
			name: "rwo volume",
			volume: &longhornclient.Volume{
				AccessMode: string(longhorn.AccessModeReadWriteOnce),
			},
			capability: &csi.VolumeCapability{
				AccessMode: &csi.VolumeCapability_AccessMode{
					Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
				},
			},
			expected: false,
		},
		{
			name: "rwx volume",
			volume: &longhornclient.Volume{
				AccessMode: string(longhorn.AccessModeReadWriteMany),
			},
			capability: &csi.VolumeCapability{
				AccessMode: &csi.VolumeCapability_AccessMode{
					Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
				},
			},
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := requireExclusiveAccess(tc.volume, tc.capability)
			assert.Equal(t, tc.expected, result)
		})
	}
}
