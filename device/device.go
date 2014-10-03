package device

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/opentarock/service-api/go/proto_presence"
)

const (
	defaultBucketResolution = time.Minute
)

type UserDevice struct {
	prefix           string
	device           *proto_presence.Device
	BucketResolution time.Duration
	userKey          string
}

func New(userId string, device *proto_presence.Device, prefix string) *UserDevice {
	return &UserDevice{
		prefix:           prefix,
		device:           device,
		BucketResolution: defaultBucketResolution,
		userKey:          newUserKey(prefix, userId),
	}
}

func Parse(userDeviceKey string) (*UserDevice, error) {
	parts := strings.SplitN(userDeviceKey, ":", 4)
	if len(parts) != 4 {
		return nil, errors.New("Invalid device string")
	}
	device, err := parseProtoDevice(parts[2], parts[3])
	if err != nil {
		return nil, err
	}
	userDevice := New(parts[1], device, parts[0])
	return userDevice, nil
}

func parseProtoDevice(deviceTypeString, regId string) (*proto_presence.Device, error) {
	deviceTypeInt, ok := proto_presence.Device_Type_value[deviceTypeString]
	if !ok {
		return nil, fmt.Errorf("Invalid device type: '%s'", deviceTypeString)
	}
	deviceType := proto_presence.Device_Type(deviceTypeInt)
	device := &proto_presence.Device{
		Type: deviceType.Enum(),
	}
	switch deviceType {
	case proto_presence.Device_ANDROID_GCM:
		device.GcmRegistrationId = &regId
	}
	return device, nil
}

func (d *UserDevice) ProtoDevice() *proto_presence.Device {
	return d.device
}

func (d *UserDevice) CurrentBucketKey() string {
	return d.RelativeBucketKey(0)
}

func (d *UserDevice) RelativeBucketKey(i int) string {
	return fmt.Sprintf("%s:%s", d.userKey, NewBucket(d.BucketResolution, i))
}

func (d *UserDevice) UserDeviceKey() (string, error) {
	deviceString, err := newDeviceString(d.device)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%s", d.userKey, deviceString), nil
}

func newDeviceString(device *proto_presence.Device) (string, error) {
	if device == nil {
		return "", errors.New("Device is nil")
	}
	deviceId, err := deviceId(device)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%s", device.GetType().String(), deviceId), nil
}

func newUserKey(prefix string, userId string) string {
	return fmt.Sprintf("%s:%s", prefix, userId)
}

func deviceId(device *proto_presence.Device) (string, error) {
	if device.GetType() == proto_presence.Device_ANDROID_GCM {
		return device.GetGcmRegistrationId(), nil
	}
	return "", fmt.Errorf("Unknown device type: ", device.GetType())
}

func NewBucket(res time.Duration, i int) string {
	bucketTime := time.Now().Add(time.Duration(i) * res)
	return fmt.Sprintf("%d", bucketTime.Unix()/int64(res.Seconds()))
}

func AllActiveBuckets(d *UserDevice, n uint) []string {
	keys := make([]string, 0, n)
	for i := 0; i > -int(n); i-- {
		keys = append(keys, d.RelativeBucketKey(i))
	}
	return keys
}
