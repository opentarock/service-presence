package device

import "github.com/opentarock/service-api/go/proto_presence"

type Repository interface {
	SetDeviceStatus(
		userId string,
		status proto_presence.UpdateUserStatusRequest_Status,
		device *proto_presence.Device) error

	GetActiveDevices(userId string) ([]*proto_presence.Device, error)
}
