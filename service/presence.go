package service

import (
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/garyburd/redigo/redis"

	"github.com/opentarock/service-api/go/proto"
	"github.com/opentarock/service-api/go/proto_errors"
	"github.com/opentarock/service-api/go/proto_presence"
	"github.com/opentarock/service-api/go/service"
	"github.com/opentarock/service-presence/util/redisutil"
)

const (
	presenceKeyPrefix = "presence.user"
	defaultTImeout    = 60
)

type presenceServiceHandlers struct {
	redisConn redis.Conn
}

func NewPresenceServiceHandlers(conn redis.Conn) *presenceServiceHandlers {
	return &presenceServiceHandlers{
		redisConn: conn,
	}
}

func (s *presenceServiceHandlers) SetUserStatusHandler() service.MessageHandler {
	return service.MessageHandlerFunc(func(msg *proto.Message) proto.CompositeMessage {
		var request proto_presence.UpdateUserStatusRequest
		err := msg.Unmarshal(&request)
		if err != nil {
			log.Println(err)
			return proto.CompositeMessage{
				Message: proto_errors.NewMalformedMessage(request.GetMessageType()),
			}
		}

		deviceString := makeDeviceString(request.GetDevice())
		userKey := makeUserKey(request.GetUserId())
		deviceKey := makeUserDeviceKey(userKey, deviceString)
		if request.GetStatus() == proto_presence.UpdateUserStatusRequest_ONLINE {
			s.redisConn.Send(redisutil.Multi)
			s.redisConn.Send(redisutil.SAdd, userKey, deviceString)
			s.redisConn.Send(redisutil.Incr, deviceKey)
			s.redisConn.Send(redisutil.Expire, deviceKey, defaultTImeout)
			_, err = s.redisConn.Do(redisutil.Exec)
			if err != nil {
				return makeUpdatingUserPresenceError("online", err)
			}
		} else {
			_, err := s.redisConn.Do(redisutil.Del, deviceKey)
			if err != nil {
				return makeUpdatingUserPresenceError("offline", err)
			}
		}

		response := proto_presence.UpdateUserStatusResponse{}
		return proto.CompositeMessage{Message: &response}
	})
}

func makeUpdatingUserPresenceError(status string, err error) proto.CompositeMessage {
	errorString := fmt.Sprintf("Problem updating user presence: %s", status)
	log.Printf("%s: %s", errorString, err)
	return proto.CompositeMessage{
		Message: proto_errors.NewInternalError(fmt.Sprintf("%s.", errorString)),
	}
}

func makeDeviceString(device *proto_presence.Device) string {
	return fmt.Sprintf("%s:%s", device.GetType().String(), getIdForDevice(device))
}

func makeUserKey(userId string) string {
	return fmt.Sprintf("%s:%s", presenceKeyPrefix, userId)
}

func makeUserDeviceKey(userKey, deviceString string) string {
	return fmt.Sprintf("%s:%s", userKey, deviceString)
}

func getIdForDevice(device *proto_presence.Device) string {
	if device.GetType() == proto_presence.Device_ANDROID_GCM {
		return device.GetGcmRegistrationId()
	}
	log.Fatalf("Unknown device type.")
	return ""
}

func (s *presenceServiceHandlers) GetUserDevicesHandler() service.MessageHandler {
	return service.MessageHandlerFunc(func(msg *proto.Message) proto.CompositeMessage {
		var request proto_presence.GetUserDevicesRequest
		err := msg.Unmarshal(&request)
		if err != nil {
			log.Println(err)
			return proto.CompositeMessage{
				Message: proto_errors.NewMalformedMessage(request.GetMessageType()),
			}
		}
		log.Printf("Getting devices for user [user_id=%s]", request.GetUserId())

		userKey := makeUserKey(request.GetUserId())
		deviceList, err := redis.Strings(s.redisConn.Do(redisutil.SMembers, userKey))
		if err != nil {
			return makeGettingUserDevicesError(err)
		}

		devices := make([]*proto_presence.Device, 0)
		for _, deviceString := range deviceList {
			deviceKey := makeUserDeviceKey(userKey, deviceString)
			deviceExists, err := redis.Bool(s.redisConn.Do(redisutil.Exists, deviceKey))
			if err != nil {
				return makeGettingUserDevicesError(err)
			}
			if deviceExists {
				device, err := parseDeviceString(deviceString)
				if err != nil {
					return makeGettingUserDevicesError(err)
				}
				devices = append(devices, device)
			}
		}

		response := proto_presence.GetUserDevicesResponse{
			Devices: devices,
		}
		return proto.CompositeMessage{Message: &response}
	})
}

func makeGettingUserDevicesError(err error) proto.CompositeMessage {
	errorString := "Problem retrieving user's devices"
	log.Printf("%s: %s", errorString, err)
	return proto.CompositeMessage{
		Message: proto_errors.NewInternalError(fmt.Sprintf("%s.", errorString)),
	}
}

func parseDeviceString(deviceString string) (*proto_presence.Device, error) {
	parts := strings.SplitN(deviceString, ":", 2)
	if len(parts) != 2 {
		return nil, errors.New("Invalid device string.")
	}
	deviceTypeInt, ok := proto_presence.Device_Type_value[parts[0]]
	if !ok {
		return nil, fmt.Errorf("Invalid device type: '%s'", parts[0])
	}
	deviceType := proto_presence.Device_Type(deviceTypeInt)
	device := &proto_presence.Device{
		Type: deviceType.Enum(),
	}
	switch deviceType {
	case proto_presence.Device_ANDROID_GCM:
		device.GcmRegistrationId = &parts[1]
	}
	return device, nil
}
