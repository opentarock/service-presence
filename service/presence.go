package service

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"code.google.com/p/go.net/context"

	"github.com/garyburd/redigo/redis"
	log "gopkg.in/inconshreveable/log15.v2"

	"github.com/opentarock/service-api/go/proto"
	"github.com/opentarock/service-api/go/proto_errors"
	"github.com/opentarock/service-api/go/proto_presence"
	"github.com/opentarock/service-api/go/reqcontext"
	"github.com/opentarock/service-api/go/service"
	"github.com/opentarock/service-presence/util/redisutil"
)

const (
	presenceKeyPrefix = "presence.user"
	defaultTimeout    = 5 * time.Minute
	bucketResolution  = time.Minute

	requestTimeout = 500 * time.Millisecond
)

var numActiveBuckets = uint(defaultTimeout / bucketResolution)

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
		ctx, cancel := context.WithTimeout(reqcontext.NewContext(context.Background(), msg), requestTimeout)
		defer cancel()

		logger := reqcontext.ContextLogger(ctx)

		var request proto_presence.UpdateUserStatusRequest
		err := msg.Unmarshal(&request)
		if err != nil {
			logger.Warn("Problem unmarshalling request", "error", err.Error())
			return proto.CompositeMessage{
				Message: proto_errors.NewMalformedMessage(request.GetMessageType()),
			}
		}

		deviceString, err := makeDeviceString(request.GetDevice())
		if err != nil {
			logger.Error("Problem creating device string", "error", err.Error())
			return proto.CompositeMessage{
				Message: proto_errors.NewInternalError("Unknown device"),
			}
		}
		userKey := makeUserKey(request.GetUserId())
		bucketKey := makeBucketKey(userKey, bucketTag(0))
		deviceKey := makeUserDeviceKey(userKey, deviceString)
		if request.GetStatus() == proto_presence.UpdateUserStatusRequest_ONLINE {
			s.redisConn.Send(redisutil.Multi)
			s.redisConn.Send(redisutil.SAdd, bucketKey, deviceString)
			s.redisConn.Send(redisutil.Expire, bucketKey, uint(defaultTimeout.Seconds()))
			s.redisConn.Send(redisutil.Incr, deviceKey)
			s.redisConn.Send(redisutil.Expire, deviceKey, uint(defaultTimeout.Seconds()))
			_, err = s.redisConn.Do(redisutil.Exec)
			if err != nil {
				return makeUpdatingUserPresenceError(logger, "online", err)
			}
		} else {
			_, err := s.redisConn.Do(redisutil.Del, deviceKey)
			if err != nil {
				return makeUpdatingUserPresenceError(logger, "offline", err)
			}
		}

		response := proto_presence.UpdateUserStatusResponse{}
		return proto.CompositeMessage{Message: &response}
	})
}

func makeUpdatingUserPresenceError(logger log.Logger, status string, err error) proto.CompositeMessage {
	errorString := "Problem updating user presence"
	logger.Error(errorString, log.Ctx{"status": status, "error": err.Error()})
	return proto.CompositeMessage{
		Message: proto_errors.NewInternalError(fmt.Sprintf(errorString, errorString)),
	}
}

func makeDeviceString(device *proto_presence.Device) (string, error) {
	deviceId, err := getIdForDevice(device)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%s", device.GetType().String(), deviceId), nil
}

func makeUserKey(userId string) string {
	return fmt.Sprintf("%s:%s", presenceKeyPrefix, userId)
}

func makeBucketKey(userKey string, bucket string) string {
	return fmt.Sprintf("%s:%s", userKey, bucket)
}

func bucketTag(index int) string {
	bucketTime := time.Now().Add(time.Duration(index) * bucketResolution)
	return fmt.Sprintf("%d", bucketTime.Unix()/int64(bucketResolution.Seconds()))
}

func makeUserDeviceKey(userKey, deviceString string) string {
	return fmt.Sprintf("%s:%s", userKey, deviceString)
}

func getIdForDevice(device *proto_presence.Device) (string, error) {
	if device.GetType() == proto_presence.Device_ANDROID_GCM {
		return device.GetGcmRegistrationId(), nil
	}
	return "", fmt.Errorf("Unknown device type: ", device.GetType())
}

func (s *presenceServiceHandlers) GetUserDevicesHandler() service.MessageHandler {
	return service.MessageHandlerFunc(func(msg *proto.Message) proto.CompositeMessage {
		ctx, cancel := context.WithTimeout(reqcontext.NewContext(context.Background(), msg), requestTimeout)
		defer cancel()

		logger := reqcontext.ContextLogger(ctx)

		var request proto_presence.GetUserDevicesRequest
		err := msg.Unmarshal(&request)
		if err != nil {
			logger.Warn(err.Error())
			return proto.CompositeMessage{
				Message: proto_errors.NewMalformedMessage(request.GetMessageType()),
			}
		}
		userId := request.GetUserId()
		logger.Info("Getting devices for user", "user_id", userId)

		userKey := makeUserKey(userId)
		bucketKeys := activeBucketKeys(userKey, numActiveBuckets)
		deviceList, err := redis.Strings(s.redisConn.Do(redisutil.SUnion, bucketKeys...))
		if err != nil {
			return makeGettingUserDevicesError(logger, userId, err)
		}

		devices := make([]*proto_presence.Device, 0)
		for _, deviceString := range deviceList {
			deviceKey := makeUserDeviceKey(userKey, deviceString)
			deviceExists, err := redis.Bool(s.redisConn.Do(redisutil.Exists, deviceKey))
			if err != nil {
				return makeGettingUserDevicesError(logger, userId, err)
			}
			if deviceExists {
				device, err := parseDeviceString(deviceString)
				if err != nil {
					return makeGettingUserDevicesError(logger, userId, err)
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

func activeBucketKeys(userKey string, num uint) []interface{} {
	keys := make([]interface{}, 0, num)
	for i := 0; i > -int(num); i-- {
		keys = append(keys, makeBucketKey(userKey, bucketTag(i)))
	}
	return keys
}

func makeGettingUserDevicesError(logger log.Logger, userId string, err error) proto.CompositeMessage {
	errorString := "Problem retrieving user's devices"
	logger.Error(errorString, log.Ctx{"user_id": userId, "error": err.Error()})
	return proto.CompositeMessage{
		Message: proto_errors.NewInternalError(fmt.Sprintf(errorString, errorString)),
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
