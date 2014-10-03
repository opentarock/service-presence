package service

import (
	"fmt"
	"time"

	"code.google.com/p/go.net/context"

	"github.com/garyburd/redigo/redis"
	log "gopkg.in/inconshreveable/log15.v2"

	"github.com/opentarock/service-api/go/proto"
	"github.com/opentarock/service-api/go/proto_errors"
	"github.com/opentarock/service-api/go/proto_presence"
	"github.com/opentarock/service-api/go/reqcontext"
	"github.com/opentarock/service-api/go/service"
	"github.com/opentarock/service-presence/device"
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

		device := device.New(request.GetUserId(), request.GetDevice(), presenceKeyPrefix)
		bucketKey := device.CurrentBucketKey()
		deviceKey, err := device.UserDeviceKey()
		if err != nil {
			logger.Error("Problem creating device string", "error", err.Error())
			return proto.CompositeMessage{
				Message: proto_errors.NewInternalError("Device error"),
			}
		}
		if request.GetStatus() == proto_presence.UpdateUserStatusRequest_ONLINE {
			s.redisConn.Send(redisutil.Multi)
			s.redisConn.Send(redisutil.SAdd, bucketKey, deviceKey)
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

		userDevice := device.New(request.GetUserId(), nil, presenceKeyPrefix)
		bucketKeys := device.AllActiveBuckets(userDevice, numActiveBuckets)
		deviceList, err := redis.Strings(s.redisConn.Do(redisutil.SUnion, toInterfaceSlice(bucketKeys)...))
		if err != nil {
			return makeGettingUserDevicesError(logger, userId, err)
		}

		devices := make([]*proto_presence.Device, 0)
		for _, deviceKey := range deviceList {
			userDevice, err := device.Parse(deviceKey)
			if err != nil {
				return makeGettingUserDevicesError(logger, userId, err)
			}
			// This error can not occur here because we just parsed the device above.
			deviceKey, _ := userDevice.UserDeviceKey()
			deviceExists, err := redis.Bool(s.redisConn.Do(redisutil.Exists, deviceKey))
			if err != nil {
				return makeGettingUserDevicesError(logger, userId, err)
			}
			if deviceExists {
				devices = append(devices, userDevice.ProtoDevice())
			}
		}

		response := proto_presence.GetUserDevicesResponse{
			Devices: devices,
		}
		return proto.CompositeMessage{Message: &response}
	})
}

func toInterfaceSlice(slice []string) []interface{} {
	result := make([]interface{}, 0, len(slice))
	for _, s := range slice {
		result = append(result, s)
	}
	return result
}

func makeGettingUserDevicesError(logger log.Logger, userId string, err error) proto.CompositeMessage {
	errorString := "Problem retrieving user's devices"
	logger.Error(errorString, log.Ctx{"user_id": userId, "error": err.Error()})
	return proto.CompositeMessage{
		Message: proto_errors.NewInternalError(fmt.Sprintf(errorString, errorString)),
	}
}
