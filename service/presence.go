package service

import (
	"time"

	"code.google.com/p/go.net/context"

	log "gopkg.in/inconshreveable/log15.v2"

	"github.com/opentarock/service-api/go/proto"
	"github.com/opentarock/service-api/go/proto_errors"
	"github.com/opentarock/service-api/go/proto_presence"
	"github.com/opentarock/service-api/go/reqcontext"
	"github.com/opentarock/service-api/go/service"
	"github.com/opentarock/service-presence/device"
)

const (
	presenceKeyPrefix = "presence.user"
	defaultTimeout    = 5 * time.Minute
	bucketResolution  = time.Minute

	requestTimeout = 500 * time.Millisecond
)

var numActiveBuckets = uint(defaultTimeout / bucketResolution)

type presenceServiceHandlers struct {
	repository device.Repository
}

func NewPresenceServiceHandlers(repo device.Repository) *presenceServiceHandlers {
	return &presenceServiceHandlers{
		repository: repo,
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

		err = s.repository.SetDeviceStatus(request.GetUserId(), request.GetStatus(), request.GetDevice())
		if err != nil {
			errorString := "Problem updating user presence"
			logger.Error(errorString, log.Ctx{"status": request.GetStatus().String(), "error": err.Error()})
			return proto.CompositeMessage{
				Message: proto_errors.NewInternalError(errorString),
			}
		}

		response := proto_presence.UpdateUserStatusResponse{}
		return proto.CompositeMessage{Message: &response}
	})
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

		devices, err := s.repository.GetActiveDevices(userId)
		if err != nil {
			errorString := "Problem retrieving user's devices"
			logger.Error(errorString, log.Ctx{"user_id": userId, "error": err.Error()})
			return proto.CompositeMessage{
				Message: proto_errors.NewInternalError(errorString),
			}
		}

		response := proto_presence.GetUserDevicesResponse{
			Devices: devices,
		}
		return proto.CompositeMessage{Message: &response}
	})
}
