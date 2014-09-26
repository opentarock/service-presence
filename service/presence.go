package service

import (
	"log"

	"github.com/opentarock/service-api/go/proto"
	"github.com/opentarock/service-api/go/proto_errors"
	"github.com/opentarock/service-api/go/proto_presence"
	"github.com/opentarock/service-api/go/service"
)

type presenceServiceHandlers struct{}

func NewPresenceServiceHandlers() *presenceServiceHandlers {
	return &presenceServiceHandlers{}
}

func (s *presenceServiceHandlers) SetUserStatusHandler() service.MessageHandler {
	return service.MessageHandlerFunc(func(msg *proto.Message) proto.CompositeMessage {
		var request proto_presence.SetUserStatusRequest
		err := msg.Unmarshal(&request)
		if err != nil {
			log.Println(err)
			return proto.CompositeMessage{
				Message: proto_errors.NewMalformedMessage(request.GetMessageType()),
			}
		}
		response := proto_presence.SetUserStatusResponse{}
		return proto.CompositeMessage{Message: &response}
	})
}
