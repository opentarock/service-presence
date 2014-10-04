package device

import (
	"errors"
	"fmt"
	"time"

	"github.com/garyburd/redigo/redis"

	"github.com/opentarock/service-api/go/proto_presence"
	"github.com/opentarock/service-presence/util/redisutil"
)

const (
	presenceKeyPrefix = "presence.user"
	defaultTimeout    = 5 * time.Minute
	bucketResolution  = time.Minute
)

var numActiveBuckets = uint(defaultTimeout / bucketResolution)

type RedisRepository struct {
	pool *redis.Pool
}

func NewRedisRepository(pool *redis.Pool) *RedisRepository {
	return &RedisRepository{
		pool: pool,
	}
}

func (r *RedisRepository) SetDeviceStatus(
	userId string,
	status proto_presence.UpdateUserStatusRequest_Status,
	userDevice *proto_presence.Device) error {

	conn := r.pool.Get()
	defer conn.Close()

	if userDevice == nil {
		return errors.New("User device is nil")
	}

	device := New(userId, userDevice, presenceKeyPrefix)
	bucketKey := device.CurrentBucketKey()
	deviceKey, err := device.UserDeviceKey()
	if err != nil {
		return err
	}

	var redisErr error
	if status == proto_presence.UpdateUserStatusRequest_ONLINE {
		conn.Send(redisutil.Multi)
		conn.Send(redisutil.SAdd, bucketKey, deviceKey)
		conn.Send(redisutil.Expire, bucketKey, uint(defaultTimeout.Seconds()))
		conn.Send(redisutil.Incr, deviceKey)
		conn.Send(redisutil.Expire, deviceKey, uint(defaultTimeout.Seconds()))
		_, redisErr = conn.Do(redisutil.Exec)
	} else {
		_, redisErr = conn.Do(redisutil.Del, deviceKey)
	}
	if redisErr != nil {
		return fmt.Errorf("Error updating user status (%s): %s", status.String(), redisErr)
	}
	return nil
}

func (r *RedisRepository) GetActiveDevices(userId string) ([]*proto_presence.Device, error) {
	conn := r.pool.Get()
	defer conn.Close()

	userDevice := New(userId, nil, presenceKeyPrefix)
	bucketKeys := AllActiveBuckets(userDevice, numActiveBuckets)
	deviceList, err := redis.Strings(conn.Do(redisutil.SUnion, toInterfaceSlice(bucketKeys)...))
	if err != nil {
		return nil, err
	}

	devices := make([]*proto_presence.Device, 0)
	for _, deviceKey := range deviceList {
		userDevice, err := Parse(deviceKey)
		// TODO: this error is critical and represents an error in the
		// implementaion, should be logged
		if err != nil {
			return nil, err
		}
		// This error can not occur here because we just parsed the device above.
		deviceKey, _ := userDevice.UserDeviceKey()
		deviceExists, err := redis.Bool(conn.Do(redisutil.Exists, deviceKey))
		if err != nil {
			return nil, err
		}
		if deviceExists {
			devices = append(devices, userDevice.ProtoDevice())
		}
	}
	return devices, nil
}

func toInterfaceSlice(slice []string) []interface{} {
	result := make([]interface{}, 0, len(slice))
	for _, s := range slice {
		result = append(result, s)
	}
	return result
}
