package redisutil

import (
	"time"

	"github.com/garyburd/redigo/redis"
)

const (
	maxIdleConnextions    = 100
	idleConnectionTimeout = 5 * time.Minute
)

func NewPool(server string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     maxIdleConnextions,
		IdleTimeout: idleConnectionTimeout,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			return DoPing(c)
		},
	}
}

func DoPing(c redis.Conn) error {
	_, err := c.Do(Ping)
	return err
}
