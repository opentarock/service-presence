package main

import (
	"flag"
	"log"
	"net/url"
	"os"
	"os/signal"
	"runtime/pprof"

	"github.com/opentarock/service-api/go/proto_presence"
	nservice "github.com/opentarock/service-api/go/service"
	"github.com/opentarock/service-presence/device"
	"github.com/opentarock/service-presence/service"
	"github.com/opentarock/service-presence/util/redisutil"
)

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

func main() {
	flag.Parse()
	// profiliing related flag
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	log.SetFlags(log.Ldate | log.Lmicroseconds)

	pool := redisutil.NewPool(newRedisAddress())
	defer pool.Close()

	notifyService := nservice.NewRepService(nservice.MakeServiceBindAddress(nservice.PresenceServiceDefaultPort))

	handlers := service.NewPresenceServiceHandlers(device.NewRedisRepository(pool))
	notifyService.AddHandler(proto_presence.UpdateUserStatusRequestMessage, handlers.SetUserStatusHandler())
	notifyService.AddHandler(proto_presence.GetUserDevicesRequestMessage, handlers.GetUserDevicesHandler())

	err := notifyService.Start()
	if err != nil {
		log.Fatalf("Error starting presence service: %s", err)
	}
	defer notifyService.Close()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	sig := <-c
	log.Printf("Interrupted by %s", sig)
}

func newRedisAddress() string {
	const defaultAddress = "localhost:6379"
	v := os.Getenv("REDIS_PORT")
	addr, err := parseRedisAddress(v)
	if v == "" || err != nil {
		return defaultAddress
	}
	return addr
}

func parseRedisAddress(rawUrl string) (string, error) {
	u, err := url.Parse(rawUrl)
	if err != nil {
		return "", err
	}
	return u.Host, nil
}
