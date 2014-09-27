package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"runtime/pprof"

	"github.com/garyburd/redigo/redis"

	"github.com/opentarock/service-api/go/proto_presence"
	nservice "github.com/opentarock/service-api/go/service"
	"github.com/opentarock/service-presence/service"
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

	redisConn, err := redis.Dial("tcp", "localhost:6379")
	if err != nil {
		log.Fatalf("Problem connecting to redis server: %d", err)
	}
	defer redisConn.Close()

	notifyService := nservice.NewRepService(nservice.MakeServiceBindAddress(nservice.PresenceServiceDefaultPort))

	handlers := service.NewPresenceServiceHandlers(redisConn)
	notifyService.AddHandler(proto_presence.UpdateUserStatusRequestMessage, handlers.SetUserStatusHandler())
	notifyService.AddHandler(proto_presence.GetUserDevicesRequestMessage, handlers.GetUserDevicesHandler())

	err = notifyService.Start()
	if err != nil {
		log.Fatalf("Error starting presence service: %s", err)
	}
	defer notifyService.Close()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	sig := <-c
	log.Printf("Interrupted by %s", sig)
}
