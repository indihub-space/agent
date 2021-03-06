package solo

import (
	"context"
	"log"
	"time"

	"github.com/indihub-space/agent/proto/indihub"
)

type Mode struct {
	indiServerAddr string
	indiHubClient  indihub.INDIHubClient
	regInfo        *indihub.RegisterInfo

	stopCh chan struct{}
	status string
}

func NewMode(indiHubClient indihub.INDIHubClient, regInfo *indihub.RegisterInfo, indiServerAddr string) *Mode {
	return &Mode{
		indiServerAddr: indiServerAddr,
		indiHubClient:  indiHubClient,
		regInfo:        regInfo,
		stopCh:         make(chan struct{}, 1),
	}
}

func (s *Mode) Start() {
	// solo mode - equipment sharing is not available but host still sends all images to INDIHUB
	log.Println("'solo' parameter was provided. Your session is in solo-mode: equipment sharing is not available")
	log.Println("Starting INDIHUB agent in solo mode!")

	soloClient, err := s.indiHubClient.SoloMode(context.Background())
	if err != nil {
		log.Fatalf("Could not start agent in solo mode: %v", err)
	}

	soloAgent := New(
		s.indiServerAddr,
		soloClient,
	)

	go func() {
		<-s.stopCh
		log.Println("Closing INDIHUB solo-session")
		// close connections to local INDI-server
		soloAgent.Close()
	}()

	// start agent in solo-mode
	go func() {
		soloAgent.Start(s.regInfo.SessionID, s.regInfo.SessionIDPublic)
	}()

	s.status = "running"
}

func (s *Mode) Stop() {
	s.status = "stopped"
	s.stopCh <- struct{}{}
	time.Sleep(3 * time.Second) // give some time to get and display solo-session summary
}

func (s *Mode) GetStatus() map[string]interface{} {
	return map[string]interface{}{
		"status": s.status,
	}
}
