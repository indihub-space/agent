package share

import (
	"context"
	"log"

	"github.com/fatih/color"

	"github.com/indihub-space/agent/hostutils"
	"github.com/indihub-space/agent/lib"
	"github.com/indihub-space/agent/proto/indihub"
	"github.com/indihub-space/agent/proxy"
)

type Mode struct {
	indiHubClient indihub.INDIHubClient
	regInfo       *indihub.RegisterInfo

	indiServerAddr string
	phd2ServerAddr string

	addrData []proxy.PublicServerAddr

	stopCh chan struct{}
	status string
	mode   string
}

func NewMode(indiHubClient indihub.INDIHubClient, regInfo *indihub.RegisterInfo, indiServerAddr string, phd2ServerAddr string, mode string) *Mode {
	return &Mode{
		indiHubClient:  indiHubClient,
		regInfo:        regInfo,
		indiServerAddr: indiServerAddr,
		phd2ServerAddr: phd2ServerAddr,
		mode:           mode,
		addrData:       []proxy.PublicServerAddr{},
		stopCh:         make(chan struct{}, 1),
	}
}

func (m *Mode) Start() {
	// main equipment sharing mode
	if m.mode == lib.ModeRobotic {
		log.Println("'robotic' parameter was provided. Your session is in robotic-mode: equipment sharing is not available")
	}
	// open INDI server tunnel
	log.Println("Starting INDI-Server in the cloud...")
	indiServTunnel, err := m.indiHubClient.INDIServer(
		context.Background(),
	)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("...OK")

	indiFilterConf := &hostutils.INDIFilterConfig{} // TODO: add reading config
	indiFilter := hostutils.NewINDIFilter(indiFilterConf)
	indiServerProxy := proxy.New("INDI-Server", m.indiServerAddr, indiServTunnel, indiFilter)

	// start PHD2 server proxy if specified
	var phd2ServerProxy *proxy.TcpProxy
	if m.phd2ServerAddr != "" {
		// open PHD2 server tunnel
		log.Println("Starting PHD2-Server in the cloud...")
		phd2ServTunnel, err := m.indiHubClient.PHD2Server(
			context.Background(),
		)
		if err != nil {
			log.Fatal(err)
		}
		log.Println("...OK")
		phd2ServerProxy = proxy.New("PHD2-Server", m.phd2ServerAddr, phd2ServTunnel, nil)
	}

	go func() {
		<-m.stopCh

		log.Printf("Closing %s-session\n", m.mode)

		// close connections to tunnels
		indiServTunnel.CloseSend()
		if phd2ServerProxy != nil {
			phd2ServerProxy.Tunnel.CloseSend()
		}

		// close connections to local INDI-server and PHD2-Server
		indiServerProxy.Close()
		if phd2ServerProxy != nil {
			phd2ServerProxy.Close()
		}
	}()

	serverAddrChan := make(chan proxy.PublicServerAddr, 3)

	// INDI Server Proxy start
	go indiServerProxy.Start(serverAddrChan, m.regInfo.SessionID, m.regInfo.SessionIDPublic)
	sAddr := <-serverAddrChan
	if m.mode != lib.ModeRobotic {
		m.addrData = append(m.addrData, sAddr)
	}

	// PHD2 Server proxy start
	if m.phd2ServerAddr != "" {
		go phd2ServerProxy.Start(serverAddrChan, m.regInfo.SessionID, m.regInfo.SessionIDPublic)
		sAddr := <-serverAddrChan
		if m.mode != lib.ModeRobotic {
			m.addrData = append(m.addrData, sAddr)
		}
	}

	c := color.New(color.FgCyan)
	gc := color.New(color.FgGreen)
	yc := color.New(color.FgYellow)
	if m.mode != lib.ModeRobotic {
		c.Println()
		c.Println("                                ************************************************************")
		c.Println("                                *               INDIHUB public address list!!              *")
		c.Println("                                ************************************************************")
		c.Println("                                                                                            ")
		for _, sAddr := range m.addrData {
			gc.Printf("                                   %s: %s\n", sAddr.Name, sAddr.Addr)
		}
		c.Println("                                                                                            ")
		c.Println("                                ************************************************************")
		c.Println()
		c.Println("                                Please provide your guest with this information:")
		c.Println()
		c.Println("                                1. Public address list from the above")
		c.Println("                                2. Focal length and aperture of your main telescope")
		c.Println("                                3. Focal length and aperture of your guiding telescope")
		c.Println("                                4. Type of guiding you use: PHD2 or guiding via camera")
		c.Println("                                5. Names of your imaging camera and guiding cameras")
		c.Println()
		yc.Println("                                NOTE: These public addresses will be available ONLY while")
		yc.Println("                                the agent is running! (Ctrl+C will stop the session)")
		c.Println()
	} else {
		c.Println()
		c.Println("                                ************************************************************")
		c.Println("                                *               INDIHUB robotic-session started!!          *")
		c.Println("                                ************************************************************")
		c.Println("                                                                                            ")
	}

	m.status = "running"
}

func (m *Mode) Stop() {
	m.status = "stopped"
	m.stopCh <- struct{}{}

	c := color.New(color.FgCyan)
	rc := color.New(color.FgMagenta)
	c.Println()
	c.Println("                                ************************************************************")
	c.Println("                                *               INDIHUB session finished!!                 *")
	c.Println("                                ************************************************************")
	c.Println("                                                                                            ")
	if m.mode != lib.ModeRobotic {
		for _, sAddr := range m.addrData {
			rc.Printf("                                   %s: %s - CLOSED!!\n", sAddr.Name, sAddr.Addr)
		}
	} else {
		c.Println("                                *         INDIHUB robotic-session finished.                 *")
		c.Println("                                *         Thank you for your contribution!                  *")
	}
	c.Println("                                                                                            ")
	c.Println("                                ************************************************************")

	m.addrData = []proxy.PublicServerAddr{}
}

func (m *Mode) GetStatus() map[string]interface{} {
	return map[string]interface{}{
		"status":          m.status,
		"publicEndpoints": m.addrData,
	}
}
