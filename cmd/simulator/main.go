package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"time"
)

func main() {
	fmt.Println("==================================================")
	fmt.Println("      5G Core PDU Session E2E Simulation Runner    ")
	fmt.Println("==================================================")

	// Set environment for in-memory DB fallback
	os.Setenv("DATABASE_URL", "memory")

	// 1. Start Network Functions
	fmt.Println("[+] Starting UDM on Port 8082...")
	udmCmd := exec.Command("go", "run", "cmd/udm/main.go")
	udmCmd.Stdout = os.Stdout
	udmCmd.Stderr = os.Stderr
	if err := udmCmd.Start(); err != nil {
		fmt.Printf("[-] Failed to start UDM: %v\n", err)
		return
	}
	defer killCmd(udmCmd, "UDM")

	fmt.Println("[+] Starting UPF on UDP 8805 / HTTP 8083...")
	upfCmd := exec.Command("go", "run", "cmd/upf/main.go")
	upfCmd.Stdout = os.Stdout
	upfCmd.Stderr = os.Stderr
	if err := upfCmd.Start(); err != nil {
		fmt.Printf("[-] Failed to start UPF: %v\n", err)
		return
	}
	defer killCmd(upfCmd, "UPF")

	fmt.Println("[+] Starting SMF on Port 8081...")
	smfCmd := exec.Command("go", "run", "cmd/smf/main.go")
	smfCmd.Stdout = os.Stdout
	smfCmd.Stderr = os.Stderr
	if err := smfCmd.Start(); err != nil {
		fmt.Printf("[-] Failed to start SMF: %v\n", err)
		return
	}
	defer killCmd(smfCmd, "SMF")

	fmt.Println("[+] Starting AMF on Port 8080...")
	amfCmd := exec.Command("go", "run", "cmd/amf/main.go")
	amfCmd.Stdout = os.Stdout
	amfCmd.Stderr = os.Stderr
	if err := amfCmd.Start(); err != nil {
		fmt.Printf("[-] Failed to start AMF: %v\n", err)
		return
	}
	defer killCmd(amfCmd, "AMF")

	// 2. Wait for NFs to be healthy
	fmt.Println("[*] Waiting for NFs to initialize...")
	time.Sleep(3 * time.Second)

	if !checkHealth("http://localhost:8082/health", "UDM") ||
		!checkHealth("http://localhost:8083/health", "UPF") ||
		!checkHealth("http://localhost:8081/health", "SMF") ||
		!checkHealth("http://localhost:8080/health", "AMF") {
		fmt.Println("[-] Not all NFs are healthy. Aborting simulation.")
		return
	}

	fmt.Println("[+] All Network Functions are healthy! Proceeding with trigger.")

	// 3. Trigger Session Establishment
	triggerUrl := "http://localhost:8080/trigger"
	payload := map[string]interface{}{
		"supi":         "imsi-452040000000001",
		"gpsi":         "msisdn-84900000001",
		"pduSessionId": 101,
		"dnn":          "v-internet",
		"sst":          1,
		"sd":           "000001",
	}

	bodyBytes, _ := json.Marshal(payload)
	fmt.Printf("[*] Sending Trigger POST to %s with IMSI: %s...\n", triggerUrl, payload["supi"])

	resp, err := http.Post(triggerUrl, "application/json", bytes.NewBuffer(bodyBytes))
	if err != nil {
		fmt.Printf("[-] Failed to send trigger: %v\n", err)
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	fmt.Printf("[+] AMF Response Code: %d\n", resp.StatusCode)
	fmt.Printf("[+] AMF Response Body: %s\n", string(body))

	if resp.StatusCode == http.StatusCreated {
		fmt.Println("\n==================================================")
		fmt.Println("   SUCCESS: PDU Session established successfully! ")
		fmt.Println("==================================================")
	} else {
		fmt.Println("\n==================================================")
		fmt.Println("   FAILURE: PDU Session establishment failed.     ")
		fmt.Println("==================================================")
	}

	// Wait a bit to observe background tasks completing
	fmt.Println("[*] Let simulation settle for 2 seconds...")
	time.Sleep(2 * time.Second)

	fmt.Println("[*] Cleaning up background processes...")
}

func checkHealth(url string, name string) bool {
	client := http.Client{Timeout: 500 * time.Millisecond}
	resp, err := client.Get(url)
	if err != nil {
		fmt.Printf("[-] %s Health check failed: %v\n", name, err)
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		fmt.Printf("[+] %s is healthy (200 OK)\n", name)
		return true
	}
	fmt.Printf("[-] %s health check returned status %d\n", name, resp.StatusCode)
	return false
}

func killCmd(cmd *exec.Cmd, name string) {
	if cmd.Process != nil {
		fmt.Printf("[*] Stopping %s...\n", name)
		// Send interrupt signal for graceful shutdown
		cmd.Process.Signal(os.Interrupt)
		
		// Wait or kill
		done := make(chan error, 1)
		go func() {
			done <- cmd.Wait()
		}()
		
		select {
		case <-done:
			// exited cleanly
		case <-time.After(2 * time.Second):
			// force kill
			cmd.Process.Kill()
		}
		fmt.Printf("[+] Stopped %s\n", name)
	}
}
