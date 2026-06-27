package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type Stats struct {
	Active  int64 `json:"active"`
	Pending int64 `json:"pending"`
	Failed  int64 `json:"failed"`
}

func main() {
	targetTPS := 2000
	durationSeconds := 3600

	fmt.Printf(" Đang khởi động chiến dịch Load Test: %d TPS trong %d giây...\n", targetTPS, durationSeconds)

	payload := []byte(`{
		"supi":"imsi-452040000000001",
		"gpsi":"msisdn-84900000001",
		"pduSessionId": 101,
		"dnn":"v-internet",
		"sst":1,
		"sd":"000001"
	}`)

	client := &http.Client{
		Timeout: 3 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        5000,
			MaxIdleConnsPerHost: 5000,
		},
	}

	var successCount int64
	var errorCount int64
	var wg sync.WaitGroup

	// Batch-per-tick: mỗi 10ms bắn 1 batch
	// 2500 TPS / 100 tick/giây = 25 requests/tick
	tickInterval := 10 * time.Millisecond
	batchSize := targetTPS / 100 // 25 requests mỗi tick

	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	timer := time.NewTimer(time.Duration(durationSeconds) * time.Second)
	defer timer.Stop()

	startTime := time.Now()

Loop:
	for {
		select {
		case <-timer.C:
			break Loop
		case <-ticker.C:
			// Mỗi tick spawn đúng batchSize goroutine
			for i := 0; i < batchSize; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()

					req, _ := http.NewRequest("POST", "http://localhost:8080/trigger", bytes.NewBuffer(payload))
					req.Header.Set("Content-Type", "application/json")

					resp, err := client.Do(req)
					if err != nil {
						atomic.AddInt64(&errorCount, 1)
						return
					}
					resp.Body.Close()

					if resp.StatusCode == http.StatusCreated {
						atomic.AddInt64(&successCount, 1)
					} else {
						atomic.AddInt64(&errorCount, 1)
					}
				}()
			}
		}
	}

	fmt.Println("⏳ Đã dừng bắn request. Đang chờ các request cuối cùng hoàn tất...")
	wg.Wait()

	actualDuration := time.Since(startTime).Seconds()
	totalReq := successCount + errorCount

	fmt.Println("\n==================================================")
	fmt.Println("BÁO CÁO INGRESS (FRONTEND) TPS - KẾT QUẢ GỬI REQUEST")
	fmt.Println("==================================================")
	fmt.Printf("Mục tiêu:       %d TPS x %d giây = %d requests\n", targetTPS, durationSeconds, targetTPS*durationSeconds)
	fmt.Printf("Thực tế bắn:    %d requests trong %.2f giây\n", totalReq, actualDuration)
	fmt.Printf("Gửi Thành công: %d (%.2f%%)\n", successCount, float64(successCount)/float64(totalReq)*100)
	fmt.Printf("Lỗi/Timeout:    %d (%.2f%%)\n", errorCount, float64(errorCount)/float64(totalReq)*100)
	fmt.Printf("Ingress TPS:    %.2f req/s\n", float64(successCount)/actualDuration)
	fmt.Println("==================================================")

	// --- ĐO TRUE E2E TPS ---
	fmt.Println("\n Đang chờ SMF Worker xử lý xong toàn bộ luồng E2E (UPF + N1N2)...")
	startE2E := time.Now()

	for {
		resp, err := client.Get("http://localhost:8081/api/stats")
		if err != nil {
			fmt.Printf("[-] Không kết nối được SMF để lấy trạng thái session: %v\n", err)
			break
		}

		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			fmt.Printf("\n[-] SMF /api/stats trả về HTTP %d: %s\n", resp.StatusCode, string(body))
			break
		}

		var stats Stats
		if err := json.Unmarshal(body, &stats); err != nil {
			fmt.Printf("\n[-] Không parse được JSON từ SMF /api/stats: %v\n", err)
			break
		}

		totalDone := stats.Active + stats.Failed

		fmt.Printf("\rTrạng thái SMF: [Hoàn thành E2E: %d] - [Đang kẹt ở Queue: %d] - [Lỗi: %d]", stats.Active, stats.Pending, stats.Failed)

		if totalDone >= successCount || (stats.Pending == 0 && totalDone > 0) {
			e2eDuration := time.Since(startE2E).Seconds() + actualDuration
			fmt.Println("\n\n==================================================")
			fmt.Println("BÁO CÁO TRUE E2E (BACKEND) TPS")
			fmt.Println("==================================================")
			fmt.Printf("Tổng thời gian hoàn thành E2E: %.2f giây\n", e2eDuration)
			fmt.Printf("Số Sessions hoàn thành (ACTIVE): %d\n", stats.Active)
			fmt.Printf("Số Sessions lỗi (FAILED):        %d\n", stats.Failed)
			fmt.Printf("TRUE E2E TPS:                    %.2f req/s\n", float64(stats.Active)/e2eDuration)
			fmt.Println("==================================================")
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
}
