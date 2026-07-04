import sys

with open('/home/Lan233861/smf/report.md', 'r') as f:
    content = f.read()

# We want to replace everything from "## 3.2. Quá trình tối ưu hiệu năng" to the end of the file.
start_idx = content.find("## 3.2. Quá trình tối ưu hiệu năng")

if start_idx != -1:
    new_content = content[:start_idx] + """## 3.2. Quá trình tối ưu hiệu năng

### Lần kiểm thử 1 – Chuyển đổi sang kiến trúc bất đồng bộ
**Kịch bản:** Benchmark kiến trúc SMF ban đầu theo mô hình xử lý đồng bộ.
**Vấn đề:** Hệ thống chỉ đạt khoảng ~50 TPS, phần lớn request bị timeout (`context deadline exceeded`).
**Phân tích nguyên nhân:** HTTP Handler phải hoàn thành toàn bộ quá trình xử lý (Database, UDM, PFCP, N1N2...) trước khi trả phản hồi cho AMF. Khi số lượng request tăng cao, toàn bộ luồng xử lý bị chặn, dẫn đến timeout và thông lượng rất thấp.
**Giải pháp:** Thiết kế lại SMF theo mô hình Asynchronous Ingress + Worker Pool:
- Handler chỉ tiếp nhận request, cấp phát `smContextRef` và IP.
- Request được đưa vào Queue để xử lý bất đồng bộ.
- Worker Pool thực hiện các bước nghiệp vụ ở phía sau.
**Kết quả:** Thông lượng Ingress tăng lên ~500 req/s, tạo nền tảng cho các bước tối ưu tiếp theo (dù lỗi Error Rate ban đầu vẫn cao do Queue nhỏ).

### Lần kiểm thử 2 – Hoàn thiện công cụ benchmark
**Ngữ cảnh:** Hệ thống vừa được cấu trúc lại dùng Asynchronous Worker Pool, chuẩn bị đo đạc với mốc 2000 - 30.000 TPS.

**🔴 Kết quả TRƯỚC KHI khắc phục:**
*   **Hiện tượng:** Công cụ test báo cáo tốc độ *Ingress TPS* cực cao (lên tới 16.000 req/s với 100% thành công). Tuy nhiên, thực tế phía sau SMF gần như không xử lý được.
*   **Phân tích nguyên nhân:** Công cụ chỉ đo thời điểm HTTP Handler trả về HTTP 201 Created. Trong khi đó, hàng đợi (Queue) nội bộ của SMF bị hardcode ở mức `5000`. Khi hàng chục ngàn request ập tới, Queue đầy và âm thầm vứt bỏ (drop) các request đến sau. Tệ hơn, các session bị drop vẫn vĩnh viễn mang trạng thái `PENDING` trong DB, làm sai lệch toàn bộ dữ liệu giám sát.

**🟢 Kết quả SAU KHI khắc phục:**
*   **Giải pháp áp dụng:** 
    1. Cập nhật `run_load.go` bổ sung vòng lặp Polling API (GET `/api/sessions`) để đo đạc chỉ số **End-to-End (E2E) TPS** thực tế.
    2. Đưa `queueSize` ra file `config.yaml` để tùy biến.
    3. Thêm logic: Khi Queue đầy, lập tức gạch tên session thành `FAILED` trong DB để ngắt chuỗi PENDING vô hạn.
*   **Kết quả đo mới:** Lộ diện giới hạn thực tế của máy tính. Bài test 2500 TPS cho thấy Ingress TPS thực chất chỉ đạt ~2025 req/s (do giới hạn tick 1ms của OS) và E2E TPS bám sát ở mức ~2007 req/s. Mọi session bị drop đều được hạch toán minh bạch là `FAILED`.

### Lần kiểm thử 3 – Tối ưu PostgreSQL
**Ngữ cảnh:** Chuyển từ In-Memory DB sang Database thật (PostgreSQL) và bắt đầu ép giới hạn phần cứng khắc nghiệt (1 Core CPU, 1GB RAM).

**🔴 Kết quả TRƯỚC KHI khắc phục:**
*   **Hiện tượng:** Ngay khi bắt đầu chạy load test, hệ thống rơi vào trạng thái nghẽn cục bộ. E2E TPS rớt thê thảm, log màn hình trôi liên tục cắn hết tài nguyên CPU.
*   **Phân tích nguyên nhân:** Nút thắt cổ chai nằm ở khâu đọc (Read) và I/O.
    1. Cứ mỗi 500ms, script test gọi API `/api/stats`. SMF thực hiện 3 lệnh `SELECT COUNT(*)` riêng lẻ (cho trạng thái ACTIVE, PENDING, FAILED) gây Table Scan liên tục trên bảng dữ liệu hàng triệu dòng.
    2. Logging mặc định (info/debug) sinh ra hàng nghìn thao tác Syscall I/O mỗi giây ra màn hình, cướp đi tài nguyên quý giá của Core CPU duy nhất.

**🟢 Kết quả SAU KHI khắc phục:**
*   **Giải pháp áp dụng:**
    1. Gộp 3 lệnh `SELECT COUNT(*)` thành một lệnh duy nhất dùng `GROUP BY status`.
    2. Giảm cấp độ log toàn hệ thống xuống `LOG_LEVEL=error`.
*   **Kết quả đo mới:** API `/api/stats` phản hồi tức thì, tải CPU và Read-I/O của DB giảm mạnh. Tuy nhiên, với bài test cường độ cao (7.2 triệu request), hệ thống vẫn bộc lộ yếu điểm mới khi sinh ra tới **147.186 lỗi FAILED** (do Queue đẩy không kịp ghi).

### Lần kiểm thử 4 – Tối ưu kiến trúc xử lý (Đột phá hiệu năng)
**Ngữ cảnh:** Quyết tâm dập tắt 147.186 lỗi FAILED ở bài test trước, hướng tới mốc 100% Success Rate cho tải 2000 TPS trong 1 tiếng trên 1 Core CPU.

**🔴 Kết quả TRƯỚC KHI khắc phục:**
*   **Hiện tượng:** Throughput E2E chỉ lẹt đẹt ở ~1952 req/s và mất tới ~2% request (FAILED).
*   **Phân tích nguyên nhân:** Nút thắt cổ chai lúc này chuyển từ khâu Đọc (Read) sang khâu Ghi (Write) của Database.
    1. `synchronous_commit = on` mặc định của Postgres buộc mọi thao tác lưu DB phải chờ ghi vật lý (fsync) xuống đĩa cứng (mất tới ~0.7ms/lần). 
    2. Luồng code chưa tối ưu: Mỗi session đi qua 5-6 lần gọi DB, trong đó có tới 3 lần `GetSession` vô nghĩa chỉ để phục vụ Dashboard hoặc chuẩn bị tạo N1N2 message.

**🟢 Kết quả SAU KHI khắc phục:**
*   **Giải pháp áp dụng:**
    1. Chạy lệnh `ALTER SYSTEM SET synchronous_commit = 'off';` (cho phép DB flush xuống đĩa ngầm sau 200ms).
    2. Tái cấu trúc luồng RAM: Truyền thẳng `PduSessionID, DNN, SST, SD` vào cấu trúc `Job` để loại bỏ toàn bộ các truy vấn `GetSession` thừa. Ép số lần gọi DB xuống mức tối thiểu (3 lần/session).
    3. Thêm hệ thống tracking `failure_reason` thẳng vào DB. Nới rộng không gian hấp thụ xung kích lên `maxWorkers: 50` và `queueSize: 50000`.
*   **Kết quả đo mới (Kỷ lục tuyệt đối):**
    - Mức tải duy trì: **7.200.000 requests** (2000 TPS x 3600s).
    - Tỷ lệ lỗi (FAILED): **Giảm từ 147.186 xuống còn ĐÚNG 0 (Thành công 100%)**.
    - Thời gian phản hồi I/O: Giảm từ `0.7ms` xuống `0.05ms`.
    - True E2E TPS: Ổn định tại mốc **1999.92 req/s**. Hệ thống khai thác triệt để 100% sức mạnh của 1 Core CPU mà không đánh rơi bất kỳ tín hiệu nào.

### Lần kiểm thử 5 – Triển khai trên Kubernetes
**Kịch bản:** Triển khai hệ thống lên Kubernetes và benchmark với tải lớn.
**Vấn đề:** Hệ thống dừng xử lý sau khoảng 30,000 request (chỉ khoảng 15 giây chạy) với lỗi `cannot assign requested address`.
**Phân tích nguyên nhân:** HTTP Client không đọc hết Response Body, khiến Go Runtime không thể tái sử dụng kết nối TCP và dẫn đến Ephemeral Port Exhaustion (Hệ điều hành cạn sạch ~60,000 cổng TCP động).
**Giải pháp:** Bổ sung `io.Copy(io.Discard, resp.Body)` trước khi đóng toàn bộ HTTP Response (UDM, AMF).
**Kết quả:** Loại bỏ hoàn toàn lỗi cạn kiệt cổng TCP, hệ thống duy trì kết nối ổn định trong các lần benchmark tiếp theo (Inress có thể vươn lên mốc ~1978 req/s).

### Lần kiểm thử 6 – Tối ưu Kubernetes (Kết quả Tối ưu Cuối cùng)
**Kịch bản:** Benchmark liên tục 2000 TPS trong 3600 giây (1 giờ) với tài nguyên phần cứng bị giới hạn (1 CPU, 1GB RAM).
**Vấn đề:** Pod SMF bị khởi động lại (Restart) 5 lần và xuất hiện nhiều phản hồi HTTP 503 khi tải tăng cao.
**Phân tích nguyên nhân:**
- Queue đầy tạm thời khi xuất hiện burst traffic (gây lỗi 503).
- Pod sử dụng QoS thấp (`requests.memory: 128Mi`), dẫn đến bị Kubernetes ưu tiên thu hồi tài nguyên (Eviction) để bảo vệ Node.
- Go Runtime chưa chủ động giới hạn bộ nhớ trước khi tràn RAM (OOM).
**Giải pháp:**
- Thiết lập Guaranteed QoS (`requests.cpu = 1`, `requests.memory = 1Gi`).
- Cấu hình biến môi trường giới hạn Garbage Collector `GOMEMLIMIT=900MiB`.
- Duy trì cơ chế Load Shedding (chủ động từ chối bằng mã 503) để bảo vệ hệ thống khi Queue đầy.
**Kết quả cuối cùng đo được:**
- **Tổng Request nạp vào:** 7,199,900 requests / 3600.02s
- **Ingress TPS Thực tế:** 1,999.34 req/s
- **Ingress Success:** 7,197,680 (99.97%)
- **TRUE E2E TPS:** 1,996.38 req/s
- **E2E FAILED Sessions:** 0 (Tuyệt đối an toàn)
- **Tình trạng K8s:** Pod hoạt động cực kỳ ổn định trong suốt 1 giờ, 0 lần Restart. Mức sử dụng RAM duy trì ở khoảng 60 MB.
"""

    with open('/home/Lan233861/smf/report.md', 'w') as f:
        f.write(new_content)
    print("Successfully replaced.")
else:
    print("Could not find start index.")
