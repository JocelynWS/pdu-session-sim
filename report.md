# Báo Cáo Tìm Hiểu 5G Core & Chức Năng SMF

Báo cáo này trình bày các nội dung tìm hiểu về kiến trúc mạng lõi 5G (5G Core - 5GC) theo chuẩn 3GPP, vai trò của Session Management Function (SMF) và chi tiết luồng nghiệp vụ thiết lập PDU Session (PDU Session Establishment).

---

## 1. Kiến Trúc Mạng 5G Core (Service-Based Architecture - SBA)

Kiến trúc mạng lõi 5G (5G Core) được thiết kế theo mô hình kiến trúc dịch vụ (Service-Based Architecture - SBA), trong đó các phần tử mạng được gọi là các **Network Function (NF)**. Các NF giao tiếp với nhau qua các giao diện dịch vụ chuẩn hóa (SBI - Service-Based Interface) sử dụng giao thức HTTP/2 (hoặc HTTP/3) và định dạng dữ liệu JSON.

### Các thành phần chính trong hệ thống mô phỏng:
1. **AMF (Access and Mobility Management Function)**:
   - Quản lý truy cập và di động của UE (User Equipment).
   - Tiếp nhận các thông điệp NAS từ UE và chuyển tiếp các yêu cầu quản lý phiên (Session Management - SM) tới SMF.
2. **SMF (Session Management Function)**:
   - Chịu trách nhiệm quản lý phiên kết nối (PDU Session): thiết lập, sửa đổi và giải phóng phiên.
   - Phân bổ địa chỉ IP cho UE.
   - Lựa chọn và điều khiển UPF thông qua giao thức PFCP (Packet Forwarding Control Protocol).
   - Truy vấn thông tin thuê bao từ UDM.
3. **UDM (Unified Data Management)**:
   - Quản lý dữ liệu thuê bao, thông tin xác thực và hồ sơ dịch vụ (Subscription Data).
4. **UPF (User Plane Function)**:
   - Chịu trách nhiệm xử lý luồng dữ liệu người dùng (User Plane).
   - Định tuyến, chuyển tiếp gói tin, thực thi QoS và đếm dung lượng.

---

## 2. Vai Trò Của SMF Trong Hệ Thống

SMF là "trái tim" điều khiển phiên của 5G Core. Vai trò chính bao gồm:
- **Quản lý trạng thái phiên**: Theo dõi vòng đời của PDU Session (PENDING, CREATING, ACTIVE, CONNECTED, RELEASED).
- **Điều khiển User Plane**: Giao tiếp với UPF qua giao diện N4 (sử dụng giao thức PFCP) để cài đặt các quy tắc chuyển tiếp gói tin (PDR - Packet Detection Rules, FAR - Forwarding Action Rules).
- **Phân bổ IP**: Cấp phát địa chỉ IPv4/IPv6 cho UE.
- **Tương tác với AMF và UDM**: Tiếp nhận các yêu cầu thiết lập từ AMF, kiểm tra điều kiện thuê bao từ UDM và gửi thông tin cấu hình vô tuyến (N1/N2 container) về AMF để gửi tiếp xuống gNodeB/UE.

---

## 3. Quy Trình Thiết Lập PDU Session (PDU Session Establishment)

Quy trình thiết lập PDU Session tối giản bao gồm 17 bước chính được mô tả chi tiết dưới đây:

### Sơ Đồ Sequence Diagram (Mermaid)

```mermaid
sequenceDiagram
    autonumber
    participant AMF as AMF (Port 8080)
    participant SMF as SMF (Port 8081)
    participant UDM as UDM (Port 8082)
    participant UPF as UPF (Port 8805 UDP)
    database DB as PostgreSQL / In-Memory

    Note over AMF, SMF: Bước 3: Khởi tạo SM Context
    AMF->>SMF: HTTP/2 POST /nsmf-pdusession/v1/sm-contexts<br>(supi, gpsi, pduSessionId, dnn, sNssai)
    Note over SMF: Trạng thái: PENDING
    SMF->>DB: Lưu Session tạm thời (PENDING)

    Note over SMF, UDM: Bước 4: Xác thực thuê bao
    SMF->>UDM: HTTP/2 GET /nudm-sdm/v2/{imsi}/sm-data
    UDM->>DB: Query thông tin thuê bao
    DB-->>UDM: Trả về kết quả
    UDM-->>SMF: HTTP/2 200 OK (Subscription Data)
    
    Note over SMF: Validate dnn & sNssai
    
    Note over SMF, AMF: Bước 5: Trả lời CreateSMContext
    SMF-->>AMF: HTTP/2 201 Created (smContextRef, cause)
    
    Note over SMF: Asynchronous Processing<br>Trạng thái: CREATING
    SMF->>DB: Update Status = CREATING

    Note over SMF, UPF: Bước 10a & 10b: Thiết lập N4 Session
    SMF->>UPF: UDP PFCP Session Establishment Request (Type 50)
    Note over UPF: Tạo session nội bộ
    UPF-->>SMF: UDP PFCP Session Establishment Response (Type 51, Cause=1)
    Note over SMF: Trạng thái: ACTIVE
    SMF->>DB: Update Status = ACTIVE

    Note over SMF, AMF: Bước 11: N1N2 Message Transfer
    SMF->>AMF: HTTP/2 POST /namf-comm/v1/ue-context/{imsi}/n1-n2-messages
    AMF-->>SMF: HTTP/2 200 OK
    
    Note over AMF: Giả lập Access Network (AN) setup hoàn tất
    
    Note over AMF, SMF: Bước 15: Cập nhật SM Context
    AMF->>SMF: HTTP/2 POST /nsmf-pdusession/v1/sm-contexts/{smContextRef}/modify<br>(upCnxState = ACTIVATED)
    Note over SMF: Trạng thái: ACTIVATING
    SMF->>DB: Update Status = ACTIVATING

    Note over SMF, UPF: Bước 16a & 16b: Cập nhật N4 Session
    SMF->>UPF: UDP PFCP Session Modification Request (Type 52)
    Note over UPF: Cập nhật session nội bộ
    UPF-->>SMF: UDP PFCP Session Modification Response (Type 53, Cause=1)
    
    Note over SMF: Trạng thái: CONNECTED
    SMF->>DB: Update Status = CONNECTED

    Note over SMF, AMF: Bước 17: Phản hồi UpdateSMContext
    SMF-->>AMF: HTTP/2 200 OK (upCnxState = ACTIVATED, cause)
    Note over AMF: PDU Session hoàn tất thành công!
```

---

## 4. Giải Pháp Hiệu Năng Cao (High TPS)

Để hệ thống SMF xử lý hàng nghìn yêu cầu thiết lập PDU Session mỗi giây (TPS > 1000) một cách bất đồng bộ và không bị blocking:
1. **Worker Pool & Buffered Channel**:
   - HTTP/2 server tiếp nhận yêu cầu từ AMF chỉ thực hiện validate nhanh và đẩy công việc vào hàng đợi (Buffered Channel).
   - Các Worker chạy nền (background workers) lấy công việc từ hàng đợi để xử lý tuần tự/song song các bước tiếp theo (giao tiếp UDM, UPF, DB).
   - Điều này giúp SMF giải phóng kết nối HTTP/2 nhanh chóng, tăng khả năng chịu tải.
2. **PostgreSQL Connection Pool**:
   - Sử dụng thư viện `pgx/v5` hỗ trợ connection pooling tốt để tránh nghẽn luồng kết nối DB.
3. **UDP Response Multiplexing**:
   - Giao tiếp UDP giữa SMF và UPF sử dụng Sequence Number trong Header.
   - SMF lắng nghe UDP tập trung trên 1 port duy nhất, sử dụng cơ chế gom luồng (multiplexing) để phân phối phản hồi về cho các worker đang chờ thông qua map/channel an toàn (thread-safe).
