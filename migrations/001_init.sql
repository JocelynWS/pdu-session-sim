-- Table for Subscriber Profile (used by UDM)
CREATE TABLE IF NOT EXISTS subscribers (
    imsi VARCHAR(50) PRIMARY KEY,
    dnn VARCHAR(50) NOT NULL,
    sst INT NOT NULL,
    sd VARCHAR(10) NOT NULL
);

-- Table for PDU Sessions (used by SMF)
CREATE TABLE IF NOT EXISTS pdu_sessions (
    sm_context_ref VARCHAR(100) PRIMARY KEY,
    supi VARCHAR(50) NOT NULL,
    gpsi VARCHAR(50) NOT NULL,
    pdu_session_id INT NOT NULL,
    dnn VARCHAR(50) NOT NULL,
    sst INT NOT NULL,
    sd VARCHAR(10) NOT NULL,
    serving_nf_id VARCHAR(100) NOT NULL,
    an_type VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL,
    ip_address VARCHAR(50) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Seed UDM with test subscribers
INSERT INTO subscribers (imsi, dnn, sst, sd)
VALUES 
('imsi-452040000000001', 'v-internet', 1, '000001'),
('imsi-452040000000002', 'v-internet', 1, '000002'),
('imsi-452040000000003', 'v-internet', 2, '000003')
ON CONFLICT (imsi) DO NOTHING;
