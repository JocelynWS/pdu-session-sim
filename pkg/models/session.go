package models

import "time"

type SNssai struct {
	Sst int    `json:"sst"`
	Sd  string `json:"sd"`
}

type CreateSMContextRequest struct {
	Supi         string `json:"supi"`
	Gpsi         string `json:"gpsi"`
	PduSessionId int    `json:"pduSessionId"`
	Dnn          string `json:"dnn"`
	SNssai       SNssai `json:"sNssai"`
	ServingNfId  string `json:"servingNfId"`
	AnType       string `json:"anType"`
}

type CreateSMContextResponse struct {
	SmContextRef string `json:"smContextRef"`
	Cause        string `json:"cause"`
}

type UpdateSMContextRequest struct {
	AnType             string `json:"anType"`
	AnTypeToReactivate string `json:"anTypeToReactivate"`
	UpCnxState         string `json:"upCnxState"`
}

type UpdateSMContextResponse struct {
	UpCnxState string `json:"upCnxState"`
	Cause      string `json:"cause"`
}

type N1N2MessageTransfer struct {
	PduSessionId int    `json:"pduSessionId"`
	SNssai       SNssai `json:"sNssai"`
	Dnn          string `json:"dnn"`
}

type SubscriptionData struct {
	Imsi   string `json:"imsi"`
	Dnn    string `json:"dnn"`
	SNssai SNssai `json:"sNssai"`
}

type PDUSession struct {
	SMContextRef string    `json:"smContextRef"`
	SUPI         string    `json:"supi"`
	GPSI         string    `json:"gpsi"`
	PduSessionID int       `json:"pduSessionId"`
	DNN          string    `json:"dnn"`
	SST          int       `json:"sst"`
	SD           string    `json:"sd"`
	ServingNfID  string    `json:"servingNfId"`
	AnType       string    `json:"anType"`
	Status       string    `json:"status"`
	IPAddress    string    `json:"ipAddress"`
	CreatedAt    time.Time `json:"createdAt"`
	UpdatedAt    time.Time `json:"updatedAt"`
}
