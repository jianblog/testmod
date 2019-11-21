package goquery

import (
	"encoding/json"
	"net"
	"strings"
	"time"
)

type SourceRecord struct {
	Localtime   time.Time `json:"localtime"`
	ClientIP    net.IP `json:"clientip"`
	Url         string `json:"url"`
	RequestBody string `json:"request_body"`
	SessionId   string `json:"session_id"`
	Agent       string `json:"agent"`
}

// 针对于自定义类型中的时间格式进行解析
func (s *SourceRecord) UnmarshalJSON(j []byte) error {
	var rawStrings map[string]string

	err := json.Unmarshal(j, &rawStrings)
	if err != nil {
		return err
	}
	for k, v := range rawStrings {
		if strings.ToLower(k) == "localtime" {
			t, err := time.Parse(time.RFC3339, v)
			if err != nil {
				return err
			}
			s.Localtime = t
		}
		if strings.ToLower(k) == "clientip" {
			// 非法值则为nil
			s.ClientIP = net.ParseIP(v)
		}
		if strings.ToLower(k) == "url" {
			s.Url =v
		}
		if strings.ToLower(k) == "request_body" {
			s.RequestBody = v
		}
		if strings.ToLower(k) == "session_id" {
			s.SessionId = v
		}
		if strings.ToLower(k) == "agent" {
			s.Agent = v
		}
	}
	return nil
}
