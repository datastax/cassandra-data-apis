package models

type Rows struct {
	Rows      []map[string]interface{} `json:"rows,omitempty"`
	PageState string                   `json:"pageState,omitempty"`
	Count     int                      `json:"_count,omitempty"`
}
