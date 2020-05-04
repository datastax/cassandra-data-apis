package models

type RowsResponse struct {
	Success bool `json:"success,omitempty"`

	RowsModified int32 `json:"rowsModified,omitempty"`
}
