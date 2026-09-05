package broker

type Request struct {
	ID        string `json:"id"`
	Owner     Owner  `json:"owner"`
	Operation string `json:"operation"`
}
type Response struct {
	ID    string `json:"id"`
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}
