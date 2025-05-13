package models

import (
	"encoding/json"
)

// GenericDataset представляет общий датасет
type GenericDataset struct {
	Items []map[string]interface{} `json:"items"`
}

// NewGenericDataset создаёт новый пустой общий датасет
func NewGenericDataset() *GenericDataset {
	return &GenericDataset{
		Items: make([]map[string]interface{}, 0),
	}
}

// AddItem добавляет элемент в датасет
func (d *GenericDataset) AddItem(item map[string]interface{}) {
	d.Items = append(d.Items, item)
}

// FromJSON загружает датасет из JSON-строки
func (d *GenericDataset) FromJSON(data []byte) error {
	return json.Unmarshal(data, &d.Items)
}

// ToJSON сериализует датасет в JSON
func (d *GenericDataset) ToJSON() ([]byte, error) {
	return json.Marshal(d.Items)
}

// Len возвращает количество элементов в датасете
func (d *GenericDataset) Len() int {
	return len(d.Items)
}

// GetData возвращает все элементы датасета
func (d *GenericDataset) GetData() []map[string]interface{} {
	return d.Items
}

// SetData устанавливает элементы датасета
func (d *GenericDataset) SetData(items []map[string]interface{}) {
	d.Items = items
}

// CompletionRequest представляет запрос к LLM API
type CompletionRequest struct {
	Model       string    `json:"model"`
	Messages    []Message `json:"messages"`
	Temperature float64   `json:"temperature"`
}

// Message представляет сообщение в запросе к LLM API
type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// CompletionResponse представляет ответ от LLM API
type CompletionResponse struct {
	Choices []Choice `json:"choices"`
}

// Choice представляет выбор в ответе от LLM API
type Choice struct {
	Message Message `json:"message"`
}
