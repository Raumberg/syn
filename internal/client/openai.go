package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"syn/internal/models"
)

const (
	DefaultTimeout = 30 * time.Second
)

// LLMClient представляет клиент для взаимодействия с LLM API
type LLMClient struct {
	BaseURL    string
	APIKey     string
	HTTPClient *http.Client
}

// NewLLMClient создаёт нового клиента LLM API
func NewLLMClient(baseURL, apiKey string) *LLMClient {
	return &LLMClient{
		BaseURL:    baseURL,
		APIKey:     apiKey,
		HTTPClient: &http.Client{Timeout: DefaultTimeout},
	}
}

// GenerateCompletion генерирует ответ от LLM API
func (c *LLMClient) GenerateCompletion(model, systemPrompt, userPrompt string, temperature float64) (string, error) {
	reqURL := fmt.Sprintf("%s/chat/completions", c.BaseURL)

	request := models.CompletionRequest{
		Model: model,
		Messages: []models.Message{
			{
				Role:    "system",
				Content: systemPrompt,
			},
			{
				Role:    "user",
				Content: userPrompt,
			},
		},
		Temperature: temperature,
	}

	reqBody, err := json.Marshal(request)
	if err != nil {
		return "", fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequest("POST", reqURL, bytes.NewBuffer(reqBody))
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.APIKey))

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("API returned non-200 status: %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	var response models.CompletionResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return "", fmt.Errorf("failed to decode response: %w", err)
	}

	if len(response.Choices) == 0 {
		return "", fmt.Errorf("no choices in response")
	}

	return response.Choices[0].Message.Content, nil
}

// SetTimeout устанавливает таймаут для HTTP-клиента
func (c *LLMClient) SetTimeout(timeout time.Duration) {
	c.HTTPClient.Timeout = timeout
}
