package models

import (
	"encoding/json"
	"os"
)

// Config представляет конфигурацию приложения
type Config struct {
	// Настройки API
	API struct {
		BaseURL string `json:"base_url" yaml:"base_url"`
		APIKey  string `json:"api_key" yaml:"api_key"`
		Model   string `json:"model" yaml:"model"`
	} `json:"api" yaml:"api"`

	// Настройки входных и выходных данных
	IO struct {
		InputFile  string `json:"input_file" yaml:"input_file"`
		OutputDir  string `json:"output_dir" yaml:"output_dir"`
		OutputFile string `json:"output_file" yaml:"output_file"`
	} `json:"io" yaml:"io"`

	// Настройки HuggingFace
	HuggingFace struct {
		Enabled     bool                   `json:"enabled" yaml:"enabled"`
		PythonPath  string                 `json:"python_path" yaml:"python_path"`
		DatasetName string                 `json:"dataset_name" yaml:"dataset_name"`
		Split       string                 `json:"split" yaml:"split"`
		Streamable  bool                   `json:"streamable" yaml:"streamable"`
		Shuffle     bool                   `json:"shuffle" yaml:"shuffle"`
		Seed        int                    `json:"seed" yaml:"seed"`
		Limit       int                    `json:"limit" yaml:"limit"`
		Offset      int                    `json:"offset" yaml:"offset"`
		Filters     map[string]interface{} `json:"filters" yaml:"filters"`
		Fields      []string               `json:"fields" yaml:"fields"`
		ExtraArgs   map[string]string      `json:"extra_args" yaml:"extra_args"`
	} `json:"huggingface" yaml:"huggingface"`

	// Настройки обработки
	Processing struct {
		MaxConcurrency int     `json:"max_concurrency" yaml:"max_concurrency"`
		Temperature    float64 `json:"temperature" yaml:"temperature"`
		NoLLM          bool    `json:"no_llm" yaml:"no_llm"`
		// Настройки фильтрации
		Filter struct {
			Enabled    bool              `json:"enabled" yaml:"enabled"`
			Field      string            `json:"field" yaml:"field"`
			Conditions map[string]string `json:"conditions" yaml:"conditions"`
			MinValue   float64           `json:"min_value" yaml:"min_value"`
			MaxValue   float64           `json:"max_value" yaml:"max_value"`
			Operator   string            `json:"operator" yaml:"operator"`
		} `json:"filter" yaml:"filter"`
	} `json:"processing" yaml:"processing"`

	// Настройки полей для обработки
	Fields struct {
		Input  []FieldMapping `json:"input" yaml:"input"`
		Output []FieldMapping `json:"output" yaml:"output"`
	} `json:"fields" yaml:"fields"`

	// Настройки промпта
	Prompt struct {
		System string `json:"system" yaml:"system"`
		User   string `json:"user" yaml:"user"`
	} `json:"prompt" yaml:"prompt"`

	// Режим отладки
	Debug bool `json:"debug" yaml:"debug"`
}

// FieldMapping представляет маппинг поля из входного датасета в выходной
type FieldMapping struct {
	InputField  string `json:"input_field" yaml:"input_field"`
	OutputField string `json:"output_field" yaml:"output_field"`
	ProcessorID string `json:"processor_id" yaml:"processor_id"` // ID процессора для обработки (llm, regex, etc.)
}

// LoadConfigFromFile загружает конфигурацию из файла
func LoadConfigFromFile(path string) (*Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	config := &Config{}
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(config); err != nil {
		return nil, err
	}

	return config, nil
}

// DefaultConfig возвращает конфигурацию по умолчанию
func DefaultConfig() *Config {
	cfg := &Config{}

	// API настройки по умолчанию
	cfg.API.BaseURL = "http://0.0.0.0:8000/v1"
	cfg.API.APIKey = "token-abc123"
	cfg.API.Model = "t-tech/T-pro-it-1.0"

	// Настройки IO по умолчанию
	cfg.IO.OutputDir = "output"
	cfg.IO.OutputFile = "dataset.json"

	// Настройки HuggingFace по умолчанию
	cfg.HuggingFace.Enabled = false
	cfg.HuggingFace.PythonPath = "python3"
	cfg.HuggingFace.Split = "train"
	cfg.HuggingFace.Seed = 42
	cfg.HuggingFace.Limit = 0
	cfg.HuggingFace.Offset = 0
	cfg.HuggingFace.Shuffle = false
	cfg.HuggingFace.ExtraArgs = make(map[string]string)

	// Настройки обработки по умолчанию
	cfg.Processing.MaxConcurrency = 4
	cfg.Processing.Temperature = 0.6

	// Настройки фильтрации по умолчанию
	cfg.Processing.Filter.Enabled = false

	// Системный промпт по умолчанию
	cfg.Prompt.System = "Ты хороший помощник."

	return cfg
}

// SaveToFile сохраняет конфигурацию в файл
func (c *Config) SaveToFile(path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(c); err != nil {
		return err
	}

	return nil
}
