package processor

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"syn/internal/client"
)

// Processor интерфейс для процессоров, которые обрабатывают данные
type Processor interface {
	// Process обрабатывает значение и возвращает результат
	Process(ctx context.Context, value interface{}) (interface{}, error)
	// ID возвращает идентификатор процессора
	ID() string
}

// Debuggable интерфейс для процессоров, поддерживающих режим отладки
type Debuggable interface {
	SetDebug(debug bool)
}

// Manager управляет процессорами
type Manager struct {
	processors map[string]Processor
	mu         sync.RWMutex
}

// NewManager создаёт новый менеджер процессоров
func NewManager() *Manager {
	return &Manager{
		processors: make(map[string]Processor),
	}
}

// Register регистрирует процессор
func (m *Manager) Register(processor Processor) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.processors[processor.ID()] = processor
}

// Get возвращает процессор по ID
func (m *Manager) Get(id string) (Processor, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	processor, ok := m.processors[id]
	if !ok {
		return nil, fmt.Errorf("processor with ID %s not found", id)
	}
	return processor, nil
}

// LLMProcessor процессор, использующий LLM API
type LLMProcessor struct {
	id           string
	client       *client.LLMClient
	model        string
	systemPrompt string
	userTemplate string
	temperature  float64
}

// NewLLMProcessor создаёт новый процессор LLM
func NewLLMProcessor(id string, client *client.LLMClient, model, systemPrompt, userTemplate string, temperature float64) *LLMProcessor {
	return &LLMProcessor{
		id:           id,
		client:       client,
		model:        model,
		systemPrompt: systemPrompt,
		userTemplate: userTemplate,
		temperature:  temperature,
	}
}

// ID возвращает идентификатор процессора
func (p *LLMProcessor) ID() string {
	return p.id
}

// Process обрабатывает значение с помощью LLM API
func (p *LLMProcessor) Process(ctx context.Context, value interface{}) (interface{}, error) {
	str, ok := value.(string)
	if !ok {
		return nil, errors.New("value must be a string")
	}

	// Формируем пользовательский промпт
	userPrompt := p.userTemplate
	if userPrompt == "" {
		userPrompt = str
	} else {
		// TODO: Реализовать шаблонизацию для userPrompt
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return p.client.GenerateCompletion(p.model, p.systemPrompt, userPrompt, p.temperature)
	}
}

// IdentityProcessor просто возвращает значение без изменений
type IdentityProcessor struct{}

// NewIdentityProcessor создаёт новый процессор без изменений
func NewIdentityProcessor() *IdentityProcessor {
	return &IdentityProcessor{}
}

// ID возвращает идентификатор процессора
func (p *IdentityProcessor) ID() string {
	return "identity"
}

// Process возвращает значение без изменений
func (p *IdentityProcessor) Process(ctx context.Context, value interface{}) (interface{}, error) {
	return value, nil
}

// FilterProcessor реализует фильтрацию записей
type FilterProcessor struct {
	field       string
	minValue    float64
	maxValue    float64
	stringValue string
	comparator  string
}

// NewFilterProcessor создаёт новый процессор фильтрации
func NewFilterProcessor(field string, minValue, maxValue float64, stringValue, comparator string) *FilterProcessor {
	return &FilterProcessor{
		field:       field,
		minValue:    minValue,
		maxValue:    maxValue,
		stringValue: stringValue,
		comparator:  comparator,
	}
}

// ID возвращает идентификатор процессора
func (p *FilterProcessor) ID() string {
	return "filter"
}

// ShouldInclude проверяет, должна ли запись быть включена в результат
func (p *FilterProcessor) ShouldInclude(item map[string]interface{}) (bool, error) {
	fieldValue, ok := item[p.field]
	if !ok {
		return false, fmt.Errorf("field %s not found in item", p.field)
	}

	// Проверяем числовые значения
	if p.minValue != 0 || p.maxValue != 0 {
		// Пытаемся преобразовать к числу
		var numVal float64
		switch v := fieldValue.(type) {
		case int:
			numVal = float64(v)
		case int64:
			numVal = float64(v)
		case float64:
			numVal = v
		case float32:
			numVal = float64(v)
		default:
			return false, fmt.Errorf("field %s is not a number", p.field)
		}

		// Проверяем минимальное значение
		if p.minValue != 0 && numVal < p.minValue {
			return false, nil
		}

		// Проверяем максимальное значение
		if p.maxValue != 0 && numVal > p.maxValue {
			return false, nil
		}

		return true, nil
	}

	// Проверяем строковые значения
	if p.stringValue != "" && p.comparator != "" {
		strValue, ok := fieldValue.(string)
		if !ok {
			return false, fmt.Errorf("field %s is not a string", p.field)
		}

		switch p.comparator {
		case "eq":
			return strValue == p.stringValue, nil
		case "ne":
			return strValue != p.stringValue, nil
		case "contains":
			// TODO: Реализовать contains
			return true, nil
		default:
			return false, fmt.Errorf("unknown comparator %s", p.comparator)
		}
	}

	// По умолчанию включаем все записи
	return true, nil
}

// Process обрабатывает значение (не используется для фильтра)
func (p *FilterProcessor) Process(ctx context.Context, value interface{}) (interface{}, error) {
	return value, nil
}
