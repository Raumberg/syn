package processor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// HuggingFaceProcessor процессор для работы с Hugging Face Datasets
type HuggingFaceProcessor struct {
	id             string
	pythonPath     string
	tempDir        string
	datasetName    string
	split          string
	streamable     bool
	limit          int
	offset         int
	filters        map[string]interface{}
	numericFilters map[string]NumericFilter
	fields         []string
	extraArgs      map[string]string
	debug          bool
}

// NumericFilter представляет фильтр для числовых значений
type NumericFilter struct {
	Min      float64
	Max      float64
	Operator string
}

// NewHuggingFaceProcessor создаёт новый процессор для Hugging Face
func NewHuggingFaceProcessor(
	id string,
	pythonPath string,
	tempDir string,
	datasetName string,
	split string,
	streamable bool,
	filters map[string]interface{},
	fields []string,
) *HuggingFaceProcessor {
	if pythonPath == "" {
		pythonPath = "python3" // По умолчанию используем python3
	}

	if split == "" {
		split = "train" // По умолчанию берем train split
	}

	return &HuggingFaceProcessor{
		id:             id,
		pythonPath:     pythonPath,
		tempDir:        tempDir,
		datasetName:    datasetName,
		split:          split,
		streamable:     streamable,
		limit:          0, // 0 означает без ограничений
		offset:         0,
		filters:        filters,
		numericFilters: make(map[string]NumericFilter),
		fields:         fields,
		extraArgs:      make(map[string]string),
		debug:          false,
	}
}

// ID возвращает идентификатор процессора
func (p *HuggingFaceProcessor) ID() string {
	return p.id
}

// SetLimit устанавливает лимит для количества элементов
func (p *HuggingFaceProcessor) SetLimit(limit int) *HuggingFaceProcessor {
	p.limit = limit
	return p
}

// SetOffset устанавливает смещение для выборки
func (p *HuggingFaceProcessor) SetOffset(offset int) *HuggingFaceProcessor {
	p.offset = offset
	return p
}

// AddNumericFilter добавляет числовой фильтр
func (p *HuggingFaceProcessor) AddNumericFilter(field string, min, max float64, operator string) *HuggingFaceProcessor {
	p.numericFilters[field] = NumericFilter{
		Min:      min,
		Max:      max,
		Operator: operator,
	}
	return p
}

// AddExtraArg добавляет дополнительный аргумент для Python-скрипта
func (p *HuggingFaceProcessor) AddExtraArg(key, value string) *HuggingFaceProcessor {
	p.extraArgs[key] = value
	return p
}

// Process обрабатывает данные
func (p *HuggingFaceProcessor) Process(ctx context.Context, data interface{}) (interface{}, error) {
	return p.loadDatasetFromHF(ctx)
}

// loadDatasetFromHF загружает данные из Hugging Face используя Python
func (p *HuggingFaceProcessor) loadDatasetFromHF(ctx context.Context) (interface{}, error) {
	// Создаем временный Python-скрипт
	scriptPath, err := p.createPythonScript()
	if err != nil {
		return nil, fmt.Errorf("не удалось создать Python-скрипт: %w", err)
	}
	defer os.Remove(scriptPath)

	if p.debug {
		fmt.Printf("🐍 Создан временный скрипт Python: %s\n", scriptPath)
	}

	// Создаем временный файл для результатов
	outputPath := filepath.Join(p.tempDir, fmt.Sprintf("hf_dataset_%s.json", p.id))
	defer os.Remove(outputPath)

	if p.debug {
		fmt.Printf("📄 Временный выходной файл: %s\n", outputPath)
	}

	// Запускаем Python-скрипт
	cmd := exec.CommandContext(ctx, p.pythonPath, scriptPath, "--output", outputPath,
		"--dataset", p.datasetName, "--split", p.split)

	if p.streamable {
		cmd.Args = append(cmd.Args, "--streamable")
	}

	// Устанавливаем лимит и смещение, если они указаны
	if p.limit > 0 {
		cmd.Args = append(cmd.Args, "--limit", fmt.Sprintf("%d", p.limit))
	}

	if p.offset > 0 {
		cmd.Args = append(cmd.Args, "--offset", fmt.Sprintf("%d", p.offset))
	}

	// Добавляем фильтры в виде JSON
	if len(p.filters) > 0 {
		filtersJSON, err := json.Marshal(p.filters)
		if err != nil {
			return nil, fmt.Errorf("ошибка маршаллинга фильтров: %w", err)
		}
		cmd.Args = append(cmd.Args, "--filters", string(filtersJSON))

		if p.debug {
			fmt.Printf("🔍 Фильтры: %s\n", string(filtersJSON))
		}
	}

	// Добавляем числовые фильтры
	if len(p.numericFilters) > 0 {
		numFiltersJSON, err := json.Marshal(p.numericFilters)
		if err != nil {
			return nil, fmt.Errorf("ошибка маршаллинга числовых фильтров: %w", err)
		}
		cmd.Args = append(cmd.Args, "--numeric-filters", string(numFiltersJSON))

		if p.debug {
			fmt.Printf("🔢 Числовые фильтры: %s\n", string(numFiltersJSON))
		}
	}

	// Добавляем поля
	if len(p.fields) > 0 {
		cmd.Args = append(cmd.Args, "--fields", strings.Join(p.fields, ","))

		if p.debug {
			fmt.Printf("🏷️ Выбранные поля: %s\n", strings.Join(p.fields, ","))
		}
	}

	// Добавляем дополнительные аргументы
	for k, v := range p.extraArgs {
		cmd.Args = append(cmd.Args, fmt.Sprintf("--%s", k), v)
	}

	// Добавляем verbose режим, если включена отладка
	if p.debug {
		cmd.Args = append(cmd.Args, "--verbose")
	}

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if p.debug {
		fmt.Printf("🚀 Запуск команды: %s\n", strings.Join(cmd.Args, " "))
	}

	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("ошибка запуска Python-скрипта: %w\nSTDOUT: %s\nSTDERR: %s", err, stdout.String(), stderr.String())
	}

	// Выводим информацию из stdout только в режиме отладки
	stdoutStr := stdout.String()
	if stdoutStr != "" && p.debug {
		fmt.Printf("📝 Python stdout: %s\n", stdoutStr)
	}

	// Читаем результаты
	if _, err := os.Stat(outputPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("выходной файл не был создан: %s\nSTDOUT: %s\nSTDERR: %s", outputPath, stdout.String(), stderr.String())
	}

	data, err := os.ReadFile(outputPath)
	if err != nil {
		return nil, fmt.Errorf("ошибка чтения результатов: %w", err)
	}

	if p.debug {
		fmt.Printf("📊 Размер полученных данных: %s\n", humanizeBytes(int64(len(data))))
	}

	if len(data) == 0 {
		return nil, fmt.Errorf("выходной файл пуст: %s\nSTDOUT: %s\nSTDERR: %s", outputPath, stdout.String(), stderr.String())
	}

	// Парсим JSON
	var result []map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, fmt.Errorf("ошибка парсинга JSON: %w\nData: %s", err, string(data))
	}

	if p.debug {
		fmt.Printf("✅ Получено %d записей из датасета\n", len(result))
		if len(result) > 0 {
			fmt.Printf("🔍 Пример первой записи:\n")
			printMapPreview(result[0], 3)
		}
	}

	return result, nil
}

// humanizeBytes преобразует байты в человекочитаемый формат
func humanizeBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// printMapPreview выводит часть карты для отладки
func printMapPreview(m map[string]interface{}, maxItems int) {
	fmt.Println("  {")
	i := 0
	for k, v := range m {
		if i >= maxItems {
			fmt.Println("    ...")
			break
		}

		var valueStr string
		switch val := v.(type) {
		case string:
			if len(val) > 50 {
				valueStr = fmt.Sprintf("%q...", val[:50])
			} else {
				valueStr = fmt.Sprintf("%q", val)
			}
		case []interface{}:
			if len(val) > 3 {
				valueStr = fmt.Sprintf("[%v, %v, %v, ...]", val[0], val[1], val[2])
			} else {
				valueStr = fmt.Sprintf("%v", val)
			}
		default:
			valueStr = fmt.Sprintf("%v", val)
		}

		fmt.Printf("    %q: %s,\n", k, valueStr)
		i++
	}
	fmt.Println("  }")
}

// createPythonScript создает временный Python-скрипт для загрузки датасета
func (p *HuggingFaceProcessor) createPythonScript() (string, error) {
	script := `
import json
import argparse
from datasets import load_dataset
import numpy as np
import sys

def apply_numeric_filter(dataset, field, filter_config):
    """Применяет числовой фильтр к датасету"""
    min_value = filter_config.get('Min', -float('inf'))
    max_value = filter_config.get('Max', float('inf'))
    operator = filter_config.get('Operator', '')
    
    if operator == 'gte':
        return dataset.filter(lambda x: x[field] >= min_value)
    elif operator == 'lte':
        return dataset.filter(lambda x: x[field] <= max_value)
    elif operator == 'gt':
        return dataset.filter(lambda x: x[field] > min_value)
    elif operator == 'lt':
        return dataset.filter(lambda x: x[field] < max_value)
    elif operator == 'eq':
        if min_value != -float('inf'):
            return dataset.filter(lambda x: x[field] == min_value)
        return dataset
    elif min_value > -float('inf') and max_value < float('inf'):
        return dataset.filter(lambda x: min_value <= x[field] <= max_value)
    elif min_value > -float('inf'):
        return dataset.filter(lambda x: x[field] >= min_value)
    elif max_value < float('inf'):
        return dataset.filter(lambda x: x[field] <= max_value)
    else:
        return dataset

def main():
    parser = argparse.ArgumentParser(description='Load a dataset from Hugging Face')
    parser.add_argument('--output', required=True, help='Output file path')
    parser.add_argument('--dataset', required=True, help='Dataset name')
    parser.add_argument('--split', default='train', help='Dataset split')
    parser.add_argument('--streamable', action='store_true', help='Load dataset in streaming mode')
    parser.add_argument('--filters', default='{}', help='Filters as JSON')
    parser.add_argument('--numeric-filters', default='{}', help='Numeric filters as JSON')
    parser.add_argument('--fields', help='Comma-separated list of fields to include')
    parser.add_argument('--limit', type=int, default=0, help='Maximum number of examples')
    parser.add_argument('--offset', type=int, default=0, help='Offset to start from')
    parser.add_argument('--shuffle', action='store_true', help='Shuffle the dataset')
    parser.add_argument('--seed', type=int, default=42, help='Random seed for shuffling')
    parser.add_argument('--verbose', action='store_true', help='Verbose output')
    
    args, unknown = parser.parse_known_args()
    
    try:
        # Парсим фильтры из JSON
        filters = json.loads(args.filters)
        numeric_filters = json.loads(args.numeric_filters)
        
        if args.verbose:
            print(f"Loading dataset: {args.dataset} (split: {args.split})")
        
        # Загружаем датасет
        dataset = load_dataset(args.dataset, split=args.split, streaming=args.streamable)
        
        # Выбираем только нужные поля, если указаны
        if args.fields:
            fields = args.fields.split(',')
            dataset = dataset.select_columns(fields)
        
        # Применяем фильтры
        if filters:
            def filter_func(x):
                for k, v in filters.items():
                    if k not in x or x[k] != v:
                        return False
                return True
            
            dataset = dataset.filter(filter_func)
        
        # Применяем числовые фильтры
        for field, filter_config in numeric_filters.items():
            if field not in dataset.column_names:
                if args.verbose:
                    print(f"Warning: Field {field} not found in dataset")
                continue
            
            dataset = apply_numeric_filter(dataset, field, filter_config)
        
        # Перемешиваем если нужно
        if args.shuffle and not args.streamable:
            dataset = dataset.shuffle(seed=args.seed)
        
        # Применяем смещение и лимит
        if args.offset > 0 and not args.streamable:
            dataset = dataset.skip(args.offset)
        
        if args.limit > 0:
            dataset = dataset.take(args.limit)
        
        # Преобразуем в список словарей и сохраняем
        result = []
        for item in dataset:
            # Конвертируем numpy типы в стандартные Python типы
            item_dict = {}
            for k, v in item.items():
                if isinstance(v, np.integer):
                    item_dict[k] = int(v)
                elif isinstance(v, np.floating):
                    item_dict[k] = float(v)
                elif isinstance(v, np.ndarray):
                    item_dict[k] = v.tolist()
                else:
                    item_dict[k] = v
            result.append(item_dict)
        
        if args.verbose:
            print(f"Processed {len(result)} examples")
        
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        
        if args.verbose:
            print(f"Saved {len(result)} examples to {args.output}")
    
    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    main()
`

	// Создаем временный файл
	tmpFile, err := os.CreateTemp(p.tempDir, "hf_script_*.py")
	if err != nil {
		return "", err
	}
	defer tmpFile.Close()

	if _, err := tmpFile.WriteString(script); err != nil {
		return "", err
	}

	return tmpFile.Name(), nil
}

// LoadFromHuggingFace загружает датасет из Hugging Face
func LoadFromHuggingFace(
	ctx context.Context,
	pythonPath string,
	datasetName string,
	split string,
	streamable bool,
	filters map[string]interface{},
	fields []string,
) ([]map[string]interface{}, error) {
	processor := NewHuggingFaceProcessor(
		"hf_loader",
		pythonPath,
		os.TempDir(),
		datasetName,
		split,
		streamable,
		filters,
		fields,
	)

	result, err := processor.Process(ctx, nil)
	if err != nil {
		return nil, err
	}

	return result.([]map[string]interface{}), nil
}

// SetDebug устанавливает режим отладки
func (p *HuggingFaceProcessor) SetDebug(debug bool) {
	p.debug = debug
}
