package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/briandowns/spinner"
	"github.com/fatih/color"

	"syn/internal/client"
	"syn/internal/generator"
	"syn/internal/models"
)

func main() {
	// Настраиваем цветной вывод
	cyan := color.New(color.FgCyan).SprintFunc()
	green := color.New(color.FgGreen).SprintFunc()
	yellow := color.New(color.FgYellow).SprintFunc()
	red := color.New(color.FgRed).SprintFunc()

	// Параметры командной строки
	configFile := flag.String("config", "", "Путь к конфигурационному файлу")

	// API настройки
	baseURL := flag.String("api-url", "", "URL API (переопределяет конфиг)")
	apiKey := flag.String("api-key", "", "Ключ API (переопределяет конфиг)")
	model := flag.String("model", "", "Название модели (переопределяет конфиг)")

	// IO настройки
	inputFile := flag.String("input", "", "Входной файл с датасетом (переопределяет конфиг)")
	outputDir := flag.String("output-dir", "", "Директория для сохранения результатов (переопределяет конфиг)")
	outputFile := flag.String("output-file", "", "Имя выходного файла (переопределяет конфиг)")

	// Настройки Hugging Face
	hfEnabled := flag.Bool("hf", false, "Использовать Hugging Face Datasets (переопределяет конфиг)")
	hfDataset := flag.String("hf-dataset", "", "Имя датасета Hugging Face (переопределяет конфиг)")
	hfSplit := flag.String("hf-split", "", "Split для датасета Hugging Face (переопределяет конфиг)")
	hfPython := flag.String("hf-python", "", "Путь к Python для Hugging Face (переопределяет конфиг)")
	hfStreamable := flag.Bool("hf-stream", false, "Использовать streaming режим для Hugging Face (переопределяет конфиг)")
	hfFields := flag.String("hf-fields", "", "Список полей через запятую для выбора из Hugging Face (переопределяет конфиг)")
	hfLimit := flag.Int("hf-limit", 0, "Лимит записей для Hugging Face (переопределяет конфиг)")
	hfOffset := flag.Int("hf-offset", 0, "Смещение записей для Hugging Face (переопределяет конфиг)")
	hfShuffle := flag.Bool("hf-shuffle", false, "Перемешать записи Hugging Face (переопределяет конфиг)")
	hfSeed := flag.Int("hf-seed", 42, "Seed для перемешивания (переопределяет конфиг)")
	hfFilter := flag.String("hf-filter", "", "Фильтры для Hugging Face в формате key=value,key2=value2 (переопределяет конфиг)")

	// Настройки обработки
	maxConcurrency := flag.Int("concurrency", 0, "Максимальное количество параллельных запросов (переопределяет конфиг)")
	temperature := flag.Float64("temperature", 0.0, "Температура генерации (переопределяет конфиг)")
	noLLM := flag.Bool("no-llm", false, "Не обрабатывать данные через LLM, только загрузить датасет")
	debug := flag.Bool("debug", false, "Включить подробный вывод отладочной информации")

	// Настройки фильтрации
	filterEnabled := flag.Bool("filter", false, "Включить фильтрацию (переопределяет конфиг)")
	filterField := flag.String("filter-field", "", "Поле для фильтрации (переопределяет конфиг)")
	filterMinValue := flag.Float64("filter-min", 0.0, "Минимальное значение для фильтрации (переопределяет конфиг)")
	filterMaxValue := flag.Float64("filter-max", 0.0, "Максимальное значение для фильтрации (переопределяет конфиг)")
	filterOperator := flag.String("filter-op", "", "Оператор для фильтрации (eq, lt, gt, etc.) (переопределяет конфиг)")

	// Настройки промпта
	systemPrompt := flag.String("system-prompt", "", "Системный промпт (переопределяет конфиг)")

	// Генерация примера конфига
	generateConfig := flag.Bool("generate-config", false, "Сгенерировать пример конфига")

	// Разбор командной строки
	flag.Parse()

	// Проверяем, нужно ли сгенерировать пример конфига
	if *generateConfig {
		generateExampleConfig()
		return
	}

	// Показываем заголовок
	fmt.Println(green("  ███████╗██╗   ██╗███╗   ██╗"))
	fmt.Println(green("  ██╔════╝╚██╗ ██╔╝████╗  ██║"))
	fmt.Println(green("  ███████╗ ╚████╔╝ ██╔██╗ ██║"))
	fmt.Println(green("  ╚════██║  ╚██╔╝  ██║╚██╗██║"))
	fmt.Println(green("  ███████║   ██║   ██║ ╚████║"))
	fmt.Println(green("  ╚══════╝   ╚═╝   ╚═╝  ╚═══╝"))
	fmt.Println()

	// Инициализируем спиннер для показа статуса загрузки
	s := spinner.New(spinner.CharSets[11], 100*time.Millisecond)
	s.Suffix = " Загрузка конфигурации..."
	s.Color("cyan")
	s.Start()

	// Загружаем конфигурацию
	var config *models.Config
	var err error

	if *configFile != "" {
		config, err = models.LoadConfigFromFile(*configFile)
		if err != nil {
			s.Stop()
			fmt.Printf("%s Ошибка загрузки конфигурации: %v\n", red("✗"), err)
			os.Exit(1)
		}
	} else {
		config = models.DefaultConfig()
	}

	// Тут же остановим спиннер
	s.Stop()

	// Переопределяем параметры из командной строки, если они указаны
	if *baseURL != "" {
		config.API.BaseURL = *baseURL
	}
	if *apiKey != "" {
		config.API.APIKey = *apiKey
	}
	if *model != "" {
		config.API.Model = *model
	}
	if *inputFile != "" {
		config.IO.InputFile = *inputFile
	}
	if *outputDir != "" {
		config.IO.OutputDir = *outputDir
	}
	if *outputFile != "" {
		config.IO.OutputFile = *outputFile
	}
	if *maxConcurrency > 0 {
		config.Processing.MaxConcurrency = *maxConcurrency
	}
	if *temperature > 0.0 {
		config.Processing.Temperature = *temperature
	}
	if *systemPrompt != "" {
		config.Prompt.System = *systemPrompt
	}

	// Настройки фильтрации
	if *filterEnabled {
		config.Processing.Filter.Enabled = true
	}
	if *filterField != "" {
		config.Processing.Filter.Field = *filterField
	}
	if *filterMinValue != 0.0 {
		config.Processing.Filter.MinValue = *filterMinValue
	}
	if *filterMaxValue != 0.0 {
		config.Processing.Filter.MaxValue = *filterMaxValue
	}
	if *filterOperator != "" {
		config.Processing.Filter.Operator = *filterOperator
	}

	// Настройки Hugging Face
	if *hfEnabled {
		config.HuggingFace.Enabled = true
	}
	if *hfDataset != "" {
		config.HuggingFace.DatasetName = *hfDataset
	}
	if *hfSplit != "" {
		config.HuggingFace.Split = *hfSplit
	}
	if *hfPython != "" {
		config.HuggingFace.PythonPath = *hfPython
	}
	if *hfStreamable {
		config.HuggingFace.Streamable = true
	}
	if *hfFields != "" {
		config.HuggingFace.Fields = strings.Split(*hfFields, ",")
	}
	if *hfLimit > 0 {
		config.HuggingFace.Limit = *hfLimit
	}
	if *hfOffset > 0 {
		config.HuggingFace.Offset = *hfOffset
	}
	if *hfShuffle {
		config.HuggingFace.Shuffle = true
	}
	if *hfSeed != 42 {
		config.HuggingFace.Seed = *hfSeed
	}
	if *hfFilter != "" {
		// Парсим фильтры из формата key=value,key2=value2
		filters := make(map[string]interface{})
		filterPairs := strings.Split(*hfFilter, ",")
		for _, pair := range filterPairs {
			kv := strings.SplitN(pair, "=", 2)
			if len(kv) == 2 {
				filters[kv[0]] = kv[1]
			}
		}
		if len(filters) > 0 {
			config.HuggingFace.Filters = filters
		}
	}

	// Проверяем обязательные параметры
	if !config.HuggingFace.Enabled && config.IO.InputFile == "" {
		fmt.Printf("%s Необходимо указать входной файл с датасетом (через конфиг или флаг -input) или включить режим Hugging Face (-hf)\n", red("✗"))
		os.Exit(1)
	}

	if config.HuggingFace.Enabled && config.HuggingFace.DatasetName == "" {
		fmt.Printf("%s Необходимо указать имя датасета Hugging Face (через конфиг или флаг -hf-dataset)\n", red("✗"))
		os.Exit(1)
	}

	// Устанавливаем значения по умолчанию, если не заданы
	if config.Processing.MaxConcurrency <= 0 {
		config.Processing.MaxConcurrency = runtime.NumCPU()
	}
	if config.IO.OutputFile == "" {
		config.IO.OutputFile = "dataset.json"
	}
	if config.IO.OutputDir == "" {
		config.IO.OutputDir = "output"
	}

	// Выводим параметры
	fmt.Println(green("📝 Параметры запуска:"))
	fmt.Println()

	// API параметры
	if *noLLM {
		fmt.Printf("  %s %s\n", cyan("API URL:"), red("NULL"))
		fmt.Printf("  %s %s\n", cyan("Модель:"), red("NULL"))
		fmt.Printf("  %s %s\n", cyan("Режим без LLM:"), yellow("✅ Включен"))
	} else {
		fmt.Printf("  %s %s\n", cyan("API URL:"), config.API.BaseURL)
		fmt.Printf("  %s %s\n", cyan("Модель:"), config.API.Model)
		fmt.Printf("  %s %.2f\n", cyan("Температура:"), config.Processing.Temperature)
	}

	// Источник данных
	if config.HuggingFace.Enabled {
		fmt.Println()
		fmt.Printf("  %s %s\n", cyan("Источник:"), green("Hugging Face"))
		fmt.Printf("  %s %s\n", cyan("Датасет:"), config.HuggingFace.DatasetName)
		fmt.Printf("  %s %s\n", cyan("Сплит:"), config.HuggingFace.Split)

		if config.HuggingFace.Limit > 0 {
			fmt.Printf("  %s %d\n", cyan("Лимит записей:"), config.HuggingFace.Limit)
		}
		if config.HuggingFace.Offset > 0 {
			fmt.Printf("  %s %d\n", cyan("Смещение:"), config.HuggingFace.Offset)
		}
		if config.HuggingFace.Shuffle {
			fmt.Printf("  %s %s (seed: %d)\n", cyan("Перемешивание:"), green("Включено"), config.HuggingFace.Seed)
		}
		if len(config.HuggingFace.Fields) > 0 {
			fmt.Printf("  %s %s\n", cyan("Поля:"), strings.Join(config.HuggingFace.Fields, ", "))
		}
		if config.Processing.Filter.Enabled {
			fmt.Printf("  %s %s %s %.1f\n",
				cyan("Фильтр:"),
				config.Processing.Filter.Field,
				config.Processing.Filter.Operator,
				config.Processing.Filter.MinValue)
		}
	} else {
		fmt.Println()
		fmt.Printf("  %s %s\n", cyan("Источник:"), yellow("Локальный файл"))
		fmt.Printf("  %s %s\n", cyan("Входной файл:"), config.IO.InputFile)
	}

	// Выходные данные
	fmt.Println()
	fmt.Printf("  %s %s\n", cyan("Выходная директория:"), config.IO.OutputDir)
	fmt.Printf("  %s %s\n", cyan("Выходной файл:"), config.IO.OutputFile)

	// Параметры обработки
	fmt.Println()
	fmt.Printf("  %s %d\n", cyan("Параллельные запросы:"), config.Processing.MaxConcurrency)

	if !*noLLM {
		fmt.Printf("  %s %s\n", cyan("Системный промпт:"), truncateString(config.Prompt.System, 50))
	} else {
		config.Processing.NoLLM = true
	}

	if *debug {
		config.Debug = true
		fmt.Printf("  %s %s\n", cyan("Режим отладки:"), yellow("✅ Включен"))
	}

	fmt.Println()

	// Создаём клиент API
	llmClient := client.NewLLMClient(config.API.BaseURL, config.API.APIKey)

	// Создаём генератор датасета
	datasetGenerator := generator.NewDatasetGenerator(config, llmClient)

	// Запускаем генерацию
	startTime := time.Now()
	if err := datasetGenerator.Run(context.Background()); err != nil {
		fmt.Printf("\n%s %v\n", red("❌ Ошибка:"), err)
		os.Exit(1)
	}

	elapsed := time.Since(startTime)
	fmt.Printf("\n%s %s\n", green("⏱️ Время выполнения:"), formatDuration(elapsed))
}

// formatDuration преобразует длительность в читаемый формат
func formatDuration(d time.Duration) string {
	// Округляем до секунд
	d = d.Round(time.Second)

	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	s := d / time.Second

	if h > 0 {
		return fmt.Sprintf("%dч %dм %dс", h, m, s)
	}
	if m > 0 {
		return fmt.Sprintf("%dм %dс", m, s)
	}
	return fmt.Sprintf("%dс", s)
}

// generateExampleConfig генерирует и сохраняет пример конфигурационного файла
func generateExampleConfig() {
	config := models.DefaultConfig()

	// Настраиваем более полный пример
	config.API.BaseURL = "http://0.0.0.0:8000/v1"
	config.API.APIKey = "token-abc123"
	config.API.Model = "t-tech/T-pro-it-1.0"

	config.IO.InputFile = "input.json"
	config.IO.OutputDir = "output"
	config.IO.OutputFile = "dataset.json"

	config.Processing.MaxConcurrency = runtime.NumCPU()
	config.Processing.Temperature = 0.6

	// Настраиваем фильтрацию
	config.Processing.Filter.Enabled = true
	config.Processing.Filter.Field = "difficulty"
	config.Processing.Filter.MinValue = 8
	config.Processing.Filter.Operator = "gte"

	// Настраиваем Hugging Face
	config.HuggingFace.Enabled = true
	config.HuggingFace.PythonPath = "python3"
	config.HuggingFace.DatasetName = "zwhe99/DeepMath-103K"
	config.HuggingFace.Split = "train"
	config.HuggingFace.Fields = []string{"question", "final_answer", "r1_solution_3", "difficulty"}
	config.HuggingFace.Shuffle = true
	config.HuggingFace.Seed = 42
	config.HuggingFace.Limit = 1000
	config.HuggingFace.Offset = 0

	// Добавляем фильтры HF
	config.HuggingFace.Filters = map[string]interface{}{
		"language": "English",
	}

	// Экстра аргументы
	config.HuggingFace.ExtraArgs = map[string]string{
		"local_dir": "./cache",
	}

	// Настраиваем системный промпт
	config.Prompt.System = `
Тебе будут даны выражения, задачи или размышления по математике.
Твоя задача перевести их на русский язык.
Ты ДОЛЖЕН сохранять все профессиональные обозначения, аббревиатуры или символы (включая математические)
так, как они написаны в оригинале (по-английски). Твой перевод должен быть консистентным и логичным.
Не добавляй ничего лишнего, ничего не решай и не объясняй, только переводи. 
Если тебе дают просто число или LaTeX выражение - возвращай его как есть.
`

	// Добавляем пример маппинга полей
	config.Fields.Input = []models.FieldMapping{
		{
			InputField:  "question",
			OutputField: "problem",
			ProcessorID: "llm",
		},
		{
			InputField:  "final_answer",
			OutputField: "answer",
			ProcessorID: "llm",
		},
		{
			InputField:  "r1_solution_3",
			OutputField: "reflection",
			ProcessorID: "llm",
		},
	}

	// Сохраняем конфиг в файл
	if err := config.SaveToFile("config_example.json"); err != nil {
		log.Fatalf("Ошибка при сохранении примера конфига: %v", err)
	}

	fmt.Println("Пример конфигурационного файла сохранён в config_example.json")
}

// truncateString сокращает строку до указанной длины, добавляя многоточие
func truncateString(s string, maxLen int) string {
	s = strings.TrimSpace(s)
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}
