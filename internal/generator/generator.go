package generator

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/briandowns/spinner"
	"github.com/fatih/color"
	"github.com/schollz/progressbar/v3"

	"syn/internal/client"
	"syn/internal/models"
	"syn/internal/processor"
)

// DatasetGenerator обрабатывает и генерирует новый датасет
type DatasetGenerator struct {
	Config         *models.Config
	Client         *client.LLMClient
	ProcessorMgr   *processor.Manager
	InputDataset   *models.GenericDataset
	OutputDataset  *models.GenericDataset
	Shutdown       atomic.Bool
	ProcessedCount atomic.Int32
	progressBar    *progressbar.ProgressBar
	spinner        *spinner.Spinner
	successCount   atomic.Int32
	errorCount     atomic.Int32
}

// NewDatasetGenerator создаёт новый генератор датасета
func NewDatasetGenerator(config *models.Config, client *client.LLMClient) *DatasetGenerator {
	g := &DatasetGenerator{
		Config:        config,
		Client:        client,
		ProcessorMgr:  processor.NewManager(),
		InputDataset:  models.NewGenericDataset(),
		OutputDataset: models.NewGenericDataset(),
		Shutdown:      atomic.Bool{},
	}

	// Регистрируем процессоры
	g.RegisterProcessors()

	return g
}

// RegisterProcessors регистрирует процессоры
func (g *DatasetGenerator) RegisterProcessors() {
	// Регистрируем базовые процессоры
	g.ProcessorMgr.Register(processor.NewIdentityProcessor())

	// Регистрируем LLM процессор для каждого маппинга с processorID = "llm"
	for _, mapping := range g.Config.Fields.Input {
		if mapping.ProcessorID == "llm" {
			proc := processor.NewLLMProcessor(
				"llm_"+mapping.InputField,
				g.Client,
				g.Config.API.Model,
				g.Config.Prompt.System,
				"", // Пустой шаблон означает, что будем использовать само значение поля
				g.Config.Processing.Temperature,
			)
			g.ProcessorMgr.Register(proc)
		}
	}

	// Если в конфиге есть фильтр, регистрируем его
	if g.Config.Processing.Filter.Enabled {
		filterProc := processor.NewFilterProcessor(
			g.Config.Processing.Filter.Field,
			g.Config.Processing.Filter.MinValue,
			g.Config.Processing.Filter.MaxValue,
			"",                                  // TODO: Добавить поддержку строковых условий
			g.Config.Processing.Filter.Operator, // Используем оператор из конфига
		)
		g.ProcessorMgr.Register(filterProc)
	}

	// Если включена поддержка Hugging Face, регистрируем процессор
	if g.Config.HuggingFace.Enabled {
		hfProc := processor.NewHuggingFaceProcessor(
			"huggingface",
			g.Config.HuggingFace.PythonPath,
			os.TempDir(),
			g.Config.HuggingFace.DatasetName,
			g.Config.HuggingFace.Split,
			g.Config.HuggingFace.Streamable,
			g.Config.HuggingFace.Filters,
			g.Config.HuggingFace.Fields,
		)
		g.ProcessorMgr.Register(hfProc)
	}
}

// Run запускает генерацию датасета
func (g *DatasetGenerator) Run(ctx context.Context) error {
	// Настраиваем обработку сигналов прерывания
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Инициализируем спиннер для показа статуса загрузки
	g.spinner = spinner.New(spinner.CharSets[14], 100*time.Millisecond)
	g.spinner.Suffix = " Инициализация..."
	g.spinner.Color("cyan")

	// Горутина для обработки сигналов
	go func() {
		select {
		case <-sigChan:
			if g.spinner.Active() {
				g.spinner.Stop()
			}
			if g.progressBar != nil {
				_ = g.progressBar.Close()
			}

			fmt.Println(color.YellowString("\n🛑 Получен сигнал прерывания, заканчиваем работу..."))
			g.Shutdown.Store(true)
			cancel()
		case <-ctx.Done():
		}
	}()

	g.spinner.Start()

	// Загружаем данные из источника
	var err error
	if g.Config.HuggingFace.Enabled {
		datasetName := color.CyanString(g.Config.HuggingFace.DatasetName)
		g.spinner.Suffix = fmt.Sprintf(" Загрузка данных из %s...", datasetName)
		err = g.loadDatasetFromHF(ctx)
	} else {
		fileName := color.CyanString(filepath.Base(g.Config.IO.InputFile))
		g.spinner.Suffix = fmt.Sprintf(" Загрузка данных из файла %s...", fileName)
		err = g.loadDataset()
	}

	if err != nil {
		g.spinner.Stop()
		return fmt.Errorf("ошибка загрузки данных: %w", err)
	}

	// Проверяем, что входной датасет загружен
	if g.InputDataset == nil || g.InputDataset.Len() == 0 {
		g.spinner.Stop()
		return fmt.Errorf("входной датасет пуст или не был загружен")
	}

	// Запускаем обработку датасета
	data := g.InputDataset.GetData()
	total := len(data)

	g.spinner.Stop()

	successColor := color.New(color.FgGreen).SprintFunc()
	errorColor := color.New(color.FgRed).SprintFunc()

	fmt.Printf("\n🚀 Начинаем обработку датасета из %s записей\n\n", successColor(fmt.Sprintf("%d", total)))

	// Создаем прогресс-бар
	g.progressBar = progressbar.NewOptions(total,
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionShowCount(),
		progressbar.OptionShowIts(),
		progressbar.OptionSetItsString("записей"),
		progressbar.OptionSetWidth(50),
		progressbar.OptionThrottle(65*time.Millisecond),
		progressbar.OptionSetDescription("🧠 Обработка..."),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}),
		progressbar.OptionOnCompletion(func() {
			fmt.Printf("\n")
		}),
	)

	if g.Config.Processing.NoLLM {
		// В режиме без LLM просто копируем данные в выходной датасет
		fmt.Println(color.YellowString("⚠️ Режим без LLM: данные не будут обрабатываться через модель"))

		// Обновляем прогресс-бар
		g.progressBar.ChangeMax(1)
		g.progressBar.Describe("💾 Копирование данных...")
		g.OutputDataset.SetData(data)
		g.progressBar.Add(1)

		err = g.saveDataset()
		return err
	}

	err = g.processDataset(ctx, data)
	if err != nil {
		fmt.Println(errorColor(fmt.Sprintf("\n❌ Ошибка обработки датасета: %v", err)))
	}

	// Сохраняем результаты
	if g.OutputDataset.Len() > 0 {
		if g.progressBar != nil {
			g.progressBar.Finish()
		}

		g.spinner = spinner.New(spinner.CharSets[11], 100*time.Millisecond)
		g.spinner.Suffix = " Сохранение результатов..."
		g.spinner.Color("cyan")
		g.spinner.Start()

		err = g.saveDataset()
		g.spinner.Stop()

		if err != nil {
			fmt.Println(errorColor(fmt.Sprintf("❌ Ошибка сохранения датасета: %v", err)))
		} else {
			fmt.Println()
			fmt.Println("📊 " + successColor("Статистика:"))
			fmt.Printf("  ✅ Успешно: %s\n", successColor(fmt.Sprintf("%d", g.successCount.Load())))
			if g.errorCount.Load() > 0 {
				fmt.Printf("  ❌ Ошибок: %s\n", errorColor(fmt.Sprintf("%d", g.errorCount.Load())))
			}
			fmt.Printf("  📦 Всего обработано: %s из %s\n",
				successColor(fmt.Sprintf("%d", g.ProcessedCount.Load())),
				successColor(fmt.Sprintf("%d", total)))

			outputPath := filepath.Join(g.Config.IO.OutputDir, g.Config.IO.OutputFile)
			fileInfo, err := os.Stat(outputPath)

			fmt.Println()
			fmt.Printf("💾 Результаты сохранены в %s", color.CyanString(outputPath))

			if err == nil {
				size := fileInfo.Size()
				sizeStr := humanizeBytes(size)
				fmt.Printf(" (%s)\n", color.GreenString(sizeStr))
			} else {
				fmt.Println()
			}
		}
	} else {
		fmt.Println(errorColor("\n⚠️ Нет данных для сохранения"))
	}

	return err
}

// loadDatasetFromHF загружает датасет из Hugging Face
func (g *DatasetGenerator) loadDatasetFromHF(ctx context.Context) error {
	if g.Config.HuggingFace.DatasetName == "" {
		return fmt.Errorf("не указано имя датасета Hugging Face")
	}

	// Получаем процессор Hugging Face
	proc, err := g.ProcessorMgr.Get("huggingface")
	if err != nil {
		return fmt.Errorf("процессор HuggingFace не зарегистрирован: %w", err)
	}

	hfProc, ok := proc.(*processor.HuggingFaceProcessor)
	if !ok {
		return fmt.Errorf("некорректный тип процессора HuggingFace")
	}

	// Устанавливаем опции
	if g.Config.HuggingFace.Limit > 0 {
		hfProc.SetLimit(g.Config.HuggingFace.Limit)
	}
	if g.Config.HuggingFace.Offset > 0 {
		hfProc.SetOffset(g.Config.HuggingFace.Offset)
	}
	if g.Config.HuggingFace.Shuffle {
		hfProc.AddExtraArg("shuffle", "True")
		hfProc.AddExtraArg("seed", fmt.Sprintf("%d", g.Config.HuggingFace.Seed))
	}

	// Устанавливаем режим отладки
	hfProc.SetDebug(g.Config.Debug)

	// Загружаем числовые фильтры
	if g.Config.Processing.Filter.Enabled && g.Config.Processing.Filter.Field != "" {
		hfProc.AddNumericFilter(
			g.Config.Processing.Filter.Field,
			g.Config.Processing.Filter.MinValue,
			g.Config.Processing.Filter.MaxValue,
			g.Config.Processing.Filter.Operator,
		)
	}

	// Обновляем статус спиннера
	if g.spinner != nil && g.spinner.Active() {
		g.spinner.Suffix = fmt.Sprintf(" Загружаем датасет %s...", color.CyanString(g.Config.HuggingFace.DatasetName))
	}

	// Загружаем данные
	data, err := hfProc.Process(ctx, nil)
	if err != nil {
		return fmt.Errorf("ошибка загрузки датасета из Hugging Face: %w", err)
	}

	items, ok := data.([]map[string]interface{})
	if !ok {
		return fmt.Errorf("некорректный формат данных от процессора HuggingFace")
	}

	// Создаем датасет
	g.InputDataset = models.NewGenericDataset()
	g.InputDataset.SetData(items)

	if g.Config.Debug {
		log.Printf("✅ Загружено %d элементов из Hugging Face датасета %s",
			len(items),
			color.CyanString(g.Config.HuggingFace.DatasetName))
	}

	return nil
}

// loadDataset загружает датасет из файла
func (g *DatasetGenerator) loadDataset() error {
	filePath := g.Config.IO.InputFile
	if filePath == "" {
		return fmt.Errorf("не указан путь к файлу с датасетом")
	}

	// Обновляем статус спиннера
	if g.spinner != nil && g.spinner.Active() {
		g.spinner.Suffix = fmt.Sprintf(" Загружаем файл %s...", color.CyanString(filepath.Base(filePath)))
	}

	// Читаем файл
	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("ошибка чтения файла: %w", err)
	}

	// Распознаем формат файла и парсим его
	var items []map[string]interface{}
	ext := strings.ToLower(filepath.Ext(filePath))

	switch ext {
	case ".json":
		// Парсим JSON
		if err := json.Unmarshal(data, &items); err != nil {
			// Пробуем парсить как отдельный объект, не массив
			var singleItem map[string]interface{}
			if err := json.Unmarshal(data, &singleItem); err != nil {
				return fmt.Errorf("ошибка парсинга JSON: %w", err)
			}
			items = []map[string]interface{}{singleItem}
		}
	case ".jsonl":
		// Парсим JSONL (каждая строка - JSON объект)
		scanner := bufio.NewScanner(bytes.NewReader(data))
		for scanner.Scan() {
			line := scanner.Text()
			if line == "" {
				continue
			}
			var item map[string]interface{}
			if err := json.Unmarshal([]byte(line), &item); err != nil {
				return fmt.Errorf("ошибка парсинга строки JSONL: %w", err)
			}
			items = append(items, item)
		}
		if err := scanner.Err(); err != nil {
			return fmt.Errorf("ошибка сканирования JSONL: %w", err)
		}
	default:
		return fmt.Errorf("неподдерживаемый формат файла: %s, поддерживаются только .json и .jsonl", ext)
	}

	// Создаем датасет
	g.InputDataset = models.NewGenericDataset()
	g.InputDataset.SetData(items)

	if g.Config.Debug {
		log.Printf("✅ Загружено %d элементов из файла %s",
			len(items),
			color.CyanString(filepath.Base(filePath)))
	}

	return nil
}

// processDataset обрабатывает датасет параллельно
func (g *DatasetGenerator) processDataset(ctx context.Context, data []map[string]interface{}) error {
	// Создаем пул рабочих горутин
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, g.Config.Processing.MaxConcurrency)
	resultChan := make(chan map[string]interface{}, g.Config.Processing.MaxConcurrency)
	errChan := make(chan error, 1)
	doneChan := make(chan bool)

	// Запускаем горутину для сбора результатов
	go func() {
		for result := range resultChan {
			if result != nil {
				g.OutputDataset.AddItem(result)
			}
		}
		doneChan <- true
	}()

	// Запускаем обработку элементов
	for i, item := range data {
		if g.Shutdown.Load() {
			break
		}

		wg.Add(1)
		semaphore <- struct{}{}

		go func(index int, item map[string]interface{}) {
			defer wg.Done()
			defer func() { <-semaphore }()

			// Проверяем, не был ли контекст отменен
			select {
			case <-ctx.Done():
				return
			default:
			}

			processed, err := g.processItem(ctx, item)
			if err != nil {
				if g.Config.Debug {
					log.Printf("Ошибка обработки элемента %d: %v", index, err)
				}
				g.errorCount.Add(1)
				if len(errChan) == 0 {
					select {
					case errChan <- err:
					default:
					}
				}
				return
			}

			if processed != nil {
				resultChan <- processed
				g.successCount.Add(1)
			}

			count := g.ProcessedCount.Add(1)
			if g.progressBar != nil {
				_ = g.progressBar.Set(int(count))
			}
		}(i, item)
	}

	// Ожидаем завершения всех рабочих горутин
	wg.Wait()
	close(resultChan)

	// Ожидаем завершения сбора результатов
	<-doneChan

	// Проверяем, была ли ошибка
	select {
	case err := <-errChan:
		return err
	default:
		return nil
	}
}

// processItem обрабатывает отдельный элемент датасета
func (g *DatasetGenerator) processItem(ctx context.Context, item map[string]interface{}) (map[string]interface{}, error) {
	// Результат обработки
	result := make(map[string]interface{})

	// Обрабатываем поля согласно конфигурации
	for _, field := range g.Config.Fields.Input {
		// Получаем значение поля из входного элемента
		inputField := field.InputField
		outputField := field.OutputField
		processorID := field.ProcessorID

		// Если в конфигурации не указано выходное поле, используем имя входного
		if outputField == "" {
			outputField = inputField
		}

		// Получаем значение поля
		value, exists := item[inputField]
		if !exists {
			if g.Config.Debug {
				log.Printf("⚠️ Поле %s не найдено в элементе", inputField)
			}
			continue
		}

		// Если указан специальный процессор, используем его
		// Иначе просто копируем значение
		var processed interface{}
		if processorID != "" {
			proc, err := g.ProcessorMgr.Get(processorID)
			if err != nil {
				return nil, fmt.Errorf("не найден процессор %s: %w", processorID, err)
			}

			// Выполняем обработку с таймаутом
			procCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
			defer cancel()

			processed, err = proc.Process(procCtx, value)
			if err != nil {
				return nil, fmt.Errorf("ошибка при обработке поля %s процессором %s: %w", inputField, processorID, err)
			}
		} else {
			// По умолчанию просто копируем значение
			processed = value
		}

		// Добавляем обработанное значение в результат
		result[outputField] = processed
	}

	// Добавляем дополнительные поля из исходного элемента, если они не были перезаписаны
	for key, value := range item {
		_, exists := result[key]
		if !exists {
			result[key] = value
		}
	}

	return result, nil
}

// saveDataset сохраняет обработанный датасет
func (g *DatasetGenerator) saveDataset() error {
	// Проверяем, что есть данные для сохранения
	if g.OutputDataset.Len() == 0 {
		return fmt.Errorf("нет данных для сохранения")
	}

	// Создаем директорию, если ее нет
	outputDir := g.Config.IO.OutputDir
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("ошибка создания директории %s: %w", outputDir, err)
	}

	// Формируем путь к файлу
	outputPath := filepath.Join(outputDir, g.Config.IO.OutputFile)

	// Получаем данные в формате JSON
	data, err := g.OutputDataset.ToJSON()
	if err != nil {
		return fmt.Errorf("ошибка сериализации данных: %w", err)
	}

	// Записываем в файл
	if err := os.WriteFile(outputPath, data, 0644); err != nil {
		return fmt.Errorf("ошибка записи в файл %s: %w", outputPath, err)
	}

	// Вычисляем размер файла для красивого вывода
	fileInfo, err := os.Stat(outputPath)
	if err == nil {
		size := fileInfo.Size()
		sizeStr := humanizeBytes(size)
		if g.Config.Debug {
			log.Printf("💾 Датасет сохранен в %s (размер: %s)",
				color.CyanString(outputPath),
				color.GreenString(sizeStr))
		}
	}

	return nil
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
