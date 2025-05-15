package dsl

import (
	"fmt"
	"strings"
)

// Compiler compiles AST into Python code
type Compiler struct {
	program             *Program
	imports             []string
	datasets            map[string]bool // Tracks created datasets
	debug               bool
	enableSigIntHandler bool // Flag for enabling SIGINT signal handler
}

// NewCompiler creates a new compiler
func NewCompiler(program *Program) *Compiler {
	return &Compiler{
		program: program,
		imports: []string{
			"import datasets",
			"from datasets import load_dataset, Dataset, concatenate_datasets",
			"import pandas as pd",
			"import os",
			"import sys",
			"import json",
			"from openai import AsyncOpenAI",
			"import time",
			"import asyncio",
			"from tqdm import tqdm",
			"import signal",
		},
		datasets:            make(map[string]bool),
		debug:               false,
		enableSigIntHandler: false, // Disabled by default
	}
}

// SetDebug sets debug mode
func (c *Compiler) SetDebug(debug bool) {
	c.debug = debug
}

// EnableSigIntHandler enables or disables the SIGINT signal handler
func (c *Compiler) EnableSigIntHandler(enable bool) {
	c.enableSigIntHandler = enable
}

// Compile compiles the program into Python code
func (c *Compiler) Compile() string {
	var builder strings.Builder

	// Imports
	for _, imp := range c.imports {
		builder.WriteString(imp + "\n")
	}
	builder.WriteString("\n")

	// Main code
	builder.WriteString("def main():\n")

	// Get debug mode from environment variable
	builder.WriteString("    # Define debug mode\n")
	builder.WriteString("    debug = os.environ.get('SYN_DEBUG', '0') == '1'\n")
	builder.WriteString("\n")

	// Default variable declarations
	builder.WriteString("    # Default values\n")
	builder.WriteString("    concurrency = 1\n")
	builder.WriteString("    stream = False\n")
	builder.WriteString("    model = None\n")
	builder.WriteString("    api_key = None\n")
	builder.WriteString("    api_url = None\n")
	builder.WriteString("    output_file = 'output.json'\n")
	builder.WriteString("    loaded_datasets = {}\n")
	builder.WriteString("    was_saved = False\n")
	builder.WriteString("    prompt_templates = {}\n")
	builder.WriteString("    system_prompts = {}\n")
	builder.WriteString("    shutdown = False\n")
	builder.WriteString(fmt.Sprintf("    sigint_handler_registered = %s  # Flag indicating whether SIGINT handler is registered\n",
		func() string {
			if c.enableSigIntHandler {
				return "True"
			}
			return "False"
		}()))
	builder.WriteString("\n")

	// Add signal handler for graceful shutdown
	builder.WriteString("    # Ctrl+C signal handler\n")
	builder.WriteString("    def signal_handler(sig, frame):\n")
	builder.WriteString("        nonlocal shutdown\n")
	builder.WriteString("        if shutdown:\n")
	builder.WriteString("            return  # If already processing a signal, don't do it again\n")
	builder.WriteString("        print('\\n🛑 Termination signal received. Saving current results...')\n")
	builder.WriteString("        shutdown = True\n")
	builder.WriteString("        # Save current results\n")
	builder.WriteString("        save_current_results()\n")
	builder.WriteString("        print('👋 Shutting down.')\n")
	builder.WriteString("        # Explicitly terminate the process with code 0\n")
	builder.WriteString("        sys.exit(0)\n")
	builder.WriteString("    \n")

	// Register the signal handler only if the flag is enabled
	if c.enableSigIntHandler {
		builder.WriteString("    signal.signal(signal.SIGINT, signal_handler)\n\n")
	} else {
		builder.WriteString("    # SIGINT signal handler is disabled\n\n")
	}

	// Add function to save current results
	builder.WriteString("    # Function to save current results\n")
	builder.WriteString("    def save_current_results():\n")
	builder.WriteString("        if not loaded_datasets:\n")
	builder.WriteString("            print('❌ No data to save.')\n")
	builder.WriteString("            return\n")
	builder.WriteString("        \n")
	builder.WriteString("        # Select the last loaded dataset\n")
	builder.WriteString("        last_dataset_name = list(loaded_datasets.keys())[-1]\n")
	builder.WriteString("        last_dataset = loaded_datasets[last_dataset_name]\n")
	builder.WriteString("        \n")
	builder.WriteString("        # Create output directory if it doesn't exist\n")
	builder.WriteString("        os.makedirs('output', exist_ok=True)\n")
	builder.WriteString("        \n")
	builder.WriteString("        # Generate emergency save filename if name is not specified\n")
	builder.WriteString("        save_filename = output_file\n")
	builder.WriteString("        if not was_saved:\n")
	builder.WriteString("            # Get timestamp for filename\n")
	builder.WriteString("            timestamp = time.strftime('%Y%m%d_%H%M%S')\n")
	builder.WriteString("            save_filename = f'emergency_save_{timestamp}.json'\n")
	builder.WriteString("        \n")
	builder.WriteString("        # Form the full path to the directory where the dataset will be saved\n")
	builder.WriteString("        dataset_dir = os.path.join('output', os.path.splitext(save_filename)[0])\n")
	builder.WriteString("        json_path = os.path.join('output', save_filename)\n")
	builder.WriteString("        \n")
	builder.WriteString("        # Check if the directory for saving already exists\n")
	builder.WriteString("        if os.path.exists(dataset_dir):\n")
	builder.WriteString("            print(f'ℹ️ Dataset already saved to {dataset_dir}. Skipping re-save.')\n")
	builder.WriteString("            return\n")
	builder.WriteString("        \n")
	builder.WriteString("        print(f'💾 Saving dataset {last_dataset_name} to {dataset_dir}...')\n")
	builder.WriteString("        \n")
	builder.WriteString("        try:\n")
	builder.WriteString("            # Save the dataset using Hugging Face's native method\n")
	builder.WriteString("            last_dataset.save_to_disk(dataset_dir)\n")
	builder.WriteString("            \n")
	builder.WriteString("            # Also create a JSON version for compatibility\n")
	builder.WriteString("            with open(json_path, 'w', encoding='utf-8') as f:\n")
	builder.WriteString("                json.dump([item for item in last_dataset], f, ensure_ascii=False, indent=2)\n")
	builder.WriteString("            \n")
	builder.WriteString("            print(f'✅ Done! Processed {last_dataset.num_rows} records. Dataset saved to {dataset_dir} and as JSON to {json_path}')\n")
	builder.WriteString("        except Exception as e:\n")
	builder.WriteString("            print(f'❌ Error saving results: {e}')\n")
	builder.WriteString("    \n")

	// Add functions for asynchronous content generation with OpenAI
	builder.WriteString("    # Function for asynchronous OpenAI API calls\n")
	builder.WriteString("    async def call_openai_api_async(prompt, model_name='gpt-3.5-turbo', temperature=0.7, max_tokens=1024, semaphore=None, system_prompt=None):\n")
	builder.WriteString("        client = None\n")
	builder.WriteString("        \n")
	builder.WriteString("        # If semaphore is provided, use it to control concurrency\n")
	builder.WriteString("        async with semaphore or asyncio.Semaphore(1):\n")
	builder.WriteString("            try:\n")
	builder.WriteString("                if debug:\n")
	builder.WriteString("                    print(f'Request to model {model_name} with temperature {temperature}')\n")
	builder.WriteString("                    print(f'Prompt: {prompt[:100]}...' if len(prompt) > 100 else f'Prompt: {prompt}')\n")
	builder.WriteString("                    if system_prompt:\n")
	builder.WriteString("                        print(f'System prompt: {system_prompt[:100]}...' if len(system_prompt) > 100 else f'System prompt: {system_prompt}')\n")
	builder.WriteString("                \n")
	builder.WriteString("                client = AsyncOpenAI(api_key=api_key, base_url=api_url if api_url else None)\n")
	builder.WriteString("                \n")
	builder.WriteString("                # Set timeout to 30 seconds\n")
	builder.WriteString("                start_time = time.time()\n")
	builder.WriteString("                max_time = 30  # maximum wait time in seconds\n")
	builder.WriteString("                \n")
	builder.WriteString("                # Format messages based on whether a system prompt is provided\n")
	builder.WriteString("                messages = []\n")
	builder.WriteString("                if system_prompt:\n")
	builder.WriteString("                    messages.append({'role': 'system', 'content': system_prompt})\n")
	builder.WriteString("                messages.append({'role': 'user', 'content': prompt})\n")
	builder.WriteString("                \n")
	builder.WriteString("                response = await client.chat.completions.create(\n")
	builder.WriteString("                    model=model_name,\n")
	builder.WriteString("                    messages=messages,\n")
	builder.WriteString("                    temperature=temperature,\n")
	builder.WriteString("                    max_tokens=max_tokens,\n")
	builder.WriteString("                    timeout=20  # Timeout in seconds for HTTP request\n")
	builder.WriteString("                )\n")
	builder.WriteString("                \n")
	builder.WriteString("                return response.choices[0].message.content.strip()\n")
	builder.WriteString("            except Exception as e:\n")
	builder.WriteString("                error_msg = str(e)\n")
	builder.WriteString("                print(f'Error calling OpenAI API: {error_msg}')\n")
	builder.WriteString("                if 'authentication' in error_msg.lower() or 'key' in error_msg.lower():\n")
	builder.WriteString("                    print('Problem with API key. Check your key.')\n")
	builder.WriteString("                elif 'timeout' in error_msg.lower() or 'connection' in error_msg.lower():\n")
	builder.WriteString("                    print('Timeout exceeded. Check your internet connection or API availability.')\n")
	builder.WriteString("                return f'[Generation error: {error_msg}]'\n")
	builder.WriteString("            finally:\n")
	builder.WriteString("                # Close the client if possible\n")
	builder.WriteString("                if client and hasattr(client, 'close'):\n")
	builder.WriteString("                    try:\n")
	builder.WriteString("                        await client.close()\n")
	builder.WriteString("                    except:\n")
	builder.WriteString("                        pass\n\n")

	// Aynchronous function for processing one record of the dataset
	builder.WriteString("    # Function for asynchronous processing of one dataset record\n")
	builder.WriteString("    async def process_item_async(item, source_field, target_field, model_name, temperature, max_tokens, prompt_template, semaphore, pbar=None):\n")
	builder.WriteString("        try:\n")
	builder.WriteString("            if shutdown:\n")
	builder.WriteString("                return item\n")
	builder.WriteString("            \n")
	builder.WriteString("            item_dict = dict(item)\n")
	builder.WriteString("            system_prompt = None\n")
	builder.WriteString("            \n")
	builder.WriteString("            # Check for system prompt presence\n")
	builder.WriteString("            if prompt_template is not None and prompt_template in system_prompts:\n")
	builder.WriteString("                system_prompt = system_prompts[prompt_template]\n")
	builder.WriteString("            \n")
	builder.WriteString("            # Form prompt based on template or source field\n")
	builder.WriteString("            if prompt_template is not None and prompt_template in prompt_templates:\n")
	builder.WriteString("                template = prompt_templates[prompt_template]['template']\n")
	builder.WriteString("                fields = prompt_templates[prompt_template]['fields']\n")
	builder.WriteString("                \n")
	builder.WriteString("                # Replace fields in template\n")
	builder.WriteString("                prompt = template\n")
	builder.WriteString("                for field in fields:\n")
	builder.WriteString("                    if field in item_dict:\n")
	builder.WriteString("                        prompt = prompt.replace('{' + field + '}', str(item_dict[field]))\n")
	builder.WriteString("            else:\n")
	builder.WriteString("                # Use source field directly\n")
	builder.WriteString("                if source_field in item_dict:\n")
	builder.WriteString("                    prompt = str(item_dict[source_field])\n")
	builder.WriteString("                else:\n")
	builder.WriteString("                    print(f'Warning: field {source_field} is missing in record')\n")
	builder.WriteString("                    prompt = ''\n")
	builder.WriteString("            \n")
	builder.WriteString("            # Generate response\n")
	builder.WriteString("            response = await call_openai_api_async(prompt, model_name, temperature, max_tokens, semaphore, system_prompt)\n")
	builder.WriteString("            \n")
	builder.WriteString("            # Add result\n")
	builder.WriteString("            item_dict[target_field] = response\n")
	builder.WriteString("            return item_dict\n")
	builder.WriteString("        except Exception as e:\n")
	builder.WriteString("            print(f'Error processing record: {e}')\n")
	builder.WriteString("            return item\n")
	builder.WriteString("        finally:\n")
	builder.WriteString("            if pbar:\n")
	builder.WriteString("                pbar.update(1)\n\n")

	// Aynchronous function for generating content
	builder.WriteString("    # Function for asynchronous content generation for the entire dataset\n")
	builder.WriteString("    async def generate_content_async(dataset, source_field, target_field, model_name=None, temperature=0.7, max_tokens=1024, prompt_template=None):\n")
	builder.WriteString("        if model_name is None:\n")
	builder.WriteString("            if model is None:\n")
	builder.WriteString("                print('❌ Error: model not specified for generation')\n")
	builder.WriteString("                return dataset\n")
	builder.WriteString("            model_name = model\n")
	builder.WriteString("        \n")
	builder.WriteString("        if api_key is None:\n")
	builder.WriteString("            print('❌ Error: API key not specified for accessing OpenAI API')\n")
	builder.WriteString("            return dataset\n")
	builder.WriteString("        \n")
	builder.WriteString("        print(f'🔄 Generating field {target_field} based on {source_field} using model {model_name}...')\n")
	builder.WriteString("        \n")
	builder.WriteString("        try:\n")
	builder.WriteString("            # Create semaphore for controlling concurrency\n")
	builder.WriteString("            semaphore = asyncio.Semaphore(concurrency)\n")
	builder.WriteString("            \n")
	builder.WriteString("            # Demonstration mode - process a limited number of records\n")
	builder.WriteString("            if debug:\n")
	builder.WriteString("                sample_size = min(5, len(dataset))\n")
	builder.WriteString("                dataset_sample = dataset.select(range(sample_size))\n")
	builder.WriteString("            else:\n")
	builder.WriteString("                # In real mode process the entire dataset\n")
	builder.WriteString("                sample_size = len(dataset)\n")
	builder.WriteString("                dataset_sample = dataset\n")
	builder.WriteString("            \n")
	builder.WriteString("            print(f'Processing {sample_size} records...')\n")
	builder.WriteString("            \n")
	builder.WriteString("            # Create list of tasks for asynchronous processing\n")
	builder.WriteString("            tasks = []\n")
	builder.WriteString("            all_items = list(dataset_sample)\n")
	builder.WriteString("            processed_items = []\n")
	builder.WriteString("            \n")
	builder.WriteString("            # Prepare progress bar\n")
	builder.WriteString("            pbar = tqdm(total=sample_size, desc='Generation')\n")
	builder.WriteString("            \n")
	builder.WriteString("            # Process records asynchronously\n")
	builder.WriteString("            batch_size = min(100, sample_size)  # Process 100 records at a time\n")
	builder.WriteString("            \n")
	builder.WriteString("            for i in range(0, sample_size, batch_size):\n")
	builder.WriteString("                if shutdown:\n")
	builder.WriteString("                    print('\\n🛑 Stopping processing due to signal')\n")
	builder.WriteString("                    break\n")
	builder.WriteString("                \n")
	builder.WriteString("                # Define current batch\n")
	builder.WriteString("                current_batch = all_items[i:min(i+batch_size, sample_size)]\n")
	builder.WriteString("                batch_tasks = []\n")
	builder.WriteString("                \n")
	builder.WriteString("                # Create tasks for current batch\n")
	builder.WriteString("                for item in current_batch:\n")
	builder.WriteString("                    task = asyncio.create_task(process_item_async(\n")
	builder.WriteString("                        item, source_field, target_field, model_name, \n")
	builder.WriteString("                        temperature, max_tokens, prompt_template, semaphore, pbar\n")
	builder.WriteString("                    ))\n")
	builder.WriteString("                    batch_tasks.append(task)\n")
	builder.WriteString("                \n")
	builder.WriteString("                # Wait for current batch completion\n")
	builder.WriteString("                batch_results = await asyncio.gather(*batch_tasks)\n")
	builder.WriteString("                processed_items.extend(batch_results)\n")
	builder.WriteString("                \n")
	builder.WriteString("                # If stop signal received, stop processing but save what we processed\n")
	builder.WriteString("                if shutdown:\n")
	builder.WriteString("                    print('\\n🛑 Stopping after processing current batch...')\n")
	builder.WriteString("                    break\n")
	builder.WriteString("            \n")
	builder.WriteString("            pbar.close()\n")
	builder.WriteString("            \n")
	builder.WriteString("            # Create new dataset with results\n")
	builder.WriteString("            print('✅ Generation completed!')\n")
	builder.WriteString("            \n")
	builder.WriteString("            # Check if all records were processed\n")
	builder.WriteString("            if len(processed_items) < sample_size:\n")
	builder.WriteString("                print(f'ℹ️ Processed {len(processed_items)} out of {sample_size} records (stopped by user)')\n")
	builder.WriteString("            \n")
	builder.WriteString("            # Create dataset from processed records\n")
	builder.WriteString("            return Dataset.from_list(processed_items)\n")
	builder.WriteString("        except Exception as e:\n")
	builder.WriteString("            print(f'❌ Error generating content: {e}')\n")
	builder.WriteString("            # Save what we processed\n")
	builder.WriteString("            if 'processed_items' in locals() and processed_items:\n")
	builder.WriteString("                print(f'💾 Saving {len(processed_items)} processed records...')\n")
	builder.WriteString("                return Dataset.from_list(processed_items)\n")
	builder.WriteString("            # Return original dataset in case of error\n")
	builder.WriteString("            return dataset\n\n")

	// Function for generating content (synchronous version)
	builder.WriteString("    # Function for generating content (synchronous version)\n")
	builder.WriteString("    def generate_content(dataset, source_field, target_field, model_name=None, temperature=0.7, max_tokens=1024, prompt_template=None):\n")
	builder.WriteString("        # Start asynchronous version through event loop\n")
	builder.WriteString("        loop = asyncio.new_event_loop()\n")
	builder.WriteString("        asyncio.set_event_loop(loop)\n")
	builder.WriteString("        try:\n")
	builder.WriteString("            # Set signal handler for loop\n")
	builder.WriteString("            def handle_loop_signal():\n")
	builder.WriteString("                for task in asyncio.all_tasks(loop):\n")
	builder.WriteString("                    task.cancel()\n")
	builder.WriteString("            \n")
	builder.WriteString("            # Add graceful shutdown handler\n")
	builder.WriteString("            # Добавляем обработчик для graceful shutdown\n")
	builder.WriteString("            if not shutdown and not sigint_handler_registered:  # Не регистрируем, если уже включен на уровне скрипта\n")
	builder.WriteString("                loop.add_signal_handler(signal.SIGINT, handle_loop_signal)\n")
	builder.WriteString("            \n")
	builder.WriteString("            return loop.run_until_complete(generate_content_async(\n")
	builder.WriteString("                dataset, source_field, target_field, model_name, temperature, max_tokens, prompt_template\n")
	builder.WriteString("            ))\n")
	builder.WriteString("        except (KeyboardInterrupt, asyncio.CancelledError):\n")
	builder.WriteString("            print('\\n🛑 Обработка прервана пользователем.')\n")
	builder.WriteString("            if 'processed_items' in locals() and processed_items:\n")
	builder.WriteString("                print(f'💾 Сохраняем {len(processed_items)} обработанных записей...')\n")
	builder.WriteString("                return Dataset.from_list(processed_items)\n")
	builder.WriteString("            return dataset\n")
	builder.WriteString("        finally:\n")
	builder.WriteString("            # Очищаем все незавершенные задачи\n")
	builder.WriteString("            try:\n")
	builder.WriteString("                pending = asyncio.all_tasks(loop)\n")
	builder.WriteString("                for task in pending:\n")
	builder.WriteString("                    task.cancel()\n")
	builder.WriteString("                \n")
	builder.WriteString("                # Даем задачам возможность завершиться корректно\n")
	builder.WriteString("                if pending:\n")
	builder.WriteString("                    loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))\n")
	builder.WriteString("            except Exception:\n")
	builder.WriteString("                pass\n")
	builder.WriteString("            \n")
	builder.WriteString("            # Закрываем loop\n")
	builder.WriteString("            loop.close()\n\n")

	// Функции для работы с датасетами
	builder.WriteString("    # Функция для загрузки датасета\n")
	builder.WriteString("    def load_dataset_with_config(name, streaming=False, fields=None, filters=None):\n")
	builder.WriteString("        if debug:\n")
	builder.WriteString("            print(f'Загрузка датасета {name}...')\n")
	builder.WriteString("        else:\n")
	builder.WriteString("            print(f'Загрузка датасета {name}...')\n")
	builder.WriteString("        ds = load_dataset(name, streaming=streaming)\n")
	builder.WriteString("        \n")
	builder.WriteString("        # Выбираем сплит 'train', если это DatasetDict\n")
	builder.WriteString("        if not streaming and isinstance(ds, dict):\n")
	builder.WriteString("            if debug:\n")
	builder.WriteString("                print(f'Выбираем сплит train для датасета')\n")
	builder.WriteString("            ds = ds['train']\n")
	builder.WriteString("        \n")
	builder.WriteString("        # Применение фильтров\n")
	builder.WriteString("        if filters:\n")
	builder.WriteString("            if debug:\n")
	builder.WriteString("                print(f'Применение фильтров: {filters}')\n")
	builder.WriteString("            if streaming:\n")
	builder.WriteString("                ds = ds.filter(lambda x: all(x.get(k) is not None and eval(f\"x['{k}'] {v['op']} {v['value']}\") for k, v in filters.items()))\n")
	builder.WriteString("            else:\n")
	builder.WriteString("                for key, filter_info in filters.items():\n")
	builder.WriteString("                    if '.' in key:\n")
	builder.WriteString("                        print(f'Предупреждение: Вложенные фильтры пока не поддерживаются: {key}')\n")
	builder.WriteString("                        continue\n")
	builder.WriteString("                    ds = ds.filter(lambda x: key in x and eval(f\"x['{key}'] {filter_info['op']} {filter_info['value']}\"))\n")
	builder.WriteString("        \n")
	builder.WriteString("        # Выбор полей\n")
	builder.WriteString("        if fields:\n")
	builder.WriteString("            if debug:\n")
	builder.WriteString("                print(f'Выбор полей: {fields}')\n")
	builder.WriteString("            ds = ds.select_columns(fields)\n")
	builder.WriteString("        \n")
	builder.WriteString("        return ds\n\n")

	// Компиляция утверждений
	for _, stmt := range c.program.Statements {
		c.compileStatement(&builder, stmt, 1)
	}

	// Сохранение результатов, если не был использован оператор SAVE и только при прерывании сигналом
	builder.WriteString("    # Если явное сохранение не было произведено, сохранение будет выполнено только при прерывании сигналом\n")
	builder.WriteString("    # Автоматическое сохранение при SIGINT реализовано через signal_handler\n\n")

	// Вызов main
	builder.WriteString("if __name__ == '__main__':\n")
	builder.WriteString("    main()\n")

	return builder.String()
}

// compileStatement компилирует одно утверждение
func (c *Compiler) compileStatement(builder *strings.Builder, node Node, indent int) {
	indentStr := strings.Repeat("    ", indent)

	switch n := node.(type) {
	case *FromStatement:
		// Генерируем уникальное имя для датасета
		datasetVar := fmt.Sprintf("ds_%s", sanitizeVarName(n.Dataset))
		c.datasets[datasetVar] = true

		if c.debug {
			builder.WriteString(fmt.Sprintf("%s# Загрузка датасета %s\n", indentStr, n.Dataset))
		} else {
			builder.WriteString(fmt.Sprintf("%s# Загрузка датасета %s\n", indentStr, n.Dataset))
		}

		// Объявляем переменные для этого датасета
		builder.WriteString(fmt.Sprintf("%sfields_%s = []\n", indentStr, datasetVar))
		builder.WriteString(fmt.Sprintf("%sfilters_%s = {}\n", indentStr, datasetVar))

		// Создаем три слайса для разных типов инструкций
		var setupInstructions []Node
		var generateStatements []Node
		var saveStatements []Node

		// Если есть блок, распределяем его инструкции по типам
		if n.Block != nil {
			for _, stmt := range n.Block.Statements {
				switch stmt.(type) {
				case *GenerateStatement:
					generateStatements = append(generateStatements, stmt)
				case *SaveStatement:
					saveStatements = append(saveStatements, stmt)
				default:
					setupInstructions = append(setupInstructions, stmt)
				}
			}
		}

		// 1. Сначала компилируем все инструкции настройки (FIELDS, USING, WITH, PROMPT и т.д.)
		for _, stmt := range setupInstructions {
			c.compileBlockStatement(builder, stmt, indent, datasetVar)
		}

		// 2. Затем загружаем датасет с настроенными параметрами
		builder.WriteString(fmt.Sprintf("%s# Загружаем датасет с настроенными параметрами\n", indentStr))
		builder.WriteString(fmt.Sprintf("%s%s = load_dataset_with_config('%s', streaming=stream, fields=fields_%s, filters=filters_%s)\n",
			indentStr, datasetVar, n.Dataset, datasetVar, datasetVar))
		builder.WriteString(fmt.Sprintf("%sloaded_datasets['%s'] = %s\n", indentStr, datasetVar, datasetVar))

		// 3. После загрузки датасета компилируем инструкции генерации
		if len(generateStatements) > 0 {
			builder.WriteString(fmt.Sprintf("%s# Генерация новых полей в датасете\n", indentStr))
			for _, stmt := range generateStatements {
				c.compileBlockStatement(builder, stmt, indent, datasetVar)
			}
		}

		// 4. И в конце инструкции сохранения
		if len(saveStatements) > 0 {
			builder.WriteString(fmt.Sprintf("%s# Сохранение результатов\n", indentStr))
			for _, stmt := range saveStatements {
				c.compileBlockStatement(builder, stmt, indent, datasetVar)
			}
		}

	case *WithStatement:
		if n.Type == "CONCURRENCY" {
			builder.WriteString(fmt.Sprintf("%sconcurrency = %v\n", indentStr, n.Value))
		} else if n.Type == "STREAM" {
			builder.WriteString(fmt.Sprintf("%sstream = True\n", indentStr))
		}

		// Если это WithStatement вне блока FROM, обрабатываем его блок как обычные утверждения
		if n.Block != nil {
			for _, stmt := range n.Block.Statements {
				c.compileStatement(builder, stmt, indent)
			}
		}

	case *PragmaStatement:
		if n.Type == "AUTOSAVE" {
			// Обработка PRAGMA AUTOSAVE
			c.enableSigIntHandler = true
			builder.WriteString(fmt.Sprintf("%s# Директива PRAGMA AUTOSAVE: включаем автосохранение при SIGINT\n", indentStr))
			builder.WriteString(fmt.Sprintf("%s# Регистрируем обработчик сигнала\n", indentStr))
			builder.WriteString(fmt.Sprintf("%ssigint_handler_registered = True  # Устанавливаем флаг регистрации обработчика SIGINT\n", indentStr))
			builder.WriteString(fmt.Sprintf("%ssignal.signal(signal.SIGINT, signal_handler)\n", indentStr))
		} else if n.Type == "CONCURRENCY" {
			// Обработка PRAGMA CONCURRENCY
			concurrencyValue, ok := n.Value.(int)
			if !ok {
				concurrencyValue = 4 // Значение по умолчанию, если что-то пошло не так
			}
			builder.WriteString(fmt.Sprintf("%s# Директива PRAGMA CONCURRENCY: устанавливаем глобальную конкурентность\n", indentStr))
			builder.WriteString(fmt.Sprintf("%sconcurrency = %d\n", indentStr, concurrencyValue))
		}

	case *FieldsStatement:
		// Если нет активного датасета, создаем общие поля
		builder.WriteString(fmt.Sprintf("%sfields = %s\n", indentStr, formatPythonList(n.Fields)))

	case *UsingStatement:
		if n.Type == "MODEL" {
			builder.WriteString(fmt.Sprintf("%smodel = '%s'\n", indentStr, n.Value))
		} else if n.Type == "KEY" {
			builder.WriteString(fmt.Sprintf("%sapi_key = '%s'\n", indentStr, n.Value))
		} else if n.Type == "URL" {
			builder.WriteString(fmt.Sprintf("%sapi_url = '%s'\n", indentStr, n.Value))
		}

	case *UsingBlock:
		for _, stmt := range n.Statements {
			if stmt.Type == "MODEL" {
				builder.WriteString(fmt.Sprintf("%smodel = '%s'\n", indentStr, stmt.Value))
			} else if stmt.Type == "KEY" {
				builder.WriteString(fmt.Sprintf("%sapi_key = '%s'\n", indentStr, stmt.Value))
			} else if stmt.Type == "URL" {
				builder.WriteString(fmt.Sprintf("%sapi_url = '%s'\n", indentStr, stmt.Value))
			}
		}

	case *FilterStatement:
		pythonOp := convertOperatorToPython(n.Operator)
		valueStr := formatPythonValue(n.Value)
		builder.WriteString(fmt.Sprintf("%sfilters['%s'] = {'op': '%s', 'value': %s}\n",
			indentStr, n.Field, pythonOp, valueStr))

	case *FilterBlock:
		for _, condition := range n.Conditions {
			pythonOp := convertOperatorToPython(condition.Operator)
			valueStr := formatPythonValue(condition.Value)
			builder.WriteString(fmt.Sprintf("%sfilters['%s.%s'] = {'op': '%s', 'value': %s}\n",
				indentStr, n.Field, condition.Field, pythonOp, valueStr))
		}

	case *Block:
		for _, stmt := range n.Statements {
			c.compileStatement(builder, stmt, indent)
		}

	case *DatasetMergeStatement:
		// Создаем новый объединенный датасет
		mergedVar := fmt.Sprintf("merged_ds_%d", len(c.datasets)+1)
		c.datasets[mergedVar] = true

		builder.WriteString(fmt.Sprintf("%s# Объединение датасетов\n", indentStr))
		builder.WriteString(fmt.Sprintf("%s%s = concatenate_datasets([", indentStr, mergedVar))

		for i, dsName := range n.Datasets {
			dsVar := fmt.Sprintf("ds_%s", sanitizeVarName(dsName))
			builder.WriteString(dsVar)
			if i < len(n.Datasets)-1 {
				builder.WriteString(", ")
			}
		}

		builder.WriteString("])\n")

		// Сохраняем датасет в словарь
		builder.WriteString(fmt.Sprintf("%sloaded_datasets['%s'] = %s\n", indentStr, mergedVar, mergedVar))

	case *SaveStatement:
		builder.WriteString(fmt.Sprintf("%s# Сохранение в файл\n", indentStr))
		builder.WriteString(fmt.Sprintf("%soutput_file = '%s'\n", indentStr, n.Filename))
		builder.WriteString(fmt.Sprintf("%swas_saved = True\n", indentStr))

		// Используем общую функцию для сохранения результатов
		builder.WriteString(fmt.Sprintf("%s# Сохраняем датасет\n", indentStr))
		builder.WriteString(fmt.Sprintf("%ssave_current_results()\n", indentStr))

	case *PromptStatement:
		builder.WriteString(fmt.Sprintf("%s# Определение шаблона промпта %s\n", indentStr, n.Name))

		// Определяем список полей для замены в шаблоне
		fieldsStr := "[]"
		if len(n.Fields) > 0 {
			fieldsStr = fmt.Sprintf("[%s]", strings.Join(formatPythonStringList(n.Fields), ", "))
		}

		// В зависимости от типа промпта сохраняем его в соответствующий словарь
		if n.PromptType == "system" {
			// Для системного промпта сохраняем только текст
			builder.WriteString(fmt.Sprintf("%ssystem_prompts['%s'] = '%s'\n", indentStr, n.Name, n.Template))

			if c.debug {
				builder.WriteString(fmt.Sprintf("%sif debug:\n", indentStr))
				builder.WriteString(fmt.Sprintf("%s    print(f'Определен системный промпт: %s')\n", indentStr, n.Name))
			}
		} else {
			// Для пользовательского промпта сохраняем шаблон в словарь
			builder.WriteString(fmt.Sprintf("%sprompt_templates['%s'] = {\n", indentStr, n.Name))
			builder.WriteString(fmt.Sprintf("%s    'template': '%s',\n", indentStr, n.Template))
			builder.WriteString(fmt.Sprintf("%s    'fields': %s\n", indentStr, fieldsStr))
			builder.WriteString(fmt.Sprintf("%s}\n", indentStr))

			if c.debug {
				builder.WriteString(fmt.Sprintf("%sif debug:\n", indentStr))
				builder.WriteString(fmt.Sprintf("%s    print(f'Определен шаблон промпта: %s')\n", indentStr, n.Name))
			}
		}

	case *GenerateStatement:
		builder.WriteString(fmt.Sprintf("%s# Генерация поля %s на основе %s\n", indentStr, n.TargetField, n.SourceField))

		// Если не указана модель явно, используем глобальную
		modelStr := "None"
		if n.Model != "" {
			modelStr = fmt.Sprintf("'%s'", n.Model)
		}

		// Определяем промпт, если он указан
		promptStr := "None"
		if len(n.PromptTemplates) > 0 {
			promptStr = fmt.Sprintf("'%s'", n.PromptTemplates[0])
		}

		// Получаем последний добавленный датасет
		builder.WriteString(fmt.Sprintf("%s# Выбираем последний загруженный датасет\n", indentStr))
		builder.WriteString(fmt.Sprintf("%slast_dataset_name = list(loaded_datasets.keys())[-1]\n", indentStr))
		builder.WriteString(fmt.Sprintf("%slast_dataset = loaded_datasets[last_dataset_name]\n", indentStr))

		// Генерируем контент с асинхронной обработкой
		builder.WriteString(fmt.Sprintf("%s# Запускаем асинхронную генерацию контента\n", indentStr))
		builder.WriteString(fmt.Sprintf("%slast_dataset = generate_content(last_dataset, '%s', '%s', %s, %.1f, %d, %s)\n",
			indentStr, n.SourceField, n.TargetField, modelStr, n.Temperature, n.Tokens, promptStr))

		// Обновляем датасет в словаре
		builder.WriteString(fmt.Sprintf("%sloaded_datasets[last_dataset_name] = last_dataset\n", indentStr))
	}
}

// compileBlockStatement компилирует утверждение внутри блока FROM
func (c *Compiler) compileBlockStatement(builder *strings.Builder, node Node, indent int, datasetVar string) {
	indentStr := strings.Repeat("    ", indent)

	switch n := node.(type) {
	case *FieldsStatement:
		builder.WriteString(fmt.Sprintf("%sfields_%s = %s\n", indentStr, datasetVar, formatPythonList(n.Fields)))

	case *FilterStatement:
		pythonOp := convertOperatorToPython(n.Operator)
		valueStr := formatPythonValue(n.Value)
		builder.WriteString(fmt.Sprintf("%sfilters_%s['%s'] = {'op': '%s', 'value': %s}\n",
			indentStr, datasetVar, n.Field, pythonOp, valueStr))

	case *FilterBlock:
		for _, condition := range n.Conditions {
			pythonOp := convertOperatorToPython(condition.Operator)
			valueStr := formatPythonValue(condition.Value)
			builder.WriteString(fmt.Sprintf("%sfilters_%s['%s.%s'] = {'op': '%s', 'value': %s}\n",
				indentStr, datasetVar, n.Field, condition.Field, pythonOp, valueStr))
		}

	case *UsingStatement:
		if n.Type == "MODEL" {
			builder.WriteString(fmt.Sprintf("%smodel = '%s'\n", indentStr, n.Value))
		} else if n.Type == "KEY" {
			builder.WriteString(fmt.Sprintf("%sapi_key = '%s'\n", indentStr, n.Value))
		} else if n.Type == "URL" {
			builder.WriteString(fmt.Sprintf("%sapi_url = '%s'\n", indentStr, n.Value))
		}

	case *UsingBlock:
		for _, stmt := range n.Statements {
			if stmt.Type == "MODEL" {
				builder.WriteString(fmt.Sprintf("%smodel = '%s'\n", indentStr, stmt.Value))
			} else if stmt.Type == "KEY" {
				builder.WriteString(fmt.Sprintf("%sapi_key = '%s'\n", indentStr, stmt.Value))
			} else if stmt.Type == "URL" {
				builder.WriteString(fmt.Sprintf("%sapi_url = '%s'\n", indentStr, stmt.Value))
			}
		}

	case *WithStatement:
		// Устанавливаем параметры WithStatement
		if n.Type == "CONCURRENCY" {
			builder.WriteString(fmt.Sprintf("%sconcurrency = %v\n", indentStr, n.Value))
		} else if n.Type == "STREAM" {
			builder.WriteString(fmt.Sprintf("%sstream = True\n", indentStr))
		}

		// Если у WithStatement есть блок, обрабатываем его содержимое
		// в контексте текущего датасета (для WITH CONCURRENCY внутри FROM)
		if n.Block != nil {
			// Для WITH блоков внутри FROM мы собираем инструкции Generate и компилируем их позже
			var generateStatements []Node
			var otherStatements []Node

			// Сначала разделяем на операторы генерации и все остальное
			for _, stmt := range n.Block.Statements {
				switch stmt.(type) {
				case *GenerateStatement:
					generateStatements = append(generateStatements, stmt)
				default:
					otherStatements = append(otherStatements, stmt)
				}
			}

			// Сначала обрабатываем все, кроме операторов генерации (промпты, настройки и т.д.)
			for _, stmt := range otherStatements {
				c.compileBlockStatement(builder, stmt, indent, datasetVar)
			}

			// Затем обрабатываем операторы генерации
			for _, stmt := range generateStatements {
				c.compileBlockStatement(builder, stmt, indent, datasetVar)
			}
		}

	case *GenerateStatement:
		builder.WriteString(fmt.Sprintf("%s# Генерация поля %s на основе %s\n", indentStr, n.TargetField, n.SourceField))

		// Если не указана модель явно, используем глобальную
		modelStr := "None"
		if n.Model != "" {
			modelStr = fmt.Sprintf("'%s'", n.Model)
		}

		// Определяем промпт, если он указан
		promptStr := "None"
		if len(n.PromptTemplates) > 0 {
			promptStr = fmt.Sprintf("'%s'", n.PromptTemplates[0])
		}

		// Генерируем контент с асинхронной обработкой
		builder.WriteString(fmt.Sprintf("%s# Запускаем асинхронную генерацию контента\n", indentStr))
		builder.WriteString(fmt.Sprintf("%s%s = generate_content(%s, '%s', '%s', %s, %.1f, %d, %s)\n",
			indentStr, datasetVar, datasetVar, n.SourceField, n.TargetField, modelStr, n.Temperature, n.Tokens, promptStr))

		// Обновляем датасет в словаре
		builder.WriteString(fmt.Sprintf("%sloaded_datasets['%s'] = %s\n", indentStr, datasetVar, datasetVar))

	case *SaveStatement:
		builder.WriteString(fmt.Sprintf("%s# Сохранение датасета в файл\n", indentStr))
		builder.WriteString(fmt.Sprintf("%soutput_file = '%s'\n", indentStr, n.Filename))
		builder.WriteString(fmt.Sprintf("%swas_saved = True\n", indentStr))
		builder.WriteString(fmt.Sprintf("%ssave_current_results()\n", indentStr))

	case *PromptStatement:
		builder.WriteString(fmt.Sprintf("%s# Определение шаблона промпта %s\n", indentStr, n.Name))

		// Определяем список полей для замены в шаблоне
		fieldsStr := "[]"
		if len(n.Fields) > 0 {
			fieldsStr = fmt.Sprintf("[%s]", strings.Join(formatPythonStringList(n.Fields), ", "))
		}

		// В зависимости от типа промпта сохраняем его в соответствующий словарь
		if n.PromptType == "system" {
			// Для системного промпта сохраняем только текст
			builder.WriteString(fmt.Sprintf("%ssystem_prompts['%s'] = '%s'\n", indentStr, n.Name, n.Template))

			if c.debug {
				builder.WriteString(fmt.Sprintf("%sif debug:\n", indentStr))
				builder.WriteString(fmt.Sprintf("%s    print(f'Определен системный промпт: %s')\n", indentStr, n.Name))
			}
		} else {
			// Для пользовательского промпта сохраняем шаблон в словарь
			builder.WriteString(fmt.Sprintf("%sprompt_templates['%s'] = {\n", indentStr, n.Name))
			builder.WriteString(fmt.Sprintf("%s    'template': '%s',\n", indentStr, n.Template))
			builder.WriteString(fmt.Sprintf("%s    'fields': %s\n", indentStr, fieldsStr))
			builder.WriteString(fmt.Sprintf("%s}\n", indentStr))

			if c.debug {
				builder.WriteString(fmt.Sprintf("%sif debug:\n", indentStr))
				builder.WriteString(fmt.Sprintf("%s    print(f'Определен шаблон промпта: %s')\n", indentStr, n.Name))
			}
		}
	}
}

// convertOperatorToPython преобразует оператор из DSL в Python-оператор
func convertOperatorToPython(op string) string {
	switch op {
	case "=":
		return "=="
	default:
		return op
	}
}

// formatPythonList форматирует список строк в Python-список
func formatPythonList(items []string) string {
	quotedItems := make([]string, len(items))
	for i, item := range items {
		quotedItems[i] = fmt.Sprintf("'%s'", item)
	}
	return fmt.Sprintf("[%s]", strings.Join(quotedItems, ", "))
}

// formatPythonValue форматирует значение для Python
func formatPythonValue(value interface{}) string {
	switch v := value.(type) {
	case string:
		return fmt.Sprintf("'%s'", v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// sanitizeVarName преобразует строку в допустимое имя переменной Python
func sanitizeVarName(name string) string {
	// Заменяем недопустимые символы на подчеркивание
	re := strings.NewReplacer("/", "_", "-", "_", ".", "_")
	return re.Replace(name)
}

// formatPythonStringList форматирует список строк для Python
func formatPythonStringList(items []string) []string {
	quotedItems := make([]string, len(items))
	for i, item := range items {
		quotedItems[i] = fmt.Sprintf("'%s'", item)
	}
	return quotedItems
}
