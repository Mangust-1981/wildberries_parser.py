# Wildberries Parser
- Этот проект представляет собой парсер для сайта Wildberries, который собирает данные о товарах (название, цена, скидка, рейтинг, ссылка) из заданной категории и сохраняет их в CSV-файл. Парсер использует Selenium для автоматизации браузера и ротацию прокси для обхода блокировок, что делает его устойчивым к ограничениям Wildberries.

## Пример результата
- ![Screenshot_20250405_000654](https://github.com/user-attachments/assets/5d52a494-fcf7-42cf-b9a0-25f4e01756be)
- CSV-файл
- Лог-файл

## Возможности
- Парсит товары из заданной категории Wildberries (например, женские юбки).
- Собирает данные: название, цена, скидка (%), рейтинг, ссылка на товар.
- Использует ротацию прокси для обхода блокировок (поддерживает HTTP-прокси).
- Добавлены случайные задержки между запросами (1–3 секунды) для снижения риска бана.
- Пропускает страницы, которые не удалось загрузить, чтобы продолжить парсинг.
- Сохраняет данные в CSV с промежуточным сохранением каждые 10 страниц.
- Обрабатывает до 106+ страниц (10 000+ товаров) за один запуск.
- Логирует весь процесс для удобной отладки.

## Требования
- Python 3.6 или выше.
- Библиотеки: `selenium`, `pandas`.
- Установленный Firefox и geckodriver (должен быть в `/usr/local/bin`).
- Список рабочих HTTP-прокси (указывается в коде в `PROXY_LIST_EUROPE_USA`).

## Установка
- Убедитесь, что Python установлен:
  python3 --version
- Установите необходимые библиотеки с помощью pip:
  pip3 install selenium pandas
- Установите Firefox и geckodriver:
  Для Ubuntu выполните команды:
  sudo apt-get install firefox
  wget https://github.com/mozilla/geckodriver/releases/download/v0.34.0/geckodriver-v0.34.0-linux64.tar.gz
  tar -xzf geckodriver-v0.34.0-linux64.tar.gz
  sudo mv geckodriver /usr/local/bin/
- Склонируйте репозиторий или скачайте файл `wildberries_parser.py`:
  git clone https://github.com/1Mangust1981/wildberries-parser.git
- (Опционально) Настройте прокси:
  Добавьте свои прокси в список `PROXY_LIST_EUROPE_USA` в коде. Рекомендуется использовать платные прокси (например, Smartproxy, Oxylabs) для большей надёжности.

## Использование
- Перейдите в папку с проектом: `cd путь_к_папке_с_файлом`
- Запустите скрипт: python3 wildberries_parser.py
- Результаты:
- Данные сохранятся в `cd путь_к_папке_с_файлом/wildberries_data.csv`.
- Логи работы парсера будут в `cd путь_к_папке_с_файлом/wildberries_parser.log`.
- Пример парсинга:
- Парсер настроен для категории женских юбок с фильтром `f57021=94964` и сортировкой по популярности. Чтобы парсить другую категорию, измените `url` в коде: python
- url = "https://www.wildberries.ru/catalog/<ваша_категория>?sort=popular&page=1"

## Структура проекта
- wildberries_parser.py — основной скрипт парсера.
- wildberries_data.csv — файл с результатами парсинга (создаётся автоматически).
- wildberries_parser.log — файл логов для отладки.
- README.md — описание проекта.
- .gitignore — файл для исключения ненужных файлов при загрузке в GitHub
- LICENSE — лицензия проекта.

## Автор
- Mangust-1981

## Лицензия
- MIT. Подробности в `LICENSE`.
