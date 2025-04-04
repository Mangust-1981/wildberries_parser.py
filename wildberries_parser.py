# Импортируем модули для работы с Selenium, обработки данных и логирования
# time для пауз между действиями, os для работы с файловой системой
# random для случайного выбора прокси и задержек, logging для записи логов
# selenium для управления браузером, pandas для сохранения данных в CSV
import time
import os
import random
import logging
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
from selenium.common.exceptions import TimeoutException

# Настраиваем логирование для отслеживания работы парсера
# Логи будут сохраняться в файл wildberries_parser.log
# Уровень INFO для записи основных событий, формат логов с датой и уровнем
logging.basicConfig(
    filename="/home/mangust1981/Документы/3Пайтон/wildberries_parser.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# === Настраиваемые параметры времени ожидания (в секундах) ===
# Тайминги для первой страницы и страниц после ротации прокси
# Задержки больше, чтобы гарантировать загрузку страницы
# Используются для прокрутки и перехода между страницами
FIRST_PAGE_SCROLL_DOWN_PAUSE = 1.3  # Пауза после прокрутки вниз
FIRST_PAGE_SCROLL_UP_PAUSE = 0.7  # Пауза после прокрутки вверх
FIRST_PAGE_SCROLL_DOWN_AGAIN_PAUSE = 1.3  # Пауза после повторной прокрутки вниз
FIRST_PAGE_PAGE_TRANSITION_PAUSE = 2.5  # Пауза после перехода на следующую страницу
FIRST_PAGE_PROXY_TEST_PAUSE = 3.0  # Пауза для проверки прокси

# Тайминги для последующих страниц: Блок 2 (базовые значения)
# Меньшие задержки, так как страницы загружаются быстрее
# Используются для прокрутки и перехода между страницами
# Блок 2 — первый набор таймингов для последующих страниц
SUBSEQUENT_PAGES_BLOCK_2_SCROLL_DOWN_PAUSE = 0.18  # Пауза после прокрутки вниз
SUBSEQUENT_PAGES_BLOCK_2_SCROLL_UP_PAUSE = 0.13  # Пауза после прокрутки вверх
SUBSEQUENT_PAGES_BLOCK_2_SCROLL_DOWN_AGAIN_PAUSE = 0.18  # Пауза после повторной прокрутки
SUBSEQUENT_PAGES_BLOCK_2_PAGE_TRANSITION_PAUSE = 0.23  # Пауза после перехода на страницу

# Тайминги для последующих страниц: Блок 3 (блок 2 + 0.01 секунды)
# Чуть больше, чем в блоке 2, для стабильности загрузки
# Используются для прокрутки и перехода между страницами
# Блок 3 — второй набор таймингов для последующих страниц
SUBSEQUENT_PAGES_BLOCK_3_SCROLL_DOWN_PAUSE = 0.19  # Пауза после прокрутки вниз
SUBSEQUENT_PAGES_BLOCK_3_SCROLL_UP_PAUSE = 0.14  # Пауза после прокрутки вверх
SUBSEQUENT_PAGES_BLOCK_3_SCROLL_DOWN_AGAIN_PAUSE = 0.19  # Пауза после повторной прокрутки
SUBSEQUENT_PAGES_BLOCK_3_PAGE_TRANSITION_PAUSE = 0.24  # Пауза после перехода на страницу

# Тайминги для последующих страниц: Блок 4 (блок 3 + 0.01 секунды)
# Ещё немного больше для дополнительной стабильности
# Используются для прокрутки и перехода между страницами
# Блок 4 — третий набор таймингов для последующих страниц
SUBSEQUENT_PAGES_BLOCK_4_SCROLL_DOWN_PAUSE = 0.20  # Пауза после прокрутки вниз
SUBSEQUENT_PAGES_BLOCK_4_SCROLL_UP_PAUSE = 0.15  # Пауза после прокрутки вверх
SUBSEQUENT_PAGES_BLOCK_4_SCROLL_DOWN_AGAIN_PAUSE = 0.20  # Пауза после повторной прокрутки
SUBSEQUENT_PAGES_BLOCK_4_PAGE_TRANSITION_PAUSE = 0.25  # Пауза после перехода на страницу

# Список прокси (только Европа и США) для ротации
# Каждый прокси в формате "http://ip:port"
# Бесплатные прокси из free-proxy-list.net, лучше заменить на платные
# Используются для обхода блокировок Wildberries
PROXY_LIST_EUROPE_USA = [
    "http://35.180.150.118:8888",
    "http://91.134.55.236:8080",
    "http://78.80.228.150:80",
    "http://62.210.15.199:80",
    "http://168.121.242.66:999",
    "http://198.41.205.155",
    "http://185.162.229.29",
    "http://141.101.123.15",
    "http://45.131.7.115",
    "http://162.159.251.36",
    "http://162.159.244.242",
    "http://162.159.242.165",
    "http://141.101.123.226",
    "http://162.159.242.43",
    "http://104.25.185.75",
    "http://31.44.91.218",
    "http://95.66.244.250",
    "http://62.182.204.81",
    "http://213.108.172.244",
    "http://62.205.169.74",
    "http://37.140.51.159",
    "http://176.119.16.40",
    "http://31.10.83.158",
    "http://188.191.165.159",
    "http://109.95.220.45",
    "http://95.66.138.21",
    "http://178.177.54.157",
    "http://46.47.197.210",
    "http://188.235.146.220",
    "http://77.238.103.98",
]

# Периодичность смены прокси (каждые 20 страниц)
# Используется для ротации прокси
# Помогает избежать блокировки Wildberries
# Значение можно настроить в зависимости от ситуации
PROXY_ROTATION_INTERVAL = 20

# Путь для сохранения CSV-файла с данными
# Абсолютный путь для точного сохранения
# Файл будет перезаписываться при каждом запуске
# Используется для сохранения результатов парсинга
SAVE_PATH = "/home/mangust1981/Документы/3Пайтон/wildberries_data.csv"

# Создаём множество для хранения нерабочих прокси
# Используется для исключения нерабочих прокси из списка
# Помогает избежать повторных попыток с нерабочими прокси
# Множество выбрано для быстрого поиска
BAD_PROXIES = set()

# Функция для проверки, работает ли прокси
# Проверяет доступность через тестовую страницу
# Принимает драйвер и время ожидания
# Возвращает True, если прокси работает, иначе False
def test_proxy(driver, proxy_test_pause):
    try:
        # Открываем тестовую страницу Google для проверки
        # Это простой способ проверить прокси
        # Используем Google, так как он всегда доступен
        # Проверяем, что страница загрузилась
        driver.get("https://www.google.com")
        # Ждем proxy_test_pause секунд, чтобы страница загрузилась
        # Используем WebDriverWait для ожидания элемента
        # Проверяем наличие тега body на странице
        # Если элемент найден, прокси работает
        WebDriverWait(driver, proxy_test_pause).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        # Логируем успешную проверку прокси
        # Сообщение будет записано в лог-файл
        # Указываем, что прокси работает
        # Помогает отслеживать работу прокси
        logging.info("Прокси работает")
        return True
    except Exception as e:
        # Если страница не загрузилась, прокси не работает
        # Логируем ошибку с описанием
        # Указываем причину сбоя прокси
        # Помогает в отладке проблем с прокси
        logging.error(f"Прокси не работает: {e}")
        return False

# Функция для настройки Selenium с прокси и headless-режимом для Firefox
# Принимает прокси в качестве аргумента
# Настраивает браузер для парсинга
# Возвращает драйвер или None при ошибке
def setup_driver(proxy):
    # Создаем объект опций для Firefox
    # Опции позволяют настроить поведение браузера
    # Используем для headless-режима и прокси
    # Настраиваем браузер для парсинга
    firefox_options = Options()
    # Включаем headless-режим (без графического интерфейса)
    # Позволяет запускать браузер в фоновом режиме
    # Снижает нагрузку на систему
    # Не открывает окно браузера
    firefox_options.add_argument("--headless")
    # Добавляем User-Agent для имитации реального браузера
    # Эмулируем Chrome на Windows
    # Снижает вероятность блокировки Wildberries
    # Делает запросы более похожими на человеческие
    firefox_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " \
        "(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    )
    # Если прокси передан, добавляем его в настройки
    # Прокси используется для обхода блокировок
    # Указываем прокси в формате --proxy-server
    # Позволяет скрыть реальный IP-адрес
    if proxy:
        firefox_options.add_argument(f"--proxy-server={proxy}")
    try:
        # Создаем драйвер Firefox с заданными опциями
        # Драйвер управляет браузером
        # Используется для парсинга страниц
        # Запускаем браузер в фоновом режиме
        driver = webdriver.Firefox(options=firefox_options)
        # Логируем успешное создание драйвера
        # Указываем, с каким прокси создан драйвер
        # Сообщение записывается в лог-файл
        # Помогает отслеживать работу драйвера
        logging.info(f"Драйвер успешно создан с прокси: {proxy}")
        return driver
    except Exception as e:
        # Если драйвер не удалось создать, логируем ошибку
        # Указываем причину сбоя
        # Сообщение записывается в лог-файл
        # Помогает в отладке проблем с драйвером
        logging.error(f"Ошибка при создания драйвера с прокси {proxy}: {e}")
        return None

# Функция для выбора рабочего прокси из списка
# Проверяет доступные прокси
# Возвращает драйвер с рабочим прокси
# Если прокси не найдены, возвращает None
def get_working_driver(proxy_list):
    # Проверяем, есть ли прокси в списке
    # Если список пуст, работаем без прокси
    # Логируем отсутствие прокси
    # Создаем драйвер без прокси
    if not proxy_list:
        print("Список прокси пуст, работаем без прокси")
        logging.info("Список прокси пуст, работаем без прокси")
        return setup_driver(None)

    # Фильтруем список прокси, исключая нерабочие
    # Используем list comprehension для фильтрации
    # Проверяем, есть ли прокси в BAD_PROXIES
    # Создаем новый список только с рабочими прокси
    available_proxies = [p for p in proxy_list if p not in BAD_PROXIES]
    # Если нет доступных прокси, завершаем работу
    # Логируем отсутствие рабочих прокси
    # Сообщение записывается в лог-файл
    # Возвращаем None, чтобы завершить парсинг
    if not available_proxies:
        logging.error("Все прокси в списке не работают, завершаем работу")
        return None

    # Случайно выбираем прокси из доступных
    # Используем random.choice для выбора
    # Случайный выбор снижает нагрузку на один прокси
    # Помогает избежать блокировки прокси
    proxy = random.choice(available_proxies)
    # Создаем драйвер с выбранным прокси
    # Используем функцию setup_driver
    # Драйвер будет настроен с прокси
    # Если драйвер не создан, добавляем прокси в BAD_PROXIES
    driver = setup_driver(proxy)
    if not driver:
        BAD_PROXIES.add(proxy)
        logging.warning(f"Прокси {proxy} не работает, добавлен в список нерабочих")
        return None

    # Выводим сообщение о проверке прокси
    # Сообщение отображается в терминале
    # Указываем, какой прокси проверяется
    # Помогает отслеживать процесс
    print(f"Проверяем прокси: {proxy}")
    # Проверяем прокси с помощью функции test_proxy
    # Если прокси работает, используем его
    # Если нет, добавляем в BAD_PROXIES
    # Возвращаем драйвер или None
    if test_proxy(driver, FIRST_PAGE_PROXY_TEST_PAUSE):
        print(f"Используем прокси: {proxy}")
        return driver

    # Закрываем драйвер, если прокси не работает
    # Освобождаем ресурсы
    # Добавляем прокси в список нерабочих
    # Логируем проблему с прокси
    driver.quit()
    BAD_PROXIES.add(proxy)
    logging.warning(f"Прокси {proxy} не работает, добавлен в список нерабочих")
    return None

# Функция для ротации прокси
# Закрывает текущий драйвер и создает новый
# Принимает драйвер, номер страницы и максимальное количество попыток
# Возвращает новый драйвер или вызывает исключение
def rotate_proxy(driver, page_number, max_attempts=3):
    # Закрываем текущий драйвер
    # Освобождаем ресурсы
    # Подготавливаем к созданию нового драйвера
    # Логируем ротацию прокси
    driver.quit()
    logging.info(f"Ротация прокси на странице {page_number}")

    # Счетчик попыток для поиска рабочего прокси
    # Пробуем max_attempts раз
    # Если не удалось, вызываем исключение
    # Логируем каждую попытку
    attempts = 0
    while attempts < max_attempts:
        # Пытаемся создать новый драйвер с рабочим прокси
        # Используем get_working_driver
        # Если драйвер создан, возвращаем его
        # Если нет, увеличиваем счетчик попыток
        new_driver = get_working_driver(PROXY_LIST_EUROPE_USA)
        if new_driver:
            return new_driver
        attempts += 1
        logging.warning(f"Попытка {attempts}/{max_attempts} найти рабочий прокси не удалась")

    # Если рабочий прокси не найден, логируем ошибку
    # Указываем, что превышено количество попыток
    # Сообщение записывается в лог-файл
    # Вызываем исключение для завершения работы
    logging.error(f"Не удалось найти рабочий прокси после {max_attempts} попыток, " \
                  "завершаем работу")
    raise Exception("Не удалось найти рабочий прокси после максимального количества попыток")

# Функции для очистки данных перед сохранением
# Очищаем цену, оставляя только цифры
# Принимает текст цены
# Возвращает очищенную цену или "0"
def clean_price(price_text):
    # Оставляем только цифры в цене
    # Используем filter для удаления нечисловых символов
    # Преобразуем в строку
    # Если цена пустая, возвращаем "0"
    cleaned = ''.join(filter(str.isdigit, price_text))
    return cleaned if cleaned else "0"

# Очищаем название, удаляя слэши и пробелы
# Принимает текст названия
# Удаляет символы "/" и пробелы в начале/конце
# Возвращает очищенное название
def clean_name(name_text):
    # Удаляем слэши из названия
    # Удаляем пробелы в начале и конце
    # Используем replace и strip
    # Возвращаем очищенное название
    return name_text.replace("/", "").strip()

# Очищаем рейтинг, оставляя цифры и точки
# Принимает текст рейтинга
# Удаляет все символы, кроме цифр и точек
# Возвращает очищенный рейтинг или "0"
def clean_rating(rating_text):
    # Оставляем только цифры и точки
    # Используем генератор списка
    # Преобразуем в строку
    # Если рейтинг пустой, возвращаем "0"
    cleaned = ''.join(c for c in rating_text if c.isdigit() or c == '.')
    return cleaned if cleaned else "0"

# Очищаем скидку, оставляя только цифры
# Принимает текст скидки
# Удаляет все символы, кроме цифр
# Возвращает очищенную скидку или "0"
def clean_discount(discount_text):
    # Оставляем только цифры в скидке
    # Используем filter для удаления нечисловых символов
    # Преобразуем в строку
    # Если скидка пустая, возвращаем "0"
    cleaned = ''.join(filter(str.isdigit, discount_text))
    return cleaned if cleaned else "0"

# Основная функция парсинга Wildberries
# Принимает URL категории для парсинга
# Собирает данные о товарах
# Возвращает список товаров
def parse_wildberries(category_url):
    # Создаем драйвер с рабочим прокси
    # Используем get_working_driver
    # Если драйвер не создан, завершаем работу
    # Логируем ошибку при создании драйвера
    driver = get_working_driver(PROXY_LIST_EUROPE_USA)
    if not driver:
        logging.error("Не удалось создать драйвер, завершаем работу")
        return []

    # Открываем начальную страницу категории
    # Используем driver.get для загрузки URL
    # Логируем открытие страницы
    # Указываем, какая страница открыта
    driver.get(category_url)
    logging.info(f"Открыта страница: {category_url}")

    # Создаем список для хранения всех товаров
    # Список будет содержать словари с данными
    # Инициализируем пустой список
    # Используется для накопления данных
    all_products = []
    # Счетчик текущей страницы
    # Начинаем с первой страницы
    # Увеличиваем на 1 после каждой страницы
    # Используется для пагинации
    page_number = 1
    # Максимальное количество попыток загрузки страницы
    # Если страница не загрузилась, пробуем max_retries раз
    # Используется для обработки ошибок
    # Значение можно настроить
    max_retries = 5
    # Максимальное количество циклов ротации прокси
    # Если прокси не работает, пробуем max_rotation_cycles раз
    # Увеличено до 5 (пункт 3)
    # Помогает обойти блокировки
    max_rotation_cycles = 5

    # Основной цикл парсинга страниц
    # Продолжаем, пока есть страницы
    # Обрабатываем каждую страницу
    # Собираем данные о товарах
    while True:
        # Проверяем, нужно ли сменить прокси (каждые 20 страниц)
        # Если страница кратна PROXY_ROTATION_INTERVAL, ротируем прокси
        # Используем rotate_proxy для смены прокси
        # Переоткрываем страницу после ротации
        if page_number > 1 and (page_number - 1) % PROXY_ROTATION_INTERVAL == 0:
            driver = rotate_proxy(driver, page_number)
            driver.get(category_url + f"&page={page_number}")
            logging.info(f"Переоткрыта страница {page_number} после ротации прокси")

        # Выбираем тайминги в зависимости от номера страницы
        # Если это первая страница или страница после ротации
        # Используем более длинные тайминги
        # Гарантируем загрузку страницы
        if page_number == 1 or (page_number - 1) % PROXY_ROTATION_INTERVAL == 0:
            scroll_down_pause = FIRST_PAGE_SCROLL_DOWN_PAUSE
            scroll_up_pause = FIRST_PAGE_SCROLL_UP_PAUSE
            scroll_down_again_pause = FIRST_PAGE_SCROLL_DOWN_AGAIN_PAUSE
            page_transition_pause = FIRST_PAGE_PAGE_TRANSITION_PAUSE
            logging.info(f"Страница {page_number}: применяем тайминги первого блока " \
                         "(после ротации)")
        else:
            # Выбираем блок таймингов для последующих страниц
            # Используем остаток от деления для выбора блока
            # Блоки чередуются для разнообразия
            # Помогает избежать блокировки
            block_index = (page_number - 2) % 3
            if block_index == 0:
                scroll_down_pause = SUBSEQUENT_PAGES_BLOCK_2_SCROLL_DOWN_PAUSE
                scroll_up_pause = SUBSEQUENT_PAGES_BLOCK_2_SCROLL_UP_PAUSE
                scroll_down_again_pause = SUBSEQUENT_PAGES_BLOCK_2_SCROLL_DOWN_AGAIN_PAUSE
                page_transition_pause = SUBSEQUENT_PAGES_BLOCK_2_PAGE_TRANSITION_PAUSE
            elif block_index == 1:
                scroll_down_pause = SUBSEQUENT_PAGES_BLOCK_3_SCROLL_DOWN_PAUSE
                scroll_up_pause = SUBSEQUENT_PAGES_BLOCK_3_SCROLL_UP_PAUSE
                scroll_down_again_pause = SUBSEQUENT_PAGES_BLOCK_3_SCROLL_DOWN_AGAIN_PAUSE
                page_transition_pause = SUBSEQUENT_PAGES_BLOCK_3_PAGE_TRANSITION_PAUSE
            else:
                scroll_down_pause = SUBSEQUENT_PAGES_BLOCK_4_SCROLL_DOWN_PAUSE
                scroll_up_pause = SUBSEQUENT_PAGES_BLOCK_4_SCROLL_UP_PAUSE
                scroll_down_again_pause = SUBSEQUENT_PAGES_BLOCK_4_SCROLL_DOWN_AGAIN_PAUSE
                page_transition_pause = SUBSEQUENT_PAGES_BLOCK_4_PAGE_TRANSITION_PAUSE
            logging.info(f"Страница {page_number}: применяем тайминги блока {block_index + 2}")

        # Прокручиваем страницу, чтобы подгрузить все товары
        # Максимальное количество попыток прокрутки
        # Прокручиваем, пока не подгрузятся все товары
        # Используем JavaScript для прокрутки
        max_scroll_attempts = 10
        # Счетчик попыток прокрутки
        # Начинаем с 0
        # Увеличиваем на 1 после каждой попытки
        # Ограничиваем max_scroll_attempts
        scroll_attempts = 0
        # Получаем текущую высоту страницы
        # Используем JavaScript для получения высоты
        # Сравниваем с новой высотой после прокрутки
        # Определяем, подгрузились ли новые товары
        last_height = driver.execute_script("return document.body.scrollHeight")

        # Цикл прокрутки страницы
        # Прокручиваем вниз, вверх и снова вниз
        # Ждем подгрузки товаров
        # Проверяем, изменилась ли высота страницы
        while scroll_attempts < max_scroll_attempts:
            # Прокручиваем страницу вниз
            # Используем JavaScript для прокрутки
            # Прокручиваем до конца страницы
            # Подгружаем новые товары
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(scroll_down_pause)
            # Прокручиваем страницу вверх
            # Возвращаемся в начало страницы
            # Используем JavaScript для прокрутки
            # Помогает стабилизировать загрузку
            driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(scroll_up_pause)
            # Прокручиваем страницу вниз снова
            # Повторная прокрутка для подгрузки
            # Используем JavaScript для прокрутки
            # Гарантируем загрузку всех товаров
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(scroll_down_again_pause)
            # Получаем новую высоту страницы
            # Используем JavaScript для получения высоты
            # Сравниваем с предыдущей высотой
            # Проверяем, подгрузились ли новые товары
            new_height = driver.execute_script("return document.body.scrollHeight")
            # Находим все элементы товаров на странице
            # Используем класс product-card__wrapper
            # Сохраняем в список items
            # Проверяем количество найденных товаров
            items = driver.find_elements(By.CLASS_NAME, "product-card__wrapper")
            # Логируем количество найденных товаров
            # Указываем номер страницы и попытку прокрутки
            # Сообщение записывается в лог-файл
            # Выводим в терминал для отслеживания
            logging.info(f"Страница {page_number}, Прокрутка {scroll_attempts + 1}: " \
                         f"найдено {len(items)} товаров")
            print(f"Страница {page_number}, Прокрутка {scroll_attempts + 1}: " \
                  f"найдено {len(items)} товаров")
            # Проверяем, изменилась ли высота страницы
            # Если высота не изменилась, новых товаров нет
            # Завершаем прокрутку
            # Логируем завершение прокрутки
            if new_height == last_height:
                logging.info(f"Страница {page_number}: новых товаров не подгрузилось, " \
                             "завершаем прокрутку")
                break
            last_height = new_height
            scroll_attempts += 1

        # Проверяем, найдены ли товары на странице
        # Используем класс product-card__wrapper
        # Сохраняем в список items
        # Проверяем, есть ли товары
        items = driver.find_elements(By.CLASS_NAME, "product-card__wrapper")
        # Если товаров нет, пробуем сменить прокси
        # Логируем отсутствие товаров
        # Ротируем прокси с помощью rotate_proxy
        # Переоткрываем страницу после ротации
        if len(items) == 0:
            logging.warning(f"На странице {page_number} не найдено товаров после всех " \
                            "прокруток, пробуем сменить прокси")
            driver = rotate_proxy(driver, page_number)
            driver.get(category_url + f"&page={page_number}")
            logging.info(f"Переоткрыта страница {page_number} после принудительной " \
                         "ротации прокси")
            # Сбрасываем счетчик попыток прокрутки
            # Начинаем прокрутку заново
            # Используем тайминги первой страницы
            # Прокручиваем страницу после ротации
            scroll_attempts = 0
            last_height = driver.execute_script("return document.body.scrollHeight")
            # Цикл прокрутки после ротации прокси
            # Прокручиваем вниз, вверх и снова вниз
            # Ждем подгрузки товаров
            # Проверяем, изменилась ли высота страницы
            while scroll_attempts < max_scroll_attempts:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(FIRST_PAGE_SCROLL_DOWN_PAUSE)
                driver.execute_script("window.scrollTo(0, 0);")
                time.sleep(FIRST_PAGE_SCROLL_UP_PAUSE)
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(FIRST_PAGE_SCROLL_DOWN_AGAIN_PAUSE)
                new_height = driver.execute_script("return document.body.scrollHeight")
                items = driver.find_elements(By.CLASS_NAME, "product-card__wrapper")
                logging.info(f"Страница {page_number}, Прокрутка {scroll_attempts + 1} " \
                             f"(после ротации): найдено {len(items)} товаров")
                print(f"Страница {page_number}, Прокрутка {scroll_attempts + 1} " \
                      f"(после ротации): найдено {len(items)} товаров")
                if new_height == last_height:
                    logging.info(f"Страница {page_number}: новых товаров не подгрузилось " \
                                 "после ротации, завершаем прокрутку")
                    break
                last_height = new_height
                scroll_attempts += 1

        # Ждем, пока элементы товаров появятся (с обработкой ошибок)
        # Счетчик циклов ротации прокси
        # Пробуем max_rotation_cycles раз
        # Если не удалось, пропускаем страницу
        rotation_cycles = 0
        while rotation_cycles < max_rotation_cycles:
            # Счетчик попыток загрузки страницы
            # Пробуем max_retries раз
            # Если не удалось, ротируем прокси
            # Сбрасываем счетчик после ротации
            retry_count = 0
            while retry_count < max_retries:
                try:
                    # Ждем появления элементов товаров
                    # Используем WebDriverWait для ожидания
                    # Ищем элементы по классу product-card__wrapper
                    # Ожидаем 10 секунд
                    items = WebDriverWait(driver, 10).until(
                        EC.presence_of_all_elements_located(
                            (By.CLASS_NAME, "product-card__wrapper")
                        )
                    )
                    break
                except TimeoutException as e:
                    # Увеличиваем счетчик попыток
                    # Логируем ошибку загрузки
                    # Указываем номер попытки
                    # Если попытки закончились, ротируем прокси
                    retry_count += 1
                    logging.warning(f"Ошибка загрузки товаров на странице {page_number}, " \
                                    f"попытка {retry_count}/{max_retries}: {e}")
                    if retry_count == max_retries:
                        rotation_cycles += 1
                        if rotation_cycles == max_rotation_cycles:
                            # Если превышено количество циклов, пропускаем страницу
                            # Логируем пропуск страницы (пункт 5)
                            # Переходим на следующую страницу
                            # Сбрасываем счетчик ротации
                            logging.warning(f"Не удалось загрузить товары на странице " \
                                            f"{page_number} после {max_rotation_cycles} " \
                                            "циклов ротации, пропускаем страницу (пункт 5)")
                            page_number += 1
                            driver.get(category_url + f"&page={page_number}")
                            logging.info(f"Переходим на страницу {page_number} после пропуска")
                            rotation_cycles = 0
                            break
                        logging.error(f"Не удалось загрузить товары на странице {page_number} " \
                                      f"после {max_retries} попыток, пробуем сменить прокси " \
                                      f"(цикл {rotation_cycles}/{max_rotation_cycles})")
                        driver = rotate_proxy(driver, page_number)
                        driver.get(category_url + f"&page={page_number}")
                        logging.info(f"Переоткрыта страница {page_number} после " \
                                     f"{max_retries} неудачных попыток загрузки")
                        retry_count = 0
                        continue
                    driver.refresh()
                    time.sleep(page_transition_pause)
            if retry_count < max_retries:
                break

        # Логируем общее количество найденных товаров на странице
        # Указываем номер страницы
        # Сообщение записывается в лог-файл
        # Выводим в терминал для отслеживания
        logging.info(f"Страница {page_number}: всего найдено {len(items)} товаров")
        print(f"Страница {page_number}: всего найдено {len(items)} товаров")

        # Проходим по каждому товару и собираем данные
        # Обрабатываем каждый элемент в списке items
        # Извлекаем название, цену, скидку, рейтинг, ссылку
        # Добавляем данные в all_products
        for item in items:
            try:
                # Извлекаем название товара
                # Используем класс product-card__name
                # Удаляем пробелы в начале и конце
                # Проверяем, что название не пустое
                name = item.find_element(By.CLASS_NAME, "product-card__name").text.strip()
                if not name:
                    continue
                cleaned_name = clean_name(name)
                # Извлекаем цену товара
                # Используем класс price__lower-price
                # Удаляем пробелы в начале и конце
                # Очищаем цену с помощью clean_price
                price = item.find_element(By.CLASS_NAME, "price__lower-price").text.strip()
                cleaned_price = clean_price(price)
                try:
                    # Извлекаем скидку товара
                    # Используем класс discount
                    # Удаляем пробелы в начале и конце
                    # Очищаем скидку с помощью clean_discount
                    discount = item.find_element(By.CLASS_NAME, "discount").text.strip()
                    cleaned_discount = clean_discount(discount)
                except:
                    cleaned_discount = "0"
                try:
                    # Извлекаем рейтинг товара
                    # Используем класс product-card__rating
                    # Удаляем пробелы в начале и конце
                    # Очищаем рейтинг с помощью clean_rating
                    rating = item.find_element(By.CLASS_NAME, "product-card__rating").text.strip()
                    cleaned_rating = clean_rating(rating)
                except:
                    cleaned_rating = "0"
                try:
                    # Извлекаем ссылку на товар
                    # Используем класс product-card__link
                    # Получаем атрибут href
                    # Сохраняем ссылку
                    link = item.find_element(By.CLASS_NAME, "product-card__link").get_attribute("href")
                except:
                    link = ""
                # Добавляем данные о товаре в список
                # Создаем словарь с данными
                # Включаем название, цену, скидку, рейтинг, ссылку
                # Добавляем в all_products
                all_products.append({
                    "Название": cleaned_name,
                    "Цена": cleaned_price,
                    "Скидка (%)": cleaned_discount,
                    "Рейтинг": cleaned_rating,
                    "Ссылка": link
                })
                # Логируем успешный парсинг товара
                # Указываем название и цену
                # Сообщение записывается в лог-файл
                # Помогает отслеживать прогресс
                logging.info(f"Спарсен товар: {cleaned_name}, Цена: {cleaned_price}")
            except Exception as e:
                # Логируем ошибку при парсинге товара
                # Указываем причину ошибки
                # Сообщение записывается в лог-файл
                # Продолжаем парсинг следующего товара
                logging.error(f"Ошибка при парсинге товара: {e}")
                continue

        # Сохраняем промежуточные результаты каждые 10 страниц
        # Проверяем, кратна ли страница 10
        # Вызываем save_to_csv для сохранения
        # Логируем промежуточное сохранение
        if page_number % 10 == 0:
            save_to_csv(all_products)
            logging.info(f"Промежуточное сохранение на странице {page_number}")

        # Проверяем, есть ли кнопка "Следующая страница"
        # Максимальное количество попыток поиска кнопки
        # Пробуем pagination_retries раз
        # Если кнопка не найдена, завершаем парсинг
        pagination_retries = 3
        for attempt in range(pagination_retries):
            try:
                # Ждем появления кнопки "Следующая страница"
                # Используем WebDriverWait для ожидания
                # Ищем кнопку по классу pagination-next
                # Ожидаем 10 секунд
                next_button = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "pagination-next"))
                )
                # Проверяем, активна ли кнопка
                # Если кнопка неактивна, завершаем парсинг
                # Логируем завершение
                # Сохраняем данные и возвращаем результат
                if "disabled" in next_button.get_attribute("class"):
                    logging.info("Кнопка 'Следующая страница' неактивна, завершаем")
                    driver.quit()
                    save_to_csv(all_products)
                    return all_products
                # Нажимаем на кнопку "Следующая страница"
                # Переходим на следующую страницу
                # Добавляем случайную задержку (пункт 4)
                # Снижаем риск блокировки
                next_button.click()
                time.sleep(page_transition_pause + random.uniform(1, 3))
                page_number += 1
                break
            except Exception as e:
                # Логируем ошибку поиска кнопки
                # Указываем номер попытки
                # Если попытки закончились, завершаем парсинг
                # Сохраняем данные и возвращаем результат
                logging.warning(f"Не удалось найти кнопку пагинации на странице " \
                                f"{page_number}, попытка {attempt + 1}/{pagination_retries}: {e}")
                if attempt == pagination_retries - 1:
                    logging.info(f"Пагинация не найдена после {pagination_retries} попыток, " \
                                 "завершаем сбор товаров")
                    driver.quit()
                    save_to_csv(all_products)
                    return all_products
                driver.refresh()
                time.sleep(page_transition_pause)

    # Логируем общее количество собранных товаров
    # Указываем, сколько товаров собрано
    # Сообщение записывается в лог-файл
    # Выводим в терминал для отслеживания
    logging.info(f"Всего собрано товаров: {len(all_products)}")
    print(f"Всего собрано товаров: {len(all_products)}")
    # Закрываем драйвер
    # Освобождаем ресурсы
    # Логируем завершение парсинга
    # Возвращаем список товаров
    driver.quit()
    logging.info("Парсинг завершен")
    return all_products

# Функция для сохранения данных в CSV
# Принимает список товаров и путь к файлу
# Сохраняет данные в формате CSV
# Использует pandas для создания DataFrame
def save_to_csv(data, filename=SAVE_PATH):
    # Создаем DataFrame из списка товаров
    # Используем pandas для работы с данными
    # Преобразуем список словарей в таблицу
    # Сохраняем в CSV
    df = pd.DataFrame(data)
    # Сохраняем DataFrame в CSV-файл
    # Используем кодировку utf-8-sig для корректного отображения
    # Разделитель — точка с запятой
    # Не сохраняем индексы
    df.to_csv(filename, index=False, encoding="utf-8-sig", sep=";")
    # Логируем успешное сохранение
    # Указываем путь к файлу
    # Сообщение записывается в лог-файл
    # Выводим в терминал для отслеживания
    logging.info(f"Данные сохранены в {filename}")
    print(f"Данные сохранены в {filename}")

# Точка входа в программу
# Задаем URL категории для парсинга
# Запускаем парсинг
# Сохраняем результаты
if __name__ == "__main__":
    # URL категории Wildberries для парсинга
    # Указываем категорию женских юбок
    # Сортировка по популярности, фильтр f57021=94964
    # Начинаем с первой страницы
    url = "https://www.wildberries.ru/catalog/zhenshchinam/odezhda/yubki?" \
          "sort=popular&page=1&f57021=94964"
    # Запускаем парсинг
    # Вызываем parse_wildberries
    # Получаем список товаров
    # Сохраняем результаты
    products = parse_wildberries(url)
    save_to_csv(products)
