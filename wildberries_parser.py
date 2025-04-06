# Блок 1: Импорт библиотек и настройка логирования
import time
import os
import random
import logging
import itertools
import requests
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    WebDriverException,
    ElementClickInterceptedException,
)
from urllib3.exceptions import NewConnectionError

# Настройка логирования для записи действий только в файл
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("/home/mangust1981/Документы/3Пайтон/wildberries_parser.log"),
        # logging.StreamHandler() убран, чтобы логи не шли в терминал
    ],
)

# Блок 2: Определение таймингов для парсинга
# Тайминги для первой страницы (в секундах)
FIRST_PAGE_SCROLL_DOWN_PAUSE = 1.3
FIRST_PAGE_SCROLL_UP_PAUSE = 0.7
FIRST_PAGE_SCROLL_DOWN_AGAIN_PAUSE = 1.3
FIRST_PAGE_PAGE_TRANSITION_PAUSE = 6.0
FIRST_PAGE_PROXY_TEST_PAUSE = 5.0

# Тайминги для последующих страниц (блок 2, в секундах)
SUBSEQUENT_PAGES_BLOCK_2_SCROLL_DOWN_PAUSE = 0.18
SUBSEQUENT_PAGES_BLOCK_2_SCROLL_UP_PAUSE = 0.13
SUBSEQUENT_PAGES_BLOCK_2_SCROLL_DOWN_AGAIN_PAUSE = 0.18
SUBSEQUENT_PAGES_BLOCK_2_PAGE_TRANSITION_PAUSE = 6.0

# Тайминги для последующих страниц (блок 3, в секундах)
SUBSEQUENT_PAGES_BLOCK_3_SCROLL_DOWN_PAUSE = 0.19
SUBSEQUENT_PAGES_BLOCK_3_SCROLL_UP_PAUSE = 0.14
SUBSEQUENT_PAGES_BLOCK_3_SCROLL_DOWN_AGAIN_PAUSE = 0.19
SUBSEQUENT_PAGES_BLOCK_3_PAGE_TRANSITION_PAUSE = 6.0

# Тайминги для последующих страниц (блок 4, в секундах)
SUBSEQUENT_PAGES_BLOCK_4_SCROLL_DOWN_PAUSE = 0.20
SUBSEQUENT_PAGES_BLOCK_4_SCROLL_UP_PAUSE = 0.15
SUBSEQUENT_PAGES_BLOCK_4_SCROLL_DOWN_AGAIN_PAUSE = 0.20
SUBSEQUENT_PAGES_BLOCK_4_PAGE_TRANSITION_PAUSE = 6.0

# Блок 3: Константы и начальные данные
# Список User-Agent'ов для имитации браузеров
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) \
        Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like \
        Gecko) Version/14.0.3 Safari/605.1.15",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, \
        like Gecko) Version/14.1.1 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (Linux; Android 10; SM-G975F) AppleWebKit/537.36 (KHTML, like Gecko) \
        Chrome/91.0.4472.114 Mobile Safari/537.36",
]

# Константы для работы парсера
PROXY_ROTATION_INTERVAL = 20
DRIVER_RESTART_INTERVAL = 20  # Перезапуск драйвера каждые 20 страниц
CLICK_PAGES = sorted(random.sample(range(1, 21), 7))
SAVE_PATH = "/home/mangust1981/Документы/3Пайтон/wildberries_data.csv"
MAX_PRODUCTS = 10540  # Обновлено количество товаров
PAUSE_INCREASE_FACTOR = 1.2
BAD_PROXIES = set()
USER_AGENT_CYCLE = itertools.cycle(USER_AGENTS)
SCREEN_RESOLUTIONS = [(1920, 1080), (1366, 768), (1440, 900)]
start_time = datetime.now()

# Интервалы для новых действий
SITE_EXIT_INTERVAL = 15  # Уход с сайта каждые 15 страниц
SITE_EXIT_DURATION = 90  # Пауза 1.5 минуты (90 секунд)
IDLE_INTERVAL = 7  # Зависание каждые 7 страниц
IDLE_DURATION = 30  # Пауза 30 секунд
DUPLICATE_PAGE_THRESHOLD = 2  # Количество подряд идущих страниц с дубликатами
DUPLICATE_EXIT_DURATION = 180  # Пауза 3 минуты (180 секунд) при дубликатах

# Блок 4: Функции для сбора и проверки прокси
# Функция для сбора прокси с онлайн-ресурсов
def fetch_proxies():
    proxies = []
    # Пробуем ProxyScrape.com
    try:
        url = "https://api.proxyscrape.com/v2/?request=getproxies&protocol=http&timeout=1000\
            &country=All&ssl=all&anonymity=elite"
        response = requests.get(url, timeout=10)
        proxies.extend(response.text.splitlines())
        logging.info(f"Собрано {len(proxies)} прокси с ProxyScrape.com")
    except Exception as e:
        logging.error(f"Ошибка при сборе прокси с ProxyScrape.com: {e}")

    # Пробуем Htmlweb.ru (без API-ключа для простоты, ограниченный доступ)
    try:
        url = "http://htmlweb.ru/json/proxy/get?perpage=50"
        response = requests.get(url, timeout=10)
        data = response.json()
        htmlweb_proxies = [f"http://{proxy['ip']}:{proxy['port']}" for proxy in \
            data.get('rows', [])]
        proxies.extend(htmlweb_proxies)
        logging.info(f"Собрано {len(htmlweb_proxies)} прокси с Htmlweb.ru")
    except Exception as e:
        logging.error(f"Ошибка при сборе прокси с Htmlweb.ru: {e}")

    return list(set(proxies))  # Удаляем дубликаты

# Функция для проверки прокси на работоспособность
def test_proxy(driver, proxy_test_pause):
    # Проверка на Google
    try:
        driver.get("https://www.google.com")
        WebDriverWait(driver, proxy_test_pause).until(EC.presence_of_element_located(\
            (By.TAG_NAME, "body")))
        logging.info("Прокси работает с Google")
    except Exception as e:
        logging.error(f"Прокси не работает с Google: {e}")
        return False

    # Проверка на Wildberries
    try:
        driver.get("https://www.wildberries.ru/")
        WebDriverWait(driver, proxy_test_pause).until(EC.presence_of_element_located(\
            (By.TAG_NAME, "body")))
        logging.info("Прокси работает с Wildberries")
        return True
    except Exception as e:
        logging.error(f"Прокси не работает с Wildberries: {e}")
        return False

# Функция для выбора 5 рабочих прокси
def get_working_proxies(proxy_list):
    working_proxies = []
    for proxy in proxy_list:
        if len(working_proxies) >= 5:
            break
        if proxy in BAD_PROXIES:
            continue
        driver = setup_driver(proxy)
        if not driver:
            BAD_PROXIES.add(proxy)
            continue
        print(f"Проверяем прокси: {proxy}")
        if test_proxy(driver, FIRST_PAGE_PROXY_TEST_PAUSE):
            working_proxies.append(proxy)
            print(f"Добавлен рабочий прокси: {proxy}")
        else:
            BAD_PROXIES.add(proxy)
        driver.quit()
    if not working_proxies:
        logging.error("Не удалось найти рабочие прокси")
        raise Exception("Не удалось найти рабочие прокси")
    logging.info(f"Выбрано {len(working_proxies)} рабочих прокси")
    return working_proxies

# Блок 5: Настройка драйвера и работа с прокси
# Функция для настройки драйвера Firefox
def setup_driver(proxy):
    firefox_options = Options()
    firefox_options.add_argument("--headless")
    user_agent = next(USER_AGENT_CYCLE)
    firefox_options.add_argument(f"user-agent={user_agent}")
    logging.info(f"Используем User-Agent: {user_agent}")
    if proxy:
        firefox_options.add_argument(f"--proxy-server={proxy}")
    try:
        driver = webdriver.Firefox(options=firefox_options)
        width, height = random.choice(SCREEN_RESOLUTIONS)
        driver.set_window_size(width, height)
        logging.info(f"Драйвер создан с разрешением {width}x{height}")
        return driver
    except Exception as e:
        logging.error(f"Ошибка при создании драйвера с прокси {proxy}: {e}")
        return None

# Функция для получения рабочего драйвера
def get_working_driver(proxy_list):
    if not proxy_list:
        logging.error("Список прокси пуст")
        return None
    proxy = random.choice(proxy_list)
    driver = setup_driver(proxy)
    if not driver:
        BAD_PROXIES.add(proxy)
        return None
    if test_proxy(driver, FIRST_PAGE_PROXY_TEST_PAUSE):
        return driver
    driver.quit()
    BAD_PROXIES.add(proxy)
    return None

# Блок 6: Функции для имитации поведения пользователя
# Имитация движения мыши
def simulate_mouse_movement(driver):
    try:
        driver.execute_script("window.scrollTo(0, 0);")
        for _ in range(2):
            x = random.randint(100, 500)
            y = random.randint(100, 500)
            driver.execute_script(f"window.scrollBy({x}, {y});")
            time.sleep(random.uniform(0.1, 0.3))
        logging.info("Имитация движения мыши выполнена")
    except Exception as e:
        logging.error(f"Ошибка при имитации движения мыши: {e}")

# Случайный клик по товару
def simulate_random_click(driver, items):
    try:
        if not items:
            logging.info("Нет товаров для клика")
            return
        time.sleep(random.uniform(0.5, 2))
        simulate_mouse_movement(driver)
        random_item = random.choice(items)
        link = random_item.find_element(By.CLASS_NAME, "product-card__link")
        link.click()
        time.sleep(random.uniform(2, 5))
        driver.back()
        logging.info("Выполнен случайный клик по товару")
        WebDriverWait(driver, 15).until(EC.presence_of_all_elements_located(\
            (By.CLASS_NAME, "product-card__wrapper")))
    except Exception as e:
        logging.error(f"Ошибка при случайном клике: {e}")

# Случайная прокрутка страницы
def simulate_random_scrolls(driver):
    try:
        page_height = driver.execute_script("return document.body.scrollHeight")
        num_scrolls = random.randint(1, 2)
        for _ in range(num_scrolls):
            scroll_position = random.uniform(0.2 * page_height, 0.8 * page_height)
            driver.execute_script(f"window.scrollTo(0, {scroll_position});")
            time.sleep(random.uniform(0.5, 1.5))
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        logging.info(f"Выполнены случайные скроллы ({num_scrolls} раз)")
    except Exception as e:
        logging.error(f"Ошибка при случайных скроллах: {e}")

# Случайный переход в другую категорию
def simulate_category_wandering(driver):
    try:
        driver.get("https://www.wildberries.ru/catalog/zhenshchinam/odezhda/platya")
        time.sleep(random.uniform(3, 6))
        logging.info("Переход в категорию 'Платья'")
        driver.back()
        WebDriverWait(driver, 15).until(EC.presence_of_all_elements_located(\
            (By.CLASS_NAME, "product-card__wrapper")))
    except Exception as e:
        logging.error(f"Ошибка при переходе в категорию: {e}")

# Применение случайного фильтра (например, сортировка по цене)
def apply_random_filter(driver, category_url, page_number):
    try:
        # Переходим на страницу с сортировкой по цене (по возрастанию)
        sort_url = category_url + f"&page={page_number}&sort=priceup"
        driver.get(sort_url)
        logging.info(f"Применён фильтр: сортировка по цене (по возрастанию) на странице {page_number}")
        time.sleep(random.uniform(3, 6))
        # Возвращаемся на исходную страницу
        driver.get(category_url + f"&page={page_number}")
        WebDriverWait(driver, 15).until(EC.presence_of_all_elements_located(\
            (By.CLASS_NAME, "product-card__wrapper")))
    except Exception as e:
        logging.error(f"Ошибка при применении фильтра: {e}")

# Функция: Закрытие баннера с куки
def close_cookies_banner(driver):
    try:
        cookies_banner = driver.find_element(By.CLASS_NAME, "cookies")
        accept_button = cookies_banner.find_element(By.XPATH, ".//button[contains(text(), 'Принять')]")
        driver.execute_script("arguments[0].scrollIntoView(true);", accept_button)
        accept_button.click()
        logging.info("Баннер с куки закрыт")
        time.sleep(random.uniform(1, 2))  # Даём время на закрытие баннера
    except NoSuchElementException:
        logging.info("Баннер с куки не найден")
    except Exception as e:
        logging.error(f"Ошибка при закрытии баннера с куки: {e}")

# Функция: Уход с сайта на 1.5 минуты каждые 15 страниц
def simulate_site_exit(driver, page_number, category_url, all_products):
    logging.info(f"Имитация ухода с сайта на странице {page_number} (каждые 15 страниц)")
    # Сохраняем данные перед уходом
    save_to_csv(all_products)
    logging.info(f"Данные сохранены перед уходом с сайта на странице {page_number}")
    try:
        # Очищаем куки и кеш
        driver.delete_all_cookies()
        driver.execute_script("window.localStorage.clear();")
        driver.execute_script("window.sessionStorage.clear();")
        logging.info("Куки и кеш очищены перед уходом")

        # Переходим на Google
        driver.get("https://www.google.com")
        logging.info("Перешли на Google для имитации ухода")

        # Делаем простой поисковый запрос
        search_box = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.NAME, "q"))
        )
        search_query = "погода в Москве"
        search_box.send_keys(search_query)
        search_box.submit()
        logging.info(f"Выполнен поиск в Google: '{search_query}'")

        # Ждём 1.5 минуты (90 секунд)
        time.sleep(SITE_EXIT_DURATION)
        logging.info(f"Пауза {SITE_EXIT_DURATION} секунд завершена")

        # Возвращаемся на Wildberries
        driver.get(category_url + f"&page={page_number}")
        logging.info(f"Вернулись на страницу {page_number} Wildberries")
        WebDriverWait(driver, 15).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, "product-card__wrapper"))
        )
    except Exception as e:
        logging.error(f"Ошибка при имитации ухода с сайта: {e}")
        driver.get(category_url + f"&page={page_number}")
        logging.info(f"Принудительно вернулись на страницу {page_number} Wildberries")

# Функция: Уход с сайта при дубликатах на 3 минуты с ротацией прокси
def simulate_duplicate_exit(driver, page_number, category_url, all_products):
    logging.info(f"Имитация ухода с сайта на странице {page_number} из-за дубликатов")
    # Сохраняем данные перед уходом
    save_to_csv(all_products)
    logging.info(f"Данные сохранены перед уходом с сайта на странице {page_number}")
    try:
        # Очищаем куки и кеш
        driver.delete_all_cookies()
        driver.execute_script("window.localStorage.clear();")
        driver.execute_script("window.sessionStorage.clear();")
        logging.info("Куки и кеш очищены перед уходом")

        # Переходим на Google
        driver.get("https://www.google.com")
        logging.info("Перешли на Google из-за дубликатов")

        # Делаем простой поисковый запрос
        search_box = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.NAME, "q"))
        )
        search_query = "погода в Москве"
        search_box.send_keys(search_query)
        search_box.submit()
        logging.info(f"Выполнен поиск в Google: '{search_query}'")

        # Ждём 3 минуты (180 секунд)
        time.sleep(DUPLICATE_EXIT_DURATION)
        logging.info(f"Пауза {DUPLICATE_EXIT_DURATION} секунд завершена")

        # Ротация прокси
        driver = rotate_proxy(driver, page_number, category_url)

        # Возвращаемся на Wildberries
        driver.get(category_url + f"&page={page_number}")
        logging.info(f"Вернулись на страницу {page_number} Wildberries после ротации прокси")
        WebDriverWait(driver, 15).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, "product-card__wrapper"))
        )

        # Дополнительные "человечные" действия
        simulate_category_wandering(driver)
        apply_random_filter(driver, category_url, page_number)
    except Exception as e:
        logging.error(f"Ошибка при имитации ухода из-за дубликатов: {e}")
        driver.get(category_url + f"&page={page_number}")
        logging.info(f"Принудительно вернулись на страницу {page_number} Wildberries")

# Функция: Имитация зависания на 30 секунд каждые 7 страниц
def simulate_idle(page_number):
    logging.info(f"Имитация зависания на странице {page_number} на {IDLE_DURATION} секунд")
    time.sleep(IDLE_DURATION)
    logging.info(f"Зависание завершено на странице {page_number}")

# Блок 7: Функции для работы с прокси и CAPTCHA
# "Хитрости" после ротации прокси
def perform_tricks_after_rotation(driver, page_number, category_url):
    logging.info(f"Выполняем 'хитрости' на странице {page_number}")
    driver.delete_all_cookies()
    logging.info("Куки очищены")
    if page_number > 1:
        prev_page = page_number - 1
        driver.get(category_url + f"&page={prev_page}")
        logging.info(f"Вернулись на страницу {prev_page}")
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(random.uniform(3, 6))
    driver.get("https://www.wildberries.ru/")
    logging.info("Перешли на главную страницу")
    if random.random() < 0.3:
        driver.refresh()
        logging.info("Обновление главной страницы")
    time.sleep(random.uniform(5, 10))
    # Добавляем переход на случайную страницу
    random_page = random.randint(1, page_number - 1)
    driver.get(category_url + f"&page={random_page}")
    logging.info(f"Перешли на случайную страницу {random_page}")
    time.sleep(random.uniform(3, 6))
    driver.get(category_url + f"&page={page_number}")
    logging.info(f"Вернулись на страницу {page_number}")
    if random.random() > 0.5:
        driver.refresh()
        logging.info("Обновление страницы")
        try:
            WebDriverWait(driver, 15).until(EC.presence_of_all_elements_located(\
                (By.CLASS_NAME, "product-card__wrapper")))
        except TimeoutException:
            logging.warning("Не удалось найти элементы product-card__wrapper, пробуем j-card-item")
            WebDriverWait(driver, 15).until(EC.presence_of_all_elements_located(\
                (By.CLASS_NAME, "j-card-item")))
    delay = random.uniform(10, 20)
    time.sleep(delay)
    logging.info(f"Задержка после ротации: {delay:.2f} секунд")

# Проверка наличия CAPTCHA
def check_for_captcha(driver):
    try:
        driver.find_element(By.CLASS_NAME, "captcha__container")
        logging.warning("Обнаружена CAPTCHA (класс captcha__container)")
        return True
    except NoSuchElementException:
        pass
    try:
        driver.find_element(By.XPATH, "//*[contains(text(), 'Подтвердите, что вы не робот')]")
        logging.warning("Обнаружена CAPTCHA (текст 'Подтвердите, что вы не робот')")
        return True
    except NoSuchElementException:
        logging.info("CAPTCHA не обнаружена")
        return False

# Обработка CAPTCHA
def handle_captcha(driver, page_number, category_url):
    logging.info(f"Обработка CAPTCHA на странице {page_number}")
    pause = random.uniform(10, 15)
    time.sleep(pause)
    logging.info(f"Пауза перед обработкой CAPTCHA: {pause:.2f} секунд")
    driver.delete_all_cookies()
    logging.info("Куки очищены")
    driver = rotate_proxy(driver, page_number, category_url.split("&page=")[0])
    logging.info("Драйвер перезапущен с новым прокси")
    return driver

# Ротация прокси
def rotate_proxy(driver, page_number, category_url, max_attempts=5):
    try:
        driver.quit()
    except Exception as e:
        logging.error(f"Ошибка при закрытии драйвера во время ротации: {e}")
    logging.info(f"Ротация прокси на странице {page_number}")
    time.sleep(random.uniform(5, 10))
    attempts = 0
    while attempts < max_attempts:
        new_driver = get_working_driver(PROXY_LIST)
        if new_driver:
            perform_tricks_after_rotation(new_driver, page_number, category_url)
            return new_driver
        attempts += 1
        logging.warning(f"Попытка {attempts}/{max_attempts} найти прокси не удалась")
    logging.error(f"Не удалось найти прокси после {max_attempts} попыток")
    raise Exception("Не удалось найти прокси после максимального количества попыток")

# Перезапуск драйвера
def restart_driver(driver, page_number, category_url):
    logging.info(f"Перезапуск драйвера на странице {page_number}")
    try:
        driver.quit()
    except Exception as e:
        logging.error(f"Ошибка при закрытии драйвера во время перезапуска: {e}")
    time.sleep(random.uniform(5, 10))
    new_driver = get_working_driver(PROXY_LIST)
    if new_driver:
        perform_tricks_after_rotation(new_driver, page_number, category_url)
        return new_driver
    logging.error("Не удалось перезапустить драйвер")
    raise Exception("Не удалось перезапустить драйвер")

# Блок 8: Функции для очистки данных
# Очистка цены
def clean_price(price_text):
    cleaned = ''.join(filter(str.isdigit, price_text))
    return cleaned if cleaned else "0"

# Очистка названия
def clean_name(name_text):
    return name_text.replace("/", "").strip()

# Очистка рейтинга
def clean_rating(rating_text):
    # Удаляем всё, кроме цифр, точки и запятой
    cleaned = ''.join(c for c in rating_text if c.isdigit() or c in [',', '.'])
    # Заменяем запятую на точку
    cleaned = cleaned.replace(',', '.')
    return cleaned if cleaned else "0"

# Блок 9: Основная функция парсинга
def parse_wildberries(category_url):
    # Собираем и проверяем прокси
    proxy_list = fetch_proxies()
    global PROXY_LIST
    PROXY_LIST = get_working_proxies(proxy_list)

    # Инициализация данных
    all_products = []
    unique_product_links = set()
    driver = None
    attempts = 0
    max_attempts = 3
    consecutive_duplicate_pages = 0  # Счётчик страниц с дубликатами
    while attempts < max_attempts:
        try:
            driver = get_working_driver(PROXY_LIST)
            if not driver:
                logging.error("Не удалось создать драйвер")
                return []
            driver.get(category_url)
            logging.info(f"Открыта страница: {category_url}")
            # Закрываем баннер с куки при первом входе
            close_cookies_banner(driver)
            break
        except (TimeoutException, WebDriverException) as e:
            logging.error(f"Ошибка загрузки страницы: {e}")
            attempts += 1
            if driver:
                driver.quit()
            if attempts == max_attempts:
                logging.error("Не удалось загрузить страницу после всех попыток")
                return []
            time.sleep(random.uniform(2, 5))
            continue

    # Основной цикл парсинга
    page_number = 1
    max_retries = 5
    max_rotation_cycles = 5
    while True:
        # Засекаем время начала обработки страницы
        page_start_time = datetime.now()

        # Очистка unique_product_links каждые 50 страниц
        if page_number % 50 == 0 and page_number > 1:
            unique_product_links.clear()
            logging.info(f"Очищен список unique_product_links на странице {page_number}")

        # Уход с сайта каждые 15 страниц
        if page_number % SITE_EXIT_INTERVAL == 0 and page_number > 1:
            simulate_site_exit(driver, page_number, category_url.split("&page=")[0], all_products)

        # Имитация зависания каждые 7 страниц
        if page_number % IDLE_INTERVAL == 0 and page_number > 1:
            simulate_idle(page_number)

        # Ротация прокси каждые 20 страниц
        if page_number > 1 and (page_number - 1) % PROXY_ROTATION_INTERVAL == 0:
            driver = rotate_proxy(driver, page_number, category_url.split("&page=")[0])
            logging.info(f"Переоткрыта страница {page_number} после ротации")

        # Перезапуск драйвера каждые 20 страниц
        if page_number > 1 and (page_number - 1) % DRIVER_RESTART_INTERVAL == 0:
            driver = restart_driver(driver, page_number, category_url.split("&page=")[0])
            logging.info(f"Драйвер перезапущен на странице {page_number}")

        # Установка таймингов в зависимости от страницы
        if page_number == 1 or (page_number - 1) % PROXY_ROTATION_INTERVAL == 0:
            scroll_down_pause = FIRST_PAGE_SCROLL_DOWN_PAUSE
            scroll_up_pause = FIRST_PAGE_SCROLL_UP_PAUSE
            scroll_down_again_pause = FIRST_PAGE_SCROLL_DOWN_AGAIN_PAUSE
            page_transition_pause = FIRST_PAGE_PAGE_TRANSITION_PAUSE
            logging.info(f"Страница {page_number}: тайминги первого блока")
        else:
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
            logging.info(f"Страница {page_number}: тайминги блока {block_index + 2}")

        # Прокрутка страницы
        max_scroll_attempts = 15
        scroll_attempts = 0
        last_height = driver.execute_script("return document.body.scrollHeight")
        if random.random() < 0.5:
            simulate_random_scrolls(driver)
            time.sleep(random.uniform(0.5, 2))
            items = driver.find_elements(By.CLASS_NAME, "product-card__wrapper")
            if not items:
                items = driver.find_elements(By.CLASS_NAME, "j-card-item")
                logging.info("Переключение на селектор j-card-item")
            cycle_page = (page_number - 1) % 20 + 1
            if cycle_page in CLICK_PAGES:
                simulate_random_click(driver, items)
                logging.info(f"Случайный клик на странице {page_number}")

        while scroll_attempts < max_scroll_attempts:
            if check_for_captcha(driver):
                driver = handle_captcha(driver, page_number, category_url)
                driver.get(category_url + f"&page={page_number}")
                logging.info(f"Переоткрыта страница {page_number} после CAPTCHA")
            try:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            except (WebDriverException, NewConnectionError) as e:
                logging.error(f"Ошибка при прокрутке страницы {page_number}: {e}")
                # Перезапускаем драйвер
                driver = restart_driver(driver, page_number, category_url.split("&page=")[0])
                driver.get(category_url + f"&page={page_number}")
                logging.info(f"Переоткрыта страница {page_number} после перезапуска драйвера")
                continue
            current_scroll_down_pause = scroll_down_pause * \
                (PAUSE_INCREASE_FACTOR ** scroll_attempts)
            time.sleep(current_scroll_down_pause)
            try:
                driver.execute_script("window.scrollTo(0, 0);")
            except (WebDriverException, NewConnectionError) as e:
                logging.error(f"Ошибка при прокрутке страницы {page_number}: {e}")
                driver = restart_driver(driver, page_number, category_url.split("&page=")[0])
                driver.get(category_url + f"&page={page_number}")
                logging.info(f"Переоткрыта страница {page_number} после перезапуска драйвера")
                continue
            current_scroll_up_pause = scroll_up_pause * \
                (PAUSE_INCREASE_FACTOR ** scroll_attempts)
            time.sleep(current_scroll_up_pause)
            try:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            except (WebDriverException, NewConnectionError) as e:
                logging.error(f"Ошибка при прокрутке страницы {page_number}: {e}")
                driver = restart_driver(driver, page_number, category_url.split("&page=")[0])
                driver.get(category_url + f"&page={page_number}")
                logging.info(f"Переоткрыта страница {page_number} после перезапуска драйвера")
                continue
            current_scroll_down_again_pause = scroll_down_again_pause * \
                (PAUSE_INCREASE_FACTOR ** scroll_attempts)
            time.sleep(current_scroll_down_again_pause)
            new_height = driver.execute_script("return document.body.scrollHeight")
            items = driver.find_elements(By.CLASS_NAME, "product-card__wrapper")
            if not items:
                items = driver.find_elements(By.CLASS_NAME, "j-card-item")
                logging.info("Переключение на селектор j-card-item")
            logging.info(f"Страница {page_number}, Прокрутка {scroll_attempts + 1}: \
                найдено {len(items)} товаров")
            print(f"Страница {page_number}, Прокрутка {scroll_attempts + 1}: \
                найдено {len(items)} товаров")
            if new_height == last_height:
                logging.info(f"Страница {page_number}: новых товаров не подгрузилось")
                break
            last_height = new_height
            scroll_attempts += 1

        # Дополнительные действия для имитации
        if random.random() >= 0.5:
            simulate_random_scrolls(driver)
            time.sleep(random.uniform(0.5, 2))
            items = driver.find_elements(By.CLASS_NAME, "product-card__wrapper")
            if not items:
                items = driver.find_elements(By.CLASS_NAME, "j-card-item")
                logging.info("Переключение на селектор j-card-item")
            cycle_page = (page_number - 1) % 20 + 1
            if cycle_page in CLICK_PAGES:
                simulate_random_click(driver, items)
                logging.info(f"Случайный клик на странице {page_number}")
        if random.random() < 0.05:
            simulate_category_wandering(driver)
            logging.info(f"Переход в другую категорию на странице {page_number}")

        # Повторная загрузка товаров после действий
        items = driver.find_elements(By.CLASS_NAME, "product-card__wrapper")
        if not items:
            items = driver.find_elements(By.CLASS_NAME, "j-card-item")
            logging.info("Переключение на селектор j-card-item")
        if len(items) == 0:
            logging.warning(f"На странице {page_number} не найдено товаров")
            driver = rotate_proxy(driver, page_number, category_url.split("&page=")[0])
            driver.get(category_url + f"&page={page_number}")
            logging.info(f"Переоткрыта страница {page_number} после ротации")
            scroll_attempts = 0
            last_height = driver.execute_script("return document.body.scrollHeight")
            while scroll_attempts < max_scroll_attempts:
                if check_for_captcha(driver):
                    driver = handle_captcha(driver, page_number, category_url)
                    driver.get(category_url + f"&page={page_number}")
                    logging.info(f"Переоткрыта страница {page_number} после CAPTCHA")
                try:
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                except (WebDriverException, NewConnectionError) as e:
                    logging.error(f"Ошибка при прокрутке страницы {page_number}: {e}")
                    driver = restart_driver(driver, page_number, category_url.split("&page=")[0])
                    driver.get(category_url + f"&page={page_number}")
                    logging.info(f"Переоткрыта страница {page_number} после перезапуска драйвера")
                    continue
                time.sleep(FIRST_PAGE_SCROLL_DOWN_PAUSE)
                driver.execute_script("window.scrollTo(0, 0);")
                time.sleep(FIRST_PAGE_SCROLL_UP_PAUSE)
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(FIRST_PAGE_SCROLL_DOWN_AGAIN_PAUSE)
                new_height = driver.execute_script("return document.body.scrollHeight")
                items = driver.find_elements(By.CLASS_NAME, "product-card__wrapper")
                if not items:
                    items = driver.find_elements(By.CLASS_NAME, "j-card-item")
                    logging.info("Переключение на селектор j-card-item")
                logging.info(f"Страница {page_number}, Прокрутка {scroll_attempts + 1} \
                    (после ротации): найдено {len(items)} товаров")
                print(f"Страница {page_number}, Прокрутка {scroll_attempts + 1} \
                    (после ротации): найдено {len(items)} товаров")
                if new_height == last_height:
                    logging.info(f"Страница {page_number}: новых товаров не подгрузилось")
                    break
                last_height = new_height
                scroll_attempts += 1

        # Обработка товаров
        rotation_cycles = 0
        while rotation_cycles < max_rotation_cycles:
            retry_count = 0
            while retry_count < max_retries:
                if check_for_captcha(driver):
                    driver = handle_captcha(driver, page_number, category_url)
                    driver.get(category_url + f"&page={page_number}")
                    logging.info(f"Переоткрыта страница {page_number} после CAPTCHA")
                try:
                    items = WebDriverWait(driver, 15).until(\
                        EC.presence_of_all_elements_located(\
                        (By.CLASS_NAME, "product-card__wrapper")))
                    break
                except TimeoutException:
                    try:
                        items = WebDriverWait(driver, 15).until(\
                            EC.presence_of_all_elements_located(\
                            (By.CLASS_NAME, "j-card-item")))
                        logging.info("Переключение на селектор j-card-item")
                        break
                    except TimeoutException as e:
                        retry_count += 1
                        logging.warning(f"Ошибка загрузки товаров на странице {page_number}, \
                            попытка {retry_count}/{max_retries}: {e}")
                        if retry_count == max_retries:
                            rotation_cycles += 1
                            if rotation_cycles == max_rotation_cycles:
                                logging.warning(f"Не удалось загрузить товары на странице \
                                    {page_number} после {max_rotation_cycles} ротаций")
                                page_number += 1
                                driver.get(category_url + f"&page={page_number}")
                                logging.info(f"Переходим на страницу {page_number}")
                                rotation_cycles = 0
                                break
                            logging.error(f"Не удалось загрузить товары на странице \
                                {page_number}, пробуем сменить прокси (цикл \
                                {rotation_cycles}/{max_rotation_cycles})")
                            driver = rotate_proxy(driver, page_number, \
                                category_url.split("&page=")[0])
                            driver.get(category_url + f"&page={page_number}")
                            logging.info(f"Переоткрыта страница {page_number} после \
                                неудачных попыток")
                            retry_count = 0
                            continue
                        current_page_transition_pause = page_transition_pause * \
                            (PAUSE_INCREASE_FACTOR ** retry_count)
                        driver.refresh()
                        time.sleep(current_page_transition_pause)
            if retry_count < max_retries:
                break

        # Парсинг товаров
        logging.info(f"Страница {page_number}: всего найдено {len(items)} товаров")
        print(f"Страница {page_number}: всего найдено {len(items)} товаров")
        duplicates_count = 0
        for item in items:
            if len(all_products) >= MAX_PRODUCTS:
                logging.info(f"Достигнут лимит в {MAX_PRODUCTS} товаров")
                driver.quit()
                save_to_csv(all_products)
                return all_products
            try:
                name = item.find_element(By.CLASS_NAME, "product-card__name").text.strip()
                if not name:
                    continue
                cleaned_name = clean_name(name)
                price = item.find_element(By.CLASS_NAME, "price__lower-price").text.strip()
                cleaned_price = clean_price(price)
                # Извлечение рейтинга
                try:
                    rating = item.find_element(By.CLASS_NAME, "address-rate-mini").text.strip()
                    cleaned_rating = clean_rating(rating)
                except:
                    cleaned_rating = "0"
                try:
                    link = item.find_element(By.CLASS_NAME, "product-card__link").\
                        get_attribute("href")
                except:
                    link = ""
                if link in unique_product_links:
                    duplicates_count += 1
                    logging.info(f"Пропущен дубликат: {cleaned_name}, Ссылка: {link}")
                    continue
                unique_product_links.add(link)
                logging.info(f"Добавлена новая ссылка: {link}")
                # Добавляем товар в список (без address и discount)
                all_products.append({
                    "Название": cleaned_name,
                    "Цена": cleaned_price,
                    "Рейтинг": cleaned_rating,
                    "Ссылка": link
                })
                logging.info(f"Спарсен товар: {cleaned_name}, Цена: {cleaned_price}, \
                    Рейтинг: {cleaned_rating}")
            except Exception as e:
                logging.error(f"Ошибка при парсинге товара: {e}")
                continue

        # Проверка на дубликаты
        if duplicates_count == len(items) and len(items) > 0:
            consecutive_duplicate_pages += 1
            logging.info(f"Страница {page_number}: все товары — дубликаты, \
                consecutive_duplicate_pages = {consecutive_duplicate_pages}")
        else:
            consecutive_duplicate_pages = 0
            logging.info(f"Страница {page_number}: найдены новые товары, \
                сброс consecutive_duplicate_pages")

        # Если 2 страницы подряд содержат только дубликаты
        if consecutive_duplicate_pages >= DUPLICATE_PAGE_THRESHOLD:
            simulate_duplicate_exit(driver, page_number, category_url.split("&page=")[0], all_products)
            consecutive_duplicate_pages = 0  # Сбрасываем счётчик после ухода

        # Вычисляем время, потраченное на страницу и общее время
        page_end_time = datetime.now()
        page_duration = (page_end_time - page_start_time).total_seconds()
        total_duration = (page_end_time - start_time).total_seconds()

        # Итоговая информация по странице
        print(f"--- Итог страницы {page_number} ---")
        print(f"Пропущено дубликатов: {duplicates_count}")
        print(f"Всего собрано товаров: {len(all_products)}")
        print(f"Время на страницу: {page_duration:.2f} сек")
        print(f"Общее время парсинга: {total_duration:.2f} сек")
        print(f"-------------------------")

        # Сохранение данных
        if page_number % 10 == 0:
            save_to_csv(all_products)
            logging.info(f"Промежуточное сохранение на странице {page_number}")
            print(f"Промежуточное сохранение на странице {page_number}")

        # Переход на следующую страницу
        pagination_retries = 10
        pagination_class_found = False
        for attempt in range(pagination_retries):
            try:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            except (WebDriverException, NewConnectionError) as e:
                logging.error(f"Ошибка при прокрутке страницы {page_number}: {e}")
                driver = restart_driver(driver, page_number, category_url.split("&page=")[0])
                driver.get(category_url + f"&page={page_number}")
                logging.info(f"Переоткрыта страница {page_number} после перезапуска драйвера")
                continue
            time.sleep(page_transition_pause)
            if check_for_captcha(driver):
                driver = handle_captcha(driver, page_number, category_url)
                driver.get(category_url + f"&page={page_number}")
                logging.info(f"Переоткрыта страница {page_number} после CAPTCHA")
            # Закрываем баннер с куки перед попыткой клика
            close_cookies_banner(driver)
            try:
                # Ищем кнопку пагинации по классу
                next_button = WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "pagination-next"))
                )
                pagination_class_found = True
                if "disabled" in next_button.get_attribute("class"):
                    logging.info("Кнопка 'Следующая страница' неактивна")
                    driver.quit()
                    save_to_csv(all_products)
                    return all_products
                # Прокручиваем к кнопке, чтобы она была в зоне видимости
                driver.execute_script("arguments[0].scrollIntoView(true);", next_button)
                time.sleep(random.uniform(0.5, 1))  # Даём время на прокрутку
                # Пытаемся скрыть перекрывающий элемент
                try:
                    driver.execute_script("document.querySelector('.dropdown-filter__btn-name').style.display = 'none';")
                    logging.info("Перекрывающий элемент dropdown-filter__btn-name скрыт")
                except:
                    logging.info("Перекрывающий элемент dropdown-filter__btn-name не найден")
                # Проверяем, кликабельна ли кнопка
                WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CLASS_NAME, "pagination-next")))
                next_button.click()
                logging.info(f"Успешно кликнули на кнопку пагинации (pagination-next) на странице {page_number}")
                current_page_transition_pause = page_transition_pause * \
                    (PAUSE_INCREASE_FACTOR ** attempt)
                time.sleep(current_page_transition_pause + random.uniform(1, 3))
                page_number += 1
                break
            except (TimeoutException, ElementClickInterceptedException) as e:
                logging.warning(f"Не удалось найти или кликнуть на кнопку пагинации по классу на странице "
                                f"{page_number}, попытка {attempt + 1}/{pagination_retries}: {e}")
                try:
                    # Пробуем найти кнопку по символу стрелки (→)
                    next_button = WebDriverWait(driver, 5).until(
                        EC.element_to_be_clickable((By.XPATH, "//a[contains(@class, 'pagination-next') and contains(text(), '→')]"))
                    )
                    logging.info("Кнопка пагинации найдена по символу '→'")
                    if "disabled" in next_button.get_attribute("class"):
                        logging.info("Кнопка '→' неактивна")
                        driver.quit()
                        save_to_csv(all_products)
                        return all_products
                    # Прокручиваем к кнопке
                    driver.execute_script("arguments[0].scrollIntoView(true);", next_button)
                    time.sleep(random.uniform(0.5, 1))
                    next_button.click()
                    logging.info(f"Успешно кликнули на кнопку пагинации (по символу '→') на странице {page_number}")
                    current_page_transition_pause = page_transition_pause * \
                        (PAUSE_INCREASE_FACTOR ** attempt)
                    time.sleep(current_page_transition_pause + random.uniform(1, 3))
                    page_number += 1
                    break
                except Exception as e_alt:
                    logging.warning(f"Не удалось найти кнопку пагинации по символу '→' на странице "
                                    f"{page_number}, попытка {attempt + 1}/{pagination_retries}: {e_alt}")
                    # Пробуем перейти через кнопку с номером страницы
                    try:
                        next_page_number = page_number + 1
                        page_button = WebDriverWait(driver, 5).until(
                            EC.element_to_be_clickable((By.XPATH, f"//a[@class='pagination-item pagination__item j-page' and text()='{next_page_number}']"))
                        )
                        driver.execute_script("arguments[0].scrollIntoView(true);", page_button)
                        time.sleep(random.uniform(0.5, 1))
                        page_button.click()
                        logging.info(f"Успешно кликнули на кнопку с номером страницы {next_page_number}")
                        current_page_transition_pause = page_transition_pause * \
                            (PAUSE_INCREASE_FACTOR ** attempt)
                        time.sleep(current_page_transition_pause + random.uniform(1, 3))
                        page_number += 1
                        break
                    except Exception as e_page:
                        logging.warning(f"Не удалось найти кнопку с номером страницы {next_page_number}, попытка {attempt + 1}/{pagination_retries}: {e_page}")
                        # Последняя попытка: извлекаем href и переходим напрямую
                        try:
                            next_button = driver.find_element(By.CLASS_NAME, "pagination-next")
                            next_page_url = next_button.get_attribute("href")
                            if next_page_url:
                                driver.get(next_page_url)
                                logging.info(f"Перешли на страницу {page_number + 1} через прямой URL: {next_page_url}")
                                current_page_transition_pause = page_transition_pause * \
                                    (PAUSE_INCREASE_FACTOR ** attempt)
                                time.sleep(current_page_transition_pause + random.uniform(1, 3))
                                page_number += 1
                                break
                        except Exception as e_href:
                            logging.warning(f"Не удалось извлечь href кнопки пагинации на странице {page_number}, попытка {attempt + 1}/{pagination_retries}: {e_href}")
                            if attempt == pagination_retries - 1:
                                logging.info(f"Пагинация не найдена после {pagination_retries} попыток")
                                driver.quit()
                                save_to_csv(all_products)
                                return all_products
                            current_page_transition_pause = page_transition_pause * \
                                (PAUSE_INCREASE_FACTOR ** attempt)
                            driver.refresh()
                            time.sleep(current_page_transition_pause)
        if not pagination_class_found:
            logging.warning(f"Класс pagination-next не найден на странице {page_number}")

    # Завершение парсинга
    logging.info(f"Всего собрано товаров: {len(all_products)}")
    print(f"Всего собрано товаров: {len(all_products)}")
    duration = (datetime.now() - start_time).total_seconds() / 60
    logging.info(f"Парсинг завершен за {duration:.2f} минут")
    print(f"Парсинг завершен за {duration:.2f} минут")
    driver.quit()
    logging.info("Парсинг завершен")
    return all_products

# Блок 10: Сохранение данных в CSV
def save_to_csv(data, filename=SAVE_PATH):
    df = pd.DataFrame(data)
    # Удаляем ненужные столбцы (address и discount)
    columns_to_keep = ["Название", "Цена", "Рейтинг", "Ссылка"]
    df = df[columns_to_keep]
    # Оборачиваем значения рейтинга в кавычки, чтобы запятая не воспринималась как разделитель
    if 'Рейтинг' in df.columns:
        df['Рейтинг'] = df['Рейтинг'].apply(lambda x: f'"{x}"')
    df.to_csv(filename, index=False, encoding="utf-8-sig", sep=";")
    logging.info(f"Данные сохранены в {filename}")
    print(f"Данные сохранены в {filename}")

# Блок 11: Запуск парсинга
if __name__ == "__main__":
    url = "https://www.wildberries.ru/catalog/0/search.aspx?page=1&sort=popular&search=%D1%8E%D0%B1%D0%BA%D0%B0+%D0%B6%D0%B5%D0%BD%D1%81%D0%BA%D0%B0%D1%8F&f57021=64270"
    products = parse_wildberries(url)
    save_to_csv(products)
