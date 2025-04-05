import time
import os
import random
import logging
import itertools
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException

logging.basicConfig(filename="/home/mangust1981/Документы/3Пайтон/wildberries_parser.log",
                    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

FIRST_PAGE_SCROLL_DOWN_PAUSE = 1.3
FIRST_PAGE_SCROLL_UP_PAUSE = 0.7
FIRST_PAGE_SCROLL_DOWN_AGAIN_PAUSE = 1.3
FIRST_PAGE_PAGE_TRANSITION_PAUSE = 2.5
FIRST_PAGE_PROXY_TEST_PAUSE = 3.0

SUBSEQUENT_PAGES_BLOCK_2_SCROLL_DOWN_PAUSE = 0.18
SUBSEQUENT_PAGES_BLOCK_2_SCROLL_UP_PAUSE = 0.13
SUBSEQUENT_PAGES_BLOCK_2_SCROLL_DOWN_AGAIN_PAUSE = 0.18
SUBSEQUENT_PAGES_BLOCK_2_PAGE_TRANSITION_PAUSE = 0.23

SUBSEQUENT_PAGES_BLOCK_3_SCROLL_DOWN_PAUSE = 0.19
SUBSEQUENT_PAGES_BLOCK_3_SCROLL_UP_PAUSE = 0.14
SUBSEQUENT_PAGES_BLOCK_3_SCROLL_DOWN_AGAIN_PAUSE = 0.19
SUBSEQUENT_PAGES_BLOCK_3_PAGE_TRANSITION_PAUSE = 0.24

SUBSEQUENT_PAGES_BLOCK_4_SCROLL_DOWN_PAUSE = 0.20
SUBSEQUENT_PAGES_BLOCK_4_SCROLL_UP_PAUSE = 0.15
SUBSEQUENT_PAGES_BLOCK_4_SCROLL_DOWN_AGAIN_PAUSE = 0.20
SUBSEQUENT_PAGES_BLOCK_4_PAGE_TRANSITION_PAUSE = 0.25

PROXY_LIST_EUROPE_USA = [
    "http://31.148.204.183:8080",
    "http://93.91.112.247:41258",
    "http://31.211.69.52:3128",
    "http://91.228.51.26:3128",
    "http://82.146.37.145:80",
    "http://94.23.9.170:80",
    "http://146.59.202.70:80",
    "http://51.91.109.83:80",
    "http://62.210.15.199:80",
    "http://173.249.40.64:8118",
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (Linux; Android 10; SM-G975F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Mobile Safari/537.36",
]

PROXY_ROTATION_INTERVAL = 20
CLICK_PAGES = sorted(random.sample(range(1, 21), 7))
SAVE_PATH = "/home/mangust1981/Документы/3Пайтон/wildberries_data.csv"
MAX_PRODUCTS = 13740
PAUSE_INCREASE_FACTOR = 1.2
BAD_PROXIES = set()
USER_AGENT_CYCLE = itertools.cycle(USER_AGENTS)
SCREEN_RESOLUTIONS = [(1920, 1080), (1366, 768), (1440, 900)]
start_time = datetime.now()

def test_proxy(driver, proxy_test_pause):
    try:
        driver.get("https://www.google.com")
        WebDriverWait(driver, proxy_test_pause).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        logging.info("Прокси работает")
        return True
    except Exception as e:
        logging.error(f"Прокси не работает: {e}")
        return False

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
        logging.info(f"Драйвер успешно создан с прокси: {proxy}")
        return driver
    except Exception as e:
        logging.error(f"Ошибка при создания драйвера с прокси {proxy}: {e}")
        return None

def get_working_driver(proxy_list):
    if not proxy_list:
        print("Список прокси пуст, работаем без прокси")
        logging.info("Список прокси пуст, работаем без прокси")
        return setup_driver(None)
    available_proxies = [p for p in proxy_list if p not in BAD_PROXIES]
    if not available_proxies:
        logging.error("Все прокси в списке не работают, завершаем работу")
        return None
    proxy = random.choice(available_proxies)
    driver = setup_driver(proxy)
    if not driver:
        BAD_PROXIES.add(proxy)
        logging.warning(f"Прокси {proxy} не работает, добавлен в список нерабочих")
        return None
    print(f"Проверяем прокси: {proxy}")
    if test_proxy(driver, FIRST_PAGE_PROXY_TEST_PAUSE):
        print(f"Используем прокси: {proxy}")
        return driver
    driver.quit()
    BAD_PROXIES.add(proxy)
    logging.warning(f"Прокси {proxy} не работает, добавлен в список нерабочих")
    return None

def simulate_mouse_movement(driver):
    try:
        driver.execute_script("window.scrollTo(0, 0);")
        for _ in range(3):
            x = random.randint(100, 500)
            y = random.randint(100, 500)
            driver.execute_script(f"window.scrollBy({x}, {y});")
            time.sleep(random.uniform(0.1, 0.3))
        logging.info("Имитация движения мыши выполнена")
    except Exception as e:
        logging.error(f"Ошибка при имитации движения мыши: {e}")

def simulate_random_click(driver, items):
    try:
        if not items:
            logging.info("Нет товаров для случайного клика")
            return
        time.sleep(random.uniform(0.5, 2))
        simulate_mouse_movement(driver)
        random_item = random.choice(items)
        link = random_item.find_element(By.CLASS_NAME, "product-card__link")
        link.click()
        time.sleep(random.uniform(2, 5))
        driver.back()
        logging.info("Выполнен случайный клик по товару и возврат назад")
        WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.CLASS_NAME, "product-card__wrapper")))
    except Exception as e:
        logging.error(f"Ошибка при выполнении случайного клика: {e}")

def simulate_random_scrolls(driver):
    try:
        page_height = driver.execute_script("return document.body.scrollHeight")
        num_scrolls = random.randint(2, 4)
        for _ in range(num_scrolls):
            scroll_position = random.uniform(0.2 * page_height, 0.8 * page_height)
            driver.execute_script(f"window.scrollTo(0, {scroll_position});")
            time.sleep(random.uniform(0.5, 1.5))
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        logging.info(f"Выполнены случайные скроллы ({num_scrolls} раз)")
    except Exception as e:
        logging.error(f"Ошибка при выполнении случайных скроллов: {e}")

def simulate_random_category_or_search_click(driver):
    try:
        actions = [("category", "menu-burger__link"), ("search", "search__input")]
        action_type, class_name = random.choice(actions)
        element = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.CLASS_NAME, class_name)))
        time.sleep(random.uniform(0.5, 2))
        simulate_mouse_movement(driver)
        if action_type == "search":
            element.click()
            element.send_keys("юбки женские")
            time.sleep(random.uniform(1, 2))
            driver.back()
            logging.info("Выполнен случайный клик по поисковой строке и возврат назад")
        else:
            element.click()
            time.sleep(random.uniform(2, 4))
            driver.back()
            logging.info("Выполнен случайный клик по категории и возврат назад")
        WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.CLASS_NAME, "product-card__wrapper")))
    except Exception as e:
        logging.error(f"Ошибка при выполнении случайного клика по категории/поиску: {e}")

def simulate_category_wandering(driver):
    try:
        driver.get("https://www.wildberries.ru/catalog/zhenshchinam/odezhda/platya")
        time.sleep(random.uniform(3, 6))
        logging.info("Выполнен переход в категорию 'Платья' для имитации")
        driver.back()
        WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.CLASS_NAME, "product-card__wrapper")))
    except Exception as e:
        logging.error(f"Ошибка при переходе в другую категорию: {e}")

def perform_tricks_after_rotation(driver, page_number, category_url):
    logging.info(f"Выполняем 'хитрости' после ротации прокси на странице {page_number}")
    driver.delete_all_cookies()
    logging.info("Куки и кэш очищены после ротации прокси")
    if page_number > 1:
        prev_page = page_number - 1
        driver.get(category_url + f"&page={prev_page}")
        logging.info(f"Вернулись на предыдущую страницу {prev_page} для имитации")
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(random.uniform(3, 6))
    driver.get("https://www.wildberries.ru/")
    logging.info("Перешли на главную страницу Wildberries для имитации")
    if random.random() < 0.3:
        driver.refresh()
        logging.info("Выполнено случайное обновление главной страницы")
    time.sleep(random.uniform(5, 10))
    driver.get(category_url + f"&page={page_number}")
    logging.info(f"Вернулись на страницу {page_number} после 'прогулки'")
    if random.random() > 0.5:
        driver.refresh()
        logging.info("Выполнено случайное обновление страницы")
        WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.CLASS_NAME, "product-card__wrapper")))
    delay = random.uniform(10, 20)
    time.sleep(delay)
    logging.info(f"Случайная задержка после ротации: {delay:.2f} секунд")

def check_for_captcha(driver):
    try:
        driver.find_element(By.CLASS_NAME, "captcha__container")
        logging.warning("Обнаружена CAPTCHA на странице")
        return True
    except NoSuchElementException:
        logging.info("CAPTCHA не обнаружена")
        return False

def handle_captcha(driver, page_number, category_url):
    logging.info(f"Обработка CAPTCHA на странице {page_number}")
    pause = random.uniform(5, 10)
    time.sleep(pause)
    logging.info(f"Пауза перед обработкой CAPTCHA: {pause:.2f} секунд")
    driver.delete_all_cookies()
    logging.info("Куки очищены для обхода CAPTCHA")
    driver = rotate_proxy(driver, page_number, category_url.split("&page=")[0])
    logging.info("Драйвер перезапущен с новым User-Agent для обхода CAPTCHA")
    return driver

def rotate_proxy(driver, page_number, category_url, max_attempts=3):
    driver.quit()
    logging.info(f"Ротация прокси на странице {page_number}")
    attempts = 0
    while attempts < max_attempts:
        new_driver = get_working_driver(PROXY_LIST_EUROPE_USA)
        if new_driver:
            perform_tricks_after_rotation(new_driver, page_number, category_url)
            return new_driver
        attempts += 1
        logging.warning(f"Попытка {attempts}/{max_attempts} найти рабочий прокси не удалась")
    logging.error(f"Не удалось найти рабочий прокси после {max_attempts} попыток, завершаем работу")
    raise Exception("Не удалось найти рабочий прокси после максимального количества попыток")

def clean_price(price_text):
    cleaned = ''.join(filter(str.isdigit, price_text))
    return cleaned if cleaned else "0"

def clean_name(name_text):
    return name_text.replace("/", "").strip()

def clean_rating(rating_text):
    cleaned = ''.join(c for c in rating_text if c.isdigit() or c == '.')
    return cleaned if cleaned else "0"

def clean_discount(discount_text):
    cleaned = ''.join(filter(str.isdigit, discount_text))
    return cleaned if cleaned else "0"

def parse_wildberries(category_url):
    driver = None
    attempts = 0
    max_attempts = 3
    while attempts < max_attempts:
        try:
            driver = get_working_driver(PROXY_LIST_EUROPE_USA)
            if not driver:
                logging.error("Не удалось создать драйвер, завершаем работу")
                return []
            driver.get(category_url)
            logging.info(f"Открыта страница: {category_url}")
            break
        except (TimeoutException, WebDriverException) as e:
            logging.error(f"Ошибка загрузки страницы: {e}")
            attempts += 1
            if driver:
                driver.quit()
            if attempts == max_attempts:
                logging.error("Не удалось загрузить начальную страницу после всех попыток")
                return []
            time.sleep(random.uniform(2, 5))
            continue
    all_products = []
    unique_product_links = set()
    page_number = 1
    max_retries = 5
    max_rotation_cycles = 5
    while True:
        if page_number > 1 and (page_number - 1) % PROXY_ROTATION_INTERVAL == 0:
            driver = rotate_proxy(driver, page_number, category_url.split("&page=")[0])
            logging.info(f"Переоткрыта страница {page_number} после ротации прокси")
        if page_number == 1 or (page_number - 1) % PROXY_ROTATION_INTERVAL == 0:
            scroll_down_pause = FIRST_PAGE_SCROLL_DOWN_PAUSE
            scroll_up_pause = FIRST_PAGE_SCROLL_UP_PAUSE
            scroll_down_again_pause = FIRST_PAGE_SCROLL_DOWN_AGAIN_PAUSE
            page_transition_pause = FIRST_PAGE_PAGE_TRANSITION_PAUSE
            logging.info(f"Страница {page_number}: применяем тайминги первого блока (после ротации)")
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
            logging.info(f"Страница {page_number}: применяем тайминги блока {block_index + 2}")
        max_scroll_attempts = 10
        scroll_attempts = 0
        last_height = driver.execute_script("return document.body.scrollHeight")
        if random.random() < 0.5:
            simulate_random_scrolls(driver)
            time.sleep(random.uniform(0.5, 2))
            items = driver.find_elements(By.CLASS_NAME, "product-card__wrapper")
            cycle_page = (page_number - 1) % 20 + 1
            if cycle_page in CLICK_PAGES:
                simulate_random_click(driver, items)
                logging.info(f"Случайный клик выполнен на странице {page_number} (цикл: {cycle_page})")
        while scroll_attempts < max_scroll_attempts:
            if check_for_captcha(driver):
                driver = handle_captcha(driver, page_number, category_url)
                driver.get(category_url + f"&page={page_number}")
                logging.info(f"Переоткрыта страница {page_number} после обработки CAPTCHA")
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            current_scroll_down_pause = scroll_down_pause * (PAUSE_INCREASE_FACTOR ** scroll_attempts)
            time.sleep(current_scroll_down_pause)
            driver.execute_script("window.scrollTo(0, 0);")
            current_scroll_up_pause = scroll_up_pause * (PAUSE_INCREASE_FACTOR ** scroll_attempts)
            time.sleep(current_scroll_up_pause)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            current_scroll_down_again_pause = scroll_down_again_pause * (PAUSE_INCREASE_FACTOR ** scroll_attempts)
            time.sleep(current_scroll_down_again_pause)
            new_height = driver.execute_script("return document.body.scrollHeight")
            items = driver.find_elements(By.CLASS_NAME, "product-card__wrapper")
            logging.info(f"Страница {page_number}, Прокрутка {scroll_attempts + 1}: найдено {len(items)} товаров")
            print(f"Страница {page_number}, Прокрутка {scroll_attempts + 1}: найдено {len(items)} товаров")
            if new_height == last_height:
                logging.info(f"Страница {page_number}: новых товаров не подгрузилось, завершаем прокрутку")
                break
            last_height = new_height
            scroll_attempts += 1
        if random.random() >= 0.5:
            simulate_random_scrolls(driver)
            time.sleep(random.uniform(0.5, 2))
            items = driver.find_elements(By.CLASS_NAME, "product-card__wrapper")
            cycle_page = (page_number - 1) % 20 + 1
            if cycle_page in CLICK_PAGES:
                simulate_random_click(driver, items)
                logging.info(f"Случайный клик выполнен на странице {page_number} (цикл: {cycle_page})")
        if random.random() < 0.3:
            simulate_random_category_or_search_click(driver)
            logging.info(f"Случайный клик по категории/поиску выполнен на странице {page_number}")
        if random.random() < 0.1:
            simulate_category_wandering(driver)
            logging.info(f"Выполнен случайный переход в другую категорию на странице {page_number}")
        items = driver.find_elements(By.CLASS_NAME, "product-card__wrapper")
        if len(items) == 0:
            logging.warning(f"На странице {page_number} не найдено товаров после всех прокруток, пробуем сменить прокси")
            driver = rotate_proxy(driver, page_number, category_url.split("&page=")[0])
            driver.get(category_url + f"&page={page_number}")
            logging.info(f"Переоткрыта страница {page_number} после принудительной ротации прокси")
            scroll_attempts = 0
            last_height = driver.execute_script("return document.body.scrollHeight")
            while scroll_attempts < max_scroll_attempts:
                if check_for_captcha(driver):
                    driver = handle_captcha(driver, page_number, category_url)
                    driver.get(category_url + f"&page={page_number}")
                    logging.info(f"Переоткрыта страница {page_number} после обработки CAPTCHA")
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(FIRST_PAGE_SCROLL_DOWN_PAUSE)
                driver.execute_script("window.scrollTo(0, 0);")
                time.sleep(FIRST_PAGE_SCROLL_UP_PAUSE)
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(FIRST_PAGE_SCROLL_DOWN_AGAIN_PAUSE)
                new_height = driver.execute_script("return document.body.scrollHeight")
                items = driver.find_elements(By.CLASS_NAME, "product-card__wrapper")
                logging.info(f"Страница {page_number}, Прокрутка {scroll_attempts + 1} (после ротации): найдено {len(items)} товаров")
                print(f"Страница {page_number}, Прокрутка {scroll_attempts + 1} (после ротации): найдено {len(items)} товаров")
                if new_height == last_height:
                    logging.info(f"Страница {page_number}: новых товаров не подгрузилось после ротации, завершаем прокрутку")
                    break
                last_height = new_height
                scroll_attempts += 1
        rotation_cycles = 0
        while rotation_cycles < max_rotation_cycles:
            retry_count = 0
            while retry_count < max_retries:
                if check_for_captcha(driver):
                    driver = handle_captcha(driver, page_number, category_url)
                    driver.get(category_url + f"&page={page_number}")
                    logging.info(f"Переоткрыта страница {page_number} после обработки CAPTCHA")
                try:
                    items = WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.CLASS_NAME, "product-card__wrapper")))
                    break
                except TimeoutException as e:
                    retry_count += 1
                    logging.warning(f"Ошибка загрузки товаров на странице {page_number}, попытка {retry_count}/{max_retries}: {e}")
                    if retry_count == max_retries:
                        rotation_cycles += 1
                        if rotation_cycles == max_rotation_cycles:
                            logging.warning(f"Не удалось загрузить товары на странице {page_number} после {max_rotation_cycles} циклов ротации, пропускаем страницу")
                            page_number += 1
                            driver.get(category_url + f"&page={page_number}")
                            logging.info(f"Переходим на страницу {page_number} после пропуска")
                            rotation_cycles = 0
                            break
                        logging.error(f"Не удалось загрузить товары на странице {page_number} после {max_retries} попыток, пробуем сменить прокси (цикл {rotation_cycles}/{max_rotation_cycles})")
                        driver = rotate_proxy(driver, page_number, category_url.split("&page=")[0])
                        driver.get(category_url + f"&page={page_number}")
                        logging.info(f"Переоткрыта страница {page_number} после {max_retries} неудачных попыток загрузки")
                        retry_count = 0
                        continue
                    current_page_transition_pause = page_transition_pause * (PAUSE_INCREASE_FACTOR ** retry_count)
                    driver.refresh()
                    time.sleep(current_page_transition_pause)
            if retry_count < max_retries:
                break
        logging.info(f"Страница {page_number}: всего найдено {len(items)} товаров")
        print(f"Страница {page_number}: всего найдено {len(items)} товаров")
        for item in items:
            if len(all_products) >= MAX_PRODUCTS:
                logging.info(f"Достигнут лимит в {MAX_PRODUCTS} товаров, завершаем парсинг")
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
                try:
                    discount = item.find_element(By.CLASS_NAME, "discount").text.strip()
                    cleaned_discount = clean_discount(discount)
                except:
                    cleaned_discount = "0"
                try:
                    rating = item.find_element(By.CLASS_NAME, "product-card__rating").text.strip()
                    cleaned_rating = clean_rating(rating)
                except:
                    cleaned_rating = "0"
                try:
                    link = item.find_element(By.CLASS_NAME, "product-card__link").get_attribute("href")
                except:
                    link = ""
                if link in unique_product_links:
                    logging.info(f"Пропущен дубликат товара: {cleaned_name}, Ссылка: {link}")
                    continue
                unique_product_links.add(link)
                logging.info(f"Добавлена новая уникальная ссылка: {link}")
                all_products.append({
                    "Название": cleaned_name,
                    "Цена": cleaned_price,
                    "Скидка (%)": cleaned_discount,
                    "Рейтинг": cleaned_rating,
                    "Ссылка": link
                })
                logging.info(f"Спарсен товар: {cleaned_name}, Цена: {cleaned_price}")
            except Exception as e:
                logging.error(f"Ошибка при парсинге товара: {e}")
                continue
        if page_number % 10 == 0:
            save_to_csv(all_products)
            logging.info(f"Промежуточное сохранение на странице {page_number}")
        pagination_retries = 7
        pagination_class_found = False
        for attempt in range(pagination_retries):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(page_transition_pause)
            if check_for_captcha(driver):
                driver = handle_captcha(driver, page_number, category_url)
                driver.get(category_url + f"&page={page_number}")
                logging.info(f"Переоткрыта страница {page_number} после обработки CAPTCHA")
            try:
                next_button = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "pagination-next")))
                pagination_class_found = True
                if "disabled" in next_button.get_attribute("class"):
                    logging.info("Кнопка 'Следующая страница' неактивна, завершаем")
                    driver.quit()
                    save_to_csv(all_products)
                    return all_products
                next_button.click()
                current_page_transition_pause = page_transition_pause * (PAUSE_INCREASE_FACTOR ** attempt)
                time.sleep(current_page_transition_pause + random.uniform(1, 3))
                page_number += 1
                break
            except Exception as e:
                logging.warning(f"Не удалось найти кнопку пагинации по классу pagination-next на странице {page_number}, попытка {attempt + 1}/{pagination_retries}: {e}")
                try:
                    next_button = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, "//a[contains(text(), 'Далее')]")))
                    logging.info("Кнопка пагинации найдена по тексту 'Далее' (альтернативный способ)")
                    if "disabled" in next_button.get_attribute("class"):
                        logging.info("Кнопка 'Далее' неактивна, завершаем")
                        driver.quit()
                        save_to_csv(all_products)
                        return all_products
                    next_button.click()
                    current_page_transition_pause = page_transition_pause * (PAUSE_INCREASE_FACTOR ** attempt)
                    time.sleep(current_page_transition_pause + random.uniform(1, 3))
                    page_number += 1
                    break
                except Exception as e_alt:
                    logging.warning(f"Не удалось найти кнопку пагинации по тексту 'Далее' на странице {page_number}, попытка {attempt + 1}/{pagination_retries}: {e_alt}")
                    if attempt == pagination_retries - 1:
                        logging.info(f"Пагинация не найдена после {pagination_retries} попыток, завершаем сбор товаров на странице {page_number}")
                        driver.quit()
                        save_to_csv(all_products)
                        return all_products
                    current_page_transition_pause = page_transition_pause * (PAUSE_INCREASE_FACTOR ** attempt)
                    driver.refresh()
                    time.sleep(current_page_transition_pause)
        if not pagination_class_found:
            logging.warning(f"Класс pagination-next не найден на странице {page_number}, использовался альтернативный способ поиска кнопки (по тексту 'Далее')")
    logging.info(f"Всего собрано товаров: {len(all_products)}")
    print(f"Всего собрано товаров: {len(all_products)}")
    duration = (datetime.now() - start_time).total_seconds() / 60
    logging.info(f"Парсинг завершен за {duration:.2f} минут")
    print(f"Парсинг завершен за {duration:.2f} минут")
    driver.quit()
    logging.info("Парсинг завершен")
    return all_products

def save_to_csv(data, filename=SAVE_PATH):
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False, encoding="utf-8-sig", sep=";")
    logging.info(f"Данные сохранены в {filename}")
    print(f"Данные сохранены в {filename}")

if __name__ == "__main__":
    url = "https://www.wildberries.ru/catalog/zhenshchinam/odezhda/yubki?sort=popular&page=1&f57021=94964"
    products = parse_wildberries(url)
    save_to_csv(products)
