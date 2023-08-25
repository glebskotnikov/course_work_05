import psycopg2 as psycopg2
import requests as requests
from tqdm import tqdm

URL_HH = 'https://api.hh.ru/vacancies'


def get_vacancies(employees):
    """
    Функция для получения страницы со списком вакансий.
    """
    all_vacancies = []
    for employee in tqdm(employees, ncols=155, desc='Получаем данные от компаний'):
        params = {
            'employer_id': employee,
            'page': 0,
            'per_page': 100,
            'only_with_salary': True
        }

        response = requests.get(URL_HH, params)
        data = response.json()['items']
        page_count = 1
        if len(data) == 100:
            while True:
                params['page'] = page_count
                response = requests.get(URL_HH, params)
                new_data = response.json()['items']
                data += new_data
                if len(data) == 1000:
                    break
                if len(new_data) == 100:
                    page_count += 1
                else:
                    break
        for item in data:
            all_vacancies.append({'vacancy_id': item['id'],
                                  'vacancy_name': item['name'],
                                  'city': item['area']['name'],
                                  'salary_from': item['salary']['from'],
                                  'salary_to': item['salary']['to'],
                                  'published_at': item['published_at'],
                                  'url': item['alternate_url'],
                                  'company_id': item['employer']['id']})
    print(f'\nЗагружено {len(all_vacancies)} вакансий')
    return all_vacancies


def create_database(db_name, params) -> None:
    """Создает новую базу данных."""
    conn = psycopg2.connect(dbname='postgres', **params)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(f"DROP DATABASE IF EXISTS {db_name}")
    cur.execute(f"CREATE DATABASE {db_name}")
    cur.close()
    conn.close()


def create_companies_table(cur) -> None:
    """Создает таблицу companies."""
    cur.execute("""
    DROP TABLE IF EXISTS companies;
    CREATE TABLE companies(
    company_id int PRIMARY KEY,
    company_name varchar(100))
    """)


def create_vacancies_table(cur) -> None:
    """Создает таблицу vacancies."""
    cur.execute("""
    DROP TABLE IF EXISTS vacancies;
    CREATE TABLE vacancies(
    vacancy_id int,
    vacancy_name varchar(100),
    city varchar(50),
    salary_from int,
    salary_to int,
    published_at date,
    url text,
    company_id int)
    """)


def insert_companies_data(cur, companies) -> None:
    """Добавляет данные в таблицу companies."""
    for company in companies:
        cur.execute("""
        INSERT INTO companies (company_id, company_name)
        VALUES (%s, %s)""",
                    (company, companies[company]))


def insert_vacancies_data(cur, vacancies: list[dict]) -> None:
    """Добавляет данные в таблицу vacancies."""
    for vacancy in vacancies:
        cur.execute("""
        INSERT INTO vacancies(vacancy_id, vacancy_name, city, salary_from, salary_to, published_at, url, company_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (vacancy['vacancy_id'], vacancy['vacancy_name'], vacancy['city'],
              vacancy['salary_from'], vacancy['salary_to'], vacancy['published_at'],
              vacancy['url'], vacancy['company_id']))


def add_foreign_keys(cur) -> None:
    """Добавляет foreign key со ссылкой на company_id в таблицу vacancies."""
    cur.execute("""
    ALTER TABLE vacancies ADD CONSTRAINT fk_vacancies_company_id FOREIGN KEY (company_id) 
    REFERENCES companies(company_id)""")
