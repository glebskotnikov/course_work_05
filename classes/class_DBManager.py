import psycopg2


class DBManager:
    def __init__(self, params):
        self.conn = psycopg2.connect(**params)
        self.conn.autocommit = True
        self.cur = self.conn.cursor()

    def get_companies_and_vacancies_count(self):
        # Получает список всех компаний и количество вакансий у каждой компании.
        self.cur.execute("""
        SELECT company_name, COUNT(*) FROM vacancies
        JOIN companies USING (company_id)
        GROUP BY company_name
        """)
        count_vac = self.cur.fetchall()
        print(count_vac)

    def get_all_vacancies(self):
        # Получает список всех вакансий с указанием названия компании,
        # названия вакансии и зарплаты и ссылки на вакансию.
        self.cur.execute("""
        SELECT company_name, vacancy_name, salary_from, salary_to, url
        FROM vacancies
        JOIN companies USING(company_id)
        """)
        list_vac = self.cur.fetchall()
        print(list_vac)

    def get_avg_salary(self):
        # Получает среднюю зарплату по вакансиям.
        self.cur.execute("""
        SELECT AVG(salary_from) as AVG_salary_from, 
        AVG(salary_to) as AVG_salary_to
        FROM vacancies
        """)
        avg_list = self.cur.fetchall()
        print(avg_list)

    def get_vacancies_with_higher_salary(self):
        # Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям.
        self.cur.execute("""
        SELECT * FROM vacancies
        WHERE salary_from > (SELECT AVG(salary_from) FROM vacancies)
        OR salary_to > (SELECT AVG(salary_to) FROM vacancies)
        """)
        list_vac = self.cur.fetchall()
        print(list_vac)

    def get_vacancies_with_keyword(self, words):
        # Получает список всех вакансий, в названии которых содержатся переданные в метод слова, например python.
        for word in words.split():
            self.cur.execute(f"SELECT * FROM vacancies WHERE vacancy_name LIKE '%{word}%'")
        list_vac = self.cur.fetchall()
        print(list_vac)

    def close_connection(self):
        # Закрывает все соединения
        self.cur.close()
        self.conn.close()
