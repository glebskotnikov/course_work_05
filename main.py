from classes.class_DBManager import DBManager
from config.config import config
from utils.utils import *


def main():
    db_name = 'course_work_05'
    params = config()
    conn = None
    companies = {1740: 'Яндекс',
                 1111672: 'GeekBrains',
                 2180: 'Ozon',
                 2863076: 'Skillbox',
                 69797: 'Электронный город (ООО Новотелеком)',
                 64174: '2Гис',
                 3776: 'Мтс диджитал',
                 84585: 'Авито',
                 1122462: 'Skyeng',
                 15478: 'VK'}

    vacancies = get_vacancies(companies)
    create_database(db_name, params)
    print(f"БД {db_name} успешно создана")
    params.update({'dbname': db_name})
    try:
        with psycopg2.connect(**params) as conn:
            with conn.cursor() as cur:
                create_companies_table(cur)
                print("Таблица companies успешно создана")
                create_vacancies_table(cur)
                print("Таблица vacancies успешно создана")
                insert_companies_data(cur, companies)
                print("Таблица companies успешно заполнена")
                insert_vacancies_data(cur, vacancies)
                print("Таблица vacancies успешно заполнена")
                add_foreign_keys(cur)
                print("Связывание таблиц прошло успешно")
    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    db_manager = DBManager(params)
    # db_manager.get_companies_and_vacancies_count()
    # db_manager.get_all_vacancies()
    # db_manager.get_avg_salary()
    # db_manager.get_vacancies_with_higher_salary()
    # db_manager.get_vacancies_with_keyword('Разработчик')
    db_manager.close_connection()


if __name__ == '__main__':
    main()
