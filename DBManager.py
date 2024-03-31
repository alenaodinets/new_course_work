import psycopg2
from config import config


class DBManager:

    def __init__(self) -> None:
        self.param = config()

    def create_database(self, database_name):
        """Создание базы данных."""

        conn = psycopg2.connect(dbname='postgres', **self.param)
        conn.autocommit = True

        cur = conn.cursor()

        cur.execute(f'DROP DATABASE IF EXISTS {database_name}')
        cur.execute(f'CREATE DATABASE {database_name}')

        cur.close()
        conn.close()

    def create_tables(self, database_name) -> None:
        """ Создание таблицы"""
        with psycopg2.connect(dbname=database_name, **self.param) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS companies (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    vacancies_count INT
                    )
                    """)

                cur.execute("""
                    CREATE TABLE IF NOT EXISTS vacancies (
                    id SERIAL PRIMARY KEY,
                    company_id INT REFERENCES companies(id),
                    title VARCHAR(500),
                    salary_from INT,
                    salary_to INT,
                    currency VARCHAR(10),
                    city VARCHAR(255),
                    link VARCHAR(255)
                    )
                    """)
        conn.close()

    def insert_data(self, database_name, employers_info) -> None:
        """
        Vетод для вставки данных о вакансиях и работодателях в
        соответствующие таблицы
        """
        with psycopg2.connect(dbname=database_name, **self.param) as conn:
            with conn.cursor() as cur:
                for employer_name, employer_info in employers_info.items():
                    cur.execute(
                        """INSERT INTO companies (name, vacancies_count)
                    VALUES (%s, %s)
                    RETURNING id""", (employer_name, employer_info['vacancies_count']))
                company_id = cur.fetchone()[0]
                for vacancy in employer_info['vacancies']:
                    cur.execute(
                        """INSERT INTO vacancies (company_id, title, salary_from, salary_to, currency, city, link) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                    (company_id, (vacancy['title']), (vacancy['salary_from']), (vacancy['salary_to']),
                     vacancy["currency"], vacancy["city"], vacancy["link"]))
        conn.close()

    def get_companies_and_vacancies_count(self, database_name):
        """Получает список всех компаний и количество вакансий у каждой компании."""
        with psycopg2.connect(dbname=database_name, **self.param) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT name, vacancies_count
                    FROM companies
                    """)
                result = cur.fetchall()
        conn.close()
        return result

    def get_all_vacancies(self, database_name):
        """Получает список всех вакансий"""
        with psycopg2.connect(dbname=database_name, **self.param) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT c.name AS company_name, v.title, v.salary_from, v.salary_to, v.currency, v.link
                    FROM vacancies AS v
                    JOIN companies AS c ON v.company_id = c.id
                    """)
                result = cur.fetchall()
        conn.close()
        return result

    def get_avg_salary(self, database_name):
        """Получает среднюю зарплату по вакансиям"""
        with psycopg2.connect(dbname=database_name, **self.param) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT AVG((salary_from + salary_to) / 2) AS avg_salary
                    FROM vacancies
                    WHERE salary_from IS NOT NULL AND salary_to IS NOT NULL
                    """)
                avg_salary = cur.fetchall()
        conn.close()
        return avg_salary

    def get_vacancies_with_higher_salary(self, database_name):
        """Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям"""
        with psycopg2.connect(dbname=database_name, **self.param) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                        SELECT c.name AS company_name, v.title, v.salary_from, v.salary_to, v.currency, v.link
                        FROM vacancies AS v
                        JOIN companies AS c ON v.company_id = c.id
                        WHERE (v.salary_from + v.salary_to)/2 > (SELECT AVG((salary_from + salary_to) / 2) AS avg_salary
                        FROM vacancies
                        WHERE salary_from IS NOT NULL AND salary_to IS NOT NULL)
                        """)

                results = cur.fetchall()
        conn.close()
        return results

    def get_vacancies_with_keyword(self, database_name, keyword: str):
        """Получает список всех вакансий, в названии которых содержатся переданные в метод слова"""
        with psycopg2.connect(dbname=database_name, **self.param) as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT c.name AS company_name, v.title, v.salary_from, v.salary_to, v.currency, v.link
                    FROM vacancies AS v
                    JOIN companies AS c ON v.company_id = c.id
                    WHERE v.title LIKE '%{keyword}%'
                    """)
                result = cur.fetchall()
        conn.close()
        return result
