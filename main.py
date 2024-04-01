import dotenv

from parser import *
from DBManager import *

target_employers = ['Тинькофф', 'Компания Лимарк', 'Carbon Soft', 'Тензор', 'ЭР-Телеком']

employers_info = parse_employers_info(target_employers)


dotenv.load_dotenv()


db_manager = DBManager()

db_manager.create_database('parser')

db_manager.create_tables('parser')

db_manager.insert_data('parser', employers_info)

companies_and_vacancies = db_manager.get_companies_and_vacancies_count('parser')
all_vacancies = db_manager.get_all_vacancies('parser')
avg_salary = db_manager.get_avg_salary('parser')
vacancies_higher_salary = db_manager.get_vacancies_with_higher_salary('parser')
keyword = 'Инженер-электрик'
keyword_vacancies = db_manager.get_vacancies_with_keyword('parser', keyword)

print("Список компаний и количество вакансий в компаниях:")
for row in db_manager.get_companies_and_vacancies_count('parser'):
    print(f"{row[0]} - {row[1]}")

print("Список всех вакансий с указанием названия компании:")
for row in db_manager.get_all_vacancies('parser'):
    print(f"{row[0]} - {row[1]}")

print("Получает среднюю зарплату по вакансиям:")
for row in db_manager.get_avg_salary('parser'):
    print(f"{row[0]}")

print("Список всех вакансий, у которых зарплата выше средней по всем вакансиям:")
for row in db_manager.get_vacancies_with_higher_salary('parser'):
    print(f"{row[0]}")


print("Список всех вакансий, в названии которых содержатся переданные в метод слова:")
for row in db_manager.get_vacancies_with_keyword('parser', keyword):
    print(f"{row[0]} ")
print(companies_and_vacancies)

