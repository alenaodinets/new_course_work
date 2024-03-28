import os

from dotenv import load_dotenv

from parser import *
from DBManager import *

target_employers = ['Тинькофф', 'Компания Лимарк', 'Carbon Soft', 'Тензор', 'ЭР-Телеком']

employers_info = parse_employers_info(target_employers)

load_dotenv()


db_manager = DBManager(
    host=os.getenv("host"),
    database=os.getenv("DB"),
    user=os.getenv("user_name"),
    password=os.getenv("password")
)


db_manager.create_tables()
db_manager.insert_data(employers_info)


companies_and_vacancies = db_manager.get_companies_and_vacancies_count()
all_vacancies = db_manager.get_all_vacancies()
avg_salary = db_manager.get_avg_salary()
vacancies_higher_salary = db_manager.get_vacancies_with_higher_salary()
keyword_vacancies = db_manager.get_vacancies_with_keyword('Инженер-электрик')

print(companies_and_vacancies)


db_manager.close_connection()