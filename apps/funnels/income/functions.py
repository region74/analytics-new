from typing import Dict, Any
import pandas as pd
import datetime
from pathlib import Path

from apps.sources.models import PaymentAnalytic
from apps.utils import queryset_as_dataframe
from config.settings import PROJECT_DATA


def parse_email(value: str) -> str:
    return value.replace(' ', '').lower()


def format_percent(x):
    return '{:.2%}'.format(x)


def data_preparation(start_event: datetime.date, end_event: datetime.date, select_event: list = None) -> pd.DataFrame:
    file_preorders = Path(PROJECT_DATA) / 'intensives_preorders.pkl'
    file_registrations = Path(PROJECT_DATA) / 'intensives_registrations.pkl'
    file_members = Path(PROJECT_DATA) / 'intensives_members.pkl'

    # Чтение данных из файла .pkl
    data_preorders = pd.read_pickle(file_preorders)
    data_registrations = pd.read_pickle(file_registrations)
    data_members = pd.read_pickle(file_members)

    # Подготовка таблиц
    preorders: pd.DataFrame = data_preorders
    preorders.insert(0, 'type', 'Предзаказы', False)

    registrations: pd.DataFrame = data_registrations
    registrations.insert(0, 'type', 'Регистрации', False)
    registrations.loc[registrations['course'] == 'Акции', 'type'] = 'Предзаказы'

    members: pd.DataFrame = data_members
    members.insert(0, 'type', 'Участники', False)

    # Сборка единого frame
    full_frame = pd.concat([preorders, registrations, members], ignore_index=True)
    full_frame['email'] = full_frame['email'].str.lower()
    # Выбор данных из диапазона
    filtered_frame = full_frame[(full_frame['date'] >= start_event) & (full_frame['date'] <= end_event)]
    # Фильтруем выбранные мероприятия
    if select_event:
        if select_event[0] == 'Все':
            return filtered_frame
        else:
            filtered_frame = filtered_frame[filtered_frame['course'].isin(select_event)]
    return filtered_frame


""" *********************ОТЧЕТ ОБОРОТ С ВОРОНКИ*********************"""


def get_payment(date_from: datetime.date, date_to: datetime.date) -> Dict[str, Any]:
    df = queryset_as_dataframe(PaymentAnalytic.objects.all())
    df.drop(columns=["id", "date_created", "date_last_paid", "amocrm_id","roistat_url"], inplace=True)
    df['email'] = df['email'].apply(parse_email)
    df['date_payment'] = pd.to_datetime(df['date_payment'], format='%d.%m.%Y')
    # Делаем выборку данных
    selected_data = df[
        (df['date_payment'] >= pd.to_datetime(date_from)) & (df['date_payment'] <= pd.to_datetime(date_to)) & (
                df['type'] != 'surcharge')]
    selected_data['profit'] = selected_data['profit'].astype(int)
    result = selected_data['profit'].sum()
    # Фрейм диапазона для обработки в функции просмотра таблиц мероприятий
    explore_frame = selected_data.loc[:, ['email', 'profit']]
    # Сохраняем пары значений в список списков
    email_list = explore_frame.values.tolist()
    data = {'sum': result, 'data': email_list}
    return data


def get_funnel_payment(event_df: datetime.date, event_dt: datetime.date, emails) -> int:
    full_data = data_preparation(event_df, event_dt, None)
    check = full_data['email']
    result_summ = 0
    for email, price in emails:
        if email in check.dropna().tolist():
            result_summ += price
    return result_summ


def get_data(date_from: datetime.date, date_to: datetime.date, start_event: datetime.date, end_event: datetime.date):
    try:
        payment = get_payment(date_from, date_to)
        funnel_payment = get_funnel_payment(start_event, end_event, payment.get('data'))
        result = {'payment_all': payment.get('sum'), 'payment_event': funnel_payment}
        return result
    except Exception:
        result_error = {'payment_all': 0, 'payment_event': 0}
        return result_error


""" *********************ОТЧЕТ ОБОРОТ ПО МЕРОПРИЯТИЯМ*********************"""


# Получение оплат
def get_payment_event(date_from: datetime.date) -> pd.DataFrame:
    df = queryset_as_dataframe(PaymentAnalytic.objects.all())
    df.drop(columns=["id", "date_created", "date_last_paid", "amocrm_id","roistat_url"], inplace=True)
    df['email'] = df['email'].apply(parse_email)
    df['date_payment'] = pd.to_datetime(df['date_payment'], format='%d.%m.%Y')
    # Делаем выборку данных
    selected_data = df[
        (df['date_payment'] >= pd.to_datetime(date_from)) & (
                df['type'] != 'surcharge')]
    selected_data['profit'] = selected_data['profit'].astype(int)
    explore_frame = selected_data.loc[:, ['email', 'profit', 'date_payment']]
    return explore_frame


# Формирование отчета
def get_funnel_payment_event(start_event: datetime.date, end_event: datetime.date, start_pay: datetime.date,
                             end_pay: datetime.date,
                             select_event: list) -> pd.DataFrame:
    # Это значение нужно для того, чтобы понимать на сколько дней сдвигать динамический фильтр массива оплат
    date_difference = end_pay - start_pay
    # Преобразование разницы в количество дней (целое число)
    date_difference_in_days = date_difference.days
    # Получаем данные из БД
    dataset = data_preparation(start_event, end_event, select_event).fillna('empty').drop_duplicates()
    # Получаем df оплат
    payments = get_payment_event(start_pay)

    # Результирующий dataframe
    result_dataframe = pd.DataFrame(
        columns=['date', 'event', 'full_profit', 'reg_profit', 'peop_profit', 'so_profit', 'email'])
    # Логика работает от почты из набора документов
    for items in dataset.itertuples():
        # заходим в документ оплат и ищем там почту
        for row in payments.itertuples():
            if items[4] == row[1] and pd.Timestamp(items[3]) <= row[3] <= pd.Timestamp(
                    items[3] + datetime.timedelta(days=date_difference_in_days)):
                price_reg = row[2] if items[1] == 'Регистрации' else 0
                price_mem = row[2] if items[1] == 'Участники' else 0
                price_pre = row[2] if items[1] == 'Предзаказы' else 0
                rows = [items[3], items[2], row[2], price_reg, price_mem, price_pre, items[4]]
                result_dataframe.loc[len(result_dataframe)] = rows
    # Собираем финальный df
    int_columns = ['full_profit', 'reg_profit', 'peop_profit', 'so_profit']
    result_dataframe[int_columns] = result_dataframe[int_columns].astype(int)
    result_dataframe = result_dataframe.groupby(['date', 'event', 'email']).agg(
        {'full_profit': 'max', 'reg_profit': 'max', 'peop_profit': 'max', 'so_profit': 'max'}).reset_index()
    grouped = result_dataframe.groupby(['date', 'event']).agg(
        {'full_profit': 'sum', 'reg_profit': 'sum', 'peop_profit': 'sum', 'so_profit': 'sum'}).reset_index()
    grouped['percent_reg'] = grouped['reg_profit'] / grouped['full_profit']
    grouped['percent_peop'] = grouped['peop_profit'] / grouped['full_profit']
    grouped['percent_so'] = grouped['so_profit'] / grouped['full_profit']
    # Применяем форматирование вывода
    grouped['percent_reg'] = grouped['percent_reg'].apply(format_percent)
    grouped['percent_peop'] = grouped['percent_peop'].apply(format_percent)
    grouped['percent_so'] = grouped['percent_so'].apply(format_percent)
    return grouped


def get_report(data_filters: Dict) -> pd.DataFrame:
    # Фильтры
    event_from = data_filters.get('event_df')
    event_to = data_filters.get('event_dt')
    range_filters = list(data_filters.items())

    events_dict = {
        'type_all': 'Все',
        'type_intensiv_two': 'Интенсив 2 дня',
        'type_intensiv_three': 'Интенсив 3 дня',
        'type_intensiv_gpt': 'Интенсив chatGPT',
        'type_vebianrs': 'Вебинары',
        'type_mini_lesson': 'Мини-урок'
    }
    payments_dict = {
        'pay_1week': '1week',
        'pay_2week': '2week',
        'pay_4week': '4week',
        'pay_8week': '8week'
    }

    events_list = []
    for key, value in range_filters[2:8]:
        if value == True:
            events_list.append(events_dict.get(key))
    payments_list = []
    for key, value in range_filters[9:]:
        if value == True:
            payments_list.append(payments_dict.get(key))

    filter_event = events_list
    select_checkbox = payments_list
    custom_period = None

    # Возвращаемая таблица
    result_table = pd.DataFrame()

    # Работа с фильтрами
    if custom_period:
        pass
        # start_date_custom, end_date_custom = custom_period
        # start_date_pay = start_date_custom
        # end_date_pay = end_date_custom
        # result_table = get_funnel_payment_event(event_from, event_to, start_date_pay, end_date_pay, filter_event)

    elif select_checkbox:
        checkbox_values = {
            '1week': 7,
            '2week': 14,
            '4week': 28,
            '8week': 56
        }
        if len(select_checkbox) == 1:
            start_date_pay = event_from
            end_date_pay = event_from + datetime.timedelta(
                days=checkbox_values.get(select_checkbox[0]))
            result_table = get_funnel_payment_event(event_from, event_to, start_date_pay, end_date_pay, filter_event)
        else:
            final_table = pd.DataFrame()
            for select in select_checkbox:
                start_date_pay = event_from
                end_date_pay = (event_from + datetime.timedelta(
                    days=checkbox_values.get(select)))

                table = get_funnel_payment_event(event_from, event_to, start_date_pay, end_date_pay, filter_event)
                # Data-frame шапка
                title_df = pd.DataFrame([[select]], columns=['Срез'])
                # Преобразование всей таблицы в строковый тип
                table = table.astype(str)
                # Объедините данные: title_df + данные из table
                combined_table = pd.concat([title_df, table], ignore_index=True)
                # Замена NaN на пустую строку
                combined_table = combined_table.fillna(select)
                # Добавление текущей combined_table к final_table
                final_table = pd.concat([final_table, combined_table], ignore_index=True)
            result_table = final_table
    return result_table
