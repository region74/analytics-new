from apps.datatable.filters import dataframe_filter, DataframeFilterSet


def fake_data_filter(dataframe, name, value, exclude):
    return dataframe


def fake_events_filter(dataframe, name, value, exclude):
    return dataframe


class FunnelFilter(DataframeFilterSet):
    payment_from = dataframe_filter.DateFilter(
        label="Оплаты с", lookup_expr=fake_data_filter
    )
    payment_to = dataframe_filter.DateFilter(
        label="Оплаты до", lookup_expr=fake_data_filter
    )
    event_from = dataframe_filter.DateFilter(
        label="Мероприятия с", lookup_expr=fake_data_filter
    )
    event_to = dataframe_filter.DateFilter(
        label="Мероприятия до", lookup_expr=fake_data_filter
    )


class EventsFilter(DataframeFilterSet):
    event_df = dataframe_filter.DateFilter(
        label="Мероприятия с", lookup_expr=fake_data_filter
    )
    event_dt = dataframe_filter.DateFilter(
        label="Мероприятия до", lookup_expr=fake_data_filter
    )
    type_all = dataframe_filter.BooleanFilter(label="Все", lookup_expr=fake_events_filter)
    type_intensiv_two = dataframe_filter.BooleanFilter(label="Интенсив 2 дня", lookup_expr=fake_events_filter)
    type_intensiv_three = dataframe_filter.BooleanFilter(label="Интенсив 3 дня", lookup_expr=fake_events_filter)
    type_intensiv_gpt = dataframe_filter.BooleanFilter(label="Интенсив chatGPT", lookup_expr=fake_events_filter)
    type_vebianrs = dataframe_filter.BooleanFilter(label="Вебинары", lookup_expr=fake_events_filter)
    type_mini_lesson = dataframe_filter.BooleanFilter(label="Мини-урок", lookup_expr=fake_events_filter)
    type_bonus = dataframe_filter.BooleanFilter(label="Акции", lookup_expr=fake_events_filter)
    pay_1week = dataframe_filter.BooleanFilter(label="1 неделя", lookup_expr=fake_events_filter)
    pay_2week = dataframe_filter.BooleanFilter(label="2 недели", lookup_expr=fake_events_filter)
    pay_4week = dataframe_filter.BooleanFilter(label="4 недели", lookup_expr=fake_events_filter)
    pay_8week = dataframe_filter.BooleanFilter(label="8 недель", lookup_expr=fake_events_filter)
