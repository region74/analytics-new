from urllib.parse import urlparse, parse_qs, urlencode

from django.utils.safestring import mark_safe
from django_tables2 import RequestConfig
from django_tables2.tables import columns

from apps.sources.models import TildaLead
from apps.datatable.table import Table, DataframeTable
from apps.datatable.forms import PerPageForm
from apps.datatable.fields import ActionsField
from apps.datatable.renderer import Renderer

from . import fields


class LeadsTable(Table):
    actions = ActionsField(action_views={"detail": "api:v1:tilda:detail"})

    per_page_form_class = PerPageForm

    class Meta(Table.Meta):
        model = TildaLead
        fields = (
            "actions",
            "tranid",
            "name",
            "phone",
            "email",
            "utm_source",
            "utm_campaign",
            "utm_medium",
        )


class UploadLeadsDetailTable(DataframeTable):
    name = columns.Column(verbose_name="ФИО", empty_values={}, orderable=False)
    email = columns.Column(
        verbose_name="E-mail", empty_values={}, orderable=False
    )
    phone = columns.Column(
        verbose_name="Телефон", empty_values={}, orderable=False
    )
    roistat_url = columns.Column(
        verbose_name="Roistat URL", empty_values={}, orderable=False
    )
    index = columns.Column(verbose_name="", empty_values={}, orderable=False)
    created = columns.Column(
        verbose_name="Дата создания", empty_values={}, orderable=False
    )

    per_page_form_class = PerPageForm

    class Meta(DataframeTable.Meta):
        sequence = (
            "index",
            "created",
            "name",
            "email",
            "phone",
            "roistat_url",
        )

    def render_roistat_url(self, value):
        url = str(value)
        if not url.startswith("http"):
            return ""
        url = urlparse(url)
        return f"{url.scheme}://{url.netloc}{url.path}"

    def __init__(self, *args, **kwargs):
        super(UploadLeadsDetailTable, self).__init__(*args, **kwargs)
        RequestConfig(self.request, paginate={"per_page": 10}).configure(self)


class UploadLeadsTable(DataframeTable):
    roistat_url = columns.Column(
        verbose_name="Список посадочных страниц", empty_values={}
    )

    class Meta(DataframeTable.Meta):
        sequence = ("roistat_url",)


class IPLReportTable(DataframeTable):
    actions = fields.IPLReportActionsField()
    id = columns.Column(verbose_name="ID")
    title = columns.Column(verbose_name="Название")
    leads = columns.Column(verbose_name="Лиды")
    ipl = columns.Column(verbose_name="IPL")
    expenses = columns.Column(verbose_name="Расход")
    romi = columns.Column(verbose_name="ROMI")
    cpl = columns.Column(verbose_name="CPL")

    per_page_form_class = PerPageForm

    class Meta(DataframeTable.Meta):
        template_name = "traffic/ipl/table.html"
        sequence = (
            "actions",
            "title",
            "leads",
            "ipl",
            "expenses",
            "romi",
            "cpl",
        )

    def render_leads(self, value):
        return Renderer.int(value)

    def render_ipl(self, value):
        return Renderer.money(value)

    def render_expenses(self, value):
        return Renderer.money(value)

    def render_romi(self, value):
        return Renderer.percent(value)

    def render_cpl(self, value):
        return Renderer.money(value)


class ChannelsTable(DataframeTable):
    channel = columns.Column(verbose_name="Канал")
    expenses = columns.Column(verbose_name="Расход")
    profit = columns.Column(verbose_name="Оборот")
    percent = columns.Column(verbose_name="Процент")
    leads_quantity = columns.Column(verbose_name="Лиды")
    payments_quantity = columns.Column(verbose_name="Оплаты")
    conversion = columns.Column(verbose_name="Конверсия")
    average_payment = columns.Column(verbose_name="Средний чек")
    lead_price = columns.Column(verbose_name="Цена лида")
    profit_on_lead = columns.Column(verbose_name="Оборот на лид")
    ipl = columns.Column(verbose_name="IPL")

    class Meta(DataframeTable.Meta):
        template_name = "traffic/channels/table.html"
        sequence = (
            "channel",
            "expenses",
            "profit",
            "percent",
            "leads_quantity",
            "payments_quantity",
            "conversion",
            "average_payment",
            "lead_price",
            "profit_on_lead",
            "ipl",
        )

    def render_expenses(self, value):
        return Renderer.money(value)

    def render_profit(self, value):
        return Renderer.money(value)

    def render_percent(self, value):
        return Renderer.percent(value)

    def render_leads_quantity(self, value):
        return Renderer.int(value)

    def render_payments_quantity(self, value):
        return Renderer.int(value)

    def render_conversion(self, value):
        return Renderer.percent(value, 2)

    def render_average_payment(self, value):
        return Renderer.money(value)

    def render_lead_price(self, value):
        return Renderer.money(value)

    def render_profit_on_lead(self, value):
        return Renderer.money(value)

    def render_ipl(self, value):
        return Renderer.money(value)


class FunnelsTable(DataframeTable):
    channel = columns.Column(verbose_name="Канал")
    expenses_intensiv3 = columns.Column(verbose_name="Расход интенсив 3 дня")
    profit_intensiv3 = columns.Column(verbose_name="Оборот интенсив 3 дня")

    expenses_intensiv2 = columns.Column(verbose_name="Расход интенсив 2 дня")
    profit_intensiv2 = columns.Column(verbose_name="Оборот интенсив 2 дня")

    expenses_gpt = columns.Column(verbose_name="Расход ChatGPT. Курс 5 уроков")
    profit_gpt = columns.Column(verbose_name="Оборот ChatGPT. Курс 5 уроков")

    expenses_neirostaff = columns.Column(verbose_name="Расход Нейростафф")
    profit_neirostaff = columns.Column(verbose_name="Оборот Нейростафф")

    expenses_7lesson = columns.Column(verbose_name="Расход Курс AI. 7 уроков")
    profit_7lesson = columns.Column(verbose_name="Оборот Курс AI. 7 уроков")

    expenses_gptveb = columns.Column(verbose_name="Расход ChatGPT. Вебинар")
    profit_gptveb = columns.Column(verbose_name="Оборот ChatGPT. Вебинар")

    class Meta(DataframeTable.Meta):
        sequence = (
            "channel",
            "expenses_intensiv3",
            "profit_intensiv3",
            "expenses_intensiv2",
            "profit_intensiv2",
            "expenses_gpt",
            "profit_gpt",
            "expenses_neirostaff",
            "profit_neirostaff",
            "expenses_7lesson",
            "profit_7lesson",
            "expenses_gptveb",
            "profit_gptveb",
        )


class DoubleTable(DataframeTable):
    channel = columns.Column(verbose_name="Канал трафика")
    count_lead = columns.Column(verbose_name="Общее количество лидов")
    count_double = columns.Column(verbose_name="Количество дублей")
    percent_double = columns.Column(verbose_name="% дублей от общего")

    class Meta(DataframeTable.Meta):
        sequence = (
            "channel",
            "count_lead",
            "count_double",
            "percent_double",
        )

    @staticmethod
    def remove_report_params(url: str) -> str:
        """Удаляет параметры report из url"""
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query, keep_blank_values=True)
        query_params.pop("report", None)
        query_params.pop("value", None)
        query_params.pop("channel", None)
        updated_url = parsed_url._replace(
            query=urlencode(query_params, doseq=True)
        ).geturl()
        return updated_url

    def template_button(self, report_name: str, value, channel, event) -> str:
        current_url = self.request.get_full_path().split("/")[-1]
        url = self.remove_report_params(current_url)
        url += (
            f"&report={report_name}&value={value}&channel={channel}&event={event}"
            if url
            else f"?report={report_name}&value={value}&channel={channel}&event={event}"
        )
        return f'<a href="{url}">%s</a>'

    def render_count_lead(self, value, record):
        channel = record[2]
        event = record[4] or None
        template = self.template_button("count_lead", value, channel, event)
        return mark_safe(template % Renderer.int(value))

    def render_count_double(self, value, record):
        channel = record[2]
        event = record[4] or None
        template = self.template_button("count_double", value, channel, event)
        return mark_safe(template % Renderer.int(value))


class TelegramReportTable(DataframeTable):
    date_event = columns.Column(verbose_name='Дата мероприятия')
    channel = columns.Column(verbose_name="Канал трафика")
    count_reg = columns.Column(verbose_name="Кол-во регистраций")
    count_reg_duplicates = columns.Column(verbose_name="Кол-во дублей (реги)")
    count_member = columns.Column(verbose_name="Кол-во участников")
    percent_from_reg = columns.Column(verbose_name="% (из реги в уч-ка)")
    tg_visit = columns.Column(verbose_name="Кол-во переходов в Телеграм")
    percent_to_tg = columns.Column(verbose_name="% (из реги в ТГ)")

    class Meta(DataframeTable.Meta):
        sequence = (
            "date_event",
            "channel",
            "count_reg",
            "count_reg_duplicates",
            "count_member",
            "percent_from_reg",
            "tg_visit",
            "percent_to_tg",
        )

    def render_count_reg(self, value):
        return Renderer.int(value)
    
    def render_count_reg_duplicates(self, value):
        return Renderer.int(value)

    def render_count_member(self, value):
        return Renderer.int(value)

    def render_percent_from_reg(self, value):
        return Renderer.percent(value)

    def render_tg_visit(self, value):
        return Renderer.int(value)

    def render_percent_to_tg(self, value):
        return Renderer.percent(value)
