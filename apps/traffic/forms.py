from crispy_forms.layout import Button

from apps.datatable.forms import FilterForm


class IPLReportFilterForm(FilterForm):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.helper.add_input(
            Button("download", "Скачать отчет", css_class="btn-extra")
        )
