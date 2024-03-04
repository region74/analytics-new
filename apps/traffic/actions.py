def traffic_landing_page_set_paid_status(modeladmin, request, queryset):
    queryset.update(paid=True)


def traffic_landing_page_set_unpaid_status(modeladmin, request, queryset):
    queryset.update(paid=False)


traffic_landing_page_set_paid_status.short_description = (
    'Выставить статус "Платный"'
)
traffic_landing_page_set_unpaid_status.short_description = (
    'Выставить статус "Не платный"'
)
