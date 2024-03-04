from django.contrib import admin

from . import models, actions


@admin.register(models.FunnelChannelUrl)
class FunnelChannelUrlAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "url",
        "group",
    )
    search_fields = ("url",)
    list_filter = ("group",)


@admin.register(models.LandingPage)
class LandingPageAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "url",
        "paid",
    )
    search_fields = ("url",)
    list_filter = ("paid",)
    actions = [
        actions.traffic_landing_page_set_paid_status,
        actions.traffic_landing_page_set_unpaid_status,
    ]


@admin.register(models.Channel)
class ChannelAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "key",
        "value",
    )
    search_fields = (
        "key",
        "value",
    )
