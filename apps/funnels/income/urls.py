from django.urls import path

from . import views

app_name = "income"

urlpatterns = [
    path("funnel/", views.FunnelView.as_view(), name="funnel"),
    path("events/", views.EventsView.as_view(), name="events"),
]
